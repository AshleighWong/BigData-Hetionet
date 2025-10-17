import argparse
import csv
import time
from neo4j import GraphDatabase
from pymongo import MongoClient
from typing import Dict, List, Any


class HetioNetDB:
    
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str,
                 mongo_uri: str, mongo_db: str):
        self.neo4j_driver = GraphDatabase.driver(
            neo4j_uri, 
            auth=(neo4j_user, neo4j_password)
        )
        
        #MongoDB connection
        self.mongo_client = MongoClient(mongo_uri)
        self.mongo_db = self.mongo_client[mongo_db]
        self.diseases_collection = self.mongo_db['diseases']
        
        print("Connected to Neo4j and MongoDB")
    
    def close(self):
        self.neo4j_driver.close()
        self.mongo_client.close()
        print("Database connections closed")
    
    # ==================== DATA LOADING ====================
    
    def load_nodes(self, nodes_file: str):
        print(f"\nLoading nodes from {nodes_file}...")
        
        with open(nodes_file, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            nodes_by_kind = {}
            
            for row in reader:
                node_id = row.get('Id') or row.get('id')
                name = row.get('Name') or row.get('name')
                kind = row.get('Kind') or row.get('kind')
                
                if kind not in nodes_by_kind:
                    nodes_by_kind[kind] = []
                
                nodes_by_kind[kind].append({'id': node_id, 'name': name})
        
        #create nodes in Neo
        with self.neo4j_driver.session() as session:
            for kind, nodes in nodes_by_kind.items():
                session.run(f"""
                    UNWIND $nodes AS node
                    CREATE (n:{kind} {{id: node.id, name: node.name}})
                """, nodes=nodes)
                print(f"Created {len(nodes)} {kind} nodes")
        
        #create indexes
        self._create_indexes()
    
    def load_edges(self, edges_file: str):
        print(f"\nLoading edges from {edges_file}...")
        
        with open(edges_file, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            edges_by_type = {}
            
            for row in reader:
                source = row.get('Source') or row.get('source')
                target = row.get('Target') or row.get('target')
                metaedge = row.get('Metaedge') or row.get('metaedge')
                
                if metaedge not in edges_by_type:
                    edges_by_type[metaedge] = []
                
                edges_by_type[metaedge].append({
                    'source': source, 
                    'target': target
                })
        
        with self.neo4j_driver.session() as session:
            for metaedge, edges in edges_by_type.items():
                rel_type = self._parse_metaedge(metaedge)

                session.run(f"""
                    UNWIND $edges AS edge
                    MATCH (source {{id: edge.source}})
                    MATCH (target {{id: edge.target}})
                    CREATE (source)-[:{rel_type}]->(target)
                """, edges=edges)
                print(f"Created {len(edges)} {rel_type} relationships")
    
    def _parse_metaedge(self, metaedge: str) -> str:
        clean_metaedge = metaedge.replace('>', '').replace('<', '')
        
        mapping = {
            'GiG': 'INTERACTS',
            'GrG': 'REGULATES',
            'GcG': 'COVARIES',
            'CtD': 'TREATS',
            'CpD': 'PALLIATES',
            'CuG': 'UPREGULATES',
            'CdG': 'DOWNREGULATES',
            'CbG': 'BINDS',
            'DaG': 'ASSOCIATES',
            'DuG': 'UPREGULATES',
            'DdG': 'DOWNREGULATES',
            'DlA': 'LOCALIZES_TO',
            'AuG': 'UPREGULATES',
            'AdG': 'DOWNREGULATES',
            'AeG': 'EXPRESSES',
            'CRC': 'RESEMBLES',
            'DRD': 'RESEMBLES',
            'GRG': 'REGULATES'
        }
        return mapping.get(clean_metaedge, clean_metaedge.upper().replace('>', '_').replace('<', '_'))
    
    def _create_indexes(self):
        with self.neo4j_driver.session() as session:
            for node_type in ['Gene', 'Compound', 'Disease', 'Anatomy']:
                session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{node_type}) ON (n.id)")
        print("Created indexes")
    
    def build_mongo_cache(self):
        #Build MongoDB cache for Query 1
        print("\nBuilding MongoDB cache for diseases...")
        
        with self.neo4j_driver.session() as session:
            #Get all diseases
            result = session.run("MATCH (d:Disease) RETURN d.id as id, d.name as name")
            
            for record in result:
                disease_id = record['id']
                disease_name = record['name']
                
                #get disease information from Neo
                disease_info = self._get_disease_info_from_neo4j(disease_id, disease_name)
                
                #store in Mongo
                self.diseases_collection.replace_one(
                    {'_id': disease_id},
                    disease_info,
                    upsert=True
                )
        
        print(f"Cached {self.diseases_collection.count_documents({})} diseases")
    
    def _get_disease_info_from_neo4j(self, disease_id: str, disease_name: str) -> Dict:
        #Get disease information from Neo
        with self.neo4j_driver.session() as session:
            result = session.run("""
                MATCH (d:Disease {id: $diseaseId})
                OPTIONAL MATCH (c:Compound)-[r:TREATS|PALLIATES]->(d)
                OPTIONAL MATCH (d)-[gr:ASSOCIATES|UPREGULATES|DOWNREGULATES]->(g:Gene)
                OPTIONAL MATCH (d)-[:LOCALIZES_TO]->(a:Anatomy)
                RETURN 
                    d.name as disease_name,
                    collect(DISTINCT {id: c.id, name: c.name, type: type(r)}) as drugs,
                    collect(DISTINCT {id: g.id, name: g.name, relationship: type(gr)}) as genes,
                    collect(DISTINCT {id: a.id, name: a.name}) as locations
            """, diseaseId=disease_id).single()
            
            return {
                '_id': disease_id,
                'name': result['disease_name'],
                'drugs': [d for d in result['drugs'] if d['id'] is not None],
                'genes': [g for g in result['genes'] if g['id'] is not None],
                'locations': [l for l in result['locations'] if l['id'] is not None]
            }
    
    # ==================== QUERIES ====================
    
    def query1_disease_info(self, disease_id: str, use_cache: bool = True) -> Dict:
        #Query 1: Get disease information
        #Returns disease name, treating drugs, associated genes, and locations
        start_time = time.time()
        
        if use_cache:
            #Query from MongoDB cache
            result = self.diseases_collection.find_one({'_id': disease_id})
            source = "MongoDB (cached)"
        else:
            #Query directly from Neo4j
            with self.neo4j_driver.session() as session:
                neo_result = session.run("""
                    MATCH (d:Disease {id: $diseaseId})
                    OPTIONAL MATCH (c:Compound)-[r:TREATS|PALLIATES]->(d)
                    OPTIONAL MATCH (d)-[gr:ASSOCIATES|UPREGULATES|DOWNREGULATES]->(g:Gene)
                    OPTIONAL MATCH (d)-[:LOCALIZES_TO]->(a:Anatomy)
                    RETURN 
                        d.name as disease_name,
                        collect(DISTINCT {id: c.id, name: c.name, type: type(r)}) as drugs,
                        collect(DISTINCT {id: g.id, name: g.name, relationship: type(gr)}) as genes,
                        collect(DISTINCT {id: a.id, name: a.name}) as locations
                """, diseaseId=disease_id).single()
                
                result = {
                    '_id': disease_id,
                    'name': neo_result['disease_name'],
                    'drugs': [d for d in neo_result['drugs'] if d['id'] is not None],
                    'genes': [g for g in neo_result['genes'] if g['id'] is not None],
                    'locations': [l for l in neo_result['locations'] if l['id'] is not None]
                }
            source = "Neo4j (direct)"
        
        elapsed_time = time.time() - start_time
        
        if result:
            result['query_time_ms'] = round(elapsed_time * 1000, 2)
            result['data_source'] = source
        
        return result
    
    def query2_find_treatments(self, disease_id: str) -> List[Dict]:
        # Query 2: Find potential new treatments
        start_time = time.time()
        
        with self.neo4j_driver.session() as session:
            result = session.run("""
                // Find disease and its locations
                MATCH (d:Disease {id: $diseaseId})-[:LOCALIZES_TO]->(a:Anatomy)
                
                // Find genes regulated by both compound and anatomy in opposite directions
                MATCH (c:Compound)-[cr:UPREGULATES|DOWNREGULATES]->(g:Gene)
                MATCH (a)-[ar:UPREGULATES|DOWNREGULATES]->(g)
                
                // Ensure opposite regulation
                WHERE (type(cr) = 'UPREGULATES' AND type(ar) = 'DOWNREGULATES')
                   OR (type(cr) = 'DOWNREGULATES' AND type(ar) = 'UPREGULATES')
                
                // Exclude existing treatments
                AND NOT (c)-[:TREATS|PALLIATES]->(d)
                
                RETURN DISTINCT 
                    c.id as compound_id, 
                    c.name as compound_name,
                    collect(DISTINCT {
                        gene: g.name,
                        compound_effect: type(cr),
                        anatomy: a.name,
                        anatomy_effect: type(ar)
                    }) as mechanisms
                ORDER BY compound_name
            """, diseaseId=disease_id)
            
            treatments = []
            for record in result:
                treatments.append({
                    'compound_id': record['compound_id'],
                    'compound_name': record['compound_name'],
                    'mechanisms': record['mechanisms']
                })
        
        elapsed_time = time.time() - start_time
        
        return {
            'disease_id': disease_id,
            'potential_treatments_count': len(treatments),
            'treatments': treatments,
            'query_time_ms': round(elapsed_time * 1000, 2)
        }


# ==================== CLI INTERFACE ====================

def create_database(args):
    #Create and populate the database
    db = HetioNetDB(
        neo4j_uri=args.neo4j_uri,
        neo4j_user=args.neo4j_user,
        neo4j_password=args.neo4j_password,
        mongo_uri=args.mongo_uri,
        mongo_db=args.mongo_db
    )
    
    print("\n" + "="*60)
    print("CREATING HETIONET DATABASE")
    print("="*60)
    
    db.load_nodes(args.nodes_file)
    db.load_edges(args.edges_file)
    
    #MongoDB cache
    if args.build_cache:
        db.build_mongo_cache()
    
    print("\nDatabase creation complete!")
    db.close()


def query_disease(args):
    #Query 1: Get disease information
    db = HetioNetDB(
        neo4j_uri=args.neo4j_uri,
        neo4j_user=args.neo4j_user,
        neo4j_password=args.neo4j_password,
        mongo_uri=args.mongo_uri,
        mongo_db=args.mongo_db
    )
    
    print("\n" + "="*60)
    print(f"QUERY 1: DISEASE INFORMATION")
    print("="*60)
    
    result = db.query1_disease_info(args.disease_id, use_cache=not args.no_cache)
    
    if result:
        print(f"\nDisease ID: {result['_id']}")
        print(f"Disease Name: {result['name']}")
        print(f"\nDrugs ({len(result['drugs'])}):")
        for drug in result['drugs']:
            print(f"  - {drug['name']} ({drug['type']})")
        
        print(f"\nGenes ({len(result['genes'])}):")
        for gene in result['genes']:
            print(f"  - {gene['name']} ({gene['relationship']})")
        
        print(f"\nLocations ({len(result['locations'])}):")
        for loc in result['locations']:
            print(f"  - {loc['name']}")
        
        print(f"\nQuery Time: {result['query_time_ms']} ms")
        print(f"Data Source: {result['data_source']}")
    else:
        print(f"Disease {args.disease_id} not found")
    
    db.close()


def find_treatments(args):
    #Query 2: Find potential new treatments
    db = HetioNetDB(
        neo4j_uri=args.neo4j_uri,
        neo4j_user=args.neo4j_user,
        neo4j_password=args.neo4j_password,
        mongo_uri=args.mongo_uri,
        mongo_db=args.mongo_db
    )
    
    print("\n" + "="*60)
    print(f"QUERY 2: FIND POTENTIAL TREATMENTS")
    print("="*60)
    
    result = db.query2_find_treatments(args.disease_id)
    
    print(f"\nDisease ID: {result['disease_id']}")
    print(f"Potential Treatments Found: {result['potential_treatments_count']}")
    
    if result['treatments']:
        print("\nCompounds:")
        for i, treatment in enumerate(result['treatments'], 1):
            print(f"\n{i}. {treatment['compound_name']} ({treatment['compound_id']})")
            if args.verbose:
                print("   Mechanisms:")
                for mech in treatment['mechanisms'][:3]:  
                    print(f"   - Gene: {mech['gene']}")
                    print(f"     Compound: {mech['compound_effect']}")
                    print(f"     Location ({mech['anatomy']}): {mech['anatomy_effect']}")
    
    print(f"\nQuery Time: {result['query_time_ms']} ms")
    db.close()


def main():
    #main CLI
    parser = argparse.ArgumentParser(
        description='HetioNet Database Client',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    #Create database command
    create_parser = subparsers.add_parser('create', help='Create and populate database')
    create_parser.add_argument('--nodes-file', required=True, help='Path to nodes.tsv')
    create_parser.add_argument('--edges-file', required=True, help='Path to edges.tsv')
    create_parser.add_argument('--neo4j-uri', default='bolt://localhost:7687')
    create_parser.add_argument('--neo4j-user', default='neo4j')
    create_parser.add_argument('--neo4j-password', required=True)
    create_parser.add_argument('--mongo-uri', default='mongodb://localhost:27017')
    create_parser.add_argument('--mongo-db', default='hetionet')
    create_parser.add_argument('--build-cache', action='store_true', help='Build MongoDB cache')
    create_parser.set_defaults(func=create_database)
    
    #Query 1 command
    query1_parser = subparsers.add_parser('disease', help='Query disease information (Query 1)')
    query1_parser.add_argument('disease_id', help='Disease ID (e.g., Disease::DOID:263)')
    query1_parser.add_argument('--neo4j-uri', default='bolt://localhost:7687')
    query1_parser.add_argument('--neo4j-user', default='neo4j')
    query1_parser.add_argument('--neo4j-password', required=True)
    query1_parser.add_argument('--mongo-uri', default='mongodb://localhost:27017')
    query1_parser.add_argument('--mongo-db', default='hetionet')
    query1_parser.add_argument('--no-cache', action='store_true', help='Query Neo4j directly')
    query1_parser.set_defaults(func=query_disease)
    
    #Query 2 command
    query2_parser = subparsers.add_parser('treatments', help='Find potential treatments (Query 2)')
    query2_parser.add_argument('disease_id', help='Disease ID (e.g., Disease::DOID:263)')
    query2_parser.add_argument('--neo4j-uri', default='bolt://localhost:7687')
    query2_parser.add_argument('--neo4j-user', default='neo4j')
    query2_parser.add_argument('--neo4j-password', required=True)
    query2_parser.add_argument('--mongo-uri', default='mongodb://localhost:27017')
    query2_parser.add_argument('--mongo-db', default='hetionet')
    query2_parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed mechanisms')
    query2_parser.set_defaults(func=find_treatments)
    
    args = parser.parse_args()
    
    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()