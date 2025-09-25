#!/usr/bin/env python3
"""Check for reverse relationships"""

from neo4j import GraphDatabase

def check_reverse_relationships():
    driver = GraphDatabase.driver('bolt://localhost:7687')
    
    with driver.session() as session:
        # Check for BELONGS_TO relationships
        result = session.run('MATCH ()-[r]->() WHERE type(r) CONTAINS "BELONGS_TO" RETURN type(r) as rel_type, count(r) as count ORDER BY count DESC')
        print('BELONGS_TO RELATIONSHIPS:')
        records = list(result)
        if records:
            for record in records:
                print(f'  {record["rel_type"]}: {record["count"]}')
        else:
            print('  No BELONGS_TO relationships found')
        
        # Check all relationship types to see what we have
        result = session.run('MATCH ()-[r]->() RETURN DISTINCT type(r) as rel_type ORDER BY rel_type')
        print('\nALL RELATIONSHIP TYPES:')
        for record in result:
            print(f'  {record["rel_type"]}')
        
        # Check total relationship count
        result = session.run('MATCH ()-[r]->() RETURN count(r) as total')
        total = result.single()['total']
        print(f'\nTOTAL RELATIONSHIPS: {total}')
    
    driver.close()

if __name__ == "__main__":
    check_reverse_relationships()
