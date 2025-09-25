#!/usr/bin/env python3
"""Quick database check script"""

from neo4j import GraphDatabase

def check_database():
    driver = GraphDatabase.driver('bolt://localhost:7687')
    
    with driver.session() as session:
        # Check node counts
        result = session.run('MATCH (n) RETURN labels(n) as labels, count(n) as count ORDER BY count DESC')
        print('NODE COUNTS:')
        for record in result:
            print(f'  {record["labels"]}: {record["count"]}')
        
        # Check relationship counts
        result = session.run('MATCH ()-[r]->() RETURN type(r) as rel_type, count(r) as count ORDER BY count DESC')
        print('\nRELATIONSHIP COUNTS:')
        for record in result:
            print(f'  {record["rel_type"]}: {record["count"]}')
        
        # Check total counts
        result = session.run('MATCH (n) RETURN count(n) as total_nodes')
        total_nodes = result.single()['total_nodes']
        result = session.run('MATCH ()-[r]->() RETURN count(r) as total_rels')
        total_rels = result.single()['total_rels']
        print(f'\nTOTAL: {total_nodes} nodes, {total_rels} relationships')
    
    driver.close()

if __name__ == "__main__":
    check_database()
