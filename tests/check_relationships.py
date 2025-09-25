#!/usr/bin/env python3
"""Check database relationships"""

from neo4j import GraphDatabase

def check_relationships():
    driver = GraphDatabase.driver('bolt://localhost:7687')
    
    with driver.session() as session:
        # Check node counts
        result = session.run('MATCH (n) RETURN labels(n) as labels, count(n) as count ORDER BY count DESC')
        print('NODE COUNTS:')
        for record in result:
            print(f'  {record["labels"]}: {record["count"]}')
        
        print('\nRELATIONSHIP COUNTS:')
        # Check relationship counts
        result = session.run('MATCH ()-[r]->() RETURN type(r) as rel_type, count(r) as count ORDER BY count DESC')
        for record in result:
            print(f'  {record["rel_type"]}: {record["count"]}')
        
        # Check total counts
        result = session.run('MATCH ()-[r]->() RETURN count(r) as total_rels')
        total_rels = result.single()['total_rels']
        print(f'\nTOTAL: {total_rels} relationships')
        
        # Check for specific missing relationships
        print('\nCHECKING FOR MISSING RELATIONSHIPS:')
        
        # Check PARAGRAPH_CONTAINS_SENTENCE
        result = session.run('MATCH ()-[r:PARAGRAPH_CONTAINS_SENTENCE]->() RETURN count(r) as count')
        count = result.single()['count']
        print(f'  PARAGRAPH_CONTAINS_SENTENCE: {count}')
        
        # Check SECTION_CONTAINS_SUBSECTION
        result = session.run('MATCH ()-[r:SECTION_CONTAINS_SUBSECTION]->() RETURN count(r) as count')
        count = result.single()['count']
        print(f'  SECTION_CONTAINS_SUBSECTION: {count}')
        
        # Check SUBSECTION_CONTAINS_PARAGRAPH
        result = session.run('MATCH ()-[r:SUBSECTION_CONTAINS_PARAGRAPH]->() RETURN count(r) as count')
        count = result.single()['count']
        print(f'  SUBSECTION_CONTAINS_PARAGRAPH: {count}')
        
        # Check if Subsection nodes exist
        result = session.run('MATCH (n:Subsection) RETURN count(n) as count')
        count = result.single()['count']
        print(f'  Subsection nodes: {count}')
    
    driver.close()

if __name__ == "__main__":
    check_relationships()
