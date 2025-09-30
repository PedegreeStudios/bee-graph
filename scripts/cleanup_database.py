#!/usr/bin/env python3
"""
Database cleanup script to remove orphaned sentences and prepare for clean reimport.

This script removes all existing data so you can reimport textbooks properly through
the XML import process only.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

import click
from neo4j import GraphDatabase
from config.config_loader import get_neo4j_connection_params

@click.command()
@click.option('--confirm', is_flag=True, help='Skip confirmation prompt (use with caution)')
def cleanup_database(confirm):
    """Clean up the database by removing all existing data."""
    
    if not confirm:
        print("WARNING: This will delete ALL data in the database!")
        print("This includes:")
        print("  - All sentences (including orphaned ones)")
        print("  - All concepts and relationships")
        print("  - All textbook hierarchy data")
        print("  - All other nodes and relationships")
        
        response = input("\nAre you sure you want to proceed? (type 'DELETE ALL DATA' to confirm): ")
        if response != "DELETE ALL DATA":
            print("Cleanup cancelled")
            return
    
    # Get database connection
    uri, username, password, database = get_neo4j_connection_params()
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    try:
        with driver.session(database=database) as session:
            print("Cleaning up database...")
            
            # Get current node counts
            result = session.run("MATCH (n) RETURN count(n) as total_nodes")
            total_nodes = result.single()["total_nodes"]
            
            result = session.run("MATCH (s:Sentence) RETURN count(s) as sentence_count")
            sentence_count = result.single()["sentence_count"]
            
            result = session.run("MATCH (c:Concept) RETURN count(c) as concept_count")
            concept_count = result.single()["concept_count"]
            
            print(f"Current database state:")
            print(f"  Total nodes: {total_nodes}")
            print(f"  Sentences: {sentence_count}")
            print(f"  Concepts: {concept_count}")
            
            # Delete all nodes and relationships
            print("\nDeleting all nodes and relationships...")
            session.run("MATCH (n) DETACH DELETE n")
            
            # Verify cleanup
            result = session.run("MATCH (n) RETURN count(n) as remaining_nodes")
            remaining_nodes = result.single()["remaining_nodes"]
            
            print(f"\nCleanup completed!")
            print(f"Remaining nodes: {remaining_nodes}")
            
            if remaining_nodes == 0:
                print("✅ Database is now clean and ready for fresh import")
                print("\nNext steps:")
                print("1. Run: python scripts/load_textbooks.py --textbook textbooks/osbooks-biology-bundle --cleanup")
                print("2. This will import all textbooks through the proper XML hierarchy")
                print("3. All sentences will be properly connected and ready for concept extraction")
            else:
                print("⚠️  Some nodes remain - cleanup may not have been complete")
                
    except Exception as e:
        print(f"Error during cleanup: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.close()

if __name__ == "__main__":
    cleanup_database()
