#!/usr/bin/env python3
"""
Script to delete orphaned nodes from the database.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

import click
from neo4j import GraphDatabase
from config.config_loader import get_neo4j_connection_params

@click.command()
@click.option('--dry-run', is_flag=True, help='Show what would be deleted without actually deleting')
@click.option('--confirm', is_flag=True, help='Skip confirmation prompt (use with caution)')
@click.option('--batch-size', default=1000, help='Number of nodes to delete in each batch')
def delete_orphaned_nodes(dry_run, confirm, batch_size):
    """Delete orphaned nodes from the database."""
    
    # Get database connection
    uri, username, password, database = get_neo4j_connection_params()
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    try:
        with driver.session(database=database) as session:
            print("Analyzing orphaned nodes for deletion...")
            
            # Count orphaned nodes by type
            result = session.run("""
                MATCH (n)
                WHERE NOT (n)--()
                RETURN labels(n) as labels, count(n) as count
                ORDER BY count DESC
            """)
            
            orphaned_by_label = list(result)
            total_orphaned = sum(record['count'] for record in orphaned_by_label)
            
            print(f"Found {total_orphaned} orphaned nodes:")
            for record in orphaned_by_label:
                if record['count'] > 0:
                    print(f"  {record['labels']}: {record['count']} nodes")
            
            if total_orphaned == 0:
                print("No orphaned nodes found!")
                return
            
            # Show some examples
            print("\nSample orphaned nodes to be deleted:")
            
            # Sample orphaned sentences
            result = session.run("""
                MATCH (s:Sentence)
                WHERE NOT (s)--()
                RETURN s.text as text
                LIMIT 3
            """)
            sentences = list(result)
            if sentences:
                print("Orphaned sentences:")
                for record in sentences:
                    text = record['text'][:80] + "..." if len(record['text']) > 80 else record['text']
                    print(f"  - {text}")
            
            # Sample orphaned concepts
            result = session.run("""
                MATCH (c:Concept)
                WHERE NOT (c)--()
                RETURN c.name as name
                LIMIT 3
            """)
            concepts = list(result)
            if concepts:
                print("Orphaned concepts:")
                for record in concepts:
                    print(f"  - {record['name']}")
            
            if dry_run:
                print(f"\n[SUCCESS] Dry run completed - {total_orphaned} orphaned nodes would be deleted")
                print("Run without --dry-run to perform the actual deletion")
                return
            
            # Confirmation
            if not confirm:
                print(f"\n[WARNING] This will permanently delete {total_orphaned} orphaned nodes!")
                print("These nodes have no relationships and are not connected to the main graph.")
                response = input("Are you sure you want to proceed? (type 'DELETE ORPHANED' to confirm): ")
                if response != "DELETE ORPHANED":
                    print("Deletion cancelled")
                    return
            
            # Perform the deletion in batches
            print(f"\nDeleting orphaned nodes in batches of {batch_size}...")
            
            total_deleted = 0
            
            # Delete orphaned nodes in batches
            while True:
                result = session.run(f"""
                    MATCH (n)
                    WHERE NOT (n)--()
                    WITH n
                    LIMIT {batch_size}
                    DETACH DELETE n
                    RETURN count(n) as deleted_count
                """)
                
                deleted_in_batch = result.single()['deleted_count']
                total_deleted += deleted_in_batch
                
                print(f"  Deleted {deleted_in_batch} nodes in this batch (total: {total_deleted})")
                
                # If we deleted fewer than batch_size, we're done
                if deleted_in_batch < batch_size:
                    break
            
            print(f"\n[SUCCESS] Successfully deleted {total_deleted} orphaned nodes")
            
            # Verify cleanup
            result = session.run("""
                MATCH (n)
                WHERE NOT (n)--()
                RETURN count(n) as remaining_orphaned
            """)
            
            remaining_orphaned = result.single()['remaining_orphaned']
            
            # Get new total node count
            result = session.run("MATCH (n) RETURN count(n) as total_nodes")
            new_total_nodes = result.single()['total_nodes']
            
            print(f"Remaining orphaned nodes: {remaining_orphaned}")
            print(f"New total node count: {new_total_nodes}")
            
            if remaining_orphaned == 0:
                print("[SUCCESS] All orphaned nodes have been successfully deleted")
            else:
                print(f"[INFO] {remaining_orphaned} orphaned nodes still remain - these may be newly created")
            
    except Exception as e:
        print(f"Error during deletion: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.close()

if __name__ == "__main__":
    delete_orphaned_nodes()
