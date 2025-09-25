#!/usr/bin/env python3
"""
OpenStax Textbook Loader Script for Neo4j

This script loads OpenStax textbook content into Neo4j database using the XML parser
with dual labeling schema support. It processes both collection hierarchy and 
document-level content structure.

HIERARCHY PROCESSING:
Collection Level: Book → Chapter → Subchapter → Document
Document Level: Document → Section → Subsection → Paragraph → Sentence → Concept

FEATURES:
- Complete hierarchy parsing from collection XML files
- Document content extraction from module CNXML files  
- Namespaced IDs to prevent cross-textbook conflicts
- Dual labeling schema (CONTAINS + BELONGS_TO relationships)
- Content extraction and sentence segmentation
- Concept extraction (planned future feature)

Usage:
    # Load all collections (default behavior)
    python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --bulk-import --batch-size 2000
    
    # Load specific collection
    python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --collection biology-2e.collection
    
    # List available collections
    python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --list-collections
    
    # Bulk import with cleanup
    python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --bulk-import --cleanup
    
    # Dry run to test without changes
    python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --dry-run
"""

import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any
import click
import logging
from tqdm import tqdm

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from textbook_parse.xml_parser import OpenStaxXMLParser
from textbook_parse.bulk_import import create_bulk_importer
from neo4j_utils import Neo4jSchemaSetup, Neo4jNodeCreator, Neo4jRelationshipCreator
from neo4j import GraphDatabase

# Configure logging - Critical errors only
logging.basicConfig(
    level=logging.CRITICAL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress verbose logging from external libraries
logging.getLogger('neo4j').setLevel(logging.CRITICAL)
logging.getLogger('textbook_parse').setLevel(logging.CRITICAL)


def check_collection_exists(uri: str, username: str, password: str, database: str, collection_name: str) -> bool:
    """Check if a collection already exists in the database."""
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        with driver.session(database=database) as session:
            # Remove .collection suffix if present to match book_id format
            book_id = collection_name.replace('.collection', '')
            
            # Check if any Book nodes exist with this book_id
            result = session.run(
                "MATCH (b:Book) WHERE b.book_id = $book_id RETURN count(b) as count",
                book_id=book_id
            )
            count = result.single()["count"]
            return count > 0
    except Exception as e:
        logger.error(f"Error checking if collection exists: {e}")
        return False
    finally:
        if 'driver' in locals():
            driver.close()


def clear_entire_database(uri: str, username: str, password: str, database: str) -> bool:
    """Completely clear the entire database - removes all nodes and relationships."""
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(uri, auth=(username, password))
        
        with driver.session(database=database) as session:
            # Get counts before deletion
            result = session.run("MATCH (n) RETURN count(n) as node_count")
            node_count = result.single()["node_count"]
            result = session.run("MATCH ()-[r]->() RETURN count(r) as rel_count")
            rel_count = result.single()["rel_count"]
            
            print(f"Clearing entire database...")
            print(f"  Nodes to delete: {node_count}")
            print(f"  Relationships to delete: {rel_count}")
            
            # Delete all relationships first
            if rel_count > 0:
                session.run("MATCH ()-[r]->() DELETE r")
                print(f"  Deleted {rel_count} relationships")
            
            # Delete all nodes
            if node_count > 0:
                session.run("MATCH (n) DELETE n")
                print(f"  Deleted {node_count} nodes")
            
            print("Database completely cleared")
            return True
            
    except Exception as e:
        print(f"Error clearing database: {e}")
        return False
    finally:
        if 'driver' in locals():
            driver.close()


@click.command()
@click.option('--textbook-path', required=True, help='Path to OpenStax textbook directory')
@click.option('--collection', help='Specific collection to import (e.g., biology-2e, biology-ap-courses)')
@click.option('--uri', default='bolt://localhost:7687', help='Neo4j URI')
@click.option('--username', default='', help='Neo4j username (optional for no-auth)')
@click.option('--password', default='', help='Neo4j password (optional for no-auth)')
@click.option('--database', default='neo4j', help='Database name')
@click.option('--setup-schema', is_flag=True, help='Set up database schema before import')
@click.option('--dry-run', is_flag=True, help='Parse files without importing to database')
@click.option('--list-collections', is_flag=True, help='List available collections and exit')
@click.option('--verify', is_flag=True, help='Verify import after completion')
@click.option('--bulk-import', is_flag=True, help='Use bulk import for better performance with large datasets')
@click.option('--batch-size', default=2000, help='Batch size for bulk operations (default: 2000)')
@click.option('--cleanup', is_flag=True, help='Clean up existing data (sample data and textbooks) before import')
@click.option('--full-cleanup', is_flag=True, help='Completely clear the entire database before import (removes all nodes and relationships)')
@click.option('--force', is_flag=True, help='Force loading even if collections already exist')
def main(textbook_path: str, collection: str, uri: str, username: str, password: str, database: str,
         setup_schema: bool, dry_run: bool, list_collections: bool, verify: bool, bulk_import: bool, batch_size: int, cleanup: bool, full_cleanup: bool, force: bool):
    """Load OpenStax textbook content into Neo4j database using XML parser.
    
    This script parses OpenStax textbook XML files and loads the content into a Neo4j database.
    It supports both individual collection loading and bulk loading of all collections.
    By default, it will skip collections that already exist in the database.
    
    Examples:
        # Load all collections (default behavior)
        python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle
        
        # Load specific collection
        python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --collection biology-2e.collection
        
        # Bulk import with cleanup for better performance
        python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --bulk-import --cleanup
        
        # Force reload existing collections
        python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --force
        
        # Dry run to test without changes
        python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --dry-run
    """
    
    print("OPENSTAX TEXTBOOK LOADER")
    print("=" * 50)
    
    # Validate textbook path
    textbook_dir = Path(textbook_path)
    if not textbook_dir.exists():
        print(f"ERROR: Textbook directory not found: {textbook_path}")
        return
    
    if not textbook_dir.is_dir():
        print(f"ERROR: Path is not a directory: {textbook_path}")
        return
    
    # Check for collections directory
    collections_dir = textbook_dir / "collections"
    modules_dir = textbook_dir / "modules"
    
    if not collections_dir.exists():
        print(f"ERROR: No collections directory found in {textbook_path}")
        print("   Expected: collections/ directory")
        return
    
    # List available collections if requested
    if list_collections:
        print("\nAvailable Collections:")
        print("-" * 30)
        
        available_collections = []
        for collection_file in collections_dir.glob("*.xml"):
            collection_name = collection_file.stem
            available_collections.append(collection_name)
            print(f"  {collection_name}")
        
        if not available_collections:
            print("  No collections found")
        else:
            print(f"\nTIP: Use --collection <name> to import a specific collection")
            print(f"   Example: --collection {available_collections[0]}")
        
        return
    
    # Validate specific collection if provided
    if collection:
        collection_file = collections_dir / f"{collection}.xml"
        if not collection_file.exists():
            print(f"Collection '{collection}' not found")
            print("   Use --list-collections to see available collections")
            return
        
        print(f"Importing specific collection: {collection}")
        print(f"Collection file: {collection_file}")
    else:
        print("Importing all collections")
    
    print(f"Textbook: {textbook_dir.name}")
    print(f"Database: {database}")
    
    if bulk_import:
        print(f"Mode: Bulk Import (batch size: {batch_size})")
    else:
        print("Mode: Standard Import")
    
    if dry_run:
        print("DRY RUN MODE - No database changes will be made")
    
    # Check for existing data if not doing cleanup
    if not cleanup and not dry_run:
        try:
            # Quick check for existing data
            temp_parser = OpenStaxXMLParser(uri, username, password, database)
            with temp_parser.node_creator.driver.session(database=database) as session:
                result = session.run("MATCH (n) RETURN count(n) as node_count")
                node_count = result.single()["node_count"]
                
                if node_count > 0:
                    print(f"\nWARNING: Found {node_count} existing nodes in the database")
                    print("This may include sample data or previously imported textbooks.")
                    print("Use --cleanup to clear existing data before import")
                    print("Use --dry-run to test without making changes")
                    
                    confirm = input("\nContinue without clearing existing data? (yes/no): ")
                    if confirm.lower() not in ['yes', 'y']:
                        print("Import cancelled")
                        return
        except Exception as e:
            logger.warning(f"Could not check for existing data: {e}")
        finally:
            if 'temp_parser' in locals():
                temp_parser.close_connections()
    
    # Initialize XML parser
    parser = OpenStaxXMLParser(uri, username, password, database)
    
    # Initialize bulk importer if requested
    bulk_importer = None
    if bulk_import and not dry_run:
        bulk_importer = create_bulk_importer(uri, username, password, database)
        print(f"Initialized bulk importer with batch size: {batch_size}")
    
    try:
        # Set up schema if requested
        if setup_schema and not dry_run:
            print("\nSetting up database schema...")
            schema_setup = Neo4jSchemaSetup(uri, username, password, database)
            
            if not schema_setup.check_neo4j_connection():
                print("Failed to connect to Neo4j database")
                return
            
            if cleanup:
                print("Cleaning up existing data...")
                if not schema_setup.clear_database():
                    print("Failed to clear database")
                    return
                print("Database cleared")
            
            if not schema_setup.setup_constraints():
                print("Failed to set up constraints")
                return
            
            if not schema_setup.setup_indexes():
                print("Failed to set up indexes")
                return
            
            print("Schema setup completed")
        
        # Full cleanup if requested (completely clear database)
        if full_cleanup and not dry_run:
            print("\nPerforming full database cleanup...")
            if not clear_entire_database(uri, username, password, database):
                print("Failed to clear entire database")
                return
        
        # Clean up sample data if requested (even without setup_schema)
        elif cleanup and not dry_run:
            print("\nCleaning up existing data...")
            if not parser.clear_sample_data(uri, username, password, database):
                print("Failed to clear existing data")
                return
        
        # Parse and load collections
        if collection:
            # Load specific collection
            collection_file = collections_dir / f"{collection}.xml"
            
            # Check if collection already exists (unless force is used)
            if not force:
                if check_collection_exists(uri, username, password, database, collection):
                    print(f"\nCollection '{collection}' already exists in the database.")
                    print(f"Use --force to reload it anyway, or --full-cleanup to clear the database first.")
                    return
            
            print(f"\nLoading collection: {collection}")
            success = parser.load_collection(collection_file, textbook_dir, dry_run, bulk_importer, batch_size)
            if not success:
                print(f"Failed to load collection: {collection}")
                return
        else:
            # Load all collections
            collection_files = list(collections_dir.glob("*.xml"))
            print(f"\nFound {len(collection_files)} collections to load")
            
            # Filter out existing collections unless force is used
            collections_to_load = []
            if not force:
                for collection_file in collection_files:
                    collection_name = collection_file.stem
                    if check_collection_exists(uri, username, password, database, collection_name):
                        print(f"Skipping existing collection: {collection_name}")
                    else:
                        collections_to_load.append(collection_file)
                
                if not collections_to_load:
                    print("All collections already exist in the database.")
                    print("Use --force to reload them anyway, or --full-cleanup to clear the database first.")
                    return
                
                print(f"Loading {len(collections_to_load)} new collections (skipped {len(collection_files) - len(collections_to_load)} existing)")
            else:
                collections_to_load = collection_files
            
            # Create progress bar for collections
            with tqdm(total=len(collections_to_load), desc="Loading collections", unit="collection") as pbar:
                for collection_file in collections_to_load:
                    collection_name = collection_file.stem
                    pbar.set_description(f"Loading {collection_name}")
                    success = parser.load_collection(collection_file, textbook_dir, dry_run, bulk_importer, batch_size)
                    if not success:
                        print(f"\nFailed to load collection: {collection_name}")
                    pbar.update(1)
        
        # Verify import if requested
        if verify and not dry_run:
            print("\nVerifying import...")
            verification = parser.verify_import(database)
            if verification:
                print("Import verification completed")
                print(f"   Total nodes: {verification['total_nodes']}")
                print(f"   Total relationships: {verification['total_relationships']}")
            else:
                print("Import verification failed")
        
        print(f"\nLoading completed!")
        print(f"Textbook: {textbook_dir.name}")
        if collection:
            print(f"Collection: {collection}")
        print(f"Database: {database}")
        print(f"Neo4j Browser: http://localhost:7474")
        
        if not dry_run:
            print(f"\nNavigate to localhost:7474 to see your imported data")
            print(f"   ")
            print(f"\nTo clear existing data before future imports:")
            print(f"   python scripts/load_textbooks.py --textbook-path <path> --cleanup")
            print(f"   python scripts/load_textbooks.py --textbook-path <path> --full-cleanup")
        
    except KeyboardInterrupt:
        print("\nLoading cancelled by user")
    except Exception as e:
        print(f"\nLoading failed: {e}")
        logger.exception("Loading error")
    finally:
        parser.close_connections()
        if bulk_importer:
            bulk_importer.close()


if __name__ == "__main__":
    main()
