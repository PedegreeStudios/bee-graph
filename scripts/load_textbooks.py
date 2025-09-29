#!/usr/bin/env python3
"""
OpenStax Textbook Loader Script for Neo4j

This script loads OpenStax textbook content into Neo4j database with automatic concept extraction.
It processes both collection hierarchy and document-level content structure.

HIERARCHY PROCESSING:
Collection Level: Book → Chapter → Subchapter → Document
Document Level: Document → Section → Subsection → Paragraph → Sentence → Concept

FEATURES:
- Complete hierarchy parsing from collection XML files
- Document content extraction from module CNXML files  
- Namespaced IDs to prevent cross-textbook conflicts
- Dual labeling schema (CONTAINS + BELONGS_TO relationships)
- Content extraction and sentence segmentation
- Automatic concept extraction using Wikidata

Usage:
    # Load all collections with automatic concept extraction (default)
    python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle
    
    # Load specific collection
    python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --collection biology-2e
    
    # Clear existing data and reload everything
    python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --cleanup
    
    # Skip concept extraction for faster loading
    python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --no-concepts
    
    # Use more workers for faster concept extraction
    python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --workers 8
    
    # Delete a specific collection
    python scripts/load_textbooks.py --delete-collection biology-2e
    
    # Delete all collections from a textbook
    python scripts/load_textbooks.py --delete-textbook biology
    
    # List all textbooks and collections in database
    python scripts/load_textbooks.py --list-textbooks
    
    # Clean up orphaned nodes (nodes without relationships)
    python scripts/load_textbooks.py --cleanup-orphans
"""

import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any
import click
import logging
# Removed tqdm import to avoid threading issues
import time
from datetime import datetime, timedelta

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from textbook_parse.xml_parser import OpenStaxXMLParser
from textbook_parse.bulk_import import create_bulk_importer
from textbook_parse.concept_extraction.main import ConceptExtractionSystem
from textbook_parse.concept_extraction.sequential_processor import SequentialCollectionProcessor
from neo4j_utils import Neo4jSchemaSetup, Neo4jNodeCreator, Neo4jRelationshipCreator
from neo4j import GraphDatabase

# Configure logging - Show INFO level and above
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress verbose logging from external libraries
logging.getLogger('neo4j').setLevel(logging.CRITICAL)
logging.getLogger('textbook_parse').setLevel(logging.CRITICAL)

# Enable INFO logging for concept extraction
logging.getLogger('textbook_parse.concept_extraction').setLevel(logging.INFO)


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


def list_available_textbooks_and_collections(uri: str, username: str, password: str, database: str) -> None:
    """List all available textbooks and collections in the database."""
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(uri, auth=(username, password))
        
        with driver.session(database=database) as session:
            # Get all books grouped by textbook
            result = session.run("""
                MATCH (b:Book)
                WITH split(b.book_id, '-')[0] as textbook, collect(b.book_id) as collections
                RETURN textbook, collections
                ORDER BY textbook
            """)
            
            textbooks = {}
            for record in result:
                textbook = record["textbook"]
                collections = record["collections"]
                textbooks[textbook] = collections
            
            if not textbooks:
                print("No textbooks found in the database")
                return
            
            print("Available textbooks and collections:")
            print("=" * 50)
            
            for textbook, collections in textbooks.items():
                print(f"\nTextbook: {textbook}")
                print(f"  Collections ({len(collections)}):")
                for collection in sorted(collections):
                    print(f"    - {collection}")
            
            print(f"\nTotal textbooks: {len(textbooks)}")
            total_collections = sum(len(collections) for collections in textbooks.values())
            print(f"Total collections: {total_collections}")
            
            print(f"\nDelete examples:")
            print(f"  Delete entire textbook: --delete-textbook {list(textbooks.keys())[0]}")
            print(f"  Delete specific collection: --delete-collection {list(textbooks.values())[0][0]}")
            
    except Exception as e:
        print(f"Error listing textbooks and collections: {e}")
    finally:
        if 'driver' in locals():
            driver.close()


def delete_textbook_collections(uri: str, username: str, password: str, database: str, textbook_name: str) -> bool:
    """Delete all collections from a specific textbook using comprehensive batched operations."""
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(uri, auth=(username, password))
        
        with driver.session(database=database) as session:
            # Get all collections for this textbook
            result = session.run("""
                MATCH (b:Book) 
                WHERE b.book_id STARTS WITH $textbook_name
                RETURN collect(b.book_id) as book_ids
            """, textbook_name=textbook_name)
            
            collections = result.single()["book_ids"]
            
            if not collections:
                print(f"No collections found for textbook: {textbook_name}")
                return True
            
            print(f"Found {len(collections)} collections to delete from textbook: {textbook_name}")
            print(f"Collections: {', '.join(collections)}")
            
            # Use comprehensive deletion approach
            # Delete relationships first (more aggressive approach)
            print("Deleting all relationships from/to textbook nodes...")
            deleted_rels = 0
            batch_size = 500
            
            while True:
                result = session.run("""
                    MATCH (book:Book)
                    WHERE book.book_id IN $book_ids
                    OPTIONAL MATCH (book)-[:CONTAINS*0..]->(n)
                    OPTIONAL MATCH (n)-[:BELONGS_TO*0..]->(book)
                    WITH collect(DISTINCT book) + collect(DISTINCT n) as all_nodes
                    UNWIND all_nodes as node
                    WHERE node IS NOT NULL
                    WITH node
                    MATCH (node)-[r]-(other)
                    WITH r LIMIT $batch_size
                    DELETE r
                    RETURN count(r) as deleted
                """, book_ids=collections, batch_size=batch_size)
                
                deleted = result.single()["deleted"]
                deleted_rels += deleted
                print(f"  Deleted {deleted} relationships (total: {deleted_rels})")
                
                if deleted == 0:
                    break
            
            # Delete nodes in batches (more aggressive approach)
            print("Deleting textbook nodes...")
            deleted_nodes = 0
            while True:
                result = session.run("""
                    MATCH (book:Book)
                    WHERE book.book_id IN $book_ids
                    OPTIONAL MATCH (book)-[:CONTAINS*0..]->(n)
                    OPTIONAL MATCH (n)-[:BELONGS_TO*0..]->(book)
                    WITH collect(DISTINCT book) + collect(DISTINCT n) as all_nodes
                    UNWIND all_nodes as node
                    WHERE node IS NOT NULL
                    WITH node LIMIT $batch_size
                    DETACH DELETE node
                    RETURN count(node) as deleted
                """, book_ids=collections, batch_size=batch_size)
                
                deleted = result.single()["deleted"]
                deleted_nodes += deleted
                print(f"  Deleted {deleted} nodes (total: {deleted_nodes})")
                
                if deleted == 0:
                    break
            
            print(f"Successfully deleted textbook: {textbook_name}")
            print(f"Total nodes deleted: {deleted_nodes}")
            print(f"Total relationships deleted: {deleted_rels}")
            return True
            
    except Exception as e:
        print(f"Error deleting textbook: {e}")
        return False
    finally:
        if 'driver' in locals():
            driver.close()


def delete_single_collection(uri: str, username: str, password: str, database: str, collection_name: str) -> bool:
    """Delete a specific collection using comprehensive batched operations."""
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(uri, auth=(username, password))
        
        with driver.session(database=database) as session:
            # Check if collection exists
            result = session.run("""
                MATCH (b:Book) 
                WHERE b.book_id = $collection_name
                RETURN count(b) as count
            """, collection_name=collection_name)
            
            count = result.single()["count"]
            if count == 0:
                print(f"Collection '{collection_name}' not found")
                return True
            
            print(f"Deleting collection: {collection_name}")
            
            # Use a more comprehensive approach - find all nodes connected to this book
            # First, let's find all nodes that belong to this collection through any path
            all_nodes_result = session.run("""
                MATCH (book:Book {book_id: $book_id})
                OPTIONAL MATCH (book)-[:CONTAINS*0..]->(n)
                OPTIONAL MATCH (n)-[:BELONGS_TO*0..]->(book)
                WITH collect(DISTINCT book) + collect(DISTINCT n) as all_nodes
                UNWIND all_nodes as node
                WHERE node IS NOT NULL
                RETURN collect(DISTINCT id(node)) as node_ids
            """, book_id=collection_name)
            
            node_ids = all_nodes_result.single()["node_ids"]
            total_nodes = len(node_ids)
            
            print(f"Found {total_nodes} nodes to delete")
            
            if total_nodes == 0:
                print("No data found to delete")
                return True
            
            # Delete in batches to avoid Neo4j timeout
            batch_size = 500
            
            # Delete relationships first (more aggressive approach)
            print("Deleting all relationships from/to collection nodes...")
            deleted_rels = 0
            while True:
                result = session.run("""
                    MATCH (book:Book {book_id: $book_id})
                    OPTIONAL MATCH (book)-[:CONTAINS*0..]->(n)
                    OPTIONAL MATCH (n)-[:BELONGS_TO*0..]->(book)
                    WITH collect(DISTINCT book) + collect(DISTINCT n) as all_nodes
                    UNWIND all_nodes as node
                    WHERE node IS NOT NULL
                    WITH node
                    MATCH (node)-[r]-(other)
                    WITH r LIMIT $batch_size
                    DELETE r
                    RETURN count(r) as deleted
                """, book_id=collection_name, batch_size=batch_size)
                
                deleted = result.single()["deleted"]
                deleted_rels += deleted
                print(f"  Deleted {deleted} relationships (total: {deleted_rels})")
                
                if deleted == 0:
                    break
            
            # Delete nodes in batches (more aggressive approach)
            print("Deleting collection nodes...")
            deleted_nodes = 0
            while True:
                result = session.run("""
                    MATCH (book:Book {book_id: $book_id})
                    OPTIONAL MATCH (book)-[:CONTAINS*0..]->(n)
                    OPTIONAL MATCH (n)-[:BELONGS_TO*0..]->(book)
                    WITH collect(DISTINCT book) + collect(DISTINCT n) as all_nodes
                    UNWIND all_nodes as node
                    WHERE node IS NOT NULL
                    WITH node LIMIT $batch_size
                    DETACH DELETE node
                    RETURN count(node) as deleted
                """, book_id=collection_name, batch_size=batch_size)
                
                deleted = result.single()["deleted"]
                deleted_nodes += deleted
                print(f"  Deleted {deleted} nodes (total: {deleted_nodes})")
                
                if deleted == 0:
                    break
            
            print(f"Successfully deleted collection: {collection_name}")
            print(f"Total nodes deleted: {deleted_nodes}")
            print(f"Total relationships deleted: {deleted_rels}")
            return True
            
    except Exception as e:
        print(f"Error deleting collection: {e}")
        return False
    finally:
        if 'driver' in locals():
            driver.close()


def cleanup_orphaned_nodes(uri: str, username: str, password: str, database: str) -> bool:
    """Fix orphaned nodes by creating missing relationships and nodes (preserves all content)."""
    try:
        import sys
        from pathlib import Path
        sys.path.append(str(Path(__file__).parent.parent))
        from src.textbook_parse.xml_parser import OpenStaxXMLParser
        
        print("FIXING ORPHANED NODES")
        print("=" * 50)
        print("This will fix orphaned nodes by creating missing relationships and nodes.")
        print("All textbook content will be preserved.")
        print()
        
        # Initialize parser to use its orphaned node fixing functionality
        parser = OpenStaxXMLParser(uri, username, password, database)
        
        # Run the orphaned node fixing process
        fixes = parser.fix_orphaned_nodes(database)
        
        # Display results
        print("\nORPHANED NODE FIXING RESULTS")
        print("=" * 50)
        print(f"Orphaned sentences fixed: {fixes['orphaned_sentences_fixed']}")
        print(f"Orphaned documents fixed: {fixes['orphaned_documents_fixed']}")
        print(f"Orphaned subsections fixed: {fixes['orphaned_subsections_fixed']}")
        print(f"Missing paragraphs created: {fixes['missing_paragraphs_created']}")
        print(f"Remaining orphaned sentences: {fixes['remaining_orphaned_sentences']}")
        print(f"Remaining orphaned documents: {fixes['remaining_orphaned_documents']}")
        print(f"Remaining orphaned subsections: {fixes['remaining_orphaned_subsections']}")
        
        # Close connections
        parser.close_connections()
        
        return True
            
    except Exception as e:
        print(f"Error fixing orphaned nodes: {e}")
        return False


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
@click.option('--textbook-path', help='Path to OpenStax textbook directory')
@click.option('--collection', help='Specific collection to import (optional, defaults to all collections)')
@click.option('--cleanup', is_flag=True, help='Clear existing data before import')
@click.option('--dry-run', is_flag=True, help='Parse files without importing to database')
@click.option('--list-collections', is_flag=True, help='List available collections and exit')
@click.option('--list-textbooks', is_flag=True, help='List available textbooks and collections in database')
@click.option('--no-concepts', is_flag=True, help='Skip concept extraction (concepts are extracted by default)')
@click.option('--workers', type=int, default=4, help='Number of workers for concept extraction (default: 4)')
@click.option('--delete-textbook', help='Delete all collections from a specific textbook (provide textbook name)')
@click.option('--delete-collection', help='Delete a specific collection (provide collection name)')
@click.option('--cleanup-orphans', is_flag=True, help='Clean up orphaned nodes (nodes without relationships)')
def main(textbook_path: str, collection: str, cleanup: bool, dry_run: bool, list_collections: bool, list_textbooks: bool, no_concepts: bool, workers: int, delete_textbook: str, delete_collection: str, cleanup_orphans: bool):
    """Load OpenStax textbook content into Neo4j database with automatic concept extraction.
    
    This script loads textbook content and automatically extracts concepts using Wikidata.
    Collections are processed sequentially (one after another) with multi-threaded concept extraction within each collection.
    
    Examples:
        # Load all collections with concept extraction (default behavior)
        python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle
        
        # Load specific collection
        python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --collection biology-2e
        
        # Clear existing data and reload everything
        python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --cleanup
        
        # Skip concept extraction (faster loading)
        python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --no-concepts
        
        # Use more workers for faster concept extraction
        python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --workers 8
        
        # Delete a specific collection
        python scripts/load_textbooks.py --delete-collection biology-2e
        
        # Delete all collections from a textbook
        python scripts/load_textbooks.py --delete-textbook biology
        
        # List all textbooks and collections in database
        python scripts/load_textbooks.py --list-textbooks
        
        # Clean up orphaned nodes (nodes without relationships)
        python scripts/load_textbooks.py --cleanup-orphans
        
        # Test without making changes
        python scripts/load_textbooks.py --textbook-path textbooks/osbooks-biology-bundle --dry-run
    """
    
    # Load Neo4j connection parameters from config
    from config.config_loader import get_neo4j_connection_params
    uri, username, password, database = get_neo4j_connection_params()
    
    # Handle list operations first
    if list_textbooks:
        print("LISTING TEXTBOOKS AND COLLECTIONS")
        print("=" * 50)
        list_available_textbooks_and_collections(uri, username, password, database)
        return
    
    # Handle orphan cleanup
    if cleanup_orphans:
        success = cleanup_orphaned_nodes(uri, username, password, database)
        if success:
            print(f"\nOrphan cleanup completed successfully")
            print(f"Neo4j Browser: http://20.29.35.132:7474")
        else:
            print(f"\nOrphan cleanup failed")
        return
    
    # Handle delete operations
    if delete_textbook:
        print("TEXTBOOK DELETION")
        print("=" * 50)
        print(f"Deleting textbook: {delete_textbook}")
        print(f"Database: {database}")
        print("=" * 50)
        
        success = delete_textbook_collections(uri, username, password, database, delete_textbook)
        if success:
            print(f"\nSuccessfully deleted textbook: {delete_textbook}")
            print(f"Neo4j Browser: http://20.29.35.132:7474")
        else:
            print(f"\nFailed to delete textbook: {delete_textbook}")
        return
    
    if delete_collection:
        print("COLLECTION DELETION")
        print("=" * 50)
        print(f"Deleting collection: {delete_collection}")
        print(f"Database: {database}")
        print("=" * 50)
        
        success = delete_single_collection(uri, username, password, database, delete_collection)
        if success:
            print(f"\nSuccessfully deleted collection: {delete_collection}")
            print(f"Neo4j Browser: http://20.29.35.132:7474")
        else:
            print(f"\nFailed to delete collection: {delete_collection}")
        return
    
    # Validate that textbook-path is provided for non-delete operations
    if not textbook_path and not list_collections:
        print("ERROR: --textbook-path is required for loading operations")
        print("Use --help to see available options")
        return
    
    # Record start time
    start_time = time.time()
    start_datetime = datetime.now()
    
    print("OPENSTAX TEXTBOOK LOADER")
    print("=" * 50)
    print(f"Started at: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    
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
    
    print("Mode: Bulk Import (optimized for large datasets)")
    
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
    
    # Initialize bulk importer for better performance
    bulk_importer = None
    if not dry_run:
        bulk_importer = create_bulk_importer(uri, username, password, database)
        print("Initialized bulk importer (batch size: 2000)")
    
    try:
        # Set up schema only if needed
        if not dry_run:
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
            
            # Check if schema already exists
            if schema_setup.schema_exists():
                print("Database schema already exists, skipping setup")
            else:
                print("\nSetting up database schema...")
                if not schema_setup.setup_constraints():
                    print("Failed to set up constraints")
                    return
                
                if not schema_setup.setup_indexes():
                    print("Failed to set up indexes")
                    return
                
                print("Schema setup completed")
        
        # Full cleanup if requested (completely clear database)
        if cleanup and not dry_run:
            print("\nCleaning up existing data...")
            if not parser.clear_sample_data(uri, username, password, database):
                print("Failed to clear existing data")
                return
        
        # Parse and load collections
        if collection:
            # Load specific collection
            collection_file = collections_dir / f"{collection}.xml"
            
            # Check if collection already exists
            if check_collection_exists(uri, username, password, database, collection):
                print(f"\nCollection '{collection}' already exists in the database.")
                print("Proceeding with concept extraction for existing collection...")
            else:
                # Load collection if it doesn't exist
                print(f"\nLoading collection: {collection}")
                success = parser.load_collection(collection_file, textbook_dir, dry_run, bulk_importer, 2000)
                if not success:
                    print(f"Failed to load collection: {collection}")
                    return
        else:
            # Load all collections
            collection_files = list(collections_dir.glob("*.xml"))
            print(f"\nFound {len(collection_files)} collections to load")
            
            # Filter out existing collections
            collections_to_load = []
            for collection_file in collection_files:
                collection_name = collection_file.stem
                if check_collection_exists(uri, username, password, database, collection_name):
                    print(f"Skipping existing collection: {collection_name}")
                else:
                    collections_to_load.append(collection_file)
            
            if not collections_to_load:
                print("All collections already exist in the database.")
                print("Proceeding with concept extraction for existing collections...")
            
            print(f"Loading {len(collections_to_load)} new collections (skipped {len(collection_files) - len(collections_to_load)} existing)")
            
            # Load collections with simple progress logging
            if collections_to_load:
                print(f"Loading {len(collections_to_load)} collections...")
                for i, collection_file in enumerate(collections_to_load, 1):
                    collection_name = collection_file.stem
                    print(f"Loading collection {i}/{len(collections_to_load)}: {collection_name}")
                    success = parser.load_collection(collection_file, textbook_dir, dry_run, bulk_importer, 2000)
                    if not success:
                        print(f"\nFailed to load collection: {collection_name}")
                print("Collection loading completed!")
        
        # Extract concepts by default (unless disabled)
        if not no_concepts and not dry_run:
            print(f"\nStarting sequential collection processing for concept extraction...")
            sequential_processor = SequentialCollectionProcessor(
                neo4j_uri=uri,
                neo4j_user=username,
                neo4j_password=password,
                neo4j_database=database,
                cache_file="wikidata_cache.json",
                max_workers=workers
            )
            
            try:
                concept_stats = sequential_processor.process_collections_sequentially(textbook_path)
                
                print("\n=== CONCEPT EXTRACTION COMPLETE ===")
                print(f"Collections processed: {concept_stats['collections_processed']}")
                print(f"Sentences processed: {concept_stats['sentences_processed']}")
                print(f"Concepts created: {concept_stats['concepts_created']}")
                print(f"Entities extracted: {concept_stats['entities_extracted']}")
                print(f"API calls made: {concept_stats.get('api_calls', 0)}")
                print(f"Cache hits: {concept_stats.get('cache_hits', 0)}")
                print(f"Cache hit rate: {concept_stats.get('cache_hit_rate', 0):.1f}%")
                
                if concept_stats['sentences_processed'] > 0:
                    success_rate = concept_stats['concepts_created']/concept_stats['sentences_processed']*100
                    print(f"Success rate: {success_rate:.1f}%")
                
            except Exception as e:
                print(f"Concept extraction failed: {e}")
                logger.exception("Concept extraction error")
            finally:
                sequential_processor.close()

        # Calculate and display execution time
        end_time = time.time()
        end_datetime = datetime.now()
        execution_time = end_time - start_time
        execution_timedelta = timedelta(seconds=execution_time)
        
        print(f"\nLoading completed!")
        print(f"Textbook: {textbook_dir.name}")
        if collection:
            print(f"Collection: {collection}")
        print(f"Database: {database}")
        print(f"Neo4j Browser: http://20.29.35.132:7474")
        
        print(f"\n=== EXECUTION SUMMARY ===")
        print(f"Started: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Finished: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total time: {execution_timedelta}")
        print(f"Duration: {execution_time:.2f} seconds")
        
        if not dry_run:
            print(f"\nNavigate to 20.29.35.132:7474 to see your imported data")
            print(f"\nTo clear existing data before future imports:")
            print(f"   python scripts/load_textbooks.py --textbook-path <path> --cleanup")
        
    except KeyboardInterrupt:
        print("\nLoading cancelled by user")
        # Still show execution time even if cancelled
        end_time = time.time()
        execution_time = end_time - start_time
        execution_timedelta = timedelta(seconds=execution_time)
        print(f"\n=== EXECUTION SUMMARY (CANCELLED) ===")
        print(f"Started: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Cancelled: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Time before cancellation: {execution_timedelta}")
        print(f"Duration: {execution_time:.2f} seconds")
    except Exception as e:
        print(f"\nLoading failed: {e}")
        logger.exception("Loading error")
        # Still show execution time even if failed
        end_time = time.time()
        execution_time = end_time - start_time
        execution_timedelta = timedelta(seconds=execution_time)
        print(f"\n=== EXECUTION SUMMARY (FAILED) ===")
        print(f"Started: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Failed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Time before failure: {execution_timedelta}")
        print(f"Duration: {execution_time:.2f} seconds")
    finally:
        parser.close_connections()
        if bulk_importer:
            bulk_importer.close()


if __name__ == "__main__":
    main()
