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
from neo4j_utils.relationships import Neo4jRelationshipCreator
from typing import List, Dict
from datetime import datetime
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

def check_for_json_resume_files(textbook_dir: Path) -> List[Path]:
    """Check for existing JSON files that can be used for resume processing.
    
    This function looks for JSON files based on the actual collection names in the textbook.
    It searches for files with the pattern [collection_name]_sentences.json.
    
    Args:
        textbook_dir: Path to the textbook directory
        
    Returns:
        List of JSON file paths that exist and can be used for resume
    """
    json_files = []
    
    # Get collection names from the textbook directory
    collections_dir = textbook_dir / "collections"
    if not collections_dir.exists():
        print(f"No collections directory found in {textbook_dir}")
        return json_files
    
    # Find all collection XML files
    collection_files = list(collections_dir.glob("*.xml"))
    if not collection_files:
        print(f"No collection XML files found in {collections_dir}")
        return json_files
    
    print(f"Checking for JSON resume files for {len(collection_files)} collections...")
    
    # Look for corresponding JSON files based on collection names
    for collection_file in collection_files:
        collection_name = collection_file.stem
        
        # Remove .collection suffix if present to match the expected JSON file naming
        if collection_name.endswith('.collection'):
            collection_name = collection_name[:-10]  # Remove '.collection'
        
        # Also remove any trailing periods that might be present
        collection_name = collection_name.rstrip('.')
        
        # Look for [collection_name]_sentences.json file in the project root
        # (JSON files are typically in the same directory as the script)
        project_root = textbook_dir.parent.parent  # Go up from textbooks/ to project root
        json_file = project_root / f"{collection_name}_sentences.json"
        
        if json_file.exists():
            json_files.append(json_file)
            print(f"Found resume file: {json_file.name} (for collection: {collection_name})")
        else:
            print(f"No resume file found: {json_file.name} (for collection: {collection_name})")
    
    return json_files

def ensure_sentence_paragraph_relationships(uri: str, username: str, password: str, database: str, sentence_ids: List[str]) -> Dict[str, int]:
    """Ensure sentences are properly connected to paragraphs using the same method as XML parser.
    
    Args:
        uri: Neo4j URI
        username: Neo4j username  
        password: Neo4j password
        database: Neo4j database name
        sentence_ids: List of sentence IDs to check/connect
        
    Returns:
        Dictionary with relationship creation statistics
    """
    driver = GraphDatabase.driver(uri, auth=(username, password))
    rel_creator = Neo4jRelationshipCreator(uri, username, password, database)
    
    stats = {
        'sentences_checked': 0,
        'relationships_created': 0,
        'missing_paragraphs_created': 0
    }
    
    try:
        with driver.session(database=database) as session:
            # Check which sentences need paragraph relationships
            check_query = """
            MATCH (s:Sentence)
            WHERE s.sentence_id IN $sentence_ids
            AND NOT (s)<-[:PARAGRAPH_CONTAINS_SENTENCE]-()
            RETURN s.sentence_id as sentence_id, s.paragraph_id as paragraph_id
            """
            
            result = session.run(check_query, sentence_ids=sentence_ids)
            disconnected_sentences = [(record['sentence_id'], record['paragraph_id']) for record in result]
            
            stats['sentences_checked'] = len(sentence_ids)
            
            if not disconnected_sentences:
                print("All sentences already have proper paragraph relationships")
                return stats
            
            print(f"Found {len(disconnected_sentences)} sentences without paragraph relationships")
            
            # Group by paragraph_id to create missing paragraphs
            paragraph_groups = {}
            for sentence_id, paragraph_id in disconnected_sentences:
                if paragraph_id not in paragraph_groups:
                    paragraph_groups[paragraph_id] = []
                paragraph_groups[paragraph_id].append(sentence_id)
            
            # Create missing paragraphs and connect sentences
            for paragraph_id, sentences in paragraph_groups.items():
                # Check if paragraph exists
                paragraph_check = session.run(
                    "MATCH (p:Paragraph {paragraph_id: $paragraph_id}) RETURN p",
                    paragraph_id=paragraph_id
                )
                
                if not paragraph_check.single():
                    # Create missing paragraph node
                    paragraph_data = {
                        'paragraph_id': paragraph_id,
                        'text': '',  # Will be updated later if needed
                        'uuid': '',
                        'order': 0,
                        'lens': 'content',
                        'created_at': datetime.now().isoformat()
                    }
                    
                    session.run("""
                        CREATE (p:Paragraph {
                            paragraph_id: $paragraph_id,
                            text: $text,
                            uuid: $uuid,
                            order: $order,
                            lens: $lens,
                            created_at: $created_at
                        })
                    """, paragraph_data)
                    
                    stats['missing_paragraphs_created'] += 1
                    print(f"Created missing paragraph: {paragraph_id}")
                
                # Connect sentences to paragraph using the same method as XML parser
                for sentence_id in sentences:
                    # Use the same relationship creation methods as XML parser
                    if rel_creator.create_paragraph_contains_sentence_relationship(paragraph_id, sentence_id):
                        rel_creator.create_sentence_belongs_paragraph_relationship(sentence_id, paragraph_id)
                        stats['relationships_created'] += 2  # Bidirectional
                
                print(f"Connected {len(sentences)} sentences to paragraph {paragraph_id}")
            
            print(f"Relationship creation completed: {stats['relationships_created']} relationships created")
            
    except Exception as e:
        print(f"Error ensuring sentence-paragraph relationships: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.close()
    
    return stats

def resume_from_json_files(sequential_processor, json_files: List[Path], force: bool = False) -> Dict[str, int]:
    """Resume processing from existing JSON files, ensuring proper relationships.
    
    Args:
        sequential_processor: SequentialCollectionProcessor instance
        json_files: List of JSON file paths to process
        
    Returns:
        Dictionary with processing statistics
    """
    total_stats = {
        'sentences_processed': 0,
        'relationships_created': 0,
        'concepts_imported': 0,
        'files_processed': 0
    }
    
    for json_file in json_files:
        print(f"\nProcessing resume file: {json_file.name}")
        
        try:
            # First, ensure sentences have proper paragraph relationships
            import json
            with open(json_file, 'r', encoding='utf-8') as f:
                sentences_data = json.load(f)
            
            sentence_ids = list(sentences_data.keys())
            
            # Ensure proper sentence-to-paragraph relationships
            print(f"Ensuring proper relationships for {len(sentence_ids)} sentences...")
            relationship_stats = ensure_sentence_paragraph_relationships(
                sequential_processor.neo4j_uri,
                sequential_processor.neo4j_user, 
                sequential_processor.neo4j_password,
                sequential_processor.neo4j_database,
                sentence_ids
            )
            
            total_stats['relationships_created'] += relationship_stats['relationships_created']
            
            # Now import concepts using the existing method or force re-import
            if force:
                print(f"Force re-importing concepts from {json_file.name}...")
                concept_stats = force_import_concepts_from_sentence_file(sequential_processor, json_file)
            else:
                print(f"Importing concepts from {json_file.name}...")
                concept_stats = sequential_processor.import_concepts_from_sentence_file(json_file)
            
            total_stats['sentences_processed'] += concept_stats['sentences_processed']
            total_stats['concepts_imported'] += concept_stats['concepts_imported']
            total_stats['files_processed'] += 1
            
            print(f"File {json_file.name} completed:")
            print(f"  Sentences: {concept_stats['sentences_processed']}")
            print(f"  Concepts: {concept_stats['concepts_imported']}")
            print(f"  Relationships: {concept_stats['relationships_created']}")
            
        except Exception as e:
            print(f"Error processing {json_file.name}: {e}")
            import traceback
            traceback.print_exc()
    
    return total_stats

def force_import_concepts_from_sentence_file(sequential_processor, sentences_file: Path) -> Dict[str, int]:
    """Force import concepts from a sentence file, bypassing existing checks and recreating all nodes.
    
    This function forces re-import of concepts even if they already exist, ensuring all expected
    fields are populated with standard property keys.
    
    Args:
        sequential_processor: SequentialCollectionProcessor instance
        sentences_file: Path to the sentence file containing extracted concepts
        
    Returns:
        Dictionary with import statistics
    """
    stats = {
        'sentences_processed': 0,
        'concepts_imported': 0,
        'relationships_created': 0
    }
    
    try:
        # Load sentences data
        import json
        with open(sentences_file, 'r', encoding='utf-8') as f:
            sentences_data = json.load(f)
        
        print(f"Force importing concepts from {sentences_file}")
        
        with sequential_processor.driver.session(database=sequential_processor.neo4j_database) as session:
            for sentence_id, sentence_data in sentences_data.items():
                entities = sentence_data.get('entities', {})
                
                if entities:
                    # Process each entity with a Wikidata ID
                    for entity_name, entity_data in entities.items():
                        if entity_data.get('wikidata_id'):
                            wikidata_id = entity_data['wikidata_id']
                            
                            # Force create/update concept node with all expected fields
                            force_concept_query = """
                            // First, verify the sentence exists
                            MATCH (s:Sentence {sentence_id: $sentence_id})
                            
                            // Only proceed if sentence exists
                            WITH s
                            
                            // Force create/update the concept node with all expected fields
                            MERGE (c:Concept {wikidata_id: $wikidata_id})
                            ON CREATE SET
                                c.name = $name,
                                c.wikidata_name = $name,
                                c.label = $name,
                                c.description = $description,
                                c.aliases = $aliases,
                                c.wikidata_url = $wikidata_url,
                                c.created_at = datetime(),
                                c.updated_at = datetime()
                            ON MATCH SET
                                c.name = $name,
                                c.wikidata_name = $name,
                                c.label = $name,
                                c.description = $description,
                                c.aliases = $aliases,
                                c.wikidata_url = $wikidata_url,
                                c.updated_at = datetime()
                            
                            // Force recreate bidirectional relationships
                            MERGE (s)-[r1:SENTENCE_CONTAINS_CONCEPT]->(c)
                            ON CREATE SET r1.created_at = datetime()
                            ON MATCH SET r1.updated_at = datetime()
                            
                            MERGE (c)-[r2:CONCEPT_BELONGS_TO_SENTENCE]->(s)
                            ON CREATE SET r2.created_at = datetime()
                            ON MATCH SET r2.updated_at = datetime()
                            
                            RETURN c.wikidata_id as concept_id
                            """
                            
                            # Apply standard property keys for missing fields
                            description = entity_data.get('description', entity_name)
                            aliases = entity_data.get('aliases', [entity_name])
                            wikidata_url = f"https://www.wikidata.org/wiki/{wikidata_id}"
                            
                            result = session.run(force_concept_query, {
                                'sentence_id': sentence_id,
                                'name': entity_name,
                                'wikidata_id': wikidata_id,
                                'description': description,
                                'aliases': aliases,
                                'wikidata_url': wikidata_url
                            })
                            
                            record = result.single()
                            if record:
                                stats['concepts_imported'] += 1
                                stats['relationships_created'] += 2  # Bidirectional
                
                stats['sentences_processed'] += 1
                
                # Progress update
                if stats['sentences_processed'] % 1000 == 0:
                    print(f"  Force processed {stats['sentences_processed']} sentences, imported {stats['concepts_imported']} concepts")
        
        print(f"Force concept import completed: {stats['concepts_imported']} concepts force imported for {stats['sentences_processed']} sentences")
        return stats
        
    except Exception as e:
        print(f"Error force importing concepts from {sentences_file}: {e}")
        import traceback
        traceback.print_exc()
        raise

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
        from src.textbook_parse.concept_extraction.concept_manager import ConceptManager
        from neo4j import GraphDatabase
        
        print("FIXING ORPHANED NODES")
        print("=" * 50)
        print("This will fix orphaned nodes by creating missing relationships and nodes.")
        print("All textbook content will be preserved.")
        print()
        
        # Initialize parser to use its orphaned node fixing functionality
        parser = OpenStaxXMLParser(uri, username, password, database)
        
        # Run the orphaned node fixing process
        fixes = parser.fix_orphaned_nodes(database)
        
        # Clean up orphaned concept nodes
        print("\nCleaning up orphaned concept nodes...")
        driver = GraphDatabase.driver(uri, auth=(username, password))
        concept_manager = ConceptManager(driver)
        orphaned_concepts_removed = concept_manager.cleanup_orphaned_concepts()
        driver.close()
        
        # Display results
        print("\nORPHANED NODE FIXING RESULTS")
        print("=" * 50)
        print(f"Orphaned sentences fixed: {fixes['orphaned_sentences_fixed']}")
        print(f"Orphaned documents fixed: {fixes['orphaned_documents_fixed']}")
        print(f"Orphaned subsections fixed: {fixes['orphaned_subsections_fixed']}")
        print(f"Missing paragraphs created: {fixes['missing_paragraphs_created']}")
        print(f"Orphaned concepts removed: {orphaned_concepts_removed}")
        print(f"Remaining orphaned sentences: {fixes['remaining_orphaned_sentences']}")
        print(f"Remaining orphaned documents: {fixes['remaining_orphaned_documents']}")
        print(f"Remaining orphaned subsections: {fixes['remaining_orphaned_subsections']}")
        
        # Close connections
        parser.close_connections()
        
        return True
            
    except Exception as e:
        print(f"Error fixing orphaned nodes: {e}")
        return False


def get_all_available_textbooks() -> List[Path]:
    """Get all available textbook directories."""
    textbooks_dir = Path("textbooks")
    available_textbooks = []
    
    if textbooks_dir.exists():
        for textbook_dir in textbooks_dir.iterdir():
            if (textbook_dir.is_dir() and 
                textbook_dir.name.startswith('osbooks-') and 
                (textbook_dir / "collections").exists()):
                available_textbooks.append(textbook_dir)
    
    return sorted(available_textbooks)


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
@click.option('--textbook-path', help='Path to OpenStax textbook directory (or "all" to load all available textbooks)')
@click.option('--collection', help='Specific collection to import (optional, defaults to all collections)')
@click.option('--cleanup', is_flag=True, help='Clear existing data before import')
@click.option('--dry-run', is_flag=True, help='Parse files without importing to database')
@click.option('--list-collections', is_flag=True, help='List available collections and exit')
@click.option('--list-textbooks', is_flag=True, help='List available textbooks and collections in database')
@click.option('--no-concepts', is_flag=True, help='Skip concept extraction (concepts are extracted by default)')
@click.option('--workers', type=int, default=4, help='Number of workers for concept extraction (default: 4)')
@click.option('--force', is_flag=True, help='Force re-import of concepts from JSON files, bypassing existing checks')
@click.option('--delete-textbook', help='Delete all collections from a specific textbook (provide textbook name)')
@click.option('--delete-collection', help='Delete a specific collection (provide collection name)')
@click.option('--cleanup-orphans', is_flag=True, help='Clean up orphaned nodes (nodes without relationships)')
def main(textbook_path: str, collection: str, cleanup: bool, dry_run: bool, list_collections: bool, list_textbooks: bool, no_concepts: bool, workers: int, force: bool, delete_textbook: str, delete_collection: str, cleanup_orphans: bool):
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
        print("Use 'all' as textbook-path to load all available textbooks")
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
            
            # Check for resume capability - look for existing JSON files
            resume_from_json = check_for_json_resume_files(textbook_dir)
            if resume_from_json:
                if force:
                    print(f"\nFound existing JSON files for FORCED re-import processing...")
                    print(f"Force flag enabled - bypassing existing checks and re-importing all concepts...")
                else:
                    print(f"\nFound existing JSON files for resume processing...")
                    print(f"Processing JSON files to ensure proper sentence-to-paragraph relationships...")
                
                json_stats = resume_from_json_files(sequential_processor, resume_from_json, force=force)
                print(f"JSON processing completed: {json_stats}")
            
            try:
                # Pass force flag to sequential processor if needed
                if force:
                    concept_stats = sequential_processor.process_collections_sequentially(textbook_path, force=force)
                else:
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
