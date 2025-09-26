#!/usr/bin/env python3
"""
Standalone Concept Extraction Script for Neo4j

This script extracts concepts from existing sentences in a Neo4j database,
looking them up in Wikidata and creating concept nodes with relationships.

Usage:
    # Extract concepts from all sentences
    python scripts/extract_concepts.py
    
    # Extract with specific batch size and limit
    python scripts/extract_concepts.py --batch-size 25 --max-sentences 100
    
    # Test run (dry run equivalent)
    python scripts/extract_concepts.py --max-sentences 5
"""

import os
import sys
from pathlib import Path
import click
import logging
from tqdm import tqdm

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from textbook_parse.concept_extraction.main import ConceptExtractionSystem

# Configure logging - suppress verbose output
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress verbose logging from external libraries
logging.getLogger('textbook_parse.concept_extraction').setLevel(logging.WARNING)
logging.getLogger('neo4j').setLevel(logging.WARNING)

@click.command()
@click.option('--uri', default='bolt://localhost:7687', help='Neo4j URI')
@click.option('--username', default='neo4j', help='Neo4j username')
@click.option('--password', default='', help='Neo4j password (optional for no-auth)')
@click.option('--database', default='neo4j', help='Neo4j database name')
@click.option('--max-sentences', type=int, help='Maximum sentences to process (for testing)')
def main(uri: str, username: str, password: str, database: str, max_sentences: int):
    """Extract concepts from sentences in Neo4j database using Wikidata.
    
    This script processes sentences that don't have concept relationships,
    extracts entities using NLP, looks them up in Wikidata, and creates
    concept nodes with bidirectional relationships.
    
    Examples:
        # Extract concepts from all sentences
        python scripts/extract_concepts.py
        
        # Test with limited sentences
        python scripts/extract_concepts.py --max-sentences 10
    """
    
    print("NEO4J CONCEPT EXTRACTION")
    print("=" * 40)
    print(f"Database: {database}")
    if max_sentences:
        print(f"Max sentences: {max_sentences}")
    print("=" * 40)
    
    # Create and run the system
    system = ConceptExtractionSystem(
        neo4j_uri=uri,
        neo4j_user=username,
        neo4j_password=password,
        neo4j_database=database,
        cache_file="wikidata_cache.json"
    )
    
    try:
        # Get initial stats
        initial_stats = system.get_system_stats()
        print(f"\nBefore extraction:")
        print(f"  Total concepts in database: {initial_stats['total_concepts_in_db']}")
        print(f"  Sentences with concepts: {initial_stats['sentences_with_concepts']}")
        
        # Run extraction
        print(f"\nStarting concept extraction...")
        
        # Get total sentences to process for progress bar
        total_sentences = system.concept_manager.get_sentences_without_concepts(limit=10000)
        total_count = len(total_sentences)
        
        if max_sentences:
            total_count = min(total_count, max_sentences)
        
        # Create progress bar
        with tqdm(total=total_count, desc="Processing sentences", unit="sentence", 
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
            stats = system.process_sentences(
                batch_size=50,
                max_sentences=max_sentences,
                progress_callback=lambda processed: pbar.update(1)
            )
        
        print("\n=== EXTRACTION COMPLETE ===")
        print(f"Sentences processed: {stats['processed_sentences']}")
        print(f"Concepts created: {stats['concepts_created']}")
        print(f"Entities extracted: {stats['entities_extracted']}")
        print(f"Wikidata lookups: {stats['wikidata_lookups']}")
        print(f"API calls made: {stats.get('api_calls', 0)}")
        print(f"Cache hits: {stats.get('cache_hits', 0)}")
        
        if stats['processed_sentences'] > 0:
            success_rate = stats['concepts_created']/stats['processed_sentences']*100
            print(f"Success rate: {success_rate:.1f}%")
        
        # Get final system stats
        final_stats = system.get_system_stats()
        print(f"\nAfter extraction:")
        print(f"  Total concepts in database: {final_stats['total_concepts_in_db']}")
        print(f"  Sentences with concepts: {final_stats['sentences_with_concepts']}")
        
        print(f"\nConcept extraction completed successfully!")
        print(f"Neo4j Browser: http://localhost:7474")
        
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
    except Exception as e:
        print(f"\nError during processing: {e}")
        logger.exception("Processing error")
    finally:
        system.close()

if __name__ == "__main__":
    main()
