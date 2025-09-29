#!/usr/bin/env python3
"""
Script to complete missing concept extractions for sentences with 'needs_api_lookup' status.

This script processes existing sentence files and completes the API lookups for entities
that have 'needs_api_lookup' status.
"""

import json
import sys
from pathlib import Path
from src.textbook_parse.concept_extraction.sequential_processor import SequentialCollectionProcessor
from src.config.config_loader import load_neo4j_config

def complete_missing_concepts(sentences_file: str):
    """Complete missing concept extractions for a sentences file."""
    
    # Load configuration
    neo4j_config = load_neo4j_config()
    
    # Initialize processor
    processor = SequentialCollectionProcessor(
        neo4j_uri=neo4j_config['uri'],
        neo4j_user=neo4j_config['username'],
        neo4j_password=neo4j_config['password'],
        neo4j_database=neo4j_config['database'],
        cache_file='wikidata_cache.json',
        max_workers=4
    )
    
    try:
        # Load sentences data
        sentences_path = Path(sentences_file)
        if not sentences_path.exists():
            print(f"Error: File {sentences_file} not found")
            return False
            
        print(f"Loading sentences from {sentences_file}...")
        with open(sentences_path, 'r', encoding='utf-8') as f:
            sentences_data = json.load(f)
        
        # Count entities that need API lookup
        needs_lookup_count = 0
        for sentence_id, sentence_data in sentences_data.items():
            if sentence_data.get('status') == 'processed':
                continue
            for entity_name, entity_data in sentence_data.get('entities', {}).items():
                if entity_data.get('status') == 'needs_api_lookup':
                    needs_lookup_count += 1
        
        if needs_lookup_count == 0:
            print("No entities need API lookup. All concepts are already complete.")
            return True
            
        print(f"Found {needs_lookup_count} entities that need API lookup")
        
        # Process the uncached entities
        collection_name = sentences_path.stem.replace('_sentences', '')
        stats = processor._process_uncached_entities(sentences_data, collection_name, needs_lookup_count)
        
        # Save the updated sentences data
        processor._save_sentences_file(sentences_path, sentences_data)
        
        print(f"Completed concept extraction:")
        print(f"  - Concepts created: {stats['concepts_created']}")
        print(f"  - API calls made: {stats['api_calls']}")
        print(f"  - Cache hits: {stats['cache_hits']}")
        
        return True
        
    except Exception as e:
        print(f"Error completing concepts: {e}")
        return False
    finally:
        processor.close()

def main():
    """Main function to handle command line arguments."""
    if len(sys.argv) != 2:
        print("Usage: python complete_concepts.py <sentences_file>")
        print("Example: python complete_concepts.py concepts-biology_sentences.json")
        sys.exit(1)
    
    sentences_file = sys.argv[1]
    
    print(f"Completing missing concepts for {sentences_file}...")
    success = complete_missing_concepts(sentences_file)
    
    if success:
        print("Concept completion finished successfully!")
    else:
        print("Concept completion failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
