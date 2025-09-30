"""Sequential collection processing for concept extraction workflow.

This module implements the optimized concept extraction workflow that processes
each textbook collection sequentially and completely before moving to the next.

Entity Status Flow:
==================
The system uses a clear status progression to track entity processing:

1. "not_processed" → Initial state after entity extraction
2. "cache_hit" → Found in cache with data, has wikidata_id
3. "null_no_api_value" → Found in cache but null (already failed lookup)
4. "needs_api_lookup" → Not in cache, needs API call
5. "api_lookup_complete" → API call successful, has wikidata_id
6. "api_lookup_failed" → API call failed, no wikidata_id

Benefits of "null_no_api_value" Status:
=======================================
- Clear Status: "null_no_api_value" clearly indicates the entity was already looked up and failed
- No Wasted API Calls: Entities with null cache entries won't trigger unnecessary API calls
- Better Debugging: Can easily identify which entities have been tried before
- Cleaner Logic: No need to distinguish between "not in cache" and "null cache entry"

Example JSON Structure:
======================
{
  "sentence_id": {
    "text": "The mitochondria is the powerhouse of the cell.",
    "entities": {
      "mitochondria": {
        "status": "cache_hit",
        "wikidata_id": "Q39517"
      },
      "nonexistent_entity": {
        "status": "null_no_api_value"
      },
      "new_entity": {
        "status": "needs_api_lookup"
      }
    },
    "status": "processed"
  }
}
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from .entity_extractor import EntityExtractor
from .cache_manager import CacheManager
from .wikidata_client import WikidataClient
from .concept_manager import ConceptManager
from .main import ConceptExtractionSystem

logger = logging.getLogger(__name__)

class SequentialCollectionProcessor:
    """Processes textbook collections sequentially with optimized concept extraction."""
    
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str, 
                 neo4j_database: str = "neo4j", cache_file: str = "wikidata_cache.json", max_workers: int = 4):
        """Initialize the sequential processor.
        
        Args:
            neo4j_uri: Neo4j database URI
            neo4j_user: Neo4j username
            neo4j_password: Neo4j password
            neo4j_database: Neo4j database name
            cache_file: Path to Wikidata cache file
            max_workers: Maximum number of workers for concept extraction
        """
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.neo4j_database = neo4j_database
        self.max_workers = max_workers
        self.cache_file = cache_file
        
        # Initialize components
        self.cache_manager = CacheManager(cache_file)
        self.entity_extractor = EntityExtractor()
        self.wikidata_client = WikidataClient(self.cache_manager)
        
        # Create Neo4j driver for concept manager
        from neo4j import GraphDatabase
        self.driver = GraphDatabase.driver(
            neo4j_uri, 
            auth=(neo4j_user, neo4j_password)
        )
        self.concept_manager = ConceptManager(self.driver)
        
        # Statistics tracking
        self.stats = {
            'collections_processed': 0,
            'sentences_processed': 0,
            'entities_extracted': 0,
            'concepts_created': 0,
            'cache_hits': 0,
            'api_calls': 0
        }
        
        logger.info("SequentialCollectionProcessor initialized")
    
    def process_collections_sequentially(self, textbook_path: str, force: bool = False) -> Dict[str, Any]:
        """Process all collections in a textbook sequentially and completely.
        
        Each collection is processed completely (including concept extraction) before moving to the next.
        
        Args:
            textbook_path: Path to the textbook directory
            
        Returns:
            Dictionary with processing statistics
        """
        textbook_dir = Path(textbook_path)
        collections_dir = textbook_dir / "collections"
        
        if not collections_dir.exists():
            raise ValueError(f"Collections directory not found: {collections_dir}")
        
        # Get all collection files
        collection_files = list(collections_dir.glob("*.xml"))
        logger.info(f"Found {len(collection_files)} collections to process sequentially")
        
        # Process each collection completely before moving to next
        for i, collection_file in enumerate(collection_files, 1):
            collection_name = collection_file.stem
            # Remove .collection suffix if present to match database format
            db_collection_name = collection_name.replace('.collection', '')
            logger.info(f"Processing collection {i}/{len(collection_files)}: {collection_name} (DB: {db_collection_name})")
            
            try:
                collection_stats = self._process_single_collection_with_concepts(db_collection_name, force=force)
                self._update_global_stats(collection_stats)
                self.stats['collections_processed'] += 1
                
                logger.info(f"✅ Collection {collection_name} completed successfully")
                logger.info(f"   Sentences: {collection_stats['sentences_processed']}")
                logger.info(f"   Concepts: {collection_stats['concepts_created']}")
                
            except Exception as e:
                logger.error(f"❌ Failed to process collection {collection_name}: {e}")
                raise
        
        logger.info(f"🎉 All {len(collection_files)} collections processed successfully!")
        return self.stats
    
    def _process_single_collection_with_concepts(self, collection_name: str, force: bool = False) -> Dict[str, Any]:
        """Process a single collection with JSON file tracking for resume capability.
        
        Args:
            collection_name: Name of the collection to process
            
        Returns:
            Dictionary with collection processing statistics
        """
        logger.info(f"Starting concept extraction for collection: {collection_name}")
        
        # Create sentences file path
        sentences_file = Path(f"{collection_name}_sentences.json")
        
        # Always load sentences directly from Neo4j to ensure all sentences are processed
        logger.info(f"Loading sentences directly from Neo4j for collection: {collection_name}")
        sentences_data = self._extract_collection_sentences(collection_name)
        
        # Check if concepts are already imported into database (skip if force flag is set)
        if not force and self._check_concepts_imported(collection_name):
            logger.info(f"Concepts already imported for collection: {collection_name}")
            return {
                'sentences_processed': len(sentences_data),
                'entities_extracted': sum(len(sentence.get('entities', {})) for sentence in sentences_data.values()),
                'concepts_created': 0,
                'cache_hits': 0,
                'api_calls': 0,
                'status': 'already_imported'
            }
        
        # Save the sentences data to JSON file for tracking
        self._save_sentences_file(sentences_file, sentences_data)
        
        if not sentences_data:
            logger.info(f"No sentences found for collection: {collection_name}")
            return {
                'sentences_processed': 0,
                'entities_extracted': 0,
                'concepts_created': 0,
                'cache_hits': 0,
                'api_calls': 0
            }
        
        # Process sentences through the pipeline
        collection_stats = self._process_sentences_pipeline(sentences_data, collection_name, sentences_file)
        
        logger.info(f"Collection {collection_name} concept extraction completed")
        return collection_stats
    
    def _get_collection_sentences(self, collection_name: str) -> List[Dict]:
        """Get sentences for a specific collection that don't have concepts yet.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            List of sentence dictionaries
        """
        # Simple query to get sentences for this collection that don't have concepts
        # All IDs now use dashes consistently
        query = """
        MATCH (s:Sentence)
        WHERE s.text IS NOT NULL
        AND s.sentence_id CONTAINS $collection_name
        AND NOT (s)-[:SENTENCE_CONTAINS_CONCEPT]->(:Concept)
        RETURN s.sentence_id as sentence_id, s.text as content
        """
        
        sentences = []
        
        # Create a temporary driver connection
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(
            self.neo4j_uri, 
            auth=(self.neo4j_user, self.neo4j_password)
        )
        
        try:
            with driver.session(database=self.neo4j_database) as session:
                result = session.run(query, collection_name=collection_name)
                
                for record in result:
                    sentences.append({
                        'sentence_id': record['sentence_id'],
                        'content': record['content']
                    })
        finally:
            driver.close()
        
        logger.info(f"Found {len(sentences)} sentences without concepts for collection {collection_name}")
        return sentences
    
    def _extract_collection_sentences(self, collection_name: str) -> Dict[str, Any]:
        """Extract all sentences for a collection and create optimized JSON structure.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            Dictionary with sentence data in optimized format
        """
        # Simple query to get ALL sentences for this collection
        # All IDs now use dashes consistently
        query = """
        MATCH (s:Sentence)
        WHERE s.text IS NOT NULL
        AND s.sentence_id CONTAINS $collection_name
        RETURN s.sentence_id as sentence_id, s.text as text
        """
        
        sentences_data = {}
        
        # Create a temporary driver connection
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(
            self.neo4j_uri, 
            auth=(self.neo4j_user, self.neo4j_password)
        )
        
        try:
            with driver.session(database=self.neo4j_database) as session:
                result = session.run(query, collection_name=collection_name)
                
                for record in result:
                    sentence_id = record['sentence_id']
                    text = record['text']
                    
                    # Use optimized structure
                    sentences_data[sentence_id] = {
                        'text': text,
                        'entities': {},  # Dictionary for O(1) lookup
                        'status': 'not_processed',
                        'original_sentence_id': sentence_id
                    }
        finally:
            driver.close()
        
        logger.info(f"Extracted {len(sentences_data)} sentences for collection {collection_name}")
        return sentences_data
    
    def _process_sentences_pipeline(self, sentences_data: Dict[str, Any], collection_name: str, sentences_file: Path) -> Dict[str, Any]:
        """Process sentences through the complete pipeline with JSON tracking.
        
        Args:
            sentences_data: Dictionary with sentence data
            collection_name: Name of the collection
            sentences_file: Path to the sentences JSON file
            
        Returns:
            Dictionary with processing statistics
        """
        collection_stats = {
            'sentences_processed': 0,
            'entities_extracted': 0,
            'concepts_created': 0,
            'cache_hits': 0,
            'api_calls': 0
        }
        
        # Step 1: Extract entities for sentences that need it
        logger.info(f"Step 1: Extracting entities for {collection_name}")
        sentences_data = self._extract_entities_for_sentences(sentences_data)
        collection_stats['entities_extracted'] = sum(
            len(sentence['entities']) for sentence in sentences_data.values()
        )
        self._save_sentences_file(sentences_file, sentences_data)
        
        # Step 2: Process cached entities
        cached_count = self._count_cached_entities(sentences_data)
        logger.info(f"Step 2: Processing {cached_count} cached entities for {collection_name}")
        cached_stats = self._process_cached_entities(sentences_data, collection_name, cached_count)
        collection_stats.update(cached_stats)
        self._save_sentences_file(sentences_file, sentences_data)
        
        # Step 3: Process uncached entities
        uncached_count = self._count_uncached_entities(sentences_data)
        logger.info(f"Step 3: Processing {uncached_count} uncached entities for {collection_name}")
        api_stats = self._process_uncached_entities(sentences_data, collection_name, uncached_count)
        for key, value in api_stats.items():
            collection_stats[key] += value
        self._save_sentences_file(sentences_file, sentences_data)
        
        # Mark all sentences as completed
        for sentence in sentences_data.values():
            sentence['status'] = 'processed'
        self._save_sentences_file(sentences_file, sentences_data)
        
        collection_stats['sentences_processed'] = len(sentences_data)
        return collection_stats
    
    def _extract_entities_for_sentences(self, sentences_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract entities for sentences that need it.
        
        Args:
            sentences_data: Dictionary with sentence data
            
        Returns:
            Updated sentences data with extracted entities
        """
        for sentence_id, sentence_data in sentences_data.items():
            # Skip if already processed
            if sentence_data['status'] != 'not_processed':
                continue
                
            text = sentence_data['text']
            
            # Extract entities using spaCy
            entities = self.entity_extractor.extract_entities(text)
            
            # Convert to optimized dictionary format
            entities_dict = {}
            for entity in entities:
                entities_dict[entity] = {
                    'status': 'not_processed'
                }
            
            sentence_data['entities'] = entities_dict
            sentence_data['status'] = 'entities_extracted'
        
        return sentences_data
    
    def _process_cached_entities(self, sentences_data: Dict[str, Any], collection_name: str, total_count: int) -> Dict[str, int]:
        """Process entities that are found in cache and write to Neo4j immediately.
        
        This method only processes entities with 'not_processed' status that don't already have wikidata_id.
        It handles three scenarios:
        1. Entity found in cache with data -> status: 'cache_hit'
        2. Entity found in cache with null value -> status: 'null_no_api_value' (already failed lookup)
        3. Entity not in cache -> status: 'needs_api_lookup'
        
        Args:
            sentences_data: Dictionary with sentence data
            collection_name: Name of the collection
            
        Returns:
            Dictionary with processing statistics
        """
        stats = {
            'concepts_created': 0,
            'cache_hits': 0,
            'api_calls': 0
        }
        
        processed_count = 0
        
        for sentence_id, sentence_data in sentences_data.items():
            # Skip if already processed
            if sentence_data['status'] == 'processed':
                continue
                
            entities = sentence_data['entities']
            
            for entity_name, entity_data in entities.items():
                # Skip entities that already have wikidata_id (already processed)
                if entity_data.get('wikidata_id'):
                    continue
                    
                # Only process entities that are in 'not_processed' status
                if entity_data['status'] != 'not_processed':
                    continue
                    
                # Check cache for both data and null entries
                cached_data = self.cache_manager.get_cached_concept(entity_name)
                processed_count += 1
                
                # Log progress every 100 entities
                if processed_count % 100 == 0:
                    percentage = (processed_count / total_count * 100) if total_count > 0 else 0
                    logger.info(f"  Processed {processed_count}/{total_count} entities ({percentage:.1f}%)")
                
                if cached_data is not None:
                    # Entity found in cache
                    if cached_data:
                        # Found in cache with data
                        entity_data['status'] = 'cache_hit'
                        entity_data['wikidata_id'] = cached_data['qid']
                        stats['cache_hits'] += 1
                        
                        # Create WikidataEntity object for Neo4j
                        cached_entity = self.wikidata_client._create_entity_from_cache(cached_data)
                        
                        # Write to Neo4j immediately
                        if self.concept_manager.create_concept_with_relationship(sentence_id, cached_entity):
                            stats['concepts_created'] += 1
                    else:
                        # Found in cache but null (already failed lookup)
                        entity_data['status'] = 'null_no_api_value'
                        logger.debug(f"Entity '{entity_name}' found in cache with null value (already failed lookup)")
                else:
                    # Not found in cache, mark for API lookup
                    entity_data['status'] = 'needs_api_lookup'
        
        logger.info(f"Cache processing: {stats['cache_hits']} hits, {stats['concepts_created']} concepts created")
        return stats
    
    def _process_uncached_entities(self, sentences_data: Dict[str, Any], collection_name: str, total_count: int) -> Dict[str, int]:
        """Process entities that are not in cache using API calls.
        
        This method only processes entities with status 'needs_api_lookup'.
        Entities with 'null_no_api_value' status are skipped as they already failed lookup.
        
        Args:
            sentences_data: Dictionary with sentence data
            collection_name: Name of the collection
            
        Returns:
            Dictionary with processing statistics
        """
        stats = {
            'concepts_created': 0,
            'cache_hits': 0,
            'api_calls': 0
        }
        
        processed_count = 0
        
        for sentence_id, sentence_data in sentences_data.items():
            # Skip if already processed
            if sentence_data['status'] == 'processed':
                continue
                
            entities = sentence_data['entities']
            
            for entity_name, entity_data in entities.items():
                # Skip entities that already have wikidata_id (already processed)
                if entity_data.get('wikidata_id'):
                    continue
                    
                # Skip if already processed (cached)
                if entity_data['status'] == 'cache_hit':
                    continue
                    
                # Skip entities that already failed lookup (null cache entries)
                if entity_data['status'] == 'null_no_api_value':
                    continue
                    
                # Skip if not ready for API lookup
                if entity_data['status'] != 'needs_api_lookup':
                    continue
                
                # Make API call
                wikidata_entity = self.wikidata_client.search_entity(entity_name)
                stats['api_calls'] += 1
                processed_count += 1
                
                # Log progress every 10 entities
                if processed_count % 100 == 0:
                    percentage = (processed_count / total_count * 100) if total_count > 0 else 0
                    logger.info(f"  Processed {processed_count}/{total_count} entities ({percentage:.1f}%)")
                
                if wikidata_entity:
                    # API lookup successful
                    entity_data['status'] = 'api_lookup_complete'
                    entity_data['wikidata_id'] = wikidata_entity.qid
                    
                    # Write to Neo4j immediately
                    if self.concept_manager.create_concept_with_relationship(sentence_id, wikidata_entity):
                        stats['concepts_created'] += 1
                else:
                    # API lookup failed
                    entity_data['status'] = 'api_lookup_failed'
        
        logger.info(f"API processing: {stats['api_calls']} calls, {stats['concepts_created']} concepts created")
        return stats
    
    def _count_uncached_entities(self, sentences_data: Dict[str, Any]) -> int:
        """Count entities that need API lookup (only needs_api_lookup status without wikidata_id).
        
        Args:
            sentences_data: Dictionary with sentence data
            
        Returns:
            Number of uncached entities that need API lookup
        """
        uncached_count = 0
        
        for sentence_id, sentence_data in sentences_data.items():
            # Skip if already processed
            if sentence_data['status'] == 'processed':
                continue
                
            entities = sentence_data['entities']
            
            for entity_name, entity_data in entities.items():
                # Only count entities that need API lookup (exclude null cache entries and already processed)
                if entity_data['status'] == 'needs_api_lookup' and not entity_data.get('wikidata_id'):
                    uncached_count += 1
        
        return uncached_count
    
    def _count_cached_entities(self, sentences_data: Dict[str, Any]) -> int:
        """Count entities that will be checked for cache (only not_processed entities without wikidata_id).
        
        Args:
            sentences_data: Dictionary with sentence data
            
        Returns:
            Number of entities to check for cache
        """
        cached_count = 0
        
        for sentence_id, sentence_data in sentences_data.items():
            # Skip if already processed
            if sentence_data['status'] == 'processed':
                continue
                
            entities = sentence_data['entities']
            
            for entity_name, entity_data in entities.items():
                # Only count entities that need processing (not_processed status and no wikidata_id)
                if entity_data['status'] == 'not_processed' and not entity_data.get('wikidata_id'):
                    cached_count += 1
        
        return cached_count
    
    def _save_sentences_file(self, file_path: Path, sentences_data: Dict[str, Any]) -> None:
        """Save sentences data to JSON file.
        
        Args:
            file_path: Path to save the file
            sentences_data: Dictionary with sentence data
        """
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(sentences_data, f, indent=2, ensure_ascii=False)
            logger.debug(f"Saved sentences file: {file_path}")
        except Exception as e:
            logger.error(f"Error saving sentences file {file_path}: {e}")
            raise
    
    def _load_sentences_file(self, file_path: Path) -> Dict[str, Any]:
        """Load sentences data from JSON file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with sentence data
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading sentences file {file_path}: {e}")
            raise
    
    def _check_concepts_imported(self, collection_name: str) -> bool:
        """Check if concepts are already imported for a collection.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            True if concepts are already imported, False otherwise
        """
        # Query to check if any sentences in this collection have concepts
        query = """
        MATCH (b:Book {book_id: $collection_name})-[*1..5]->(s:Sentence)
        WHERE s.text IS NOT NULL 
        AND (s)-[:SENTENCE_CONTAINS_CONCEPT]->(:Concept)
        RETURN count(s) as concept_count
        LIMIT 1
        """
        
        # Create a temporary driver connection
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(
            self.neo4j_uri, 
            auth=(self.neo4j_user, self.neo4j_password)
        )
        
        try:
            with driver.session(database=self.neo4j_database) as session:
                result = session.run(query, collection_name=collection_name)
                record = result.single()
                
                if record and record['concept_count'] > 0:
                    logger.info(f"Found {record['concept_count']} sentences with concepts for collection {collection_name}")
                    return True
                else:
                    logger.info(f"No concepts found for collection {collection_name}")
                    return False
        finally:
            driver.close()
    
    def _can_import_from_json(self, sentences_data: Dict[str, Any]) -> bool:
        """Check if we can import concepts from JSON file (entities have wikidata_id).
        
        Args:
            sentences_data: Dictionary with sentence data
            
        Returns:
            True if we can import from JSON, False otherwise
        """
        total_entities = 0
        entities_with_wikidata = 0
        
        for sentence_data in sentences_data.values():
            entities = sentence_data.get('entities', {})
            total_entities += len(entities)
            
            for entity_data in entities.values():
                if entity_data.get('wikidata_id'):
                    entities_with_wikidata += 1
        
        if total_entities == 0:
            return False
            
        # If more than 50% of entities have wikidata_id, we can import from JSON
        import_ratio = entities_with_wikidata / total_entities
        logger.info(f"JSON import check: {entities_with_wikidata}/{total_entities} entities have wikidata_id ({import_ratio:.1%})")
        
        return import_ratio > 0.5
    
    def _import_concepts_from_json(self, sentences_data: Dict[str, Any], collection_name: str, sentences_file: Path, force: bool = False) -> Dict[str, Any]:
        """Import concepts from JSON file into the database.
        
        Args:
            sentences_data: Dictionary with sentence data
            collection_name: Name of the collection
            sentences_file: Path to the sentences JSON file
            
        Returns:
            Dictionary with import statistics
        """
        stats = {
            'sentences_processed': 0,
            'entities_extracted': 0,
            'concepts_created': 0,
            'cache_hits': 0,
            'api_calls': 0
        }
        
        logger.info(f"Importing concepts from JSON for collection: {collection_name}")
        
        try:
            with self.driver.session(database=self.neo4j_database) as session:
                for json_sentence_id, sentence_data in sentences_data.items():
                    entities = sentence_data.get('entities', {})
                    stats['entities_extracted'] += len(entities)
                    
                    # Use the sentence_id directly from the JSON file
                    actual_sentence_id = json_sentence_id
                    
                    # Verify the sentence exists in the database
                    verify_sentence_query = """
                    MATCH (s:Sentence {sentence_id: $sentence_id})
                    RETURN s.sentence_id as sentence_id
                    LIMIT 1
                    """
                    
                    result = session.run(verify_sentence_query, sentence_id=actual_sentence_id)
                    record = result.single()
                    
                    if not record:
                        logger.warning(f"Could not find sentence in database with ID: {actual_sentence_id}")
                        # Track this as a failed sentence lookup
                        stats['sentences_processed'] += 1
                        continue
                    
                    # Process each entity with a Wikidata ID
                    for entity_name, entity_data in entities.items():
                        if entity_data.get('wikidata_id'):
                            wikidata_id = entity_data['wikidata_id']
                            
                            # Get cached data for the entity
                            cached_data = self.cache_manager.get_cached_concept(entity_name)
                            if cached_data:
                                # Create WikidataEntity object
                                wikidata_entity = self.wikidata_client._create_entity_from_cache(cached_data)
                                
                                # Create concept and relationship using actual sentence_id
                                if force:
                                    # Force create/update concept with all expected fields
                                    if self._force_create_concept_with_relationship(actual_sentence_id, entity_name, entity_data, wikidata_entity):
                                        stats['concepts_created'] += 1
                                        stats['cache_hits'] += 1
                                else:
                                    # Normal concept creation
                                    if self.concept_manager.create_concept_with_relationship(actual_sentence_id, wikidata_entity):
                                        stats['concepts_created'] += 1
                                        stats['cache_hits'] += 1
                    
                    stats['sentences_processed'] += 1
                    
                    # Progress update
                    if stats['sentences_processed'] % 1000 == 0:
                        logger.info(f"  Processed {stats['sentences_processed']} sentences, created {stats['concepts_created']} concepts")
            
            logger.info(f"JSON import completed: {stats['concepts_created']} concepts created for {stats['sentences_processed']} sentences")
            return stats
            
        except Exception as e:
            logger.error(f"Error importing concepts from JSON for collection {collection_name}: {e}")
            raise
    
    def _update_global_stats(self, collection_stats: Dict[str, int]) -> None:
        """Update global statistics with collection statistics.
        
        Args:
            collection_stats: Statistics from a single collection
        """
        for key, value in collection_stats.items():
            if key in self.stats:
                self.stats[key] += value
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get comprehensive processing statistics.
        
        Returns:
            Dictionary with all processing statistics
        """
        # Add client statistics
        client_stats = self.wikidata_client.get_stats()
        self.stats.update(client_stats)
        
        return self.stats.copy()
    
    def import_concepts_from_sentence_file(self, sentences_file: Path) -> Dict[str, int]:
        """Import concepts from a sentence file into the database.
        
        Args:
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
            with open(sentences_file, 'r', encoding='utf-8') as f:
                sentences_data = json.load(f)
            
            logger.info(f"Importing concepts from {sentences_file}")
            
            with self.driver.session(database=self.neo4j_database) as session:
                for sentence_id, sentence_data in sentences_data.items():
                    entities = sentence_data.get('entities', {})
                    
                    if entities:
                        # Process each entity with a Wikidata ID
                        for entity_name, entity_data in entities.items():
                            if entity_data.get('wikidata_id'):
                                wikidata_id = entity_data['wikidata_id']
                                
                                # Use atomic transaction to create concept and relationships together
                                atomic_query = """
                                // First, verify the sentence exists
                                MATCH (s:Sentence {sentence_id: $sentence_id})
                                
                                // Only proceed if sentence exists
                                WITH s
                                
                                // Create or merge the concept node (use wikidata_id as unique identifier)
                                MERGE (c:Concept {wikidata_id: $wikidata_id})
                                ON CREATE SET 
                                    c.name = $name,
                                    c.wikidata_name = $name,
                                    c.label = $name,
                                    c.description = $description,
                                    c.aliases = $aliases,
                                    c.wikidata_url = $wikidata_url,
                                    c.created_at = datetime()
                                ON MATCH SET
                                    c.name = $name,
                                    c.wikidata_name = $name,
                                    c.label = $name,
                                    c.description = $description,
                                    c.aliases = $aliases,
                                    c.wikidata_url = $wikidata_url,
                                    c.updated_at = datetime()
                                
                                // Create bidirectional relationships
                                MERGE (s)-[r1:SENTENCE_CONTAINS_CONCEPT]->(c)
                                ON CREATE SET r1.created_at = datetime()
                                
                                MERGE (c)-[r2:CONCEPT_BELONGS_TO_SENTENCE]->(s)
                                ON CREATE SET r2.created_at = datetime()
                                
                                RETURN c.wikidata_id as created_concept_id
                                """
                                
                                result = session.run(atomic_query, {
                                    'sentence_id': sentence_id,
                                    'name': entity_name,
                                    'wikidata_id': wikidata_id,
                                    'description': entity_data.get('description', ''),
                                    'aliases': entity_data.get('aliases', []),
                                    'wikidata_url': f"https://www.wikidata.org/wiki/{wikidata_id}"
                                })
                                
                                record = result.single()
                                if record:
                                    stats['concepts_imported'] += 1
                                    stats['relationships_created'] += 2  # Bidirectional
                    
                    stats['sentences_processed'] += 1
                    
                    # Progress update
                    if stats['sentences_processed'] % 1000 == 0:
                        logger.info(f"  Processed {stats['sentences_processed']} sentences, imported {stats['concepts_imported']} concepts")
            
            logger.info(f"Concept import completed: {stats['concepts_imported']} concepts imported for {stats['sentences_processed']} sentences")
            return stats
            
        except Exception as e:
            logger.error(f"Error importing concepts from {sentences_file}: {e}")
            raise

    def close(self):
        """Clean up resources."""
        if hasattr(self, 'driver'):
            self.driver.close()
        logger.info("SequentialCollectionProcessor closed")
    
    def _force_create_concept_with_relationship(self, sentence_id: str, entity_name: str, entity_data: Dict, wikidata_entity) -> bool:
        """Force create/update concept node and establish bidirectional relationship with sentence.
        
        This method bypasses existing checks and ensures all expected fields are populated.
        
        Args:
            sentence_id: ID of the sentence
            entity_name: Name of the entity/concept
            entity_data: Entity data from JSON
            wikidata_entity: WikidataEntity object
            
        Returns:
            True if concept was created/updated successfully
        """
        query = """
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
        
        try:
            with self.driver.session(database=self.neo4j_database) as session:
                # Apply standard property keys for missing fields
                description = entity_data.get('description', entity_name)
                aliases = entity_data.get('aliases', [entity_name])
                wikidata_url = f"https://www.wikidata.org/wiki/{wikidata_entity.qid}"
                
                result = session.run(query, {
                    'sentence_id': sentence_id,
                    'name': entity_name,
                    'wikidata_id': wikidata_entity.qid,
                    'description': description,
                    'aliases': aliases,
                    'wikidata_url': wikidata_url
                })
                
                record = result.single()
                return record is not None
                
        except Exception as e:
            logger.error(f"Error force creating concept for sentence {sentence_id}: {e}")
            return False
