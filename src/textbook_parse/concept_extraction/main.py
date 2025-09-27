"""Main orchestration for the concept extraction system."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, List, Tuple
from neo4j import GraphDatabase

from .cache_manager import CacheManager
from .entity_extractor import EntityExtractor
from .wikidata_client import WikidataClient
from .concept_manager import ConceptManager

logger = logging.getLogger(__name__)

class ThreadSafeStats:
    """Thread-safe statistics collector."""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._stats = {
            'processed_sentences': 0,
            'concepts_created': 0,
            'entities_extracted': 0,
            'wikidata_lookups': 0
        }
    
    def increment(self, key: str, value: int = 1):
        """Thread-safe increment of a statistic."""
        with self._lock:
            self._stats[key] += value
    
    def get_stats(self) -> Dict[str, int]:
        """Get current statistics snapshot."""
        with self._lock:
            return self._stats.copy()

class ConceptExtractionSystem:
    """Main system orchestrating the concept extraction process."""
    
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str, 
                 neo4j_database: str = "neo4j", cache_file: str = "wikidata_cache.json",
                 max_workers: int = 4):
        """Initialize the concept extraction system.
        
        Args:
            neo4j_uri: Neo4j database URI
            neo4j_user: Neo4j username
            neo4j_password: Neo4j password
            neo4j_database: Neo4j database name
            cache_file: Path to Wikidata cache file
            max_workers: Maximum number of worker threads
        """
        self.max_workers = max_workers
        
        # Initialize thread-safe components
        self.cache_manager = CacheManager(cache_file)
        self.stats = ThreadSafeStats()
        
        # Neo4j connection with connection pooling
        self.driver = GraphDatabase.driver(
            neo4j_uri, 
            auth=(neo4j_user, neo4j_password),
            max_connection_pool_size=max_workers * 2,  # Allow for connection pooling
            connection_timeout=30
        )
        self.concept_manager = ConceptManager(self.driver)
        
        # Create per-thread components
        self._wikidata_clients = {}
        self._entity_extractors = {}
        self._lock = threading.Lock()
        
        logger.info(f"ConceptExtractionSystem initialized with {max_workers} workers")
    
    def _get_thread_components(self) -> Tuple[EntityExtractor, WikidataClient]:
        """Get thread-local components for current thread."""
        thread_id = threading.get_ident()
        
        with self._lock:
            if thread_id not in self._entity_extractors:
                self._entity_extractors[thread_id] = EntityExtractor()
                self._wikidata_clients[thread_id] = WikidataClient(self.cache_manager)
        
        return self._entity_extractors[thread_id], self._wikidata_clients[thread_id]
    
    def _create_concept_with_relationship_thread_safe(self, sentence_id: str, wikidata_entity) -> bool:
        """Thread-safe concept creation using the main driver connection."""
        # Use the main driver connection which has connection pooling
        return self.concept_manager.create_concept_with_relationship(sentence_id, wikidata_entity)
    
    def _process_sentence_batch(self, sentences_batch: List[Dict]) -> Dict[str, int]:
        """Process a batch of sentences in a single thread."""
        thread_id = threading.get_ident()
        logger.debug(f"Thread {thread_id}: Starting batch of {len(sentences_batch)} sentences")
        
        entity_extractor, wikidata_client = self._get_thread_components()
        
        local_stats = {
            'processed_sentences': 0,
            'concepts_created': 0,
            'entities_extracted': 0,
            'wikidata_lookups': 0
        }
        
        try:
            for i, sentence_data in enumerate(sentences_batch):
                sentence_id = sentence_data['sentence_id']
                content = sentence_data['content']
                
                # Skip short or empty content
                if not content or len(content.strip()) < 10:
                    local_stats['processed_sentences'] += 1
                    continue
                
                # Extract entities from sentence content
                entities = entity_extractor.extract_entities(content)
                local_stats['entities_extracted'] += len(entities)
                
                if not entities:
                    local_stats['processed_sentences'] += 1
                    continue
                
                # Try to find Wikidata match for entities (take first valid match)
                concept_created = False
                for entity_text in entities:
                    local_stats['wikidata_lookups'] += 1
                    wikidata_entity = wikidata_client.search_entity(entity_text)
                    
                    if wikidata_entity and wikidata_entity.qid:
                        # Create concept and relationship
                        if self.concept_manager.create_concept_with_relationship(sentence_id, wikidata_entity):
                            local_stats['concepts_created'] += 1
                            concept_created = True
                            break  # One concept per sentence for now
                
                local_stats['processed_sentences'] += 1
                
                # Log progress every 10 sentences
                if (i + 1) % 10 == 0:
                    logger.debug(f"Thread {thread_id}: Processed {i + 1}/{len(sentences_batch)} sentences")
        
        except Exception as e:
            logger.error(f"Thread {thread_id}: Error in batch processing: {e}")
            raise
        
        logger.debug(f"Thread {thread_id}: Completed batch with stats: {local_stats}")
        return local_stats
    
    def _process_single_sentence(self, sentence_data: Dict) -> Dict[str, int]:
        """Process a single sentence in a thread."""
        thread_id = threading.get_ident()
        
        local_stats = {
            'processed_sentences': 0,
            'concepts_created': 0,
            'entities_extracted': 0,
            'wikidata_lookups': 0
        }
        
        try:
            sentence_id = sentence_data['sentence_id']
            content = sentence_data['content']
            
            # Skip short or empty content
            if not content or len(content.strip()) < 10:
                local_stats['processed_sentences'] = 1
                return local_stats
            
            # Get thread-local components
            entity_extractor, wikidata_client = self._get_thread_components()
            
            # Extract entities from sentence content
            entities = entity_extractor.extract_entities(content)
            local_stats['entities_extracted'] = len(entities)
            
            if not entities:
                local_stats['processed_sentences'] = 1
                return local_stats
            
            # Try to find Wikidata match for entities (take first valid match)
            for entity_text in entities:
                local_stats['wikidata_lookups'] += 1
                wikidata_entity = wikidata_client.search_entity(entity_text)
                
                if wikidata_entity and wikidata_entity.qid:
                    # Create concept and relationship
                    if self.concept_manager.create_concept_with_relationship(sentence_id, wikidata_entity):
                        local_stats['concepts_created'] = 1
                        break  # One concept per sentence for now
            
            local_stats['processed_sentences'] = 1
            
        except Exception as e:
            logger.error(f"Thread {thread_id}: Error processing sentence: {e}")
            local_stats['processed_sentences'] = 1  # Still count as processed to avoid infinite retries
        
        return local_stats
    
    def process_sentences(self, batch_size: int = 50, max_sentences: Optional[int] = None, 
                         progress_callback=None) -> Dict[str, int]:
        """Process sentences without concept nodes using multi-threading.
        
        Args:
            batch_size: Number of sentences to process per thread batch
            max_sentences: Maximum total sentences to process (None for all)
            progress_callback: Callback function for progress updates
            
        Returns:
            Dictionary with processing statistics
        """
        logger.info("Starting multi-threaded concept extraction process...")
        
        # Get ALL sentences without concepts at once
        sentences = self.concept_manager.get_all_sentences_without_concepts()
        
        if not sentences:
            logger.info("No sentences to process")
            return self.stats.get_stats()
        
        # Apply max_sentences limit if specified
        if max_sentences:
            sentences = sentences[:max_sentences]
        
        logger.info(f"Processing {len(sentences)} sentences with {self.max_workers} workers...")
        
        # Use a simpler approach: process sentences individually in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit individual sentences as jobs
            future_to_sentence = {
                executor.submit(self._process_single_sentence, sentence): sentence 
                for sentence in sentences
            }
            
            # Process completed sentences
            completed_count = 0
            for future in as_completed(future_to_sentence, timeout=600):  # 10 minute total timeout
                try:
                    sentence_stats = future.result(timeout=30)  # 30 second timeout per sentence
                    
                    # Update global statistics
                    for key, value in sentence_stats.items():
                        self.stats.increment(key, value)
                    
                    # Update progress
                    if progress_callback:
                        progress_callback(1)
                    
                    completed_count += 1
                    if completed_count % 100 == 0:
                        logger.info(f"Completed {completed_count}/{len(sentences)} sentences")
                        
                except TimeoutError:
                    sentence = future_to_sentence[future]
                    logger.error(f"Timeout processing sentence: {sentence.get('sentence_id', 'unknown')}")
                except Exception as e:
                    sentence = future_to_sentence[future]
                    logger.error(f"Error processing sentence {sentence.get('sentence_id', 'unknown')}: {e}")
        
        # Add aggregated client stats
        client_stats = self._aggregate_client_stats()
        final_stats = self.stats.get_stats()
        final_stats.update(client_stats)
        
        logger.info(f"Completed multi-threaded processing. Stats: {final_stats}")
        return final_stats
    
    def _aggregate_client_stats(self) -> Dict[str, int]:
        """Aggregate statistics from all Wikidata clients."""
        total_api_calls = 0
        total_cache_hits = 0
        
        with self._lock:
            for client in self._wikidata_clients.values():
                stats = client.get_stats()
                total_api_calls += stats['api_calls']
                total_cache_hits += stats['cache_hits']
        
        total_requests = total_api_calls + total_cache_hits
        cache_hit_rate = (total_cache_hits / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'api_calls': total_api_calls,
            'cache_hits': total_cache_hits,
            'cache_hit_rate': cache_hit_rate
        }
    
    def get_system_stats(self) -> Dict[str, int]:
        """Get comprehensive system statistics."""
        concept_count = self.concept_manager.get_concept_count()
        sentences_with_concepts = self.concept_manager.get_sentences_with_concepts_count()
        cache_stats = self.cache_manager.get_stats()
        
        current_stats = self.stats.get_stats()
        return {
            **current_stats,
            'total_concepts_in_db': concept_count,
            'sentences_with_concepts': sentences_with_concepts,
            **cache_stats
        }
    
    def close(self):
        """Clean up resources."""
        if hasattr(self, 'driver'):
            self.driver.close()
        logger.info("ConceptExtractionSystem closed")
