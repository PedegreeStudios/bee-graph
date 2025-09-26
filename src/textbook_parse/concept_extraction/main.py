"""Main orchestration for the concept extraction system."""

import logging
from typing import Dict, Optional
from neo4j import GraphDatabase

from .cache_manager import CacheManager
from .entity_extractor import EntityExtractor
from .wikidata_client import WikidataClient
from .concept_manager import ConceptManager

logger = logging.getLogger(__name__)

class ConceptExtractionSystem:
    """Main system orchestrating the concept extraction process."""
    
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str, 
                 neo4j_database: str = "neo4j", cache_file: str = "wikidata_cache.json"):
        """Initialize the concept extraction system.
        
        Args:
            neo4j_uri: Neo4j database URI
            neo4j_user: Neo4j username
            neo4j_password: Neo4j password
            neo4j_database: Neo4j database name
            cache_file: Path to Wikidata cache file
        """
        # Initialize components
        self.cache_manager = CacheManager(cache_file)
        self.entity_extractor = EntityExtractor()
        self.wikidata_client = WikidataClient(self.cache_manager)
        
        # Neo4j connection
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.concept_manager = ConceptManager(self.driver)
        
        # Statistics
        self.stats = {
            'processed_sentences': 0,
            'concepts_created': 0,
            'entities_extracted': 0,
            'wikidata_lookups': 0
        }
        
        logger.info("ConceptExtractionSystem initialized")
    
    def process_sentences(self, batch_size: int = 50, max_sentences: Optional[int] = None, progress_callback=None) -> Dict[str, int]:
        """Process sentences without concept nodes.
        
        Args:
            batch_size: Number of sentences to process per batch (unused, kept for compatibility)
            max_sentences: Maximum total sentences to process (None for all)
            
        Returns:
            Dictionary with processing statistics
        """
        logger.info("Starting concept extraction process...")
        
        # Get ALL sentences without concepts at once
        sentences = self.concept_manager.get_all_sentences_without_concepts()
        
        if not sentences:
            logger.info("No sentences to process")
            return self.stats
        
        logger.info(f"Processing {len(sentences)} sentences...")
        
        total_processed = 0
        
        for sentence_data in sentences:
            sentence_id = sentence_data['sentence_id']
            content = sentence_data['content']
            
            if not content or len(content.strip()) < 10:
                self.stats['processed_sentences'] += 1
                total_processed += 1
                if progress_callback:
                    progress_callback(1)
                continue
            
            # Extract entities from sentence content
            entities = self.entity_extractor.extract_entities(content)
            self.stats['entities_extracted'] += len(entities)
            
            if not entities:
                self.stats['processed_sentences'] += 1
                total_processed += 1
                if progress_callback:
                    progress_callback(1)
                continue
            
            # Try to find Wikidata match for entities (take first valid match)
            concept_created = False
            for entity_text in entities:
                self.stats['wikidata_lookups'] += 1
                wikidata_entity = self.wikidata_client.search_entity(entity_text)
                
                if wikidata_entity and wikidata_entity.qid:
                    # Create concept and relationship
                    if self.concept_manager.create_concept_with_relationship(sentence_id, wikidata_entity):
                        self.stats['concepts_created'] += 1
                        concept_created = True
                        break  # One concept per sentence for now
            
            self.stats['processed_sentences'] += 1
            total_processed += 1
            
            # Update progress bar if callback provided
            if progress_callback:
                progress_callback(1)
            
            # Check if we've reached the limit
            if max_sentences and total_processed >= max_sentences:
                break
        
        # Add client stats to our stats
        client_stats = self.wikidata_client.get_stats()
        self.stats.update(client_stats)
        
        logger.info(f"Completed processing. Stats: {self.stats}")
        return self.stats
    
    def get_system_stats(self) -> Dict[str, int]:
        """Get comprehensive system statistics."""
        concept_count = self.concept_manager.get_concept_count()
        sentences_with_concepts = self.concept_manager.get_sentences_with_concepts_count()
        cache_stats = self.cache_manager.get_stats()
        
        return {
            **self.stats,
            'total_concepts_in_db': concept_count,
            'sentences_with_concepts': sentences_with_concepts,
            **cache_stats
        }
    
    def close(self):
        """Clean up resources."""
        if hasattr(self, 'driver'):
            self.driver.close()
        logger.info("ConceptExtractionSystem closed")
