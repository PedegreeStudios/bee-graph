"""Neo4j concept node and relationship management."""

import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class ConceptManager:
    """Manages concept nodes and relationships in Neo4j."""
    
    def __init__(self, driver):
        self.driver = driver
    
    def get_sentences_without_concepts(self, limit: int = 100) -> List[Dict]:
        """Get sentences that don't have concept relationships."""
        query = """
        MATCH (s:Sentence)
        WHERE NOT (s)-[:SENTENCE_CONTAINS_CONCEPT]->(:Concept)
        AND s.text IS NOT NULL
        RETURN s.sentence_id as sentence_id, 
               s.text as content
        LIMIT $limit
        """
        
        with self.driver.session() as session:
            result = session.run(query, limit=limit)
            return [dict(record) for record in result]
    
    def get_all_sentences_without_concepts(self) -> List[Dict]:
        """Get ALL sentences that don't have concept relationships."""
        query = """
        MATCH (s:Sentence)
        WHERE NOT (s)-[:SENTENCE_CONTAINS_CONCEPT]->(:Concept)
        AND s.text IS NOT NULL
        RETURN s.sentence_id as sentence_id, 
               s.text as content
        """
        
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(record) for record in result]
    
    def create_concept_with_relationship(self, sentence_id: str, wikidata_entity) -> bool:
        """Create concept node and establish bidirectional relationship with sentence."""
        query = """
        // First, verify the sentence exists
        MATCH (s:Sentence {sentence_id: $sentence_id})
        
        // Only proceed if sentence exists
        WITH s
        
        // Create or merge the concept node (use wikidata_id as unique identifier)
        MERGE (c:Concept {wikidata_id: $wikidata_id})
        ON CREATE SET 
            c.name = $name,
            c.wikidata_name = $wikidata_name,
            c.label = $label,
            c.description = $description,
            c.aliases = $aliases,
            c.wikidata_url = $wikidata_url,
            c.created_at = datetime()
        ON MATCH SET
            c.name = $name,
            c.wikidata_name = $wikidata_name,
            c.label = $label,
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
        
        try:
            with self.driver.session() as session:
                result = session.run(
                    query,
                    sentence_id=sentence_id,
                    name=wikidata_entity.label,
                    wikidata_id=wikidata_entity.qid,
                    wikidata_name=wikidata_entity.label,
                    label=wikidata_entity.label,
                    description=wikidata_entity.description,
                    aliases=wikidata_entity.aliases,
                    wikidata_url=wikidata_entity.wikidata_url
                )
                
                record = result.single()  # Get the single record from the result
                if record:
                    return True
                return False
                
        except Exception as e:
            logger.error(f"Error creating concept for sentence {sentence_id}: {e}")
            return False
    
    def get_concept_count(self) -> int:
        """Get total number of concept nodes."""
        query = "MATCH (c:Concept) RETURN count(c) as total"
        
        with self.driver.session() as session:
            result = session.run(query)
            record = result.single()  # Get the single record from the result
            return record['total'] if record else 0
    
    def get_sentences_with_concepts_count(self) -> int:
        """Get count of sentences that have concept relationships."""
        query = """
        MATCH (s:Sentence)-[:SENTENCE_CONTAINS_CONCEPT]->(:Concept)
        RETURN count(DISTINCT s) as total
        """
        
        with self.driver.session() as session:
            result = session.run(query)
            record = result.single()  # Get the single record from the result
            return record['total'] if record else 0
    
    def cleanup_orphaned_concepts(self) -> int:
        """Remove concept nodes that have no relationships to sentences."""
        query = """
        MATCH (c:Concept)
        WHERE NOT (c)-[:CONCEPT_BELONGS_TO_SENTENCE]->(:Sentence)
        AND NOT (c)<-[:SENTENCE_CONTAINS_CONCEPT]-(:Sentence)
        DETACH DELETE c
        RETURN count(c) as deleted_count
        """
        
        with self.driver.session() as session:
            result = session.run(query)
            record = result.single()
            deleted_count = record['deleted_count'] if record else 0
            logger.info(f"Cleaned up {deleted_count} orphaned concept nodes")
            return deleted_count
