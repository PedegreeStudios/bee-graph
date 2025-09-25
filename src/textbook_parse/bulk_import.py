#!/usr/bin/env python3
"""
Bulk Import Utilities for Neo4j

This module provides optimized bulk import functions for loading large amounts
of textbook data into Neo4j efficiently.
"""

import logging
from typing import List, Dict, Any, Tuple, Optional
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, TransientError

logger = logging.getLogger(__name__)


class BulkImporter:
    """Handles bulk import operations for Neo4j."""
    
    def __init__(self, uri: str, username: str, password: str, database: str):
        """Initialize the bulk importer with Neo4j connection details."""
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
    
    def bulk_create_nodes(self, nodes: List[Tuple[str, Dict[str, Any]]], batch_size: int = 1000) -> int:
        """
        Create nodes in bulk using UNWIND for better performance.
        
        Args:
            nodes: List of (node_type, node_data) tuples
            batch_size: Number of nodes to process in each batch
            
        Returns:
            Number of nodes successfully created
        """
        if not nodes:
            return 0
        
        # Group nodes by type for batch processing
        nodes_by_type = {}
        for node_type, node_data in nodes:
            if node_type not in nodes_by_type:
                nodes_by_type[node_type] = []
            nodes_by_type[node_type].append(node_data)
        
        total_created = 0
        
        with self.driver.session(database=self.database) as session:
            for node_type, node_list in nodes_by_type.items():
                logger.info(f"Bulk creating {len(node_list)} {node_type} nodes...")
                
                # Process in batches
                for i in range(0, len(node_list), batch_size):
                    batch = node_list[i:i + batch_size]
                    created_count = self._create_node_batch(session, node_type, batch)
                    total_created += created_count
                    
                    if i + batch_size < len(node_list):
                        logger.debug(f"Created {created_count} {node_type} nodes (batch {i//batch_size + 1})")
                
                logger.info(f"Successfully created {total_created} {node_type} nodes")
        
        return total_created
    
    def _create_node_batch(self, session, node_type: str, node_batch: List[Dict[str, Any]]) -> int:
        """Create a batch of nodes using UNWIND for efficiency."""
        if not node_batch:
            return 0
        
        # Build the Cypher query using UNWIND
        query = f"""
        UNWIND $nodes AS node
        CREATE (n:{node_type})
        SET n += node
        RETURN count(n) as created_count
        """
        
        try:
            result = session.run(query, nodes=node_batch)
            record = result.single()
            return record["created_count"] if record else 0
        except Exception as e:
            logger.error(f"Error creating {node_type} nodes batch: {e}")
            return 0
    
    def bulk_create_relationships(self, relationships: List[Tuple[str, str, str]], batch_size: int = 1000) -> int:
        """
        Create relationships in bulk using UNWIND for better performance.
        
        Args:
            relationships: List of (rel_type, source_id, target_id) tuples
            batch_size: Number of relationships to process in each batch
            
        Returns:
            Number of relationships successfully created
        """
        if not relationships:
            return 0
        
        # Group relationships by type for batch processing
        rels_by_type = {}
        for rel_type, source_id, target_id in relationships:
            if rel_type not in rels_by_type:
                rels_by_type[rel_type] = []
            rels_by_type[rel_type].append({
                'source_id': source_id,
                'target_id': target_id
            })
        
        total_created = 0
        
        with self.driver.session(database=self.database) as session:
            for rel_type, rel_list in rels_by_type.items():
                logger.info(f"Bulk creating {len(rel_list)} {rel_type} relationships...")
                
                # Process in batches
                for i in range(0, len(rel_list), batch_size):
                    batch = rel_list[i:i + batch_size]
                    created_count = self._create_relationship_batch(session, rel_type, batch)
                    total_created += created_count
                    
                    if i + batch_size < len(rel_list):
                        logger.debug(f"Created {created_count} {rel_type} relationships (batch {i//batch_size + 1})")
                
                logger.info(f"Successfully created {total_created} {rel_type} relationships")
        
        return total_created
    
    def _create_relationship_batch(self, session, rel_type: str, rel_batch: List[Dict[str, str]]) -> int:
        """Create a batch of relationships using UNWIND for efficiency."""
        if not rel_batch:
            return 0
        
        # Map relationship types to their source and target node types and ID properties
        rel_mapping = {
            'BOOK_CONTAINS_CHAPTER': ('Book', 'book_id', 'Chapter', 'chapter_id'),
            'CHAPTER_CONTAINS_SUBCHAPTER': ('Chapter', 'chapter_id', 'Subchapter', 'subchapter_id'),
            'CHAPTER_CONTAINS_DOCUMENT': ('Chapter', 'chapter_id', 'Document', 'document_id'),
            'SUBCHAPTER_CONTAINS_DOCUMENT': ('Subchapter', 'subchapter_id', 'Document', 'document_id'),
            'DOCUMENT_CONTAINS_SECTION': ('Document', 'document_id', 'Section', 'section_id'),
            'DOCUMENT_CONTAINS_PARAGRAPH': ('Document', 'document_id', 'Paragraph', 'paragraph_id'),
            'SECTION_CONTAINS_SUBSECTION': ('Section', 'section_id', 'Subsection', 'subsection_id'),
            'SECTION_CONTAINS_PARAGRAPH': ('Section', 'section_id', 'Paragraph', 'paragraph_id'),
            'SUBSECTION_CONTAINS_PARAGRAPH': ('Subsection', 'subsection_id', 'Paragraph', 'paragraph_id'),
            'PARAGRAPH_CONTAINS_SENTENCE': ('Paragraph', 'paragraph_id', 'Sentence', 'sentence_id'),
            'SENTENCE_CONTAINS_CONCEPT': ('Sentence', 'sentence_id', 'Concept', 'concept_id'),
        }
        
        if rel_type not in rel_mapping:
            logger.error(f"Unknown relationship type: {rel_type}")
            return 0
        
        source_type, source_id_prop, target_type, target_id_prop = rel_mapping[rel_type]
        
        # Build the Cypher query using UNWIND with correct property names
        query = f"""
        UNWIND $relationships AS rel
        MATCH (source:{source_type} {{{source_id_prop}: rel.source_id}})
        MATCH (target:{target_type} {{{target_id_prop}: rel.target_id}})
        MERGE (source)-[r:{rel_type}]->(target)
        RETURN count(r) as created_count
        """
        
        try:
            result = session.run(query, relationships=rel_batch)
            record = result.single()
            return record["created_count"] if record else 0
        except Exception as e:
            logger.error(f"Error creating {rel_type} relationships batch: {e}")
            return 0
    
    def bulk_create_bidirectional_relationships(self, relationships: List[Tuple[str, str, str]], batch_size: int = 1000) -> int:
        """
        Create bidirectional relationships in bulk using UNWIND for better performance.
        
        Args:
            relationships: List of (rel_type, source_id, target_id) tuples
            batch_size: Number of relationships to process in each batch
            
        Returns:
            Number of bidirectional relationships successfully created
        """
        logger.info(f"Starting bulk_create_bidirectional_relationships with {len(relationships)} relationships")
        
        if not relationships:
            logger.info("No relationships provided for bidirectional creation")
            return 0
        
        # Map CONTAINS relationships to their BELONGS_TO counterparts
        # Format: (reverse_rel_type, source_type, source_id_prop, target_type, target_id_prop)
        # For BELONGS_TO relationships, the source and target are swapped from CONTAINS
        bidirectional_map = {
            'BOOK_CONTAINS_CHAPTER': ('CHAPTER_BELONGS_TO_BOOK', 'Chapter', 'chapter_id', 'Book', 'book_id'),
            'CHAPTER_CONTAINS_SUBCHAPTER': ('SUBCHAPTER_BELONGS_TO_CHAPTER', 'Subchapter', 'subchapter_id', 'Chapter', 'chapter_id'),
            'CHAPTER_CONTAINS_DOCUMENT': ('DOCUMENT_BELONGS_TO_CHAPTER', 'Document', 'document_id', 'Chapter', 'chapter_id'),
            'SUBCHAPTER_CONTAINS_DOCUMENT': ('DOCUMENT_BELONGS_TO_SUBCHAPTER', 'Document', 'document_id', 'Subchapter', 'subchapter_id'),
            'DOCUMENT_CONTAINS_SECTION': ('SECTION_BELONGS_TO_DOCUMENT', 'Section', 'section_id', 'Document', 'document_id'),
            'DOCUMENT_CONTAINS_PARAGRAPH': ('PARAGRAPH_BELONGS_TO_DOCUMENT', 'Paragraph', 'paragraph_id', 'Document', 'document_id'),
            'SECTION_CONTAINS_SUBSECTION': ('SUBSECTION_BELONGS_TO_SECTION', 'Subsection', 'subsection_id', 'Section', 'section_id'),
            'SECTION_CONTAINS_PARAGRAPH': ('PARAGRAPH_BELONGS_TO_SECTION', 'Paragraph', 'paragraph_id', 'Section', 'section_id'),
            'SUBSECTION_CONTAINS_PARAGRAPH': ('PARAGRAPH_BELONGS_TO_SUBSECTION', 'Paragraph', 'paragraph_id', 'Subsection', 'subsection_id'),
            'PARAGRAPH_CONTAINS_SENTENCE': ('SENTENCE_BELONGS_TO_PARAGRAPH', 'Sentence', 'sentence_id', 'Paragraph', 'paragraph_id'),
            'SENTENCE_CONTAINS_CONCEPT': ('CONCEPT_BELONGS_TO_SENTENCE', 'Concept', 'concept_id', 'Sentence', 'sentence_id'),
        }
        
        # Create bidirectional relationships
        bidirectional_rels = []
        for rel_type, source_id, target_id in relationships:
            if rel_type in bidirectional_map:
                reverse_rel_type, source_type, source_id_prop, target_type, target_id_prop = bidirectional_map[rel_type]
                # For BELONGS_TO relationships, source and target are swapped
                bidirectional_rels.append((reverse_rel_type, target_id, source_id))
        
        logger.info(f"Created {len(bidirectional_rels)} bidirectional relationships from {len(relationships)} forward relationships")
        
        if not bidirectional_rels:
            return 0
        
        # Group bidirectional relationships by type for batch processing
        rels_by_type = {}
        for rel_type, source_id, target_id in bidirectional_rels:
            if rel_type not in rels_by_type:
                rels_by_type[rel_type] = []
            rels_by_type[rel_type].append({
                'source_id': source_id,
                'target_id': target_id
            })
        
        # Create reverse mapping for BELONGS_TO relationships
        reverse_bidirectional_map = {}
        for forward_rel, (reverse_rel, source_type, source_id_prop, target_type, target_id_prop) in bidirectional_map.items():
            reverse_bidirectional_map[reverse_rel] = (source_type, source_id_prop, target_type, target_id_prop)
        
        total_created = 0
        
        with self.driver.session(database=self.database) as session:
            for rel_type, rel_list in rels_by_type.items():
                logger.info(f"Bulk creating {len(rel_list)} {rel_type} relationships...")
                
                # Get the mapping for this relationship type
                if rel_type in reverse_bidirectional_map:
                    source_type, source_id_prop, target_type, target_id_prop = reverse_bidirectional_map[rel_type]
                else:
                    logger.error(f"Unknown bidirectional relationship type: {rel_type}")
                    continue
                
                # Process in batches
                for i in range(0, len(rel_list), batch_size):
                    batch = rel_list[i:i + batch_size]
                    created_count = self._create_bidirectional_relationship_batch(session, rel_type, batch, source_type, source_id_prop, target_type, target_id_prop)
                    total_created += created_count
                    
                    if i + batch_size < len(rel_list):
                        logger.debug(f"Created {created_count} {rel_type} relationships (batch {i//batch_size + 1})")
                
                logger.info(f"Successfully created {total_created} {rel_type} relationships")
        
        return total_created
    
    def _create_bidirectional_relationship_batch(self, session, rel_type: str, rel_batch: List[Dict[str, str]], source_type: str, source_id_prop: str, target_type: str, target_id_prop: str) -> int:
        """Create a batch of bidirectional relationships using UNWIND for efficiency."""
        if not rel_batch:
            return 0
        
        # Build the Cypher query using UNWIND with correct property names
        query = f"""
        UNWIND $relationships AS rel
        MATCH (source:{source_type} {{{source_id_prop}: rel.source_id}})
        MATCH (target:{target_type} {{{target_id_prop}: rel.target_id}})
        MERGE (source)-[r:{rel_type}]->(target)
        RETURN count(r) as created_count
        """
        
        try:
            result = session.run(query, relationships=rel_batch)
            record = result.single()
            created_count = record["created_count"] if record else 0
            return created_count
        except Exception as e:
            logger.error(f"Error creating {rel_type} relationships batch: {e}")
            return 0

    def bulk_update_nodes(self, updates: List[Tuple[str, str, Dict[str, Any]]], batch_size: int = 1000) -> int:
        """
        Update nodes in bulk using UNWIND for better performance.
        
        Args:
            updates: List of (node_type, node_id, update_data) tuples
            batch_size: Number of updates to process in each batch
            
        Returns:
            Number of nodes successfully updated
        """
        if not updates:
            return 0
        
        # Group updates by node type
        updates_by_type = {}
        for node_type, node_id, update_data in updates:
            if node_type not in updates_by_type:
                updates_by_type[node_type] = []
            updates_by_type[node_type].append({
                'node_id': node_id,
                **update_data
            })
        
        total_updated = 0
        
        with self.driver.session(database=self.database) as session:
            for node_type, update_list in updates_by_type.items():
                logger.info(f"Bulk updating {len(update_list)} {node_type} nodes...")
                
                # Process in batches
                for i in range(0, len(update_list), batch_size):
                    batch = update_list[i:i + batch_size]
                    updated_count = self._update_node_batch(session, node_type, batch)
                    total_updated += updated_count
                    
                    if i + batch_size < len(update_list):
                        logger.debug(f"Updated {updated_count} {node_type} nodes (batch {i//batch_size + 1})")
                
                logger.info(f"Successfully updated {total_updated} {node_type} nodes")
        
        return total_updated
    
    def _update_node_batch(self, session, node_type: str, update_batch: List[Dict[str, Any]]) -> int:
        """Update a batch of nodes using UNWIND for efficiency."""
        if not update_batch:
            return 0
        
        # Build the Cypher query using UNWIND
        query = f"""
        UNWIND $updates AS update
        MATCH (n:{node_type} {{id: update.node_id}})
        SET n += update
        REMOVE n.node_id
        RETURN count(n) as updated_count
        """
        
        try:
            result = session.run(query, updates=update_batch)
            record = result.single()
            return record["updated_count"] if record else 0
        except Exception as e:
            logger.error(f"Error updating {node_type} nodes batch: {e}")
            return 0
    
    def get_import_statistics(self) -> Dict[str, Any]:
        """Get statistics about the current database state."""
        with self.driver.session(database=self.database) as session:
            # Get node counts by type
            node_result = session.run("""
                MATCH (n) 
                RETURN labels(n) as labels, count(n) as count 
                ORDER BY count DESC
            """)
            node_counts = [{"labels": record["labels"], "count": record["count"]} for record in node_result]
            
            # Get relationship counts by type
            rel_result = session.run("""
                MATCH ()-[r]->() 
                RETURN type(r) as rel_type, count(r) as count 
                ORDER BY count DESC
            """)
            rel_counts = [{"rel_type": record["rel_type"], "count": record["count"]} for record in rel_result]
            
            # Calculate totals
            total_nodes = sum(node["count"] for node in node_counts)
            total_relationships = sum(rel["count"] for rel in rel_counts)
            
            return {
                "total_nodes": total_nodes,
                "total_relationships": total_relationships,
                "node_counts": node_counts,
                "rel_counts": rel_counts
            }
    
    def close(self):
        """Close the Neo4j driver connection."""
        if self.driver:
            self.driver.close()


def create_bulk_importer(uri: str, username: str, password: str, database: str) -> BulkImporter:
    """Factory function to create a BulkImporter instance."""
    return BulkImporter(uri, username, password, database)
