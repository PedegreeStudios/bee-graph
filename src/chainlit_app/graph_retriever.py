"""
Neo4j Graph Retriever Module

This module provides the GraphRetriever class for querying the Neo4j knowledge graph
to retrieve relevant educational content based on user questions.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError


class GraphRetriever:
    """Neo4j graph retriever for educational content."""
    
    def __init__(self, uri: str = None, 
                 username: str = None, 
                 password: str = None,
                 database: str = None):
        """
        Initialize the graph retriever.
        
        Args:
            uri: Neo4j connection URI (if None, loads from config)
            username: Neo4j username (if None, loads from config)
            password: Neo4j password (if None, loads from config)
            database: Neo4j database name (if None, loads from config)
        """
        # Load from config if parameters not provided
        if uri is None or username is None or password is None or database is None:
            from config.config_loader import get_neo4j_connection_params
            config_uri, config_username, config_password, config_database = get_neo4j_connection_params()
            self.uri = uri if uri is not None else config_uri
            self.username = username if username is not None else config_username
            self.password = password if password is not None else config_password
            self.database = database if database is not None else config_database
        else:
            self.uri = uri
            self.username = username
            self.password = password
            self.database = database
        self.driver = None
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
    
    def _connect(self) -> None:
        """Establish connection to Neo4j database."""
        if self.driver is None:
            try:
                # Use no-auth if no username/password provided
                if not self.username and not self.password:
                    self.driver = GraphDatabase.driver(self.uri)
                else:
                    self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
                
                # Test the connection
                with self.driver.session(database=self.database) as session:
                    session.run("RETURN 1")
                    
                self.logger.info(f"Connected to Neo4j at {self.uri}")
                
            except ServiceUnavailable:
                raise ServiceUnavailable(f"Neo4j is not running or not accessible at {self.uri}")
            except AuthError:
                raise AuthError("Authentication failed. Check your Neo4j credentials.")
            except Exception as e:
                raise Exception(f"Error connecting to Neo4j: {e}")
    
    def search_concepts(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for concepts matching the query terms.
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            
        Returns:
            List of concept dictionaries
        """
        if self.driver is None:
            self._connect()
        
        try:
            with self.driver.session(database=self.database) as session:
                # Search concepts by label, text, or wikidata_name
                cypher_query = """
                MATCH (c:Concept)
                WHERE toLower(c.label) CONTAINS toLower($query)
                   OR toLower(c.text) CONTAINS toLower($query)
                   OR toLower(c.wikidata_name) CONTAINS toLower($query)
                RETURN c.concept_id as concept_id,
                       c.label as label,
                       c.text as text,
                       c.wikidata_id as wikidata_id,
                       c.wikidata_name as wikidata_name,
                       c.lens as lens
                ORDER BY 
                    CASE 
                        WHEN toLower(c.label) CONTAINS toLower($query) THEN 1
                        WHEN toLower(c.wikidata_name) CONTAINS toLower($query) THEN 2
                        ELSE 3
                    END
                LIMIT $limit
                """
                
                result = session.run(cypher_query, query=query, limit=limit)
                concepts = [dict(record) for record in result]
                
                self.logger.info(f"Found {len(concepts)} concepts for query: {query}")
                return concepts
                
        except Exception as e:
            self.logger.error(f"Error searching concepts: {e}")
            return []
    
    def search_sentences_by_content(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Search for sentences containing the query text.
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            
        Returns:
            List of sentence dictionaries with context
        """
        if self.driver is None:
            self._connect()
        
        try:
            with self.driver.session(database=self.database) as session:
                # Search sentences by text content
                cypher_query = """
                MATCH (s:Sentence)
                WHERE toLower(s.text) CONTAINS toLower($query)
                OPTIONAL MATCH (s)-[:SENTENCE_BELONGS_TO_PARAGRAPH]->(p:Paragraph)
                OPTIONAL MATCH (p)-[:PARAGRAPH_BELONGS_TO_SUBSECTION]->(ss:Subsection)
                OPTIONAL MATCH (ss)-[:SUBSECTION_BELONGS_TO_SECTION]->(sec:Section)
                OPTIONAL MATCH (sec)-[:SECTION_BELONGS_TO_DOCUMENT]->(d:Document)
                OPTIONAL MATCH (d)-[:DOCUMENT_BELONGS_TO_CHAPTER]->(c:Chapter)
                OPTIONAL MATCH (d)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc:Subchapter)
                RETURN s.sentence_id as sentence_id,
                       s.text as text,
                       s.lens as lens,
                       p.paragraph_id as paragraph_id,
                       ss.subsection_id as subsection_id,
                       ss.title as subsection_title,
                       sec.section_id as section_id,
                       sec.title as section_title,
                       d.document_id as document_id,
                       d.title as document_title,
                       c.chapter_id as chapter_id,
                       c.title as chapter_title,
                       sc.subchapter_id as subchapter_id,
                       sc.title as subchapter_title
                ORDER BY s.text
                LIMIT $limit
                """
                
                result = session.run(cypher_query, query=query, limit=limit)
                sentences = [dict(record) for record in result]
                
                self.logger.info(f"Found {len(sentences)} sentences for query: {query}")
                return sentences
                
        except Exception as e:
            self.logger.error(f"Error searching sentences: {e}")
            return []
    
    def get_sentences_for_concepts(self, concept_ids: List[str], limit: int = 30) -> List[Dict[str, Any]]:
        """
        Get sentences linked to specific concepts.
        
        Args:
            concept_ids: List of concept IDs
            limit: Maximum number of results to return
            
        Returns:
            List of sentence dictionaries with context
        """
        if not concept_ids:
            return []
        
        if self.driver is None:
            self._connect()
        
        try:
            with self.driver.session(database=self.database) as session:
                cypher_query = """
                MATCH (c:Concept)-[:CONCEPT_BELONGS_TO_SENTENCE]->(s:Sentence)
                WHERE c.concept_id IN $concept_ids
                OPTIONAL MATCH (s)-[:SENTENCE_BELONGS_TO_PARAGRAPH]->(p:Paragraph)
                OPTIONAL MATCH (p)-[:PARAGRAPH_BELONGS_TO_SUBSECTION]->(ss:Subsection)
                OPTIONAL MATCH (ss)-[:SUBSECTION_BELONGS_TO_SECTION]->(sec:Section)
                OPTIONAL MATCH (sec)-[:SECTION_BELONGS_TO_DOCUMENT]->(d:Document)
                OPTIONAL MATCH (d)-[:DOCUMENT_BELONGS_TO_CHAPTER]->(c:Chapter)
                OPTIONAL MATCH (d)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc:Subchapter)
                RETURN DISTINCT
                       s.sentence_id as sentence_id,
                       s.text as text,
                       s.lens as lens,
                       c.concept_id as concept_id,
                       c.label as concept_label,
                       c.text as concept_text,
                       p.paragraph_id as paragraph_id,
                       ss.subsection_id as subsection_id,
                       ss.title as subsection_title,
                       sec.section_id as section_id,
                       sec.title as section_title,
                       d.document_id as document_id,
                       d.title as document_title,
                       c.chapter_id as chapter_id,
                       c.title as chapter_title,
                       sc.subchapter_id as subchapter_id,
                       sc.title as subchapter_title
                ORDER BY c.label, s.text
                LIMIT $limit
                """
                
                result = session.run(cypher_query, concept_ids=concept_ids, limit=limit)
                sentences = [dict(record) for record in result]
                
                self.logger.info(f"Found {len(sentences)} sentences for {len(concept_ids)} concepts")
                return sentences
                
        except Exception as e:
            self.logger.error(f"Error getting sentences for concepts: {e}")
            return []
    
    def get_hierarchical_context(self, sentence_ids: List[str]) -> Dict[str, Any]:
        """
        Get hierarchical context for sentences (chapter, section, etc.).
        
        Args:
            sentence_ids: List of sentence IDs
            
        Returns:
            Dictionary containing hierarchical context information
        """
        if not sentence_ids:
            return {}
        
        if self.driver is None:
            self._connect()
        
        try:
            with self.driver.session(database=self.database) as session:
                cypher_query = """
                MATCH (s:Sentence)
                WHERE s.sentence_id IN $sentence_ids
                OPTIONAL MATCH (s)-[:SENTENCE_BELONGS_TO_PARAGRAPH]->(p:Paragraph)
                OPTIONAL MATCH (p)-[:PARAGRAPH_BELONGS_TO_SUBSECTION]->(ss:Subsection)
                OPTIONAL MATCH (ss)-[:SUBSECTION_BELONGS_TO_SECTION]->(sec:Section)
                OPTIONAL MATCH (sec)-[:SECTION_BELONGS_TO_DOCUMENT]->(d:Document)
                OPTIONAL MATCH (d)-[:DOCUMENT_BELONGS_TO_CHAPTER]->(c:Chapter)
                OPTIONAL MATCH (d)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc:Subchapter)
                OPTIONAL MATCH (c)-[:CHAPTER_BELONGS_TO_BOOK]->(b:Book)
                RETURN DISTINCT
                       s.sentence_id as sentence_id,
                       b.book_id as book_id,
                       b.title as book_title,
                       c.chapter_id as chapter_id,
                       c.title as chapter_title,
                       sc.subchapter_id as subchapter_id,
                       sc.title as subchapter_title,
                       d.document_id as document_id,
                       d.title as document_title,
                       sec.section_id as section_id,
                       sec.title as section_title,
                       ss.subsection_id as subsection_id,
                       ss.title as subsection_title
                """
                
                result = session.run(cypher_query, sentence_ids=sentence_ids)
                context_data = [dict(record) for record in result]
                
                # Organize context by sentence_id
                context = {}
                for record in context_data:
                    sentence_id = record['sentence_id']
                    if sentence_id not in context:
                        context[sentence_id] = record
                
                self.logger.info(f"Retrieved context for {len(context)} sentences")
                return context
                
        except Exception as e:
            self.logger.error(f"Error getting hierarchical context: {e}")
            return {}
    
    def get_related_concepts(self, concept_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get concepts that appear in the same sentences as the given concept.
        
        Args:
            concept_id: ID of the concept to find related concepts for
            limit: Maximum number of related concepts to return
            
        Returns:
            List of related concept dictionaries
        """
        if self.driver is None:
            self._connect()
        
        try:
            with self.driver.session(database=self.database) as session:
                cypher_query = """
                MATCH (c1:Concept {concept_id: $concept_id})-[:CONCEPT_BELONGS_TO_SENTENCE]->(s:Sentence)<-[:CONCEPT_BELONGS_TO_SENTENCE]-(c2:Concept)
                WHERE c1.concept_id <> c2.concept_id
                RETURN DISTINCT
                       c2.concept_id as concept_id,
                       c2.label as label,
                       c2.text as text,
                       c2.wikidata_id as wikidata_id,
                       c2.wikidata_name as wikidata_name,
                       c2.lens as lens,
                       count(s) as co_occurrence_count
                ORDER BY co_occurrence_count DESC
                LIMIT $limit
                """
                
                result = session.run(cypher_query, concept_id=concept_id, limit=limit)
                related_concepts = [dict(record) for record in result]
                
                self.logger.info(f"Found {len(related_concepts)} related concepts for {concept_id}")
                return related_concepts
                
        except Exception as e:
            self.logger.error(f"Error getting related concepts: {e}")
            return []
    
    def test_connection(self) -> bool:
        """
        Test the Neo4j connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            self._connect()
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def trace_node_hierarchy(self, node_id: str, node_type: str = None) -> Dict[str, Any]:
        """
        Trace the full hierarchy from a node back to the Book node.
        
        Args:
            node_id: The ID of the node to trace
            node_type: The type of node (optional, will be detected if not provided)
            
        Returns:
            Dictionary containing the full hierarchy path
        """
        try:
            with self.driver.session(database=self.database) as session:
                # First, determine the node type if not provided
                if not node_type:
                    node_type_query = """
                    MATCH (n) 
                    WHERE n.sentence_id = $node_id 
                       OR n.paragraph_id = $node_id 
                       OR n.subsection_id = $node_id 
                       OR n.section_id = $node_id 
                       OR n.document_id = $node_id 
                       OR n.subchapter_id = $node_id 
                       OR n.chapter_id = $node_id 
                       OR n.book_id = $node_id
                       OR n.concept_id = $node_id
                    RETURN labels(n) as labels
                    """
                    result = session.run(node_type_query, node_id=node_id)
                    record = result.single()
                    if record:
                        node_type = record['labels'][0] if record['labels'] else 'Unknown'
                    else:
                        return {'error': f'Node with ID {node_id} not found'}
                
                # Build the hierarchy trace query based on node type
                hierarchy_query = self._build_hierarchy_query(node_type, node_id)
                
                if not hierarchy_query:
                    return {'error': f'Unsupported node type: {node_type}'}
                
                # Execute the hierarchy query
                result = session.run(hierarchy_query, node_id=node_id)
                record = result.single()
                
                if not record:
                    return {'error': f'No hierarchy found for {node_type} with ID {node_id}'}
                
                # Build the hierarchy path
                hierarchy = self._build_hierarchy_path(record, node_type)
                
                self.logger.info(f"Successfully traced hierarchy for {node_type} {node_id}")
                return hierarchy
                
        except Exception as e:
            self.logger.error(f"Error tracing hierarchy for {node_id}: {e}")
            return {'error': str(e)}
    
    def _build_hierarchy_query(self, node_type: str, node_id: str) -> str:
        """Build the appropriate Cypher query for hierarchy tracing based on node type."""
        
        # Build the query based on node type
        if node_type == 'Sentence':
            return """
            MATCH (s:Sentence {sentence_id: $node_id})
            OPTIONAL MATCH (s)-[:SENTENCE_BELONGS_TO_PARAGRAPH]->(p:Paragraph)
            OPTIONAL MATCH (p)-[:PARAGRAPH_BELONGS_TO_SUBSECTION]->(ss:Subsection)
            OPTIONAL MATCH (p)-[:PARAGRAPH_BELONGS_TO_SECTION]->(sec:Section)
            OPTIONAL MATCH (p)-[:PARAGRAPH_BELONGS_TO_DOCUMENT]->(d:Document)
            OPTIONAL MATCH (ss)-[:SUBSECTION_BELONGS_TO_SECTION]->(sec2:Section)
            OPTIONAL MATCH (sec)-[:SECTION_BELONGS_TO_DOCUMENT]->(d2:Document)
            OPTIONAL MATCH (sec2)-[:SECTION_BELONGS_TO_DOCUMENT]->(d3:Document)
            OPTIONAL MATCH (d)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc:Subchapter)
            OPTIONAL MATCH (d2)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc2:Subchapter)
            OPTIONAL MATCH (d3)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc3:Subchapter)
            OPTIONAL MATCH (sc)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c:Chapter)
            OPTIONAL MATCH (sc2)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c2:Chapter)
            OPTIONAL MATCH (sc3)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c3:Chapter)
            OPTIONAL MATCH (c)-[:CHAPTER_BELONGS_TO_BOOK]->(b:Book)
            OPTIONAL MATCH (c2)-[:CHAPTER_BELONGS_TO_BOOK]->(b2:Book)
            OPTIONAL MATCH (c3)-[:CHAPTER_BELONGS_TO_BOOK]->(b3:Book)
            RETURN s, p, ss, sec, sec2, d, d2, d3, sc, sc2, sc3, c, c2, c3, b, b2, b3
            """
        
        elif node_type == 'Paragraph':
            return """
            MATCH (p:Paragraph {paragraph_id: $node_id})
            OPTIONAL MATCH (p)-[:PARAGRAPH_BELONGS_TO_SUBSECTION]->(ss:Subsection)
            OPTIONAL MATCH (ss)-[:SUBSECTION_BELONGS_TO_SECTION]->(sec:Section)
            OPTIONAL MATCH (sec)-[:SECTION_BELONGS_TO_DOCUMENT]->(d:Document)
            OPTIONAL MATCH (d)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc:Subchapter)
            OPTIONAL MATCH (sc)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c:Chapter)
            OPTIONAL MATCH (c)-[:CHAPTER_BELONGS_TO_BOOK]->(b:Book)
            RETURN p, ss, sec, d, sc, c, b
            """
        
        elif node_type == 'Subsection':
            return """
            MATCH (ss:Subsection {subsection_id: $node_id})
            OPTIONAL MATCH (ss)-[:SUBSECTION_BELONGS_TO_SECTION]->(sec:Section)
            OPTIONAL MATCH (sec)-[:SECTION_BELONGS_TO_DOCUMENT]->(d:Document)
            OPTIONAL MATCH (d)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc:Subchapter)
            OPTIONAL MATCH (sc)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c:Chapter)
            OPTIONAL MATCH (c)-[:CHAPTER_BELONGS_TO_BOOK]->(b:Book)
            RETURN ss, sec, d, sc, c, b
            """
        
        elif node_type == 'Section':
            return """
            MATCH (sec:Section {section_id: $node_id})
            OPTIONAL MATCH (sec)-[:SECTION_BELONGS_TO_DOCUMENT]->(d:Document)
            OPTIONAL MATCH (d)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc:Subchapter)
            OPTIONAL MATCH (sc)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c:Chapter)
            OPTIONAL MATCH (c)-[:CHAPTER_BELONGS_TO_BOOK]->(b:Book)
            RETURN sec, d, sc, c, b
            """
        
        elif node_type == 'Document':
            return """
            MATCH (d:Document {document_id: $node_id})
            OPTIONAL MATCH (d)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc:Subchapter)
            OPTIONAL MATCH (sc)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c:Chapter)
            OPTIONAL MATCH (c)-[:CHAPTER_BELONGS_TO_BOOK]->(b:Book)
            RETURN d, sc, c, b
            """
        
        elif node_type == 'Subchapter':
            return """
            MATCH (sc:Subchapter {subchapter_id: $node_id})
            OPTIONAL MATCH (sc)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c:Chapter)
            OPTIONAL MATCH (c)-[:CHAPTER_BELONGS_TO_BOOK]->(b:Book)
            RETURN sc, c, b
            """
        
        elif node_type == 'Chapter':
            return """
            MATCH (c:Chapter {chapter_id: $node_id})
            OPTIONAL MATCH (c)-[:CHAPTER_BELONGS_TO_BOOK]->(b:Book)
            RETURN c, b
            """
        
        elif node_type == 'Book':
            return """
            MATCH (b:Book {book_id: $node_id})
            RETURN b
            """
        
        elif node_type == 'Concept':
            return """
            MATCH (concept:Concept {wikidata_id: $node_id})
            OPTIONAL MATCH (concept)-[:CONCEPT_IN_SENTENCE]->(s:Sentence)
            OPTIONAL MATCH (s)-[:SENTENCE_BELONGS_TO_PARAGRAPH]->(p:Paragraph)
            OPTIONAL MATCH (p)-[:PARAGRAPH_BELONGS_TO_SUBSECTION]->(ss:Subsection)
            OPTIONAL MATCH (p)-[:PARAGRAPH_BELONGS_TO_SECTION]->(sec:Section)
            OPTIONAL MATCH (p)-[:PARAGRAPH_BELONGS_TO_DOCUMENT]->(d:Document)
            OPTIONAL MATCH (ss)-[:SUBSECTION_BELONGS_TO_SECTION]->(sec2:Section)
            OPTIONAL MATCH (sec)-[:SECTION_BELONGS_TO_DOCUMENT]->(d2:Document)
            OPTIONAL MATCH (sec2)-[:SECTION_BELONGS_TO_DOCUMENT]->(d3:Document)
            OPTIONAL MATCH (d)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc:Subchapter)
            OPTIONAL MATCH (d2)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc2:Subchapter)
            OPTIONAL MATCH (d3)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc3:Subchapter)
            OPTIONAL MATCH (sc)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c:Chapter)
            OPTIONAL MATCH (sc2)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c2:Chapter)
            OPTIONAL MATCH (sc3)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c3:Chapter)
            OPTIONAL MATCH (c)-[:CHAPTER_BELONGS_TO_BOOK]->(b:Book)
            OPTIONAL MATCH (c2)-[:CHAPTER_BELONGS_TO_BOOK]->(b2:Book)
            OPTIONAL MATCH (c3)-[:CHAPTER_BELONGS_TO_BOOK]->(b3:Book)
            RETURN concept, s, p, ss, sec, sec2, d, d2, d3, sc, sc2, sc3, c, c2, c3, b, b2, b3
            """
        
        return None
    
    def _build_hierarchy_path(self, record: Any, node_type: str) -> Dict[str, Any]:
        """Build a structured hierarchy path from the query result."""
        hierarchy = {
            'node_type': node_type,
            'path': [],
            'full_path': '',
            'book': None,
            'chapter': None,
            'subchapter': None,
            'document': None,
            'section': None,
            'subsection': None,
            'paragraph': None,
            'sentence': None,
            'concept': None
        }
        
        # Extract nodes from the record
        nodes = {}
        for key, value in record.items():
            if value is not None and hasattr(value, 'items'):
                # It's a Neo4j node
                node_data = dict(value.items())
                node_labels = list(value.labels) if hasattr(value, 'labels') else []
                if node_labels:
                    nodes[node_labels[0]] = node_data
        
        # Build the path from bottom to top
        path_parts = []
        
        # Add the starting node
        if node_type in nodes:
            hierarchy[node_type.lower()] = nodes[node_type]
            path_parts.append(f"{node_type}: {nodes[node_type].get('title', nodes[node_type].get('text', 'Unknown'))}")
        
        # Trace up the hierarchy in correct order, handling multiple possible paths
        # Select the best available path by choosing non-null values
        
        # Book level
        book = nodes.get('Book') or nodes.get('b') or nodes.get('b2') or nodes.get('b3')
        if book:
            hierarchy['book'] = book
            path_parts.append(f"Book: {book.get('title', 'Unknown')}")
        
        # Chapter level
        chapter = nodes.get('Chapter') or nodes.get('c') or nodes.get('c2') or nodes.get('c3')
        if chapter:
            hierarchy['chapter'] = chapter
            path_parts.append(f"Chapter: {chapter.get('title', 'Unknown')}")
        
        # Subchapter level
        subchapter = nodes.get('Subchapter') or nodes.get('sc') or nodes.get('sc2') or nodes.get('sc3')
        if subchapter:
            hierarchy['subchapter'] = subchapter
            path_parts.append(f"Subchapter: {subchapter.get('title', 'Unknown')}")
        
        # Document level
        document = nodes.get('Document') or nodes.get('d') or nodes.get('d2') or nodes.get('d3')
        if document:
            hierarchy['document'] = document
            path_parts.append(f"Document: {document.get('title', 'Unknown')}")
        
        # Section level
        section = nodes.get('Section') or nodes.get('sec') or nodes.get('sec2')
        if section:
            hierarchy['section'] = section
            path_parts.append(f"Section: {section.get('title', 'Unknown')}")
        
        # Subsection level
        subsection = nodes.get('Subsection') or nodes.get('ss')
        if subsection:
            hierarchy['subsection'] = subsection
            path_parts.append(f"Subsection: {subsection.get('title', 'Unknown')}")
        
        # Paragraph level
        paragraph = nodes.get('Paragraph') or nodes.get('p')
        if paragraph:
            hierarchy['paragraph'] = paragraph
            path_parts.append(f"Paragraph: {paragraph.get('text', 'Unknown')[:50]}...")
        
        # Sentence level
        sentence = nodes.get('Sentence') or nodes.get('s')
        if sentence:
            hierarchy['sentence'] = sentence
            path_parts.append(f"Sentence: {sentence.get('text', 'Unknown')[:50]}...")
        
        # Concept level
        concept = nodes.get('Concept')
        if concept:
            hierarchy['concept'] = concept
            path_parts.append(f"Concept: {concept.get('name', concept.get('text', 'Unknown'))}")
        
        # Reverse the path to go from top to bottom
        hierarchy['path'] = list(reversed(path_parts))
        hierarchy['full_path'] = ' → '.join(reversed(path_parts))
        
        return hierarchy

    def close(self) -> None:
        """Close the database connection."""
        if self.driver:
            self.driver.close()
            self.driver = None
            self.logger.info("Neo4j connection closed")
