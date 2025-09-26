"""
Neo4j Node Creation Module for OpenStax Knowledge Graph

This module contains utilities for creating nodes in the Neo4j database
for the OpenStax textbook knowledge graph with dual labeling schema.

Features:
- Node creation for all entity types
- Batch node creation
- Node validation and verification
- Error handling and reporting
"""

from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from config.config_loader import get_neo4j_connection_params


class Neo4jNodeCreator:
    """Neo4j node creation manager."""
    
    def __init__(self, uri: str = None, 
                 username: str = None, 
                 password: str = None,
                 database: str = None):
        """
        Initialize the Neo4j node creator.
        
        Args:
            uri: Neo4j connection URI (if None, loads from config)
            username: Neo4j username (if None, loads from config)
            password: Neo4j password (if None, loads from config)
            database: Neo4j database name (if None, loads from config)
        """
        # Load from config if parameters not provided
        if uri is None or username is None or password is None or database is None:
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
    
    def _connect(self) -> None:
        """Establish connection to Neo4j database."""
        if self.driver is None:
            # Use no-auth if no username/password provided
            if not self.username and not self.password:
                self.driver = GraphDatabase.driver(self.uri)
            else:
                self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        
        # Test the connection
        with self.driver.session(database=self.database) as session:
            session.run("RETURN 1")
    
    def create_book(self, book_data: Dict[str, Any]) -> bool:
        """Create a Book node."""
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                CREATE (b:Book {
                    book_id: $book_id,
                    title: $title,
                    uuid: $uuid,
                    lens: $lens,
                    created_at: $created_at
                })
                """
                session.run(query, book_data)
                print(f"Created Book: {book_data.get('title', 'Unknown')}")
                return True
        except Exception as e:
            print(f"Error creating Book: {e}")
            return False
    
    def create_chapter(self, chapter_data: Dict[str, Any]) -> bool:
        """Create a Chapter node."""
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                CREATE (c:Chapter {
                    chapter_id: $chapter_id,
                    book_id: $book_id,
                    title: $title,
                    uuid: $uuid,
                    order: $order,
                    lens: $lens,
                    created_at: $created_at
                })
                """
                session.run(query, chapter_data)
                print(f"Created Chapter: {chapter_data.get('title', 'Unknown')}")
                return True
        except Exception as e:
            print(f"Error creating Chapter: {e}")
            return False
    
    def create_subchapter(self, subchapter_data: Dict[str, Any]) -> bool:
        """Create a Subchapter node."""
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                CREATE (sc:Subchapter {
                    subchapter_id: $subchapter_id,
                    chapter_id: $chapter_id,
                    title: $title,
                    uuid: $uuid,
                    order: $order,
                    lens: $lens,
                    created_at: $created_at
                })
                """
                session.run(query, subchapter_data)
                print(f"Created Subchapter: {subchapter_data.get('title', 'Unknown')}")
                return True
        except Exception as e:
            print(f"Error creating Subchapter: {e}")
            return False
    
    def create_document(self, document_data: Dict[str, Any]) -> bool:
        """Create a Document node."""
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                CREATE (d:Document {
                    document_id: $document_id,
                    book_id: $book_id,
                    title: $title,
                    uuid: $uuid,
                    lens: $lens,
                    created_at: $created_at
                })
                """
                session.run(query, document_data)
                print(f"Created Document: {document_data.get('title', 'Unknown')}")
                return True
        except Exception as e:
            print(f"Error creating Document: {e}")
            return False
    
    def create_section(self, section_data: Dict[str, Any]) -> bool:
        """Create a Section node."""
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                CREATE (s:Section {
                    section_id: $section_id,
                    subchapter_id: $subchapter_id,
                    document_id: $document_id,
                    title: $title,
                    uuid: $uuid,
                    order: $order,
                    lens: $lens,
                    created_at: $created_at
                })
                """
                session.run(query, section_data)
                print(f"Created Section: {section_data.get('title', 'Unknown')}")
                return True
        except Exception as e:
            print(f"Error creating Section: {e}")
            return False
    
    def create_subsection(self, subsection_data: Dict[str, Any]) -> bool:
        """Create a Subsection node."""
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                CREATE (ss:Subsection {
                    subsection_id: $subsection_id,
                    section_id: $section_id,
                    title: $title,
                    uuid: $uuid,
                    order: $order,
                    lens: $lens,
                    created_at: $created_at
                })
                """
                session.run(query, subsection_data)
                print(f"Created Subsection: {subsection_data.get('title', 'Unknown')}")
                return True
        except Exception as e:
            print(f"Error creating Subsection: {e}")
            return False
    
    def create_paragraph(self, paragraph_data: Dict[str, Any]) -> bool:
        """Create a Paragraph node."""
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                CREATE (p:Paragraph {
                    paragraph_id: $paragraph_id,
                    subsection_id: $subsection_id,
                    text: $text,
                    uuid: $uuid,
                    order: $order,
                    lens: $lens,
                    created_at: $created_at
                })
                """
                session.run(query, paragraph_data)
                print(f"Created Paragraph: {paragraph_data.get('text', 'Unknown')[:50]}...")
                return True
        except Exception as e:
            print(f"Error creating Paragraph: {e}")
            return False
    
    def create_sentence(self, sentence_data: Dict[str, Any]) -> bool:
        """Create a Sentence node."""
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                CREATE (sent:Sentence {
                    sentence_id: $sentence_id,
                    paragraph_id: $paragraph_id,
                    text: $text,
                    uuid: $uuid,
                    order: $order,
                    lens: $lens,
                    created_at: $created_at
                })
                """
                session.run(query, sentence_data)
                print(f"Created Sentence: {sentence_data.get('text', 'Unknown')[:50]}...")
                return True
        except Exception as e:
            print(f"Error creating Sentence: {e}")
            return False
    
    def create_concept(self, concept_data: Dict[str, Any]) -> bool:
        """Create a Concept node."""
        try:
            with self.driver.session(database=self.database) as session:
                query = """
                CREATE (c:Concept {
                    concept_id: $concept_id,
                    text: $text,
                    wikidata_id: $wikidata_id,
                    wikidata_name: $wikidata_name,
                    uuid: $uuid,
                    lens: $lens,
                    created_at: $created_at
                })
                """
                session.run(query, concept_data)
                print(f"Created Concept: {concept_data.get('text', 'Unknown')}")
                return True
        except Exception as e:
            print(f"Error creating Concept: {e}")
            return False
    
    def create_nodes_batch(self, nodes_data: List[Dict[str, Any]], node_type: str) -> int:
        """Create multiple nodes of the same type in batch."""
        try:
            success_count = 0
            
            with self.driver.session(database=self.database) as session:
                for node_data in nodes_data:
                    try:
                        if node_type == "Book":
                            if self.create_book(node_data):
                                success_count += 1
                        elif node_type == "Chapter":
                            if self.create_chapter(node_data):
                                success_count += 1
                        elif node_type == "Subchapter":
                            if self.create_subchapter(node_data):
                                success_count += 1
                        elif node_type == "Document":
                            if self.create_document(node_data):
                                success_count += 1
                        elif node_type == "Section":
                            if self.create_section(node_data):
                                success_count += 1
                        elif node_type == "Subsection":
                            if self.create_subsection(node_data):
                                success_count += 1
                        elif node_type == "Paragraph":
                            if self.create_paragraph(node_data):
                                success_count += 1
                        elif node_type == "Sentence":
                            if self.create_sentence(node_data):
                                success_count += 1
                        elif node_type == "Concept":
                            if self.create_concept(node_data):
                                success_count += 1
                    except Exception as e:
                        print(f"Warning creating {node_type}: {e}")
                        continue
            
            print(f"Batch creation completed: {success_count}/{len(nodes_data)} {node_type} nodes created")
            return success_count
            
        except Exception as e:
            print(f"Error in batch creation: {e}")
            return 0
    
    def get_node_count(self, node_type: str) -> int:
        """Get count of nodes of a specific type."""
        try:
            with self.driver.session(database=self.database) as session:
                result = session.run(f"MATCH (n:{node_type}) RETURN count(n) as count")
                return result.single()["count"]
        except Exception as e:
            print(f"Error getting node count: {e}")
            return 0
    
    def close(self) -> None:
        """Close the database connection."""
        if self.driver:
            self.driver.close()
