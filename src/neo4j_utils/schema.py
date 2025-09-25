"""
Neo4j Schema Setup Module for OpenStax Knowledge Graph

This module contains the Neo4jSchemaSetup class for managing database schema
including constraints and indexes for the OpenStax textbook knowledge graph
with dual labeling schema.

Features:
- Database constraints creation
- Performance indexes creation
- Schema verification
- Error handling and reporting
"""

import json
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError


class Neo4jSchemaSetup:
    """Neo4j database schema setup manager."""
    
    def __init__(self, uri: str = "bolt://localhost:7687", 
                 username: str = "", 
                 password: str = "",
                 database: str = "neo4j"):
        """
        Initialize the Neo4j schema setup manager.
        
        Args:
            uri: Neo4j connection URI
            username: Neo4j username (optional for no-auth)
            password: Neo4j password (optional for no-auth)
            database: Database name to use
        """
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database
        self.driver = None
    
    def check_neo4j_connection(self) -> bool:
        """Check if Neo4j is installed and accessible."""
        try:
            self._connect()
            print(f"Neo4j is running and accessible at {self.uri}")
            return True
        except ServiceUnavailable:
            print(f"Neo4j is not running or not accessible")
            print(f"   Expected at: {self.uri}")
            print("   Please start your Neo4j Docker container or local instance")
            return False
        except AuthError:
            print("Authentication failed")
            print(f"   Username: {self.username}")
            print("   Check your Neo4j credentials or try without authentication")
            return False
        except Exception as e:
            print(f"Error connecting to Neo4j: {e}")
            return False
    
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
    
    def setup_constraints(self) -> bool:
        """Set up database constraints."""
        try:
            print("Creating constraints...")
            
            with self.driver.session(database=self.database) as session:
                constraints = self._get_constraints()
                success_count = 0
                
                for constraint in constraints:
                    try:
                        session.run(constraint)
                        print(f"   Created constraint: {constraint[:60]}...")
                        success_count += 1
                    except Exception as e:
                        print(f"   Warning creating constraint: {e}")
                
                print(f"Constraints setup completed: {success_count}/{len(constraints)} created")
                return True
                
        except Exception as e:
            print(f"Error setting up constraints: {e}")
            return False
    
    def setup_indexes(self) -> bool:
        """Set up performance indexes."""
        try:
            print("Creating indexes...")
            
            with self.driver.session(database=self.database) as session:
                indexes = self._get_indexes()
                success_count = 0
                
                for index in indexes:
                    try:
                        session.run(index)
                        print(f"   Created index: {index[:60]}...")
                        success_count += 1
                    except Exception as e:
                        print(f"   Warning creating index: {e}")
                
                print(f"Indexes setup completed: {success_count}/{len(indexes)} created")
                return True
                
        except Exception as e:
            print(f"Error setting up indexes: {e}")
            return False
    
    def setup_relationships(self) -> bool:
        """Set up relationship hierarchies between nodes."""
        try:
            print("Creating relationship hierarchies...")
            
            with self.driver.session(database=self.database) as session:
                relationships = self._get_relationships()
                success_count = 0
                
                for relationship in relationships:
                    try:
                        session.run(relationship)
                        print(f"   Created relationship: {relationship[:60]}...")
                        success_count += 1
                    except Exception as e:
                        print(f"   Warning creating relationship: {e}")
                
                print(f"Relationships setup completed: {success_count}/{len(relationships)} created")
                return True
                
        except Exception as e:
            print(f"Error setting up relationships: {e}")
            return False
    
    def delete_schema(self) -> bool:
        """Delete all constraints and indexes from the database."""
        try:
            print("Deleting schema (constraints and indexes)...")
            
            # Ensure connection is established
            if self.driver is None:
                self._connect()
            
            with self.driver.session(database=self.database) as session:
                # Delete all constraints
                constraints_result = session.run("SHOW CONSTRAINTS")
                constraints = [record["name"] for record in constraints_result]
                
                constraints_deleted = 0
                for constraint_name in constraints:
                    try:
                        session.run(f"DROP CONSTRAINT {constraint_name}")
                        print(f"   Dropped constraint: {constraint_name}")
                        constraints_deleted += 1
                    except Exception as e:
                        print(f"   Warning dropping constraint {constraint_name}: {e}")
                
                # Delete all indexes
                indexes_result = session.run("SHOW INDEXES")
                indexes = [record["name"] for record in indexes_result]
                
                indexes_deleted = 0
                for index_name in indexes:
                    try:
                        session.run(f"DROP INDEX {index_name}")
                        print(f"   Dropped index: {index_name}")
                        indexes_deleted += 1
                    except Exception as e:
                        print(f"   Warning dropping index {index_name}: {e}")
                
                print(f"Schema deletion completed:")
                print(f"   Constraints deleted: {constraints_deleted}/{len(constraints)}")
                print(f"   Indexes deleted: {indexes_deleted}/{len(indexes)}")
                return True
                
        except Exception as e:
            print(f"Error deleting schema: {e}")
            return False

    def clear_database(self) -> bool:
        """Clear all nodes and relationships from the database."""
        try:
            print("Clearing all data from database...")
            
            # Ensure connection is established
            if self.driver is None:
                self._connect()
            
            with self.driver.session(database=self.database) as session:
                # Get counts before deletion
                node_count_result = session.run("MATCH (n) RETURN count(n) as count")
                node_count = node_count_result.single()["count"]
                
                rel_count_result = session.run("MATCH ()-[r]->() RETURN count(r) as count")
                rel_count = rel_count_result.single()["count"]
                
                # Delete all nodes and relationships
                session.run("MATCH (n) DETACH DELETE n")
                
                print(f"Database cleared:")
                print(f"   Nodes deleted: {node_count}")
                print(f"   Relationships deleted: {rel_count}")
                return True
                
        except Exception as e:
            print(f"Error clearing database: {e}")
            return False

    def reset_database(self) -> bool:
        """Reset the entire database (clear data + delete schema)."""
        try:
            print("Resetting entire database...")
            
            # Clear all data first
            if not self.clear_database():
                return False
            
            # Then delete schema
            if not self.delete_schema():
                return False
            
            print("Database reset completed successfully!")
            return True
            
        except Exception as e:
            print(f"Error resetting database: {e}")
            return False
    
    def _get_constraints(self) -> List[str]:
        """Get Cypher scripts for creating database constraints."""
        return [
            # Unique constraints for identifiers
            "CREATE CONSTRAINT book_id_unique IF NOT EXISTS FOR (b:Book) REQUIRE b.book_id IS UNIQUE",
            "CREATE CONSTRAINT chapter_id_unique IF NOT EXISTS FOR (c:Chapter) REQUIRE c.chapter_id IS UNIQUE",
            "CREATE CONSTRAINT subchapter_id_unique IF NOT EXISTS FOR (sc:Subchapter) REQUIRE sc.subchapter_id IS UNIQUE",
            "CREATE CONSTRAINT document_id_unique IF NOT EXISTS FOR (d:Document) REQUIRE d.document_id IS UNIQUE",
            "CREATE CONSTRAINT section_id_unique IF NOT EXISTS FOR (s:Section) REQUIRE s.section_id IS UNIQUE",
            "CREATE CONSTRAINT subsection_id_unique IF NOT EXISTS FOR (ss:Subsection) REQUIRE ss.subsection_id IS UNIQUE",
            "CREATE CONSTRAINT paragraph_id_unique IF NOT EXISTS FOR (p:Paragraph) REQUIRE p.paragraph_id IS UNIQUE",
            "CREATE CONSTRAINT sentence_id_unique IF NOT EXISTS FOR (s:Sentence) REQUIRE s.sentence_id IS UNIQUE",
            "CREATE CONSTRAINT concept_id_unique IF NOT EXISTS FOR (c:Concept) REQUIRE c.concept_id IS UNIQUE"
        ]
    
    def _get_indexes(self) -> List[str]:
        """Get Cypher scripts for creating performance indexes.

        Indexes are provided for properties that are not already covered by unique constraints.
        """
        return [
            # Title indexes for search
            "CREATE INDEX book_title_index IF NOT EXISTS FOR (b:Book) ON (b.title)",
            "CREATE INDEX chapter_title_index IF NOT EXISTS FOR (c:Chapter) ON (c.title)",
            "CREATE INDEX subchapter_title_index IF NOT EXISTS FOR (sc:Subchapter) ON (sc.title)",
            "CREATE INDEX document_title_index IF NOT EXISTS FOR (d:Document) ON (d.title)",
            "CREATE INDEX section_title_index IF NOT EXISTS FOR (s:Section) ON (s.title)",
            "CREATE INDEX subsection_title_index IF NOT EXISTS FOR (ss:Subsection) ON (ss.title)",

            # Content text indexes
            "CREATE INDEX paragraph_text_index IF NOT EXISTS FOR (p:Paragraph) ON (p.text)",
            "CREATE INDEX sentence_text_index IF NOT EXISTS FOR (s:Sentence) ON (s.text)",
            "CREATE INDEX concept_text_index IF NOT EXISTS FOR (c:Concept) ON (c.text)",

            #keep the content ids 
            "CREATE INDEX document_id_index IF NOT EXISTS FOR (d:Document) ON (d.document_id)",
            "CREATE INDEX chapter_id_index IF NOT EXISTS FOR (c:Chapter) ON (c.chapter_id)",
            "CREATE INDEX subchapter_id_index IF NOT EXISTS FOR (sc:Subchapter) ON (sc.subchapter_id)",
            "CREATE INDEX section_id_index IF NOT EXISTS FOR (s:Section) ON (s.section_id)",
            "CREATE INDEX subsection_id_index IF NOT EXISTS FOR (ss:Subsection) ON (ss.subsection_id)",
            "CREATE INDEX paragraph_id_index IF NOT EXISTS FOR (p:Paragraph) ON (p.paragraph_id)",
            "CREATE INDEX sentence_id_index IF NOT EXISTS FOR (s:Sentence) ON (s.sentence_id)",
            "CREATE INDEX concept_id_index IF NOT EXISTS FOR (c:Concept) ON (c.concept_id)",

            # Concept/semantic indexes
            "CREATE INDEX concept_wikidata_id_index IF NOT EXISTS FOR (c:Concept) ON (c.wikidata_id)",
            "CREATE INDEX concept_wikidata_name_index IF NOT EXISTS FOR (c:Concept) ON (c.wikidata_name)",

            # Lens-based indexes for improved query performance
            "CREATE INDEX book_lens_index IF NOT EXISTS FOR (b:Book) ON (b.lens)",
            "CREATE INDEX chapter_lens_index IF NOT EXISTS FOR (c:Chapter) ON (c.lens)", 
            "CREATE INDEX subchapter_lens_index IF NOT EXISTS FOR (sc:Subchapter) ON (sc.lens)",
            "CREATE INDEX document_lens_index IF NOT EXISTS FOR (d:Document) ON (d.lens)",
            "CREATE INDEX section_lens_index IF NOT EXISTS FOR (s:Section) ON (s.lens)",
            "CREATE INDEX subsection_lens_index IF NOT EXISTS FOR (ss:Subsection) ON (ss.lens)",
            "CREATE INDEX paragraph_lens_index IF NOT EXISTS FOR (p:Paragraph) ON (p.lens)",
            "CREATE INDEX sentence_lens_index IF NOT EXISTS FOR (s:Sentence) ON (s.lens)",  
             "CREATE INDEX concept_lens_index IF NOT EXISTS FOR (c:Concept) ON (c.lens)"
        ]

    def _get_relationships(self) -> List[str]:
        """Get Cypher scripts for creating relationship hierarchies."""
        return [
            # Top-down CONTAINS relationships
            "MATCH (b:Book), (c:Chapter) WHERE c.book_id = b.book_id MERGE (b)-[:BOOK_CONTAINS_CHAPTER]->(c)",
            "MATCH (c:Chapter), (sc:Subchapter) WHERE sc.chapter_id = c.chapter_id MERGE (c)-[:CHAPTER_CONTAINS_SUBCHAPTER]->(sc)",
            "MATCH (c:Chapter), (d:Document) WHERE d.chapter_id = c.chapter_id MERGE (c)-[:CHAPTER_CONTAINS_DOCUMENT]->(d)",
            "MATCH (sc:Subchapter), (d:Document) WHERE d.subchapter_id = sc.subchapter_id MERGE (sc)-[:SUBCHAPTER_CONTAINS_DOCUMENT]->(d)",
            "MATCH (d:Document), (s:Section) WHERE s.document_id = d.document_id MERGE (d)-[:DOCUMENT_CONTAINS_SECTION]->(s)",
            "MATCH (d:Document), (p:Paragraph) WHERE p.document_id = d.document_id MERGE (d)-[:DOCUMENT_CONTAINS_PARAGRAPH]->(p)",
            "MATCH (s:Section), (ss:Subsection) WHERE ss.section_id = s.section_id MERGE (s)-[:SECTION_CONTAINS_SUBSECTION]->(ss)",
            "MATCH (s:Section), (p:Paragraph) WHERE p.section_id = s.section_id MERGE (s)-[:SECTION_CONTAINS_PARAGRAPH]->(p)",
            "MATCH (ss:Subsection), (p:Paragraph) WHERE p.subsection_id = ss.subsection_id MERGE (ss)-[:SUBSECTION_CONTAINS_PARAGRAPH]->(p)",
            "MATCH (p:Paragraph), (sent:Sentence) WHERE sent.paragraph_id = p.paragraph_id MERGE (p)-[:PARAGRAPH_CONTAINS_SENTENCE]->(sent)",
            # Note: SENTENCE_CONTAINS_CONCEPT relationships are created during data import
            # when concepts are extracted from sentences, not through schema setup
            
            # Bottom-up BELONGS_TO relationships
            # Note: CONCEPT_BELONGS_TO_SENTENCE relationships are created during data import
            # when concepts are extracted from sentences, not through schema setup
            "MATCH (sent:Sentence), (p:Paragraph) WHERE sent.paragraph_id = p.paragraph_id MERGE (sent)-[:SENTENCE_BELONGS_TO_PARAGRAPH]->(p)",
            "MATCH (p:Paragraph), (ss:Subsection) WHERE p.subsection_id = ss.subsection_id MERGE (p)-[:PARAGRAPH_BELONGS_TO_SUBSECTION]->(ss)",
            # Note: PARAGRAPH_BELONGS_TO_SECTION and PARAGRAPH_BELONGS_TO_DOCUMENT relationships
            # are created during data import based on the hierarchical structure,
            # not through schema setup with field matching
            "MATCH (ss:Subsection), (s:Section) WHERE ss.section_id = s.section_id MERGE (ss)-[:SUBSECTION_BELONGS_TO_SECTION]->(s)",
            "MATCH (s:Section), (d:Document) WHERE s.document_id = d.document_id MERGE (s)-[:SECTION_BELONGS_TO_DOCUMENT]->(d)",
            # Note: DOCUMENT_BELONGS_TO_SUBCHAPTER and DOCUMENT_BELONGS_TO_CHAPTER relationships
            # are created during data import based on the hierarchical structure,
            # not through schema setup with field matching
            "MATCH (sc:Subchapter), (c:Chapter) WHERE sc.chapter_id = c.chapter_id MERGE (sc)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c)",
            "MATCH (c:Chapter), (b:Book) WHERE c.book_id = b.book_id MERGE (c)-[:CHAPTER_BELONGS_TO_BOOK]->(b)"
        ]

    def verify_schema(self) -> Dict[str, Any]:
        """Verify that the schema was created successfully."""
        try:
            with self.driver.session(database=self.database) as session:
                # Check constraints
                constraints_result = session.run("SHOW CONSTRAINTS")
                constraints = [record["name"] for record in constraints_result]
                
                # Check indexes
                indexes_result = session.run("SHOW INDEXES")
                indexes = [record["name"] for record in indexes_result]
                
                # Check node counts
                node_counts_result = session.run("MATCH (n) RETURN labels(n) as labels, count(n) as count ORDER BY count DESC")
                node_counts = [{"labels": record["labels"], "count": record["count"]} for record in node_counts_result]
                
                verification = {
                    "constraints": constraints,
                    "indexes": indexes,
                    "node_counts": node_counts,
                    "constraint_count": len(constraints),
                    "index_count": len(indexes)
                }
                
                print(f"Schema verification completed:")
                print(f"   Constraints: {len(constraints)}")
                print(f"   Indexes: {len(indexes)}")
                print(f"   Node types: {len(node_counts)}")
                
                return verification
                
        except Exception as e:
            print(f"Error verifying schema: {e}")
            return {}
    
    def show_schema_info(self) -> None:
        """Display detailed schema information."""
        try:
            with self.driver.session(database=self.database) as session:
                print("\nCONSTRAINTS:")
                constraints_result = session.run("SHOW CONSTRAINTS")
                for record in constraints_result:
                    print(f"   {record['name']}: {record['description']}")
                
                print("\nINDEXES:")
                indexes_result = session.run("SHOW INDEXES")
                for record in indexes_result:
                    print(f"   {record['name']}: {record['labelsOrTypes']} on {record['properties']}")
                
                print("\nNODE COUNTS:")
                node_counts_result = session.run("MATCH (n) RETURN labels(n) as labels, count(n) as count ORDER BY count DESC")
                for record in node_counts_result:
                    labels = record['labels']
                    count = record['count']
                    print(f"   {labels}: {count}")
                
        except Exception as e:
            print(f"Error showing schema info: {e}")
    
    def create_sample_data(self) -> bool:
        """Create sample nodes and relationships for testing."""
        try:
            from .nodes import Neo4jNodeCreator
            from .relationships import Neo4jRelationshipCreator
            
            # Initialize creators
            node_creator = Neo4jNodeCreator(self.uri, self.username, self.password, self.database)
            rel_creator = Neo4jRelationshipCreator(self.uri, self.username, self.password, self.database)
            
            # Ensure connections are established
            node_creator._connect()
            rel_creator._connect()
            
            # Sample data
            current_time = datetime.now().isoformat()
            
            # Create sample book
            book_data = {
                "book_id": "bio-2e-sample",
                "title": "Biology 2e Sample",
                "uuid": "book-uuid-001",
                "lens": "biology",
                "created_at": current_time
            }
            if not node_creator.create_book(book_data):
                return False
            
            # Create sample chapter
            chapter_data = {
                "chapter_id": "ch01-sample",
                "book_id": "bio-2e-sample",
                "title": "Introduction to Biology",
                "uuid": "chapter-uuid-001",
                "order": 1,
                "lens": "biology",
                "created_at": current_time
            }
            if not node_creator.create_chapter(chapter_data):
                return False
            
            # Create sample subchapter
            subchapter_data = {
                "subchapter_id": "sc01-01-sample",
                "chapter_id": "ch01-sample",
                "title": "What is Biology?",
                "uuid": "subchapter-uuid-001",
                "order": 1,
                "lens": "biology",
                "created_at": current_time
            }
            if not node_creator.create_subchapter(subchapter_data):
                return False
            
            # Create sample document
            document_data = {
                "document_id": "doc01-sample",
                "book_id": "bio-2e-sample",
                "title": "Biology Introduction Document",
                "uuid": "document-uuid-001",
                "lens": "biology",
                "created_at": current_time
            }
            if not node_creator.create_document(document_data):
                return False
            
            # Create sample section
            section_data = {
                "section_id": "sec01-sample",
                "subchapter_id": "sc01-01-sample",
                "document_id": "doc01-sample",
                "title": "Definition of Biology",
                "uuid": "section-uuid-001",
                "order": 1,
                "lens": "biology",
                "created_at": current_time
            }
            if not node_creator.create_section(section_data):
                return False
            
            # Create sample subsection
            subsection_data = {
                "subsection_id": "subsec01-sample",
                "section_id": "sec01-sample",
                "title": "Scientific Study of Life",
                "uuid": "subsection-uuid-001",
                "order": 1,
                "lens": "biology",
                "created_at": current_time
            }
            if not node_creator.create_subsection(subsection_data):
                return False
            
            # Create sample paragraph
            paragraph_data = {
                "paragraph_id": "para01-sample",
                "subsection_id": "subsec01-sample",
                "text": "Biology is the scientific study of life. It encompasses all living organisms and their interactions with the environment.",
                "uuid": "paragraph-uuid-001",
                "order": 1,
                "lens": "biology",
                "created_at": current_time
            }
            if not node_creator.create_paragraph(paragraph_data):
                return False
            
            # Create sample sentence
            sentence_data = {
                "sentence_id": "sent01-sample",
                "paragraph_id": "para01-sample",
                "text": "Biology is the scientific study of life.",
                "uuid": "sentence-uuid-001",
                "order": 1,
                "lens": "biology",
                "created_at": current_time
            }
            if not node_creator.create_sentence(sentence_data):
                return False
            
            # Create sample concept
            concept_data = {
                "concept_id": "concept01-sample",
                "text": "biology",
                "wikidata_id": "Q420",
                "wikidata_name": "Biology",
                "uuid": "concept-uuid-001",
                "lens": "biology",
                "created_at": current_time
            }
            if not node_creator.create_concept(concept_data):
                return False
            
            # Create relationships
            print("Creating relationships...")
            
            # Top-down relationships
            rel_creator.create_book_contains_chapter_relationship("bio-2e-sample", "ch01-sample")
            rel_creator.create_chapter_contains_subchapter_relationship("ch01-sample", "sc01-01-sample")
            rel_creator.create_chapter_contains_document_relationship("ch01-sample", "doc01-sample")
            rel_creator.create_subchapter_contains_document_relationship("sc01-01-sample", "doc01-sample")
            rel_creator.create_document_contains_section_relationship("doc01-sample", "sec01-sample")
            rel_creator.create_section_contains_subsection_relationship("sec01-sample", "subsec01-sample")
            rel_creator.create_subsection_contains_paragraph_relationship("subsec01-sample", "para01-sample")
            rel_creator.create_paragraph_contains_sentence_relationship("para01-sample", "sent01-sample")
            rel_creator.create_sentence_contains_concept_relationship("sent01-sample", "concept01-sample")
            
            # Bottom-up relationships
            rel_creator.create_concept_belongs_to_sentence_relationship("concept01-sample", "sent01-sample")
            rel_creator.create_sentence_belongs_paragraph_relationship("sent01-sample", "para01-sample")
            rel_creator.create_paragraph_belongs_to_subsection_relationship("para01-sample", "subsec01-sample")
            rel_creator.create_subsection_belongs_to_section_relationship("subsec01-sample", "sec01-sample")
            rel_creator.create_section_belongs_to_document_relationship("sec01-sample", "doc01-sample")
            rel_creator.create_document_belongs_to_subchapter_relationship("doc01-sample", "sc01-01-sample")
            rel_creator.create_document_belongs_to_chapter_relationship("doc01-sample", "ch01-sample")
            rel_creator.create_subchapter_belongs_to_chapter_relationship("sc01-01-sample", "ch01-sample")
            rel_creator.create_chapter_belongs_to_book_relationship("ch01-sample", "bio-2e-sample")
            
            # Close connections
            node_creator.close()
            rel_creator.close()
            
            return True
            
        except Exception as e:
            print(f"Error creating sample data: {e}")
            return False
    
    def close(self) -> None:
        """Close the database connection."""
        if self.driver:
            self.driver.close()
