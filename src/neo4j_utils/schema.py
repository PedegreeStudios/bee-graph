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
    
    def close(self) -> None:
        """Close the database connection."""
        if self.driver:
            self.driver.close()
