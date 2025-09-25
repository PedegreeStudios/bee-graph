"""
Neo4j Relationship Creation Module for OpenStax Knowledge Graph

This module contains utilities for creating relationships between nodes
in the Neo4j database for the OpenStax textbook knowledge graph.

Features:
- Relationship creation for all entity types
- Batch relationship creation
- Relationship validation and verification
- Error handling and reporting
"""

from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError


class Neo4jRelationshipCreator:
    """Neo4j relationship creation manager."""
    
    def __init__(self, uri: str = "bolt://localhost:7687", 
                 username: str = "", 
                 password: str = "",
                 database: str = "neo4j"):
        """
        Initialize the Neo4j relationship creator.
        
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
    
    
    #create the top down relationships
    def create_book_contains_chapter_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a BOOK_CONTAINS_CHAPTER relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (b:Book {book_id: $source_id}), (c:Chapter {chapter_id: $target_id})
                MERGE (b)-[:BOOK_CONTAINS_CHAPTER]->(c)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created BOOK_CONTAINS_CHAPTER: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating BOOK_CONTAINS_CHAPTER relationship: {e}")
            return False

    def create_chapter_contains_subchapter_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a CHAPTER_CONTAINS_SUBCHAPTER relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (c:Chapter {chapter_id: $source_id}), (sc:Subchapter {subchapter_id: $target_id})
                MERGE (c)-[:CHAPTER_CONTAINS_SUBCHAPTER]->(sc)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created CHAPTER_CONTAINS_SUBCHAPTER: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating CHAPTER_CONTAINS_SUBCHAPTER relationship: {e}")
            return False

    def create_chapter_contains_document_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a CHAPTER_CONTAINS_DOCUMENT relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (c:Chapter {chapter_id: $source_id}), (d:Document {document_id: $target_id})
                MERGE (c)-[:CHAPTER_CONTAINS_DOCUMENT]->(d)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created CHAPTER_CONTAINS_DOCUMENT: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating CHAPTER_CONTAINS_DOCUMENT relationship: {e}")
            return False

    def create_subchapter_contains_document_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a SUBCHAPTER_CONTAINS_DOCUMENT relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (sc:Subchapter {subchapter_id: $source_id}), (d:Document {document_id: $target_id})
                MERGE (sc)-[:SUBCHAPTER_CONTAINS_DOCUMENT]->(d)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created SUBCHAPTER_CONTAINS_DOCUMENT: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating SUBCHAPTER_CONTAINS_DOCUMENT relationship: {e}")
            return False

    def create_document_contains_section_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a DOCUMENT_CONTAINS_SECTION relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (d:Document {document_id: $source_id}), (s:Section {section_id: $target_id})
                MERGE (d)-[:DOCUMENT_CONTAINS_SECTION]->(s)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created DOCUMENT_CONTAINS_SECTION: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating DOCUMENT_CONTAINS_SECTION relationship: {e}")
            return False

    def create_document_contains_paragraph_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a DOCUMENT_CONTAINS_PARAGRAPH relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (d:Document {document_id: $source_id}), (p:Paragraph {paragraph_id: $target_id})
                MERGE (d)-[:DOCUMENT_CONTAINS_PARAGRAPH]->(p)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created DOCUMENT_CONTAINS_PARAGRAPH: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating DOCUMENT_CONTAINS_PARAGRAPH relationship: {e}")
            return False

    def create_section_contains_subsection_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a SECTION_CONTAINS_SUBSECTION relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (s:Section {section_id: $source_id}), (ss:Subsection {subsection_id: $target_id})
                MERGE (s)-[:SECTION_CONTAINS_SUBSECTION]->(ss)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created SECTION_CONTAINS_SUBSECTION: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating SECTION_CONTAINS_SUBSECTION relationship: {e}")
            return False

    def create_section_contains_paragraph_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a SECTION_CONTAINS_PARAGRAPH relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (s:Section {section_id: $source_id}), (p:Paragraph {paragraph_id: $target_id})
                MERGE (s)-[:SECTION_CONTAINS_PARAGRAPH]->(p)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created SECTION_CONTAINS_PARAGRAPH: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating SECTION_CONTAINS_PARAGRAPH relationship: {e}")
            return False

    def create_subsection_contains_paragraph_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a SUBSECTION_CONTAINS_PARAGRAPH relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (ss:Subsection {subsection_id: $source_id}), (p:Paragraph {paragraph_id: $target_id})
                MERGE (ss)-[:SUBSECTION_CONTAINS_PARAGRAPH]->(p)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created SUBSECTION_CONTAINS_PARAGRAPH: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating SUBSECTION_CONTAINS_PARAGRAPH relationship: {e}")
            return False

    def create_paragraph_contains_sentence_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a PARAGRAPH_CONTAINS_SENTENCE relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (p:Paragraph {paragraph_id: $source_id}), (sent:Sentence {sentence_id: $target_id})
                MERGE (p)-[:PARAGRAPH_CONTAINS_SENTENCE]->(sent)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created PARAGRAPH_CONTAINS_SENTENCE: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating PARAGRAPH_CONTAINS_SENTENCE relationship: {e}")
            return False

    def create_sentence_contains_concept_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a SENTENCE_CONTAINS_CONCEPT relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (sent:Sentence {sentence_id: $source_id}), (c:Concept {concept_id: $target_id})
                MERGE (sent)-[:SENTENCE_CONTAINS_CONCEPT]->(c)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created SENTENCE_CONTAINS_CONCEPT: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating SENTENCE_CONTAINS_CONCEPT relationship: {e}")
            return False

    #create the bottom up relationships
    def create_concept_belongs_to_sentence_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a CONCEPT_BELONGS_TO_SENTENCE relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (c:Concept {concept_id: $source_id}), (sent:Sentence {sentence_id: $target_id})
                MERGE (c)-[:CONCEPT_BELONGS_TO_SENTENCE]->(sent)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created CONCEPT_BELONGS_TO_SENTENCE: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating CONCEPT_BELONGS_TO_SENTENCE relationship: {e}")
            return False

    def create_sentence_belongs_paragraph_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a SENTENCE_BELONGS_TO_PARAGRAPH relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (sent:Sentence {sentence_id: $source_id}), (p:Paragraph {paragraph_id: $target_id})
                MERGE (sent)-[:SENTENCE_BELONGS_TO_PARAGRAPH]->(p)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created SENTENCE_BELONGS_TO_PARAGRAPH: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating SENTENCE_BELONGS_TO_PARAGRAPH relationship: {e}")
            return False
    
    def create_paragraph_belongs_to_subsection_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a PARAGRAPH_BELONGS_TO_SUBSECTION relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (p:Paragraph {paragraph_id: $source_id}), (ss:Subsection {subsection_id: $target_id})
                MERGE (p)-[:PARAGRAPH_BELONGS_TO_SUBSECTION]->(ss)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created PARAGRAPH_BELONGS_TO_SUBSECTION: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating PARAGRAPH_BELONGS_TO_SUBSECTION relationship: {e}")
            return False
    
    #paragraph belongs to section
    def create_paragraph_belongs_to_section_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a PARAGRAPH_BELONGS_TO_SECTION relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (p:Paragraph {paragraph_id: $source_id}), (s:Section {section_id: $target_id})
                MERGE (p)-[:PARAGRAPH_BELONGS_TO_SECTION]->(s)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created PARAGRAPH_BELONGS_TO_SECTION: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating PARAGRAPH_BELONGS_TO_SECTION relationship: {e}")
            return False
    
    def create_subsection_belongs_to_section_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a SUBSECTION_BELONGS_TO_SECTION relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (ss:Subsection {subsection_id: $source_id}), (s:Section {section_id: $target_id})
                MERGE (ss)-[:SUBSECTION_BELONGS_TO_SECTION]->(s)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created SUBSECTION_BELONGS_TO_SECTION: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating SUBSECTION_BELONGS_TO_SECTION relationship: {e}")
            return False

    #section belongs to document 
    def create_section_belongs_to_document_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a SECTION_BELONGS_TO_DOCUMENT relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (s:Section {section_id: $source_id}), (d:Document {document_id: $target_id})
                MERGE (s)-[:SECTION_BELONGS_TO_DOCUMENT]->(d)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created SECTION_BELONGS_TO_DOCUMENT: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating SECTION_BELONGS_TO_DOCUMENT relationship: {e}")
            return False
    
    #paragraph belongs to document 
    def create_paragraph_belongs_to_document_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a PARAGRAPH_BELONGS_TO_DOCUMENT relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (p:Paragraph {paragraph_id: $source_id}), (d:Document {document_id: $target_id})
                MERGE (p)-[:PARAGRAPH_BELONGS_TO_DOCUMENT]->(d)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created PARAGRAPH_BELONGS_TO_DOCUMENT: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating PARAGRAPH_BELONGS_TO_DOCUMENT relationship: {e}")
            return False

    #document belong to subchapter
    def create_document_belongs_to_subchapter_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a DOCUMENT_BELONGS_TO_SUBCHAPTER relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (d:Document {document_id: $source_id})
                MATCH (sc:Subchapter {subchapter_id: $target_id})
                MERGE (d)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created DOCUMENT_BELONGS_TO_SUBCHAPTER: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating DOCUMENT_BELONGS_TO_SUBCHAPTER relationship: {e}")
            return False
    
    
    #document belongs to chapter 
    def create_document_belongs_to_chapter_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a DOCUMENT_BELONGS_TO_CHAPTER relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (d:Document {document_id: $source_id})
                MATCH (c:Chapter {chapter_id: $target_id})
                MERGE (d)-[:DOCUMENT_BELONGS_TO_CHAPTER]->(c)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created DOCUMENT_BELONGS_TO_CHAPTER: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating DOCUMENT_BELONGS_TO_CHAPTER relationship: {e}")
            return False

    #subchapter belongs to chapter
    def create_subchapter_belongs_to_chapter_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a SUBCHAPTER_BELONGS_TO_CHAPTER relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (sc:Subchapter {subchapter_id: $source_id})
                MATCH (c:Chapter {chapter_id: $target_id})
                MERGE (sc)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created SUBCHAPTER_BELONGS_TO_CHAPTER: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating SUBCHAPTER_BELONGS_TO_CHAPTER relationship: {e}")
            return False
    
    #chapter belongs to book
    def create_chapter_belongs_to_book_relationship(self, source_id: str, target_id: str, **kwargs) -> bool:
        """Create a CHAPTER_BELONGS_TO_BOOK relationship."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = """
                MATCH (c:Chapter {chapter_id: $source_id})
                MATCH (b:Book {book_id: $target_id})
                MERGE (c)-[:CHAPTER_BELONGS_TO_BOOK]->(b)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created CHAPTER_BELONGS_TO_BOOK: {source_id} -> {target_id}")
                return True
        except Exception as e:
            print(f"Error creating CHAPTER_BELONGS_TO_BOOK relationship: {e}")
            return False
    
    def create_generic_contains_relationship(self, source_label: str, target_label: str, 
                                           source_id: str, target_id: str, **kwargs) -> bool:
        """Create a generic CONTAINS relationship between any two nodes."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = f"""
                MATCH (source:{source_label} {{{source_label.lower()}_id: $source_id}}), 
                      (target:{target_label} {{{target_label.lower()}_id: $target_id}})
                MERGE (source)-[:CONTAINS]->(target)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created CONTAINS: {source_label}({source_id}) -> {target_label}({target_id})")
                return True
        except Exception as e:
            print(f"Error creating CONTAINS relationship: {e}")
            return False

    def create_generic_belongs_to_relationship(self, source_label: str, target_label: str, 
                                             source_id: str, target_id: str, **kwargs) -> bool:
        """Create a generic BELONGS_TO relationship between any two nodes."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                query = f"""
                MATCH (source:{source_label} {{{source_label.lower()}_id: $source_id}}), 
                      (target:{target_label} {{{target_label.lower()}_id: $target_id}})
                MERGE (source)-[:BELONGS_TO]->(target)
                """
                session.run(query, {"source_id": source_id, "target_id": target_id, **kwargs})
                print(f"Created BELONGS_TO: {source_label}({source_id}) -> {target_label}({target_id})")
                return True
        except Exception as e:
            print(f"Error creating BELONGS_TO relationship: {e}")
            return False

    def find_contains_relationships(self, source_label: str = None, target_label: str = None, 
                                  source_id: str = None, target_id: str = None) -> List[Dict[str, Any]]:
        """Find CONTAINS relationships with optional filters."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                # Build dynamic query based on provided filters
                where_conditions = []
                params = {}
                
                if source_label:
                    where_conditions.append(f"source:{source_label}")
                if target_label:
                    where_conditions.append(f"target:{target_label}")
                if source_id:
                    where_conditions.append(f"source.{source_label.lower() if source_label else 'id'}_id = $source_id")
                    params["source_id"] = source_id
                if target_id:
                    where_conditions.append(f"target.{target_label.lower() if target_label else 'id'}_id = $target_id")
                    params["target_id"] = target_id
                
                where_clause = " AND ".join(where_conditions) if where_conditions else ""
                
                query = f"""
                MATCH (source)-[r:CONTAINS]->(target)
                {f"WHERE {where_clause}" if where_clause else ""}
                RETURN source, target, r, labels(source) as source_labels, labels(target) as target_labels
                """
                
                result = session.run(query, params)
                relationships = []
                for record in result:
                    relationships.append({
                        "source": dict(record["source"]),
                        "target": dict(record["target"]),
                        "relationship": dict(record["r"]),
                        "source_labels": record["source_labels"],
                        "target_labels": record["target_labels"]
                    })
                
                print(f"Found {len(relationships)} CONTAINS relationships")
                return relationships
                
        except Exception as e:
            print(f"Error finding CONTAINS relationships: {e}")
            return []

    def find_belongs_to_relationships(self, source_label: str = None, target_label: str = None, 
                                     source_id: str = None, target_id: str = None) -> List[Dict[str, Any]]:
        """Find BELONGS_TO relationships with optional filters."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                # Build dynamic query based on provided filters
                where_conditions = []
                params = {}
                
                if source_label:
                    where_conditions.append(f"source:{source_label}")
                if target_label:
                    where_conditions.append(f"target:{target_label}")
                if source_id:
                    where_conditions.append(f"source.{source_label.lower() if source_label else 'id'}_id = $source_id")
                    params["source_id"] = source_id
                if target_id:
                    where_conditions.append(f"target.{target_label.lower() if target_label else 'id'}_id = $target_id")
                    params["target_id"] = target_id
                
                where_clause = " AND ".join(where_conditions) if where_conditions else ""
                
                query = f"""
                MATCH (source)-[r:BELONGS_TO]->(target)
                {f"WHERE {where_clause}" if where_clause else ""}
                RETURN source, target, r, labels(source) as source_labels, labels(target) as target_labels
                """
                
                result = session.run(query, params)
                relationships = []
                for record in result:
                    relationships.append({
                        "source": dict(record["source"]),
                        "target": dict(record["target"]),
                        "relationship": dict(record["r"]),
                        "source_labels": record["source_labels"],
                        "target_labels": record["target_labels"]
                    })
                
                print(f"Found {len(relationships)} BELONGS_TO relationships")
                return relationships
                
        except Exception as e:
            print(f"Error finding BELONGS_TO relationships: {e}")
            return []

    def get_relationship_count(self, relationship_type: str) -> int:
        """Get count of relationships of a specific type."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                result = session.run(f"MATCH ()-[r:{relationship_type}]->() RETURN count(r) as count")
                return result.single()["count"]
        except Exception as e:
            print(f"Error getting relationship count: {e}")
            return 0

    def get_all_relationship_counts(self) -> Dict[str, int]:
        """Get counts of all relationship types."""
        try:
            self._connect()
            with self.driver.session(database=self.database) as session:
                result = session.run("MATCH ()-[r]->() RETURN type(r) as rel_type, count(r) as count ORDER BY count DESC")
                return {record["rel_type"]: record["count"] for record in result}
        except Exception as e:
            print(f"Error getting relationship counts: {e}")
            return {}
    
    def close(self) -> None:
        """Close the database connection."""
        if self.driver:
            self.driver.close()
