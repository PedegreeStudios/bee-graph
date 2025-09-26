"""
GraphRAG Pipeline Module

This module provides the GraphRAGPipeline class that orchestrates graph retrieval
with LLM response generation for educational content Q&A.
"""

import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.language_models.chat_models import BaseChatModel

from .azure_config import AzureConfig
from .graph_retriever import GraphRetriever


class GraphRAGPipeline:
    """GraphRAG pipeline for educational content Q&A."""
    
    def __init__(self, neo4j_params: Dict[str, str], azure_config_file: str = "src/config/azure_llm_lite.json"):
        """
        Initialize the GraphRAG pipeline.
        
        Args:
            neo4j_params: Dictionary containing Neo4j connection parameters
            azure_config_file: Path to Azure configuration file
        """
        # Initialize components
        self.azure_config = AzureConfig(azure_config_file)
        self.graph_retriever = GraphRetriever(
            uri=neo4j_params.get('uri'),
            username=neo4j_params.get('username', ''),
            password=neo4j_params.get('password', ''),
            database=neo4j_params.get('database', 'neo4j')
        )
        
        # Ensure Neo4j connection is established
        self.graph_retriever._connect()
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Default model configuration
        self.default_model = "gpt-4o-mini"
        self.temperature = 0.1
        self.max_results = 10
        self.max_cypher_attempts = 3
        self.include_cypher_in_response = True
        self.include_metadata = True
        self.response_style = "educational"  # "educational", "concise", "detailed"
        
        # Initialize LLM
        self.llm = self.azure_config.get_chat_llm(self.default_model, self.temperature)
        
        # Setup prompt templates
        self.cypher_prompt_template = self._create_cypher_prompt_template()
        self.rag_prompt_template = self._create_rag_prompt_template()
    
    def update_parameters(self, model: str = None, temperature: float = None, 
                         max_results: int = None, max_cypher_attempts: int = None,
                         include_cypher_in_response: bool = None, include_metadata: bool = None,
                         response_style: str = None):
        """
        Update pipeline parameters dynamically.
        
        Args:
            model: LLM model to use
            temperature: Temperature for LLM generation
            max_results: Maximum number of query results to return
            max_cypher_attempts: Maximum attempts for Cypher query generation
            include_cypher_in_response: Whether to include Cypher query in response
            include_metadata: Whether to include metadata in response
            response_style: Style of response ("educational", "concise", "detailed")
        """
        if model is not None and model != self.default_model:
            self.default_model = model
            self.llm = self.azure_config.get_chat_llm(self.default_model, self.temperature)
            self.logger.info(f"Updated LLM model to: {model}")
        
        if temperature is not None and temperature != self.temperature:
            self.temperature = temperature
            self.llm = self.azure_config.get_chat_llm(self.default_model, self.temperature)
            self.logger.info(f"Updated temperature to: {temperature}")
        
        if max_results is not None:
            self.max_results = max_results
            self.logger.info(f"Updated max_results to: {max_results}")
        
        if max_cypher_attempts is not None:
            self.max_cypher_attempts = max_cypher_attempts
            self.logger.info(f"Updated max_cypher_attempts to: {max_cypher_attempts}")
        
        if include_cypher_in_response is not None:
            self.include_cypher_in_response = include_cypher_in_response
            self.logger.info(f"Updated include_cypher_in_response to: {include_cypher_in_response}")
        
        if include_metadata is not None:
            self.include_metadata = include_metadata
            self.logger.info(f"Updated include_metadata to: {include_metadata}")
        
        if response_style is not None and response_style in ["educational", "concise", "detailed"]:
            self.response_style = response_style
            self.logger.info(f"Updated response_style to: {response_style}")
    
    def get_available_models(self) -> List[str]:
        """
        Get list of available models from Azure configuration.
        
        Returns:
            List of available model names
        """
        try:
            return list(self.azure_config.config.keys())
        except Exception as e:
            self.logger.error(f"Error getting available models: {e}")
            return ["gpt-4o-mini"]  # fallback
    
    def _create_cypher_prompt_template(self) -> ChatPromptTemplate:
        """Create the prompt template for Cypher query generation."""
        # Use a raw string to avoid issues with curly braces in Cypher examples
        template = r"""You are a Cypher query generator for a Neo4j knowledge graph database. Convert natural language requests into valid Cypher queries.

## DATABASE SCHEMA

### Node Types:
- **Book**: Represents books with properties: book_id, title, created_at, updated_at
- **Chapter**: Book chapters with properties: chapter_id, title, order, created_at, updated_at
- **Subchapter**: Chapter subdivisions with properties: subchapter_id, title, order, created_at, updated_at
- **Document**: Content documents with properties: document_id, title, text, abstract, created_at, updated_at
- **Section**: Document sections with properties: section_id, title, order, created_at, updated_at
- **Subsection**: Section subdivisions with properties: subsection_id, title, order, created_at, updated_at
- **Paragraph**: Text paragraphs with properties: paragraph_id, text, order, created_at, updated_at
- **Sentence**: Individual sentences with properties: sentence_id, text, order, created_at, updated_at
- **Concept**: Knowledge concepts with properties: concept_id, wikidata_id, wikidata_name, title, label, description, aliases, wikidata_url, lens, uuid, created_at, updated_at

### Relationship Types:
**Hierarchical Structure (Contains):**
- BOOK_CONTAINS_CHAPTER
- CHAPTER_CONTAINS_SUBCHAPTER
- SUBCHAPTER_CONTAINS_DOCUMENT
- DOCUMENT_CONTAINS_SECTION
- DOCUMENT_CONTAINS_PARAGRAPH
- SECTION_CONTAINS_SUBSECTION
- SECTION_CONTAINS_PARAGRAPH
- SUBSECTION_CONTAINS_PARAGRAPH
- PARAGRAPH_CONTAINS_SENTENCE

**Hierarchical Structure (Belongs To):**
- SENTENCE_BELONGS_TO_PARAGRAPH
- PARAGRAPH_BELONGS_TO_DOCUMENT
- PARAGRAPH_BELONGS_TO_SECTION
- PARAGRAPH_BELONGS_TO_SUBSECTION
- SUBSECTION_BELONGS_TO_SECTION
- SECTION_BELONGS_TO_DOCUMENT
- DOCUMENT_BELONGS_TO_SUBCHAPTER
- SUBCHAPTER_BELONGS_TO_CHAPTER
- CHAPTER_BELONGS_TO_BOOK

**Semantic Relationships:**
- SENTENCE_HAS_CONCEPT
- CONCEPT_IN_SENTENCE

## QUERY PATTERNS

### Common Query Types:
1. **Find content**: "Show me all books about X" → MATCH (b:Book) WHERE b.title CONTAINS "X"
2. **Navigate hierarchy**: "Get chapters in book Y" → MATCH (b:Book)-[:BOOK_CONTAINS_CHAPTER]->(c:Chapter) WHERE b.title CONTAINS "Y"
3. **Find concepts**: "What concepts are mentioned in document Z" → MATCH (d:Document)-[:DOCUMENT_CONTAINS_PARAGRAPH]->(p:Paragraph)-[:PARAGRAPH_CONTAINS_SENTENCE]->(s:Sentence)-[:SENTENCE_HAS_CONCEPT]->(c:Concept) WHERE d.title CONTAINS "Z"
4. **Search by properties**: Use CONTAINS for text matching
5. **Count/aggregate**: Use COUNT(), COLLECT() for summaries

### Navigation Rules:
**Flexible Hierarchy Structure:**
- Books contain chapters, chapters contain subchapters
- Subchapters can contain documents directly
- Documents can contain sections and/or paragraphs directly
- Sections can contain subsections and/or paragraphs directly
- Subsections contain paragraphs, paragraphs contain sentences
- Sentences belong to paragraphs, paragraphs can belong to documents, sections, or subsections
- All levels have reverse "BELONGS_TO" relationships for upward navigation
- Sentences are linked to concepts via SENTENCE_HAS_CONCEPT and CONCEPT_IN_SENTENCE

**Key Points:**
- The hierarchy is flexible - paragraphs can belong directly to documents or sections, bypassing subsections
- Use both "CONTAINS" and "BELONGS_TO" relationships for comprehensive traversal
- Documents can exist at multiple levels in the hierarchy

## GUIDELINES

1. **Always use proper node labels** (Book, Chapter, etc.)
2. **Use exact relationship type names** from the schema
3. **For text searches**, use CONTAINS for partial matches, = for exact matches
4. **Include RETURN statements** with relevant properties
5. **Use ORDER BY** when sequence matters (use 'order' property)
6. **Limit results** with LIMIT when appropriate
7. **Handle case sensitivity** with toLower() if needed
8. **CRITICAL**: Always include ID fields for hierarchy tracing in RETURN statements:
   - For Sentence nodes: ALWAYS include `s.sentence_id`
   - For Concept nodes: ALWAYS include `c.wikidata_id`
   - For other nodes: include their respective ID fields
   - Without these ID fields, the system cannot trace the hierarchy

## EXAMPLES

**User**: "Find all books"
**Cypher**: `MATCH (b:Book) RETURN b.title, b.book_id`

**User**: "Show chapters in the book called 'Introduction to AI'"
**Cypher**: `MATCH (b:Book)-[:BOOK_CONTAINS_CHAPTER]->(c:Chapter) WHERE b.title CONTAINS 'Introduction to AI' RETURN c.title, c.order ORDER BY c.order`

**User**: "What concepts are mentioned in sentences containing 'machine learning'"
**Cypher**: `MATCH (s:Sentence)-[:SENTENCE_HAS_CONCEPT]->(c:Concept) WHERE s.text CONTAINS 'machine learning' RETURN DISTINCT c.label, c.description, c.wikidata_id, s.sentence_id`

**User**: "Find sentences about bones and their concepts"
**Cypher**: `MATCH (s:Sentence)-[:SENTENCE_HAS_CONCEPT]->(c:Concept) WHERE s.text CONTAINS 'bones' RETURN s.text, s.sentence_id, c.label, c.wikidata_id`

**User**: "Count how many paragraphs are in each subsection"
**Cypher**: `MATCH (ss:Subsection)-[:SUBSECTION_CONTAINS_PARAGRAPH]->(p:Paragraph) RETURN ss.title, COUNT(p) as paragraph_count ORDER BY paragraph_count DESC`

**User**: "Find documents that mention concepts related to 'artificial intelligence'"
**Cypher**: `MATCH (d:Document)-[:DOCUMENT_CONTAINS_SECTION]->(sec:Section)-[:SECTION_CONTAINS_SUBSECTION]->(ss:Subsection)-[:SUBSECTION_CONTAINS_PARAGRAPH]->(p:Paragraph)-[:PARAGRAPH_CONTAINS_SENTENCE]->(s:Sentence)-[:SENTENCE_HAS_CONCEPT]->(c:Concept) WHERE c.label CONTAINS 'artificial intelligence' OR c.description CONTAINS 'artificial intelligence' RETURN DISTINCT d.title, d.document_id`

**User**: "Find paragraphs that belong directly to documents (not in sections)"
**Cypher**: `MATCH (d:Document)-[:DOCUMENT_CONTAINS_PARAGRAPH]->(p:Paragraph) RETURN d.title, p.paragraph_id`

**User**: "Find all sentences in a specific document using flexible hierarchy"
**Cypher**: `MATCH (d:Document)-[:DOCUMENT_CONTAINS_PARAGRAPH|DOCUMENT_CONTAINS_SECTION*]->(p:Paragraph)-[:PARAGRAPH_CONTAINS_SENTENCE]->(s:Sentence) WHERE d.title CONTAINS 'Example Document' RETURN s.text, s.sentence_id`

**User**: "Trace hierarchy from sentence back to book"
**Cypher**: `MATCH (s:Sentence)-[:SENTENCE_BELONGS_TO_PARAGRAPH]->(p:Paragraph)-[:PARAGRAPH_BELONGS_TO_DOCUMENT|PARAGRAPH_BELONGS_TO_SECTION|PARAGRAPH_BELONGS_TO_SUBSECTION*]->(d:Document)-[:DOCUMENT_BELONGS_TO_SUBCHAPTER]->(sc:Subchapter)-[:SUBCHAPTER_BELONGS_TO_CHAPTER]->(c:Chapter)-[:CHAPTER_BELONGS_TO_BOOK]->(b:Book) WHERE s.sentence_id = 'specific_sentence_id' RETURN b.title, c.title, sc.title, d.title`

Now convert this user request into a Cypher query:

User Request: {question}

Cypher Query:"""
        
        return ChatPromptTemplate.from_template(template)
    
    def _create_rag_prompt_template(self) -> ChatPromptTemplate:
        """Create the prompt template for RAG response generation."""
        template = """You are an expert educational assistant with access to a comprehensive knowledge graph of textbook content. Your role is to provide accurate, detailed, and educational responses based on the retrieved data from the Neo4j database.

## Database Query Results:
{query_results}

## User Question:
{question}

## Response Style: {response_style}

## Content Hierarchy Structure:
The knowledge graph follows this hierarchy: Book → Chapter → Subchapter → Document → Section → Subsection → Paragraph → Sentence. Each level provides context for the content below it.

## Instructions:
1. Answer the question based primarily on the provided database results
2. If the results don't contain enough information, clearly state what information is missing
3. Provide specific examples or details from the results when relevant
4. Structure your response clearly with proper formatting
5. If you reference specific content, mention the source hierarchy (book, chapter, subchapter, document, section, subsection, paragraph, sentence) when available
6. Be educational and help the user understand the topic better
7. If the query returned no results, suggest alternative search terms or approaches
8. Use the hierarchy information to provide better context about where the information comes from

## Response Style Guidelines:
- **Educational**: Provide comprehensive explanations with context, examples, and learning objectives
- **Concise**: Give direct, brief answers focusing on key points
- **Detailed**: Include extensive information, analysis, and comprehensive coverage

## General Guidelines:
- Use clear, educational language
- Organize information logically
- Include relevant examples from the data
- Cite sources when available
- Adapt length and depth based on response style

Response:"""
        
        return ChatPromptTemplate.from_template(template)
    
    def retrieve_context(self, question: str) -> Dict[str, Any]:
        """
        Retrieve relevant context from the knowledge graph.
        
        Args:
            question: User's question
            
        Returns:
            Dictionary containing retrieved context
        """
        self.logger.info(f"Retrieving context for question: {question}")
        
        # Extract key terms from the question for concept search
        question_terms = self._extract_key_terms(question)
        
        context = {
            'concepts': [],
            'sentences': [],
            'hierarchical_context': {},
            'related_concepts': []
        }
        
        try:
            # 1. Search for relevant concepts
            for term in question_terms:
                concepts = self.graph_retriever.search_concepts(term, limit=5)
                context['concepts'].extend(concepts)
            
            # Remove duplicates
            seen_concept_ids = set()
            unique_concepts = []
            for concept in context['concepts']:
                if concept['concept_id'] not in seen_concept_ids:
                    unique_concepts.append(concept)
                    seen_concept_ids.add(concept['concept_id'])
            context['concepts'] = unique_concepts[:10]  # Limit to top 10
            
            # 2. Get sentences for the found concepts
            if context['concepts']:
                concept_ids = [c['concept_id'] for c in context['concepts']]
                sentences = self.graph_retriever.get_sentences_for_concepts(concept_ids, limit=20)
                context['sentences'].extend(sentences)
            
            # 3. Search for sentences containing question terms
            for term in question_terms:
                sentences = self.graph_retriever.search_sentences_by_content(term, limit=10)
                context['sentences'].extend(sentences)
            
            # Remove duplicate sentences
            seen_sentence_ids = set()
            unique_sentences = []
            for sentence in context['sentences']:
                if sentence['sentence_id'] not in seen_sentence_ids:
                    unique_sentences.append(sentence)
                    seen_sentence_ids.add(sentence['sentence_id'])
            context['sentences'] = unique_sentences[:25]  # Limit to top 25
            
            # 4. Get hierarchical context for sentences
            if context['sentences']:
                sentence_ids = [s['sentence_id'] for s in context['sentences']]
                context['hierarchical_context'] = self.graph_retriever.get_hierarchical_context(sentence_ids)
            
            # 5. Get related concepts for the most relevant concepts
            if context['concepts']:
                top_concept = context['concepts'][0]
                related = self.graph_retriever.get_related_concepts(top_concept['concept_id'], limit=5)
                context['related_concepts'] = related
            
            self.logger.info(f"Retrieved context: {len(context['concepts'])} concepts, {len(context['sentences'])} sentences")
            
        except Exception as e:
            self.logger.error(f"Error retrieving context: {e}")
            context['error'] = str(e)
        
        return context
    
    def _extract_key_terms(self, question: str) -> List[str]:
        """
        Extract key terms from the question for searching.
        
        Args:
            question: User's question
            
        Returns:
            List of key terms
        """
        # Simple keyword extraction - remove common words and split
        common_words = {'what', 'is', 'are', 'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'how', 'why', 'when', 'where', 'who', 'which', 'that', 'this', 'these', 'those', 'can', 'could', 'would', 'should', 'will', 'do', 'does', 'did', 'have', 'has', 'had', 'be', 'been', 'being', 'was', 'were'}
        
        # Split by spaces and filter
        words = question.lower().split()
        key_terms = [word.strip('.,!?;:') for word in words if word.strip('.,!?;:') not in common_words and len(word.strip('.,!?;:')) > 2]
        
        # Also include the full question as a search term
        key_terms.append(question)
        
        return key_terms[:5]  # Limit to 5 terms
    
    def format_context(self, graph_results: Dict[str, Any]) -> str:
        """
        Format graph results into structured context for the LLM.
        
        Args:
            graph_results: Dictionary containing retrieved graph data
            
        Returns:
            Formatted context string
        """
        if 'error' in graph_results:
            return f"Error retrieving context: {graph_results['error']}"
        
        context_parts = []
        
        # Add concepts section
        if graph_results.get('concepts'):
            context_parts.append("## Key Concepts Found:")
            for concept in graph_results['concepts'][:5]:  # Top 5 concepts
                concept_info = f"- **{concept.get('label', 'Unknown')}**"
                if concept.get('wikidata_name'):
                    concept_info += f" (Wikidata: {concept['wikidata_name']})"
                if concept.get('text'):
                    concept_info += f": {concept['text']}"
                context_parts.append(concept_info)
            context_parts.append("")
        
        # Add sentences section
        if graph_results.get('sentences'):
            context_parts.append("## Relevant Content:")
            for sentence in graph_results['sentences'][:15]:  # Top 15 sentences
                sentence_text = sentence.get('text', '')
                if sentence_text:
                    # Add source information
                    source_info = []
                    if sentence.get('chapter_title'):
                        source_info.append(f"Chapter: {sentence['chapter_title']}")
                    if sentence.get('section_title'):
                        source_info.append(f"Section: {sentence['section_title']}")
                    if sentence.get('subsection_title'):
                        source_info.append(f"Subsection: {sentence['subsection_title']}")
                    
                    source_str = f" ({', '.join(source_info)})" if source_info else ""
                    context_parts.append(f"- {sentence_text}{source_str}")
            context_parts.append("")
        
        # Add related concepts section
        if graph_results.get('related_concepts'):
            context_parts.append("## Related Concepts:")
            for concept in graph_results['related_concepts'][:3]:  # Top 3 related
                context_parts.append(f"- **{concept.get('label', 'Unknown')}**: {concept.get('text', '')}")
            context_parts.append("")
        
        if not context_parts:
            return "No relevant context found in the knowledge graph for this question."
        
        return "\n".join(context_parts)
    
    def generate_cypher_query(self, question: str) -> str:
        """
        Generate Cypher query using the LLM.
        
        Args:
            question: User's question
            
        Returns:
            Generated Cypher query
        """
        try:
            # Create the prompt
            prompt = self.cypher_prompt_template.format_messages(
                question=question
            )
            
            # Generate response
            response = self.llm.invoke(prompt)
            
            # Extract Cypher query from response (remove markdown formatting if present)
            cypher_query = response.content.strip()
            
            # Remove markdown code blocks if present
            if cypher_query.startswith('```cypher'):
                cypher_query = cypher_query[9:]  # Remove ```cypher
            if cypher_query.startswith('```'):
                cypher_query = cypher_query[3:]   # Remove ```
            if cypher_query.endswith('```'):
                cypher_query = cypher_query[:-3]  # Remove trailing ```
            
            return cypher_query.strip()
            
        except Exception as e:
            self.logger.error(f"Error generating Cypher query: {e}")
            return f"// Error generating Cypher query: {str(e)}"
    
    def validate_cypher_query(self, cypher_query: str) -> bool:
        """
        Validate a Cypher query by attempting to explain it.
        
        Args:
            cypher_query: The Cypher query to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not cypher_query or cypher_query.startswith('// Error'):
            return False
        
        try:
            # Use Neo4j's EXPLAIN to validate the query
            with self.graph_retriever.driver.session(database=self.graph_retriever.database) as session:
                # Try to explain the query (this validates syntax without executing)
                explain_query = f"EXPLAIN {cypher_query}"
                session.run(explain_query)
                return True
                
        except Exception as e:
            self.logger.warning(f"Cypher query validation failed: {e}")
            return False
    
    def execute_cypher_query(self, cypher_query: str, limit: int = None) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query against the Neo4j database.
        
        Args:
            cypher_query: The Cypher query to execute
            limit: Maximum number of results to return
            
        Returns:
            List of query results with enhanced hierarchy information
        """
        try:
            # Use instance max_results if limit not provided
            if limit is None:
                limit = self.max_results
            
            # Add LIMIT if not already present
            if 'LIMIT' not in cypher_query.upper():
                cypher_query = f"{cypher_query} LIMIT {limit}"
            
            with self.graph_retriever.driver.session(database=self.graph_retriever.database) as session:
                result = session.run(cypher_query)
                records = []
                
                for record in result:
                    # Convert Neo4j record to dictionary
                    record_dict = {}
                    for key in record.keys():
                        value = record[key]
                        # Convert Neo4j node objects to dictionaries
                        if hasattr(value, 'items'):
                            record_dict[key] = dict(value.items())
                        else:
                            record_dict[key] = value
                    
                    # Enhance with hierarchy information
                    enhanced_record = self._enhance_record_with_hierarchy(record_dict)
                    records.append(enhanced_record)
                
                self.logger.info(f"Executed query returned {len(records)} results")
                return records
                
        except Exception as e:
            self.logger.error(f"Error executing Cypher query: {e}")
            return []
    
    def _enhance_record_with_hierarchy(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhance a query result record with full hierarchy information.
        
        Args:
            record: The original query result record
            
        Returns:
            Enhanced record with hierarchy information
        """
        try:
            # Look for sentence_id in the record
            sentence_id = None
            if 's.sentence_id' in record:
                sentence_id = record['s.sentence_id']
            elif 'sentence_id' in record:
                sentence_id = record['sentence_id']
            
            if sentence_id:
                # Trace the hierarchy for this sentence
                hierarchy = self.graph_retriever.trace_node_hierarchy(sentence_id, 'Sentence')
                if 'error' not in hierarchy:
                    record['hierarchy'] = hierarchy
                    self.logger.debug(f"Enhanced record with hierarchy for sentence {sentence_id}")
                else:
                    self.logger.warning(f"Failed to trace hierarchy for sentence {sentence_id}: {hierarchy.get('error')}")
            
            # Look for concept_id in the record (try different field names)
            concept_id = None
            if 'c.concept_id' in record:
                concept_id = record['c.concept_id']
            elif 'c.wikidata_id' in record:
                concept_id = record['c.wikidata_id']
            elif 'concept_id' in record:
                concept_id = record['concept_id']
            elif 'wikidata_id' in record:
                concept_id = record['wikidata_id']
            
            if concept_id:
                # Trace the hierarchy for this concept
                hierarchy = self.graph_retriever.trace_node_hierarchy(concept_id, 'Concept')
                if 'error' not in hierarchy:
                    record['hierarchy'] = hierarchy
                    self.logger.debug(f"Enhanced record with hierarchy for concept {concept_id}")
                else:
                    self.logger.warning(f"Failed to trace hierarchy for concept {concept_id}: {hierarchy.get('error')}")
            
            return record
            
        except Exception as e:
            self.logger.warning(f"Error enhancing record with hierarchy: {e}")
            return record
    
    def generate_rag_response(self, question: str, query_results: List[Dict[str, Any]]) -> str:
        """
        Generate RAG response using query results.
        
        Args:
            question: Original user question
            query_results: Results from the Cypher query
            
        Returns:
            Generated educational response
        """
        try:
            # Format query results for the prompt
            if not query_results:
                formatted_results = "No results found in the database for this query."
            else:
                formatted_results = json.dumps(query_results, indent=2, default=str)
            
            # Create the prompt
            prompt = self.rag_prompt_template.format_messages(
                query_results=formatted_results,
                question=question,
                response_style=self.response_style
            )
            
            # Generate response
            response = self.llm.invoke(prompt)
            
            return response.content.strip()
            
        except Exception as e:
            self.logger.error(f"Error generating RAG response: {e}")
            return f"I apologize, but I encountered an error while generating a response: {str(e)}"
    
    def query(self, question: str) -> Dict[str, Any]:
        """
        Main entry point for RAG pipeline with Cypher query generation and execution.
        
        Args:
            question: User's question
            
        Returns:
            Dictionary containing RAG response and metadata
        """
        self.logger.info(f"Processing question: {question}")
        
        try:
            # Step 1: Generate and validate Cypher query
            cypher_query = None
            max_attempts = self.max_cypher_attempts
            
            for attempt in range(max_attempts):
                self.logger.info(f"Generating Cypher query (attempt {attempt + 1}/{max_attempts})")
                cypher_query = self.generate_cypher_query(question)
                
                # Validate the query
                if self.validate_cypher_query(cypher_query):
                    self.logger.info("Cypher query validation successful")
                    break
                else:
                    self.logger.warning(f"Cypher query validation failed (attempt {attempt + 1})")
                    if attempt == max_attempts - 1:
                        return {
                            'question': question,
                            'response': f"I was unable to generate a valid Cypher query for your question after {max_attempts} attempts. Please try rephrasing your question.",
                            'query_type': 'rag_generation',
                            'metadata': {
                                'error': 'Invalid Cypher query after multiple attempts',
                                'model_used': self.default_model,
                                'temperature': self.temperature,
                                'attempts': max_attempts
                            }
                        }
            
            # Step 2: Execute the validated Cypher query
            self.logger.info("Executing Cypher query against Neo4j database")
            query_results = self.execute_cypher_query(cypher_query)
            
            # Step 3: Generate RAG response based on query results
            self.logger.info("Generating RAG response based on query results")
            rag_response = self.generate_rag_response(question, query_results)
            
            # Prepare result
            result = {
                'question': question,
                'response': rag_response,
                'query_type': 'rag_generation',
                'metadata': {
                    'cypher_query': cypher_query if self.include_cypher_in_response else None,
                    'query_results_count': len(query_results) if self.include_metadata else None,
                    'model_used': self.default_model if self.include_metadata else None,
                    'temperature': self.temperature if self.include_metadata else None,
                    'response_style': self.response_style if self.include_metadata else None,
                    'query_results': query_results if self.include_metadata else None
                }
            }
            
            self.logger.info(f"Successfully completed RAG pipeline with {len(query_results)} query results")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in RAG pipeline: {e}")
            return {
                'question': question,
                'response': f"I apologize, but I encountered an error while processing your question: {str(e)}",
                'query_type': 'rag_generation',
                'metadata': {
                    'error': str(e),
                    'model_used': self.default_model,
                    'temperature': self.temperature
                }
            }
    
    def test_components(self) -> Dict[str, bool]:
        """
        Test all pipeline components.
        
        Returns:
            Dictionary with test results for each component
        """
        results = {}
        
        # Test Azure configuration
        try:
            self.azure_config.validate_config()
            results['azure_config'] = True
        except Exception as e:
            self.logger.error(f"Azure config test failed: {e}")
            results['azure_config'] = False
        
        # Test Neo4j connection
        try:
            results['neo4j_connection'] = self.graph_retriever.test_connection()
        except Exception as e:
            self.logger.error(f"Neo4j connection test failed: {e}")
            results['neo4j_connection'] = False
        
        # Test LLM
        try:
            test_response = self.llm.invoke("Hello, this is a test message.")
            results['llm'] = bool(test_response.content)
        except Exception as e:
            self.logger.error(f"LLM test failed: {e}")
            results['llm'] = False
        
        return results
    
    def close(self) -> None:
        """Close all connections and cleanup resources."""
        if self.graph_retriever:
            self.graph_retriever.close()
        self.logger.info("GraphRAG pipeline closed")
