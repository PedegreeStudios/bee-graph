"""
Streamlit Cypher Query Generator Application

Streamlit application that provides a web interface for generating
Cypher queries for Neo4j knowledge graph using Azure OpenAI.
"""

import os
import json
import logging
import streamlit as st
from pathlib import Path
from typing import Dict, Any

from src.chainlit_app.rag_pipeline import GraphRAGPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_neo4j_config() -> Dict[str, str]:
    """Load Neo4j configuration from config file."""
    config_path = Path("src/config/neo4j_config.json")
    
    if not config_path.exists():
        logger.warning("Neo4j config file not found, using defaults")
        return {
            'uri': 'bolt://localhost:7687',
            'username': '',
            'password': '',
            'database': 'neo4j'
        }
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config.get('neo4j', {})
    except Exception as e:
        logger.error(f"Error loading Neo4j config: {e}")
        return {
            'uri': 'bolt://localhost:7687',
            'username': '',
            'password': '',
            'database': 'neo4j'
        }


@st.cache_resource
def initialize_pipeline():
    """Initialize the GraphRAG pipeline with caching."""
    try:
        # Load Neo4j configuration
        neo4j_config = load_neo4j_config()
        
        # Initialize pipeline
        pipeline = GraphRAGPipeline(
            neo4j_params=neo4j_config,
            azure_config_file="src/config/azure_llm_lite.json"
        )
        
        return pipeline
    except Exception as e:
        st.error(f"Failed to initialize pipeline: {str(e)}")
        return None


def extract_document_info(result):
    """Extract document information from a query result with enhanced hierarchy."""
    doc_info = {}
    
    # Check if we have enhanced hierarchy information
    if 'hierarchy' in result and isinstance(result['hierarchy'], dict):
        hierarchy = result['hierarchy']
        
        # Use the full hierarchy path for better context
        doc_info['full_hierarchy'] = hierarchy.get('full_path', '')
        doc_info['hierarchy_path'] = hierarchy.get('path', [])
        
        # Extract specific hierarchy levels - check if each level exists and has data
        if hierarchy.get('book') and isinstance(hierarchy['book'], dict):
            doc_info['book_title'] = hierarchy['book'].get('title', 'Unknown Book')
            doc_info['book_id'] = hierarchy['book'].get('book_id', '')
        
        if hierarchy.get('chapter') and isinstance(hierarchy['chapter'], dict):
            doc_info['chapter_title'] = hierarchy['chapter'].get('title', 'Unknown Chapter')
            doc_info['chapter_id'] = hierarchy['chapter'].get('chapter_id', '')
        
        if hierarchy.get('subchapter') and isinstance(hierarchy['subchapter'], dict):
            doc_info['subchapter_title'] = hierarchy['subchapter'].get('title', 'Unknown Subchapter')
            doc_info['subchapter_id'] = hierarchy['subchapter'].get('subchapter_id', '')
        
        if hierarchy.get('document') and isinstance(hierarchy['document'], dict):
            doc_info['document_title'] = hierarchy['document'].get('title', 'Unknown Document')
            doc_info['document_id'] = hierarchy['document'].get('document_id', '')
        
        if hierarchy.get('section') and isinstance(hierarchy['section'], dict):
            doc_info['section_title'] = hierarchy['section'].get('title', 'Unknown Section')
            doc_info['section_id'] = hierarchy['section'].get('section_id', '')
        
        if hierarchy.get('subsection') and isinstance(hierarchy['subsection'], dict):
            doc_info['subsection_title'] = hierarchy['subsection'].get('title', 'Unknown Subsection')
            doc_info['subsection_id'] = hierarchy['subsection'].get('subsection_id', '')
        
        if hierarchy.get('paragraph') and isinstance(hierarchy['paragraph'], dict):
            doc_info['paragraph_text'] = hierarchy['paragraph'].get('text', '')
            doc_info['paragraph_id'] = hierarchy['paragraph'].get('paragraph_id', '')
        
        if hierarchy.get('sentence') and isinstance(hierarchy['sentence'], dict):
            doc_info['sentence_text'] = hierarchy['sentence'].get('text', '')
            doc_info['sentence_id'] = hierarchy['sentence'].get('sentence_id', '')
        
        if hierarchy.get('concept') and isinstance(hierarchy['concept'], dict):
            doc_info['concept_name'] = hierarchy['concept'].get('name', hierarchy['concept'].get('label', hierarchy['concept'].get('text', 'Unknown Concept')))
            doc_info['concept_id'] = hierarchy['concept'].get('concept_id', hierarchy['concept'].get('wikidata_id', ''))
        
        # Set a readable title from the highest available level
        doc_info['title'] = (doc_info.get('book_title') or 
                           doc_info.get('chapter_title') or 
                           doc_info.get('document_title') or 
                           'Unknown Document')
        
        return doc_info
    
    # Fallback to original logic for backward compatibility
    # Handle the actual Cypher query result structure (s.text, s.sentence_id)
    if 's.sentence_id' in result:
        sentence_id = result['s.sentence_id']
        doc_info['sentence_id'] = sentence_id
        doc_info['document_id'] = sentence_id  # Use sentence_id as document identifier
        
        # Extract document hierarchy from sentence_id
        # Format: anatomy_and_physiology_2e_fs-id1530911_sent_1
        parts = sentence_id.split('_')
        if len(parts) >= 3:
            # Extract book name (e.g., "anatomy_and_physiology_2e")
            book_part = '_'.join(parts[:-2])  # Everything except last two parts
            doc_info['book_title'] = book_part.replace('_', ' ').title()
            
            # Extract module/section ID (e.g., "fs-id1530911")
            module_part = parts[-2]
            doc_info['module_id'] = module_part
            
            # Extract sentence number
            sentence_part = parts[-1]
            doc_info['sentence_number'] = sentence_part
        
        # Set a readable title
        doc_info['title'] = doc_info.get('book_title', 'Unknown Document')
        
        # Add the sentence text for context
        if 's.text' in result:
            doc_info['sentence_text'] = result['s.text']
    
    # Look for sentence data in nested structure (fallback)
    elif 'sentence' in result and isinstance(result['sentence'], dict):
        sentence = result['sentence']
        doc_info['document_id'] = sentence.get('document_id', 'unknown')
        doc_info['module_id'] = sentence.get('module_id', 'unknown')
        doc_info['title'] = sentence.get('title', 'Unknown Document')
        
        # Try to extract hierarchical information
        if 'book_title' in sentence:
            doc_info['book_title'] = sentence['book_title']
        if 'chapter_title' in sentence:
            doc_info['chapter_title'] = sentence['chapter_title']
        if 'section_title' in sentence:
            doc_info['section_title'] = sentence['section_title']
    
    # Look for concept data as fallback
    elif 'concept' in result and isinstance(result['concept'], dict):
        concept = result['concept']
        doc_info['document_id'] = concept.get('concept_id', 'unknown')
        doc_info['title'] = concept.get('name', 'Unknown Concept')
    
    # Look for any other document-related fields
    else:
        for key, value in result.items():
            if isinstance(value, dict):
                if 'document_id' in value:
                    doc_info['document_id'] = value['document_id']
                if 'title' in value:
                    doc_info['title'] = value['title']
                if 'module_id' in value:
                    doc_info['module_id'] = value['module_id']
    
    return doc_info if doc_info.get('document_id') else None


def test_system_components(pipeline):
    """Test system components and display results."""
    if pipeline is None:
        return False
    
    with st.spinner("Testing system components..."):
        test_results = pipeline.test_components()
    
    # Display test results
    col1, col2, col3 = st.columns(3)
    
    with col1:
        azure_status = "✅" if test_results.get('azure_config') else "❌"
        st.metric("Azure Config", azure_status)
    
    with col2:
        neo4j_status = "✅" if test_results.get('neo4j_connection') else "❌"
        st.metric("Neo4j Connection", neo4j_status)
    
    with col3:
        llm_status = "✅" if test_results.get('llm') else "❌"
        st.metric("LLM", llm_status)
    
    return all(test_results.values())


def main():
    """Main Streamlit application."""
    # Page configuration
    st.set_page_config(
        page_title="Knowledge Graph Assistant",
        page_icon="🧠",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Title and description
    st.title("🧠 Knowledge Graph Assistant")
    st.markdown("""
    Ask questions about your educational content and get intelligent answers based on your Neo4j knowledge graph.
    I'll generate Cypher queries, execute them against your database, and provide educational responses.
    """)
    
    # Initialize pipeline
    pipeline = initialize_pipeline()
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ System Parameters")
        
        if pipeline is not None:
            # Parameter controls - vertically stacked for better visibility
            st.subheader("🤖 Model Configuration")
            
            # Model selection
            available_models = pipeline.get_available_models()
            default_model = getattr(pipeline, 'default_model', 'gpt-4o-mini')
            selected_model = st.selectbox(
                "🤖 LLM Model",
                available_models,
                index=available_models.index(default_model) if default_model in available_models else 0,
                help="Select the language model for query generation and response"
            )
            
            # Temperature control
            temperature = st.slider(
                "🌡️ Temperature",
                min_value=0.0,
                max_value=1.0,
                value=getattr(pipeline, 'temperature', 0.1),
                step=0.1,
                help="Controls randomness in model responses (0.0 = deterministic, 2.0 = very creative)"
            )
            
            st.subheader("📊 Query Configuration")
            
            # Max results control
            max_results = st.slider(
                "📊 Max Results",
                min_value=0,
                max_value=50,
                value=10,  # Force default to 10
                step=1,
                help="Maximum number of database results to retrieve"
            )
            
            # Max attempts control
            max_attempts = st.slider(
                "🔄 Max Attempts",
                min_value=1,
                max_value=10,
                value=getattr(pipeline, 'max_cypher_attempts', 3),
                step=1,
                help="Maximum attempts for Cypher query generation"
            )
            
            st.subheader("🎛️ Response Configuration")
            
            # Include Cypher in response
            include_cypher = st.checkbox(
                "🔍 Show Cypher Query",
                value=getattr(pipeline, 'include_cypher_in_response', True),
                help="Include generated Cypher query in response"
            )
            
            # Include metadata
            include_metadata = st.checkbox(
                "📊 Show Metadata",
                value=getattr(pipeline, 'include_metadata', True),
                help="Include metadata (model, results count, etc.) in response"
            )
            
            # Update parameters button
            if st.button("🔄 Update Parameters", type="primary", use_container_width=True):
                pipeline.update_parameters(
                    model=selected_model,
                    temperature=temperature,
                    max_results=max_results,
                    max_cypher_attempts=max_attempts,
                    include_cypher_in_response=include_cypher,
                    include_metadata=include_metadata,
                    response_style="educational"  # Default response style
                )
                st.success("✅ Parameters updated successfully!")
            
            st.markdown("---")
            
            # Current settings display
            st.subheader("📋 Current Settings")
            st.json({
                "Model": getattr(pipeline, 'default_model', 'gpt-4o-mini'),
                "Temperature": getattr(pipeline, 'temperature', 0.1),
                "Max Results": 10,  # Force default to 10
                "Max Attempts": getattr(pipeline, 'max_cypher_attempts', 3),
                "Response Style": "educational",  # Default response style
                "Show Cypher": getattr(pipeline, 'include_cypher_in_response', True),
                "Show Metadata": getattr(pipeline, 'include_metadata', True)
            })
            
            st.markdown("---")
            
            with st.expander("🔧 System Status", expanded=False):
                system_ok = test_system_components(pipeline)
                
                if system_ok:
                    st.success("✅ All systems operational")
                else:
                    st.error("❌ Some systems have issues")
        else:
            st.error("❌ Pipeline initialization failed")
        
        with st.expander("Example Queries"):
            st.markdown("""
            **Simple Queries:**
            - Find all books
            - Show all chapters
            - Count documents
            
            **Hierarchical Queries:**
            - Show chapters in book "Introduction to AI"
            - Get paragraphs in document "Machine Learning Basics"
            - Find sentences in section "Neural Networks"
            
            **Concept Queries:**
            - What concepts are mentioned in sentences containing "machine learning"
            - Find documents that mention "artificial intelligence"
            - Show concepts related to "neural networks"
            
            **Aggregation Queries:**
            - Count paragraphs per document
            - Count sentences per chapter
            - Find most mentioned concepts
            """)
        
        st.header("Database Schema")
        with st.expander("Node Types"):
            st.markdown("""
            **Available Node Labels:**
            - **Book** (label: "Book")
            - **Chapter** (label: "Chapter") 
            - **Subchapter** (label: "Subchapter")
            - **Document** (label: "Document")
            - **Section** (label: "Section")
            - **Subsection** (label: "Subsection")
            - **Paragraph** (label: "Paragraph")
            - **Sentence** (label: "Sentence")
            - **Concept** (label: "Concept")
            
            **Key Properties:**
            - **Book**: book_id, title, created_at, updated_at
            - **Chapter**: chapter_id, title, order, created_at, updated_at
            - **Subchapter**: subchapter_id, title, order, created_at, updated_at
            - **Document**: document_id, title, text, abstract, created_at, updated_at
            - **Section**: section_id, title, order, created_at, updated_at
            - **Subsection**: subsection_id, title, order, created_at, updated_at
            - **Paragraph**: paragraph_id, text, order, created_at, updated_at
            - **Sentence**: sentence_id, text, order, created_at, updated_at
            - **Concept**: wikidata_id, wikidata_name, title, label, description, aliases, wikidata_url, lens, uuid, created_at, updated_at
            """)
        
        with st.expander("Relationship Types"):
            st.markdown("""
            **Available Relationship Types:**
            - **BOOK_CONTAINS_CHAPTER** (relationship: "BOOK_CONTAINS_CHAPTER")
            - **CHAPTER_CONTAINS_SUBCHAPTER** (relationship: "CHAPTER_CONTAINS_SUBCHAPTER")
            - **DOCUMENT_CONTAINS_SECTION** (relationship: "DOCUMENT_CONTAINS_SECTION")
            - **SECTION_CONTAINS_SUBSECTION** (relationship: "SECTION_CONTAINS_SUBSECTION")
            - **SUBSECTION_CONTAINS_PARAGRAPH** (relationship: "SUBSECTION_CONTAINS_PARAGRAPH")
            - **PARAGRAPH_CONTAINS_SENTENCE** (relationship: "PARAGRAPH_CONTAINS_SENTENCE")
            - **SENTENCE_BELONGS_TO_PARAGRAPH** (relationship: "SENTENCE_BELONGS_TO_PARAGRAPH")
            - **PARAGRAPH_BELONGS_TO_SUBSECTION** (relationship: "PARAGRAPH_BELONGS_TO_SUBSECTION")
            - **SUBSECTION_BELONGS_TO_SECTION** (relationship: "SUBSECTION_BELONGS_TO_SECTION")
            
            **Hierarchy Structure:**
            ```
            Book → Chapter → Subchapter
                      ↓
                   Document → Section → Subsection → Paragraph → Sentence
            ```
            
            **Additional Relationships:**
            - SENTENCE_HAS_CONCEPT, CONCEPT_IN_SENTENCE
            """)
    
    # Main content area
    if pipeline is None:
        st.error("❌ System initialization failed. Please check your configuration files.")
        st.stop()
    
    # Query input
    st.header("Ask a Question")
    
    # Text input for the query
    user_input = st.text_area(
        "Ask a question about your educational content:",
        placeholder="e.g., What is DNA? Find all sentences about photosynthesis",
        height=100
    )
    
    # Generate button
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        generate_button = st.button("🧠 Get Answer", type="primary", use_container_width=True)
    
    # Process the query
    if generate_button and user_input.strip():
        with st.spinner("Generating response..."):
            try:
                # Process the question through the pipeline
                result = pipeline.query(user_input.strip())
                
                # Display the RAG response
                st.header("Answer")
                
                rag_response = result['response']
                st.markdown(rag_response)
                
                # Add metadata
                metadata = result.get('metadata', {})
                
                # Show Cypher query if available and enabled
                if metadata.get('cypher_query'):
                    with st.expander("🔍 View Generated Cypher Query"):
                        st.code(metadata.get('cypher_query'), language="cypher")
                
                # Show metadata if enabled
                if getattr(pipeline, 'include_metadata', True):
                    metadata_cols = st.columns(3)
                    
                    with metadata_cols[0]:
                        if metadata.get('query_results_count') is not None:
                            st.metric("Results Found", metadata.get('query_results_count'))
                    
                    with metadata_cols[1]:
                        if metadata.get('model_used'):
                            st.metric("Model", metadata.get('model_used'))
                    
                    with metadata_cols[2]:
                        if metadata.get('response_style'):
                            st.metric("Style", metadata.get('response_style').title())
                
                # Show detailed query results with hierarchy and document metadata
                if metadata.get('query_results') and len(metadata['query_results']) > 0:
                    st.header("📚 Source Documents & Hierarchy")
                    
                    # Process and display query results
                    query_results = metadata['query_results']
                    
                    # Group results by document/source for better organization
                    documents = {}
                    for result in query_results:
                        # Extract document information from the result
                        doc_info = extract_document_info(result)
                        if doc_info:
                            doc_key = doc_info['document_id']
                            if doc_key not in documents:
                                documents[doc_key] = {
                                    'info': doc_info,
                                    'results': []
                                }
                            documents[doc_key]['results'].append(result)
                    
                    # Display each document and its results
                    for doc_key, doc_data in documents.items():
                        with st.expander(f"📖 {doc_data['info'].get('title', 'Unknown Document')} ({len(doc_data['results'])} results)"):
                            # Document metadata
                            st.subheader("📋 Document Information")
                            doc_info = doc_data['info']
                            
                            # Show clean arrow hierarchy
                            hierarchy_parts = []
                            if doc_info.get('book_title'):
                                hierarchy_parts.append(doc_info['book_title'])
                            if doc_info.get('chapter_title'):
                                hierarchy_parts.append(doc_info['chapter_title'])
                            if doc_info.get('subchapter_title'):
                                hierarchy_parts.append(doc_info['subchapter_title'])
                            if doc_info.get('document_title'):
                                hierarchy_parts.append(doc_info['document_title'])
                            if doc_info.get('section_title'):
                                hierarchy_parts.append(doc_info['section_title'])
                            if doc_info.get('subsection_title'):
                                hierarchy_parts.append(doc_info['subsection_title'])
                            if doc_info.get('paragraph_text'):
                                # Truncate paragraph text for display
                                para_text = doc_info['paragraph_text'][:50] + "..." if len(doc_info['paragraph_text']) > 50 else doc_info['paragraph_text']
                                hierarchy_parts.append(f"Paragraph: {para_text}")
                            if doc_info.get('sentence_text'):
                                # Truncate sentence text for display
                                sent_text = doc_info['sentence_text'][:50] + "..." if len(doc_info['sentence_text']) > 50 else doc_info['sentence_text']
                                hierarchy_parts.append(f"Sentence: {sent_text}")
                            if doc_info.get('concept_name'):
                                # Add concept information
                                concept_name = doc_info['concept_name'][:50] + "..." if len(doc_info['concept_name']) > 50 else doc_info['concept_name']
                                hierarchy_parts.append(f"Concept: {concept_name}")
                            
                            if hierarchy_parts:
                                clean_hierarchy = " > ".join(hierarchy_parts)
                                st.write(f"**📚 Hierarchy:** {clean_hierarchy}")
                                st.markdown("---")
                            
                            # Show full hierarchy path if available (fallback)
                            if doc_info.get('full_hierarchy') and not hierarchy_parts:
                                st.write(f"**📚 Full Hierarchy:** {doc_info['full_hierarchy']}")
                                st.markdown("---")
                            
                            # Show detailed hierarchy levels (fallback)
                            if doc_info.get('hierarchy_path') and not hierarchy_parts:
                                st.write("**🏗️ Hierarchy Path:**")
                                for i, level in enumerate(doc_info['hierarchy_path']):
                                    st.write(f"  {i+1}. {level}")
                                st.markdown("---")
                            
                            # Show specific document details
                            doc_cols = st.columns(2)
                            with doc_cols[0]:
                                if doc_info.get('book_title'):
                                    st.write(f"**📖 Book:** {doc_info['book_title']}")
                                if doc_info.get('chapter_title'):
                                    st.write(f"**📑 Chapter:** {doc_info['chapter_title']}")
                                if doc_info.get('subchapter_title'):
                                    st.write(f"**📄 Subchapter:** {doc_info['subchapter_title']}")
                                if doc_info.get('document_title'):
                                    st.write(f"**📋 Document:** {doc_info['document_title']}")
                                if doc_info.get('section_title'):
                                    st.write(f"**📝 Section:** {doc_info['section_title']}")
                                if doc_info.get('subsection_title'):
                                    st.write(f"**📌 Subsection:** {doc_info['subsection_title']}")
                            
                            with doc_cols[1]:
                                if doc_info.get('book_id'):
                                    st.write(f"**Book ID:** `{doc_info['book_id']}`")
                                if doc_info.get('chapter_id'):
                                    st.write(f"**Chapter ID:** `{doc_info['chapter_id']}`")
                                if doc_info.get('document_id'):
                                    st.write(f"**Document ID:** `{doc_info['document_id']}`")
                                if doc_info.get('section_id'):
                                    st.write(f"**Section ID:** `{doc_info['section_id']}`")
                                if doc_info.get('sentence_id'):
                                    st.write(f"**Sentence ID:** `{doc_info['sentence_id']}`")
                                if doc_info.get('concept_id'):
                                    st.write(f"**Concept ID:** `{doc_info['concept_id']}`")
                                if doc_info.get('concept_name'):
                                    st.write(f"**Concept:** {doc_info['concept_name']}")
                            
                            # Display results from this document
                            st.subheader("🔍 Retrieved Content")
                            for i, result in enumerate(doc_data['results'][:5]):  # Limit to first 5 results per document
                                with st.container():
                                    st.write(f"**Result {i+1}:**")
                                    
                                    # Show sentence content if available (new structure)
                                    if 's.text' in result:
                                        content = result['s.text']
                                        st.write(f"*{content[:300]}{'...' if len(content) > 300 else ''}*")
                                    
                                    # Show sentence ID and metadata
                                    if 's.sentence_id' in result:
                                        sentence_id = result['s.sentence_id']
                                        st.write(f"**Sentence ID:** `{sentence_id}`")
                                        
                                        # Parse sentence ID for additional info
                                        parts = sentence_id.split('_')
                                        if len(parts) >= 3:
                                            module_id = parts[-2]
                                            sentence_num = parts[-1]
                                            st.write(f"**Module:** {module_id} | **Sentence:** {sentence_num}")
                                    
                                    # Show sentence content if available (old structure)
                                    elif 'sentence' in result:
                                        sentence_data = result['sentence']
                                        if isinstance(sentence_data, dict):
                                            content = sentence_data.get('content', 'No content available')
                                            st.write(f"*{content[:200]}{'...' if len(content) > 200 else ''}*")
                                    
                                    # Show concept information if available
                                    if 'concept' in result:
                                        concept_data = result['concept']
                                        if isinstance(concept_data, dict):
                                            concept_name = concept_data.get('name', 'Unknown concept')
                                            st.write(f"**Concept:** {concept_name}")
                                    
                                    # Show any additional properties (excluding the main content fields)
                                    other_props = {k: v for k, v in result.items() 
                                                 if k not in ['sentence', 'concept', 's.text', 's.sentence_id'] and v is not None}
                                    if other_props:
                                        st.json(other_props)
                                    
                                    st.markdown("---")
                            
                            if len(doc_data['results']) > 5:
                                st.write(f"... and {len(doc_data['results']) - 5} more results from this document")
                
                # Log the interaction
                logger.info(f"Processed query: {user_input[:100]}...")
                logger.info(f"RAG response generated successfully")
                
            except Exception as e:
                st.error(f"❌ Error generating response: {str(e)}")
                logger.error(f"Error processing query: {e}")
    
    elif generate_button and not user_input.strip():
        st.warning("⚠️ Please enter a query description first.")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p>Cypher Query Generator | Powered by Azure OpenAI & Neo4j</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
