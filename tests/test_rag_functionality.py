"""
Test script for the new RAG functionality
"""

import json
import logging
from pathlib import Path
from src.chainlit_app.rag_pipeline import GraphRAGPipeline

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_rag_functionality():
    """Test the new RAG functionality."""
    
    # Load Neo4j config
    config_path = Path("src/config/neo4j_config.json")
    if config_path.exists():
        with open(config_path, 'r') as f:
            config = json.load(f)
        neo4j_config = config.get('neo4j', {})
    else:
        neo4j_config = {
            'uri': 'bolt://localhost:7687',
            'username': '',
            'password': '',
            'database': 'neo4j'
        }
    
    # Initialize pipeline
    pipeline = GraphRAGPipeline(
        neo4j_params=neo4j_config,
        azure_config_file="src/config/azure_llm_lite.json"
    )
    
    # Test query
    test_question = "Find all sentences related to DNA"
    
    print(f"Testing RAG functionality with question: {test_question}")
    print("=" * 60)
    
    try:
        # Test the full RAG pipeline
        result = pipeline.query(test_question)
        
        print("RAG Pipeline Result:")
        print(f"Question: {result['question']}")
        print(f"Query Type: {result['query_type']}")
        print(f"Response: {result['response']}")
        
        metadata = result.get('metadata', {})
        if metadata.get('cypher_query'):
            print(f"\nGenerated Cypher Query:")
            print(metadata['cypher_query'])
        
        if metadata.get('query_results_count') is not None:
            print(f"\nQuery Results Count: {metadata['query_results_count']}")
        
        print(f"\nModel Used: {metadata.get('model_used')}")
        
        print("\n" + "=" * 60)
        print("✅ RAG functionality test completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        pipeline.close()

if __name__ == "__main__":
    test_rag_functionality()
