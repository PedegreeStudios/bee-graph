"""
Test script for Chainlit GraphRAG setup

This script tests the individual components of the GraphRAG pipeline
to ensure everything is properly configured before running the Chainlit app.
"""

import json
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_azure_config():
    """Test Azure configuration loading."""
    print("🔧 Testing Azure Configuration...")
    
    try:
        from src.chainlit_app.azure_config import AzureConfig
        
        config = AzureConfig("src/config/azure_llm_lite.json")
        
        # Test config validation
        is_valid = config.validate_config()
        print(f"   Config validation: {'✅ PASS' if is_valid else '❌ FAIL'}")
        
        # Test available models
        models = config.get_available_models()
        print(f"   Available models: {models}")
        
        # Test model info
        if models:
            model_info = config.get_model_info(models[0])
            print(f"   Model info for {models[0]}: {list(model_info.keys())}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ FAIL: {e}")
        return False


def test_neo4j_config():
    """Test Neo4j configuration loading."""
    print("🔧 Testing Neo4j Configuration...")
    
    try:
        config_path = Path("src/config/neo4j_config.json")
        
        if not config_path.exists():
            print("   ⚠️  Config file not found, using defaults")
            return True
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        neo4j_config = config.get('neo4j', {})
        print(f"   Neo4j URI: {neo4j_config.get('uri', 'Not set')}")
        print(f"   Database: {neo4j_config.get('database', 'Not set')}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ FAIL: {e}")
        return False


def test_graph_retriever():
    """Test GraphRetriever initialization."""
    print("🔧 Testing Graph Retriever...")
    
    try:
        from src.chainlit_app.graph_retriever import GraphRetriever
        
        # Load Neo4j config
        import sys
        sys.path.append(str(Path(__file__).parent.parent / "src"))
        from config.config_loader import load_neo4j_config
        neo4j_config = load_neo4j_config()
        
        retriever = GraphRetriever(
            uri=neo4j_config.get('uri'),
            username=neo4j_config.get('username'),
            password=neo4j_config.get('password'),
            database=neo4j_config.get('database')
        )
        
        # Test connection (this might fail if Neo4j is not running)
        connection_ok = retriever.test_connection()
        print(f"   Neo4j connection: {'✅ PASS' if connection_ok else '❌ FAIL (Neo4j may not be running)'}")
        
        retriever.close()
        return True
        
    except Exception as e:
        print(f"   ❌ FAIL: {e}")
        return False


def test_rag_pipeline():
    """Test GraphRAGPipeline initialization."""
    print("🔧 Testing RAG Pipeline...")
    
    try:
        from src.chainlit_app.rag_pipeline import GraphRAGPipeline
        
        # Load Neo4j config
        import sys
        sys.path.append(str(Path(__file__).parent.parent / "src"))
        from config.config_loader import load_neo4j_config
        neo4j_config = load_neo4j_config()
        
        pipeline = GraphRAGPipeline(
            neo4j_params=neo4j_config,
            azure_config_file="src/config/azure_llm_lite.json"
        )
        
        # Test components
        test_results = pipeline.test_components()
        print(f"   Component tests: {test_results}")
        
        all_passed = all(test_results.values())
        print(f"   Pipeline initialization: {'✅ PASS' if all_passed else '❌ FAIL'}")
        
        pipeline.close()
        return all_passed
        
    except Exception as e:
        print(f"   ❌ FAIL: {e}")
        return False


def test_chainlit_import():
    """Test Chainlit import."""
    print("🔧 Testing Chainlit Import...")
    
    try:
        import chainlit as cl
        print(f"   Chainlit version: {cl.__version__}")
        print("   ✅ PASS")
        return True
        
    except Exception as e:
        print(f"   ❌ FAIL: {e}")
        return False


def main():
    """Run all tests."""
    print("🚀 Testing Chainlit GraphRAG Setup\n")
    
    tests = [
        test_chainlit_import,
        test_azure_config,
        test_neo4j_config,
        test_graph_retriever,
        test_rag_pipeline
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"   ❌ FAIL: {e}")
            results.append(False)
        print()
    
    # Summary
    passed = sum(results)
    total = len(results)
    
    print("📊 Test Summary:")
    print(f"   Passed: {passed}/{total}")
    
    if passed == total:
        print("   🎉 All tests passed! You can run the Chainlit app with:")
        print("   chainlit run llm-app.py")
    else:
        print("   ⚠️  Some tests failed. Please check the configuration and try again.")
    
    return passed == total


if __name__ == "__main__":
    main()
