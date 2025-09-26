"""
Chainlit Cypher Query Generator Application

Main Chainlit application that provides a chat interface for generating
Cypher queries for Neo4j knowledge graph using Azure OpenAI.
"""

import os
import json
import logging
import asyncio
from pathlib import Path
from typing import Dict, Any

import chainlit as cl
from chainlit import user_session

from src.chainlit_app.rag_pipeline import GraphRAGPipeline


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Global pipeline instance
pipeline = None


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


@cl.on_chat_start
async def start():
    """Initialize the GraphRAG pipeline when a chat session starts."""
    global pipeline
    
    # Show welcome message
    await cl.Message(
        content="Welcome to the Knowledge Graph Assistant! 🧠\n\n"
                "I can help you explore your educational content by asking questions about your Neo4j knowledge graph. "
                "I'll generate Cypher queries, execute them against your database, and provide intelligent answers.\n\n"
                "Examples of questions you can ask:\n"
                "• What is DNA?\n"
                "• Find all sentences about photosynthesis\n"
                "• What concepts are mentioned in biology textbooks?\n"
                "• Show me information about cellular respiration\n"
                "• What are the main topics in chemistry?\n\n"
                "Let me initialize the system for you..."
    ).send()
    
    # Show loading indicator
    loading_msg = cl.Message(content="🔄 Initializing Knowledge Graph Assistant...")
    await loading_msg.send()
    
    try:
        # Load Neo4j configuration
        neo4j_config = load_neo4j_config()
        
        # Initialize pipeline
        pipeline = GraphRAGPipeline(
            neo4j_params=neo4j_config,
            azure_config_file="src/config/azure_llm_lite.json"
        )
        
        # Test components
        test_results = pipeline.test_components()
        
        # Update loading message with results
        status_parts = []
        if test_results.get('azure_config'):
            status_parts.append("✅ Azure OpenAI configuration loaded")
        else:
            status_parts.append("❌ Azure OpenAI configuration failed")
        
        if test_results.get('neo4j_connection'):
            status_parts.append("✅ Neo4j connection established")
        else:
            status_parts.append("❌ Neo4j connection failed")
        
        if test_results.get('llm'):
            status_parts.append("✅ Language model ready")
        else:
            status_parts.append("❌ Language model failed")
        
        status_text = "\n".join(status_parts)
        
        # Check if all components are working
        all_working = all(test_results.values())
        
        if all_working:
            await loading_msg.update(
                content=f"🎉 System initialized successfully!\n\n{status_text}\n\n"
                        "You can now ask me questions about your educational content!"
            )
        else:
            await loading_msg.update(
                content=f"⚠️ System initialized with some issues:\n\n{status_text}\n\n"
                        "Some features may not work properly. Please check the configuration files."
            )
        
        # Store pipeline in user session
        user_session.set("pipeline", pipeline)
        
    except Exception as e:
        logger.error(f"Error initializing pipeline: {e}")
        await loading_msg.update(
            content=f"❌ Failed to initialize the system: {str(e)}\n\n"
                    "Please check your configuration files and try again."
        )


@cl.on_message
async def main(message: cl.Message):
    """Handle incoming messages and generate responses."""
    global pipeline
    
    # Get pipeline from session
    pipeline = user_session.get("pipeline")
    
    if pipeline is None:
        await cl.Message(
            content="❌ System not properly initialized. Please refresh the page and try again."
        ).send()
        return
    
    # Show that we're processing the message
    processing_msg = cl.Message(content="🧠 Thinking...")
    await processing_msg.send()
    
    try:
        # Process the question through the pipeline
        result = pipeline.query(message.content)
        
        # Update processing message with the response
        rag_response = result['response']
        
        # Format the response nicely
        response_content = rag_response
        
        # Add metadata if available
        metadata = result.get('metadata', {})
        if metadata.get('cypher_query'):
            response_content += f"\n\n---\n\n**Generated Cypher Query:**\n```cypher\n{metadata.get('cypher_query')}\n```"
        
        if metadata.get('query_results_count') is not None:
            response_content += f"\n\n📊 *Found {metadata.get('query_results_count')} results in the database*"
        
        if metadata.get('model_used'):
            response_content += f"\n\n🤖 *Generated using {metadata.get('model_used')} model*"
        
        await processing_msg.update(content=response_content)
        
        # Log the interaction
        logger.info(f"Processed question: {message.content[:100]}...")
        logger.info(f"RAG response generated successfully")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        await processing_msg.update(
            content=f"❌ I encountered an error while processing your question: {str(e)}\n\n"
                    "Please try rephrasing your question or check if the system is properly configured."
        )


@cl.on_chat_end
async def end():
    """Clean up when chat session ends."""
    global pipeline
    
    if pipeline:
        pipeline.close()
        logger.info("Pipeline closed, chat session ended")


# Optional: Add settings and file upload capabilities
@cl.action_callback("test_system")
async def test_system():
    """Test system components."""
    global pipeline
    
    if pipeline is None:
        await cl.Message(content="❌ System not initialized").send()
        return
    
    test_results = pipeline.test_components()
    
    status_parts = []
    for component, status in test_results.items():
        emoji = "✅" if status else "❌"
        status_parts.append(f"{emoji} {component.replace('_', ' ').title()}")
    
    await cl.Message(content="🔧 System Test Results:\n\n" + "\n".join(status_parts)).send()


@cl.action_callback("show_available_models")
async def show_available_models():
    """Show available Azure models."""
    try:
        from src.chainlit_app.azure_config import AzureConfig
        config = AzureConfig("src/config/azure_llm_lite.json")
        models = config.get_available_models()
        
        model_list = "\n".join([f"• {model}" for model in models])
        await cl.Message(content=f"🤖 Available Models:\n\n{model_list}").send()
        
    except Exception as e:
        await cl.Message(content=f"❌ Error loading models: {str(e)}").send()


# Add actions to the UI
@cl.on_chat_start
async def add_actions():
    """Add action buttons to the UI."""
    actions = [
        cl.Action(name="test_system", value="test", label="🔧 Test System"),
        cl.Action(name="show_available_models", value="models", label="🤖 Show Models"),
    ]
    
    await cl.Message(
        content="You can use the action buttons below to test the system or view available models.",
        actions=actions
    ).send()


if __name__ == "__main__":
    # This allows running the app directly with: python llm-app.py
    import subprocess
    import sys
    
    # Run chainlit
    subprocess.run([sys.executable, "-m", "chainlit", "run", "llm-app.py"])
