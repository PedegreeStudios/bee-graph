"""
Centralized configuration loader for Neo4j connections.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)


def load_neo4j_config(config_path: str = "src/config/neo4j_config.json") -> Dict[str, Any]:
    """
    Load Neo4j configuration from config file.
    
    Args:
        config_path: Path to the Neo4j configuration file
        
    Returns:
        Dictionary containing Neo4j connection parameters
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        logger.warning(f"Neo4j config file not found at {config_path}, using defaults")
        return {
            'uri': 'bolt://20.29.35.132:7687',
            'username': 'neo4j',
            'password': 'pedegree',
            'database': 'neo4j',
            'connection_pool_size': 50,
            'max_retry_time': 30,
            'initial_retry_delay': 1.0,
            'max_retry_delay': 30.0,
            'jitter': True
        }
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config.get('neo4j', {})
    except Exception as e:
        logger.error(f"Error loading Neo4j config from {config_path}: {e}")
        return {
            'uri': 'bolt://20.29.35.132:7687',
            'username': 'neo4j',
            'password': 'pedegree',
            'database': 'neo4j',
            'connection_pool_size': 50,
            'max_retry_time': 30,
            'initial_retry_delay': 1.0,
            'max_retry_delay': 30.0,
            'jitter': True
        }


def get_neo4j_connection_params(config_path: str = "src/config/neo4j_config.json") -> tuple:
    """
    Get Neo4j connection parameters as a tuple for easy unpacking.
    
    Args:
        config_path: Path to the Neo4j configuration file
        
    Returns:
        Tuple of (uri, username, password, database)
    """
    config = load_neo4j_config(config_path)
    return (
        config.get('uri', 'bolt://20.29.35.132:7687'),
        config.get('username', 'neo4j'),
        config.get('password', 'pedegree'),
        config.get('database', 'neo4j')
    )
