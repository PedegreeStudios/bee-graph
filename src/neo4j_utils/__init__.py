"""
Neo4j utilities for the OpenStax Knowledge Graph project.

This package contains modules for Neo4j database operations including:
- Schema setup and management
- Database connections and utilities
- Query helpers and utilities
"""

from .schema import Neo4jSchemaSetup

__all__ = ['Neo4jSchemaSetup']
