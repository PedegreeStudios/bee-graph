#!/usr/bin/env python3
"""
Neo4j Schema Setup Script for OpenStax Knowledge Graph

This script manages the database schema including constraints and indexes
for the OpenStax textbook knowledge graph with dual labeling schema.

Features:
- Database constraints creation
- Performance indexes creation
- Schema deletion (constraints and indexes)
- Schema verification
- Error handling and reporting

Usage:
    python setup_neo4j_schema.py --help
    python setup_neo4j_schema.py --setup-schema
    python setup_neo4j_schema.py --delete-schema
    python setup_neo4j_schema.py --verify-schema
"""

import sys
from pathlib import Path
import click

# Add src directory to Python path to enable imports
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))
from neo4j_utils import Neo4jSchemaSetup


@click.command()
@click.option('--uri', default='bolt://localhost:7687', help='Neo4j URI')
@click.option('--username', default='', help='Neo4j username (optional for no-auth)')
@click.option('--password', default='', help='Neo4j password (optional for no-auth)')
@click.option('--database', default='neo4j', help='Database name')
@click.option('--setup-schema', is_flag=True, help='Set up database schema (constraints and indexes)')
@click.option('--delete-schema', is_flag=True, help='Delete all constraints and indexes from database')
@click.option('--verify-schema', is_flag=True, help='Verify schema setup')
@click.option('--show-schema', is_flag=True, help='Show detailed schema information')
@click.option('--test', is_flag=True, help='Test connection only')
def main(uri: str, username: str, password: str, database: str,
         setup_schema: bool, delete_schema: bool, verify_schema: bool, show_schema: bool, test: bool):
    """Set up Neo4j schema for OpenStax Knowledge Graph RAG System."""
    
    print("OPENSTAX KNOWLEDGE GRAPH - NEO4J SCHEMA SETUP")
    print("=" * 60)
    
    setup = Neo4jSchemaSetup(uri, username, password, database)
    
    try:
        # Check if Neo4j is accessible
        if not setup.check_neo4j_connection():
            return
        
        # If just testing connection, exit here
        if test:
            print("Connection test successful!")
            return
        
        # Set up schema if requested
        if setup_schema:
            print(f"\nSetting up schema for database: {database}")
            if not setup.setup_constraints():
                return
            if not setup.setup_indexes():
               return
            print("Schema setup completed successfully!")
        
        # Delete schema if requested
        if delete_schema:
            print(f"\nDeleting schema for database: {database}")
            if not setup.delete_schema():
                return
            print("Schema deletion completed successfully!")
        
        # Verify schema if requested
        if verify_schema:
            print(f"\nVerifying schema for database: {database}")
            verification = setup.verify_schema()
            if verification:
                print("\nVERIFICATION SUMMARY:")
                print(f"   Database: {database}")
                print(f"   Constraints: {verification['constraint_count']}")
                print(f"   Indexes: {verification['index_count']}")
                print(f"   Node types: {len(verification.get('node_counts', []))}")
        
        # Show detailed schema info if requested
        if show_schema:
            print(f"\nDetailed schema information for database: {database}")
            setup.show_schema_info()
        
        if not any([setup_schema, delete_schema, verify_schema, show_schema]):
            print("\nNo schema operations specified.")
            print("Use --setup-schema to create constraints and indexes")
            print("Use --delete-schema to remove all constraints and indexes")
            print("Use --verify-schema to check schema status")
            print("Use --show-schema to display detailed schema information")
        
    except KeyboardInterrupt:
        print("\nSetup cancelled by user")
    except Exception as e:
        print(f"\nSetup failed: {e}")
    finally:
        setup.close()


if __name__ == "__main__":
    main()
