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
    python scripts/setup_neo4j_schema.py --help
    python scripts/setup_neo4j_schema.py --setup-schema --create-sample-data
    python scripts/setup_neo4j_schema.py --reset-database --setup-schema --create-sample-data
    python scripts/setup_neo4j_schema.py --clear-database
    python scripts/setup_neo4j_schema.py --delete-schema
    python scripts/setup_neo4j_schema.py --verify-schema
"""

import sys
from pathlib import Path
import click

# Add src directory to Python path to enable imports
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))
from neo4j_utils import Neo4jSchemaSetup
from config.config_loader import get_neo4j_connection_params


@click.command()
@click.option('--uri', default=None, help='Neo4j URI (if not provided, loads from config)')
@click.option('--username', default=None, help='Neo4j username (if not provided, loads from config)')
@click.option('--password', default=None, help='Neo4j password (if not provided, loads from config)')
@click.option('--database', default=None, help='Database name (if not provided, loads from config)')
@click.option('--setup-schema', is_flag=True, help='Set up complete database schema (constraints, indexes, and relationships)')
@click.option('--create-sample-data', is_flag=True, help='Create sample nodes and relationships')
@click.option('--delete-schema', is_flag=True, help='Delete all constraints and indexes from database')
@click.option('--clear-database', is_flag=True, help='Clear all nodes and relationships from database')
@click.option('--reset-database', is_flag=True, help='Reset entire database (clear data + delete schema)')
@click.option('--verify-schema', is_flag=True, help='Verify schema setup')
@click.option('--show-schema', is_flag=True, help='Show detailed schema information')
@click.option('--test', is_flag=True, help='Test connection only')
def main(uri: str, username: str, password: str, database: str,
         setup_schema: bool, create_sample_data: bool, 
         delete_schema: bool, clear_database: bool, reset_database: bool,
         verify_schema: bool, show_schema: bool, test: bool):
    
    # Load from config if parameters not provided
    if uri is None or username is None or password is None or database is None:
        config_uri, config_username, config_password, config_database = get_neo4j_connection_params()
        uri = uri if uri is not None else config_uri
        username = username if username is not None else config_username
        password = password if password is not None else config_password
        database = database if database is not None else config_database
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
        
        # Reset database if requested (do this first)
        if reset_database:
            print(f"\nResetting database: {database}")
            if not setup.reset_database():
                return
            print("Database reset completed successfully!")
        
        # Clear database if requested
        if clear_database:
            print(f"\nClearing database: {database}")
            if not setup.clear_database():
                return
            print("Database clearing completed successfully!")
        
        # Set up complete schema if requested (constraints, indexes, and relationships)
        if setup_schema:
            print(f"\nSetting up complete schema for database: {database}")
            print("Creating constraints...")
            if not setup.setup_constraints():
                return
            print("Creating indexes...")
            if not setup.setup_indexes():
               return
            print("Creating relationships...")
            if not setup.setup_relationships():
                return
            print("Complete schema setup finished successfully!")
        
        # Create sample data if requested
        if create_sample_data:
            print(f"\nCreating sample data for database: {database}")
            if not setup.create_sample_data():
                return
            print("Sample data creation completed successfully!")
        
        # Delete schema if requested
        if delete_schema:
            print(f"\nDeleting schema for database: {database}")
            if not setup.delete_schema():
                return
            print("Schema deletion completed successfully!")
        
        # Clear database if requested
        if clear_database:
            print(f"\nClearing database: {database}")
            if not setup.clear_database():
                return
            print("Database clearing completed successfully!")
        
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
        
        if not any([setup_schema, create_sample_data, delete_schema, clear_database, reset_database, verify_schema, show_schema]):
            print("\nNo schema operations specified.")
            print("Use --setup-schema to create complete schema (constraints, indexes, and relationships)")
            print("Use --create-sample-data to create sample nodes and relationships")
            print("Use --delete-schema to remove all constraints and indexes")
            print("Use --clear-database to remove all nodes and relationships")
            print("Use --reset-database to clear data and delete schema (complete reset)")
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
