#!/usr/bin/env python3
"""
Simple Neo4j Database Setup Script for OpenStax Knowledge Graph

This script provides a basic solution for setting up a Neo4j database
connection and creating the database for the OpenStax textbook knowledge graph.

Features:
- Database connection testing
- Database creation
- Configuration file generation
- Automatic Docker container startup
- Docker installation checking

Usage:
    python scripts/setup_database.py --help
    python scripts/setup_database.py --create-database --auto-start-docker  
"""

import json
import subprocess
import time
from pathlib import Path
from typing import Optional
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
import click


class Neo4jDatabaseSetup:
    """Simple Neo4j database setup and configuration manager."""
    
    def __init__(self, uri: str = "bolt://localhost:7687", 
                 username: str = "", 
                 password: str = ""):
        self.uri = uri
        self.username = username
        self.password = password
        self.driver = None
        self.config_dir = Path("src/config")       
        self.config_dir.mkdir(exist_ok=True)
    
    def check_docker_installed(self) -> bool:
        """Check if Docker is installed and running."""
        try:
            result = subprocess.run(['docker', '--version'], 
                                 capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                print("Docker is installed and accessible")
                return True
            else:
                print("Docker is not installed or not accessible")
                return False
        except (subprocess.TimeoutExpired, FileNotFoundError):
            print("Docker is not installed or not accessible")
            return False
        except Exception as e:
            print(f"Error checking Docker: {e}")
            return False
    
    def check_neo4j_container_running(self) -> bool:
        """Check if Neo4j Docker container is running."""
        try:
            result = subprocess.run(['docker', 'ps', '--filter', 'ancestor=neo4j:latest', '--format', '{{.Names}}'], 
                                 capture_output=True, text=True, timeout=10)
            if result.returncode == 0 and result.stdout.strip():
                print("Neo4j Docker container is already running")
                return True
            else:
                print("No Neo4j Docker container is running")
                return False
        except Exception as e:
            print(f"Error checking Neo4j container: {e}")
            return False
    
    def remove_existing_neo4j_containers(self) -> bool:
        """Remove any existing Neo4j containers to start fresh."""
        try:
            print("Checking for existing Neo4j containers...")
            
            # Stop and remove running containers
            result = subprocess.run(['docker', 'ps', '-a', '--filter', 'ancestor=neo4j:latest', '--format', '{{.Names}}'], 
                                 capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0 and result.stdout.strip():
                containers = result.stdout.strip().split('\n')
                print(f"Found {len(containers)} existing Neo4j container(s)")
                
                for container in containers:
                    if container.strip():
                        print(f"Stopping and removing container: {container}")
                        # Stop container
                        subprocess.run(['docker', 'stop', container], capture_output=True, timeout=30)
                        # Remove container
                        subprocess.run(['docker', 'rm', container], capture_output=True, timeout=30)
                        print(f"Removed container: {container}")
                
                print("All existing Neo4j containers removed")
                return True
            else:
                print("No existing Neo4j containers found")
                return True
                
        except Exception as e:
            print(f"Error removing existing containers: {e}")
            return False
    
    def start_neo4j_container(self, remove_existing: bool = False) -> bool:
        """Start Neo4j Docker container."""
        try:
            # Remove existing containers if requested
            if remove_existing:
                self.remove_existing_neo4j_containers()
            
            print("Starting Neo4j Docker container...")
            print("Note: First startup may take longer as Docker downloads the Neo4j image")
            
            cmd = [
                'docker', 'run', '-d',
                '-p', '7474:7474',  # HTTP port for browser
                '-p', '7687:7687',  # Bolt port for database connections
                '-e', 'NEO4J_AUTH=none',
                '--name', 'neo4j-openstax',
                'neo4j:latest'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                print("Neo4j Docker container started successfully")
                print("Waiting for Neo4j to be ready...")
                
                # Wait for Neo4j to be ready (up to 60 seconds)
                for i in range(60):
                    try:
                        self._connect()
                        print("Neo4j is ready!")
                        return True
                    except:
                        time.sleep(1)
                        if i % 10 == 0 and i > 0:
                            print(f"Still waiting for Neo4j... ({i}s)")
                
                print("Neo4j container started but may not be fully ready yet")
                return True
            else:
                print(f"Failed to start Neo4j container: {result.stderr}")
                return False
        except subprocess.TimeoutExpired:
            print("Timeout while starting Neo4j container")
            return False
        except Exception as e:
            print(f"Error starting Neo4j container: {e}")
            return False

    def check_neo4j_connection(self) -> bool:
        """Check if Neo4j is installed and accessible."""
        try:
            self._connect()
            print(f"Neo4j is running and accessible at {self.uri}")
            return True
        except ServiceUnavailable:
            print(f"Neo4j is not running or not accessible")
            print(f"   Expected at: {self.uri}")
            return False
        except AuthError:
            print("Authentication failed")
            print(f"   Username: {self.username}")
            print("   Check your Neo4j credentials or try without authentication")
            return False
        except Exception as e:
            print(f"Error connecting to Neo4j: {e}")
            return False
    
    def _connect(self) -> None:
        """Establish connection to Neo4j database."""
        if self.driver is None:
            # Use no-auth if no username/password provided
            if not self.username and not self.password:
                self.driver = GraphDatabase.driver(self.uri)
            else:
                self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        
        # Test the connection
        with self.driver.session() as session:
            session.run("RETURN 1")
    
    def create_database(self, database_name: str = "neo4j") -> bool:
        """Create a new database for the textbook knowledge graph."""
        try:
            with self.driver.session() as session:
                # Check if database exists
                result = session.run("SHOW DATABASES")
                existing_dbs = [record["name"] for record in result]
                
                if database_name not in existing_dbs:
                    session.run(f"CREATE DATABASE {database_name}")
                    print(f"Created database: {database_name}")
                else:
                    print(f"Database {database_name} already exists")
                
                return True
        except Exception as e:
            print(f"Error creating database: {e}")
            if "Unsupported administration command" in str(e):
                print("Note: Database creation may not be supported in this Neo4j edition")
                print("   Using default database (neo4j)")
            return False
    
    
    def verify_database(self, database_name: str = "neo4j") -> bool:
        """Verify that the database exists and is accessible."""
        try:
            with self.driver.session(database=database_name) as session:
                # Simple test query
                result = session.run("RETURN 1 as test")
                test_value = result.single()["test"]
                
                if test_value == 1:
                    print(f"Database '{database_name}' is accessible and working")
                    return True
                else:
                    print(f"Database '{database_name}' test failed")
                    return False
                
        except Exception as e:
            print(f"Error verifying database: {e}")
            return False
    
    def generate_config_file(self) -> None:
        """Generate configuration file for database connection."""
        config = {
            "neo4j": {
                "uri": self.uri,
                "username": self.username,
                "password": self.password,
                "database": "neo4j",
                "connection_pool_size": 50,
                "max_retry_time": 30,
                "initial_retry_delay": 1.0,
                "max_retry_delay": 30.0,
                "jitter": True
            }
        }
        
        config_file = self.config_dir / "neo4j_config.json"
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
        
        print(f"Configuration file created: {config_file}")
    
    def get_installation_instructions(self) -> str:
        """Return Neo4j installation instructions."""
        return """
        NEO4J INSTALLATION INSTRUCTIONS
        ================================
        
        Option 1: Neo4j Community Edition (Docker) - RECOMMENDED
        --------------------------------------------------------
        1. Install Docker
        2. Run: docker run -p7474:7474 -p7687:7687 -e NEO4J_AUTH=none neo4j:latest
        3. Access Neo4j Browser at: http://localhost:7474
        4. No authentication required (or set your own password)
        
        Option 2: Automatic Docker Container Startup
        ---------------------------------------------
        Use the --auto-start-docker flag to automatically start a Docker container:
        python scripts/setup_database.py --auto-start-docker
        
        After installation, run this script again to set up the database.
        """
    
    def close(self) -> None:
        """Close the database connection."""
        if self.driver:
            self.driver.close()


@click.command()
@click.option('--uri', default='bolt://localhost:7687', help='Neo4j URI')
@click.option('--username', default='', help='Neo4j username (optional for no-auth)')
@click.option('--password', default='', help='Neo4j password (optional for no-auth)')
@click.option('--database', default='neo4j', help='Database name')
@click.option('--create-database', is_flag=True, help='Create the database')
@click.option('--verify', is_flag=True, help='Verify database connection')
@click.option('--test', is_flag=True, help='Test connection only')
@click.option('--auto-start-docker', is_flag=True, help='Automatically start Docker container if not running')
@click.option('--remove-existing', is_flag=True, help='Remove existing Neo4j containers before starting new one')
def main(uri: str, username: str, password: str, database: str,
         create_database: bool, verify: bool, test: bool, auto_start_docker: bool, remove_existing: bool):
    """Set up Neo4j database for OpenStax Knowledge Graph RAG System."""
    
    print("OPENSTAX KNOWLEDGE GRAPH - NEO4J SETUP")
    print("=" * 60)
    
    setup = Neo4jDatabaseSetup(uri, username, password)
    
    try:
        # Check if Neo4j is accessible
        if not setup.check_neo4j_connection():
            # If auto-start-docker is enabled, try to start Docker container
            if auto_start_docker:
                print("\nAttempting to start Neo4j Docker container...")
                
                # Check if Docker is installed
                if not setup.check_docker_installed():
                    print("Docker is not installed. Please install Docker first.")
                    print(setup.get_installation_instructions())
                    return
                
                # Check if Neo4j container is already running
                if setup.check_neo4j_container_running():
                    print("Neo4j container is already running, retrying connection...")
                    if setup.check_neo4j_connection():
                        print("Connection successful!")
                    else:
                        print("Container is running but connection failed")
                        return
                else:
                    # Start the Neo4j container
                    if setup.start_neo4j_container(remove_existing=remove_existing):
                        print("Neo4j container started successfully!")
                    else:
                        print("Failed to start Neo4j container")
                        return
            else:
                print(setup.get_installation_instructions())
                print("\nTip: Use --auto-start-docker to automatically start a Docker container")
                return
        
        # If just testing connection, exit here
        if test:
            print("Connection test successful!")
            return
        
        # Generate configuration file
        setup.generate_config_file()
        
        # Create database if requested
        if create_database:
            if not setup.create_database(database):
                return
        
        # Verify database if requested
        if verify:
            if not setup.verify_database(database):
                return
        
        print(f"\nSETUP COMPLETED SUCCESSFULLY!")
        print(f"   Database: {database}")
        print(f"   URI: {uri}")
        print(f"   Browser: http://localhost:7474")
        
        print(f"\nNext Steps:")
        print(f"   1. Database is ready for data import")
        print(f"   2. Configuration saved to: config/neo4j_config.json")
        print(f"   3. You can now run your data import scripts")
        
    except KeyboardInterrupt:
        print("\nSetup cancelled by user")
    except Exception as e:
        print(f"\nSetup failed: {e}")
    finally:
        setup.close()


if __name__ == "__main__":
    main()
