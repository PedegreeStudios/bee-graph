# Bee-Graph

A Neo4j-based knowledge graph system to query content in OpenStax textbooks using LLMs.

## Quick Setup

### 1. Prerequisites
- [Python 3.8+](https://www.python.org/) installed 
- [Docker Desktop](https://www.docker.com/) installed and running
- Git (optional)

### 2. Setup Database
Docker provides a consistent and portable environment for running applications. Neo4j is a graph database optimized for connected data. Using Docker with Neo4j makes it easy to set up, run, and scale graph databases reliably across environments.


#### Clone the repository
```bash
git clone <your-repo-url>
cd bee-graph
```
#### Create virtual environment
```bash
python -m venv venv
```

#### Activate Python virtual environment
```bash
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate
```

#### Install requirements
```bash
pip install -r requirements.txt
````

#### Setup Neo4j database usign Docker
```bash
python scripts/setup_database.py --auto-start-docker --create-database

#### Test database connection
python scripts/setup_database.py --test
```

#### Access Neo4j Browser
 Open in your browser: http://localhost:7474


## Project Structure
```
bee-graph/
├── config/              # Configuration files
├── scripts/             # Setup and utility scripts
├── src/                 # Source code
└── requirements.txt     # Python dependencies
```
