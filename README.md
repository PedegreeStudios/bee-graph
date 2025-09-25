# OpenStax Knowledge Graph - Bee Graph

A Neo4j-based knowledge graph system for OpenStax textbooks with LLM integration.

## Quick Setup

### 1. Prerequisites
- Python 3.8+ installed
- Docker installed and running
- Git (optional)

### 2. Setup Database

#### Option A: Automatic Setup (Recommended)
```bash
# Clone the repository
git clone <your-repo-url>
cd bee-graph

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install requirements
pip install -r requirements.txt

# Setup Neo4j database (automatically starts Docker container)
python scripts/setup_database.py --auto-start-docker --create-database

# Test database connection
python scripts/setup_database.py --test

# Access Neo4j Browser
# Open: http://localhost:7474
```

### 4. Load OpenStax Textbooks
```bash
python scripts/load_textbooks.py
```

### 5. Open LLM Application
```bash
python scripts/llm_app.py
```

## Project Structure
```
bee-graph/
├── config/              # Configuration files
├── scripts/             # Setup and utility scripts
├── src/                 # Source code
└── requirements.txt     # Python dependencies
```
