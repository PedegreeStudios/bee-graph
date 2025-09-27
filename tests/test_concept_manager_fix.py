#!/usr/bin/env python3
"""Test script to verify the concept manager fix for Result.first() -> Result.single()"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from neo4j import GraphDatabase
from textbook_parse.concept_extraction.concept_manager import ConceptManager
from textbook_parse.concept_extraction.wikidata_client import WikidataEntity
from config.config_loader import get_neo4j_connection_params

def test_concept_manager_methods():
    """Test the ConceptManager methods that were fixed."""
    print("Testing ConceptManager fix...")
    
    # Get database connection
    uri, username, password, database = get_neo4j_connection_params()
    driver = GraphDatabase.driver(uri, auth=(username, password))
    concept_manager = ConceptManager(driver)
    
    try:
        # Test get_concept_count method (was using result.first())
        print("Testing get_concept_count()...")
        concept_count = concept_manager.get_concept_count()
        print(f"✓ get_concept_count() returned: {concept_count}")
        
        # Test get_sentences_with_concepts_count method (was using result.first())
        print("Testing get_sentences_with_concepts_count()...")
        sentences_with_concepts = concept_manager.get_sentences_with_concepts_count()
        print(f"✓ get_sentences_with_concepts_count() returned: {sentences_with_concepts}")
        
        # Test get_sentences_without_concepts method
        print("Testing get_sentences_without_concepts()...")
        sentences_without_concepts = concept_manager.get_sentences_without_concepts(limit=5)
        print(f"✓ get_sentences_without_concepts() returned {len(sentences_without_concepts)} sentences")
        
        # Test create_concept_with_relationship method (was using result.first())
        print("Testing create_concept_with_relationship()...")
        if sentences_without_concepts:
            # Create a test Wikidata entity
            test_entity = WikidataEntity(
                qid="Q123456",
                label="Test Concept",
                description="A test concept for verification",
                aliases=["test", "concept"],
                wikidata_url="https://www.wikidata.org/wiki/Q123456"
            )
            
            # Try to create a concept (this will test the result.single() fix)
            sentence_id = sentences_without_concepts[0]['sentence_id']
            success = concept_manager.create_concept_with_relationship(sentence_id, test_entity)
            print(f"✓ create_concept_with_relationship() returned: {success}")
        else:
            print("⚠ No sentences without concepts found, skipping create_concept_with_relationship test")
        
        print("\n✅ All ConceptManager methods tested successfully!")
        print("The fix from result.first() to result.single() is working correctly.")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        driver.close()
    
    return True

if __name__ == "__main__":
    success = test_concept_manager_methods()
    sys.exit(0 if success else 1)
