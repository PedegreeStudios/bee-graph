#!/usr/bin/env python3
"""
Test Script for XML Parser Hierarchy Verification

This script tests the XML parser to verify that the book hierarchy is being
correctly parsed and labeled. It shows the structure without importing to Neo4j.
"""

import sys
from pathlib import Path
from typing import Dict, Any, List

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from textbook_parse.xml_parser import OpenStaxXMLParser


def print_hierarchy_structure(collection_data: Dict[str, Any], parser: OpenStaxXMLParser, indent: int = 0):
    """Print the hierarchical structure of a collection."""
    content = collection_data.get('content', [])
    
    def print_recursive(items: List[Dict[str, Any]], level: int = 0):
        for item in items:
            prefix = "  " * level
            
            if item['type'] == 'module':
                if level == 0:
                    print(f"{prefix}📄 ROOT MODULE: {item['document_id']} (level {item['level']}) → Introduction Chapter")
                else:
                    print(f"{prefix}📄 MODULE: {item['document_id']} (level {item['level']}) → Document")
                
            elif item['type'] == 'subcollection':
                if level == 0:
                    print(f"{prefix}📖 CHAPTER: {item['title']} (level {item['level']})")
                elif level == 1:
                    print(f"{prefix}📑 SUBCHAPTER: {item['title']} (level {item['level']})")
                else:
                    print(f"{prefix}📁 SUBCOLLECTION: {item['title']} (level {item['level']})")
                
                if 'content' in item and item['content']:
                    print_recursive(item['content'], level + 1)
    
    print_recursive(content)
    
    # Show document-level processing hierarchy
    print(f"\n📋 DOCUMENT-LEVEL PROCESSING HIERARCHY:")
    print(f"   📄 Document (from module files)")
    print(f"   ├── 📑 Section (main content sections)")
    print(f"   │   ├── 📄 Subsection (nested sections)")
    print(f"   │   │   └── 📝 Paragraph (text content)")
    print(f"   │   └── 📝 Paragraph (direct paragraphs)")
    print(f"   └── 📝 Paragraph (standalone paragraphs)")
    print(f"   ")
    print(f"   📝 Paragraph → 🔤 Sentence → 🧠 Concept")
    print(f"   ")
    print(f"   🔗 RELATIONSHIPS:")
    print(f"   - DOCUMENT_CONTAINS_SECTION")
    print(f"   - SECTION_CONTAINS_SUBSECTION")
    print(f"   - SECTION_CONTAINS_PARAGRAPH")
    print(f"   - SUBSECTION_CONTAINS_PARAGRAPH")
    print(f"   - PARAGRAPH_CONTAINS_SENTENCE")
    print(f"   - SENTENCE_CONTAINS_CONCEPT (TODO)")


def test_collection_hierarchy(collection_file: Path, parser: OpenStaxXMLParser):
    """Test a single collection's hierarchy parsing."""
    print(f"\n{'='*80}")
    print(f"TESTING COLLECTION: {collection_file.name}")
    print(f"{'='*80}")
    
    # Parse collection
    collection_data = parser.parse_collection(collection_file)
    
    # Print metadata
    metadata = collection_data['metadata']
    print(f"\n📚 BOOK METADATA:")
    print(f"   Title: {metadata.get('title', 'N/A')}")
    print(f"   Language: {metadata.get('language', 'N/A')}")
    print(f"   License: {metadata.get('license', 'N/A')}")
    print(f"   UUID: {metadata.get('uuid', 'N/A')}")
    print(f"   Slug: {metadata.get('slug', 'N/A')}")
    
    # Print hierarchy structure
    print(f"\n🏗️  HIERARCHY STRUCTURE:")
    print_hierarchy_structure(collection_data, parser)
    
    # Extract and count modules
    referenced_modules = parser._extract_module_ids_from_collection(collection_data.get('content', []))
    print(f"\n📊 STATISTICS:")
    print(f"   Total referenced modules: {len(referenced_modules)}")
    
    # Create nodes and relationships (dry run)
    print(f"\n🔧 CREATING NODES AND RELATIONSHIPS:")
    nodes, relationships, document_parent_map = parser.create_nodes_from_collection(collection_data, Path("textbooks/osbooks-biology-bundle"))
    
    # Analyze the created structure
    print(f"   Collection nodes created: {len(nodes)}")
    print(f"   Collection relationships created: {len(relationships)}")
    
    # Group nodes by type
    node_types = {}
    for node_type, node_data in nodes:
        if node_type not in node_types:
            node_types[node_type] = []
        node_types[node_type].append(node_data)
    
    print(f"\n📋 NODE BREAKDOWN:")
    for node_type, node_list in node_types.items():
        print(f"   {node_type}: {len(node_list)} nodes")
        # Show first few examples
        for i, node_data in enumerate(node_list[:3]):
            if node_type == 'Book':
                print(f"     - {node_data.get('title', 'N/A')} (ID: {node_data.get('book_id', 'N/A')})")
            elif node_type == 'Chapter':
                print(f"     - {node_data.get('title', 'N/A')} (ID: {node_data.get('chapter_id', 'N/A')})")
            elif node_type == 'Subchapter':
                print(f"     - {node_data.get('title', 'N/A')} (ID: {node_data.get('subchapter_id', 'N/A')})")
            elif node_type == 'Document':
                print(f"     - {node_data.get('title', 'N/A')} (ID: {node_data.get('document_id', 'N/A')})")
        if len(node_list) > 3:
            print(f"     ... and {len(node_list) - 3} more")
    
    # Group relationships by type
    rel_types = {}
    for rel_type, source_id, target_id in relationships:
        if rel_type not in rel_types:
            rel_types[rel_type] = []
        rel_types[rel_type].append((source_id, target_id))
    
    print(f"\n🔗 RELATIONSHIP BREAKDOWN:")
    for rel_type, rel_list in rel_types.items():
        print(f"   {rel_type}: {len(rel_list)} relationships")
        # Show first few examples
        for i, (source_id, target_id) in enumerate(rel_list[:3]):
            print(f"     - {source_id} -> {target_id}")
        if len(rel_list) > 3:
            print(f"     ... and {len(rel_list) - 3} more")
    
    return len(referenced_modules), len(nodes), len(relationships)


def main():
    """Main test function."""
    print("XML PARSER HIERARCHY TEST")
    print("=" * 80)
    print("This script tests the XML parser to verify correct hierarchy parsing")
    print("=" * 80)
    
    # Initialize parser
    parser = OpenStaxXMLParser()
    
    # Test directory
    textbook_dir = Path("textbooks/osbooks-biology-bundle")
    collections_dir = textbook_dir / "collections"
    
    if not collections_dir.exists():
        print(f"ERROR: Collections directory not found: {collections_dir}")
        return
    
    # Find all collection files
    collection_files = list(collections_dir.glob("*.xml"))
    print(f"\nFound {len(collection_files)} collection files:")
    for cf in collection_files:
        print(f"  - {cf.name}")
    
    # Test each collection
    results = []
    for collection_file in collection_files:
        try:
            modules, nodes, relationships = test_collection_hierarchy(collection_file, parser)
            results.append({
                'file': collection_file.name,
                'modules': modules,
                'nodes': nodes,
                'relationships': relationships
            })
        except Exception as e:
            print(f"\n❌ ERROR testing {collection_file.name}: {e}")
            results.append({
                'file': collection_file.name,
                'modules': 0,
                'nodes': 0,
                'relationships': 0,
                'error': str(e)
            })
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY RESULTS")
    print(f"{'='*80}")
    print(f"{'Collection':<25} {'Modules':<10} {'Nodes':<10} {'Relationships':<15} {'Status'}")
    print("-" * 80)
    
    for result in results:
        status = "✅ OK"
        if 'error' in result:
            status = f"❌ ERROR: {result['error'][:20]}..."
        
        print(f"{result['file']:<25} {result['modules']:<10} {result['nodes']:<10} {result['relationships']:<15} {status}")
    
    print(f"\n🎯 HIERARCHY VERIFICATION:")
    print("   - Each collection should have different module counts")
    print("   - Book -> Chapter -> Subchapter -> Document hierarchy should be clear")
    print("   - Root-level modules should be processed separately")
    print("   - Relationships should properly connect parent-child nodes")
    print("   - Module IDs should match the collection structure")
    print("   ")
    print("   📋 DOCUMENT-LEVEL PROCESSING:")
    print("   - Document nodes created from module files")
    print("   - Section/Subsection/Paragraph/Sentence hierarchy")
    print("   - Content extraction and sentence segmentation")
    print("   - Concept extraction (TODO - not yet implemented)")


if __name__ == "__main__":
    main()
