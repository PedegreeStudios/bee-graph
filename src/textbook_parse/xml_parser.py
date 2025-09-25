"""
OpenStax XML Parser for Neo4j Import

Parses OpenStax collection XML and CNXML module files to extract structured data
for Neo4j database import with dual labeling schema.
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set
import logging
import re
from datetime import datetime
from tqdm import tqdm

# Try to import spaCy, but don't fail if it's not available
try:
    import spacy
    SPACY_AVAILABLE = True
except ImportError:
    spacy = None
    SPACY_AVAILABLE = False

from neo4j_utils.nodes import Neo4jNodeCreator
from neo4j_utils.relationships import Neo4jRelationshipCreator

logger = logging.getLogger(__name__)

class OpenStaxXMLParser:
    """Parser for OpenStax XML/CNXML files with dual labeling schema."""
    
    def __init__(self, neo4j_uri: str = "bolt://localhost:7687", 
                 neo4j_username: str = "", 
                 neo4j_password: str = "",
                 neo4j_database: str = "neo4j"):
        self.namespaces = {
            'col': 'http://cnx.rice.edu/collxml',
            'md': 'http://cnx.rice.edu/mdml',
            'cnxml': 'http://cnx.rice.edu/cnxml'
        }
        self.node_counter = 0
        
        # Initialize Neo4j utilities
        self.node_creator = Neo4jNodeCreator(neo4j_uri, neo4j_username, neo4j_password, neo4j_database)
        self.relationship_creator = Neo4jRelationshipCreator(neo4j_uri, neo4j_username, neo4j_password, neo4j_database)
        
        # Load spaCy model for sentence segmentation if available
        if SPACY_AVAILABLE:
            try:
                self.nlp = spacy.load("en_core_web_sm")
                logger.info("spaCy model loaded successfully")
            except OSError:
                logger.warning("spaCy model 'en_core_web_sm' not found. Using fallback regex splitting.")
                self.nlp = None
            except Exception as e:
                logger.error(f"Failed to load spaCy model: {e}")
                self.nlp = None
        else:
            logger.info("spaCy not available. Using fallback regex sentence splitting.")
            self.nlp = None
    
    def parse_collection(self, collection_path: Path) -> Dict[str, Any]:
        """Parse collection XML to extract book structure."""
        try:
            tree = ET.parse(collection_path)
            root = tree.getroot()
            
            # Extract metadata
            metadata = self._extract_collection_metadata(root)
            
            # Extract content structure
            content = self._extract_collection_content(root)
            
            return {
                'metadata': metadata,
                'content': content,
                'file_path': str(collection_path)
            }
            
        except Exception as e:
            logger.error(f"Error parsing collection {collection_path}: {e}")
            raise
    
    def parse_module(self, module_path: Path) -> Dict[str, Any]:
        """Parse CNXML module to extract content."""
        try:
            tree = ET.parse(module_path)
            root = tree.getroot()
            
            # Extract metadata
            metadata = self._extract_module_metadata(root)
            
            # Extract content sections
            sections = self._extract_module_content(root)
            
            return {
                'metadata': metadata,
                'sections': sections,
                'file_path': str(module_path)
            }
            
        except Exception as e:
            logger.error(f"Error parsing module {module_path}: {e}")
            raise
    
    def _extract_collection_metadata(self, root) -> Dict[str, Any]:
        """Extract metadata from collection."""
        # Try to find metadata element with proper namespace
        metadata_elem = root.find('.//col:metadata', self.namespaces)
        if metadata_elem is None:
            metadata_elem = root.find('.//metadata')
        if metadata_elem is None:
            metadata_elem = root.find('.//md:metadata', self.namespaces)
        if metadata_elem is None:
            logger.warning("No metadata element found in collection")
            return {}
        
        return {
            'title': self._get_text(metadata_elem, './/md:title'),
            'language': self._get_text(metadata_elem, './/md:language'),
            'license': self._get_text(metadata_elem, './/md:license'),
            'uuid': self._get_text(metadata_elem, './/md:uuid'),
            'slug': self._get_text(metadata_elem, './/md:slug')
        }
    
    def _extract_collection_content(self, root) -> List[Dict[str, Any]]:
        """Extract hierarchical content structure from collection."""
        content_elem = root.find('.//col:content', self.namespaces)
        if content_elem is None:
            return []
        
        return self._process_collection_element(content_elem)
    
    def _process_collection_element(self, element, level: int = 0) -> List[Dict[str, Any]]:
        """Recursively process collection content elements."""
        result = []
        
        for child in element:
            tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
            
            if tag == 'module':
                # Extract module reference
                document_id = child.get('document')
                if document_id:
                    result.append({
                        'type': 'module',
                        'document_id': document_id,
                        'level': level
                    })
            
            elif tag == 'subcollection':
                # Extract subcollection
                title = self._get_text(child, './/md:title')
                if title:
                    subcollection = {
                        'type': 'subcollection',
                        'title': title,
                        'level': level,
                        'content': self._process_collection_element(
                            child.find('.//col:content', self.namespaces), 
                            level + 1
                        )
                    }
                    result.append(subcollection)
        
        return result
    
    def _get_module_title(self, document_id: str, textbook_path: Path) -> str:
        """Get the title from a module file by document ID."""
        try:
            # Look for the module file in the modules directory
            module_file = textbook_path / "modules" / document_id / "index.cnxml"
            if module_file.exists():
                # Parse the module to get its title
                module_data = self.parse_module(module_file)
                return module_data['metadata'].get('title', '')
            return ""
        except Exception as e:
            logger.warning(f"Could not extract title for module {document_id}: {e}")
            return ""
    
    def _extract_module_metadata(self, root) -> Dict[str, Any]:
        """Extract metadata from CNXML module."""
        metadata_elem = root.find('cnxml:metadata', self.namespaces)
        if metadata_elem is None:
            return {}
        
        # Extract document title from the title element (not from metadata)
        # The title element is in the cnxml namespace
        title_elem = root.find('cnxml:title', self.namespaces)
        raw_title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""
        document_title = self._filter_citations(raw_title)
        
        # Log title extraction issues for debugging
        content_id = self._get_text(metadata_elem, './/md:content-id')
        fallback_title = self._get_text(metadata_elem, './/md:title')
        final_title = document_title or fallback_title
        
        if not final_title:
            logger.warning(f"Module {content_id} has no title in either title element or metadata")
        # elif not document_title and fallback_title:
        #     # logger.info(f"Module {content_id} using fallback title from metadata: '{fallback_title}'")
        # elif document_title:
        #     logger.debug(f"Module {content_id} extracted title: '{document_title}'")
        
        return {
            'content_id': content_id,
            'title': final_title,
            'abstract': self._get_text(metadata_elem, './/md:abstract'),
            'uuid': self._get_text(metadata_elem, './/md:uuid')
        }
    
    def _extract_module_content(self, root) -> List[Dict[str, Any]]:
        """Extract content sections from CNXML module."""
        content_elem = root.find('.//cnxml:content', self.namespaces)
        if content_elem is None:
            return []
        
        return self._process_module_content(content_elem)
    
    def _process_module_content(self, element, level: int = 0) -> List[Dict[str, Any]]:
        """Recursively process module content elements."""
        result = []
        
        for child in element:
            tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
            
            if tag == 'section':
                section = self._extract_section(child, level)
                if section:
                    result.append(section)
            
            elif tag == 'para':
                paragraph = self._extract_paragraph(child, level)
                if paragraph:
                    result.append(paragraph)
            
            elif tag == 'media':
                # Skip media/figure elements entirely
                continue
            
            elif tag == 'table':
                # Skip table elements entirely
                continue
            
            elif tag == 'term':
                term = self._extract_term(child, level)
                if term:
                    result.append(term)
        
        return result
    
    def _extract_section(self, element, level: int) -> Optional[Dict[str, Any]]:
        """Extract section information."""
        section_id = element.get('id', f'section_{self.node_counter}')
        title = self._get_text(element, './/cnxml:title')
        
        if not title:
            return None
        
        self.node_counter += 1
        
        # Extract subsections and content
        content = self._process_module_content(element, level + 1)
        
        return {
            'type': 'section',
            'section_id': section_id,
            'title': title,
            'level': level,
            'content': content
        }
    
    def _extract_paragraph(self, element, level: int) -> Optional[Dict[str, Any]]:
        """Extract paragraph information."""
        paragraph_id = element.get('id', f'para_{self.node_counter}')
        text = self._extract_text_content(element)
        
        if not text.strip():
            return None
        
        # Split into sentences
        sentences = self._split_into_sentences(text)
        
        # Only create paragraph if it has meaningful sentences
        if not sentences:
            return None
        
        # Sentences are already validated by _split_into_sentences
        # No additional filtering needed here
        if not sentences:
            return None
        
        self.node_counter += 1
        
        return {
            'type': 'paragraph',
            'paragraph_id': paragraph_id,
            'text': text,
            'level': level,
            'sentences': sentences
        }
    

    
    def _extract_table(self, element, level: int) -> Optional[Dict[str, Any]]:
        """Extract table information."""
        table_id = element.get('id', f'tab_{self.node_counter}')
        title = self._get_text(element, './/cnxml:title')
        # Skip caption extraction - ignore captions entirely
        
        self.node_counter += 1
        
        return {
            'type': 'table',
            'table_id': table_id,
            'title': title or f'Table {table_id}',
            'caption': None,  # Set caption to None since we're ignoring it
            'level': level
        }
    
    def _extract_term(self, element, level: int) -> Optional[Dict[str, Any]]:
        """Extract term information."""
        term_id = element.get('id', f'term_{self.node_counter}')
        name = self._get_text(element, './/cnxml:name')
        definition = self._get_text(element, './/cnxml:definition')
        
        if not name or not definition:
            return None
        
        self.node_counter += 1
        
        return {
            'type': 'term',
            'term_id': term_id,
            'name': name,
            'definition': definition,
            'level': level
        }
    
    def _filter_citations(self, text: str) -> str:
        """
        Filter out single character, single number citations, years, and URLs that might be extracted.
        
        Args:
            text: Input text to filter
            
        Returns:
            Filtered text with citations, years, and URLs removed
        """
        if not text:
            return text
        
        # Split text into words and filter out unwanted content
        words = text.split()
        filtered_words = []
        
        for word in words:
            # Skip empty words
            if not word:
                continue
                
            # Filter out years (4-digit numbers like 2023, 1995, etc.)
            if re.match(r'^\d{4}$', word):
                continue
                
            # Filter out years in parentheses like (2014), (1995), etc.
            if re.match(r'^\(\d{4}\)$', word):
                continue
                
            # Filter out URLs (http://, https://, www., etc.)
            if re.match(r'^(https?://|www\.|ftp://)', word, re.IGNORECASE):
                continue
                
            # Filter out email addresses
            if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', word):
                continue
                
            # Skip single punctuation marks and symbols
            if len(word) == 1 and not word.isalnum():
                continue
                
            # Skip words that are just parentheses with content
            if re.match(r'^\([^)]*\)$', word):
                continue
                
            # Keep words that are longer than 1 character
            if len(word) > 1:
                filtered_words.append(word)
            # Also keep single characters that are meaningful (like 'a', 'i', 'o')
            elif len(word) == 1 and word.isalpha() and word.lower() in ['a', 'i', 'o']:
                filtered_words.append(word)
            # Filter out single numbers and other single characters
            else:
                continue
        
        return ' '.join(filtered_words)
    
    def _get_text(self, element, xpath: str) -> str:
        """Get text content from element using xpath."""
        try:
            found = element.find(xpath, self.namespaces)
            raw_text = found.text.strip() if found is not None and found.text else ""
            # Apply citation filtering
            return self._filter_citations(raw_text)
        except Exception:
            return ""
    
    def _extract_text_content(self, element) -> str:
        """Extract all text content from an element recursively."""
        text_parts = []
        
        if element.text and element.text.strip():
            text_parts.append(element.text.strip())
        
        for child in element:
            child_text = self._extract_text_content(child)
            if child_text:
                text_parts.append(child_text)
            if child.tail and child.tail.strip():
                text_parts.append(child.tail.strip())
        
        # Filter out single character text parts that are likely noise
        filtered_parts = []
        for part in text_parts:
            # Skip empty parts
            if not part:
                continue
            # Skip single punctuation marks and symbols
            if len(part) == 1 and not part.isalnum():
                continue
            # Skip single characters except meaningful ones (a, i, o)
            if len(part) == 1 and part.isalpha() and part.lower() not in ['a', 'i', 'o']:
                continue
            # Skip patterns like "(n" or other incomplete fragments
            if re.match(r'^\([a-z]?$', part) or re.match(r'^[a-z]?\)$', part):
                continue
            # Skip years (4-digit numbers)
            if re.match(r'^\d{4}$', part):
                continue
            # Skip years in parentheses
            if re.match(r'^\(\d{4}\)$', part):
                continue
            # Skip any content in parentheses (like citations, years, etc.)
            if re.match(r'^\([^)]*\)$', part):
                continue
            # Keep parts longer than 1 character or meaningful single letters
            if len(part) > 1 or (len(part) == 1 and part.isalpha() and part.lower() in ['a', 'i', 'o']):
                filtered_parts.append(part)
        
        # Join text parts and clean up excessive whitespace
        combined_text = ' '.join(filtered_parts)
        
        # Fix common spacing issues:
        # 1. Remove spaces between letters (like "c o n s t r u c t i o n" -> "construction")
        # 2. Normalize multiple spaces to single spaces
        # 3. Fix spacing around punctuation
        
        # First, normalize whitespace
        combined_text = re.sub(r'\s+', ' ', combined_text)
        
        # Fix the specific issue of spaces between letters in words
        # This pattern looks for single letters separated by spaces and joins them
        # It handles cases like "c o n s t r u c t i o n" -> "construction"
        combined_text = re.sub(r'\b([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\b', 
                              r'\1\2\3\4\5\6\7\8\9\10\11', combined_text)
        combined_text = re.sub(r'\b([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\b', 
                              r'\1\2\3\4\5\6\7\8\9\10', combined_text)
        combined_text = re.sub(r'\b([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\b', 
                              r'\1\2\3\4\5\6\7\8\9', combined_text)
        combined_text = re.sub(r'\b([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\b', 
                              r'\1\2\3\4\5\6\7\8', combined_text)
        combined_text = re.sub(r'\b([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\b', 
                              r'\1\2\3\4\5\6\7', combined_text)
        combined_text = re.sub(r'\b([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\b', 
                              r'\1\2\3\4\5\6', combined_text)
        combined_text = re.sub(r'\b([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\b', 
                              r'\1\2\3\4\5', combined_text)
        combined_text = re.sub(r'\b([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\b', 
                              r'\1\2\3\4', combined_text)
        combined_text = re.sub(r'\b([a-zA-Z])\s+([a-zA-Z])\s+([a-zA-Z])\b', 
                              r'\1\2\3', combined_text)
        combined_text = re.sub(r'\b([a-zA-Z])\s+([a-zA-Z])\b', 
                              r'\1\2', combined_text)
        
        # Fix spacing around punctuation (remove spaces before punctuation)
        combined_text = re.sub(r'\s+([.,!?;:])', r'\1', combined_text)
        
        # Fix spacing after opening parentheses and before closing parentheses
        combined_text = re.sub(r'\(\s+', '(', combined_text)
        combined_text = re.sub(r'\s+\)', ')', combined_text)
        
        # Fix spacing around quotes
        combined_text = re.sub(r'\s+"', '"', combined_text)
        combined_text = re.sub(r'"\s+', '"', combined_text)
        
        # Apply citation filtering to remove single character/number citations
        filtered_text = self._filter_citations(combined_text.strip())
        
        return filtered_text
    
    def _split_into_sentences(self, text: str) -> List[str]:
        """
        Split text into sentences using spaCy for intelligent sentence segmentation.
        
        Args:
            text: Input text to split into sentences
            
        Returns:
            List of clean, valid sentences
        """
        if not text or not text.strip():
            return []
        
        # Clean the text first
        cleaned_text = self._clean_text_for_sentences(text)
        if not cleaned_text:
            return []
        
        # Use spaCy for sentence segmentation if available
        if self.nlp is not None:
            return self._split_with_spacy(cleaned_text)
        else:
            # Fallback to basic sentence splitting
            return self._split_with_regex(cleaned_text)
    
    def _clean_text_for_sentences(self, text: str) -> str:
        """Clean text before sentence segmentation."""
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common XML artifacts and noise
        text = re.sub(r'<[^>]+>', '', text)  # Remove XML tags
        text = re.sub(r'&[a-zA-Z]+;', '', text)  # Remove HTML entities
        
        # Remove parenthesized content (citations, years, etc.)
        text = re.sub(r'\([^)]*\)', '', text)
        
        # Remove standalone years
        text = re.sub(r'\b\d{4}\b', '', text)
        
        # Remove URLs
        text = re.sub(r'https?://\S+', '', text)
        text = re.sub(r'www\.\S+', '', text)
        
        # Remove email addresses
        text = re.sub(r'\S+@\S+\.\S+', '', text)
        
        # Clean up punctuation
        text = re.sub(r'[^\w\s.,!?;:]', '', text)
        
        # Normalize whitespace again
        text = re.sub(r'\s+', ' ', text.strip())
        
        return text
    
    def _split_with_spacy(self, text: str) -> List[str]:
        """Split text into sentences using spaCy."""
        try:
            doc = self.nlp(text)
            sentences = []
            
            for sent in doc.sents:
                sentence_text = sent.text.strip()
                
                # Validate sentence quality
                if self._is_valid_sentence(sentence_text):
                    sentences.append(sentence_text)
            
            return sentences
        except Exception as e:
            logger.warning(f"spaCy sentence splitting failed: {e}")
            return self._split_with_regex(text)
    
    def _split_with_regex(self, text: str) -> List[str]:
        """Fallback sentence splitting using regex."""
        sentences = re.split(r'[.!?]+', text)
        valid_sentences = []
        
        for sentence in sentences:
            sentence = sentence.strip()
            if self._is_valid_sentence(sentence):
                valid_sentences.append(sentence)
        
        return valid_sentences
    
    def _is_valid_sentence(self, sentence: str) -> bool:
        """
        Validate if a sentence is clean and meaningful.
        
        Args:
            sentence: Sentence to validate
            
        Returns:
            True if sentence is valid, False otherwise
        """
        if not sentence or len(sentence.strip()) < 3:
            return False
        
        # Skip sentences that are just punctuation or symbols
        if re.match(r'^[^\w\s]*$', sentence):
            return False
        
        # Skip sentences that are just numbers
        if re.match(r'^\d+$', sentence):
            return False
        
        # Skip sentences that are just single letters (except meaningful ones)
        if len(sentence) == 1 and sentence.isalpha() and sentence.lower() not in ['a', 'i', 'o']:
            return False
        
        # Skip sentences that are just incomplete fragments
        if re.match(r'^[a-z]?$', sentence) or re.match(r'^[a-z]?\)$', sentence):
            return False
        
        # Skip sentences that are just years
        if re.match(r'^\d{4}$', sentence):
            return False
        
        # Skip sentences that are just parenthesized content
        if re.match(r'^\([^)]*\)$', sentence):
            return False
        
        # Skip sentences that are just URLs or email addresses
        if re.match(r'^(https?://|www\.)', sentence, re.IGNORECASE):
            return False
        if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', sentence):
            return False
        
        # Ensure sentence has at least one word character
        if not re.search(r'\w', sentence):
            return False
        
        return True
    
    def create_nodes_from_collection(self, collection_data: Dict[str, Any], textbook_path: Path = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, str]]:
        """Create node data dictionaries from parsed collection data with namespaced IDs."""
        nodes = []
        relationships = []
        document_parent_map = {}  # Maps namespaced_document_id -> namespaced_parent_id
        
        # Create book node data
        metadata = collection_data['metadata']
        
        # Use collection file name as fallback for book_id and title
        collection_file_path = Path(collection_data['file_path'])
        collection_name = collection_file_path.stem
        
        # Get book_id for namespacing all child IDs
        book_id = metadata.get('slug', collection_name)
        
        book_data = {
            'book_id': book_id,
            'title': metadata.get('title', collection_name.replace('-', ' ').title()),
            'uuid': metadata.get('uuid', ''),
            'lens': 'structural',
            'created_at': datetime.now().isoformat()
        }
        nodes.append(('Book', book_data))
        
        # Helper function to create namespaced IDs
        def create_namespaced_id(original_id: str) -> str:
            """Create a namespaced ID to prevent conflicts across textbooks."""
            clean_book_id = book_id.replace('.collection', '').replace('-', '_').replace(' ', '_').lower()
            return f"{clean_book_id}_{original_id}"
        
        # Process content hierarchy recursively
        content = collection_data.get('content', [])
        chapter_counter = 1
        subchapter_counter = 1
        
        def process_content_recursive(content_items, parent_id, parent_type, level=0):
            nonlocal chapter_counter, subchapter_counter
            
            for item in content_items:
                if item['type'] == 'subcollection':
                    if level == 0:
                        # Top-level subcollection = Chapter with namespaced ID
                        namespaced_chapter_id = create_namespaced_id(f"chapter_{chapter_counter}")
                        chapter_data = {
                            'chapter_id': namespaced_chapter_id,
                            'book_id': book_id,
                            'title': item['title'],
                            'uuid': '',
                            'order': chapter_counter,
                            'lens': 'structural',
                            'created_at': datetime.now().isoformat()
                        }
                        nodes.append(('Chapter', chapter_data))
                        chapter_counter += 1
                        
                        # Create BOOK_CONTAINS_CHAPTER relationship
                        relationships.append(('BOOK_CONTAINS_CHAPTER', book_id, namespaced_chapter_id))
                        
                        # Process this chapter's content recursively
                        process_content_recursive(item.get('content', []), namespaced_chapter_id, 'chapter', level + 1)
                        
                    elif level == 1 and parent_type == 'chapter':
                        # Second-level subcollection = Subchapter with namespaced ID
                        subchapter_order = subchapter_counter
                        namespaced_subchapter_id = create_namespaced_id(f"subchapter_{subchapter_counter}")
                        subchapter_data = {
                            'subchapter_id': namespaced_subchapter_id,
                            'chapter_id': parent_id,
                            'title': item['title'],
                            'uuid': '',
                            'order': subchapter_order,
                            'lens': 'structural',
                            'created_at': datetime.now().isoformat()
                        }
                        nodes.append(('Subchapter', subchapter_data))
                        subchapter_counter += 1
                        
                        # Create CHAPTER_CONTAINS_SUBCHAPTER relationship
                        relationships.append(('CHAPTER_CONTAINS_SUBCHAPTER', parent_id, namespaced_subchapter_id))
                        
                        # Process this subchapter's content recursively
                        process_content_recursive(item.get('content', []), namespaced_subchapter_id, 'subchapter', level + 1)
                    
                    else:
                        logger.warning(f"Unexpected subcollection at level {level} with parent type {parent_type}: {item.get('title', 'Unknown')}")
                        
                elif item['type'] == 'module':
                    # Create Document node with namespaced ID and establish relationships during collection processing
                    original_document_id = item['document_id']
                    if not original_document_id:
                        logger.error(f"Module item missing document_id: {item}")
                        continue
                        
                    # Create namespaced document ID to prevent cross-textbook conflicts
                    namespaced_document_id = create_namespaced_id(original_document_id)
                    
                    # Handle root-level modules (level 0) - skip them for now
                    # We'll process them later by checking actual module files
                    if level == 0 and not parent_id:
                        logger.warning(f"Root-level module {original_document_id} found - will be processed separately")
                        continue
                    
                    # Validate parent_id is not empty
                    if not parent_id:
                        logger.error(f"Module {original_document_id} has no parent_id - this indicates a structural issue")
                        continue
                    
                    # Create Document node with fallback title
                    # Generate a proper fallback title based on original document_id
                    if original_document_id.startswith('m') and len(original_document_id) > 1 and original_document_id[1:].isdigit():
                        fallback_title = f"Module {original_document_id[1:]}"
                    elif original_document_id:
                        fallback_title = f"Module {original_document_id}"
                    else:
                        fallback_title = "Untitled Module"
                    
                    document_data = {
                        'document_id': namespaced_document_id,
                        'book_id': book_id,
                        'title': fallback_title,  # Good fallback title, will be updated from module metadata if available
                        'uuid': "",  # Will be updated from module metadata
                        'lens': 'structural',
                        'created_at': datetime.now().isoformat()
                    }
                    # logger.info(f"COLLECTION: Created document with namespaced_document_id='{namespaced_document_id}' (original: '{original_document_id}'), title='{fallback_title}'")
                    nodes.append(('Document', document_data))
                    
                    # Create the appropriate parent-child relationship immediately using namespaced IDs
                    if parent_type == 'chapter':
                        relationships.append(('CHAPTER_CONTAINS_DOCUMENT', parent_id, namespaced_document_id))
                        logger.debug(f"Created CHAPTER_CONTAINS_DOCUMENT relationship: {parent_id} -> {namespaced_document_id}")
                    elif parent_type == 'subchapter':
                        relationships.append(('SUBCHAPTER_CONTAINS_DOCUMENT', parent_id, namespaced_document_id))
                        logger.debug(f"Created SUBCHAPTER_CONTAINS_DOCUMENT relationship: {parent_id} -> {namespaced_document_id}")
                    else:
                        logger.error(f"Invalid parent_type '{parent_type}' for document {original_document_id}")
                        
                    # Store the relationship mapping for later title updates using namespaced IDs
                    document_parent_map[namespaced_document_id] = parent_id
                    logger.debug(f"Created document {namespaced_document_id} (original: {original_document_id}) under parent {parent_id} (type: {parent_type})")
                    
                else:
                    logger.warning(f"Unknown content item type: {item.get('type', 'Unknown')} - {item}")
        
        # Pre-process content to handle root-level modules
        processed_content = []
        root_modules = []
        
        for item in content:
            if item['type'] == 'module':
                root_modules.append(item)
            else:
                processed_content.append(item)
        
        # Skip root-level modules - they will be processed separately by checking actual module files
        if root_modules:
            logger.info(f"Found {len(root_modules)} root-level modules - will check modules directory for actual introduction chapters")
        
        # Start recursive processing from the root level with processed content
        process_content_recursive(processed_content, book_id, 'book', 0)
        
        return nodes, relationships, document_parent_map
    
    def create_nodes_from_module(self, module_data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
        """Create node data dictionaries from parsed module data with namespaced document lookup."""
        nodes = []
        relationships = []
        
        # Note: Document node should already exist from collection parsing with namespaced ID
        # We'll update its title later in the import process
        metadata = module_data['metadata']
        original_document_id = metadata.get('content_id', '')
        
        # Add debugging to see what's happening
        # logger.info(f"Processing module with original content_id: {original_document_id}")
        
        # Validate document_id is not empty
        if not original_document_id:
            logger.error(f"Module metadata missing content_id! Metadata: {metadata}")
            # Cannot create valid relationships without document_id
            return [], [], {'document_id': '', 'title': '', 'uuid': '', 'abstract': None}
        
        # Find the namespaced document ID from the document map
        namespaced_document_id = None
        book_id = None
        
        if hasattr(self, '_debug_document_map'):
            # Look for a namespaced document ID that contains the original ID
            for ns_doc_id in self._debug_document_map.keys():
                if ns_doc_id.endswith(f"_{original_document_id}"):
                    namespaced_document_id = ns_doc_id
                    # Extract book_id from namespaced ID
                    book_id = ns_doc_id.split(f"_{original_document_id}")[0]
                    break
            
            if not namespaced_document_id:
                # If not found in collection structure, create a standalone document
                # logger.info(f"Module {original_document_id} not in collection structure - creating standalone document")
                # Extract book_id from the first available document in the map
                if self._debug_document_map:
                    first_doc_id = list(self._debug_document_map.keys())[0]
                    book_id = first_doc_id.split('_')[0] + '_' + first_doc_id.split('_')[1]  # Extract book prefix
                else:
                    book_id = "unknown_book"
                
                namespaced_document_id = f"{book_id}_{original_document_id}"
        else:
            logger.warning(f"No document map available - creating standalone document for {original_document_id}")
            book_id = "unknown_book"
            namespaced_document_id = f"{book_id}_{original_document_id}"
        
        # logger.info(f"Found namespaced document_id: {namespaced_document_id} for original: {original_document_id}")
        
        # Check if this is a standalone document (not in collection structure)
        is_standalone = not hasattr(self, '_debug_document_map') or not any(ns_doc_id.endswith(f"_{original_document_id}") for ns_doc_id in self._debug_document_map.keys())
        
        if is_standalone:
            # Create a new Document node for standalone modules
            document_data = {
                'document_id': namespaced_document_id,
                'book_id': book_id,
                'title': metadata.get('title', f'Module {original_document_id}'),
                'uuid': metadata.get('uuid', ''),
                'lens': 'structural',
                'created_at': datetime.now().isoformat()
            }
            nodes.append(('Document', document_data))
            # logger.info(f"Created standalone Document node: {namespaced_document_id}")
        
        # Create a document update object (not a new node) - use namespaced ID for database operations
        document_update = {
            'document_id': namespaced_document_id,
            'title': metadata.get('title', ''),
            'uuid': metadata.get('uuid', ''),
            'abstract': metadata.get('abstract')
        }
        
        # Process sections and standalone content
        sections = module_data.get('sections', [])
        for section_data in sections:
            if section_data['type'] == 'section':
                section_id = section_data.get('section_id', '')
                section_title = section_data.get('title', '')
                
                # Validate section data
                if not section_id:
                    logger.warning(f"Section missing section_id! Section data: {section_data}")
                    continue
                if not section_title:
                    logger.warning(f"Section {section_id} missing title! Section data: {section_data}")
                
                # Create namespaced section ID
                namespaced_section_id = f"{book_id}_{section_id}"
                
                section_data = {
                    'section_id': namespaced_section_id,
                    'subchapter_id': None,  # Will be set based on hierarchy
                    'document_id': namespaced_document_id,
                    'title': section_title,
                    'uuid': '',
                    'order': len(nodes),
                    'lens': 'structural',
                    'created_at': datetime.now().isoformat()
                }
                nodes.append(('Section', section_data))
                # Create DOCUMENT_CONTAINS_SECTION relationship using namespaced IDs
                # logger.info(f"Creating DOCUMENT_CONTAINS_SECTION: document_id='{namespaced_document_id}' -> section_id='{namespaced_section_id}'")
                relationships.append(('DOCUMENT_CONTAINS_SECTION', namespaced_document_id, namespaced_section_id))
                # logger.info(f"Created relationship: source='{namespaced_document_id}' -> target='{namespaced_section_id}' (type: DOCUMENT_CONTAINS_SECTION)")
                
                # Document should exist since we checked above
                if hasattr(self, '_debug_document_map') and namespaced_document_id in self._debug_document_map:
                    logger.debug(f"Document {namespaced_document_id} confirmed in document_parent_map")
                
                # Process section content
                for content_item in section_data.get('content', []):
                    if content_item['type'] == 'section':
                        # This is a subsection within a section
                        subsection_id = content_item.get('section_id', '')
                        subsection_title = content_item.get('title', '')
                        
                        if not subsection_id:
                            logger.warning(f"Subsection missing section_id! Subsection data: {content_item}")
                            continue
                        if not subsection_title:
                            logger.warning(f"Subsection {subsection_id} missing title! Subsection data: {content_item}")
                        
                        # Create namespaced subsection ID
                        namespaced_subsection_id = f"{book_id}_{subsection_id}"
                        
                        subsection_data = {
                            'subsection_id': namespaced_subsection_id,
                            'section_id': namespaced_section_id,
                            'title': subsection_title,
                            'uuid': '',
                            'order': len(nodes),
                            'lens': 'structural',
                            'created_at': datetime.now().isoformat()
                        }
                        nodes.append(('Subsection', subsection_data))
                        
                        # Create SECTION_CONTAINS_SUBSECTION relationship
                        relationships.append(('SECTION_CONTAINS_SUBSECTION', namespaced_section_id, namespaced_subsection_id))
                        
                        # Process subsection content (paragraphs)
                        for subsection_content in content_item.get('content', []):
                            if subsection_content['type'] == 'paragraph':
                                # Create namespaced paragraph ID
                                original_paragraph_id = subsection_content['paragraph_id']
                                namespaced_paragraph_id = f"{book_id}_{original_paragraph_id}"
                                
                                paragraph_data = {
                                    'paragraph_id': namespaced_paragraph_id,
                                    'subsection_id': namespaced_subsection_id,
                                    'text': subsection_content['text'],
                                    'uuid': '',
                                    'order': len(nodes),
                                    'lens': 'content',
                                    'created_at': datetime.now().isoformat()
                                }
                                nodes.append(('Paragraph', paragraph_data))
                                
                                # Create SUBSECTION_CONTAINS_PARAGRAPH relationship
                                relationships.append(('SUBSECTION_CONTAINS_PARAGRAPH', namespaced_subsection_id, namespaced_paragraph_id))
                                
                                # Create sentence nodes
                                for i, sentence_text in enumerate(subsection_content.get('sentences', [])):
                                    sentence_data = {
                                        'sentence_id': f"{namespaced_paragraph_id}_sent_{i}",
                                        'paragraph_id': namespaced_paragraph_id,
                                        'text': sentence_text,
                                        'uuid': '',
                                        'order': i,
                                        'lens': 'content',
                                        'created_at': datetime.now().isoformat()
                                    }
                                    nodes.append(('Sentence', sentence_data))
                                    
                                    # Create PARAGRAPH_CONTAINS_SENTENCE relationship
                                    relationships.append(('PARAGRAPH_CONTAINS_SENTENCE', namespaced_paragraph_id, sentence_data['sentence_id']))
                    
                    elif content_item['type'] == 'paragraph':
                        # Create namespaced paragraph ID
                        original_paragraph_id = content_item['paragraph_id']
                        namespaced_paragraph_id = f"{book_id}_{original_paragraph_id}"
                        
                        paragraph_data = {
                            'paragraph_id': namespaced_paragraph_id,
                            'subsection_id': None,  # Will be set based on hierarchy
                            'text': content_item['text'],
                            'uuid': '',
                            'order': len(nodes),
                            'lens': 'content',
                            'created_at': datetime.now().isoformat()
                        }
                        nodes.append(('Paragraph', paragraph_data))
                        
                        # Create SECTION_CONTAINS_PARAGRAPH relationship
                        relationships.append(('SECTION_CONTAINS_PARAGRAPH', namespaced_section_id, namespaced_paragraph_id))
                        
                        # Create sentence nodes
                        for i, sentence_text in enumerate(content_item.get('sentences', [])):
                            sentence_data = {
                                'sentence_id': f"{namespaced_paragraph_id}_sent_{i}",
                                'paragraph_id': namespaced_paragraph_id,
                                'text': sentence_text,
                                'uuid': '',
                                'order': i,
                                'lens': 'content',
                                'created_at': datetime.now().isoformat()
                            }
                            nodes.append(('Sentence', sentence_data))
                            
                            # Create PARAGRAPH_CONTAINS_SENTENCE relationship
                            relationships.append(('PARAGRAPH_CONTAINS_SENTENCE', namespaced_paragraph_id, sentence_data['sentence_id']))
                            
                            # TODO: Add concept extraction from sentences here
                            # This would create SENTENCE_CONTAINS_CONCEPT relationships
                            # when concepts are identified within sentence text
                    
                    elif content_item['type'] == 'figure':
                        # Skip figure elements entirely
                        continue
                    
                    elif content_item['type'] == 'table':
                        # Skip table elements entirely
                        continue
                    
                    elif content_item['type'] == 'term':
                        continue
            
            elif section_data['type'] == 'paragraph':
                # Create namespaced paragraph ID for standalone paragraphs
                original_paragraph_id = section_data['paragraph_id']
                namespaced_paragraph_id = f"{book_id}_{original_paragraph_id}"
                
                paragraph_data = {
                    'paragraph_id': namespaced_paragraph_id,
                    'subsection_id': None,  # Will be set based on hierarchy
                    'text': section_data['text'],
                    'uuid': '',
                    'order': len(nodes),
                    'lens': 'content',
                    'created_at': datetime.now().isoformat()
                }
                nodes.append(('Paragraph', paragraph_data))
                
                # Create DOCUMENT_CONTAINS_PARAGRAPH relationship using namespaced IDs
                relationships.append(('DOCUMENT_CONTAINS_PARAGRAPH', namespaced_document_id, namespaced_paragraph_id))
                
                # Create sentence nodes
                for i, sentence_text in enumerate(section_data.get('sentences', [])):
                    sentence_data = {
                        'sentence_id': f"{namespaced_paragraph_id}_sent_{i}",
                        'paragraph_id': namespaced_paragraph_id,
                        'text': sentence_text,
                        'uuid': '',
                        'order': i,
                        'lens': 'content',
                        'created_at': datetime.now().isoformat()
                    }
                    nodes.append(('Sentence', sentence_data))
                    
                    # Create PARAGRAPH_CONTAINS_SENTENCE relationship
                    relationships.append(('PARAGRAPH_CONTAINS_SENTENCE', namespaced_paragraph_id, sentence_data['sentence_id']))
                    
                    # TODO: Add concept extraction from sentences here
                    # This would create SENTENCE_CONTAINS_CONCEPT relationships
                    # when concepts are identified within sentence text
            
            elif section_data['type'] == 'figure':
                # Skip figure elements entirely
                continue
            
            elif section_data['type'] == 'table':
                # Skip table elements entirely
                continue
            
            elif section_data['type'] == 'term':
                continue
        
        # logger.info(f"Module processing complete: {len(nodes)} nodes, {len(relationships)} relationships created for namespaced_document_id='{namespaced_document_id}' (original: '{original_document_id})')")
        return nodes, relationships, document_update
    
    def create_nodes_in_neo4j(self, nodes: List[Tuple[str, Dict[str, Any]]]) -> int:
        """Create nodes in Neo4j database using the node creator."""
        success_count = 0
        
        # Group nodes by type for batch processing
        nodes_by_type = {}
        for node_type, node_data in nodes:
            if node_type not in nodes_by_type:
                nodes_by_type[node_type] = []
            nodes_by_type[node_type].append(node_data)
        
        # Create nodes by type
        for node_type, node_list in nodes_by_type.items():
            try:
                count = self.node_creator.create_nodes_batch(node_list, node_type)
                success_count += count
                    # logger.info(f"Created {count}/{len(node_list)} {node_type} nodes")
            except Exception as e:
                logger.error(f"Error creating {node_type} nodes: {e}")
        
        return success_count
    
    def create_relationships_in_neo4j(self, relationships: List[Tuple[str, str, str]]) -> int:
        """Create relationships in Neo4j database using the relationship creator."""
        success_count = 0
        
        for rel_type, source_id, target_id in relationships:
            try:
                # Map relationship types to method names
                method_map = {
                    'BOOK_CONTAINS_CHAPTER': self.relationship_creator.create_book_contains_chapter_relationship,
                    'CHAPTER_CONTAINS_SUBCHAPTER': self.relationship_creator.create_chapter_contains_subchapter_relationship,
                    'CHAPTER_CONTAINS_DOCUMENT': self.relationship_creator.create_chapter_contains_document_relationship,
                    'SUBCHAPTER_CONTAINS_DOCUMENT': self.relationship_creator.create_subchapter_contains_document_relationship,
                    'DOCUMENT_CONTAINS_SECTION': self.relationship_creator.create_document_contains_section_relationship,
                    'DOCUMENT_CONTAINS_PARAGRAPH': self.relationship_creator.create_document_contains_paragraph_relationship,
                    'SECTION_CONTAINS_SUBSECTION': self.relationship_creator.create_section_contains_subsection_relationship,
                    'SECTION_CONTAINS_PARAGRAPH': self.relationship_creator.create_section_contains_paragraph_relationship,
                    'SUBSECTION_CONTAINS_PARAGRAPH': self.relationship_creator.create_subsection_contains_paragraph_relationship,
                    'PARAGRAPH_CONTAINS_SENTENCE': self.relationship_creator.create_paragraph_contains_sentence_relationship,
                    'SENTENCE_CONTAINS_CONCEPT': self.relationship_creator.create_sentence_contains_concept_relationship,
                }
                
                if rel_type in method_map:
                    if method_map[rel_type](source_id, target_id):
                        success_count += 1
                else:
                    logger.warning(f"Unknown relationship type: {rel_type}")
                    
            except Exception as e:
                logger.error(f"Error creating relationship {rel_type}: {source_id} -> {target_id}: {e}")
        
        return success_count
    
    def create_bidirectional_relationships_in_neo4j(self, relationships: List[Tuple[str, str, str]]) -> int:
        """Create bidirectional relationships (BELONGS_TO) in Neo4j database."""
        success_count = 0
        
        # Map CONTAINS relationships to their BELONGS_TO counterparts
        bidirectional_map = {
            'BOOK_CONTAINS_CHAPTER': ('CHAPTER_BELONGS_TO_BOOK', self.relationship_creator.create_chapter_belongs_to_book_relationship),
            'CHAPTER_CONTAINS_SUBCHAPTER': ('SUBCHAPTER_BELONGS_TO_CHAPTER', self.relationship_creator.create_subchapter_belongs_to_chapter_relationship),
            'CHAPTER_CONTAINS_DOCUMENT': ('DOCUMENT_BELONGS_TO_CHAPTER', self.relationship_creator.create_document_belongs_to_chapter_relationship),
            'SUBCHAPTER_CONTAINS_DOCUMENT': ('DOCUMENT_BELONGS_TO_SUBCHAPTER', self.relationship_creator.create_document_belongs_to_subchapter_relationship),
            'DOCUMENT_CONTAINS_SECTION': ('SECTION_BELONGS_TO_DOCUMENT', self.relationship_creator.create_section_belongs_to_document_relationship),
            'DOCUMENT_CONTAINS_PARAGRAPH': ('PARAGRAPH_BELONGS_TO_DOCUMENT', self.relationship_creator.create_paragraph_belongs_to_document_relationship),
            'SECTION_CONTAINS_SUBSECTION': ('SUBSECTION_BELONGS_TO_SECTION', self.relationship_creator.create_subsection_belongs_to_section_relationship),
            'SECTION_CONTAINS_PARAGRAPH': ('PARAGRAPH_BELONGS_TO_SECTION', self.relationship_creator.create_paragraph_belongs_to_section_relationship),
            'SUBSECTION_CONTAINS_PARAGRAPH': ('PARAGRAPH_BELONGS_TO_SUBSECTION', self.relationship_creator.create_paragraph_belongs_to_subsection_relationship),
            'PARAGRAPH_CONTAINS_SENTENCE': ('SENTENCE_BELONGS_TO_PARAGRAPH', self.relationship_creator.create_sentence_belongs_paragraph_relationship),
            'SENTENCE_CONTAINS_CONCEPT': ('CONCEPT_BELONGS_TO_SENTENCE', self.relationship_creator.create_concept_belongs_to_sentence_relationship),
        }
        
        for rel_type, source_id, target_id in relationships:
            if rel_type in bidirectional_map:
                reverse_rel_type, method = bidirectional_map[rel_type]
                try:
                    # Note: For BELONGS_TO relationships, source and target are swapped
                    if method(target_id, source_id):
                        success_count += 1
                except Exception as e:
                    logger.error(f"Error creating bidirectional relationship {reverse_rel_type}: {target_id} -> {source_id}: {e}")
        
        return success_count
    
    def create_concept_sentence_relationships(self, sentence_id: str, concepts: List[str], book_id: str) -> List[Tuple[str, str, str]]:
        """Create concept nodes and SENTENCE_CONTAINS_CONCEPT relationships for extracted concepts."""
        relationships = []
        
        for i, concept_text in enumerate(concepts):
            if not concept_text.strip():
                continue
                
            # Create namespaced concept ID
            concept_id = f"{book_id}_concept_{sentence_id}_{i}"
            
            concept_data = {
                'concept_id': concept_id,
                'text': concept_text.strip(),
                'wikidata_id': None,
                'wikidata_name': None,
                'uuid': '',
                'lens': 'semantic',
                'created_at': datetime.now().isoformat()
            }
            
            # Add concept node (this would need to be added to the nodes list)
            # For now, we'll just create the relationship
            relationships.append(('SENTENCE_CONTAINS_CONCEPT', sentence_id, concept_id))
            
        return relationships
    
    def update_document_in_neo4j(self, document_update: Dict[str, Any]) -> bool:
        """Update document metadata in Neo4j database."""
        try:
            # Ensure driver is connected
            if not self.node_creator.driver:
                self.node_creator._connect()
            
            with self.node_creator.driver.session(database=self.node_creator.database) as session:
                # First try to find the document by the provided document_id
                query = """
                MATCH (d:Document {document_id: $document_id})
                SET d.title = $title,
                    d.uuid = $uuid,
                    d.abstract = $abstract
                RETURN d
                """
                result = session.run(query, document_update)
                if result.single():
                    return True
                
                # If not found, try to find by original module ID (extract from namespaced ID)
                original_module_id = document_update['document_id'].split('_')[-1]
                fallback_query = """
                MATCH (d:Document) 
                WHERE d.document_id ENDS WITH $module_id
                SET d.title = $title,
                    d.uuid = $uuid,
                    d.abstract = $abstract
                RETURN d
                """
                result = session.run(fallback_query, {
                    'module_id': original_module_id,
                    'title': document_update['title'],
                    'uuid': document_update['uuid'],
                    'abstract': document_update['abstract']
                })
                if result.single():
                    return True
                
                logger.warning(f"Document {document_update['document_id']} not found for update")
                return False
        except Exception as e:
            logger.error(f"Error updating document {document_update['document_id']}: {e}")
            return False
    
    def _extract_module_ids_from_collection(self, content_items: List[Dict[str, Any]]) -> Set[str]:
        """Extract all module IDs referenced in a collection structure."""
        module_ids = set()
        
        def extract_recursive(items):
            for item in items:
                if item['type'] == 'module':
                    module_ids.add(item['document_id'])
                elif item['type'] == 'subcollection' and 'content' in item:
                    extract_recursive(item['content'])
        
        extract_recursive(content_items)
        return module_ids

    def load_collection(self, collection_file: Path, textbook_dir: Path, dry_run: bool = False, bulk_importer=None, batch_size: int = 1000) -> bool:
        """Load a single collection and its modules."""
        try:
            print(f"  Parsing collection: {collection_file.name}")
            
            # Parse collection
            collection_data = self.parse_collection(collection_file)
            
            # Create nodes and relationships from collection
            nodes, relationships, document_parent_map = self.create_nodes_from_collection(collection_data, textbook_dir)
            
            print(f"    Collection: {len(nodes)} nodes, {len(relationships)} relationships")
            
            if not dry_run:
                if bulk_importer:
                    # Use bulk import for better performance
                    node_count = bulk_importer.bulk_create_nodes(nodes, batch_size)
                    rel_count = bulk_importer.bulk_create_relationships(relationships, batch_size)
                    bidir_count = bulk_importer.bulk_create_bidirectional_relationships(relationships, batch_size)
                else:
                    # Use standard import methods
                    node_count = self.create_nodes_in_neo4j(nodes)
                    rel_count = self.create_relationships_in_neo4j(relationships)
                    bidir_count = self.create_bidirectional_relationships_in_neo4j(relationships)
            
            # Set up document map for module processing
            self._debug_document_map = document_parent_map
            
            # Process only modules referenced in this collection
            modules_dir = textbook_dir / "modules"
            if modules_dir.exists():
                # Extract module IDs referenced in this collection
                referenced_module_ids = self._extract_module_ids_from_collection(collection_data.get('content', []))
                
                processed_modules = 0
                total_nodes = 0
                total_relationships = 0
                
                # Process only the referenced modules with progress bar
                with tqdm(total=len(referenced_module_ids), desc="Processing modules", unit="module", leave=False) as pbar:
                    for module_id in referenced_module_ids:
                        try:
                            # Look for the module file
                            module_file = modules_dir / module_id / "index.cnxml"
                            if not module_file.exists():
                                logger.warning(f"Module file not found: {module_file}")
                                continue
                            
                            # Parse module
                            module_data = self.parse_module(module_file)
                            
                            # Create nodes and relationships from module
                            module_nodes, module_relationships, document_update = self.create_nodes_from_module(module_data)
                            
                            if module_nodes:  # Only process if we found nodes
                                total_nodes += len(module_nodes)
                                total_relationships += len(module_relationships)
                                
                                if not dry_run:
                                    if bulk_importer:
                                        # Use bulk import for module nodes
                                        module_node_count = bulk_importer.bulk_create_nodes(module_nodes, batch_size)
                                        module_rel_count = bulk_importer.bulk_create_relationships(module_relationships, batch_size)
                                        module_bidir_count = bulk_importer.bulk_create_bidirectional_relationships(module_relationships, batch_size)
                                    else:
                                        # Use standard import methods
                                        module_node_count = self.create_nodes_in_neo4j(module_nodes)
                                        module_rel_count = self.create_relationships_in_neo4j(module_relationships)
                                        module_bidir_count = self.create_bidirectional_relationships_in_neo4j(module_relationships)
                                    
                                    # Update document metadata - this should happen regardless of dry_run
                                    if document_update.get('title'):
                                        if not dry_run:
                                            self.update_document_in_neo4j(document_update)
                            
                            processed_modules += 1
                            pbar.update(1)
                            
                        except Exception as e:
                            logger.warning(f"Error processing module {module_id}: {e}")
                            continue
                
                print(f"    Processed {processed_modules} modules ({total_nodes} nodes, {total_relationships} relationships)")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading collection {collection_file}: {e}")
            return False

    def clear_sample_data(self, uri: str, username: str, password: str, database: str) -> bool:
        """Clear sample data and existing textbook data from the database."""
        try:
            from neo4j_utils import Neo4jSchemaSetup
            schema_setup = Neo4jSchemaSetup(uri, username, password, database)
            
            if not schema_setup.check_neo4j_connection():
                print("Failed to connect to Neo4j database")
                return False
            
            print("Clearing all existing data (sample data and textbooks)...")
            
            # Clear all nodes and relationships
            if not schema_setup.clear_database():
                print("Failed to clear database")
                return False
            
            print("Database cleared successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing sample data: {e}")
            return False

    def verify_import(self, database: str) -> Optional[Dict[str, Any]]:
        """Verify the import by checking node and relationship counts."""
        try:
            with self.node_creator.driver.session(database=database) as session:
                # Get node counts
                node_result = session.run("MATCH (n) RETURN labels(n) as labels, count(n) as count ORDER BY count DESC")
                node_counts = [{"labels": record["labels"], "count": record["count"]} for record in node_result]
                
                # Get relationship counts
                rel_result = session.run("MATCH ()-[r]->() RETURN type(r) as rel_type, count(r) as count ORDER BY count DESC")
                rel_counts = [{"rel_type": record["rel_type"], "count": record["count"]} for record in rel_result]
                
                # Calculate totals
                total_nodes = sum(node["count"] for node in node_counts)
                total_relationships = sum(rel["count"] for rel in rel_counts)
                
                return {
                    "total_nodes": total_nodes,
                    "total_relationships": total_relationships,
                    "node_counts": node_counts,
                    "rel_counts": rel_counts
                }
        
        except Exception as e:
            logger.error(f"Error verifying import: {e}")
            return None

    def close_connections(self):
        """Close Neo4j connections."""
        if hasattr(self, 'node_creator'):
            self.node_creator.close()
        if hasattr(self, 'relationship_creator'):
            self.relationship_creator.close()
