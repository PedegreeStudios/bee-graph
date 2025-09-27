"""Entity extraction from text using spaCy NLP."""

import logging
import re
from typing import List, Set
import spacy

logger = logging.getLogger(__name__)

class EntityExtractor:
    """Extracts named entities and key terms from sentence content using spaCy."""
    
    def __init__(self, model_name: str = "en_core_web_sm"):
        try:
            self.nlp = spacy.load(model_name)
            logger.info(f"Loaded spaCy model: {model_name}")
        except OSError:
            logger.error(f"spaCy model {model_name} not found. Install with: python -m spacy download {model_name}")
            raise
    
    def extract_entities(self, text: str) -> List[str]:
        """Extract meaningful entities from text."""
        if not text or not isinstance(text, str):
            return []
        
        doc = self.nlp(text)
        entities = set()
        
        # Extract named entities (expanded for educational content)
        for ent in doc.ents:
            # Extract ALL named entities for educational content, not just specific types
            clean_entity = self._clean_entity_text(ent.text)
            if self._is_valid_entity(clean_entity):
                entities.add(clean_entity)
        
        # Extract meaningful nouns and proper nouns
        for token in doc:
            if (token.pos_ in ['NOUN', 'PROPN'] and 
                not token.is_stop and 
                not token.is_punct and
                len(token.text) >= 3):
                
                clean_token = self._clean_entity_text(token.lemma_)
                if self._is_valid_entity(clean_token):
                    entities.add(clean_token)
        
        # Extract noun phrases for compound concepts (expanded for scientific terms)
        for chunk in doc.noun_chunks:
            if len(chunk.text.split()) > 1:  # Multi-word phrases
                clean_phrase = self._clean_entity_text(chunk.text)
                if self._is_valid_entity(clean_phrase) and len(clean_phrase.split()) <= 4:  # Allow longer phrases
                    entities.add(clean_phrase)
        
        # Filter out generic terms
        filtered_entities = [e for e in entities if not self._is_generic_term(e)]
        
        return sorted(filtered_entities)
    
    def _clean_entity_text(self, text: str) -> str:
        """Clean and normalize entity text."""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        cleaned = re.sub(r'\s+', ' ', text.strip())
        
        # Remove leading/trailing punctuation
        cleaned = re.sub(r'^[^\w\s]+|[^\w\s]+$', '', cleaned)
        
        return cleaned
    
    def _is_valid_entity(self, entity: str) -> bool:
        """Check if entity is valid for Wikidata lookup."""
        if not entity or len(entity) < 2:
            return False
        
        # Must contain at least one letter
        if not re.search(r'[a-zA-Z]', entity):
            return False
        
        # Reject pure numbers or years
        if entity.isdigit() or re.match(r'^\d{4}$', entity):
            return False
        
        # Reject URLs or email-like patterns
        if re.match(r'(https?://|www\.|[^\s]+@[^\s]+)', entity):
            return False
        
        return True
    
    def _is_generic_term(self, entity: str) -> bool:
        """Check if entity is a generic term to filter out."""
        generic_terms = {
            'sentence', 'paragraph', 'text', 'content', 'information', 'data',
            'thing', 'way', 'time', 'year', 'work', 'case', 'group', 'number',
            'system', 'process', 'method', 'result', 'study', 'research',
            'analysis', 'example', 'type', 'kind', 'form', 'part', 'area',
            'use', 'used', 'using', 'made', 'making', 'take', 'taken', 'taking'
        }
        
        return entity.lower() in generic_terms
