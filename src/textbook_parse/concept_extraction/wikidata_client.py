"""Wikidata API client for entity lookup."""

import logging
import time
import threading
from typing import Optional, Dict, List
import requests

logger = logging.getLogger(__name__)

# Global rate limiting for all WikidataClient instances
_api_lock = threading.Lock()
_last_api_call = 0
_min_delay = 1.0  # Minimum delay between API calls (seconds)

class WikidataEntity:
    """Represents a Wikidata entity."""
    
    def __init__(self, qid: str, label: str, description: str = "", aliases: List[str] = None, wikidata_url: str = None):
        self.qid = qid
        self.label = label
        self.description = description
        self.aliases = aliases or []
        self.wikidata_url = wikidata_url or f"https://www.wikidata.org/wiki/{qid}"
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for caching."""
        return {
            'qid': self.qid,
            'label': self.label,
            'description': self.description,
            'aliases': self.aliases,
            'wikidata_url': self.wikidata_url
        }

class WikidataClient:
    """Thread-safe client for Wikidata API interactions."""
    
    def __init__(self, cache_manager):
        self.cache_manager = cache_manager
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ConceptExtractor/1.0 (Educational Research)'
        })
        self.api_calls = 0
        self.cache_hits = 0
        self._stats_lock = threading.Lock()
    
    def search_entity(self, term: str) -> Optional[WikidataEntity]:
        """Thread-safe search for entity in Wikidata, using cache first."""
        # Check cache first
        cached_data = self.cache_manager.get_cached_concept(term)
        if cached_data is not None:
            with self._stats_lock:
                self.cache_hits += 1
            if cached_data:  # Not a null cache entry
                # Filter out cache-specific fields that aren't part of WikidataEntity
                entity_data = {k: v for k, v in cached_data.items() 
                              if k in ['qid', 'label', 'description', 'aliases', 'wikidata_url']}
                return WikidataEntity(**entity_data)
            return None
        
        # Make API request with global rate limiting
        entity = self._api_search_with_rate_limit(term)
        
        # Cache result (even if None)
        cache_data = entity.to_dict() if entity else None
        self.cache_manager.cache_concept(term, cache_data)
        
        return entity
    
    def search_entity_cached_only(self, term: str) -> Optional[WikidataEntity]:
        """Search for entity using only cached data (no API calls)."""
        cached_data = self.cache_manager.get_cached_concept(term)
        if cached_data is not None:
            with self._stats_lock:
                self.cache_hits += 1
            if cached_data:  # Not a null cache entry
                entity_data = {k: v for k, v in cached_data.items() 
                              if k in ['qid', 'label', 'description', 'aliases', 'wikidata_url']}
                return WikidataEntity(**entity_data)
            return None
        
        # Return None if not in cache (no API call)
        return None
    
    def _api_search_with_rate_limit(self, term: str) -> Optional[WikidataEntity]:
        """Make API request with global rate limiting across all threads."""
        global _last_api_call
        
        # Global rate limiting
        with _api_lock:
            current_time = time.time()
            time_since_last_call = current_time - _last_api_call
            
            if time_since_last_call < _min_delay:
                sleep_time = _min_delay - time_since_last_call
                time.sleep(sleep_time)
            
            _last_api_call = time.time()
            
            # Track API call
            with self._stats_lock:
                self.api_calls += 1
        
        # Make the actual API request
        return self._api_search(term)
    
    def _api_search(self, term: str) -> Optional[WikidataEntity]:
        """Make actual API request to Wikidata."""
        url = "https://www.wikidata.org/w/api.php"
        params = {
            'action': 'wbsearchentities',
            'search': term.strip(),
            'language': 'en',
            'format': 'json',
            'limit': 1,
            'type': 'item'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            search_results = data.get('search', [])
            
            if search_results:
                result = search_results[0]
                return WikidataEntity(
                    qid=result.get('id', ''),
                    label=result.get('label', ''),
                    description=result.get('description', ''),
                    aliases=[alias for alias in result.get('aliases', [])]
                )
            
            return None
            
        except Exception as e:
            logger.warning(f"Error searching Wikidata for '{term}': {e}")
            return None
    
    def _create_entity_from_cache(self, cached_data: Dict) -> WikidataEntity:
        """Create WikidataEntity object from cached data."""
        return WikidataEntity(
            qid=cached_data['qid'],
            label=cached_data['label'],
            description=cached_data.get('description', ''),
            aliases=cached_data.get('aliases', []),
            wikidata_url=cached_data.get('wikidata_url')
        )
    
    def get_stats(self) -> Dict[str, int]:
        """Get thread-safe client statistics."""
        with self._stats_lock:
            total_requests = self.api_calls + self.cache_hits
            cache_hit_rate = (self.cache_hits / total_requests * 100) if total_requests > 0 else 0
            
            return {
                'api_calls': self.api_calls,
                'cache_hits': self.cache_hits,
                'cache_hit_rate': cache_hit_rate
            }
