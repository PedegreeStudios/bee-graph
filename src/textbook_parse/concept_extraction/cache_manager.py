"""JSON cache manager for Wikidata API responses."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

class CacheManager:
    """Manages JSON cache for Wikidata lookups to avoid duplicate API calls."""
    
    def __init__(self, cache_file_path: str = "wikidata_cache.json"):
        self.cache_file = Path(cache_file_path)
        self.cache = {}
        self._loaded = False
        logger.info("Cache manager initialized (lazy loading enabled)")
    
    def _load_cache(self) -> Dict[str, Any]:
        """Load existing cache or create empty one."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    logger.info(f"Loaded cache from {self.cache_file} with {len(cache_data)} entries")
                    return cache_data
            except Exception as e:
                logger.warning(f"Could not load cache: {e}")
        return {}
    
    def _ensure_loaded(self):
        """Load cache only when first needed."""
        if not self._loaded:
            self.cache = self._load_cache()
            self._loaded = True
    
    def get_cached_concept(self, entity_text: str) -> Optional[Dict[str, Any]]:
        """Get cached Wikidata info for entity."""
        self._ensure_loaded()
        return self.cache.get(entity_text.lower())
    
    def cache_concept(self, entity_text: str, wikidata_info: Optional[Dict[str, Any]]) -> None:
        """Cache Wikidata info for entity (or None if not found)."""
        self._ensure_loaded()
        
        cache_entry = None
        if wikidata_info:
            cache_entry = {
                **wikidata_info,
                'cached_at': datetime.now().isoformat()
            }
        
        self.cache[entity_text.lower()] = cache_entry
        self._save_cache()
    
    def _save_cache(self) -> None:
        """Save cache to file."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Could not save cache: {e}")
    
    def optimize_cache_file(self):
        """Sort cache entries for better performance and compression."""
        self._ensure_loaded()
        
        # Sort by key for better compression and readability
        sorted_cache = dict(sorted(self.cache.items()))
        
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump(sorted_cache, f, indent=2, ensure_ascii=False)
        
        self.cache = sorted_cache
        logger.info("Cache file optimized and sorted")
    
    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        self._ensure_loaded()
        
        cached_entries = sum(1 for v in self.cache.values() if v is not None)
        null_entries = sum(1 for v in self.cache.values() if v is None)
        
        return {
            'total_entries': len(self.cache),
            'cached_concepts': cached_entries,
            'null_entries': null_entries
        }
