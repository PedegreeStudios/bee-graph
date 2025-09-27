"""JSON cache manager for Wikidata API responses."""

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

class CacheManager:
    """Thread-safe JSON cache manager for Wikidata lookups."""
    
    def __init__(self, cache_file_path: str = "wikidata_cache.json"):
        self.cache_file = Path(cache_file_path)
        self.cache = {}
        self._loaded = False
        self._lock = threading.RLock()  # Reentrant lock for nested locking
        logger.info("Thread-safe cache manager initialized (lazy loading enabled)")
    
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
        """Thread-safe cache loading - only load when first needed."""
        if not self._loaded:
            with self._lock:
                # Double-check pattern
                if not self._loaded:
                    self.cache = self._load_cache()
                    self._loaded = True
    
    def get_cached_concept(self, entity_text: str) -> Optional[Dict[str, Any]]:
        """Thread-safe get cached Wikidata info for entity."""
        self._ensure_loaded()
        with self._lock:
            return self.cache.get(entity_text.lower())
    
    def cache_concept(self, entity_text: str, wikidata_info: Optional[Dict[str, Any]]) -> None:
        """Thread-safe cache Wikidata info for entity (or None if not found)."""
        self._ensure_loaded()
        
        cache_entry = None
        if wikidata_info:
            cache_entry = {
                **wikidata_info,
                'cached_at': datetime.now().isoformat()
            }
        
        with self._lock:
            self.cache[entity_text.lower()] = cache_entry
            # Save immediately for thread safety and data persistence
            self._save_cache_unsafe()
    
    def _save_cache_unsafe(self) -> None:
        """Save cache to file - must be called within lock."""
        try:
            # Write to temporary file first, then atomic rename
            temp_file = self.cache_file.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
            temp_file.replace(self.cache_file)
        except Exception as e:
            logger.warning(f"Could not save cache: {e}")
    
    def optimize_cache_file(self):
        """Thread-safe cache optimization."""
        self._ensure_loaded()
        
        with self._lock:
            # Sort by key for better compression and readability
            sorted_cache = dict(sorted(self.cache.items()))
            self.cache = sorted_cache
            self._save_cache_unsafe()
        
        logger.info("Cache file optimized and sorted")
    
    def get_stats(self) -> Dict[str, int]:
        """Thread-safe cache statistics."""
        self._ensure_loaded()
        
        with self._lock:
            cached_entries = sum(1 for v in self.cache.values() if v is not None)
            null_entries = sum(1 for v in self.cache.values() if v is None)
            
            return {
                'total_entries': len(self.cache),
                'cached_concepts': cached_entries,
                'null_entries': null_entries
            }
