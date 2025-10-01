"""JSON cache manager for Wikidata API responses."""

import json
import logging
import threading
import time
import random
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any, Callable
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class CacheError(Exception):
    """Base exception for cache operations."""
    pass


class LockAcquisitionError(CacheError):
    """Raised when lock acquisition fails after retries."""
    pass


class JSONValidationError(CacheError):
    """Raised when JSON validation fails."""
    pass


class CacheManager:
    """Thread-safe JSON cache manager for Wikidata lookups."""
    
    def __init__(self, cache_file_path: str = "wikidata_cache.json", 
                 max_retries: int = 10, retry_delay: float = 0.1):
        self.cache_file = Path(cache_file_path)
        self.cache = {}
        self._loaded = False
        self._lock = threading.RLock()  # Reentrant lock for nested locking
        self.max_retries = max_retries
        self.retry_delay = retry_delay
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
        # For reads, we can use in-memory cache for performance, but ensure it's loaded
        self._ensure_loaded()
        with self._lock:
            return self.cache.get(entity_text.lower())
    
    def cache_concept(self, entity_text: str, wikidata_info: Optional[Dict[str, Any]]) -> None:
        """Thread-safe cache Wikidata info for entity (or None if not found)."""
        cache_entry = None
        if wikidata_info:
            cache_entry = {
                **wikidata_info,
                'cached_at': datetime.now().isoformat()
            }
        
        # Atomic read-modify-write operation
        self._atomic_update_cache(entity_text.lower(), cache_entry)
    
    def _atomic_update_cache(self, key: str, value: Any) -> None:
        """Atomic read-modify-write operation to prevent race conditions."""
        for attempt in range(self.max_retries):
            try:
                # Always read fresh data from disk to get latest state
                current_cache = self._load_cache_from_disk()
                
                # Update the cache
                current_cache[key] = value
                
                # Atomic write back to disk
                self._atomic_write_cache(current_cache)
                
                # Update in-memory cache for consistency
                with self._lock:
                    self.cache = current_cache
                    self._loaded = True
                
                logger.debug(f"Successfully updated cache for key: {key}")
                return
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    # Exponential backoff with jitter
                    delay = self.retry_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    logger.warning(f"Cache update attempt {attempt + 1} failed for key '{key}': {e}. Retrying in {delay:.2f}s")
                    time.sleep(delay)
                else:
                    logger.error(f"Failed to update cache for key '{key}' after {self.max_retries} attempts: {e}")
                    raise CacheError(f"Failed to update cache for key '{key}' after {self.max_retries} attempts: {e}")
    
    def _load_cache_from_disk(self) -> Dict[str, Any]:
        """Load cache directly from disk (bypasses in-memory cache)."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load cache from disk: {e}")
        return {}
    
    def _atomic_write_cache(self, cache_data: Dict[str, Any]) -> None:
        """Atomically write cache data to disk."""
        # Validate JSON before writing
        json_str = json.dumps(cache_data, indent=2, ensure_ascii=False)
        json.loads(json_str)  # Validate JSON structure
        
        # Write to temporary file first, then atomic rename
        temp_file = self.cache_file.with_suffix('.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(json_str)
        
        # Atomic replacement
        temp_file.replace(self.cache_file)
    
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
        try:
            # Load current cache from disk
            current_cache = self._load_cache_from_disk()
            
            # Sort by key for better compression and readability
            sorted_cache = dict(sorted(current_cache.items()))
            
            # Atomic write back
            self._atomic_write_cache(sorted_cache)
            
            # Update in-memory cache
            with self._lock:
                self.cache = sorted_cache
                self._loaded = True
            
            logger.info(f"Cache file optimized and sorted with {len(sorted_cache)} entries")
            
        except Exception as e:
            logger.error(f"Failed to optimize cache file: {e}")
            raise CacheError(f"Failed to optimize cache file: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure cache is saved."""
        try:
            # Force save any pending changes
            if self._loaded and self.cache:
                self._atomic_write_cache(self.cache)
        except Exception as e:
            logger.warning(f"Error during context manager cleanup: {e}")
    
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
