"""Runtime configuration management and hot-reloading."""

import logging
import threading
import time
from typing import Dict, Any, Optional, Callable, List
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dataclasses import dataclass
from datetime import datetime

from kafka_ops_agent.config.config_manager import ConfigurationManager, ApplicationConfig
from kafka_ops_agent.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


@dataclass
class ConfigChangeEvent:
    """Configuration change event."""
    timestamp: datetime
    changed_keys: List[str]
    old_values: Dict[str, Any]
    new_values: Dict[str, Any]
    source: str


class ConfigChangeHandler:
    """Handles configuration change events."""
    
    def __init__(self):
        """Initialize change handler."""
        self.callbacks: List[Callable[[ConfigChangeEvent], None]] = []
    
    def register_callback(self, callback: Callable[[ConfigChangeEvent], None]):
        """Register a callback for configuration changes.
        
        Args:
            callback: Function to call when configuration changes
        """
        self.callbacks.append(callback)
        logger.debug(f"Registered config change callback: {callback.__name__}")
    
    def unregister_callback(self, callback: Callable[[ConfigChangeEvent], None]):
        """Unregister a configuration change callback.
        
        Args:
            callback: Function to unregister
        """
        if callback in self.callbacks:
            self.callbacks.remove(callback)
            logger.debug(f"Unregistered config change callback: {callback.__name__}")
    
    def notify_change(self, event: ConfigChangeEvent):
        """Notify all callbacks of configuration change.
        
        Args:
            event: Configuration change event
        """
        logger.info(f"Configuration changed: {len(event.changed_keys)} keys updated")
        
        for callback in self.callbacks:
            try:
                callback(event)
            except Exception as e:
                logger.error(f"Error in config change callback {callback.__name__}: {e}")


class ConfigFileWatcher(FileSystemEventHandler):
    """Watches configuration files for changes."""
    
    def __init__(self, config_manager: ConfigurationManager, 
                 change_handler: ConfigChangeHandler):
        """Initialize file watcher.
        
        Args:
            config_manager: Configuration manager instance
            change_handler: Change handler for notifications
        """
        super().__init__()
        self.config_manager = config_manager
        self.change_handler = change_handler
        self.last_reload = time.time()
        self.reload_debounce = 1.0  # 1 second debounce
    
    def on_modified(self, event):
        """Handle file modification events.
        
        Args:
            event: File system event
        """
        if event.is_directory:
            return
        
        # Check if it's a configuration file
        if not self._is_config_file(event.src_path):
            return
        
        # Debounce rapid file changes
        current_time = time.time()
        if current_time - self.last_reload < self.reload_debounce:
            return
        
        self.last_reload = current_time
        
        try:
            logger.info(f"Configuration file changed: {event.src_path}")
            self._reload_configuration(event.src_path)
        except Exception as e:
            logger.error(f"Failed to reload configuration: {e}")
    
    def _is_config_file(self, file_path: str) -> bool:
        """Check if file is a configuration file.
        
        Args:
            file_path: Path to file
            
        Returns:
            True if it's a configuration file
        """
        config_extensions = ['.yaml', '.yml', '.json', '.toml']
        return any(file_path.endswith(ext) for ext in config_extensions)
    
    def _reload_configuration(self, file_path: str):
        """Reload configuration and notify changes.
        
        Args:
            file_path: Path to changed file
        """
        # Get current configuration
        old_config = self.config_manager.get_config()
        old_dict = old_config.dict()
        
        # Reload configuration
        new_config = self.config_manager.reload_configuration()
        new_dict = new_config.dict()
        
        # Find changes
        changed_keys, old_values, new_values = self._find_changes(old_dict, new_dict)
        
        if changed_keys:
            # Create change event
            event = ConfigChangeEvent(
                timestamp=datetime.utcnow(),
                changed_keys=changed_keys,
                old_values=old_values,
                new_values=new_values,
                source=file_path
            )
            
            # Notify change handlers
            self.change_handler.notify_change(event)
    
    def _find_changes(self, old_dict: Dict[str, Any], new_dict: Dict[str, Any], 
                     prefix: str = "") -> tuple[List[str], Dict[str, Any], Dict[str, Any]]:
        """Find changes between two configuration dictionaries.
        
        Args:
            old_dict: Old configuration
            new_dict: New configuration
            prefix: Key prefix for nested dictionaries
            
        Returns:
            Tuple of (changed_keys, old_values, new_values)
        """
        changed_keys = []
        old_values = {}
        new_values = {}
        
        # Check all keys in both dictionaries
        all_keys = set(old_dict.keys()) | set(new_dict.keys())
        
        for key in all_keys:
            full_key = f"{prefix}.{key}" if prefix else key
            
            old_value = old_dict.get(key)
            new_value = new_dict.get(key)
            
            if old_value != new_value:
                if isinstance(old_value, dict) and isinstance(new_value, dict):
                    # Recursively check nested dictionaries
                    nested_changes, nested_old, nested_new = self._find_changes(
                        old_value, new_value, full_key
                    )
                    changed_keys.extend(nested_changes)
                    old_values.update(nested_old)
                    new_values.update(nested_new)
                else:
                    # Direct value change
                    changed_keys.append(full_key)
                    old_values[full_key] = old_value
                    new_values[full_key] = new_value
        
        return changed_keys, old_values, new_values


class RuntimeConfigurationManager:
    """Manages runtime configuration updates and hot-reloading."""
    
    def __init__(self, config_manager: ConfigurationManager):
        """Initialize runtime configuration manager.
        
        Args:
            config_manager: Configuration manager instance
        """
        self.config_manager = config_manager
        self.change_handler = ConfigChangeHandler()
        self.file_watcher = ConfigFileWatcher(config_manager, self.change_handler)
        self.observer: Optional[Observer] = None
        self.lock = threading.RLock()
        self._running = False
        
        # Register built-in change handlers
        self._register_builtin_handlers()
    
    def start_watching(self, watch_paths: Optional[List[str]] = None):
        """Start watching configuration files for changes.
        
        Args:
            watch_paths: Optional list of paths to watch
        """
        if self._running:
            logger.warning("Configuration watcher is already running")
            return
        
        with self.lock:
            try:
                self.observer = Observer()
                
                # Default watch paths
                if not watch_paths:
                    watch_paths = [
                        "config/",
                        "/etc/kafka-ops-agent/",
                        str(Path.home() / ".kafka-ops-agent/")
                    ]
                
                # Add watch paths that exist
                for path in watch_paths:
                    path_obj = Path(path)
                    if path_obj.exists():
                        self.observer.schedule(self.file_watcher, str(path_obj), recursive=True)
                        logger.info(f"Watching configuration path: {path}")
                
                self.observer.start()
                self._running = True
                logger.info("Configuration file watcher started")
                
            except Exception as e:
                logger.error(f"Failed to start configuration watcher: {e}")
                raise ConfigurationError(f"Failed to start configuration watcher: {e}")
    
    def stop_watching(self):
        """Stop watching configuration files."""
        if not self._running:
            return
        
        with self.lock:
            try:
                if self.observer:
                    self.observer.stop()
                    self.observer.join(timeout=5.0)
                    self.observer = None
                
                self._running = False
                logger.info("Configuration file watcher stopped")
                
            except Exception as e:
                logger.error(f"Error stopping configuration watcher: {e}")
    
    def update_configuration(self, updates: Dict[str, Any], 
                           persist: bool = False) -> ApplicationConfig:
        """Update configuration at runtime.
        
        Args:
            updates: Configuration updates (dot notation keys)
            persist: Whether to persist changes to file
            
        Returns:
            Updated configuration
        """
        with self.lock:
            try:
                # Get current configuration
                current_config = self.config_manager.get_config()
                current_dict = current_config.dict()
                
                # Apply updates
                updated_dict = self._apply_updates(current_dict, updates)
                
                # Validate updated configuration
                new_config = ApplicationConfig(**updated_dict)
                
                # Update the configuration manager
                self.config_manager._config = new_config
                
                # Persist if requested
                if persist:
                    self._persist_configuration(new_config)
                
                # Create change event
                changed_keys = list(updates.keys())
                old_values = {key: self._get_nested_value(current_dict, key) 
                             for key in changed_keys}
                new_values = updates.copy()
                
                event = ConfigChangeEvent(
                    timestamp=datetime.utcnow(),
                    changed_keys=changed_keys,
                    old_values=old_values,
                    new_values=new_values,
                    source="runtime_update"
                )
                
                # Notify change handlers
                self.change_handler.notify_change(event)
                
                logger.info(f"Configuration updated: {len(updates)} keys changed")
                return new_config
                
            except Exception as e:
                logger.error(f"Failed to update configuration: {e}")
                raise ConfigurationError(f"Failed to update configuration: {e}")
    
    def _apply_updates(self, config_dict: Dict[str, Any], 
                      updates: Dict[str, Any]) -> Dict[str, Any]:
        """Apply configuration updates to dictionary.
        
        Args:
            config_dict: Current configuration dictionary
            updates: Updates to apply (dot notation keys)
            
        Returns:
            Updated configuration dictionary
        """
        result = config_dict.copy()
        
        for key, value in updates.items():
            self._set_nested_value(result, key, value)
        
        return result
    
    def _set_nested_value(self, data: Dict[str, Any], path: str, value: Any):
        """Set nested dictionary value using dot notation.
        
        Args:
            data: Dictionary to update
            path: Dot-separated path
            value: Value to set
        """
        keys = path.split('.')
        current = data
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        current[keys[-1]] = value
    
    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """Get nested dictionary value using dot notation.
        
        Args:
            data: Dictionary to read from
            path: Dot-separated path
            
        Returns:
            Value at path or None if not found
        """
        keys = path.split('.')
        current = data
        
        try:
            for key in keys:
                current = current[key]
            return current
        except (KeyError, TypeError):
            return None
    
    def _persist_configuration(self, config: ApplicationConfig):
        """Persist configuration to file.
        
        Args:
            config: Configuration to persist
        """
        # This is a placeholder - in a real implementation,
        # you would write the configuration back to the appropriate file
        logger.info("Configuration persistence not implemented yet")
    
    def _register_builtin_handlers(self):
        """Register built-in configuration change handlers."""
        
        def log_level_handler(event: ConfigChangeEvent):
            """Handle log level changes."""
            if 'logging.level' in event.changed_keys:
                new_level = event.new_values.get('logging.level')
                if new_level:
                    # Update root logger level
                    logging.getLogger().setLevel(getattr(logging, new_level.upper()))
                    logger.info(f"Log level updated to: {new_level}")
        
        def database_handler(event: ConfigChangeEvent):
            """Handle database configuration changes."""
            db_keys = [key for key in event.changed_keys if key.startswith('database.')]
            if db_keys:
                logger.warning("Database configuration changed - restart may be required")
        
        def api_handler(event: ConfigChangeEvent):
            """Handle API configuration changes."""
            api_keys = [key for key in event.changed_keys if key.startswith('api.')]
            if api_keys:
                logger.warning("API configuration changed - restart may be required")
        
        # Register handlers
        self.change_handler.register_callback(log_level_handler)
        self.change_handler.register_callback(database_handler)
        self.change_handler.register_callback(api_handler)
    
    def register_change_handler(self, callback: Callable[[ConfigChangeEvent], None]):
        """Register a configuration change handler.
        
        Args:
            callback: Function to call when configuration changes
        """
        self.change_handler.register_callback(callback)
    
    def unregister_change_handler(self, callback: Callable[[ConfigChangeEvent], None]):
        """Unregister a configuration change handler.
        
        Args:
            callback: Function to unregister
        """
        self.change_handler.unregister_callback(callback)
    
    def get_configuration_status(self) -> Dict[str, Any]:
        """Get current configuration status.
        
        Returns:
            Configuration status information
        """
        config = self.config_manager.get_config()
        
        return {
            "environment": config.environment,
            "version": config.version,
            "watcher_running": self._running,
            "last_reload": getattr(self.file_watcher, 'last_reload', None),
            "validation_errors": self.config_manager.validate_configuration(),
            "config_sources": {
                key: metadata.source.value 
                for key, metadata in self.config_manager.metadata.items()
            }
        }
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_watching()


# Global runtime configuration manager
_runtime_config_manager: Optional[RuntimeConfigurationManager] = None


def get_runtime_config_manager(config_manager: Optional[ConfigurationManager] = None) -> RuntimeConfigurationManager:
    """Get global runtime configuration manager.
    
    Args:
        config_manager: Optional configuration manager instance
        
    Returns:
        Runtime configuration manager
    """
    global _runtime_config_manager
    
    if _runtime_config_manager is None:
        if config_manager is None:
            from kafka_ops_agent.config.config_manager import get_config_manager
            config_manager = get_config_manager()
        
        _runtime_config_manager = RuntimeConfigurationManager(config_manager)
    
    return _runtime_config_manager


def start_config_watcher(watch_paths: Optional[List[str]] = None):
    """Start configuration file watcher.
    
    Args:
        watch_paths: Optional list of paths to watch
    """
    runtime_manager = get_runtime_config_manager()
    runtime_manager.start_watching(watch_paths)


def stop_config_watcher():
    """Stop configuration file watcher."""
    runtime_manager = get_runtime_config_manager()
    runtime_manager.stop_watching()


def update_runtime_config(updates: Dict[str, Any], persist: bool = False) -> ApplicationConfig:
    """Update configuration at runtime.
    
    Args:
        updates: Configuration updates
        persist: Whether to persist changes
        
    Returns:
        Updated configuration
    """
    runtime_manager = get_runtime_config_manager()
    return runtime_manager.update_configuration(updates, persist)