"""
Azure OpenAI Configuration Module

This module handles loading Azure OpenAI configuration from JSON files
and creating AzureChatOpenAI instances for the GraphRAG pipeline.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
from langchain_openai import AzureChatOpenAI
from langchain_core.language_models.chat_models import BaseChatModel


class AzureConfig:
    """Azure OpenAI configuration manager."""
    
    def __init__(self, config_file: str = "src/config/azure_llm_lite.json"):
        """
        Initialize the Azure configuration manager.
        
        Args:
            config_file: Path to the Azure configuration JSON file
        """
        self.config_file = config_file
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from JSON file.
        
        Returns:
            Dictionary containing Azure model configurations
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file is invalid JSON
        """
        config_path = Path(self.config_file)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_file}")
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            if 'azure_models' not in config:
                raise ValueError("Configuration file must contain 'azure_models' section")
            
            return config['azure_models']
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def get_available_models(self) -> list[str]:
        """
        Get list of available model names.
        
        Returns:
            List of model names
        """
        return list(self.config.keys())
    
    def get_chat_llm(self, model_name: str, temperature: float = 0.1) -> BaseChatModel:
        """
        Create and return an AzureChatOpenAI instance.
        
        Args:
            model_name: Name of the model to use
            temperature: Temperature for the model (0.0 to 1.0)
            
        Returns:
            Configured AzureChatOpenAI instance
            
        Raises:
            KeyError: If model_name is not found in configuration
            ValueError: If required configuration fields are missing
        """
        if model_name not in self.config:
            available_models = self.get_available_models()
            raise KeyError(f"Model '{model_name}' not found. Available models: {available_models}")
        
        model_config = self.config[model_name]
        
        # Validate required fields
        required_fields = ['api_base', 'api_key', 'api_version', 'deployment_name']
        missing_fields = [field for field in required_fields if field not in model_config]
        
        if missing_fields:
            raise ValueError(f"Missing required fields for model '{model_name}': {missing_fields}")
        
        # Create AzureChatOpenAI instance
        llm = AzureChatOpenAI(
            azure_endpoint=model_config['api_base'],
            api_key=model_config['api_key'],
            api_version=model_config['api_version'],
            azure_deployment=model_config['deployment_name'],
            temperature=temperature,
            max_tokens=4000,  # Reasonable default for educational content
            timeout=60,  # 60 second timeout
            max_retries=3
        )
        
        return llm
    
    def get_model_info(self, model_name: str) -> Dict[str, Any]:
        """
        Get configuration information for a specific model.
        
        Args:
            model_name: Name of the model
            
        Returns:
            Dictionary containing model configuration
            
        Raises:
            KeyError: If model_name is not found
        """
        if model_name not in self.config:
            available_models = self.get_available_models()
            raise KeyError(f"Model '{model_name}' not found. Available models: {available_models}")
        
        return self.config[model_name].copy()
    
    def validate_config(self) -> bool:
        """
        Validate the entire configuration.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        try:
            for model_name in self.config:
                model_config = self.config[model_name]
                required_fields = ['api_base', 'api_key', 'api_version', 'deployment_name']
                
                for field in required_fields:
                    if field not in model_config or not model_config[field]:
                        print(f"Validation error: Model '{model_name}' missing or empty field '{field}'")
                        return False
            
            return True
            
        except Exception as e:
            print(f"Configuration validation error: {e}")
            return False
