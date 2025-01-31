# ----------------------------------------------
# Define Configuration Objects
# ----------------------------------------------
from configparser import ConfigParser, NoOptionError
import os

class ConfigManager:
    """Managing configuration file and environment of Pipeline"""

    def __init__(self, config_path: str, env: str):
        self.config_path = config_path
        self.env = env
        self.parser = self._load_config(config_path)
        self.config_dict = self._load_environment(env)

    def _load_config(self, config_path: str):
        """Try loading the configuration file."""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file {config_path} not found")
        
        parser = ConfigParser()
        if not parser.read(config_path):
            raise FileNotFoundError(f"Configuration file {config_path} is empty or could not be read.")
        
        return parser

    def _load_environment(self, env: str):
        """Load the specified environment section from the config file."""
        if not self.parser.has_section(env):
            raise ValueError(f"Environment section '{env}' not found in configuration file.")
        return dict(self.parser.items(env))

    def get(self, item: str):
        """Get a configuration item for the current environment."""
        try:
            return self.parser.get(self.env, item)
        except NoOptionError:
            raise KeyError(f"Item '{item}' is not available in the configuration section '{self.env}'")