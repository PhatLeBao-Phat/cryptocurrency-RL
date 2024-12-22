# ----------------------------------------------
# Define Configuration Objects
# ----------------------------------------------
from configparser import RawConfigParser, ConfigParser

class ConfigManager:
    """Managing configuration file and environment of Pipeline"""

    def __init__(self, config_path : str, env : str):
        self.config_path = config_path
        self.env = env 

        try: 
            parser =ConfigParser()
            parser.read(config_path)
        except:
            try: 
                parser = RawConfigParser()
                parser.read(config_path)
            except FileNotFoundError:
                raise FileNotFoundError("Configuration file is not found")
        
        self.parser = parser 

        if parser.has_section(env):
            self.config_dict = dict(parser.items(env))
    
    def get(self, item : str):
        try:
            return self.parser.get(self.env, item)
        except:
            raise KeyError(f"Item is not available in configuration section {self.env}")