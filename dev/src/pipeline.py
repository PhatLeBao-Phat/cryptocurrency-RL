# ----------------------------------------------
# Define ETL pipeline object
# ----------------------------------------------

# Import
import mysql.connector as mysql
import pyodbc
import pandas as pd
from datetime import datetime
import pytz
from typing import *
from abc import ABC, abstractmethod
from tqdm import tqdm
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
import psycopg2

# Local import
from ..utils.config_manager import *
from ..utils.api_client import *
from ..utils.logging import *


class Dataset:
    """Managing a dataset"""
    def __init__(self, data : pd.DataFrame | List | Dict, name : Optional[str] = None):
        self.data = data 
        self.name = name


class PipelineStage(ABC):
    """Each stage in the Pipeline Object"""
    def __init__(self):
        super().__init__()
        pass 

    @abstractmethod
    def run():
        pass 


class Extractor(PipelineStage):
    """Extract data from API or any sources"""
    def __init__(self):
        None
    
    @abstractmethod
    def extract(
        config : Optional[ConfigManager], 
        api : Optional[APIClient]
    ) -> Dataset:
        pass 
    
    def run(self, *args, **kwargs) -> Dataset | List[Dataset]:
        return self.extract(*args, **kwargs)


class Transformer(PipelineStage):
    """Transform extracted data to ready-loaded data"""
    def __init__(self):
        None 
    
    @abstractmethod
    def transform(self, data : Dataset | Dict[str, Dataset]) -> Dataset:
        pass

    def run(self, *args, **kwargs) -> Dataset | List[Dataset]:
        return self.transform(*args, **kwargs)


class Loader(PipelineStage):
    """Load transformed data to database"""
    def __init__(self):
        None 
    
    @abstractmethod
    def load(data : Dataset) -> None:
        pass 

    @staticmethod
    def get_ingestion_time(tz: str = "Europe/Amsterdam"):
        current_time = datetime.now(pytz.timezone(tz))

        return current_time
    
    @staticmethod
    def _filter_exists(df, db_df, unique_key) -> None:
        # Get incremental values 
        if isinstance(unique_key, str):
            unique_key = [unique_key]
        incremental_values = set(zip(*[db_df[key] for key in unique_key]))

        # Filtering

        bol_filter = df.apply(
            lambda row : set(row[key] for key in unique_key) in incremental_values,
            axis=1,
        )
        df = df[bol_filter]

        return df
    
    @staticmethod
    def match_columns(df : pd.DataFrame, columns : List[str]) -> bool:
        for col in df.columns:
            if col not in columns:
                return False
        
        for col in columns:
            if col not in df.columns:
                return False
    
    def run(self, *args, **kwargs) -> None:
        self.load(*args, **kwargs)


class Pipeline:
    """Pipeline object for executing the ETL process"""

    def __init__(self, 
        stages : Optional[List[Union[PipelineStage, Callable]]] = None,
        config : Optional[ConfigManager] = None,
    ) -> None:
        self.stages = stages 
        self.config = config

    @log_operation
    @abstractmethod
    def run(self) -> Dataset | List[Dataset]:
        raise NotImplementedError("Subclasses must implement this method.")

    @staticmethod
    def _combine_dataset(lst : List[Dataset | pd.DataFrame], name : Optional[str] = None) -> Dataset:
        c_list = []
        for item in lst:
            try:
                c_list.append(item.data)
            except:
                c_list.append(item)

        data = pd.concat(c_list, axis=0)
        return Dataset(data, name)