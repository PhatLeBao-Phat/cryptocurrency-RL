# ----------------------------------------------
# Define ETL pipeline object
# ----------------------------------------------
import pandas as pd
from datetime import datetime
import pytz
from typing import Optional, List, Dict, Union, Callable
from abc import ABC, abstractmethod

from dev.utils.config_manager import ConfigManager
from dev.utils.api_client import APIClient
from dev.utils.logging import log_operation


class Dataset:
    """Managing a dataset"""
    
    def __init__(self, data: Union[pd.DataFrame, List, Dict], name: Optional[str] = None):
        self.data = data
        self.name = name


class PipelineStage(ABC):
    """Each stage in the Pipeline Object"""

    @abstractmethod
    def run(self, *args, **kwargs):
        pass


class Extractor(PipelineStage):
    """Extract data from API or any sources"""

    @abstractmethod
    def extract(self, config: Optional[ConfigManager], api: Optional[APIClient]) -> Dataset:
        pass

    def run(self, *args, **kwargs) -> Union[Dataset, List[Dataset]]:
        return self.extract(*args, **kwargs)


class Transformer(PipelineStage):
    """Transform extracted data to ready-loaded data"""

    @abstractmethod
    def transform(self, data: Union[Dataset, Dict[str, Dataset]]) -> Dataset:
        pass

    def run(self, *args, **kwargs) -> Union[Dataset, List[Dataset]]:
        return self.transform(*args, **kwargs)


class Loader(PipelineStage):
    """Load transformed data to database"""

    @abstractmethod
    def load(self, data: Dataset) -> None:
        pass

    @abstractmethod
    def db_connect(self):
        pass

    @staticmethod
    def get_ingestion_time(tz: str = "Europe/Amsterdam") -> datetime:
        """Get the current ingestion timestamp in the specified timezone"""
        return datetime.now(pytz.timezone(tz))

    @staticmethod
    def _filter_exists(df: pd.DataFrame, db_df: pd.DataFrame, unique_key: Union[str, List[str]]) -> pd.DataFrame:
        """Filter out rows that already exist in the database"""
        if isinstance(unique_key, str):
            unique_key = [unique_key]

        existing_records = db_df[unique_key].drop_duplicates()
        df = df[~df[unique_key].apply(tuple, axis=1).isin(existing_records.apply(tuple, axis=1))]

        return df

    @staticmethod
    def match_columns(df: pd.DataFrame, columns: List[str]) -> bool:
        """Check if DataFrame columns match the expected column list"""
        return set(df.columns) == set(columns)

    def run(self, *args, **kwargs) -> None:
        self.load(*args, **kwargs)


class Pipeline:
    """Pipeline object for executing the ETL process"""

    def __init__(self, 
        stages: Optional[List[Union[PipelineStage, Callable]]] = None,
        config: Optional[ConfigManager] = None,
    ) -> None:
        self.stages = stages or []
        self.config = config

    @log_operation
    def run(self) -> Union[Dataset, List[Dataset]]:
        """Execute all pipeline stages sequentially"""
        data = None
        for stage in self.stages:
            if isinstance(stage, PipelineStage):
                data = stage.run(data)
            elif callable(stage):  # Support for function-based transformations
                data = stage(data)
            else:
                raise TypeError(f"Invalid stage type: {type(stage)}")
        return data

    @staticmethod
    def _combine_dataset(lst: List[Union[Dataset, pd.DataFrame]], name: Optional[str] = None) -> Dataset:
        """Combine multiple Dataset objects or DataFrames into a single Dataset"""
        dataframes = [item.data if isinstance(item, Dataset) else item for item in lst]
        return Dataset(pd.concat(dataframes, axis=0), name)
