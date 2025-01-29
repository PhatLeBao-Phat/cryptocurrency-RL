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

# ----------------------------------------------
# Define Loader strategies
# ----------------------------------------------

class MySQLLoader(Loader):
    def __init__(self, 
        config : ConfigManager, 
        load_path : str = None,
        load_method : Literal["incremental", "append"] = None,
        unique_key : str = None):
        super().__init__()
        self.config = config 
        self.load_path = load_path
        self.load_method = load_method
        self.unique_key = unique_key

        logger.info(f"Initialized {self.__class__.__name__} with {self.__dict__}")

    
    def db_connect(self, config : ConfigManager):
        """Return a Connection object"""
        dbconnect = mysql.connect(
            host=config.get("host"),
            user=config.get("user"),
            password=config.get("password"),
            db=config.get("db"),
        )

        return dbconnect
    
    @staticmethod
    def get_sqlalchemy_engine(config : ConfigManager):
        """Return pymysql engine"""
        username = config.get("user")
        password = config.get("password")
        host = config.get("host")
        port = config.get("port")
        db = config.get("db")

        return create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{db}")
    
    @staticmethod
    def match_columns(df : pd.DataFrame, columns : List[str]) -> bool:
        for col in df.columns:
            if col not in columns:
                return False
        
        for col in columns:
            if col not in list(df.columns):
                return False
        
        return True

    @log_operation
    def load(
        self, 
        data : Dataset, 
        load_path : str = None,
        load_method : Literal["incremental", "append", "replace"] = None, # TODO: Add this method
        unique_key : List[str] = None,
    ) -> None:
        if not unique_key:
            unique_key = self.unique_key
        if not load_method:
            load_method = self.load_method
        if not load_path:
            load_path = self.load_path

        # Get connect
        try:
            dbengine = self.get_sqlalchemy_engine(self.config)
        except:
            ValueError("Cannot connect to database on given config!")


        db_name, table_name = load_path.split(".")


        # Add data to database
        df = data.data
        
        # Incremental mode
        if load_method == "incremental":    
            self._incremental_load_sqlalchemy(
                df,
                unique_key,
                db_name,
                table_name,
                load_path,
                dbengine
            )
        elif load_method == "append":
            self._append_load(
                df,
                load_path
            )
        else:
            raise ValueError(f"Invalid Load method {load_method}")
            
    
    def _incremental_load_sqlalchemy(
            self, 
            df : pd.DataFrame, 
            unique_key : str | List[str],
            db_name : str, 
            table_name : str, 
            load_path : str,
            dbengine : Optional[str] = None,
        ) -> None:
        """Loading data with sqlalchemy lib and pandas."""
        with dbengine.connect() as cursor:
            cursor.execute(f"USE {db_name}")
            d = cursor.execute(f"SELECT * FROM {load_path}")
            db_df = pd.DataFrame(d)
        
        # Get unique values to check exist
        if isinstance(unique_key, str):
            unique_key = [unique_key]
        
        # Check matching columns 
        database_cols = [col for col in db_df.columns if col != "ingestion_time"]
        if self.match_columns(df, database_cols):
            raise KeyError(
                f"Not matching between database cols {list(df.columns)}" 
                f"and table cols {database_cols}")

        # Inject ingestion Time
        df["ingestion_time"] = self.get_ingestion_time()
        colnames = ", ".join([f"`{col}`" for col in df.columns])

        # Load the PD table 
        with dbengine.connect() as cursor:
            df.to_sql(table_name, con=dbengine, if_exists='append', index=False)
            # cursor.commit()

    def _append_load_sqlalchemy(self):
        # TODO: to be implemented
        pass
    
    # TODO: Clean up
    def _incremental_load(
            self, 
            df : pd.DataFrame, 
            unique_key : str | List[str],
            db_name : str, 
            table_name : str, 
            load_path : str,
            dbconnect : Optional[str] = None, # TODO: change type hint here
            # dbengine : Optional[str] = None,
        ) -> None:
        cursor = dbconnect.cursor(buffered=True)
        cursor.execute(f"USE {db_name}")
        cursor.execute(f"SELECT * FROM {load_path}")
        db_df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        
        # Get unique values to check exist
        if isinstance(unique_key, str):
            unique_key = [unique_key]

        # Check matching columns 
        database_cols = [col for col in cursor.column_names if col != "ingestion_time"]
        if self.match_columns(df, database_cols):
            raise KeyError(
                f"Not matching between database cols {list(df.columns)}" 
                f"and table cols {database_cols}")

        # Inject ingestion Time
        df["ingestion_time"] = self.get_ingestion_time()
        colnames = ", ".join([f"`{col}`" for col in df.columns])

        # Filter only those not exists 
        # df = self._filter_exists(df, db_df, unique_key)
        
        # Create prepared cursor 
        cursor = dbconnect.cursor(prepared=True)

        # Load
        for _, row in df.iterrows():
            values = ", ".join(["?" for _ in range(len(df.columns))])
            sql = f"INSERT INTO {table_name} ({colnames}) " + f"VALUES ({values})"
            cursor.execute(sql, tuple(row))
    
    # TODO: Clean up
    def _append_load(self) -> None: 
        pass


class AzureMySQL(Loader):
    def __init__(self, 
        config : ConfigManager, 
        table_name : Optional[str] = None,
        load_method : Literal["incremental", "append"] = None,
        unique_key : str = None):
        super().__init__()
        self.config = config 
        self.table_name = table_name
        self.load_method = load_method
        self.unique_key = unique_key

        logger.info(f"Initialized {self.__class__.__name__} with {self.__dict__}")
    

    def db_connect(self, config : ConfigManager):
        driver = config.get("driver")
        server = config.get("server")
        database = config.get("db")
        username = config.get("user")
        password = config.get("password")
        dbconnect = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)

        return dbconnect
    
    @log_operation
    def load(
        self, 
        data : Dataset, 
        table_name : str = None,
        load_method : Literal["incremental", "append"] = None,
        unique_key : List[str] = None,
        load_method_mapping : Dict[str, str] = None,
    ) -> None:
        if not unique_key:
            unique_key = self.unique_key
        if not load_method:
            load_method = self.load_method
        if not table_name:
            table_name = self.table_name

        # Get available dataset
        if not load_method_mapping and not load_method:
            raise ValueError(
                f"Invalid Load method {load_method}." 
                "Input load_method or load_method_mapping"
            )
        
        name = data.name

        # Fetch load method 
        if load_method_mapping:
            load_method = load_method_mapping[name]

        # Add data to database
        df = data.data

        # Get dbconnect 
        dbconnect = self.db_connect(config=self.config)
        
        # Incremental mode
        if load_method == "incremental":
            self._incremental_load(
                df, unique_key, 
                table_name, dbconnect,
            )
        elif load_method == "append":
            self._append_load(
                df,table_name
            )
        else:
            raise ValueError(f"Invalid Load method {load_method}")
            
        dbconnect.commit()
        dbconnect.close()
    
    def _incremental_load(
            self, 
            df : pd.DataFrame, 
            unique_key : str | List[str],
            table_name : str, 
            dbconnect : str, 
        ) -> None:
        """Incremental Load data"""

        # Set up cursor
        cursor = dbconnect.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        column_names = [column[0] for column in cursor.description]
        data = [list(d) for d in cursor.fetchall()]
        db_df = pd.DataFrame(data, columns=column_names)
        
        # Get unique values to check exist
        if isinstance(unique_key, str):
            unique_key = [unique_key]

        # Create prepared cursor 
        cursor = dbconnect.cursor()

        # Check matching columns 
        database_cols = [col for col in column_names if col != "ingestion_time" and col != "id"]
        if self.match_columns(df, database_cols):
            raise KeyError(
                f"Not matching between database cols {list(df.columns)}" 
                f"and table cols {database_cols}")

        # Inject ingestion Time
        df["ingestion_time"] = self.get_ingestion_time()
        colnames = ", ".join([f"`{col}`" for col in df.columns])
        
        # Load
        for _, row in tqdm(df.iterrows()):
            values = ", ".join(["?" for _ in range(len(df.columns))])
            sql = f"INSERT INTO {table_name} ({colnames}) " + f"VALUES ({values})"
            cursor.execute(sql, tuple(row))
    
    def _append_load(self, data : Dataset, table_name : str) -> None: 
        pass


class PostgreSQLLoader(Loader):
    def __init__(self, 
        config : ConfigManager, 
        table_name : Optional[str] = None,
        load_path : Optional[str] = None,
        load_method : Literal["incremental", "append"] = None,
        unique_key : str = None):

        super().__init__()
        self.config = config 
        self.table_name = table_name
        self.load_method = load_method
        self.unique_key = unique_key
        self.load_path = load_path

        logger.info(f"Initialized {self.__class__.__name__} with {self.__dict__}")

    def get_sqlalchemy_engine(self, config : ConfigManager) -> Connection:
        """Return pymysql engine"""

        if not config:
            config = self.config
        username = config.get("username")
        password = config.get("password")
        host = config.get("host")
        port = config.get("port")
        db = config.get("database")

        return create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{db}")

    def load(
        self,
        data: Dataset,
        load_path: Optional[str] = None,
        load_method: Literal["incremental", "append", "replace"] = None,
        unique_key: List[str] = None,
    ):
        if not unique_key:
            unique_key = self.unique_key
        if not load_method:
            load_method = self.load_method
        if not load_path:
            load_path = self.load_path

        # Get connect
        try:
            dbengine = self.get_sqlalchemy_engine(self.config)
        except:
            ValueError("Cannot connect to database on given config!")

        # Get schema_name and table_name
        schema_name, table_name = load_path.split(".")
        if self.table_name:
            table_name = self.table_name
        else:
            self.table_name = table_name

        # Add data to database
        df = data.data

        # Incremental mode
        if load_method == "incremental":
            self._incremental_load_sqlalchemy(
                df, unique_key, schema_name, table_name, load_path, dbengine
            )
        elif load_method == "append":
            self._append_load(df, load_path)
        else:
            raise ValueError(f"Invalid Load method {load_method}")

    def _incremental_load_sqlalchemy(
        self,
        df: pd.DataFrame,
        unique_key: str | List[str],
        schema_name: str,
        table_name: str,
        load_path: str,
        dbengine: Optional[str] = None,
    ) -> None:
        """Loading data with sqlalchemy lib and pandas."""

        db_df = pd.read_sql(f"SELECT * FROM {load_path}", con=dbengine)

        # Get unique values to check exist
        if isinstance(unique_key, str):
            unique_key = [unique_key]

        # Check matching columns
        database_cols = [col for col in db_df.columns if col != "ingestion_time"]
        if self.match_columns(df, database_cols):
            raise KeyError(
                f"Not matching between database cols {list(df.columns)}"
                f"and table cols {database_cols}"
            )

        # Inject ingestion Time
        df["ingestion_time"] = self.get_ingestion_time()

        # Load the PD table
        with dbengine.connect() as connection:
            with connection.begin():
                df.to_sql(
                    table_name,
                    con=dbengine,
                    schema=schema_name,
                    if_exists="append",
                    index=False,
                )

    def _append_load_sqlalchemy(self):
        # TODO: to be implemented
        pass
