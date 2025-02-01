# ----------------------------------------------
#  Cryptography ETLs
# ----------------------------------------------
import requests
from pathlib import Path
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from typing import Optional, List, Dict

from dev.src.pipeline import Extractor, Dataset, Transformer, Pipeline
from dev.src.loader import MySQLLoader
from dev.src.database import DatabaseConnection
from dev.utils.config_manager import ConfigManager
from dev.utils.api_client import APIClient
from dev.utils.logging import logger, log_operation


class CryptoExtractor(Extractor):
    """Extract CryptoCurrency info"""

    def __init__(
        self,
        api_client: APIClient,
        endpoint: str | List[str],
        config: Optional[ConfigManager] = None,
    ) -> None:
        """
        Parameters
        -----------
        api_client : APIClient object to send HTTPs method. 
        config : ConfigManager includes authentication and url. Optional.
        endpoint : 
        """
        self.api_client = api_client
        self.config = config
        self.endpoint = endpoint

    @log_operation
    def extract(self) -> Dataset:
        api = self.api_client
        try:
            response = api.get(self.endpoint)
            response.raise_for_status()
            data = pd.DataFrame(response.json())
            return Dataset(data=data)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"API request failed: {e}")
        except ValueError as e:
            raise RuntimeError(f"Failed to process API response: {e}")


class CryptoTransformer(Transformer):
    """Transform the Crypto Currency info"""

    def __init__(self, rename_map: Optional[Dict[str, str]] = None):
        self.rename_map = rename_map
        logger.info(f"Initialized {self.__class__.__name__} with {self.__dict__}")

    @log_operation
    def transform(self, data: Dataset) -> Dataset:
        if self.rename_map:
            return Dataset(data=data.data.rename(columns=self.rename_map))
        return data


def CryptoETL():
    config = ConfigManager(config_path=Path.cwd() / "config.cfg", env="mysql-dev")
    api_config = ConfigManager(config_path=Path.cwd() / "config.cfg", env="finance_api")
    api = APIClient(api_config.get("api_key"))

    # Extract Stage
    extract_stage = CryptoExtractor(
        api_client=api, endpoint="symbol/available-cryptocurrencies"
    )
    # Transform Stage
    transform_stage = CryptoTransformer(
        rename_map={"stockExchange" : "stock_exchange", "exchangeShortName" : "exchange_short_name"})
    # Load Stage
    loader_stage = MySQLLoader(
        config, 
        load_path="airflowdb.dim_crypto",
        load_method="incremental",
        unique_key="symbol",
    )
    pipeline = Pipeline(
        stages=[
            extract_stage,
            transform_stage,
            loader_stage,
        ]
    )

    pipeline.run()

class CryptoPipeline(Pipeline):
    def __init__(
        self, 
        stages : Optional[List[Pipeline]] = None, 
        config : Optional[ConfigManager] = None, 
    ) -> None:
        super().__init__(stages, config)
    
    def run(self):
        # config 
        logger.info("Parsing Configuration...")
        db_config = ConfigManager(config_path=Path.cwd() / "config.cfg", env="mysql-dev")
        # azure_config = ConfigManager(config_path=Path.cwd() / "config.cfg", env="azure-mysql")
        # postgres_config = ConfigManager(config_path=Path.cwd() / "config.cfg", env="postgres-dev")
        api_config = ConfigManager(config_path=Path.cwd() / "config.cfg", env="finance-api")
        api = APIClient(api_config.get("api_key"))
        logger.info("Finished Parsing Configuration")

        # Extract 
        logger.info("Extracting data...")
        extract_stage = CryptoExtractor(
            api_client=api, endpoint="symbol/available-cryptocurrencies"
        )
        x = extract_stage.run()
        logger.info("Finished extracting data...")

        # Transform
        logger.info("Transforming data...")
        transform_stage = CryptoTransformer(
            rename_map={"stockExchange" : "stock_exchange", "exchangeShortName" : "exchange_short_name"})
        logger.info("Finished Transform data")
        x = transform_stage.run(x)

        # Load to db
        logger.info("Loading to MySQL database...")
        loader_stage = MySQLLoader(
            db_config, 
            load_path="airflowdb.dim_crypto",
            load_method="incremental",
            unique_key="symbol",
        )
        
        # loader_stage = PostgreSQLLoader(
        #     config=postgres_config,
        #     table_name="dim_crypto",
        #     load_path="public.dim_crypto",
        #     load_method="incremental",
        #     unique_key="id"
        # )
        
        # logger.info("Finished load data to MySQL db")

        # Load to Azure
        # logger.info("Loading to Azure SQL database...")
        # loader_stage = AzureMySQL(
        #     azure_config, 
        #     table_name="dim_crypto",
        #     load_method="incremental",
        #     unique_key="symbol",
        # )

        loader_stage.run(x)
        logger.info("Finished load data to MySQL db")


# ----------------------------------------------
#  Quota ETLs
# ----------------------------------------------
class QuotaPipeline(Pipeline):
    """ETL Pipline to extract quota object for top 10 Cryptocurrencies"""

    TOP_10_CRYPTO = ["BTC", "ETH", "USDT", "BNB", "USDC", "XRP", "SOL", "ADA", "DOGE", "DOT"]

    def __init__(
        self, 
        crypto_symbols : Optional[List[str]] = TOP_10_CRYPTO, 
        stages : Optional[List[Pipeline]] = None, 
        config : Optional[ConfigManager] = None, 
    ):
        """
        Parameters
        -----------
        crypto_symbols : list of cryptosymbols to be ingested for quotas.
        stages : 
        config : 
        """
        super().__init__(stages, config)
        self.crypto_symbols = crypto_symbols

    def run(self) -> None:
        logger.info("Parsing Configuration...")
        db_config = ConfigManager(config_path=Path.cwd() / "config.cfg", env="mysql-dev")
        api_config = ConfigManager(config_path=Path.cwd() / "config.cfg", env="finance-api")
        api = APIClient(api_config.get("api_key"))
        logger.info("Finished Parsing Configuration")

        # Extract 
        logger.info("Extracting data...")
        db_connection = DatabaseConnection(db_config, db_type="mysql")
        dbengine = db_connection.get_sqlalchemy_engine()
        with dbengine.connect() as cursor:
            cursor.execute(f"USE airflowdb")
            crypto_currency = pd.read_sql(
                """
                SELECT distinct REPLACE(symbol, currency, '') AS symbol 
                FROM dim_crypto
                """, 
            con=cursor)
        result = list()
        for symbol in tqdm(crypto_currency["symbol"]):
            extract_stage = CryptoExtractor(
                api_client=api, endpoint=f"quote/{symbol}"
            )
            result.append(extract_stage.run())
        x = self._combine_dataset(result)
        logger.info("Finished extracting data...")

        # Transformer 
        logger.info("Transforming data...")
        x.data["timestamp"] = pd.to_datetime(x.data["timestamp"])
        x.data["earningsAnnouncement"] = pd.to_datetime(x.data["earningsAnnouncement"])

        # Load to db
        logger.info("Loading to MySQL database...")
        loader_stage = MySQLLoader(
            db_config, 
            load_path="airflowdb.fact_cryptoquota",
            load_method="incremental",
            unique_key=["symbol", "timestamp"],
        )
        loader_stage.run(x)
        logger.info("Finished load data to MySQL db")



