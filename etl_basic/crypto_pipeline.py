# ----------------------------------------------
#  Cryptography ETLs
# ----------------------------------------------

# Import
import requests
from pathlib import Path
import pandas as pd
import json
from typing import *
from pathlib import Path

print(Path.cwd())

# Local import
from dev.scripts.pipeline import *
from dev.utils.api_client import *
from dev.utils.logging import *


class CryptoExtractor(Extractor):
    """ "Extract CryptoCurrency info"""

    def __init__(
        self,
        api_client: APIClient,
        endpoint: str | List[str],
        config: Optional[ConfigManager] = None,
    ) -> None:
        super().__init__()
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
        super().__init__()
        self.rename_map = rename_map
        logger.info(f"Initialized {self.__class__.__name__} with {self.__dict__}")

    @log_operation
    def transform(self, data: Dataset) -> Dataset:
        if self.rename_map:
            return Dataset(data=data.data.rename(self.rename_map))
        else:
            data.name = "dim_crypto"
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
    transform_stage = CryptoTransformer()

    # Load Stage
    loader_stage = MySQLLoader(
        config, 
        table_mapping="airflowdb.dim_crypto",
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
        transform_stage = CryptoTransformer()
        logger.info("Finished Transform data")
        x = transform_stage.run(x)

        # Load to db
        logger.info("Loading to database...")
        loader_stage = MySQLLoader(
            db_config, 
            table_mapping="airflowdb.dim_crypto",
            load_method="incremental",
            unique_key="symbol",
        )
        loader_stage.run(x)
        logger.info("Finished load data to db")

    
