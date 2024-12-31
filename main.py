from etl_basic.crypto_pipeline import (
    CryptoPipeline, 
    QuotaPipeline,
)
from dev.utils.logging import *

if __name__ == "__main__":
    # Fetching Crypto symbol data
    logger.info("TRIGGER CRYPTO SYMBOL PIPELINE...")
    pipe = CryptoPipeline()
    pipe.run()
    logger.info("FINISHED CRYPTO SYMBOL PIPELINE")

    # Fetching quotation for cryptocurrency
    logger.info("TRIGGER CRYPTO QUOTATOIN PIPELINE...")
    pipe = QuotaPipeline()
    pipe.run()
    logger.info("FINISHED CRYPTO QUOTATON PIPELINE")