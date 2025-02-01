# ----------------------------------------------
# Define Database Connection strategies
# ----------------------------------------------
import mysql.connector as mysql
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing import Literal, Dict, Union

from dev.utils.config_manager import ConfigManager
from dev.utils.logging import logger

DB_CONFIG_KEYS = {
    "mysql": ["host", "user", "password", "db", "port"],
    "postgres": ["host", "user", "password", "db", "port"],  
    "azsql": ["host", "user", "password", "db", "port"]  
}

class DatabaseConnection:
    """Handle DB connection functionality for different DBMS"""

    def __init__(
        self, 
        config: ConfigManager, 
        db_type: Literal["mysql", "azsql", "postgres"] = "mysql"
    ):
        self.db_type = db_type
        self.config = config

    def _get_db_config(self) -> Dict[str, str]:
        """Fetch database configuration"""
        required_keys = DB_CONFIG_KEYS.get(self.db_type)
        if not required_keys:
            raise KeyError(f"Database connection type '{self.db_type}' is not supported.")

        db_config = {}
        for key in required_keys:
            try:
                db_config[key] = self.config.get(key)
            except KeyError:
                raise RuntimeError(f"Missing required key '{key}' in the configuration.")

        return db_config

    def connect_mysql(self) -> mysql.MySQLConnection:
        """
        Connect to MySQL database using configuration data.

        Returns
        ----------
        :class: `mysql.connections.Connection`
        """
        db_config = self._get_db_config()

        try:
            connection = mysql.connect(
                host=db_config["host"],
                user=db_config["user"],
                password=db_config["password"],
                db=db_config["db"],
                port=db_config["port"]
            )
            logger.info("Successfully connected to MySQL database.")
            return connection
        except mysql.MySQLError as e:
            logger.error(f"Failed to connect to MySQL: {e}")
            raise

    def connect_postgres(self):
        """Postgres connection logic (not implemented yet)."""
        raise NotImplementedError("Postgres connection logic is not implemented yet.")

    def connect_azuresql(self):
        """Azure SQL connection logic (not implemented yet)."""
        raise NotImplementedError("Azure SQL connection logic is not implemented yet.")

    def get_sqlalchemy_engine(self) -> Engine:
        """Return SQLAlchemy engine for the configured database.

        Returns
        ----------
        :class: `sqlalchemy.Engine`
        """
        db_config = self._get_db_config()

        try:
            username = db_config["user"]
            password = db_config["password"]
            host = db_config["host"]
            port = db_config["port"]
            db = db_config["db"]

            engine_url = f"{self.db_type}+pymysql://{username}:{password}@{host}:{port}/{db}"
            logger.info("Successfully created SQLAlchemy engine.")
            return create_engine(engine_url)
        except Exception as e:
            logger.error(f"Failed to create SQLAlchemy engine: {e}")
            raise

    def connect(self) -> Union[mysql.MySQLConnection, None]:
        """
        Universal connect function that connects to the specified database type.

        Returns:
            - MySQL: `mysql.connections.Connection`
            - PostgreSQL & Azure SQL: `None` (until implemented)
        """
        if self.db_type == "mysql":
            return self.connect_mysql()
        elif self.db_type == "postgres":
            return self.connect_postgres()
        elif self.db_type == "azsql":
            return self.connect_azuresql()
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
