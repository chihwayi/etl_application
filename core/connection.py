import logging
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Class to manage database connections"""
    
    def __init__(self, config):
        """Initialize with database configuration
        
        Args:
            config (dict): Dictionary containing database connection parameters
        """
        self.host = config['host']
        self.port = config['port']
        self.user = config['user']
        self.password = config['password']
        self.database = config['database']
        self._engine = None
    
    @property
    def engine(self):
        """Lazily create and return the SQLAlchemy engine"""
        if self._engine is None:
            connection_string = (
                f"mysql+pymysql://{self.user}:{self.password}@"
                f"{self.host}:{self.port}/{self.database}"
            )
            self._engine = create_engine(connection_string)
            logger.info(f"Created engine for {self.database} database")
        return self._engine
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connection"""
        connection = self.engine.connect()
        try:
            logger.debug(f"Opened connection to {self.database}")
            yield connection
        finally:
            connection.close()
            logger.debug(f"Closed connection to {self.database}")
    
    def execute_query(self, query, params=None):
        """Execute a SQL query and return the result
        
        Args:
            query (str): SQL query string
            params (dict, optional): Parameters for the query
            
        Returns:
            pd.DataFrame: Result of the query as a pandas DataFrame
        """
        try:
            with self.get_connection() as conn:
                logger.debug(f"Executing query on {self.database}: {query}")
                if params:
                    result = pd.read_sql(query, conn, params=params)
                else:
                    result = pd.read_sql(query, conn)
                logger.debug(f"Query returned {len(result)} rows")
                return result
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def execute_update(self, query, params=None):
        """Execute a SQL update/insert query
        
        Args:
            query (str): SQL query string
            params (tuple or list, optional): Parameters for the query
            
        Returns:
            int: Number of affected rows
        """
        try:
            with self.get_connection() as conn:
                with conn.begin() as transaction:
                    logger.debug(f"Executing update on {self.database}: {query}")
                    if params:
                        result = conn.execute(query, params)
                    else:
                        result = conn.execute(query)
                    affected_rows = result.rowcount
                    logger.debug(f"Update affected {affected_rows} rows")
                    return affected_rows
        except Exception as e:
            logger.error(f"Error executing update: {str(e)}")
            raise
    
    def bulk_insert_dataframe(self, df, table_name, if_exists='append'):
        """Bulk insert a DataFrame into a database table
        
        Args:
            df (pd.DataFrame): DataFrame containing data to insert
            table_name (str): Target table name
            if_exists (str): How to behave if the table exists (append, replace, fail)
            
        Returns:
            int: Number of rows inserted
        """
        try:
            with self.get_connection() as conn:
                logger.info(f"Bulk inserting {len(df)} rows into {table_name}")
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists=if_exists,
                    index=False,
                    chunksize=1000
                )
                return len(df)
        except Exception as e:
            logger.error(f"Error during bulk insert: {str(e)}")
            raise