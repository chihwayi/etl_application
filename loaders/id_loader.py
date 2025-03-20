import logging
import pandas as pd
from core.base_etl import BaseETL

logger = logging.getLogger(__name__)

class IdentificationLoader:
    """Class responsible for loading transformed identification data into the client database"""
    
    def __init__(self, target_conn):
        """Initialize the loader
        
        Args:
            target_conn: Target database connection (client)
        """
        self.target_conn = target_conn
        
    def load(self, data):
        """Load transformed identification data into the target database
        
        Args:
            data (pd.DataFrame): Transformed identification data
            
        Returns:
            int: Number of records loaded
        """
        if data.empty:
            logger.warning("No identification data to load")
            return 0
            
        try:
            # Insert the data into the identification table
            logger.info(f"Loading {len(data)} identification records into client database")
            
            # Map DataFrame columns to table columns
            table_name = 'identification'
            
            # Perform the bulk insert
            rows_loaded = self.target_conn.bulk_insert_dataframe(data, table_name)
            
            logger.info(f"Successfully loaded {rows_loaded} identification records")
            return rows_loaded
            
        except Exception as e:
            logger.error(f"Error loading identification data: {str(e)}")
            raise