import logging
import pandas as pd

logger = logging.getLogger(__name__)

class ArtLoader:
    """Class responsible for loading transformed art data into the consultation database"""
    
    def __init__(self, target_conn):
        """Initialize the loader
        
        Args:
            target_conn: Target database connection (consultation)
        """
        self.target_conn = target_conn
        
    def load(self, data):
        """Load transformed art data into the target database
        
        Args:
            data (pd.DataFrame): Transformed art data
            
        Returns:
            int: Number of records loaded
        """
        if data.empty:
            logger.warning("No art data to load")
            return 0
            
        try:
            # Insert the data into the art table
            logger.info(f"Loading {len(data)} art records into consultation database")
            
            # Create a copy of the dataframe to avoid modifying the original
            insert_data = data.copy()
            
            # Perform the bulk insert
            table_name = 'art'
            
            # Perform the bulk insert
            rows_loaded = self.target_conn.bulk_insert_dataframe(insert_data, table_name)
            
            logger.info(f"Successfully loaded {rows_loaded} art records")
            return rows_loaded
            
        except Exception as e:
            logger.error(f"Error loading art data: {str(e)}")
            raise