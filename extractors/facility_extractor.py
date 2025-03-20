import pandas as pd
import logging
from core.query_builder import QueryBuilder

logger = logging.getLogger(__name__)

class FacilityExtractor:
    """Extract facility data from facility database"""
    
    def __init__(self, facility_conn):
        """Initialize with facility connection
        
        Args:
            facility_conn: Facility database connection
        """
        self.facility_conn = facility_conn
    
    def extract_towns(self):
        """Extract all town data
        
        Returns:
            pd.DataFrame: Extracted town data
        """
        logger.info("Extracting town data")
        
        query = QueryBuilder.select_all_from("town")
        towns_df = self.facility_conn.execute_query(query)
        
        logger.info(f"Extracted {len(towns_df)} town records")
        return towns_df
    
    def extract_facilities(self):
        """Extract all facility data
        
        Returns:
            pd.DataFrame: Extracted facility data
        """
        logger.info("Extracting facility data")
        
        query = QueryBuilder.select_all_from("facility")
        facilities_df = self.facility_conn.execute_query(query)
        
        logger.info(f"Extracted {len(facilities_df)} facility records")
        return facilities_df