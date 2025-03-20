import logging
import pandas as pd
from core.query_builder import QueryBuilder

class FamilyInfoExtractor:
    """Extracts family information data from the source database"""
    
    def __init__(self, source_connection):
        """Initialize the family info extractor
        
        Args:
            source_connection: Database connection to source database
        """
        self.logger = logging.getLogger(__name__)
        self.source_connection = source_connection
        
    def extract(self):
        """Extract family information with non-null RelativeCTCID
        
        Returns:
            DataFrame: Family information data
        """
        self.logger.info("Extracting family information data")
        
        query = QueryBuilder.select_with_condition(
            "tblfamilyinfo", 
            "RelativeCTCID IS NOT NULL AND RelativeCTCID != ''"
        )
        
        try:
            family_info_df = self.source_connection.execute_query(query)
            self.logger.info(f"Extracted {len(family_info_df)} family information records")
            return family_info_df
        except Exception as e:
            self.logger.error(f"Error extracting family information: {str(e)}")
            raise