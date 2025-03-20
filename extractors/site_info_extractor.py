import logging
import re
import pandas as pd

logger = logging.getLogger(__name__)

class SiteInfoExtractor:
    """Class for extracting site information from MRS database"""
    
    def __init__(self, mrs_conn):
        """Initialize with MRS connection
        
        Args:
            mrs_conn: MRS database connection
        """
        self.mrs_conn = mrs_conn
    
    def get_metadata_value(self, metadata, key):
        """Returns the value for a given key from the metadata string
        
        Args:
            metadata (str): Metadata string
            key (str): Key to search for
            
        Returns:
            str: Value for the key or None if not found
        """
        match = re.search(rf"<string>{key}</string><string>(.*?)</string>", metadata)
        return match.group(1) if match else None
    
    def extract_site_id(self):
        """Extract site ID from MRS database
        
        Returns:
            str: Site ID or None if not found
        """
        try:
            logger.info("Extracting site ID from MRS database")
            
            # Query to get the latest metadata entry
            metadata_query = """
            SELECT meta_data FROM domain_event_entry 
            WHERE time_stamp = (SELECT MAX(time_stamp) FROM domain_event_entry)
            """
            
            metadata_df = self.mrs_conn.execute_query(metadata_query)
            
            if metadata_df.empty:
                logger.warning("No metadata found in MRS database")
                return None
            
            # Get the metadata and decode if necessary
            meta_data = metadata_df['meta_data'].iloc[0]
            if isinstance(meta_data, bytes):
                meta_data = meta_data.decode('utf-8')
            
            # Extract site ID
            site_id = self.get_metadata_value(meta_data, 'siteId')
            
            if site_id:
                logger.info(f"Found site ID: {site_id}")
            else:
                logger.warning("Site ID not found in metadata")
                
            return site_id
            
        except Exception as e:
            logger.error(f"Error extracting site ID: {str(e)}")
            return None