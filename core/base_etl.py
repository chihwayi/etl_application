import logging
import pandas as pd
import uuid
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BaseETL(ABC):
    """Base class for ETL processes"""
    
    def __init__(self, source_conn, target_conn, batch_size=1000):
        """Initialize the ETL process
        
        Args:
            source_conn: Source database connection
            target_conn: Target database connection
            batch_size (int): Number of records to process in each batch
        """
        self.source_conn = source_conn
        self.target_conn = target_conn
        self.batch_size = batch_size
    
    @abstractmethod
    def extract(self):
        """Extract data from source database
        
        Returns:
            pd.DataFrame: Extracted data
        """
        pass
    
    @abstractmethod
    def transform(self, data):
        """Transform extracted data
        
        Args:
            data (pd.DataFrame): Data extracted from source
            
        Returns:
            pd.DataFrame: Transformed data
        """
        pass
    
    @abstractmethod
    def load(self, data):
        """Load transformed data into target database
        
        Args:
            data (pd.DataFrame): Transformed data
            
        Returns:
            int: Number of records loaded
        """
        pass
    
    def process(self):
        """Process the complete ETL pipeline
        
        Returns:
            dict: Statistics about the ETL process
        """
        logger.info(f"Starting ETL process using batch size: {self.batch_size}")
        
        stats = {
            'total_extracted': 0,
            'total_loaded': 0,
            'batch_count': 0
        }
        
        try:
            # Get total record count for progress tracking
            data = self.extract()
            total_records = len(data)
            stats['total_extracted'] = total_records
            
            logger.info(f"Extracted {total_records} records from source")
            
            # Process in batches
            for i in range(0, total_records, self.batch_size):
                batch_num = stats['batch_count'] + 1
                logger.info(f"Processing batch {batch_num}")
                
                # Get batch of data
                batch_end = min(i + self.batch_size, total_records)
                batch_data = data.iloc[i:batch_end]
                
                # Transform and load batch
                transformed_data = self.transform(batch_data)
                loaded_count = self.load(transformed_data)
                
                stats['total_loaded'] += loaded_count
                stats['batch_count'] += 1
                
                logger.info(f"Batch {batch_num} complete. Loaded {loaded_count} records")
                
            logger.info(f"ETL process complete. {stats['total_loaded']} records processed")
            return stats
            
        except Exception as e:
            logger.error(f"Error during ETL process: {str(e)}")
            raise
    
    @staticmethod
    def generate_uuid():
        """Generate a UUID string
        
        Returns:
            str: UUID string
        """
        return str(uuid.uuid4())
    
    @staticmethod
    def clean_date(date_str):
        """Clean a date string by removing time component
        
        Args:
            date_str: Date string or datetime object
            
        Returns:
            str: Cleaned date string in YYYY-MM-DD format or None
        """
        if pd.isna(date_str) or date_str is None:
            return None
            
        try:
            if isinstance(date_str, str):
                date_obj = pd.to_datetime(date_str)
            else:
                date_obj = pd.to_datetime(date_str)
            return date_obj.strftime('%Y-%m-%d')
        except Exception:
            return None