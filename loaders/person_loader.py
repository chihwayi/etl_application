import logging
import pandas as pd
from core.base_etl import BaseETL

logger = logging.getLogger(__name__)

class PersonLoader:
    """Class responsible for loading transformed person data into the client database"""
    
    def __init__(self, target_conn):
        """Initialize the loader
        
        Args:
            target_conn: Target database connection (client)
        """
        self.target_conn = target_conn
        
    def load(self, data):
        """Load transformed person data into the target database
        
        Args:
            data (pd.DataFrame): Transformed person data
            
        Returns:
            int: Number of records loaded
        """
        if data.empty:
            logger.warning("No person data to load")
            return 0
            
        try:
            # Insert the data into the person table
            logger.info(f"Loading {len(data)} person records into client database")
            
            # Create a copy of the dataframe to avoid modifying the original
            insert_data = data.copy()
            
            # Check for existing records to avoid duplicates
            try:
                existing_ids_query = """
                SELECT person_id FROM person WHERE person_id IN ({})
                """.format(','.join(['%s'] * len(insert_data)))
                
                existing_ids = self.target_conn.execute_query(
                    existing_ids_query, 
                    params=tuple(insert_data['person_id'].tolist())
                )
                
                if not existing_ids.empty:
                    # Filter out existing records
                    existing_ids_set = set(existing_ids['person_id'].tolist())
                    insert_data = insert_data[~insert_data['person_id'].isin(existing_ids_set)]
                    logger.info(f"Filtered out {len(existing_ids)} existing records")
            except Exception as e:
                logger.warning(f"Could not check for existing records: {str(e)}")
            
            # If all records already exist, return 0
            if insert_data.empty:
                logger.info("All records already exist in the database")
                return 0
                
            # Remove any columns that are not in the person table
            # Save original_patient_id in a separate variable before dropping it
            extra_columns = ['original_patient_id']  # Add any other temp columns here
            for col in extra_columns:
                if col in insert_data.columns:
                    insert_data = insert_data.drop(columns=[col])
            
            # Map DataFrame columns to table columns
            table_name = 'person'
            
            # Perform the bulk insert
            rows_loaded = self.target_conn.bulk_insert_dataframe(insert_data, table_name)
            
            logger.info(f"Successfully loaded {rows_loaded} person records")
            return rows_loaded
            
        except Exception as e:
            logger.error(f"Error loading person data: {str(e)}")
            raise