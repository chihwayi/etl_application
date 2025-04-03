import logging
import pandas as pd
import subprocess
from io import StringIO

logger = logging.getLogger(__name__)

class MDEReader:
    """Class for reading reference data from MDE files using mdbtools"""
    
    def __init__(self, mde_file_path):
        """Initialize the MDE file reader
        
        Args:
            mde_file_path (str): Path to the MDE file
        """
        self.file_path = mde_file_path
        logger.info(f"Initialized MDEReader with file: {self.file_path}")

    def read_table(self, table_name):
        """Read a specific table from the MDE file
        
        Args:
            table_name (str): Name of the table to extract
            
        Returns:
            pd.DataFrame: DataFrame containing table data
        """
        try:
            # Use mdb-export to convert table to CSV
            cmd = f"mdb-export '{self.file_path}' '{table_name}'"
            logger.debug(f"Executing command: {cmd}")
            
            # Run the command and capture output
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"Error exporting {table_name}: {result.stderr}")
                return pd.DataFrame()
            
            # Read the CSV output into a DataFrame
            df = pd.read_csv(StringIO(result.stdout))
            
            logger.info(f"Read {len(df)} rows from {table_name}")
            return df
        
        except Exception as e:
            logger.error(f"Error reading table {table_name}: {str(e)}")
            return pd.DataFrame()

    def close(self):
        """No connection to close with mdbtools approach"""
        logger.debug("MDEReader close called (no-op with mdbtools)")