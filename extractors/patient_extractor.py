import logging
import pandas as pd
from core.base_etl import BaseETL
from core.query_builder import QueryBuilder

logger = logging.getLogger(__name__)

class PatientExtractor:
    """Class for extracting patient data from source database (ctc2data)"""
    
    def __init__(self, source_conn, batch_size=1000):
        """Initialize with source connection
        
        Args:
            source_conn: Source database connection
            batch_size (int): Number of records to extract per batch
        """
        self.source_conn = source_conn
        self.batch_size = batch_size
        
    def extract(self):
        """Main extract method to fulfill the interface requirement
        This method calls extract_all() by default
        
        Returns:
            pd.DataFrame: DataFrame with all patient records
        """
        return self.extract_all()
        
    def extract_all(self):
        """Extract all patient records
        
        Returns:
            pd.DataFrame: DataFrame with all patient records
        """
        query = QueryBuilder.select_all_from("tblpatients")
        logger.info("Extracting all patients from tblpatients")
        return self.source_conn.execute_query(query)
    
    def extract_batch(self, offset=0):
        """Extract a batch of patient records
        
        Args:
            offset (int): Number of records to skip
            
        Returns:
            pd.DataFrame: DataFrame with batch of patient records
        """
        query = QueryBuilder.select_with_limit("tblpatients", self.batch_size, offset)
        logger.info(f"Extracting batch of patients with offset {offset}")
        return self.source_conn.execute_query(query)
    
    def extract_by_id(self, patient_ids):
        """Extract patient records by ID
        
        Args:
            patient_ids (list): List of patient IDs to extract
            
        Returns:
            pd.DataFrame: DataFrame with matching patient records
        """
        if not patient_ids:
            return pd.DataFrame()
            
        query = QueryBuilder.batch_select_query("tblpatients", "PatientID", patient_ids)
        logger.info(f"Extracting {len(patient_ids)} patients by ID")
        return self.source_conn.execute_query(query)
    
    def get_total_count(self):
        """Get total count of patient records
        
        Returns:
            int: Total number of patient records
        """
        query = "SELECT COUNT(*) as count FROM tblpatients"
        result = self.source_conn.execute_query(query)
        count = result['count'].iloc[0]
        logger.info(f"Total patient count: {count}")
        return count
    
    def get_facility_data(self, facility_conn):
        """Extract facility data for reference
        
        Args:
            facility_conn: Facility database connection
            
        Returns:
            dict: Dictionary containing facility reference data
        """
        # Extract provinces
        provinces = facility_conn.execute_query(QueryBuilder.select_all_from("province"))
        
        # Extract districts
        districts = facility_conn.execute_query(QueryBuilder.select_all_from("district"))
        
        # Extract facilities
        facilities = facility_conn.execute_query(QueryBuilder.select_all_from("facility"))
        
        # Extract towns
        towns = facility_conn.execute_query(QueryBuilder.select_all_from("town"))
        
        logger.info(f"Extracted reference data: {len(provinces)} provinces, {len(districts)} districts, "
                   f"{len(facilities)} facilities, {len(towns)} towns")
        
        return {
            'provinces': provinces,
            'districts': districts,
            'facilities': facilities,
            'towns': towns
        }