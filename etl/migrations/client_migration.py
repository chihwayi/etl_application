import logging
import pandas as pd

from extractors.patient_extractor import PatientExtractor
from transformers.person_transformer import PersonTransformer
from transformers.id_transformer import IdentificationTransformer
from transformers.phone_transformer import PhoneTransformer
from loaders.person_loader import PersonLoader
from loaders.id_loader import IdentificationLoader
from loaders.phone_loader import PhoneLoader


class ClientMigration:
    """Handles migration of data to the client database (person, identification, phone)"""
    
    def __init__(self, connections, batch_size):
        """Initialize client migration
        
        Args:
            connections (dict): Dictionary of database connections
            batch_size (int): Batch size for processing
        """
        self.logger = logging.getLogger(__name__)
        self.connections = connections
        self.batch_size = batch_size
        
    def run(self):
        """Run the client data migration
        
        Returns:
            tuple: (Migration statistics, Patient-person mapping)
        """
        self.logger.info("Starting client data migration")
        
        stats = {
            'person_extracted': 0,
            'person_loaded': 0,
            'identification_loaded': 0,
            'phone_loaded': 0
        }
        
        # Create a dictionary to store the mapping between original patient ID and person ID
        patient_person_mapping = {}
        
        try:
            # Get facility town data for town matching
            self.logger.info("Fetching town data from facility database")
            town_query = "SELECT town_id, name FROM town"
            town_df = self.connections['facility'].execute_query(town_query)
            
            # Extract patient data
            self.logger.info("Extracting patient data from source database")
            patient_extractor = PatientExtractor(self.connections['source'])
            patient_data = patient_extractor.extract()
            stats['person_extracted'] = len(patient_data)
            
            if patient_data.empty:
                self.logger.warning("No patient data extracted, skipping client migration")
                return stats, patient_person_mapping
            
            # First, check if records already exist and get their mappings
            patient_person_mapping = self._load_existing_mappings()
                
            # Process in batches
            for i in range(0, len(patient_data), self.batch_size):
                batch_results = self._process_batch(patient_data, i, town_df, patient_person_mapping)
                
                # Update stats with batch results
                stats['person_loaded'] += batch_results['person_loaded']
                stats['identification_loaded'] += batch_results['identification_loaded']
                stats['phone_loaded'] += batch_results['phone_loaded']
                
                # Update mapping with new mappings
                patient_person_mapping.update(batch_results['new_mappings'])
                
            self.logger.info("Client data migration complete")
            return stats, patient_person_mapping
            
        except Exception as e:
            self.logger.error(f"Error during client data migration: {str(e)}")
            raise
            
    def _load_existing_mappings(self):
        """Load existing patient-person mappings from database
        
        Returns:
            dict: Dictionary of patient ID to person ID mappings
        """
        patient_person_mapping = {}
        
        try:
            # Create a temporary table for existing patient mappings if it doesn't exist
            create_mapping_table_query = """
            CREATE TABLE IF NOT EXISTS person_patient_mapping (
                person_id VARCHAR(36) NOT NULL,
                original_patient_id VARCHAR(50) NOT NULL,
                PRIMARY KEY (original_patient_id),
                KEY (person_id)
            )
            """
            self.connections['client'].execute_update(create_mapping_table_query)
            
            # Check if we have existing mappings
            existing_patients_query = """
            SELECT person_id, original_patient_id FROM person_patient_mapping
            """
            existing_mapping_df = self.connections['client'].execute_query(existing_patients_query)
            
            if not existing_mapping_df.empty:
                # Load existing mappings
                for _, row in existing_mapping_df.iterrows():
                    patient_person_mapping[row['original_patient_id']] = row['person_id']
                self.logger.info(f"Loaded {len(patient_person_mapping)} existing mappings")
        except Exception as e:
            self.logger.warning(f"Could not load existing mappings: {str(e)}")
            
        return patient_person_mapping
        
    def _process_batch(self, patient_data, start_index, town_df, patient_person_mapping):
        """Process a batch of patient data
        
        Args:
            patient_data (DataFrame): Patient data
            start_index (int): Starting index for this batch
            town_df (DataFrame): Town data for town matching
            patient_person_mapping (dict): Existing patient-person mappings
            
        Returns:
            dict: Batch statistics and new mappings
        """
        batch_num = start_index // self.batch_size + 1
        self.logger.info(f"Processing batch {batch_num}")
        
        batch_stats = {
            'person_loaded': 0,
            'identification_loaded': 0,
            'phone_loaded': 0,
            'new_mappings': {}
        }
        
        # Get batch of data
        batch_end = min(start_index + self.batch_size, len(patient_data))
        batch_data = patient_data.iloc[start_index:batch_end].copy()
        
        # Transform person data
        person_transformer = PersonTransformer(town_df)
        person_data = person_transformer.transform(batch_data)
        
        # Update the mapping before loading
        new_mappings = []
        for _, row in person_data.iterrows():
            patient_id = row['original_patient_id']
            person_id = row['person_id']
            batch_stats['new_mappings'][patient_id] = person_id
            new_mappings.append({
                'person_id': person_id,
                'original_patient_id': patient_id
            })
        
        # Save mappings to database
        if new_mappings:
            self._save_mappings(new_mappings)
        
        # Load person data - this will strip out original_patient_id
        person_loader = PersonLoader(self.connections['client'])
        loaded_person_count = person_loader.load(person_data)
        batch_stats['person_loaded'] = loaded_person_count
        
        # Transform and load identification data
        id_transformer = IdentificationTransformer()
        id_data = id_transformer.transform(batch_data, person_data)
        
        if not id_data.empty:
            id_loader = IdentificationLoader(self.connections['client'])
            loaded_id_count = id_loader.load(id_data)
            batch_stats['identification_loaded'] = loaded_id_count
        
        # Transform and load phone data
        phone_transformer = PhoneTransformer()
        phone_data = phone_transformer.transform(batch_data, person_data)
        
        if not phone_data.empty:
            phone_loader = PhoneLoader(self.connections['client'])
            loaded_phone_count = phone_loader.load(phone_data)
            batch_stats['phone_loaded'] = loaded_phone_count
            
        self.logger.info(f"Batch {batch_num} complete")
        
        return batch_stats
        
    def _save_mappings(self, new_mappings):
        """Save patient-person mappings to database
        
        Args:
            new_mappings (list): List of mapping dictionaries
        """
        mapping_df = pd.DataFrame(new_mappings)
        try:
            # Use REPLACE to handle updates of existing mappings
            with self.connections['client'].get_connection() as conn:
                mapping_df.to_sql(
                    name='person_patient_mapping',
                    con=conn,
                    if_exists='append',
                    index=False,
                    chunksize=1000,
                    method='multi'  # Important for handling conflicts
                )
            self.logger.info(f"Saved {len(new_mappings)} mappings to database")
        except Exception as e:
            self.logger.warning(f"Could not save mappings: {str(e)}")