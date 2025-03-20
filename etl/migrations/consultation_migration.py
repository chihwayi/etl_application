import logging
import pandas as pd

from utils.helpers import get_site_id
from extractors.patient_extractor import PatientExtractor
from transformers.art_transformer import ArtTransformer
from loaders.art_loader import ArtLoader


class ConsultationMigration:
    """Handles migration of data to the consultation database"""
    
    def __init__(self, connections, batch_size, patient_person_mapping):
        """Initialize consultation migration
        
        Args:
            connections (dict): Dictionary of database connections
            batch_size (int): Batch size for processing
            patient_person_mapping (dict): Mapping of original patient IDs to person IDs
        """
        self.logger = logging.getLogger(__name__)
        self.connections = connections
        self.batch_size = batch_size
        self.patient_person_mapping = patient_person_mapping
        
    def run(self):
        """Run the consultation data migration
        
        Returns:
            dict: Migration statistics
        """
        self.logger.info("Starting consultation data migration")
        
        stats = {
            'patient_extracted': 0,
            'art_loaded': 0
        }
        
        try:
            # Get site ID from MRS database
            self.logger.info("Fetching site ID from MRS database")
            site_id = get_site_id(self.connections['mrs'])
            self.logger.info(f"Site ID: {site_id}")
            
            # Extract patient data
            self.logger.info("Extracting patient data from source database")
            patient_extractor = PatientExtractor(self.connections['source'])
            patient_data = patient_extractor.extract()
            stats['patient_extracted'] = len(patient_data)
            
            if patient_data.empty:
                self.logger.warning("No patient data extracted, skipping consultation migration")
                return stats
            
            # Create a DataFrame from the patient_person_mapping dictionary
            self.logger.info("Creating person reference data from mapping")
            person_data = pd.DataFrame([
                {'original_patient_id': k, 'person_id': v} 
                for k, v in self.patient_person_mapping.items()
            ])
            
            # Process in batches
            for i in range(0, len(patient_data), self.batch_size):
                batch_results = self._process_batch(patient_data, i, person_data)
                
                # Update stats with batch results
                stats['art_loaded'] += batch_results['art_loaded']
                
            self.logger.info("Consultation data migration complete")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error during consultation data migration: {str(e)}")
            raise
            
    def _process_batch(self, patient_data, start_index, person_data):
        """Process a batch of patient data
        
        Args:
            patient_data (DataFrame): Patient data
            start_index (int): Starting index for this batch
            person_data (DataFrame): Person data with mappings
            
        Returns:
            dict: Batch statistics
        """
        batch_num = start_index // self.batch_size + 1
        self.logger.info(f"Processing batch {batch_num}")
        
        batch_stats = {
            'art_loaded': 0
        }
        
        # Get batch of data
        batch_end = min(start_index + self.batch_size, len(patient_data))
        batch_data = patient_data.iloc[start_index:batch_end].copy()
        
        # Transform and load ART data
        art_transformer = ArtTransformer(person_data)
        art_data = art_transformer.transform(batch_data)
        
        if not art_data.empty:
            art_loader = ArtLoader(self.connections['consultation'])
            loaded_art_count = art_loader.load(art_data)
            batch_stats['art_loaded'] = loaded_art_count
            
        self.logger.info(f"Batch {batch_num} complete")
        
        return batch_stats