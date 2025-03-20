import pandas as pd
import logging
from transformers.common import CommonTransformer

logger = logging.getLogger(__name__)

class ArtTransformer:
    """Transform patient data to art data"""
    
    def __init__(self, person_df):
        """Initialize with person reference data
        
        Args:
            person_df (pd.DataFrame): DataFrame containing person data with original_patient_id
        """
        self.person_df = person_df
        # Create a mapping of original patient IDs to person IDs for easy lookup
        self.patient_to_person_map = dict(zip(
            person_df['original_patient_id'], 
            person_df['person_id']
        ))
    
    def transform(self, patients_df):
        """Transform patient data to art data
        
        Args:
            patients_df (pd.DataFrame): DataFrame containing patient data
            
        Returns:
            pd.DataFrame: Transformed art data
        """
        logger.info(f"Transforming {len(patients_df)} patient records to art records")
        
        # Create an empty list for the transformed data
        art_records = []
        
        # Process each patient
        for _, patient in patients_df.iterrows():
            patient_id = patient.get('PatientID')
            
            # Skip patients not in the person table
            if patient_id not in self.patient_to_person_map:
                logger.warning(f"Patient {patient_id} not found in person table, skipping")
                continue
                
            # Get the corresponding person_id
            person_id = self.patient_to_person_map[patient_id]
            
            # Generate art_id
            art_id = CommonTransformer.generate_uuid()
            
            # Format the art cohort number
            art_cohort_number = CommonTransformer.map_cohort_number(patient.get('FileRef'))
            
            # Create art record
            art_record = {
                'art_id': art_id,
                'art_number': patient_id,
                'date_of_hiv_test': CommonTransformer.clean_date(patient.get('DateConfirmedHIVPositive')),
                'person_id': person_id,
                'date_enrolled': CommonTransformer.clean_date(patient.get('TheTimeStamp')),
                'date': CommonTransformer.clean_date(patient.get('TheTimeStamp')),
                'art_cohort_number': art_cohort_number,
                # Set default values for other fields that aren't mapped
                'central_nervous_system': None,
                'cyanosis': None,
                'enlarged_lymph_node': None,
                'jaundice': None,
                'mental_status': None,
                'pallor': None,
                'tracing': None,
                'follow_up': None,
                'status': None,
                'relation': None,
                'date_of_disclosure': None,
                'reason': None,
                'hiv_status': None,
                'index_client_profile': None
            }
            
            art_records.append(art_record)
        
        # Convert to DataFrame
        art_df = pd.DataFrame(art_records)
        
        logger.info(f"Transformed {len(art_df)} art records")
        return art_df