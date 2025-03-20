import pandas as pd
import logging
from transformers.common import CommonTransformer
from utils.helpers import format_national_id

logger = logging.getLogger(__name__)

class IdentificationTransformer:
    """Transform patient data to identification data"""
    
    def transform(self, patients_df, persons_df):
        """Transform patient data to identification data
        
        Args:
            patients_df (pd.DataFrame): DataFrame containing patient data
            persons_df (pd.DataFrame): DataFrame containing transformed person data
            
        Returns:
            pd.DataFrame: Transformed identification data
        """
        logger.info(f"Transforming {len(patients_df)} patient records to identification records")
        
        # Create a dictionary mapping original patient IDs to person IDs
        patient_to_person = dict(zip(
            persons_df['original_patient_id'], 
            persons_df['person_id']
        ))
        
        # Create an empty list for the transformed data
        identifications = []
        
        # Process each patient
        for _, patient in patients_df.iterrows():
            # Skip patients without national IDs
            if pd.isna(patient.get('NationalID')) or not patient.get('NationalID'):
                continue
                
            # Get the corresponding person_id
            person_id = patient_to_person.get(patient.get('PatientID'))
            if not person_id:
                logger.warning(f"No person_id found for patient {patient.get('PatientID')}")
                continue
                
            # Format national ID
            national_id = format_national_id(patient.get('NationalID'))
            
            # Create identification record
            identification = {
                'identification_id': CommonTransformer.generate_uuid(),
                'number': national_id,
                'type_id': '2e68062d-adee-11e7-b30f-3372a2d8551e',
                'type': 'National Id',
                'person_id': person_id
            }
            
            identifications.append(identification)
        
        # Convert to DataFrame
        if identifications:
            identifications_df = pd.DataFrame(identifications)
            logger.info(f"Transformed {len(identifications_df)} identification records")
            return identifications_df
        else:
            logger.warning("No identification records were created")
            return pd.DataFrame(columns=[
                'identification_id', 'number', 'type_id', 'type', 'person_id'
            ])