import pandas as pd
import logging
from transformers.common import CommonTransformer

logger = logging.getLogger(__name__)

class PhoneTransformer:
    """Transform patient data to phone data"""
    
    def transform(self, patients_df, persons_df):
        """Transform patient data to phone data
        
        Args:
            patients_df (pd.DataFrame): DataFrame containing patient data
            persons_df (pd.DataFrame): DataFrame containing transformed person data
            
        Returns:
            pd.DataFrame: Transformed phone data
        """
        logger.info(f"Transforming {len(patients_df)} patient records to phone records")
        
        # Create a dictionary mapping original patient IDs to person IDs
        patient_to_person = dict(zip(
            persons_df['original_patient_id'], 
            persons_df['person_id']
        ))
        
        # Create an empty list for the transformed data
        phones = []
        
        # Process each patient
        for _, patient in patients_df.iterrows():
            # Get the corresponding person_id
            person_id = patient_to_person.get(patient.get('PatientID'))
            if not person_id:
                logger.warning(f"No person_id found for patient {patient.get('PatientID')}")
                continue
            
            # Add cell phone if available
            if not pd.isna(patient.get('CellPhone')) and patient.get('CellPhone'):
                phones.append({
                    'phone_id': CommonTransformer.generate_uuid(),
                    'number': patient.get('CellPhone'),
                    'person_id': person_id
                })
            
            # Add phone if available and different from cell phone
            if (not pd.isna(patient.get('Phone')) and patient.get('Phone') and 
                patient.get('Phone') != patient.get('CellPhone')):
                phones.append({
                    'phone_id': CommonTransformer.generate_uuid(),
                    'number': patient.get('Phone'),
                    'person_id': person_id
                })
        
        # Convert to DataFrame
        if phones:
            phones_df = pd.DataFrame(phones)
            logger.info(f"Transformed {len(phones_df)} phone records")
            return phones_df
        else:
            logger.warning("No phone records were created")
            return pd.DataFrame(columns=['phone_id', 'number', 'person_id'])