import pandas as pd
import logging
from transformers.common import CommonTransformer

logger = logging.getLogger(__name__)

class PersonTransformer:
    """Transform patient data to person data"""
    
    def __init__(self, town_df):
        """Initialize with town reference data
        
        Args:
            town_df (pd.DataFrame): DataFrame containing town data
        """
        self.town_df = town_df
    
    def transform(self, patients_df):
        """Transform patient data to person data
        
        Args:
            patients_df (pd.DataFrame): DataFrame containing patient data
            
        Returns:
            pd.DataFrame: Transformed person data
        """
        logger.info(f"Transforming {len(patients_df)} patient records to person records")
        
        # Create an empty DataFrame for the transformed data
        persons = []
        
        # Track generated UUIDs to avoid duplicates
        generated_uuids = set()
    
        # Process each patient
        for _, patient in patients_df.iterrows():
            # Generate unique person_id
            person_id = CommonTransformer.generate_uuid()
        
            # Ensure the UUID is unique
            while person_id in generated_uuids:
                person_id = CommonTransformer.generate_uuid()
        
            # Add to tracking set
            generated_uuids.add(person_id)
            
            # Map town
            town_id, town = CommonTransformer.find_matching_town(
                patient.get('PhysicalAddress'), self.town_df
            )
            
            # Map education level
            education_id, education = CommonTransformer.map_education_level(
                patient.get('EducationLevelID')
            )
            
            # Map marital status
            marital_id, marital = CommonTransformer.map_marital_status(
                patient.get('MaritalStatus')
            )
            
            # Map occupation
            occupation_id, occupation = CommonTransformer.map_occupation(
                patient.get('Occupation')
            )
            
            # Create person record
            person = {
                'person_id': person_id,
                'city': town,
                'street': patient.get('PhysicalAddress'),
                'town_id': town_id,
                'town': town,
                'birthdate': CommonTransformer.clean_date(patient.get('DateOfBirth')),
                'education_id': education_id,
                'education': education,
                'firstname': patient.get('FirstName'),
                'lastname': patient.get('SurName'),
                'marital_id': marital_id,
                'marital': marital,
                'expiration_date': CommonTransformer.clean_date(patient.get('DateOfDeath')),
                'member': patient.get('MedInsMemberName'),
                'member_number': patient.get('MedInsNumber'),
                'provider_id': None,
                'provider': patient.get('MedInsSchemeName'),
                'occupation_id': occupation_id,
                'occupation': occupation,
                'religion_id': None,
                'religion': None,
                'sex': patient.get('Sex'),
                'nationality_id': 'fa2950f8-ad06-11e8-82c7-bca8a6cc451a',
                'nationality': 'Zimbabwe',
                'denomination_id': None,
                'denomination': None,
                'country_of_birth_id': '41bf6a5e-fd7d-11e6-9840-000c29c7ff5e',
                'country_of_birth': 'Zimbabwe',
                'self_identified_gender': patient.get('Sex'),
                # Store original PatientID for reference in other transformers
                'original_patient_id': patient.get('PatientID')
            }
            
            persons.append(person)
        
        # Convert to DataFrame
        persons_df = pd.DataFrame(persons)
        
        logger.info(f"Transformed {len(persons_df)} person records")
        return persons_df