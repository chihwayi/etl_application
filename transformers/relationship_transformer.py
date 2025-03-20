import logging
import pandas as pd
import uuid
from utils.enums import RelationshipType

class RelationshipTransformer:
    """Transforms family information data to relationship data"""
    
    def __init__(self):
        """Initialize relationship transformer"""
        self.logger = logging.getLogger(__name__)
        
    def transform(self, family_info_df, patient_person_mapping):
        """Transform family information to relationship data
        
        Args:
            family_info_df (DataFrame): Family information data
            patient_person_mapping (dict): Mapping of patient IDs to person IDs
            
        Returns:
            DataFrame: Transformed relationship data
        """
        self.logger.info("Transforming family information to relationship data")
        
        if family_info_df.empty:
            self.logger.warning("No family information data to transform")
            return pd.DataFrame()
            
        # Filter records where both patient and relative exist in the mapping
        valid_records = []
        
        for _, row in family_info_df.iterrows():
            patient_id = row['PatientID']
            relative_id = row['RelativeCTCID']
            
            # Skip if either patient or relative is not in the mapping
            if patient_id not in patient_person_mapping or relative_id not in patient_person_mapping:
                continue
                
            valid_records.append({
                'relationship_id': str(uuid.uuid4()),
                'relation': RelationshipType.map_from_source(row['RelativeType']),
                'member_id': patient_person_mapping[relative_id],  # The relative's person_id
                'person_id': patient_person_mapping[patient_id],   # The patient's person_id
                'type_of_contact': 'PRIMARY'
            })
            
        if not valid_records:
            self.logger.warning("No valid relationships found after mapping")
            return pd.DataFrame()
            
        # Create DataFrame from valid records
        relationship_df = pd.DataFrame(valid_records)
        self.logger.info(f"Transformed {len(relationship_df)} relationship records")
        
        return relationship_df