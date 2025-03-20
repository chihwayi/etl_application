import logging
from extractors.family_info_extractor import FamilyInfoExtractor
from transformers.relationship_transformer import RelationshipTransformer
from loaders.relationship_loader import RelationshipLoader

class RelationshipMigration:
    """Handles migration of family information to relationships"""
    
    def __init__(self, connections, batch_size, patient_person_mapping):
        """Initialize relationship migration
        
        Args:
            connections (dict): Dictionary of database connections
            batch_size (int): Batch size for processing
            patient_person_mapping (dict): Mapping of patient IDs to person IDs
        """
        self.logger = logging.getLogger(__name__)
        self.connections = connections
        self.batch_size = batch_size
        self.patient_person_mapping = patient_person_mapping
        
    def run(self):
        """Run the relationship data migration
        
        Returns:
            int: Number of relationships loaded
        """
        self.logger.info("Starting relationship data migration")
        
        try:
            # Extract family information data
            family_info_extractor = FamilyInfoExtractor(self.connections['source'])
            family_info_data = family_info_extractor.extract()
            
            if family_info_data.empty:
                self.logger.warning("No family information data extracted")
                return 0
                
            # Transform to relationship data
            relationship_transformer = RelationshipTransformer()
            relationship_data = relationship_transformer.transform(
                family_info_data, 
                self.patient_person_mapping
            )
            
            if relationship_data.empty:
                self.logger.warning("No relationship data transformed")
                return 0
                
            # Load relationship data
            relationship_loader = RelationshipLoader(self.connections['client'])
            loaded_count = relationship_loader.load(relationship_data)
            
            self.logger.info(f"Relationship migration complete - loaded {loaded_count} records")
            return loaded_count
            
        except Exception as e:
            self.logger.error(f"Error during relationship migration: {str(e)}")
            raise