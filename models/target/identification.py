class Identification:
    """Target model for identification table"""
    
    def __init__(self):
        """Initialize an empty Identification object"""
        self.identification_id = None
        self.number = None
        self.type_id = "2e68062d-adee-11e7-b30f-3372a2d8551e"
        self.type = "National Id"
        self.person_id = None
        
    def to_dict(self):
        """Convert model to dictionary for database operations"""
        return {
            'identification_id': self.identification_id,
            'number': self.number,
            'type_id': self.type_id,
            'type': self.type,
            'person_id': self.person_id
        }