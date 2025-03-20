class Phone:
    """Target model for phone table"""
    
    def __init__(self):
        """Initialize an empty Phone object"""
        self.phone_id = None
        self.number = None
        self.person_id = None
        
    def to_dict(self):
        """Convert model to dictionary for database operations"""
        return {
            'phone_id': self.phone_id,
            'number': self.number,
            'person_id': self.person_id
        }