class Person:
    """Target model for person table"""
    
    def __init__(self):
        """Initialize an empty Person object"""
        self.person_id = None
        self.city = None
        self.street = None
        self.town_id = None
        self.town = None
        self.birthdate = None
        self.education_id = None
        self.education = None
        self.firstname = None
        self.lastname = None
        self.marital_id = None
        self.marital = None
        self.occupation_id = None
        self.occupation = None
        self.religion_id = None
        self.religion = None
        self.sex = None
        self.nationality_id = "fa2950f8-ad06-11e8-82c7-bca8a6cc451a"
        self.nationality = "Zimbabwe"
        self.denomination_id = None
        self.denomination = None
        self.country_of_birth_id = "41bf6a5e-fd7d-11e6-9840-000c29c7ff5e"
        self.country_of_birth = "Zimbabwe"
        self.self_identified_gender = None
        
    def to_dict(self):
        """Convert model to dictionary for database operations"""
        return {
            'person_id': self.person_id,
            'city': self.city,
            'street': self.street,
            'town_id': self.town_id,
            'town': self.town,
            'birthdate': self.birthdate,
            'education_id': self.education_id,
            'education': self.education,
            'firstname': self.firstname,
            'lastname': self.lastname,
            'marital_id': self.marital_id,
            'marital': self.marital,
            'occupation_id': self.occupation_id,
            'occupation': self.occupation,
            'religion_id': self.religion_id,
            'religion': self.religion,
            'sex': self.sex,
            'nationality_id': self.nationality_id,
            'nationality': self.nationality,
            'denomination_id': self.denomination_id,
            'denomination': self.denomination,
            'country_of_birth_id': self.country_of_birth_id,
            'country_of_birth': self.country_of_birth,
            'self_identified_gender': self.self_identified_gender
        }