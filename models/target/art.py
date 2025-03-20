class Art:
    """Target model for the consultation.art table"""
    
    def __init__(self, art_id=None, art_number=None, central_nervous_system=None,
                 cyanosis=None, date_of_hiv_test=None, enlarged_lymph_node=None,
                 jaundice=None, mental_status=None, pallor=None, person_id=None,
                 date_enrolled=None, tracing=None, follow_up=None, status=None,
                 relation=None, date_of_disclosure=None, reason=None, date=None,
                 hiv_status=None, art_cohort_number=None, index_client_profile=None):
        """Initialize an Art object
        
        Args:
            art_id (str): UUID for the art record
            art_number (str): Patient ID from the source system
            central_nervous_system (str): Central nervous system status
            cyanosis (bool): Cyanosis indicator
            date_of_hiv_test (str): Date of HIV test
            enlarged_lymph_node (bool): Enlarged lymph node indicator
            jaundice (bool): Jaundice indicator
            mental_status (str): Mental status
            pallor (bool): Pallor indicator
            person_id (str): Reference to the person ID in client.person
            date_enrolled (str): Date enrolled in ART
            tracing (bool): Tracing indicator
            follow_up (bool): Follow-up indicator
            status (bool): Status indicator
            relation (str): Relation information
            date_of_disclosure (str): Date of disclosure
            reason (str): Reason information
            date (str): Date information
            hiv_status (bool): HIV status indicator
            art_cohort_number (str): ART cohort number
            index_client_profile (str): Index client profile
        """
        self.art_id = art_id
        self.art_number = art_number
        self.central_nervous_system = central_nervous_system
        self.cyanosis = cyanosis
        self.date_of_hiv_test = date_of_hiv_test
        self.enlarged_lymph_node = enlarged_lymph_node
        self.jaundice = jaundice
        self.mental_status = mental_status
        self.pallor = pallor
        self.person_id = person_id
        self.date_enrolled = date_enrolled
        self.tracing = tracing
        self.follow_up = follow_up
        self.status = status
        self.relation = relation
        self.date_of_disclosure = date_of_disclosure
        self.reason = reason
        self.date = date
        self.hiv_status = hiv_status
        self.art_cohort_number = art_cohort_number
        self.index_client_profile = index_client_profile
    
    def to_dict(self):
        """Convert the Art object to a dictionary
        
        Returns:
            dict: Dictionary representation of the Art object
        """
        return {
            'art_id': self.art_id,
            'art_number': self.art_number,
            'central_nervous_system': self.central_nervous_system,
            'cyanosis': self.cyanosis,
            'date_of_hiv_test': self.date_of_hiv_test,
            'enlarged_lymph_node': self.enlarged_lymph_node,
            'jaundice': self.jaundice,
            'mental_status': self.mental_status,
            'pallor': self.pallor,
            'person_id': self.person_id,
            'date_enrolled': self.date_enrolled,
            'tracing': self.tracing,
            'follow_up': self.follow_up,
            'status': self.status,
            'relation': self.relation,
            'date_of_disclosure': self.date_of_disclosure,
            'reason': self.reason,
            'date': self.date,
            'hiv_status': self.hiv_status,
            'art_cohort_number': self.art_cohort_number,
            'index_client_profile': self.index_client_profile
        }