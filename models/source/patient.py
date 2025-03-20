class Patient:
    """Source model for tblpatients table"""
    
    def __init__(self, row):
        """
        Initialize Patient from a database row
        
        Args:
            row: Dictionary-like object containing patient data
        """
        self.patient_id = row.get('PatientID')
        self.first_name = row.get('FirstName')
        self.surname = row.get('SurName')
        self.national_id = row.get('NationalID')
        self.date_of_birth = row.get('DateOfBirth')
        self.sex = row.get('Sex')
        self.marital_status = row.get('MaritalStatus')
        self.med_ins_scheme_name = row.get('MedInsSchemeName')
        self.med_ins_member_name = row.get('MedInsMemberName')
        self.med_ins_number = row.get('MedInsNumber')
        self.med_ins_relationship = row.get('MedInsRelationshipToMember')
        self.next_kin_name = row.get('NameNextKin')
        self.physical_address = row.get('PhysicalAddress')
        self.next_kin_address = row.get('PhysicalAddressNextKin')
        self.phone = row.get('Phone')
        self.next_kin_phone = row.get('PhoneNextKin')
        self.cell_phone = row.get('CellPhone')
        self.next_kin_cell_phone = row.get('CellPhoneNextKin')
        self.occupation = row.get('Occupation')
        self.next_kin_occupation = row.get('OccupationNextKin')
        self.education_level_id = row.get('EducationLevelID')
        self.date_of_death = row.get('DateOfDeath')
        self.date_confirmed_hiv_positive = row.get('DateConfirmedHIVPositive')
        self.file_ref = row.get('FileRef')
        self.notes = row.get('Notes')
        self.timestamp = row.get('TheTimeStamp')