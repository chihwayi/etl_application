from enum import Enum
import pandas as pd

class MaritalStatus(Enum):
    """Enum for marital status mapping from old to new system"""
    DIVORCED = ('D', '03', 'DIVORCED')
    MARRIED = ('M', '02', 'MARRIED')
    SINGLE = ('S', '01', 'NEVER MARRIED')
    WIDOWED = ('W', '04', 'WIDOWED')
    MINOR = (None, '08', 'MINOR')
    OTHER = (None, '05', 'OTHER')
    SEPARATED = (None, '06', 'SEPARATED')
    COHABITANT = (None, '07', 'CO-HABITANT')
    
    def __init__(self, old_code, new_code, description):
        self.old_code = old_code
        self.new_code = new_code
        self.description = description
    
    @classmethod
    def from_old_code(cls, code):
        """Get MaritalStatus enum from old system code
        
        Args:
            code (str): Old system code
            
        Returns:
            MaritalStatus: Matching enum or SINGLE if not found
        """
        if code is None:
            return cls.SINGLE
            
        for status in cls:
            if status.old_code == code:
                return status
        return cls.SINGLE
    
    @classmethod
    def get_new_code(cls, old_code):
        """Get new system code from old system code
        
        Args:
            old_code (str): Old system code
            
        Returns:
            str: New system code
        """
        return cls.from_old_code(old_code).new_code
    
    @classmethod
    def get_description(cls, old_code):
        """Get description from old system code
        
        Args:
            old_code (str): Old system code
            
        Returns:
            str: Description
        """
        return cls.from_old_code(old_code).description


class EducationLevel(Enum):
    """Enum for education level mapping from old to new system"""
    NONE = ('N', '1', 'None')
    PRIMARY = ('P', '2', 'Primary')
    SECONDARY = ('S', '3', 'Secondary')
    TERTIARY = ('T', '4', 'Tertiary')
    
    def __init__(self, old_code, new_code, description):
        self.old_code = old_code
        self.new_code = new_code
        self.description = description
    
    @classmethod
    def from_old_code(cls, code):
        """Get EducationLevel enum from old system code
        
        Args:
            code (str): Old system code
            
        Returns:
            EducationLevel: Matching enum or NONE if not found
        """
        if code is None:
            return cls.NONE
            
        for level in cls:
            if level.old_code == code:
                return level
        return cls.NONE
    
    @classmethod
    def get_new_code(cls, old_code):
        """Get new system code from old system code
        
        Args:
            old_code (str): Old system code
            
        Returns:
            str: New system code
        """
        return cls.from_old_code(old_code).new_code
    
    @classmethod
    def get_description(cls, old_code):
        """Get description from old system code
        
        Args:
            old_code (str): Old system code
            
        Returns:
            str: Description
        """
        return cls.from_old_code(old_code).description


class Occupation(Enum):
    """Enum for occupation mapping from old to new system"""
    SELF_EMPLOYED = ('01', 'Self Employed')
    STUDENT = ('02', 'Student')
    EMPLOYED = ('03', 'Employed')
    UNEMPLOYED = ('04', 'Unemployed')
    NA = ('05', 'N/A')
    
    def __init__(self, code, description):
        self.code = code
        self.description = description
    
    @classmethod
    def categorize(cls, occupation_text):
        """Categorize occupation text into appropriate enum
        
        Args:
            occupation_text (str): Occupation text from old system
            
        Returns:
            Occupation: Matching enum
        """
        if occupation_text is None or pd.isna(occupation_text) or occupation_text.strip() == '':
            return cls.NA
            
        occupation_text = occupation_text.lower().strip()
        
        if 'student' in occupation_text:
            return cls.STUDENT
        elif 'self' in occupation_text and 'employ' in occupation_text:
            return cls.SELF_EMPLOYED
        elif any(keyword in occupation_text for keyword in ['unemploy', 'none', 'n/a', 'nil']):
            return cls.UNEMPLOYED
        else:
            return cls.EMPLOYED
    
    @classmethod
    def get_code(cls, occupation_text):
        """Get code for occupation text
        
        Args:
            occupation_text (str): Occupation text from old system
            
        Returns:
            str: Occupation code
        """
        return cls.categorize(occupation_text).code
    
    @classmethod
    def get_description(cls, occupation_text):
        """Get description for occupation text
        
        Args:
            occupation_text (str): Occupation text from old system
            
        Returns:
            str: Occupation description
        """
        return cls.categorize(occupation_text).description
    
class RelationshipType(Enum):
    """Mapping of relationship types from source to target"""
    
    PARENT = "PARENT"
    CHILD = "CHILD"
    SPOUSE = "SPOUSE"
    SIBLING = "SIBLING"
    OTHER = "OTHER"
    
    @classmethod
    def map_from_source(cls, source_value):
        """Map a source relationship type to target type
        
        Args:
            source_value (str): Source relationship type
            
        Returns:
            str: Target relationship type
        """
        if not source_value:
            return cls.OTHER.value
            
        source_value = source_value.lower()
        
        if "son" in source_value or "daughter" in source_value:
            return cls.CHILD.value
        elif "wife" in source_value or "husband" in source_value or "spouse" in source_value:
            return cls.SPOUSE.value
        elif "brother" in source_value or "sister" in source_value or "sibling" in source_value:
            return cls.SIBLING.value
        elif "parent" in source_value or "mother" in source_value or "father" in source_value:
            return cls.PARENT.value
        else:
            return cls.OTHER.value