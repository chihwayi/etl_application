import pandas as pd
import uuid
import re
from utils.enums import MaritalStatus, EducationLevel, Occupation
from utils.helpers import format_national_id, find_closest_town, parse_art_cohort_number

class CommonTransformer:
    """Common transformation methods used across multiple transformers"""
    
    @staticmethod
    def generate_uuid():
        """Generate a UUID string
        
        Returns:
            str: UUID string
        """
        return str(uuid.uuid4())
    
    @staticmethod
    def clean_date(date_value):
        """Clean a date by removing time component
        
        Args:
            date_value: Date string or datetime object
            
        Returns:
            str: Cleaned date string in YYYY-MM-DD format or None
        """
        if pd.isna(date_value) or date_value is None:
            return None
            
        try:
            if isinstance(date_value, str):
                date_obj = pd.to_datetime(date_value)
            else:
                date_obj = pd.to_datetime(date_value)
            return date_obj.strftime('%Y-%m-%d')
        except Exception:
            return None
    
    @staticmethod
    def map_marital_status(old_code):
        """Map marital status from old to new system
        
        Args:
            old_code (str): Marital status code from old system
            
        Returns:
            tuple: (marital_id, marital_description)
        """
        return (
            MaritalStatus.get_new_code(old_code),
            MaritalStatus.get_description(old_code)
        )
    
    @staticmethod
    def map_education_level(old_code):
        """Map education level from old to new system
        
        Args:
            old_code (str): Education level code from old system
            
        Returns:
            tuple: (education_id, education_description)
        """
        return (
            EducationLevel.get_new_code(old_code),
            EducationLevel.get_description(old_code)
        )
    
    @staticmethod
    def map_occupation(occupation_text):
        """Map occupation from old to new system
        
        Args:
            occupation_text (str): Occupation text from old system
            
        Returns:
            tuple: (occupation_id, occupation_description)
        """
        return (
            Occupation.get_code(occupation_text),
            Occupation.get_description(occupation_text)
        )
    
    @staticmethod
    def find_matching_town(address, town_df):
        """Find matching town for an address
        
        Args:
            address (str): Address from old system
            town_df (pd.DataFrame): DataFrame containing town data
            
        Returns:
            tuple: (town_id, town_name)
        """
        return find_closest_town(address, town_df)
    
    @staticmethod
    def map_id_numbers(national_id):
        """Map national id from old to new system
        
        Args:
            national_id (str): national id from old system
            
        Returns:
            str: national id
        """
        return format_national_id(national_id)
    
    @staticmethod
    def map_cohort_number(file_ref):
        """Map file ref from old to new system
        
        Args:
            cohort number (str): file ref from old system
            
        Returns:
            str: cohort number
        """
        return parse_art_cohort_number(file_ref)