import logging
from .mde_reader import MDEReader

logger = logging.getLogger(__name__)

class ReferenceDataManager:
    """Manages reference data from MDE files"""
    
    TABLES = [
        "tblSetupARVFixedDoseCombinations", "tblSetupARVReasonCodes", "tblSetupARVStatusCodes",
        "tblSetupDeliveryModes", "tblSetupDrugTypes", "tblSetupFamilyPlanning",
        "tblSetupFunctionalStatus", "tblSetupHIVTestReasons", "tblSetupHIVTestTypes",
        "tblSetupInfantFeedingPractices", "tblSetupIPTReasonCodes", "tblSetupMaritalStatus",
        "tblSetupOrphanStatus", "tblSetupPregnancyOutcomes", "tblSetupProblems",
        "tblSetupReferrals", "tblSetupReferralsTo", "tblSetupStatus", "tblSetupTestResults",
        "tblSetupTBStatus", "tblSetupTestTypes", "tblSetupTransferIn", "tblSetupVisitTypes"
    ]

    def __init__(self, mde_file_path):
        """Initialize with MDE file path
        
        Args:
            mde_file_path (str): Path to the MDE file
        """
        self.mde_reader = MDEReader(mde_file_path)
        self.reference_data = {}

    def load_all_tables(self):
        """Load all reference tables"""
        for table in self.TABLES:
            try:
                self.reference_data[table] = self.mde_reader.read_table(table)
            except Exception as e:
                logger.warning(f"Could not load {table}: {str(e)}")
        self.mde_reader.close()

    def get_reference_value(self, table_name, code_column, code_value, description_column="Description"):
        """Get description for a code from a reference table
        
        Args:
            table_name (str): Name of the reference table
            code_column (str): Column containing the code
            code_value: Value to look up
            description_column (str): Column containing the description
            
        Returns:
            str: Description value or None if not found
        """
        if table_name not in self.reference_data:
            return None
        df = self.reference_data[table_name]
        result = df[df[code_column] == code_value][description_column].iloc[0] if not df[df[code_column] == code_value].empty else None
        return result