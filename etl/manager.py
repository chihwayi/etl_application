import logging
from collections import defaultdict

from config.db_config import DatabaseConfig
from core.connection import DatabaseConnection
from etl.migrations.client_migration import ClientMigration
from etl.migrations.consultation_migration import ConsultationMigration
from etl.migrations.report_migration import ReportMigration


class ETLManager:
    """Manages the ETL process across different database migrations"""
    
    def __init__(self, batch_size=DatabaseConfig.BATCH_SIZE):
        """Initialize the ETL manager
        
        Args:
            batch_size (int): Batch size for processing records
        """
        self.logger = logging.getLogger(__name__)
        self.batch_size = batch_size
        self.stats = defaultdict(dict)
        self.connections = self._setup_connections()
        self.patient_person_mapping = {}
        
    def _setup_connections(self):
        """Set up database connections
        
        Returns:
            dict: Dictionary of database connections
        """
        self.logger.info("Setting up database connections")
        connections = {
            'source': DatabaseConnection(DatabaseConfig.SOURCE_CONFIG),
            'mrs': DatabaseConnection(DatabaseConfig.MRS_CONFIG),
            'facility': DatabaseConnection(DatabaseConfig.FACILITY_CONFIG),
            'client': DatabaseConnection(DatabaseConfig.CLIENT_CONFIG),
            'consultation': DatabaseConnection(DatabaseConfig.CONSULTATION_CONFIG),
            'report': DatabaseConnection(DatabaseConfig.REPORT_CONFIG)
        }
        
        return connections
    
    def run_migrations(self, run_client=True, run_consultation=True, run_report=True):
        """Run the selected migrations
        
        Args:
            run_client (bool): Whether to run client migration
            run_consultation (bool): Whether to run consultation migration
            run_report (bool): Whether to run report migration
        """
        self.logger.info("Starting migrations")
        
        # Run client migration if selected
        if run_client:
            self._run_client_migration()
        else:
            self.logger.info("Skipping client database migration")
            self._load_existing_mapping()
            
        # Run consultation migration if selected
        if run_consultation:
            self._run_consultation_migration()
        else:
            self.logger.info("Skipping consultation database migration")
            
        # Run report migration if selected
        if run_report:
            self._run_report_migration()
        else:
            self.logger.info("Skipping report database migration")
            
        self.logger.info("All migrations completed")
        
    def _run_client_migration(self):
        """Run client database migration"""
        self.logger.info("Starting client migration")
        
        client_migration = ClientMigration(
            connections=self.connections,
            batch_size=self.batch_size
        )
        
        # Run migration and store results
        self.stats['client'], self.patient_person_mapping = client_migration.run()
        
        self.logger.info("Client migration completed")
        
    def _load_existing_mapping(self):
        """Load existing patient-person mapping from database"""
        self.logger.info("Fetching existing patient-person mapping from client database")
        
        try:
            query = """
            SELECT original_patient_id, person_id 
            FROM client.person_patient_mapping
            """
            mapping_df = self.connections['client'].execute_query(query)
            self.patient_person_mapping = dict(zip(mapping_df['original_patient_id'], mapping_df['person_id']))
            self.logger.info(f"Loaded {len(self.patient_person_mapping)} existing mappings")
        except Exception as e:
            self.logger.warning(f"Could not fetch existing mapping: {str(e)}")
            
    def _run_consultation_migration(self):
        """Run consultation database migration"""
        self.logger.info("Starting consultation migration")
        
        if not self.patient_person_mapping:
            self.logger.warning("No patient-person mapping available. Consultation migration may fail.")
        
        consultation_migration = ConsultationMigration(
            connections=self.connections,
            batch_size=self.batch_size,
            patient_person_mapping=self.patient_person_mapping
        )
        
        # Run migration and store results
        self.stats['consultation'] = consultation_migration.run()
        
        self.logger.info("Consultation migration completed")
        
    def _run_report_migration(self):
        """Run report database migration"""
        self.logger.info("Starting report migration")
        
        report_migration = ReportMigration(
            connections=self.connections,
            batch_size=self.batch_size,
            patient_person_mapping=self.patient_person_mapping
        )
        
        # Run migration and store results
        self.stats['report'] = report_migration.run()
        
        self.logger.info("Report migration completed")
        
    def get_stats(self):
        """Get migration statistics
        
        Returns:
            dict: Migration statistics for each database
        """
        return dict(self.stats)