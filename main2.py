import os
import sys
import logging
import argparse
from datetime import datetime


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.db_config import DatabaseConfig
from utils.logging_utils import setup_logging
from etl.manager import ETLManager
from gui.data_validator import DataValidatorGUI
from mde_processor.mde_uploader import MDEUploaderGUI
from core.connection import DatabaseConnection

logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='ETL Application for Healthcare Data Migration')
    
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Logging level')
    
    parser.add_argument('--log-file', type=str, default='logs/etl.log',
                        help='Path to log file')
    
    parser.add_argument('--batch-size', type=int, default=DatabaseConfig.BATCH_SIZE,
                        help='Number of records to process in each batch')
    
    parser.add_argument('--skip-client', action='store_true',
                        help='Skip client database migration')
    
    parser.add_argument('--skip-consultation', action='store_true',
                        help='Skip consultation database migration')
    
    parser.add_argument('--skip-report', action='store_true',
                        help='Skip report database migration')
    
    return parser.parse_args()

def has_missing_data(source_conn):
    """Check if there is any missing data in tblpatients"""
    query = """
                SELECT COUNT(*) as count
                FROM tblpatients
                WHERE FirstName IS NULL OR FirstName = ''
                OR SurName IS NULL OR SurName = ''
                OR Sex IS NULL OR Sex = ''
            """
    try:
        result = source_conn.execute_query(query)
        count = result["count"].iloc[0]
        logger.debug(f"has_missing_data query returned count: {count}")
        return count > 0
    except Exception as e:
        logger.error(f"Error checking missing data: {str(e)}")
        return False  # Default to False on error to skip GUI

def main():
    """Main entry point for the ETL application"""
    args = parse_arguments()
    start_time = datetime.now()
    logger = setup_logging(args.log_level, args.log_file)
    logger.info(f"Starting ETL application at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Setup source connection
        source_conn = DatabaseConnection(DatabaseConfig.SOURCE_CONFIG)

        # Step 1: Check for missing data before launching GUI
        reference_manager = [None]  # Using list to allow modification in callback
        def set_reference_manager(ref_mgr):
            reference_manager[0] = ref_mgr

        if has_missing_data(source_conn):
            logger.info("Patients with missing data found, launching validator GUI")
            validator = DataValidatorGUI(source_conn)
            ref_mgr = validator.run()  # Run returns reference_manager if set
            if ref_mgr:
                reference_manager[0] = ref_mgr
            elif has_missing_data(source_conn):  # Re-check after GUI closes
                logger.info("Data validation incomplete. Please fix all missing fields.")
                return 1
            else:
                logger.info("All patient data validated, proceeding to MDE upload")
        else:
            logger.info("No patients with missing data found, proceeding directly to MDE upload")

        # Step 2: Upload MDE file if not already done
        if not reference_manager[0]:
            logger.debug("Launching MDEUploaderGUI")
            mde_uploader = MDEUploaderGUI(set_reference_manager)
            mde_uploader.run()

        if not reference_manager[0]:
            logger.error("No MDE file uploaded. Aborting.")
            return 1

        # Create ETL manager with reference data
        etl_manager = ETLManager(batch_size=args.batch_size)
        etl_manager.reference_manager = reference_manager[0]

        # Run selected migrations
        etl_manager.run_migrations(
            run_client=not args.skip_client,
            run_consultation=not args.skip_consultation,
            run_report=not args.skip_report
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"ETL application completed in {duration:.2f} seconds")
        logger.info(f"Migration statistics: {etl_manager.get_stats()}")
        return 0

    except Exception as e:
        logger.error(f"Error in ETL application: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())