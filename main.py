import os
import sys
import logging
import argparse
import pandas as pd
from datetime import datetime

# Add the project directory to the path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.db_config import DatabaseConfig
from core.connection import DatabaseConnection
from utils.logging_utils import setup_logging
from utils.helpers import get_site_id

# Import ETL components
from extractors.patient_extractor import PatientExtractor
from transformers.person_transformer import PersonTransformer
from transformers.id_transformer import IdentificationTransformer
from transformers.phone_transformer import PhoneTransformer
from transformers.art_transformer import ArtTransformer
from loaders.person_loader import PersonLoader
from loaders.id_loader import IdentificationLoader
from loaders.phone_loader import PhoneLoader
from loaders.art_loader import ArtLoader


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
    
    return parser.parse_args()


def setup_connections():
    """Set up database connections
    
    Returns:
        dict: Dictionary of database connections
    """
    connections = {
        'source': DatabaseConnection(DatabaseConfig.SOURCE_CONFIG),
        'mrs': DatabaseConnection(DatabaseConfig.MRS_CONFIG),
        'facility': DatabaseConnection(DatabaseConfig.FACILITY_CONFIG),
        'client': DatabaseConnection(DatabaseConfig.CLIENT_CONFIG),
        'consultation': DatabaseConnection(DatabaseConfig.CONSULTATION_CONFIG),
        'report': DatabaseConnection(DatabaseConfig.REPORT_CONFIG)
    }
    
    return connections


def migrate_client_data(connections, batch_size):
    """Migrate data to client database (person, identification, phone)
    
    Args:
        connections (dict): Dictionary of database connections
        batch_size (int): Batch size for processing
        
    Returns:
        tuple: (Migration statistics, Patient-person mapping)
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting client data migration")
    
    stats = {
        'person_extracted': 0,
        'person_loaded': 0,
        'identification_loaded': 0,
        'phone_loaded': 0
    }
    
    # Create a dictionary to store the mapping between original patient ID and person ID
    patient_person_mapping = {}
    
    try:
        # Get facility town data for town matching
        logger.info("Fetching town data from facility database")
        town_query = "SELECT town_id, name FROM town"
        town_df = connections['facility'].execute_query(town_query)
        
        # Extract patient data
        logger.info("Extracting patient data from source database")
        patient_extractor = PatientExtractor(connections['source'])
        patient_data = patient_extractor.extract()
        stats['person_extracted'] = len(patient_data)
        
        if patient_data.empty:
            logger.warning("No patient data extracted, skipping client migration")
            return stats, patient_person_mapping
        
        # First, check if records already exist and get their mappings
        logger.info("Checking for existing person records and mappings")
        try:
            # Create a temporary table for existing patient mappings if it doesn't exist
            create_mapping_table_query = """
            CREATE TABLE IF NOT EXISTS person_patient_mapping (
                person_id VARCHAR(36) NOT NULL,
                original_patient_id VARCHAR(50) NOT NULL,
                PRIMARY KEY (original_patient_id),
                KEY (person_id)
            )
            """
            connections['client'].execute_update(create_mapping_table_query)
            
            # Check if we have existing mappings
            existing_patients_query = """
            SELECT person_id, original_patient_id FROM person_patient_mapping
            """
            existing_mapping_df = connections['client'].execute_query(existing_patients_query)
            
            if not existing_mapping_df.empty:
                # Load existing mappings
                for _, row in existing_mapping_df.iterrows():
                    patient_person_mapping[row['original_patient_id']] = row['person_id']
                logger.info(f"Loaded {len(patient_person_mapping)} existing mappings")
        except Exception as e:
            logger.warning(f"Could not load existing mappings: {str(e)}")
            
        # Process in batches
        for i in range(0, len(patient_data), batch_size):
            batch_num = i // batch_size + 1
            logger.info(f"Processing batch {batch_num}")
            
            # Get batch of data
            batch_end = min(i + batch_size, len(patient_data))
            batch_data = patient_data.iloc[i:batch_end].copy()
            
            # Transform person data
            person_transformer = PersonTransformer(town_df)
            person_data = person_transformer.transform(batch_data)
            
            # Update the mapping before loading
            new_mappings = []
            for _, row in person_data.iterrows():
                patient_id = row['original_patient_id']
                person_id = row['person_id']
                patient_person_mapping[patient_id] = person_id
                new_mappings.append({
                    'person_id': person_id,
                    'original_patient_id': patient_id
                })
            
            # Save mappings to database
            if new_mappings:
                mapping_df = pd.DataFrame(new_mappings)
                try:
                    # Use REPLACE to handle updates of existing mappings
                    with connections['client'].get_connection() as conn:
                        mapping_df.to_sql(
                            name='person_patient_mapping',
                            con=conn,
                            if_exists='append',
                            index=False,
                            chunksize=1000,
                            method='multi'  # Important for handling conflicts
                        )
                    logger.info(f"Saved {len(new_mappings)} mappings to database")
                except Exception as e:
                    logger.warning(f"Could not save mappings: {str(e)}")
            
            # Load person data - this will strip out original_patient_id
            person_loader = PersonLoader(connections['client'])
            loaded_person_count = person_loader.load(person_data)
            stats['person_loaded'] += loaded_person_count
            
            # Transform and load identification data
            id_transformer = IdentificationTransformer()
            id_data = id_transformer.transform(batch_data, person_data)
            
            if not id_data.empty:
                id_loader = IdentificationLoader(connections['client'])
                loaded_id_count = id_loader.load(id_data)
                stats['identification_loaded'] += loaded_id_count
            
            # Transform and load phone data
            phone_transformer = PhoneTransformer()
            phone_data = phone_transformer.transform(batch_data, person_data)
            
            if not phone_data.empty:
                phone_loader = PhoneLoader(connections['client'])
                loaded_phone_count = phone_loader.load(phone_data)
                stats['phone_loaded'] += loaded_phone_count
                
            logger.info(f"Batch {batch_num} complete")
            
        logger.info("Client data migration complete")
        return stats, patient_person_mapping
        
    except Exception as e:
        logger.error(f"Error during client data migration: {str(e)}")
        raise

def migrate_consultation_data(connections, batch_size, patient_person_mapping):
    """Migrate data to consultation database (art)
    
    Args:
        connections (dict): Dictionary of database connections
        batch_size (int): Batch size for processing
        patient_person_mapping (dict): Mapping of original patient IDs to person IDs
        
    Returns:
        dict: Migration statistics
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting consultation data migration")
    
    stats = {
        'patient_extracted': 0,
        'art_loaded': 0
    }
    
    try:
        # Get site ID from MRS database
        logger.info("Fetching site ID from MRS database")
        site_id = get_site_id(connections['mrs'])
        logger.info(f"Site ID: {site_id}")
        
        # Extract patient data
        logger.info("Extracting patient data from source database")
        patient_extractor = PatientExtractor(connections['source'])
        patient_data = patient_extractor.extract()
        stats['patient_extracted'] = len(patient_data)
        
        if patient_data.empty:
            logger.warning("No patient data extracted, skipping consultation migration")
            return stats
        
        # Create a DataFrame from the patient_person_mapping dictionary
        logger.info("Creating person reference data from mapping")
        person_data = pd.DataFrame([
            {'original_patient_id': k, 'person_id': v} 
            for k, v in patient_person_mapping.items()
        ])
        
        # Process in batches
        for i in range(0, len(patient_data), batch_size):
            batch_num = i // batch_size + 1
            logger.info(f"Processing batch {batch_num}")
            
            # Get batch of data
            batch_end = min(i + batch_size, len(patient_data))
            batch_data = patient_data.iloc[i:batch_end].copy()
            
            # Transform and load ART data
            art_transformer = ArtTransformer(person_data)
            art_data = art_transformer.transform(batch_data)
            
            if not art_data.empty:
                art_loader = ArtLoader(connections['consultation'])
                loaded_art_count = art_loader.load(art_data)
                stats['art_loaded'] += loaded_art_count
                
            logger.info(f"Batch {batch_num} complete")
            
        logger.info("Consultation data migration complete")
        return stats
        
    except Exception as e:
        logger.error(f"Error during consultation data migration: {str(e)}")
        raise

def main():
    """Main entry point for the ETL application"""
    # Parse command line arguments
    args = parse_arguments()
    
    # Setup logging
    start_time = datetime.now()
    logger = setup_logging(args.log_level, args.log_file)
    logger.info(f"Starting ETL application at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Setup database connections
        logger.info("Setting up database connections")
        connections = setup_connections()
        
        # Migration statistics
        stats = {
            'client': {},
            'consultation': {}
        }
        
        # Patient to person mapping
        patient_person_mapping = {}
        
        # Migrate client data (person, identification, phone)
        if not args.skip_client:
            # Note the unpacking of the tuple here
            stats['client'], patient_person_mapping = migrate_client_data(connections, args.batch_size)
        else:
            logger.info("Skipping client database migration")
            # If skipping client migration, we need to get the mapping from existing data
            if not args.skip_consultation:
                logger.info("Fetching existing patient-person mapping from client database")
                # Query to get the mapping from person table if it exists
                try:
                    query = """
                    SELECT original_patient_id, person_id 
                    FROM client.person_patient_mapping
                    """
                    mapping_df = connections['client'].execute_query(query)
                    patient_person_mapping = dict(zip(mapping_df['original_patient_id'], mapping_df['person_id']))
                except Exception as e:
                    logger.warning(f"Could not fetch existing mapping: {str(e)}")
        
        # Migrate consultation data (art)
        if not args.skip_consultation:
            if not patient_person_mapping:
                logger.warning("No patient-person mapping available. Consultation migration may fail.")
            stats['consultation'] = migrate_consultation_data(connections, args.batch_size, patient_person_mapping)
        else:
            logger.info("Skipping consultation database migration")
        
        # Log statistics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"ETL application completed in {duration:.2f} seconds")
        logger.info(f"Migration statistics: {stats}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error in ETL application: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())