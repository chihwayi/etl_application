import logging
import pandas as pd

class RelationshipLoader:
    """Loads relationship data into the client database"""
    
    def __init__(self, client_connection):
        """Initialize relationship loader
        
        Args:
            client_connection: Database connection to client database
        """
        self.logger = logging.getLogger(__name__)
        self.client_connection = client_connection
        
    def load(self, relationship_df):
        """Load relationship data into client.relationship table
        
        Args:
            relationship_df (DataFrame): Relationship data to load
            
        Returns:
            int: Number of records loaded
        """
        if relationship_df.empty:
            self.logger.warning("No relationship data to load")
            return 0
            
        try:
            # Check for existing relationships to avoid duplicates
            filtered_df = self._filter_existing_records(relationship_df)
            
            if filtered_df.empty:
                self.logger.info("All records already exist in the database")
                return 0
            
            # Use bulk_insert_dataframe directly like in PersonLoader
            rows_loaded = self.client_connection.bulk_insert_dataframe(
                filtered_df, 
                'relationship',
                if_exists='append'
            )
            
            self.logger.info(f"Loaded {rows_loaded} relationship records")
            return rows_loaded
            
        except Exception as e:
            self.logger.error(f"Error loading relationship data: {str(e)}")
            raise
    
    def _filter_existing_records(self, relationship_df):
        """Filter out records that already exist in the database
        
        Args:
            relationship_df (DataFrame): Relationship data to filter
            
        Returns:
            DataFrame: Filtered relationship data
        """
        try:
            # Create a query to check for existing person_id and member_id combinations
            pairs = []
            for _, row in relationship_df.iterrows():
                pairs.append(f"('{row['person_id']}', '{row['member_id']}')")
            
            if not pairs:
                return relationship_df
            
            # Build the query with the pairs
            pairs_str = ", ".join(pairs)
            query = f"""
            SELECT person_id, member_id 
            FROM relationship 
            WHERE (person_id, member_id) IN ({pairs_str})
            """
            
            existing_df = self.client_connection.execute_query(query)
            
            if existing_df.empty:
                return relationship_df
                
            # Create a set of existing (person_id, member_id) tuples for faster lookup
            existing_pairs = set(zip(existing_df['person_id'], existing_df['member_id']))
            
            # Filter out existing records
            mask = ~relationship_df.apply(
                lambda row: (row['person_id'], row['member_id']) in existing_pairs, 
                axis=1
            )
            
            filtered_df = relationship_df[mask]
            self.logger.info(f"Filtered out {len(relationship_df) - len(filtered_df)} existing records")
            
            return filtered_df
            
        except Exception as e:
            self.logger.warning(f"Could not check for existing records: {str(e)}")
            # If checking fails, return original dataframe
            return relationship_df