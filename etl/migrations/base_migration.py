import logging
from abc import ABC, abstractmethod


class BaseMigration(ABC):
    """Base class for all migrations"""
    
    def __init__(self, connections, batch_size):
        """Initialize base migration
        
        Args:
            connections (dict): Dictionary of database connections
            batch_size (int): Batch size for processing
        """
        self.logger = logging.getLogger(__name__)
        self.connections = connections
        self.batch_size = batch_size
        
    @abstractmethod
    def run(self):
        """Run the migration
        
        Returns:
            dict: Migration statistics
        """
        pass
        
    def _process_in_batches(self, data, process_batch_fn):
        """Process data in batches
        
        Args:
            data (DataFrame): Data to process
            process_batch_fn (function): Function to process each batch
            
        Returns:
            dict: Aggregated statistics
        """
        stats = {}
        
        for i in range(0, len(data), self.batch_size):
            batch_num = i // self.batch_size + 1
            self.logger.info(f"Processing batch {batch_num}")
            
            # Get batch of data
            batch_end = min(i + self.batch_size, len(data))
            batch_data = data.iloc[i:batch_end].copy()
            
            # Process batch
            batch_stats = process_batch_fn(batch_data, batch_num)
            
            # Aggregate stats
            for key, value in batch_stats.items():
                if key in stats:
                    stats[key] += value
                else:
                    stats[key] = value
                    
            self.logger.info(f"Batch {batch_num} complete")
            
        return stats