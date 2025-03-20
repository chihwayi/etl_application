class QueryBuilder:
    """Utility class for building SQL queries"""
    
    @staticmethod
    def select_all_from(table):
        """Generate a SELECT * query for a table
        
        Args:
            table (str): Table name
            
        Returns:
            str: SQL query string
        """
        return f"SELECT * FROM {table}"
    
    @staticmethod
    def select_with_limit(table, limit, offset=0):
        """Generate a SELECT query with LIMIT and OFFSET
        
        Args:
            table (str): Table name
            limit (int): Maximum number of rows to return
            offset (int): Number of rows to skip
            
        Returns:
            str: SQL query string
        """
        return f"SELECT * FROM {table} LIMIT {limit} OFFSET {offset}"
    
    @staticmethod
    def select_columns_from(table, columns):
        """Generate a SELECT query for specific columns
        
        Args:
            table (str): Table name
            columns (list): List of column names
            
        Returns:
            str: SQL query string
        """
        columns_str = ", ".join(columns)
        return f"SELECT {columns_str} FROM {table}"
    
    @staticmethod
    def select_with_condition(table, condition):
        """Generate a SELECT query with a WHERE clause
        
        Args:
            table (str): Table name
            condition (str): WHERE condition
            
        Returns:
            str: SQL query string
        """
        return f"SELECT * FROM {table} WHERE {condition}"
    
    @staticmethod
    def join_query(main_table, join_table, join_condition, columns=None):
        """Generate a JOIN query
        
        Args:
            main_table (str): Main table name
            join_table (str): Table to join with
            join_condition (str): Join condition
            columns (list, optional): List of columns to select
            
        Returns:
            str: SQL query string
        """
        if columns:
            columns_str = ", ".join(columns)
        else:
            columns_str = "*"
            
        return f"""
            SELECT {columns_str}
            FROM {main_table}
            JOIN {join_table} ON {join_condition}
        """
    
    @staticmethod
    def insert_query(table, columns):
        """Generate an INSERT query with placeholders
        
        Args:
            table (str): Target table name
            columns (list): List of column names
            
        Returns:
            str: SQL query string
        """
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        return f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
    
    @staticmethod
    def batch_select_query(table, id_column, id_list):
        """Generate a SELECT query for a batch of IDs
        
        Args:
            table (str): Table name
            id_column (str): Name of the ID column
            id_list (list): List of IDs to select
            
        Returns:
            str: SQL query string
        """
        id_str = ", ".join([f"'{id_val}'" for id_val in id_list])
        return f"SELECT * FROM {table} WHERE {id_column} IN ({id_str})"