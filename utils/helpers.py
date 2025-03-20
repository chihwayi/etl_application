import re
import pandas as pd
import difflib
import logging

def format_national_id(national_id):
    """Format a national ID by removing hyphens
    
    Args:
        national_id (str): National ID from old system
        
    Returns:
        str: Formatted national ID
    """
    if national_id is None or pd.isna(national_id):
        return None
        
    # Remove hyphens and whitespace
    return re.sub(r'[-\s]', '', national_id)


def parse_art_cohort_number(file_ref):
    """Parse and format ART cohort number from FileRef
    
    Args:
        file_ref (str): FileRef from old system
        
    Returns:
        str: Formatted ART cohort number (MMM-YYYY)
    """
    if file_ref is None or pd.isna(file_ref):
        return None
        
    file_ref = str(file_ref).strip()
    
    # Define regex patterns for different formats
    patterns = [
        # Format: 3/2/11, 27/8/11, 14/5/11
        r'(\d+)/(\d+)/(\d+)',
        # Format: Nov2005-3, Oct2011-06
        r'([A-Za-z]+)(\d{4})-\d+',
        # Format: July2011-19
        r'([A-Za-z]+)(\d{4})-\d+',
        # Format: 17-Oct2011
        r'\d+-([A-Za-z]+)(\d{4})',
        # Format: 8 Jun2011
        r'\d+\s+([A-Za-z]+)(\d{4})'
    ]
    
    # Map month names to their three-letter abbreviations
    month_map = {
        'jan': 'JAN', 'feb': 'FEB', 'mar': 'MAR', 'apr': 'APR',
        'may': 'MAY', 'jun': 'JUN', 'jul': 'JUL', 'aug': 'AUG',
        'sep': 'SEP', 'oct': 'OCT', 'nov': 'NOV', 'dec': 'DEC'
    }
    
    # Try to match using the defined patterns
    for pattern in patterns:
        match = re.search(pattern, file_ref)
        if match:
            groups = match.groups()
            
            # For DD/MM/YY format
            if len(groups) == 3 and all(g.isdigit() for g in groups):
                month = int(groups[1])
                year = int(groups[2])
                
                # Convert month number to abbreviation
                month_abbr = list(month_map.values())[month - 1]
                
                # Adjust year (assuming 20YY for all years)
                if year < 100:
                    year = 2000 + year
                
                return f"{month_abbr}-{year}"
                
            # For month name and year formats
            elif len(groups) == 2 and groups[1].isdigit():
                month_name = groups[0].lower()
                year = groups[1]
                
                # Find closest month name
                closest_month = difflib.get_close_matches(month_name, month_map.keys(), n=1)
                if closest_month:
                    month_abbr = month_map[closest_month[0]]
                    return f"{month_abbr}-{year}"
    
    # If no pattern matches, try to extract month and year using simpler approach
    month_pattern = '|'.join(month_map.keys())
    month_match = re.search(f'({month_pattern})', file_ref.lower())
    year_match = re.search(r'(19\d{2}|20\d{2}|\d{2})', file_ref)
    
    if month_match and year_match:
        month_abbr = month_map[month_match.group(1)]
        year = year_match.group(1)
        
        # Adjust year (assuming 20YY for all years)
        if len(year) == 2:
            year = f"20{year}"
            
        return f"{month_abbr}-{year}"
    
    # If all else fails, return original value
    return file_ref


def find_closest_town(address, town_df, threshold=0.9):
    """Find the closest matching town from the town table
    
    Args:
        address (str): Physical address from old system
        town_df (pd.DataFrame): DataFrame containing town data
        threshold (float): Similarity threshold (0-1)
        
    Returns:
        tuple: (town_id, town_name) or (None, None) if no match found
    """
    if address is None or pd.isna(address) or town_df.empty:
        return None, None
        
    address = str(address).strip().lower()
    
    # Extract words from address
    words = re.findall(r'\b\w+\b', address)
    
    # Try to find a match for each word
    for word in words:
        if len(word) < 3:  # Skip very short words
            continue
            
        # Compare with town names
        matches = []
        for _, row in town_df.iterrows():
            town_name = str(row['name']).lower()
            similarity = difflib.SequenceMatcher(None, word, town_name).ratio()
            if similarity >= threshold:
                matches.append((similarity, row['town_id'], row['name']))
        
        # If matches found, return the best one
        if matches:
            matches.sort(reverse=True)  # Sort by similarity
            return matches[0][1], matches[0][2]
    
    # No match found
    return None, None


def get_site_id(mrs_conn):
    """Get site ID from MRS database
    
    Args:
        mrs_conn: MRS database connection
        
    Returns:
        str: Site ID or None if not found
    """
    try:
        # Execute the query to get the latest metadata
        query = """
        SELECT meta_data FROM domain_event_entry 
        WHERE time_stamp = (SELECT MAX(time_stamp) FROM domain_event_entry)
        """
        metadata_df = mrs_conn.execute_query(query)
        
        if metadata_df.empty:
            return None
            
        # Extract the site ID from the metadata
        meta_data = metadata_df['meta_data'].iloc[0]
        
        # If meta_data is bytes, decode it
        if isinstance(meta_data, bytes):
            meta_data = meta_data.decode('utf-8')
            
        # Extract site ID using regex
        match = re.search(r'<string>siteId</string><string>(.*?)</string>', meta_data)
        if match:
            return match.group(1)
            
        return None
        
    except Exception as e:
        logging.error(f"Error getting site ID: {str(e)}")
        return None