import mysql.connector
from mysql.connector import Error
import pandas as pd
import logging
from pathlib import Path
import os
from typing import Optional, Dict, List
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HousingDataLoader:
    def __init__(self, host: str = 'localhost', user: str = 'root', 
                 password: str = 'harish@21', database: str = 'luxury_housing'):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.column_mapping = {}
        logger.info(f"Data Loader initialized for database: {database}")
    
    def connect(self) -> bool:
        """Establish connection to MySQL database"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            
            if self.connection.is_connected():
                logger.info(f"Successfully connected to MySQL database: {self.database}")
                return True
            else:
                logger.error("Failed to connect to MySQL database")
                return False
                
        except Error as e:
            logger.error(f"Error connecting to MySQL database: {e}")
            self.connection = None
            return False
    
    def analyze_data_structure(self, df: pd.DataFrame) -> Dict:
        """Analyze the structure of the DataFrame and map columns"""
        logger.info("Analyzing data structure...")
        available_columns = set(df.columns)
        
        # Map common column name variations to standard names
        column_mapping = {
            'price': ['Ticket_Price_Cr', 'Price', 'price', 'ticket_price', 'Price_Cr', 'price_cr'],
            'amenity': ['Amenity_Score', 'Amenity', 'amenity_score', 'AmenityScore', 'amenity'],
            'configuration': ['Configuration', 'configuration', 'config', 'BHK', 'bhk', 'unit_type'],
            'micro_market': ['Micro_Market', 'micro_market', 'Location', 'Area', 'Market', 'location', 'area'],
            'builder': ['Builder', 'builder', 'Developer', 'developer', 'builder_name'],
            'booking_status': ['Booking_Status', 'booking_status', 'Status', 'status', 'Booking', 'booking'],
            'purchase_quarter': ['Purchase_Quarter', 'purchase_quarter', 'Quarter', 'quarter', 'Time_Period', 'period'],
            'project_id': ['Project_ID', 'project_id', 'ID', 'id', 'Property_ID']
        }
        
        # Create mapping from standard names to actual column names
        self.column_map = {}
        missing_columns = []
        
        for standard_name, possible_names in column_mapping.items():
            found = False
            for possible_name in possible_names:
                if possible_name in available_columns:
                    self.column_map[standard_name] = possible_name
                    logger.info(f"Mapped '{standard_name}' to column '{possible_name}'")
                    found = True
                    break
            
            if not found:
                missing_columns.append(standard_name)
                logger.warning(f"No column found for '{standard_name}'")
        
        # Log unmapped columns
        mapped_columns = set(self.column_map.values())
        unmapped_columns = available_columns - mapped_columns
        if unmapped_columns:
            logger.info(f"Unmapped columns: {list(unmapped_columns)}")
        
        return {
            'available_columns': list(available_columns),
            'mapped_columns': self.column_map,
            'missing_columns': missing_columns,
            'unmapped_columns': list(unmapped_columns)
        }
    
    def get_column_value(self, row: pd.Series, column_type: str, default_value=None):
        """Safely get column value with fallback"""
        if column_type in self.column_map:
            column_name = self.column_map[column_type]
            if column_name in row:
                value = row[column_name]
                # Handle NaN and None values
                if pd.isna(value):
                    return default_value
                return value
        return default_value
    
    def create_tables(self) -> bool:
        """Create necessary tables in the database"""
        if not self.connect():
            logger.error("Cannot create tables - no database connection")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Main housing data table with flexible structure
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS housing_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    project_id VARCHAR(100),
                    ticket_price_cr DECIMAL(10, 2),
                    amenity_score DECIMAL(3, 1),
                    configuration VARCHAR(50),
                    micro_market VARCHAR(100),
                    builder VARCHAR(100),
                    booking_status VARCHAR(50),
                    purchase_quarter VARCHAR(50),
                    carpet_area_sqft INT,
                    price_per_sqft DECIMAL(10, 2),
                    quarter_number INT,
                    year INT,
                    booking_flag TINYINT,
                    price_category VARCHAR(20),
                    season VARCHAR(20),
                    additional_data JSON,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            ''')
            logger.info("Table 'housing_data' created successfully")
            
            # Create indexes
            self.create_indexes(cursor)
            
            self.connection.commit()
            cursor.close()
            return True
            
        except Error as e:
            logger.error(f"Error creating tables: {e}")
            return False
    
    def create_indexes(self, cursor):
        """Create indexes for better query performance"""
        indexes = [
            "CREATE INDEX idx_micro_market ON housing_data(micro_market)",
            "CREATE INDEX idx_builder ON housing_data(builder)",
            "CREATE INDEX idx_configuration ON housing_data(configuration)",
            "CREATE INDEX idx_price_category ON housing_data(price_category)",
            "CREATE INDEX idx_booking_flag ON housing_data(booking_flag)",
            "CREATE INDEX idx_year_quarter ON housing_data(year, quarter_number)",
            "CREATE INDEX idx_ticket_price ON housing_data(ticket_price_cr)"
        ]
        
        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
                logger.info(f"Index created: {index_sql.split('ON ')[1]}")
            except Error as e:
                if "Duplicate key name" in str(e):
                    logger.info(f"Index already exists: {index_sql.split('ON ')[1]}")
                else:
                    logger.warning(f"Could not create index {index_sql.split('ON ')[1]}: {e}")
    
    def load_cleaned_data(self, csv_path: str, clear_existing: bool = False) -> bool:
        """Load cleaned data from CSV to database"""
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            return False
        
        try:
            # Load CSV data
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded CSV data with {len(df)} rows and {len(df.columns)} columns")
            
            # Analyze data structure
            structure_info = self.analyze_data_structure(df)
            print("\nData Structure Analysis:")
            print(f"Available columns: {structure_info['available_columns']}")
            print(f"Mapped columns: {structure_info['mapped_columns']}")
            if structure_info['missing_columns']:
                print(f"Missing expected columns: {structure_info['missing_columns']}")
            if structure_info['unmapped_columns']:
                print(f"Unmapped columns: {structure_info['unmapped_columns']}")
            
            # Ensure tables exist
            if not self.create_tables():
                return False
            
            # Clear existing data if requested
            if clear_existing:
                cursor = self.connection.cursor()
                cursor.execute("DELETE FROM housing_data")
                logger.info("Cleared existing data from housing_data table")
                self.connection.commit()
                cursor.close()
            
            # Insert data
            return self.insert_data(df)
            
        except Exception as e:
            logger.error(f"Error loading cleaned data: {e}")
            return False
    
    def insert_data(self, df: pd.DataFrame) -> bool:
        """Insert data into database"""
        try:
            cursor = self.connection.cursor()
            
            inserted_count = 0
            for index, row in df.iterrows():
                # Safely get values with fallbacks
                project_id = self.get_column_value(row, 'project_id', f"PROJ_{index:04d}")
                ticket_price = self.get_column_value(row, 'price', 0.0)
                amenity_score = self.get_column_value(row, 'amenity', 0.0)
                configuration = self.get_column_value(row, 'configuration', 'UNKNOWN')
                micro_market = self.get_column_value(row, 'micro_market', 'UNKNOWN')
                builder = self.get_column_value(row, 'builder', 'UNKNOWN')
                booking_status = self.get_column_value(row, 'booking_status', 'Not Booked')
                purchase_quarter = self.get_column_value(row, 'purchase_quarter', 'Q1_2023')
                
                # Calculate derived fields
                carpet_area = self.calculate_carpet_area(configuration)
                price_per_sqft = self.calculate_price_per_sqft(ticket_price, carpet_area)
                quarter_number, year = self.extract_quarter_year(purchase_quarter)
                booking_flag = self.calculate_booking_flag(booking_status)
                price_category = self.categorize_price(ticket_price)
                season = self.determine_season(quarter_number)
                
                # Prepare additional data JSON
                additional_data = self.prepare_additional_data(row)
                
                sql = """
                INSERT INTO housing_data (
                    project_id, ticket_price_cr, amenity_score, configuration, 
                    micro_market, builder, booking_status, purchase_quarter,
                    carpet_area_sqft, price_per_sqft, quarter_number, year,
                    booking_flag, price_category, season, additional_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    project_id, float(ticket_price), float(amenity_score), 
                    str(configuration), str(micro_market), str(builder), 
                    str(booking_status), str(purchase_quarter),
                    int(carpet_area), float(price_per_sqft), int(quarter_number), 
                    int(year), int(booking_flag), str(price_category), str(season),
                    additional_data
                )
                
                cursor.execute(sql, values)
                inserted_count += 1
                
                # Commit in batches to avoid large transactions
                if inserted_count % 100 == 0:
                    self.connection.commit()
                    logger.info(f"Inserted {inserted_count} records...")
            
            # Final commit
            self.connection.commit()
            cursor.close()
            
            logger.info(f"Successfully inserted {inserted_count} records into housing_data")
            return True
            
        except Error as e:
            logger.error(f"Error inserting data: {e}")
            return False
    
    def calculate_carpet_area(self, configuration: str) -> int:
        """Calculate carpet area based on configuration"""
        area_map = {
            '1BHK': 650, '2BHK': 950, '3BHK': 1250, '4BHK': 1600, 
            '5BHK': 2100, 'STUDIO': 400, '1RK': 350, '2BHK+STUDY': 1100,
            '3BHK+STUDY': 1400, '4BHK+STUDY': 1800
        }
        
        config_upper = str(configuration).upper()
        for config_pattern, area in area_map.items():
            if config_pattern in config_upper:
                return area
        
        return 1000  # Default area
    
    def calculate_price_per_sqft(self, ticket_price: float, carpet_area: int) -> float:
        """Calculate price per square foot"""
        if carpet_area > 0:
            return (ticket_price * 10000000) / carpet_area
        return 0.0
    
    def extract_quarter_year(self, purchase_quarter: str) -> tuple:
        """Extract quarter number and year from purchase quarter string"""
        try:
            # Try patterns like "Q1_2023", "2023-Q1", "Q1 2023"
            patterns = [
                r'Q(\d)[_\-]?(\d{4})',
                r'(\d{4})[_\-]?Q(\d)',
                r'(\d{4})'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, str(purchase_quarter))
                if match:
                    groups = match.groups()
                    if len(groups) == 2:
                        # Pattern with quarter and year
                        if pattern.startswith('Q'):
                            return int(groups[0]), int(groups[1])
                        else:
                            return int(groups[1]), int(groups[0])
                    elif len(groups) == 1:
                        # Pattern with only year
                        return 1, int(groups[0])  # Default to Q1
            
            # Default values if no pattern matches
            return 1, 2023
        except:
            return 1, 2023  # Default values
    
    def calculate_booking_flag(self, booking_status: str) -> int:
        """Calculate booking flag from booking status"""
        booked_statuses = ['booked', 'confirmed', 'sold', 'yes', 'true', '1']
        status_lower = str(booking_status).lower()
        return 1 if status_lower in booked_statuses else 0
    
    def categorize_price(self, ticket_price: float) -> str:
        """Categorize price into ranges"""
        if ticket_price <= 2:
            return '0-2Cr'
        elif ticket_price <= 5:
            return '2-5Cr'
        elif ticket_price <= 10:
            return '5-10Cr'
        elif ticket_price <= 20:
            return '10-20Cr'
        else:
            return '20Cr+'
    
    def determine_season(self, quarter_number: int) -> str:
        """Determine season based on quarter number"""
        season_map = {1: 'Winter', 2: 'Spring', 3: 'Summer', 4: 'Monsoon'}
        return season_map.get(quarter_number, 'Winter')
    
    def prepare_additional_data(self, row: pd.Series) -> str:
        """Prepare additional data as JSON string"""
        import json
        additional_data = {}
        
        # Include unmapped columns in additional data
        if hasattr(self, 'column_map'):
            mapped_columns = set(self.column_map.values())
            unmapped_columns = set(row.index) - mapped_columns
            
            for col in unmapped_columns:
                if not pd.isna(row[col]):
                    additional_data[col] = str(row[col])
        
        return json.dumps(additional_data) if additional_data else '{}'
    
    def get_data_summary(self) -> Optional[pd.DataFrame]:
        """Get summary of data in database"""
        if not self.connect():
            return None
        
        try:
            query = """
            SELECT 
                COUNT(*) as total_properties,
                AVG(ticket_price_cr) as avg_price,
                MIN(ticket_price_cr) as min_price,
                MAX(ticket_price_cr) as max_price,
                AVG(amenity_score) as avg_amenity_score,
                AVG(booking_flag) * 100 as booking_rate,
                COUNT(DISTINCT micro_market) as unique_markets,
                COUNT(DISTINCT builder) as unique_builders
            FROM housing_data
            """
            
            return self.execute_query(query)
        except Error as e:
            logger.error(f"Error getting data summary: {e}")
            return None
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """Execute a SQL query and return results as DataFrame"""
        if not self.connect():
            return None
        
        try:
            df = pd.read_sql(query, self.connection)
            return df
        except Error as e:
            logger.error(f"Error executing query: {e}")
            return None
    
    def close_connection(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("MySQL connection closed")

# Main execution
def main():
    """Main function to demonstrate data loading"""
    # Create loader instance
    loader = HousingDataLoader(
        host='localhost',
        user='root', 
        password='harish@21',  # Add your MySQL password here
        database='luxury_housing'
    )
    
    try:
        # CSV file path
        csv_path = 'data/processed/luxury_housing_cleaned.csv'
        
        if not os.path.exists(csv_path):
            print(f"❌ Cleaned data file not found: {csv_path}")
            print("Please run housing_data_cleaner.py first")
            return
        
        # Ask about clearing existing data
        clear_existing = input("Clear existing data from table? (y/n): ").lower().strip() == 'y'
        
        # Load data
        print("Loading data into database...")
        success = loader.load_cleaned_data(csv_path, clear_existing)
        
        if success:
            print("✅ Data loaded successfully!")
            
            # Show data summary
            print("\nData Summary from Database:")
            summary = loader.get_data_summary()
            if summary is not None and not summary.empty:
                print(summary.to_string(index=False))
            
            # Show sample data
            print("\nSample Data (first 5 records):")
            sample = loader.execute_query("SELECT * FROM housing_data LIMIT 5")
            if sample is not None:
                print(sample[['project_id', 'ticket_price_cr', 'configuration', 'micro_market', 'builder', 'booking_flag']].to_string(index=False))
        
        else:
            print("❌ Failed to load data")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        logger.error(f"Main execution error: {e}")
    
    finally:
        loader.close_connection()

if __name__ == "__main__":
    main()