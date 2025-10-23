import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, Any
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HousingDataCleaner:
    def __init__(self, csv_path: str = r'C:\project2\Luxury_Housing_Bangalore.csv'):
        """
        Initialize HousingDataCleaner with data file path.
        """
        self.csv_path = Path(csv_path)
        self.df = None
        self.cleaning_report = {}
        self.required_columns = []
        
        try:
            self._load_data()
            self._identify_columns()
            self.clean_data()
            logger.info("HousingDataCleaner initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize HousingDataCleaner: {e}")
            raise
    
    def _load_data(self):
        """Load data from CSV file with validation"""
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Data file not found: {self.csv_path}")
        
        try:
            self.df = pd.read_csv(self.csv_path)
            logger.info(f"Successfully loaded data with {len(self.df)} rows and {len(self.df.columns)} columns")
            logger.info(f"Columns found: {list(self.df.columns)}")
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise
    
    def _identify_columns(self):
        """Identify available columns and map to expected names"""
        available_columns = set(self.df.columns)
        logger.info("Available columns in dataset:")
        for col in available_columns:
            logger.info(f"  - {col}")
        
        # Map common column name variations
        column_mapping = {
            'price': ['Ticket_Price_Cr', 'Price', 'price', 'ticket_price', 'Price_Cr'],
            'amenity': ['Amenity_Score', 'Amenity', 'amenity_score', 'AmenityScore'],
            'configuration': ['Configuration', 'configuration', 'config', 'BHK'],
            'micro_market': ['Micro_Market', 'micro_market', 'Location', 'Area', 'Market'],
            'builder': ['Builder', 'builder', 'Developer', 'developer'],
            'booking_status': ['Booking_Status', 'booking_status', 'Status', 'status', 'Booking'],
            'purchase_quarter': ['Purchase_Quarter', 'purchase_quarter', 'Quarter', 'quarter', 'Time_Period']
        }
        
        # Create a mapping of standard names to actual column names
        self.column_map = {}
        for standard_name, possible_names in column_mapping.items():
            for possible_name in possible_names:
                if possible_name in available_columns:
                    self.column_map[standard_name] = possible_name
                    logger.info(f"Mapped '{standard_name}' to column '{possible_name}'")
                    break
        
        # Log unmapped columns
        unmapped = available_columns - set(self.column_map.values())
        if unmapped:
            logger.info(f"Unmapped columns: {list(unmapped)}")
    
    def get_column(self, standard_name):
        """Get actual column name from standard name"""
        return self.column_map.get(standard_name)
    
    def clean_data(self):
        """Main data cleaning pipeline"""
        logger.info("Starting comprehensive data cleaning process...")
        initial_shape = self.df.shape
        self.cleaning_report['initial_shape'] = initial_shape
        
        # Cleaning steps
        cleaning_steps = [
            ('price_cleaning', self.clean_price_column),
            ('missing_values', self.handle_missing_values),
            ('text_normalization', self.normalize_text_fields),
            ('feature_engineering', self.create_new_features),
            ('data_validation', self.validate_data),
        ]
        
        for step_name, step_func in cleaning_steps:
            try:
                initial_rows = len(self.df)
                step_func()
                rows_affected = initial_rows - len(self.df)
                self.cleaning_report[step_name] = {
                    'rows_affected': rows_affected,
                    'status': 'success'
                }
                logger.info(f"Completed {step_name}")
            except Exception as e:
                self.cleaning_report[step_name] = {
                    'rows_affected': 0,
                    'status': f'failed: {str(e)}'
                }
                logger.error(f"Error in {step_name}: {e}")
        
        self.cleaning_report['final_shape'] = self.df.shape
        logger.info(f"Data cleaning completed. Final shape: {self.df.shape}")
        logger.info(f"Final columns: {list(self.df.columns)}")
    
    def clean_price_column(self):
        """Clean and normalize price column"""
        price_col = self.get_column('price')
        if not price_col:
            logger.warning("Price column not found. Skipping price cleaning.")
            return
        
        logger.info(f"Cleaning price column: {price_col}")
        
        # Create a copy for safety
        original_prices = self.df[price_col].copy()
        
        # Convert to string and clean
        self.df[price_col] = self.df[price_col].astype(str)
        
        # Remove various currency symbols and units
        price_cleaning_patterns = [
            (r'[₹$,]', ''),           # Remove currency symbols and commas
            (r'cr\.?', '', re.IGNORECASE),  # Remove 'cr' notation
            (r'lac\.?', '', re.IGNORECASE), # Remove 'lac' notation
            (r'\s+', ''),             # Remove whitespace
            (r'[a-zA-Z]', ''),        # Remove any remaining letters
        ]
        
        for pattern, replacement, *flags in price_cleaning_patterns:
            flag = flags[0] if flags else 0
            self.df[price_col] = self.df[price_col].str.replace(
                pattern, replacement, regex=True, flags=flag
            )
        
        # Convert to numeric
        self.df[price_col] = pd.to_numeric(self.df[price_col], errors='coerce')
        
        # Handle missing values
        price_median = self.df[price_col].median()
        self.df[price_col].fillna(price_median, inplace=True)
        
        # Rename to standard name
        if price_col != 'Ticket_Price_Cr':
            self.df['Ticket_Price_Cr'] = self.df[price_col]
            # Keep original column for reference
            self.df[f'original_{price_col}'] = original_prices
        
        logger.info(f"Price cleaning completed. Median price: ₹{price_median:.2f} Cr")
    
    def handle_missing_values(self):
        """Comprehensive missing value handling"""
        logger.info("Handling missing values...")
        
        # Define imputation strategies for each column type
        for standard_name, actual_name in self.column_map.items():
            if actual_name in self.df.columns:
                null_count = self.df[actual_name].isnull().sum()
                if null_count > 0:
                    logger.info(f"Column {actual_name} has {null_count} null values")
                    
                    if standard_name in ['price', 'amenity']:
                        # Numeric columns - use median
                        median_val = self.df[actual_name].median()
                        self.df[actual_name].fillna(median_val, inplace=True)
                        logger.info(f"Filled {null_count} nulls in {actual_name} with median: {median_val}")
                    
                    elif standard_name == 'booking_status':
                        # Booking status - use 'Not Booked'
                        self.df[actual_name].fillna('Not Booked', inplace=True)
                        logger.info(f"Filled {null_count} nulls in {actual_name} with 'Not Booked'")
                    
                    else:
                        # Categorical columns - use mode or 'Unknown'
                        if not self.df[actual_name].mode().empty:
                            mode_val = self.df[actual_name].mode()[0]
                            self.df[actual_name].fillna(mode_val, inplace=True)
                            logger.info(f"Filled {null_count} nulls in {actual_name} with mode: {mode_val}")
                        else:
                            self.df[actual_name].fillna('Unknown', inplace=True)
                            logger.info(f"Filled {null_count} nulls in {actual_name} with 'Unknown'")
        
        # Handle other columns not in our mapping
        for col in self.df.columns:
            if col not in self.column_map.values():
                null_count = self.df[col].isnull().sum()
                if null_count > 0:
                    if self.df[col].dtype in ['float64', 'int64']:
                        self.df[col].fillna(self.df[col].median(), inplace=True)
                    else:
                        self.df[col].fillna('Unknown', inplace=True)
    
    def normalize_text_fields(self):
        """Normalize and clean text fields"""
        text_columns = ['configuration', 'micro_market', 'builder', 'booking_status']
        
        for std_name in text_columns:
            actual_name = self.get_column(std_name)
            if actual_name and actual_name in self.df.columns:
                # Convert to string, title case, and strip whitespace
                self.df[actual_name] = self.df[actual_name].astype(str).str.title().str.strip()
                
                # Specific normalization for configuration
                if std_name == 'configuration':
                    self.df[actual_name] = self.df[actual_name].str.upper()
                    # Standardize BHK formats
                    self.df[actual_name] = self.df[actual_name].str.replace(r'(\d)\s*BHK', r'\1BHK', regex=True)
                
                logger.info(f"Normalized text column: {actual_name}")
    
    def create_new_features(self):
        """Create derived features for analysis"""
        logger.info("Creating new features...")
        
        # 1. Create Booking_Flag if booking status exists
        booking_col = self.get_column('booking_status')
        if booking_col and booking_col in self.df.columns:
            self.df['Booking_Flag'] = self.df[booking_col].apply(
                lambda x: 1 if str(x).lower() in ['booked', 'confirmed', 'sold', 'yes'] else 0
            )
            logger.info("Created Booking_Flag feature")
        else:
            # Create a default Booking_Flag if no booking status column
            self.df['Booking_Flag'] = 0
            logger.warning("No booking status column found. Created default Booking_Flag with all zeros.")
        
        # 2. Create Carpet_Area_Sqft if not exists
        if 'Carpet_Area_Sqft' not in self.df.columns:
            # Enhanced area mapping with more configurations
            area_map = {
                '1BHK': 650, '2BHK': 950, '3BHK': 1250, '4BHK': 1600, 
                '5BHK': 2100, 'STUDIO': 400, '1RK': 350, '2BHK+STUDY': 1100,
                '3BHK+STUDY': 1400, '4BHK+STUDY': 1800, 'UNKNOWN': 1000
            }
            
            config_col = self.get_column('configuration')
            if config_col and config_col in self.df.columns:
                self.df['Carpet_Area_Sqft'] = self.df[config_col].map(area_map).fillna(1000)
                logger.info("Created Carpet_Area_Sqft based on configuration")
            else:
                self.df['Carpet_Area_Sqft'] = 1000  # Default area
                logger.warning("No configuration column found. Used default area of 1000 sqft.")
        
        # 3. Calculate Price_per_Sqft
        if 'Ticket_Price_Cr' in self.df.columns and 'Carpet_Area_Sqft' in self.df.columns:
            self.df['Price_per_Sqft'] = (self.df['Ticket_Price_Cr'] * 10000000) / self.df['Carpet_Area_Sqft']
            logger.info("Created Price_per_Sqft feature")
        
        # 4. Extract quarter and year if purchase quarter exists
        quarter_col = self.get_column('purchase_quarter')
        if quarter_col and quarter_col in self.df.columns:
            # Try to extract quarter and year
            quarter_year_pattern = r'Q(\d)[_\-]?(\d{4})'
            extracted = self.df[quarter_col].str.extract(quarter_year_pattern)
            
            if not extracted.empty:
                self.df['Quarter_Number'] = pd.to_numeric(extracted[0], errors='coerce').fillna(1)
                self.df['Year'] = pd.to_numeric(extracted[1], errors='coerce').fillna(2023)
                logger.info("Extracted Quarter_Number and Year from purchase quarter")
            else:
                # Alternative pattern matching
                alternative_pattern = r'(\d{4})'
                year_extracted = self.df[quarter_col].str.extract(alternative_pattern)
                if not year_extracted.empty:
                    self.df['Year'] = pd.to_numeric(year_extracted[0], errors='coerce').fillna(2023)
                    self.df['Quarter_Number'] = 1  # Default to Q1
                    logger.info("Extracted Year from purchase quarter")
        else:
            # Create default values
            self.df['Quarter_Number'] = 1
            self.df['Year'] = 2023
            logger.warning("No purchase quarter column found. Used default year and quarter.")
        
        # 5. Create Price Categories
        if 'Ticket_Price_Cr' in self.df.columns:
            price_bins = [0, 2, 5, 10, 20, float('inf')]
            price_labels = ['0-2Cr', '2-5Cr', '5-10Cr', '10-20Cr', '20Cr+']
            self.df['Price_Category'] = pd.cut(
                self.df['Ticket_Price_Cr'], bins=price_bins, labels=price_labels, right=False
            )
            logger.info("Created Price_Category feature")
        
        # 6. Create Season based on quarter
        if 'Quarter_Number' in self.df.columns:
            season_map = {1: 'Winter', 2: 'Spring', 3: 'Summer', 4: 'Monsoon'}
            self.df['Season'] = self.df['Quarter_Number'].map(season_map).fillna('Winter')
            logger.info("Created Season feature")
        
        logger.info("Feature engineering completed")
    
    def validate_data(self):
        """Validate data quality after cleaning"""
        logger.info("Validating data...")
        
        validation_results = {}
        
        # Check for remaining null values
        null_counts = self.df.isnull().sum()
        remaining_nulls = null_counts[null_counts > 0]
        
        if not remaining_nulls.empty:
            logger.warning(f"Remaining null values: {remaining_nulls.to_dict()}")
            validation_results['remaining_nulls'] = remaining_nulls.to_dict()
        else:
            logger.info("No remaining null values")
            validation_results['remaining_nulls'] = {}
        
        # Check data types
        validation_results['dtypes'] = self.df.dtypes.to_dict()
        
        # Basic statistics for numeric columns
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        if not numeric_cols.empty:
            validation_results['statistics'] = self.df[numeric_cols].describe().to_dict()
        
        self.cleaning_report['validation'] = validation_results
        
        # Print summary
        print("\n" + "="*50)
        print("DATA CLEANING SUMMARY")
        print("="*50)
        print(f"Initial shape: {self.cleaning_report['initial_shape']}")
        print(f"Final shape: {self.cleaning_report['final_shape']}")
        print(f"Final columns: {len(self.df.columns)}")
        print(f"Required features created:")
        print(f"  - Booking_Flag: {'Yes' if 'Booking_Flag' in self.df.columns else 'No'}")
        print(f"  - Price_per_Sqft: {'Yes' if 'Price_per_Sqft' in self.df.columns else 'No'}")
        print(f"  - Carpet_Area_Sqft: {'Yes' if 'Carpet_Area_Sqft' in self.df.columns else 'No'}")
    
    def get_cleaning_report(self) -> Dict[str, Any]:
        """Get detailed cleaning report"""
        return self.cleaning_report
    
    def save_cleaned_data(self, output_path: str):
        """Save cleaned data to CSV"""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            self.df.to_csv(output_path, index=False)
            logger.info(f"Cleaned data saved to: {output_path}")
            print(f"✓ Cleaned data saved to: {output_path}")
        except Exception as e:
            logger.error(f"Failed to save cleaned data: {e}")
            raise
    
    def get_dataframe(self) -> pd.DataFrame:
        """Get the cleaned dataframe"""
        return self.df.copy()

# Main execution with better error handling
if __name__ == "__main__":
    try:
        # Try multiple possible file locations
        possible_paths = [
            'Luxury_Housing_Bangalore.csv',
            'data/raw/luxury_housing_raw.csv',
            r'C:\project2\Luxury_Housing_Bangalore.csv',
            './Luxury_Housing_Bangalore.csv'
        ]
        
        cleaner = None
        used_path = None
        
        for csv_path in possible_paths:
            try:
                logger.info(f"Trying to load data from: {csv_path}")
                cleaner = HousingDataCleaner(csv_path)
                used_path = csv_path
                print(f"✓ Successfully loaded data from: {csv_path}")
                break
            except FileNotFoundError:
                continue
            except Exception as e:
                logger.error(f"Error with path {csv_path}: {e}")
                continue
        
        if cleaner is None:
            logger.error("Could not find data file in any expected location")
            print("❌ Could not find data file. Please ensure your CSV file is in one of these locations:")
            for path in possible_paths:
                print(f"  - {path}")
        else:
            # Save cleaned data
            cleaner.save_cleaned_data('data/processed/luxury_housing_cleaned.csv')
            
            # Print final column list
            df_clean = cleaner.get_dataframe()
            print(f"\n✓ Final dataset has {len(df_clean)} rows and {len(df_clean.columns)} columns")
            print("✓ Columns available for analysis:")
            for col in df_clean.columns:
                print(f"  - {col}")
            
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"❌ Error: {e}")