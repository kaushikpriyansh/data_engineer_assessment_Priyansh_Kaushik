import pandas as pd
import json
import ast
import numpy as np
from database import DatabaseConnection
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class AdvancedPropertyETL:
    def __init__(self, db_connection):
        self.db = db_connection
        self.property_mapping = {}
        self.df = None

    def extract_data(self, json_file_path):
        """Extract data from JSON file"""
        try:
            with open(json_file_path, 'r') as f:
                raw_data = json.load(f)

            self.df = pd.DataFrame(raw_data)
            logging.info(f"Extracted {len(self.df)} records from {json_file_path}")
            return self.df

        except Exception as e:
            logging.error(f"Error extracting data: {e}")
            raise

    def clean_data(self):
        """Clean and validate the data"""
        logging.info("Starting data cleaning...")

        # Handle missing values appropriately
        self.df = self.df.fillna('')

        # Clean numeric columns with proper validation
        numeric_mappings = {
            'Tax_Rate': 'tax_rate',
            'SQFT_Basement': 'sqft_basement',
            'Year_Built': 'year_built',
            'SQFT_MU': 'sqft_mu',
            'SQFT_Total': 'sqft_total',
            'Bed': 'bed',
            'Bath': 'bath',
            'Net_Yield': 'net_yield',
            'IRR': 'irr',
            'Neighborhood_Rating': 'neighborhood_rating',
            'Latitude': 'latitude',
            'Longitude': 'longitude',
            'Taxes': 'taxes',
            'School_Average': 'school_average'
        }

        for original_col, clean_col in numeric_mappings.items():
            if original_col in self.df.columns:
                self.df[clean_col] = pd.to_numeric(self.df[original_col], errors='coerce')

        logging.info("Data cleaning completed")
        return self.df

    def parse_nested_json(self, json_string):
        """Parse nested JSON string safely"""
        # Handle pandas Series and other array-like objects
        if hasattr(json_string, '__iter__') and not isinstance(json_string, (str, dict)):
            # If it's already a list, return it directly
            if isinstance(json_string, list):
                return json_string
            return []
            
        if not json_string or json_string == '' or pd.isna(json_string):
            return []

        try:
            if isinstance(json_string, str):
                return ast.literal_eval(json_string)
            elif isinstance(json_string, list):
                return json_string
            else:
                return []
        except:
            return []

    def load_properties(self):
        """Load main property records"""
        properties_data = []

        for idx, row in self.df.iterrows():
            # Extract all property fields according to field config
            property_data = (
                row.get('Property_Title', ''),
                row.get('Address', ''),
                row.get('Street_Address', ''),
                row.get('City', ''),
                row.get('State', ''),
                row.get('Zip', ''),
                row.get('Property_Type', ''),
                row.get('Market', ''),
                int(row.get('year_built', 0)) if pd.notna(row.get('year_built')) and row.get('year_built',
                                                                                             0) > 0 else None,
                row.get('Flood', ''),
                row.get('Highway', ''),
                row.get('Train', ''),
                float(row.get('tax_rate', 0)) if pd.notna(row.get('tax_rate')) else None,
                int(row.get('sqft_basement', 0)) if pd.notna(row.get('sqft_basement')) else None,
                row.get('HTW', ''),
                row.get('Pool', ''),
                row.get('Commercial', ''),
                row.get('Water', ''),
                row.get('Sewage', ''),
                int(row.get('sqft_mu', 0)) if pd.notna(row.get('sqft_mu')) else None,
                int(row.get('sqft_total', 0)) if pd.notna(row.get('sqft_total')) else None,
                row.get('Parking', ''),
                int(row.get('bed', 0)) if pd.notna(row.get('bed')) else None,
                int(row.get('bath', 0)) if pd.notna(row.get('bath')) else None,
                row.get('BasementYesNo', ''),
                row.get('Layout', ''),
                int(row.get('neighborhood_rating', 0)) if pd.notna(row.get('neighborhood_rating')) else None,
                float(row.get('latitude', 0)) if pd.notna(row.get('latitude')) else None,
                float(row.get('longitude', 0)) if pd.notna(row.get('longitude')) else None,
                row.get('Subdivision', ''),
                float(row.get('school_average', 0)) if pd.notna(row.get('school_average')) else None
            )
            properties_data.append(property_data)

        # Bulk insert properties
        insert_query = """
        INSERT INTO properties (
            property_title, address, street_address, city, state, zip, property_type, market, year_built,
            flood, highway, train, tax_rate, sqft_basement, htw, pool, commercial, water, sewage,
            sqft_mu, sqft_total, parking, bed, bath, basement_yes_no, layout, neighborhood_rating,
            latitude, longitude, subdivision, school_average
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            self.db.cursor.executemany(insert_query, properties_data)
            self.db.connection.commit()

            # Create property mapping
            self.db.cursor.execute("SELECT property_id FROM properties ORDER BY property_id")
            property_ids = [row[0] for row in self.db.cursor.fetchall()]

            for idx, prop_id in enumerate(property_ids[-len(self.df):]):
                self.property_mapping[idx] = prop_id

            logging.info(f"Loaded {len(properties_data)} properties")

        except Exception as e:
            logging.error(f"Error loading properties: {e}")
            self.db.connection.rollback()
            raise

    def load_leads(self):
        """Load leads data"""
        leads_data = []

        for idx, row in self.df.iterrows():
            if idx in self.property_mapping:
                property_id = self.property_mapping[idx]
                leads_data.append((
                    property_id,
                    row.get('Reviewed_Status', ''),
                    row.get('Most_Recent_Status', ''),
                    row.get('Source', ''),
                    row.get('Occupancy', ''),
                    float(row.get('net_yield', 0)) if pd.notna(row.get('net_yield')) else None,
                    float(row.get('irr', 0)) if pd.notna(row.get('irr')) else None,
                    row.get('Selling_Reason', ''),
                    row.get('Seller_Retained_Broker', ''),
                    row.get('Final_Reviewer', ''),
                    row.get('Rent_Restricted', '')
                ))

        insert_query = """
        INSERT INTO leads (property_id, reviewed_status, most_recent_status, source, occupancy,
                          net_yield, irr, selling_reason, seller_retained_broker, final_reviewer, rent_restricted)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            self.db.cursor.executemany(insert_query, leads_data)
            self.db.connection.commit()
            logging.info(f"Loaded {len(leads_data)} lead records")
        except Exception as e:
            logging.error(f"Error loading leads: {e}")
            self.db.connection.rollback()
            raise

    def load_taxes(self):
        """Load tax information"""
        taxes_data = []

        for idx, row in self.df.iterrows():
            if idx in self.property_mapping:
                property_id = self.property_mapping[idx]
                if pd.notna(row.get('taxes')):
                    taxes_data.append((
                        property_id,
                        float(row.get('taxes', 0)),
                        datetime.now().year  # Current year as default
                    ))

        if taxes_data:
            insert_query = "INSERT INTO taxes (property_id, taxes, tax_year) VALUES (%s, %s, %s)"
            try:
                self.db.cursor.executemany(insert_query, taxes_data)
                self.db.connection.commit()
                logging.info(f"Loaded {len(taxes_data)} tax records")
            except Exception as e:
                logging.error(f"Error loading taxes: {e}")
                self.db.connection.rollback()
                raise

    def load_hoa_details(self):
        """Load multiple HOA records per property"""
        hoa_data = []

        for idx, row in self.df.iterrows():
            if idx in self.property_mapping:
                property_id = self.property_mapping[idx]

                # Parse HOA list
                hoa_list = self.parse_nested_json(row.get('HOA', ''))

                for seq_num, hoa_record in enumerate(hoa_list, 1):
                    if isinstance(hoa_record, dict):
                        hoa_data.append((
                            property_id,
                            float(hoa_record.get('HOA', 0)) if hoa_record.get('HOA') else None,
                            hoa_record.get('HOA_Flag', ''),
                            seq_num
                        ))

        if hoa_data:
            insert_query = """
            INSERT INTO hoa_details (property_id, hoa_fee, hoa_flag, sequence_number)
            VALUES (%s, %s, %s, %s)
            """
            try:
                self.db.cursor.executemany(insert_query, hoa_data)
                self.db.connection.commit()
                logging.info(f"Loaded {len(hoa_data)} HOA detail records")
            except Exception as e:
                logging.error(f"Error loading HOA details: {e}")
                self.db.connection.rollback()
                raise

    def load_valuation_details(self):
        """Load multiple valuation records per property"""
        valuation_data = []

        for idx, row in self.df.iterrows():
            if idx in self.property_mapping:
                property_id = self.property_mapping[idx]

                # Parse Valuation list
                valuation_list = self.parse_nested_json(row.get('Valuation', ''))

                for seq_num, val_record in enumerate(valuation_list, 1):
                    if isinstance(val_record, dict):
                        valuation_data.append((
                            property_id,
                            seq_num,
                            float(val_record.get('Previous_Rent', 0)) if val_record.get('Previous_Rent') else None,
                            float(val_record.get('List_Price', 0)) if val_record.get('List_Price') else None,
                            float(val_record.get('Zestimate', 0)) if val_record.get('Zestimate') else None,
                            float(val_record.get('ARV', 0)) if val_record.get('ARV') else None,
                            float(val_record.get('Expected_Rent', 0)) if val_record.get('Expected_Rent') else None,
                            float(val_record.get('Rent_Zestimate', 0)) if val_record.get('Rent_Zestimate') else None,
                            float(val_record.get('Low_FMR', 0)) if val_record.get('Low_FMR') else None,
                            float(val_record.get('High_FMR', 0)) if val_record.get('High_FMR') else None,
                            float(val_record.get('Redfin_Value', 0)) if val_record.get('Redfin_Value') else None
                        ))

        if valuation_data:
            insert_query = """
            INSERT INTO valuation_details (
                property_id, sequence_number, previous_rent, list_price, zestimate, arv,
                expected_rent, rent_zestimate, low_fmr, high_fmr, redfin_value
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            try:
                self.db.cursor.executemany(insert_query, valuation_data)
                self.db.connection.commit()
                logging.info(f"Loaded {len(valuation_data)} valuation detail records")
            except Exception as e:
                logging.error(f"Error loading valuation details: {e}")
                self.db.connection.rollback()
                raise

    def load_rehab_estimates(self):
        """Load multiple rehab estimates per property with detailed breakdown"""
        rehab_estimates_data = []
        rehab_details_data = []

        for idx, row in self.df.iterrows():
            if idx in self.property_mapping:
                property_id = self.property_mapping[idx]

                # Parse Rehab list
                rehab_list = self.parse_nested_json(row.get('Rehab', ''))

                for seq_num, rehab_record in enumerate(rehab_list, 1):
                    if isinstance(rehab_record, dict):
                        # Insert rehab estimate
                        rehab_estimate_data = (
                            property_id,
                            seq_num,
                            float(rehab_record.get('Underwriting_Rehab', 0)) if rehab_record.get(
                                'Underwriting_Rehab') else None,
                            float(rehab_record.get('Rehab_Calculation', 0)) if rehab_record.get(
                                'Rehab_Calculation') else None
                        )
                        rehab_estimates_data.append(rehab_estimate_data)

        # Insert rehab estimates first
        if rehab_estimates_data:
            estimates_query = """
            INSERT INTO rehab_estimates (property_id, sequence_number, underwriting_rehab, rehab_calculation)
            VALUES (%s, %s, %s, %s)
            """
            try:
                self.db.cursor.executemany(estimates_query, rehab_estimates_data)
                self.db.connection.commit()

                # Now get the rehab_estimate_ids for details
                self.db.cursor.execute("""
                    SELECT rehab_estimate_id, property_id, sequence_number 
                    FROM rehab_estimates 
                    ORDER BY rehab_estimate_id
                """)
                rehab_id_mapping = {(row[1], row[2]): row[0] for row in self.db.cursor.fetchall()}

                # Prepare rehab details data
                for idx, row in self.df.iterrows():
                    if idx in self.property_mapping:
                        property_id = self.property_mapping[idx]
                        rehab_list = self.parse_nested_json(row.get('Rehab', ''))

                        for seq_num, rehab_record in enumerate(rehab_list, 1):
                            if isinstance(rehab_record, dict):
                                rehab_estimate_id = rehab_id_mapping.get((property_id, seq_num))
                                if rehab_estimate_id:
                                    rehab_detail_data = (
                                        rehab_estimate_id,
                                        rehab_record.get('Paint', ''),
                                        rehab_record.get('Flooring_Flag', ''),
                                        rehab_record.get('Foundation_Flag', ''),
                                        rehab_record.get('Roof_Flag', ''),
                                        rehab_record.get('HVAC_Flag', ''),
                                        rehab_record.get('Kitchen_Flag', ''),
                                        rehab_record.get('Bathroom_Flag', ''),
                                        rehab_record.get('Appliances_Flag', ''),
                                        rehab_record.get('Windows_Flag', ''),
                                        rehab_record.get('Landscaping_Flag', ''),
                                        rehab_record.get('Trashout_Flag', '')
                                    )
                                    rehab_details_data.append(rehab_detail_data)

                # Insert rehab details
                if rehab_details_data:
                    details_query = """
                    INSERT INTO rehab_details (
                        rehab_estimate_id, paint, flooring_flag, foundation_flag, roof_flag,
                        hvac_flag, kitchen_flag, bathroom_flag, appliances_flag, windows_flag,
                        landscaping_flag, trashout_flag
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.db.cursor.executemany(details_query, rehab_details_data)
                    self.db.connection.commit()

                logging.info(
                    f"Loaded {len(rehab_estimates_data)} rehab estimates and {len(rehab_details_data)} rehab details")

            except Exception as e:
                logging.error(f"Error loading rehab data: {e}")
                self.db.connection.rollback()
                raise

    def run_etl(self, json_file_path):
        """Run the complete advanced ETL process"""
        try:
            logging.info("Starting Advanced ETL process...")

            # Extract and Transform
            self.extract_data(json_file_path)
            self.clean_data()

            # Load data in dependency order
            self.load_properties()
            self.load_leads()
            self.load_taxes()
            self.load_hoa_details()
            self.load_valuation_details()
            self.load_rehab_estimates()

            logging.info("Advanced ETL process completed successfully!")

        except Exception as e:
            logging.error(f"Advanced ETL process failed: {e}")
            raise


def main():
    """Main execution function"""
    db = DatabaseConnection()

    if not db.connect():
        return

    try:
        # Create schema
        logging.info("Creating final database schema...")
        db.execute_script('C:/Users/grant/Desktop/Assignmnet 2/data_engineer_assessment_Priyansh_Kaushik/sql/create_final_schema.sql')

        # Run ETL
        etl = AdvancedPropertyETL(db)
        etl.run_etl('C:/Users/grant/Desktop/Assignmnet 2/data_engineer_assessment_Priyansh_Kaushik/data/fake_property_data.json')

    finally:
        db.close()


if __name__ == "__main__":
    main()
