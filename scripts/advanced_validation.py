# scripts/advanced_validation.py
from database import DatabaseConnection
import logging


class AdvancedDataValidator:
    def __init__(self, db_connection):
        self.db = db_connection

    def validate_record_counts(self):
        """Validate record counts across all tables"""
        queries = {
            'properties': "SELECT COUNT(*) FROM properties",
            'leads': "SELECT COUNT(*) FROM leads",
            'taxes': "SELECT COUNT(*) FROM taxes",
            'hoa_details': "SELECT COUNT(*) FROM hoa_details",
            'valuation_details': "SELECT COUNT(*) FROM valuation_details",
            'rehab_estimates': "SELECT COUNT(*) FROM rehab_estimates",
            'rehab_details': "SELECT COUNT(*) FROM rehab_details"
        }

        logging.info("=== RECORD COUNTS ===")
        for table, query in queries.items():
            self.db.cursor.execute(query)
            count = self.db.cursor.fetchone()[0]
            logging.info(f"{table}: {count:,} records")

    def validate_multiple_records_per_property(self):
        """Validate that multiple records per property are handled correctly"""
        logging.info("\n=== MULTIPLE RECORDS VALIDATION ===")

        # Check properties with multiple valuations
        self.db.cursor.execute("""
            SELECT property_id, COUNT(*) as valuation_count
            FROM valuation_details
            GROUP BY property_id
            HAVING COUNT(*) > 1
            LIMIT 5
        """)
        multi_valuations = self.db.cursor.fetchall()
        logging.info(f"Properties with multiple valuations: {len(multi_valuations)} found")
        for prop_id, count in multi_valuations[:3]:
            logging.info(f"  Property {prop_id}: {count} valuations")

        # Check properties with multiple HOA records
        self.db.cursor.execute("""
            SELECT property_id, COUNT(*) as hoa_count
            FROM hoa_details
            GROUP BY property_id
            HAVING COUNT(*) > 1
            LIMIT 5
        """)
        multi_hoa = self.db.cursor.fetchall()
        logging.info(f"Properties with multiple HOA records: {len(multi_hoa)} found")

        # Check properties with multiple rehab estimates
        self.db.cursor.execute("""
            SELECT property_id, COUNT(*) as rehab_count
            FROM rehab_estimates
            GROUP BY property_id
            HAVING COUNT(*) > 1
            LIMIT 5
        """)
        multi_rehab = self.db.cursor.fetchall()
        logging.info(f"Properties with multiple rehab estimates: {len(multi_rehab)} found")

    def validate_data_quality(self):
        """Check data quality metrics"""
        logging.info("\n=== DATA QUALITY CHECKS ===")

        # Check for missing critical fields
        self.db.cursor.execute("SELECT COUNT(*) FROM properties WHERE city = '' OR state = ''")
        missing_location = self.db.cursor.fetchone()[0]
        logging.info(f"Properties missing city/state: {missing_location}")

        # Check valuation ranges
        self.db.cursor.execute("""
            SELECT MIN(list_price), MAX(list_price), AVG(list_price)
            FROM valuation_details
            WHERE list_price IS NOT NULL AND list_price > 0
        """)
        price_stats = self.db.cursor.fetchone()
        if price_stats:
            logging.info(
                f"List Price range: ${price_stats[0]:,.0f} - ${price_stats[1]:,.0f} (avg: ${price_stats[2]:,.0f})")

        # Check rehab estimates
        self.db.cursor.execute("""
            SELECT MIN(underwriting_rehab), MAX(underwriting_rehab), AVG(underwriting_rehab)
            FROM rehab_estimates
            WHERE underwriting_rehab IS NOT NULL AND underwriting_rehab > 0
        """)
        rehab_stats = self.db.cursor.fetchone()
        if rehab_stats:
            logging.info(
                f"Rehab estimates range: ${rehab_stats[0]:,.0f} - ${rehab_stats[1]:,.0f} (avg: ${rehab_stats[2]:,.0f})")

    def run_validation(self):
        """Run all validation checks"""
        logging.info("Running advanced data validation...")
        self.validate_record_counts()
        self.validate_multiple_records_per_property()
        self.validate_data_quality()
        logging.info("Advanced validation completed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    db = DatabaseConnection()
    if db.connect():
        validator = AdvancedDataValidator(db)
        validator.run_validation()
        db.close()
