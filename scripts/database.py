# scripts/database.py
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

load_dotenv()


class DatabaseConnection:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establish database connection with correct credentials"""
        try:
            # Use the exact credentials from docker-compose.initial.yml
            self.connection = mysql.connector.connect(
                host='localhost',
                port=3306,
                database='home_db',  # MYSQL_DATABASE from Docker
                user='root',
                password='6equj5_root'  # MYSQL_ROOT_PASSWORD from Docker
            )

            if self.connection.is_connected():
                self.cursor = self.connection.cursor(buffered=True)
                print(" Successfully connected to MySQL database")
                print(f"Database: home_db")
                print(f"User: root")
                return True

        except Error as e:
            print(f" Error connecting to MySQL: {e}")

            # Try alternative user if root fails
            try:
                print("Trying alternative user credentials...")
                self.connection = mysql.connector.connect(
                    host='localhost',
                    port=3306,
                    database='home_db',
                    user='db_user',
                    password='6equj5_db_user'
                )

                if self.connection.is_connected():
                    self.cursor = self.connection.cursor(buffered=True)
                    print(" Connected with db_user credentials")
                    return True

            except Error as e2:
                print(f" Alternative connection also failed: {e2}")
                return False

        return False

    def execute_script(self, script_path):
        """Execute SQL script from file"""
        try:
            with open(script_path, 'r') as file:
                script = file.read()

            # Remove comments and split by semicolon more carefully
            lines = script.split('\n')
            clean_lines = []
            for line in lines:
                line = line.strip()
                if line and not line.startswith('--'):
                    clean_lines.append(line)
            
            clean_script = ' '.join(clean_lines)
            statements = [stmt.strip() for stmt in clean_script.split(';') if stmt.strip()]

            for statement in statements:
                if statement:
                    print(f"Executing: {statement[:50]}...")
                    self.cursor.execute(statement)

            self.connection.commit()
            print(f" Successfully executed script: {script_path}")

        except Error as e:
            print(f"Error executing script {script_path}: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Database connection closed")
