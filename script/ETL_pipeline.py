# scripts/etl.py

import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl.log'),
        logging.StreamHandler()
    ]
)

def connect_to_db():
    """Connect to MySQL database."""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='root',  # Update with actual user from docker-compose
            password='6equj5_root',  # Update with actual password
            database='home_db'  # Update with actual database name
        )
        if connection.is_connected():
            logging.info("Connected to MySQL database")
            return connection
    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        return None

def extract_data(json_path, excel_path):
    """Read JSON and Excel files."""
    try:
        # Read JSON
        df = pd.read_json(json_path, convert_dates=False)
        logging.info(f"Loaded {len(df)} records from {json_path}")

        # Read Excel field config (assumed columns: field_name, table, data_type, required)
        field_config = pd.read_excel(excel_path)
        logging.info(f"Loaded field config from {excel_path}")
        return df, field_config
    except Exception as e:
        logging.error(f"Error in extract_data: {e}")
        return None, None

def transform_data(df, field_config):
    """Clean and transform data for normalized schema."""
    try:
        # Initialize DataFrames
        properties = []
        hoa_list = []
        rehab_list = []
        valuations_list = []

        # Define fields based on JSON structure
        property_fields = [
            'Property_Title', 'Street_Address', 'City', 'State', 'Zip', 'Property_Type',
            'SQFT_Basement', 'SQFT_MU', 'SQFT_Total', 'Year_Built', 'Bed', 'Bath',
            'Parking', 'Layout', 'Highway', 'Train', 'Water', 'Sewage', 'Pool',
            'Commercial', 'HTW', 'Tax_Rate', 'Taxes', 'Net_Yield', 'IRR',
            'Rent_Restricted', 'Neighborhood_Rating', 'Latitude', 'Longitude',
            'Subdivision', 'Selling_Reason', 'Seller_Retained_Broker',
            'Final_Reviewer', 'School_Average', 'Reviewed_Status', 'Most_Recent_Status',
            'Source', 'Market', 'Occupancy', 'Flood'
        ]
        hoa_fields = ['HOA', 'HOA_Flag']
        rehab_fields = [
            'Underwriting_Rehab', 'Rehab_Calculation', 'Paint', 'Flooring_Flag',
            'Foundation_Flag', 'Roof_Flag', 'HVAC_Flag', 'Kitchen_Flag',
            'Bathroom_Flag', 'Appliances_Flag', 'Windows_Flag', 'Landscaping_Flag',
            'Trashout_Flag']

        valuation_fields = [
            'List_Price', 'Previous_Rent', 'ARV', 'Rent_Zestimate', 'Low_FMR',
            'High_FMR', 'Redfin_Value', 'Zestimate', 'Expected_Rent'
        ]

        # Process each property record
        for _, row in df.iterrows():
            # Properties
            prop_data = {f: row.get(f) for f in property_fields}
            # Clean data
            for field in ['SQFT_Basement', 'SQFT_MU', 'SQFT_Total', 'Year_Built', 'Bed', 'Bath', 'Neighborhood_Rating']:
                prop_data[field] = pd.to_numeric(prop_data.get(field), errors='coerce')
            for field in ['Tax_Rate', 'Taxes', 'Net_Yield', 'IRR', 'School_Average', 'Latitude', 'Longitude']:
                prop_data[field] = pd.to_numeric(prop_data.get(field), errors='coerce')
            # Standardize strings
            for field in ['Property_Type', 'Parking', 'Layout', 'Highway', 'Train', 'Water', 'Sewage', 'Pool', 'Commercial', 'HTW', 'Rent_Restricted', 'Subdivision', 'Selling_Reason', 'Seller_Retained_Broker', 'Final_Reviewer', 'Reviewed_Status', 'Most_Recent_Status', 'Source', 'Market', 'Occupancy', 'Flood']:
                prop_data[field] = str(prop_data[field]).strip() if prop_data[field] is not None else None
            properties.append(prop_data)

            # HOA
            for hoa_entry in row.get('HOA', []):
                hoa_data = {
                    'hoa_fee': pd.to_numeric(hoa_entry.get('HOA'), errors='coerce'),
                    'hoa_flag': str(hoa_entry.get('HOA_Flag')).strip() if hoa_entry.get('HOA_Flag') is not None else None,
                    'property_id': None,
                    'address_key': row['Street_Address']
                }
                hoa_list.append(hoa_data)

            # Rehab Estimates
            for rehab_entry in row.get('Rehab', []):
                rehab_data = {f: rehab_entry.get(f) for f in rehab_fields}
                rehab_data['property_id'] = None
                rehab_data['address_key'] = row['Street_Address']
                for field in ['Underwriting_Rehab', 'Rehab_Calculation']:
                    rehab_data[field] = pd.to_numeric(rehab_data.get(field), errors='coerce')
                for field in ['Paint', 'Flooring_Flag', 'Foundation_Flag', 'Roof_Flag', 'HVAC_Flag', 'Kitchen_Flag', 'Bathroom_Flag', 'Appliances_Flag', 'Windows_Flag', 'Landscaping_Flag', 'Trashout_Flag']:
                    rehab_data[field] = str(rehab_data[field]).strip() if rehab_data[field] is not None else None
                rehab_list.append(rehab_data)

            # Valuations
            for val_entry in row.get('Valuation', []):
                val_data = {f: val_entry.get(f) for f in valuation_fields}
                val_data['property_id'] = None
                val_data['address_key'] = row['Street_Address']
                for field in valuation_fields:
                    val_data[field] = pd.to_numeric(val_data.get(field), errors='coerce')
                valuations_list.append(val_data)

        return (
            pd.DataFrame(properties),
            pd.DataFrame(hoa_list),
            pd.DataFrame(rehab_list),
            pd.DataFrame(valuations_list)
        )
    except Exception as e:
        logging.error(f"Error in transform_data: {e}")
        return None, None, None, None

def load_data(connection, properties_df, hoa_df, rehab_df, valuations_df):
    """Load transformed data into MySQL tables."""
    try:
        cursor = connection.cursor()

        # Insert Properties
        property_ids = {}
        for _, row in properties_df.iterrows():
            sql = """
                INSERT INTO properties (
                    property_title, street_address, city, state, zip_code, property_type,
                    sqft_basement, sqft_mu, sqft_total, year_built, bedrooms, bathrooms,
                    parking, layout, highway, train, water, sewage, pool, commercial,
                    htw, tax_rate, taxes, net_yield, irr, rent_restricted, neighborhood_rating,
                    latitude, longitude, subdivision, selling_reason, seller_retained_broker,
                    final_reviewer, school_average, reviewed_status, most_recent_status,
                    source, market, occupancy, flood
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                row['Property_Title'], row['Street_Address'], row['City'], row['State'],
                row['Zip'], row['Property_Type'], row['SQFT_Basement'], row['SQFT_MU'],
                row['SQFT_Total'], row['Year_Built'], row['Bed'], row['Bath'], row['Parking'],
                row['Layout'], row['Highway'], row['Train'], row['Water'], row['Sewage'],
                row['Pool'], row['Commercial'], row['HTW'], row['Tax_Rate'], row['Taxes'],
                row['Net_Yield'], row['IRR'], row['Rent_Restricted'], row['Neighborhood_Rating'],
                row['Latitude'], row['Longitude'], row['Subdivision'], row['Selling_Reason'],
                row['Seller_Retained_Broker'], row['Final_Reviewer'], row['School_Average'],
                row['Reviewed_Status'], row['Most_Recent_Status'], row['Source'], row['Market'],
                row['Occupancy'], row['Flood']
            )
            cursor.execute(sql, values)
            property_ids[row['Street_Address']] = cursor.lastrowid
        connection.commit()
        logging.info(f"Inserted {len(properties_df)} properties")

        # Insert HOA
        for _, row in hoa_df.iterrows():
            sql = """
                INSERT INTO hoa (property_id, hoa_fee, hoa_flag)
                VALUES (%s, %s, %s)
            """
            values = (
                property_ids.get(row['address_key']),
                row['hoa_fee'],
                row['hoa_flag']
            )
            cursor.execute(sql, values)
        connection.commit()
        logging.info(f"Inserted {len(hoa_df)} HOA records")

        # Insert Rehab Estimates
        for _, row in rehab_df.iterrows():
            sql = """
                INSERT INTO rehab_estimates (
                    property_id, underwriting_rehab, rehab_calculation, paint,
                    flooring_flag, foundation_flag, roof_flag, hvac_flag,
                    kitchen_flag, bathroom_flag, appliances_flag, windows_flag,
                    landscaping_flag, trashout_flag
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                property_ids.get(row['address_key']),
                row['Underwriting_Rehab'], row['Rehab_Calculation'], row['Paint'],
                row['Flooring_Flag'], row['Foundation_Flag'], row['Roof_Flag'],
                row['HVAC_Flag'], row['Kitchen_Flag'], row['Bathroom_Flag'],
                row['Appliances_Flag'], row['Windows_Flag'], row['Landscaping_Flag'],
                row['Trashout_Flag']
            )
            cursor.execute(sql, values)
        connection.commit()
        logging.info(f"Inserted {len(rehab_df)} rehab estimates")

        # Insert Valuations
        for _, row in valuations_df.iterrows():
            sql = """
                INSERT INTO valuations (
                    property_id, list_price, previous_rent, arv, rent_zestimate,
                    low_fmr, high_fmr, redfin_value, zestimate, expected_rent
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                property_ids.get(row['address_key']),
                row['List_Price'], row['Previous_Rent'], row['ARV'],
                row['Rent_Zestimate'], row['Low_FMR'], row['High_FMR'],
                row['Redfin_Value'], row['Zestimate'], row['Expected_Rent']
            )
            cursor.execute(sql, values)
        connection.commit()
        logging.info(f"Inserted {len(valuations_df)} valuations")

    except Error as e:
        logging.error(f"Error in load_data: {e}")
        connection.rollback()
    finally:
        cursor.close()

def main():
    json_path = 'data/fake_property_data.json'
    excel_path = 'data/Field Config.xlsx'
    
    # Extract
    df, field_config = extract_data(json_path, excel_path)
    if df is None or field_config is None:
        logging.error("Extraction failed, exiting")
        return

    # Transform
    properties_df, hoa_df, rehab_df, valuations_df = transform_data(df, field_config)
    if properties_df is None:
        logging.error("Transformation failed, exiting")
        return

    # Load
    connection = connect_to_db()
    if connection:
        try:
            load_data(connection, properties_df, hoa_df, rehab_df, valuations_df)
        except Exception as e:
            logging.error(f"Error in main: {e}")
        finally:
            connection.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main()