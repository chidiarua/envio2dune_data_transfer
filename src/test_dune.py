#Testing table creation and data insertion
#This is a test to ensure that the table creation and data insertion is working correctly   

from dune_client import DuneClient
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def test_table_creation_and_data_insertion():
    client = DuneClient()
    
    # Test configuration
    namespace = os.getenv('DUNE_NAMESPACE')
    table_name = "test_swaps"
    
    # Define schema
    schema = [
        {"name": "id", "type": "varchar"},
        {"name": "from_address", "type": "varchar"},
        {"name": "token_in", "type": "varchar"},
        {"name": "token_out", "type": "varchar"},
        {"name": "amount_in", "type": "double"},
        {"name": "amount_out", "type": "double"},
        {"name": "timestamp", "type": "timestamp"}
    ]
    
    # Try to create table if it doesn't exist
    try:
        print("Attempting to create table if it doesn't exist...")
        result = client.create_table(
            namespace=namespace,
            table_name=table_name,
            description="Test table for swap data",
            schema=schema
        )
        if result:
            print("Table created successfully")
        else:
            print("Table might already exist, continuing with data upload...")
    except Exception as e:
        print(f"Note: {str(e)}")
        print("Continuing with data upload...")
    
    # Test data - this would be your actual data source
    # For demonstration, showing how to filter out records with IDs
    test_data = [
        {
            "id": "1",  # This record would be skipped
            "from_address": "0x123...abc",
            "token_in": "ETH",
            "token_out": "USDC",
            "amount_in": 1.5,
            "amount_out": 2800.0,
            "timestamp": datetime.now().isoformat()
        },
        {
            "from_address": "0x456...def",  # This record would be uploaded (no ID)
            "token_in": "USDC",
            "token_out": "ETH",
            "amount_in": 1000.0,
            "amount_out": 0.5,
            "timestamp": datetime.now().isoformat()
        }
    ]
    
    # Filter out records that already have IDs
    new_records = [record for record in test_data if 'id' not in record]
    
    if not new_records:
        print("No new records to upload - all records already have IDs")
        return
        
    print(f"\nUploading {len(new_records)} new records...")
    upload_result = client.upload_data(
        namespace=namespace,
        table_name=table_name,
        data=new_records
    )
    print("Data upload result:", upload_result)

if __name__ == "__main__":
    test_table_creation_and_data_insertion() 