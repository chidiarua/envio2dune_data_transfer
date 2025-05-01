from dune_client import DuneClient
import os
from dotenv import load_dotenv

# Force reload of .env file
load_dotenv(override=True)

def delete_dune_table():
    client = DuneClient()
    
    # Get configuration from environment variables
    namespace = os.getenv('DUNE_NAMESPACE')
    table_name = os.getenv('DUNE_TABLE_NAME')
    
    # Debug logging
    print("Environment variables:")
    print(f"DUNE_NAMESPACE: {namespace}")
    print(f"DUNE_TABLE_NAME: {table_name}")
    
    if not namespace or not table_name:
        print("Error: DUNE_NAMESPACE or DUNE_TABLE_NAME not set in environment variables")
        return
    
    print(f"Deleting table {namespace}.{table_name}...")
    result = client.delete_table(
        namespace=namespace,
        table_name=table_name
    )
    
    if result:
        print(f"Successfully deleted table {namespace}.{table_name}")
    else:
        print(f"Failed to delete table {namespace}.{table_name}")

if __name__ == "__main__":
    delete_dune_table() 