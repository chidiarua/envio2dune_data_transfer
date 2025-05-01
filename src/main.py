from envio_client import EnvioClient
from dune_client import DuneClient
from data_transformer import DataTransformer
import time
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    envio_client = EnvioClient()
    dune_client = DuneClient()
    transformer = DataTransformer()
    
    # Configuration
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 10000))
    print(f"Loaded BATCH_SIZE from .env: {BATCH_SIZE}")
    DUNE_NAMESPACE = os.getenv('DUNE_NAMESPACE')
    DUNE_TABLE_NAME = os.getenv('DUNE_TABLE_NAME', 'swaps')
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # seconds
    
    if not DUNE_NAMESPACE:
        print("Error: DUNE_NAMESPACE not set in environment variables")
        return

    # Define the schema for our swaps table
    schema = [
        {"name": "id", "type": "varchar"},
        {"name": "from", "type": "varchar"},
        {"name": "token_in", "type": "varchar"},
        {"name": "token_out", "type": "varchar"},
        {"name": "amount_in", "type": "double"},
        {"name": "amount_out", "type": "double"},
        {"name": "timestamp", "type": "timestamp"}
    ]

    # Check if table exists
    table_exists = dune_client.table_exists(DUNE_NAMESPACE, DUNE_TABLE_NAME)
    
    if not table_exists:
        print(f"Table {DUNE_NAMESPACE}.{DUNE_TABLE_NAME} does not exist. Creating...")
        # Create the table if it doesn't exist
        table_result = dune_client.create_table(
            namespace=DUNE_NAMESPACE,
            table_name=DUNE_TABLE_NAME,
            description="Swap data from Envio indexer",
            schema=schema
        )

        if not table_result:
            print("Error creating table in Dune")
            return

        print(f"Table {DUNE_NAMESPACE}.{DUNE_TABLE_NAME} created successfully")
        offset = 0  # Start from beginning if table is new
        latest_timestamp = None  # No previous data to compare against
        print(f"Starting with offset: {offset} (new table)")
    else:
        print(f"Table {DUNE_NAMESPACE}.{DUNE_TABLE_NAME} already exists")
        # Get the latest transaction data from Dune
        latest_id, latest_timestamp = dune_client.get_latest_id(DUNE_NAMESPACE, DUNE_TABLE_NAME)
        if latest_timestamp:
            print(f"Latest data in Dune - ID: {latest_id}, Timestamp: {latest_timestamp}")
            # Start from beginning to find new transactions
            offset = 0
            print(f"Starting from beginning to find new transactions after timestamp: {latest_timestamp}")
        else:
            print("No existing data found, starting from beginning")
            offset = 0
            latest_timestamp = None
            print(f"Starting with offset: {offset} (no existing data)")
    
    consecutive_empty_responses = 0
    MAX_EMPTY_RESPONSES = 3  # Number of empty responses before we assume we're done
    processed_hashes = set()  # Keep track of processed transaction hashes
    
    while True:
        # Fetch swaps from Envio with retry logic
        retries = 0
        while retries < MAX_RETRIES:
            try:
                print(f"\nFetching swaps from Envio with offset: {offset}, limit: {BATCH_SIZE}")
                swaps = envio_client.get_swaps(limit=BATCH_SIZE, offset=offset)
                if swaps is not None:  # Valid response received
                    print(f"Fetched {len(swaps)} swaps from Envio")
                    if swaps:
                        print(f"First swap ID: {swaps[0]['id']}, Last swap ID: {swaps[-1]['id']}")
                        print(f"First swap timestamp: {swaps[0]['timeStamp']}, Last swap timestamp: {swaps[-1]['timeStamp']}")
                    break
                retries += 1
                if retries < MAX_RETRIES:
                    print(f"Empty response from Envio, retrying in {RETRY_DELAY} seconds... (Attempt {retries + 1}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY)
            except Exception as e:
                retries += 1
                if retries < MAX_RETRIES:
                    print(f"Error fetching from Envio: {e}")
                    print(f"Retrying in {RETRY_DELAY} seconds... (Attempt {retries + 1}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"Failed to fetch from Envio after {MAX_RETRIES} attempts")
                    return
        
        if not swaps:
            consecutive_empty_responses += 1
            if consecutive_empty_responses >= MAX_EMPTY_RESPONSES:
                print(f"No more swaps to process after {MAX_EMPTY_RESPONSES} consecutive empty responses")
                break
            print(f"Empty response from Envio ({consecutive_empty_responses}/{MAX_EMPTY_RESPONSES}), retrying...")
            time.sleep(RETRY_DELAY)
            continue
        else:
            consecutive_empty_responses = 0  # Reset counter on successful response
            
        # Filter out already processed transactions and older transactions
        new_swaps = []
        for swap in swaps:
            swap_id = swap['id']
            swap_timestamp = int(swap['timeStamp'])
            
            # Skip if we've already processed this transaction
            if swap_id in processed_hashes:
                print(f"Skipping already processed transaction: {swap_id}")
                continue
                
            # Skip if this transaction is older than our latest timestamp
            if latest_timestamp and swap_timestamp <= int(latest_timestamp):
                print(f"Skipping older transaction: {swap_id} (timestamp: {swap_timestamp})")
                continue
                
            new_swaps.append(swap)
            processed_hashes.add(swap_id)  # Mark this hash as processed
        
        if not new_swaps:
            print("No new transactions found in this batch, moving to next batch")
            offset += BATCH_SIZE
            continue
            
        # Transform the data
        transformed_data = transformer.transform_swaps(new_swaps)
        print(f"Transformed {len(transformed_data)} swaps")
        if transformed_data:
            print(f"First transformed ID: {transformed_data[0]['id']}, Last transformed ID: {transformed_data[-1]['id']}")
        
        # Upload to Dune using SQL INSERT
        print(f"Uploading {len(transformed_data)} records to Dune with batch_size: {BATCH_SIZE}")
        result = dune_client.upload_data(
            namespace=DUNE_NAMESPACE,
            table_name=DUNE_TABLE_NAME,
            data=transformed_data,
            batch_size=BATCH_SIZE
        )
        
        if result:
            print(f"Successfully uploaded {len(transformed_data)} swaps to Dune")
            # Update offset by batch size for next iteration
            offset += BATCH_SIZE
            print(f"Updated offset to: {offset} (incremented by batch size)")
        else:
            print("Failed to upload data to Dune")
            time.sleep(RETRY_DELAY)  # Wait before retrying
            continue  # Don't increment offset, try the same batch again
            
        time.sleep(1)  # Rate limiting

if __name__ == "__main__":
    main() 