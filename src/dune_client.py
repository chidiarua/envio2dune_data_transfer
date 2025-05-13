import requests
import os
from dotenv import load_dotenv
import time
import csv
import io
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

class DuneClient:
    def __init__(self):
        self.api_key = os.getenv('DUNE_API_KEY')
        self.base_url = "https://api.dune.com/api/v1"
        self.batch_size = int(os.getenv('BATCH_SIZE', 10000))  # Default to 10000 if not set
        print(f"DuneClient initialized with batch_size: {self.batch_size}")
        self.headers = {
            "X-DUNE-API-KEY": self.api_key,
            "Content-Type": "application/json"
        }
        # Configure retry strategy
        self.session = requests.Session()
        retries = Retry(
            total=5,  # number of retries
            backoff_factor=1,  # wait 1, 2, 4, 8, 16 seconds between retries
            status_forcelist=[500, 502, 503, 504, 404, 429]  # HTTP status codes to retry on
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def create_table(self, namespace, table_name, description, schema, is_private=False):
        """
        Create a new table in Dune Analytics
        :param namespace: Your Dune username
        :param table_name: Name of the table
        :param description: Description of the table
        :param schema: List of column definitions
        :param is_private: Whether the table is private
        :return: Response from Dune API
        """
        endpoint = f"{self.base_url}/table/create"
        
        payload = {
            "namespace": namespace,
            "table_name": table_name,
            "description": description,
            "schema": schema,
            "is_private": is_private
        }
        
        try:
            response = self.session.post(
                endpoint,
                headers=self.headers,
                json=payload,
                timeout=30  # 30 second timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error creating table in Dune: {e}")
            return None

    def create_insert_query(self, namespace, table_name, data):
        """
        Create a query to insert data into a table
        :param namespace: Your Dune username
        :param table_name: Name of the table
        :param data: List of dictionaries containing the data to insert
        :return: Query ID if successful, None otherwise
        """
        endpoint = f"{self.base_url}/query/create"
        
        # Create SQL INSERT statement
        values = []
        for row in data:
            value_str = f"('{row['id']}', '{row['from']}', '{row['token_in']}', '{row['token_out']}', {row['amount_in']}, {row['amount_out']}, '{row['timestamp']}')"
            values.append(value_str)
        
        insert_query = f"""
        INSERT INTO {namespace}.{table_name} 
        (id, from, token_in, token_out, amount_in, amount_out, timestamp)
        VALUES {','.join(values)}
        """
        
        payload = {
            "name": f"Insert data into {table_name}",
            "query": insert_query,
            "parameters": {}
        }
        
        try:
            response = requests.post(
                endpoint,
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            return response.json().get('query_id')
        except requests.exceptions.RequestException as e:
            print(f"Error creating insert query: {e}")
            return None

    def execute_query(self, query_id):
        """
        Execute a query by ID
        :param query_id: The ID of the query to execute
        :return: Response from Dune API
        """
        endpoint = f"{self.base_url}/query/{query_id}/execute"
        
        try:
            response = requests.post(
                endpoint,
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error executing query: {e}")
            return None

    def upload_data(self, namespace, table_name, data, batch_size=None):
        """
        Upload data to Dune Analytics using CSV format with retry logic for rate limits
        :param namespace: Your Dune username
        :param table_name: Name of the table
        :param data: List of dictionaries containing the data to upload
        :param batch_size: Optional batch size for chunking. If not provided, uses the value from .env
        :return: Response from Dune API
        """
        endpoint = f"{self.base_url}/table/{namespace}/{table_name}/insert"
        
        # Debug logging for input data
        print("\nDebug: Data being uploaded to Dune")
        print(f"Number of records: {len(data)}")
        if data:
            print("\nFirst record structure:")
            print(f"Keys: {data[0].keys()}")
            print(f"Values: {data[0]}")
            print("\nSample of first record values:")
            for key, value in data[0].items():
                print(f"{key}: {value} (type: {type(value)})")
        
        # Validate data structure
        required_fields = ['id', 'from', 'token_in', 'token_out', 'amount_in', 'amount_out', 'timestamp']
        if data and not all(field in data[0] for field in required_fields):
            missing_fields = [field for field in required_fields if field not in data[0]]
            print(f"Error: Missing required fields in data: {missing_fields}")
            return None
        
        # Create CSV in memory
        output = io.StringIO()
        if data:
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        
        # Debug logging for CSV data
        csv_data = output.getvalue()
        print("\nDebug: First 500 characters of CSV data:")
        print(csv_data[:500])
        
        # Update headers for CSV upload
        headers = {
            "X-DUNE-API-KEY": self.api_key,
            "Content-Type": "text/csv"
        }
        
        try:
            # Use provided batch_size or fall back to the one from .env
            chunk_size = batch_size if batch_size is not None else self.batch_size
            print(f"\nDune upload using chunk_size: {chunk_size}")
            total_records = len(data)
            results = []
            
            for i in range(0, total_records, chunk_size):
                chunk = data[i:i + chunk_size]
                print(f"\nUploading chunk {i//chunk_size + 1} of {(total_records-1)//chunk_size + 1} ({len(chunk)} records)...")
                
                # Create CSV for this chunk
                chunk_output = io.StringIO()
                writer = csv.DictWriter(chunk_output, fieldnames=chunk[0].keys())
                writer.writeheader()
                writer.writerows(chunk)
                
                chunk_csv = chunk_output.getvalue()
                print(f"Chunk CSV preview (first 200 chars):\n{chunk_csv[:200]}")
                
                # Retry logic for rate limits
                max_retries = 5  # Maximum number of retries
                base_delay = 5  # Base delay in seconds
                retry_count = 0
                success = False
                
                while not success and retry_count < max_retries:
                    try:
                        response = self.session.post(
                            endpoint,
                            headers=headers,
                            data=chunk_csv.encode('utf-8'),
                            timeout=120  # 120 second timeout for larger chunks
                        )
                        
                        if response.status_code == 200:
                            success = True
                            results.append(response.json())
                            print(f"Successfully uploaded chunk {i//chunk_size + 1}")
                        else:
                            response.raise_for_status()
                            
                    except requests.exceptions.RequestException as e:
                        error_message = str(e)
                        if "too many requests" in error_message.lower():
                            retry_count += 1
                            if retry_count < max_retries:
                                # Calculate exponential backoff delay
                                delay = base_delay * (2 ** (retry_count - 1))
                                print(f"Rate limit hit. Retrying in {delay} seconds... (Attempt {retry_count + 1}/{max_retries})")
                                time.sleep(delay)
                            else:
                                print(f"Max retries reached for chunk {i//chunk_size + 1}. Moving to next chunk.")
                                break
                        else:
                            print(f"Error uploading chunk {i//chunk_size + 1}: {e}")
                            break
                
                # Add a small delay between chunks to avoid rate limiting
                if i + chunk_size < total_records:
                    time.sleep(2)
            
            return results
        except requests.exceptions.RequestException as e:
            print(f"\nError uploading data to Dune: {e}")
            if hasattr(e.response, 'text'):
                print(f"Error response: {e.response.text}")
            return None

    def delete_table(self, namespace, table_name):
        """
        Delete a table from Dune Analytics
        :param namespace: Your Dune username
        :param table_name: Name of the table to delete
        :return: Response from Dune API
        """
        endpoint = f"{self.base_url}/table/{namespace}/{table_name}"
        
        try:
            response = self.session.delete(
                endpoint,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error deleting table in Dune: {e}")
            return None

    def table_exists(self, namespace, table_name):
        """
        Check if a table exists in Dune Analytics
        :param namespace: Your Dune username
        :param table_name: Name of the table
        :return: True if table exists, False otherwise
        """
        endpoint = f"{self.base_url}/table/{namespace}/{table_name}"
        
        try:
            response = self.session.get(
                endpoint,
                headers=self.headers,
                timeout=30
            )
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False

    def get_latest_id(self, namespace, table_name):
        """
        Get the latest transaction hash and timestamp from a Dune table
        :param namespace: Your Dune username
        :param table_name: Name of the table
        :return: Tuple of (latest transaction hash, latest timestamp) or (None, None) if table is empty
        """
        query = f"""
        SELECT id as latest_id, timestamp as latest_timestamp
        FROM {namespace}.{table_name}
        ORDER BY timestamp DESC
        LIMIT 1
        """
        
        endpoint = f"{self.base_url}/query/execute"
        payload = {
            "query": query,
            "parameters": {}
        }
        
        try:
            response = self.session.post(
                endpoint,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            
            # Extract results from the response
            if 'result' in result and 'rows' in result['result'] and result['result']['rows']:
                row = result['result']['rows'][0]
                latest_id = row.get('latest_id')
                latest_timestamp = row.get('latest_timestamp')
                return latest_id, latest_timestamp
            return None, None
        except requests.exceptions.RequestException as e:
            print(f"Error getting latest transaction data from Dune: {e}")
            return None, None 