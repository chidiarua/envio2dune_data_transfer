from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import os
from dotenv import load_dotenv

# Print current working directory to verify .env location
print(f"Current working directory: {os.getcwd()}")

# Load environment variables
load_dotenv()

# Print all environment variables for debugging
print("Environment variables:")
print(f"ENVIO_GRAPHQL_URL: {os.getenv('ENVIO_GRAPHQL_URL')}")
print(f"BATCH_SIZE: {os.getenv('BATCH_SIZE')}")

class EnvioClient:
    def __init__(self):
        graphql_url = os.getenv('ENVIO_GRAPHQL_URL')
        if not graphql_url:
            raise ValueError("ENVIO_GRAPHQL_URL not set in environment variables")
            
        print(f"Initializing EnvioClient with GraphQL URL: {graphql_url}")
        
        # Verify the URL format
        if not graphql_url.startswith('https://'):
            raise ValueError(f"Invalid GraphQL URL format: {graphql_url}")
            
        transport = RequestsHTTPTransport(
            url=graphql_url,
            headers={},
            verify=True,
            retries=3  # Add retries for better reliability
        )
        self.client = Client(transport=transport, fetch_schema_from_transport=True)
        self.batch_size = int(os.getenv('BATCH_SIZE', 10000))  # Default to 10000 if not set

    def get_swaps(self, limit=None, offset=0):
        """
        Get swaps from Envio
        param limit: Number of swaps to fetch. If None, uses BATCH_SIZE from .env
        param offset: Offset for pagination
        return: List of swaps or None if error
        """
        # Use BATCH_SIZE from .env if limit is not specified
        if limit is None:
            limit = self.batch_size
            
        query = gql("""
            query GetSwaps($limit: Int!, $offset: Int!) {
                Swap(limit: $limit, offset: $offset) {
                    id
                    timeStamp
                    _tokenIn
                    _tokenOut
                    _amountIn
                    _amountOut
                    from
                }
            }
        """)
        
        variables = {
            "limit": limit,
            "offset": offset
        }
        
        try:
            print(f"\nFetching swaps from Envio:")
            print(f"URL: {self.client.transport.url}")
            print(f"Limit: {limit}, Offset: {offset}")
            
            result = self.client.execute(query, variable_values=variables)
            swaps = result.get('Swap', [])
            
            if not swaps and offset == 0:
                print("Warning: No swaps found at offset 0. This might indicate a connection issue.")
            else:
                print(f"Successfully fetched {len(swaps)} swaps")
                if swaps:
                    print(f"First swap ID: {swaps[0]['id']}")
                    print(f"Last swap ID: {swaps[-1]['id']}")
            
            return swaps
        except Exception as e:
            print(f"Error fetching swaps from Envio: {e}")
            if hasattr(e, 'response'):
                print(f"Response status: {e.response.status_code if hasattr(e.response, 'status_code') else 'N/A'}")
                print(f"Response text: {e.response.text if hasattr(e.response, 'text') else 'N/A'}")
            return None  # Return None on error instead of empty list 