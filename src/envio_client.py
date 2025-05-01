from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import os
from dotenv import load_dotenv

load_dotenv()

class EnvioClient:
    def __init__(self):
        transport = RequestsHTTPTransport(
            url=os.getenv('ENVIO_GRAPHQL_URL'),
            headers={},
            verify=True,
        )
        self.client = Client(transport=transport, fetch_schema_from_transport=True)

    def get_swaps(self, limit=100, offset=0):
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
            result = self.client.execute(query, variable_values=variables)
            swaps = result.get('Swap', [])
            if not swaps and offset == 0:
                print("Warning: No swaps found at offset 0. This might indicate a connection issue.")
            return swaps
        except Exception as e:
            print(f"Error fetching swaps from Envio: {e}")
            return None  # Return None on error instead of empty list 