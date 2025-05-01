from datetime import datetime

class DataTransformer:
    @staticmethod
    def transform_swaps(swaps):
        """
        Transform swap data from Envio format to Dune format
        :param swaps: List of swap dictionaries from Envio
        :return: List of transformed swap dictionaries for Dune
        """
        transformed_data = []
        
        for swap in swaps:
            # Convert timestamp to datetime
            timestamp = datetime.fromtimestamp(int(swap['timeStamp']))
            
            transformed_swap = {
                'id': str(swap['id']),  # Ensure VARCHAR
                'from': str(swap['from']),  # Ensure VARCHAR
                'token_in': str(swap['_tokenIn']),  # Ensure VARCHAR
                'token_out': str(swap['_tokenOut']),  # Ensure VARCHAR
                'amount_in': float(swap['_amountIn']),  # Convert to DOUBLE
                'amount_out': float(swap['_amountOut']),  # Convert to DOUBLE
                'timestamp': timestamp.isoformat()  # TIMESTAMP format
            }
            
            transformed_data.append(transformed_swap)
        
        return transformed_data 