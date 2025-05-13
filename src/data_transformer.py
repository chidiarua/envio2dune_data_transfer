from datetime import datetime

class DataTransformer:
    def transform_swaps(self, swaps):
        """
        Transform swap data from Envio format to Dune format
        swaps: List of swap dictionaries from Envio
        return: List of transformed swap dictionaries
        """
        print("\nDebug: Data Transformer")
        print(f"Number of swaps received: {len(swaps) if swaps else 0}")
        
        if not swaps:
            print("No swaps to transform")
            return []
            
        print("\nFirst swap from Envio:")
        print(f"Keys: {swaps[0].keys()}")
        print(f"Values: {swaps[0]}")
        
        transformed_swaps = []
        for swap in swaps:
            try:
                # Convert Unix timestamp to ISO format
                timestamp = int(swap["timeStamp"])
                iso_timestamp = datetime.fromtimestamp(timestamp).isoformat()
                
                transformed_swap = {
                    "id": swap["id"],
                    "from": swap["from"],
                    "token_in": swap["_tokenIn"],
                    "token_out": swap["_tokenOut"],
                    "amount_in": float(swap["_amountIn"]),
                    "amount_out": float(swap["_amountOut"]),
                    "timestamp": iso_timestamp
                }
                transformed_swaps.append(transformed_swap)
            except KeyError as e:
                print(f"Error transforming swap: Missing key {e}")
                print(f"Swap data: {swap}")
                continue
            except ValueError as e:
                print(f"Error converting values: {e}")
                print(f"Swap data: {swap}")
                continue
        
        print(f"\nTransformed {len(transformed_swaps)} swaps")
        if transformed_swaps:
            print("\nFirst transformed swap:")
            print(transformed_swaps[0])
            
        return transformed_swaps 