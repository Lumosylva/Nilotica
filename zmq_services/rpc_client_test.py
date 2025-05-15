import pprint
import time

import zmq

# Import necessary VNPY enums
from vnpy.trader.constant import Direction, Exchange, Offset, OrderType

# Gateway REP server address (replace localhost if server is on a different machine)
SERVER_REP_ADDRESS = "tcp://localhost:5558"

# Create ZMQ context and REQ socket
context = zmq.Context()
socket = context.socket(zmq.REQ)

# Set socket options for reliability
socket.setsockopt(zmq.LINGER, 0) # Discard pending messages on close
socket.setsockopt(zmq.RCVTIMEO, 3000) # Set receive timeout to 3 seconds
socket.setsockopt(zmq.SNDTIMEO, 3000) # Set send timeout to 3 seconds

print(f"Connecting to Order Execution Gateway at {SERVER_REP_ADDRESS}...")
socket.connect(SERVER_REP_ADDRESS)

# --- Test 1: Ping --- 
print("\nSending ping request...")
try:
    request = ("ping", [], {})
    socket.send_pyobj(request)
    reply = socket.recv_pyobj()
    print("Received ping reply:")
    pprint.pprint(reply)
except zmq.Again:
    print("Error: Timeout waiting for ping reply.")
except Exception as e:
    print(f"Error during ping: {e}")

time.sleep(0.5) # Short pause between requests

# --- Test 2: Query Account --- 
print("\nSending query_account request...")
try:
    # query_account takes no arguments
    request = ("query_account", [], {})
    socket.send_pyobj(request)
    
    # Receive the reply
    reply = socket.recv_pyobj()
    
    print("Received query_account reply:")
    # reply structure is [success_bool, result_or_error]
    if isinstance(reply, list) and len(reply) == 2:
        success, result = reply
        if success:
            print("Request successful. Account data:")
            pprint.pprint(result)
        else:
            print("Request failed on server. Error:")
            print(result) # Print traceback string
    else:
        print("Received unexpected reply format:")
        pprint.pprint(reply)

except zmq.Again:
    print("Error: Timeout waiting for query_account reply.")
    print("This might happen if the CTP connection is not ready or failed.")
except Exception as e:
    print(f"An error occurred during query_account: {e}")

time.sleep(0.5)

# --- Test 3: Send Order --- 
print("\nSending send_order request...")

# !!! MODIFY THESE PARAMETERS FOR YOUR TEST !!!
symbol = "SA509"         # Example: User specified contract
exchange = Exchange.CZCE     # Example: User specified exchange (Use Enum directly)
price = 1364.0           # Example: User specified price
volume = 1               # Example: Order volume
direction = Direction.LONG  # Example: Buy (Use Enum directly)
order_type = OrderType.LIMIT      # Example: Limit order (Use Enum directly)
offset = Offset.OPEN        # Example: Open position (Use Enum directly)
# !!! ------------------------------------ !!!

# Construct the dictionary using enum values
order_req_dict = {
    "symbol": symbol,
    "exchange": exchange.value,  # Send the enum value
    "direction": direction.value, # Send the enum value
    "type": order_type.value,   # Send the enum value
    "volume": volume,
    "price": price,
    "offset": offset.value,     # Send the enum value
    "reference": "rpc_client_test_2" # Optional: Updated reference
}

print(f"Constructed order request (using enum values):")
pprint.pprint(order_req_dict)

try:
    # send_order expects the order dictionary as the first argument
    request = ("send_order", [order_req_dict], {})
    socket.send_pyobj(request)
    
    # Receive the reply
    reply = socket.recv_pyobj()
    
    print("Received send_order reply:")
    # Expected successful reply: [True, vt_orderid_string]
    if isinstance(reply, list) and len(reply) == 2:
        success, result = reply
        if success and result is not None: # Check for non-None result too
            print(f"Request successful. VT OrderID: {result}")
        elif success and result is None:
            print("Request reported success by RPC, but Gateway returned None.")
            print("  (This likely means the order failed validation or sending at the Gateway level)")
            print("  (Check server logs for specific CTP errors like invalid contract, price, time, etc.)")
        else:
            print("Request failed on server. Error:")
            print(result) # Print traceback or error message string
    else:
        print("Received unexpected reply format:")
        pprint.pprint(reply)

except zmq.Again:
    print("Error: Timeout waiting for send_order reply.")
except Exception as e:
    print(f"An error occurred during send_order: {e}")

# --- Test 4: Query Contracts (Optional) --- 
# print("\nSending query_contracts request...")
# try:
#     request = ("query_contracts", [], {})
#     socket.send_pyobj(request)
#     reply = socket.recv_pyobj()
#     print("Received query_contracts reply:")
#     if isinstance(reply, list) and len(reply) == 2:
#         success, result = reply
#         if success:
#             print(f"Request successful. Found {len(result)} contracts.")
#             # pprint.pprint(result) # Uncomment to print all contracts
#         else:
#             print("Request failed on server. Error:")
#             print(result)
#     else:
#         print("Received unexpected reply format:")
#         pprint.pprint(reply)
# except zmq.Again:
#     print("Error: Timeout waiting for query_contracts reply.")
# except Exception as e:
#     print(f"An error occurred during query_contracts: {e}")

# Clean up
print("\nClosing connection.")
socket.close()
context.term()

print("Test finished.")
