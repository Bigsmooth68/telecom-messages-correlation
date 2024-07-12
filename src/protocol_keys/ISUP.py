import sys, os
sys.path.insert(0, os.getcwd())
import src.db.db_redis as db
import json

def get_correlation_key(message: dict)-> dict:
    if 'key' in message.keys():
        return message['key']
    else:
        minPC = min(message['OPC'], message['DPC'])
        maxPC = max(message['OPC'], message['DPC'])
        CIC = message['CIC']
        result = f"{minPC}-{maxPC}-{CIC}"
        return (result)

def init_xdr(message: dict) -> dict:
    current_xdr = {}
    for field in ['OPC', 'DPC', 'CIC', 'A-Number', 'B-Number']:
        current_xdr[field] = message[field]
    return current_xdr

def process_message(message: dict) -> bool:

    # print(f'Message: {message}')
    # generate key
    key = get_correlation_key(message)

    current_xdr = db.find_xdr(key)

    if current_xdr is None:
        # We must create xDR
        current_xdr = init_xdr(message)
        if message['MessageType'] == 'IAM':
            current_xdr['begin_time'] = message['timestamp']
        else:
            # Unexpected behavior
            current_xdr['xdr_status'] = 'Unexpected Message'    
        
    else:
        current_xdr = json.loads(current_xdr)
        # We can update xDR
        if message['MessageType'] == 'IAM':
            current_xdr['xdr_status'] = 'dual_seizure'
            print('Unexpected message')
            return True
        if message['MessageType'] == 'ACM':
            pass
        elif message['MessageType'] == 'ANM':
            current_xdr['answer_time'] = message['timestamp'] - current_xdr['begin_time']
        elif message['MessageType'] == 'RLC':
            current_xdr['transaction_time'] = message['timestamp'] - current_xdr['begin_time']
            current_xdr['xdr_status'] = 'OK'
            return True

    # Store result
    print(f"{message['MessageType']} - {current_xdr}")
    db.set_xdr(key, json.dumps(current_xdr))

    return False

def close_xdr(key: str):
    xdr = db.find_xdr(key)
    db.remove_xdr(key)
    return json.dumps(xdr)
    
if __name__ == "__main__":
    temp_xdr = json.loads('{"timestamp": 12345,"OPC": 111,"DPC": 222,"CIC": 333,"MessageType": "IAM","A-Number": 1234567890,"B-Number": 9876543210}') 
    process_message(temp_xdr)
    temp_xdr = json.loads('{"timestamp": 12536,"OPC": 222,"DPC": 111,"CIC": 333,"MessageType": "ANM"}')
    process_message(temp_xdr)
    temp_xdr = json.loads('{"timestamp": 12539,"OPC": 222,"DPC": 111,"CIC": 333,"MessageType": "RLC"}')
    process_message(temp_xdr)