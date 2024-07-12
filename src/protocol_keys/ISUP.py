import sys, os
sys.path.insert(0, os.getcwd())
import src.db.db_redis as db
import json
import src.custom_logger.custom_logger as custom_logger

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
    for field in ['OPC', 'DPC', 'CIC', 'A_number', 'B_number']:
        current_xdr[field] = message[field]
    return current_xdr

def process_message(message: dict) -> bool:

    logger = custom_logger.custom_logger(__name__)
    close = False

    # generate key
    key = get_correlation_key(message)

    current_xdr = db.find_xdr(key)

    if current_xdr is None:
        # We must create xDR
        if message['MessageType'] == 'IAM':
            current_xdr = init_xdr(message)
            current_xdr['begin_time'] = message['timestamp']
        else:
            # Unexpected behavior
            current_xdr = message
            current_xdr['xdr_status'] = 'Unexpected Message'
        
    else:
        current_xdr = json.loads(current_xdr)
        # We can update xDR
        if message['MessageType'] == 'IAM':
            current_xdr['xdr_status'] = 'dual_seizure'
            logger.warning('Unexpected message')
            close = True
        if message['MessageType'] == 'ACM':
            pass
        elif message['MessageType'] == 'ANM':
            try:
                current_xdr['answer_time'] = message['timestamp'] - current_xdr['begin_time']
            except KeyError:
                current_xdr['xdr_status'] = 'Error'
                close = True
        elif message['MessageType'] == 'RLC':
            try:
                current_xdr['transaction_time'] = message['timestamp'] - current_xdr['begin_time']
            except KeyError:
                current_xdr['xdr_status'] = 'Error'
                close = True
            current_xdr['xdr_status'] = 'OK'
            close = True

    # Store result
    logger.debug(f"{message['MessageType']} - {current_xdr}")
    db.set_xdr(key, json.dumps(current_xdr))

    return close

def close_xdr(message: str):
    key = get_correlation_key(message)
    xdr = db.find_xdr(key)
    db.remove_xdr(key)
    return json.dumps(xdr)
    
if __name__ == "__main__":
    temp_xdr = json.loads('{"timestamp": 12345,"OPC": 111,"DPC": 222,"CIC": 333,"MessageType": "IAM","A_number": 1234567890,"B_number": 9876543210}') 
    process_message(temp_xdr)
    temp_xdr = json.loads('{"timestamp": 12536,"OPC": 222,"DPC": 111,"CIC": 333,"MessageType": "ANM"}')
    process_message(temp_xdr)
    temp_xdr = json.loads('{"timestamp": 12539,"OPC": 222,"DPC": 111,"CIC": 333,"MessageType": "RLC"}')
    process_message(temp_xdr)