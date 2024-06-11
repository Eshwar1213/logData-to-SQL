import datetime
import pandas as pd


def get_msg_id(data):
    joined_data = " ".join(data)
    cleaned_data = joined_data.replace('[', '').replace(']', '')
    values = cleaned_data.split(",")
    second_value = values[1].strip()
    return second_value.strip('"')

def timestamp_to_time(timestamp):
    timestamp_datetime = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    time_str = timestamp_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return time_str

def get_call_action(data, msg_id):
    joined_data = " ".join(data)
    cleaned_data = joined_data.replace('[', '').replace(']', '')
    values = cleaned_data.split(",")
    third_value = values[2].strip()
    return third_value.strip('"')


def get_call_PayLoad(data):
    joined_data = " ".join(data)
    return joined_data


def cp_id_valid(cp_id):
    if ":" in cp_id:
        modified_line = cp_id.replace(":", "")
    else:
        modified_line = cp_id
    return modified_line


def message_flow(flow):
    if flow == '->' or flow == '|->':
        return 'CP - CMS'
    elif flow == '<-' or flow == '<-|':
        return 'CMS - CP'
    

def transform_log(cleaned_data):

    msg_id_actions = {}  
    temp_data = []
    df_data = []

    for line in cleaned_data:
        line_split = line.split()
        log_first = line_split[:10]  # first list up to index 9
        log_last = line_split[10:]  # last list after index 9 array

        cp_id = cp_id_valid(log_first[9])
        real_time = timestamp_to_time(log_first[0])
        received_time = timestamp_to_time(log_first[1])
        msg_flow = message_flow(log_first[8])
        msg_id = get_msg_id(log_last)
        call_action = get_call_action(log_last, msg_id)
        call_payLoad = get_call_PayLoad(log_last)

        if not call_action.isalpha():
            if msg_id in msg_id_actions:
                call_action = msg_id_actions[msg_id] 
            else:
                call_action = 'Unknown'
        else:
            msg_id_actions[msg_id] = call_action


        temp_data.append([cp_id, real_time, received_time, msg_id, msg_flow, call_action, call_payLoad])

   

    for entry in temp_data:
        cp_id, real_time, received_time, msg_id, msg_flow, call_action, call_payLoad = entry

        if call_action == 'Unknown':

            if msg_id in msg_id_actions:
                call_action = msg_id_actions[msg_id] 
        

        df_data.append([cp_id, real_time, received_time, msg_id, msg_flow, call_action, call_payLoad])

    
    
    return df_data