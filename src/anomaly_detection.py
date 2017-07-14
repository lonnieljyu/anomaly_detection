from user import *
import sys
import json
import pandas 

"""
anomaly_detection.py
- Lonnie Yu

Usage: 
python anomaly_detection.py batch_log.json stream_log.json flagged_purchases.json

Description:
Detects user's anomalous purchases based on user's social network's purchase statistics.
A network's purchase history is fitted with a Gaussian distribution with a mean and standard deviation.
Reads batch purchase records and social network befriend/unfriend events.
Builds users' purchase history and social networks.
Reads stream events to check for anomalous purchases and updates record histories/social networks.
"""

### Begin Global Variables

NUMBER_OF_TRACKED_PURCHASES = 2 # T, the maximum sample size of tracked purchases in a social network

### End Global Variables

### Begin Methods
        
# Get network purchase history in a DataFrame
def Get_Network_Purchase_History(user):
    global USERS_DICT
    global NUMBER_OF_TRACKED_PURCHASES
    purchases_df = pandas.DataFrame()
    
    # Build history from all connections
    for connection_id in user.friends | user.distant_connections:
        connection = USERS_DICT[connection_id]
        purchases_df = purchases_df.append(connection.purchase_history.tail(NUMBER_OF_TRACKED_PURCHASES))
    return purchases_df.sort_index().tail(NUMBER_OF_TRACKED_PURCHASES)
            
# Calculate each user's network statistics
def Calculate_Network_Statistics(users=None):
    if not users:
        global USERS_DICT
        users = USERS_DICT.values()
        
    # Calculate mean and std of 'amount' column 
    for user in users:
        purchases_df = Get_Network_Purchase_History(user)
        if not purchases_df.empty:
            df = purchases_df['amount'].apply(pandas.to_numeric)
            user.network_mean = df.mean()
            user.network_std = df.std(ddof=0)
            
# Process batch log events from json input stream
def Process_Events_From_Batch_Log(input_stream):
    for line in input_stream:
        event_dictionary = json.loads(line)
        if event_dictionary['event_type'] == 'purchase': 
            Handle_Batch_Purchase_Event(event_dictionary)
        elif event_dictionary['event_type'] == 'befriend' \
            or event_dictionary['event_type'] == 'unfriend': 
            Handle_Friendship_Event(event_dictionary)
            
    Build_Distant_Connections()
    Calculate_Network_Statistics()
    
# Get connections with new network purchase history
def Get_Connections_With_New_Network_Purchase_History(user):
    global USERS_DICT
    global CURRENT_LOG_INDEX
    
    connections = list()
    for connection_id in user.friends | user.distant_connections:
        connection = USERS_DICT[connection_id]
        purchases_df = Get_Network_Purchase_History(connection)
        if CURRENT_LOG_INDEX in purchases_df.index:
            connections.append(connection)
    return connections
    
# Handle purchase event in stream log
def Handle_Stream_Purchase_Event(event_dictionary):
    global USERS_DICT
    user = USERS_DICT[event_dictionary['id']]
    connections = Get_Connections_With_New_Network_Purchase_History(user)
    Calculate_Network_Statistics(connections)
    
# Truncate floating point decimal to two decimals
def Truncate_Float(float):
    left, right = str(float).split('.')
    return '.'.join((left, right[0:2]))
    
# Check if purchase is anomalous
def Is_Anomalous_Purchase(event_dictionary):
    global USERS_DICT
    user = USERS_DICT[event_dictionary['id']]
    if user \
        and user.network_mean != 0 and user.network_std != 0 \
        and float(event_dictionary['amount']) > user.network_mean + user.network_std * 3:
        event_dictionary['mean'] = Truncate_Float(user.network_mean)
        event_dictionary['sd'] = Truncate_Float(user.network_std)
        return True
    return False
    
# Process stream log events from json input stream
# Write anomalous purchases in json to output stream
def Process_Events_From_Stream_Log(input_stream, output_stream):
    anomaly_list = list()
    for line in input_stream:
        event_dictionary = json.loads(line)
        
        # Case stream purchase event
        if event_dictionary['event_type'] == 'purchase':
            if Is_Anomalous_Purchase(event_dictionary):
                anomaly_list.append(json.dumps(event_dictionary))
                del event_dictionary['mean']
                del event_dictionary['sd']
            Handle_Batch_Purchase_Event(event_dictionary)        
            Handle_Stream_Purchase_Event(event_dictionary) 
                
        # Case stream friendship event
        elif event_dictionary['event_type'] == 'befriend' \
            or event_dictionary['event_type'] == 'unfriend':
            Handle_Friendship_Event(event_dictionary)
            Build_Distant_Connections()
            Calculate_Network_Statistics()
            
    output_stream.write('\n'.join(anomaly_list))
    
# Create input file stream
def Get_Input_File_Generator(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            yield line
            
# Extract 'D' and 'T' parameters from first line of batch_log.json
def Extract_Network_Parameters(input_file_generator):
    global NUMBER_OF_DEGREES
    global NUMBER_OF_TRACKED_PURCHASES
    parameters_dictionary = json.loads(input_file_generator.__next__())
    NUMBER_OF_DEGREES = int(parameters_dictionary['D'])
    NUMBER_OF_TRACKED_PURCHASES = int(parameters_dictionary['T'])
    
# Process batch_log.json for building data structures
def Process_Batch_Log(input_file_path): 
    input_file_generator = Get_Input_File_Generator(input_file_path)
    Extract_Network_Parameters(input_file_generator)
    Process_Events_From_Batch_Log(input_file_generator)
    
# Process stream_log.json for updating data structures and anomaly detection
def Process_Stream_Log(input_file_path, output_file_path):
    with open(output_file_path, 'w') as output_file:
        Process_Events_From_Stream_Log(Get_Input_File_Generator(input_file_path), output_file)
        
### End Methods

### Begin Main 

if __name__ == '__main__':
    Process_Batch_Log(sys.argv[1])
    Process_Stream_Log(sys.argv[2], sys.argv[3])
    
### End Main 