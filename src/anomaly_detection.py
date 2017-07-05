import sys
import json
from pandas import DataFrame

"""
anomaly_detection.py
- Lonnie Yu

Usage: 
python anomaly_detection.py batch_log.json stream_log.json

Description:
Detects user's anomalous purchases based on user's social network's purchase statistics.
A network's purchase history is fitted with a Gaussian distribution with a mean and standard deviation.
Reads batch purchase records and social network friend/unfriend events.
Builds users' purchase history and social networks.
Reads stream events to check for anomalous purchases and updates record histories/social networks.
"""

### Begin Global Variables

NUMBER_OF_DEGREES = 1           # D, the maximum number of degrees of any connection in a social network
NUMBER_OF_TRACKED_PURCHASES = 2 # T, the maximum sample size of tracked purchases in a social network
CURRENT_LOG_INDEX = -1          # Used for assigning each purchase event a log index
USERS_DICT = dict()             # Dictionary of (user id, User) pairs

### End Global Variables

### Begin Classes

class User:
# Contains a user id, set of friends, set of distant connections, network's mean and standard deviation (std), and DataFrame of purchase history.
# Friends are defined as network connections of 1 degree.
# Distant connections are defined as network connections of 2 to D degrees.
# All connections are bidirectional.
# Each row of the purchase history contains one purchase event information indexed by log index.
# A DataFrame is a Pandas data structure for tabular data with easy slicing and sorting functionality.

    def __init__(self, id):
        self.id = id
        self.network_mean = 0
        self.network_std = 0
        self.friends = set()
        self.distant_connections = set()
        self.purchase_history = DataFrame()
        
    def Add_Purchase(self, event_dictionary):
        global CURRENT_LOG_INDEX
        CURRENT_LOG_INDEX += 1
        new_data_frame = DataFrame(data=[event_dictionary], index=[CURRENT_LOG_INDEX])
        self.purchase_history = self.purchase_history.append(new_data_frame)
        
    def Add_Friend(self, friend):
        self.friends.add(friend)
        
    def Remove_Friend(self, not_friend):
        self.friends.remove(not_friend)
        
    def Add_Distant_Connection(self, distant_connection):
        self.distant_connections.add(distant_connection)
        
    def Remove_Distant_Connection(self, not_distant_connection):
        self.distant_connections.remove(not_distant_connection)
        
    # Prints class contents for debugging
    def print(self):
        print(  "User Id:", self.id, 
                "Network Mean:", self.network_mean,
                "Network Standard Deviation:", self.network_std, "\n"
                "Friends:", self.friends, "\n"
                "Distant connections:", self.distant_connections, "\n"
                "Purchase history:\n", self.purchase_history, "\n")
        
### End Classes

### Begin Main Methods

# Convert timestamp formatted string to datetime object
def Convert_To_Datetime(timestamp):
    pass
    
# Create user if not found in USERS_DICT
def Verify_User_In_Users(user_id):
    global USERS_DICT
    if user_id not in USERS_DICT:
        USERS_DICT[user_id] = User(user_id)
        
# Verifify multiple users
def Verify_Users_In_Users(user_ids):
    for user_id in user_ids:
        Verify_User_In_Users(user_id)
        
# Verify user existance and add purchase to purchase history
def Handle_Purchase_Event(event_dictionary):
    global USERS_DICT
    user_id = event_dictionary['id']
    Verify_User_In_Users(user_id)            
    USERS_DICT[user_id].Add_Purchase(event_dictionary)
    
# Add friendship connection for both users
def Handle_Befriend_Event(user_id1, user_id2):
    global USERS_DICT
    USERS_DICT[user_id1].Add_Friend(user_id2)
    USERS_DICT[user_id2].Add_Friend(user_id1)
    
# Remove friendship connection for both users
def Handle_Unfriend_Event(user_id1, user_id2):
    global USERS_DICT
    USERS_DICT[user_id1].Remove_Friend(user_id2)
    USERS_DICT[user_id2].Remove_Friend(user_id1)
    
# Depth-Limited Depth-First Search recursive function
def DLS(user_id, depth, visited_ids, user_ids):
    visited_ids.add(user_id)
    global NUMBER_OF_DEGREES
    if depth < NUMBER_OF_DEGREES - 1: user_ids.add(user_id)
    if depth == 0: return
    global USERS_DICT
    for friend_id in USERS_DICT[user_id].friends - visited_ids:
        DLS(friend_id, depth-1, visited_ids, user_ids)
    
# Build every user's distant connections
def Build_Distant_Connections():
    global USERS_DICT
    global NUMBER_OF_DEGREES
    for user in USERS_DICT.values():
        user.distant_connections = set()
        DLS(user.id, NUMBER_OF_DEGREES, set(), user.distant_connections)
        
# Calculate each user's network statistics
def Calculate_Network_Statistics():
    global USERS_DICT
    global NUMBER_OF_DEGREES
    global NUMBER_OF_TRACKED_PURCHASES
    
    for user in USERS_DICT.values():
        user.print()
        network_ids = user.friends | user.distant_connections
        print("network_ids", network_ids)
        
        # build network purchase history
        # sort by latest timestamp, then greatest log index
        # find mean and std on 'amount' column 
    
# Process batch_log.json events
def Process_Events_From_Batch_Log(input_stream):
    for line in input_stream:
        event_dictionary = json.loads(line)
        print(event_dictionary)
        
        # Case purchase event (no network changes)
        if event_dictionary['event_type'] == 'purchase':
            Handle_Purchase_Event(event_dictionary)
            
        # Case friendship event
        elif event_dictionary['event_type'] == 'befriend' \
            or event_dictionary['event_type'] == 'unfriend':
            user_id1 = event_dictionary['id1']
            user_id2 = event_dictionary['id2']
            Verify_Users_In_Users([user_id1, user_id2])
            if event_dictionary['event_type'] == 'befriend':
                Handle_Befriend_Event(user_id1, user_id2)
            else:
                Handle_Unfriend_Event(user_id1, user_id2)
                
    Build_Distant_Connections()
    Calculate_Network_Statistics()
    
# Process stream_log.json events 
def Process_Events_From_Stream_Log(input_stream):
    for line in input_stream:
        event_dictionary = json.loads(line)
        print(event_dictionary)
        
        # Case purchase event (no network changes)
        if event_dictionary['event_type'] == 'purchase':
            
            # Case user does exist and network mean/std are initialized
            #   and Case event amount > network mean + network std * 3:
            #       Output anomaly detection
            
            Handle_Purchase_Event(event_dictionary)        
        
            # For each connection:
            #   Rebuild network purchase history
            #   Add event to network purchase history
            #   Case network purchase history count > T: 
            #       Sort history by most recent event and remove (T+1)th event
            #   Update network statistics (Welford's algorithm)
        
        # Case friendship event
        elif event_dictionary['event_type'] == 'befriend' \
            or event_dictionary['event_type'] == 'unfriend':
            user_id1 = event_dictionary['id1']
            user_id2 = event_dictionary['id2']
            Verify_Users_In_Users([user_id1, user_id2])
            if event_dictionary['event_type'] == 'befriend':
                Handle_Befriend_Event(user_id1, user_id2)
            else:
                Handle_Unfriend_Event(user_id1, user_id2)
                
            Build_Distant_Connections()
            
            # Rebuild each connection's network purchase history
            # Recalculate each user's network statistics
            
# Create input file stream
def Get_File_Generator(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            yield line
            
# Extract 'D' and 'T' parameters from first line of batch_log.json
def Extract_Network_Parameters(file_generator):
    global NUMBER_OF_DEGREES
    global NUMBER_OF_TRACKED_PURCHASES
    parameters_dictionary = json.loads(file_generator.__next__())
    NUMBER_OF_DEGREES = int(parameters_dictionary['D'])
    NUMBER_OF_TRACKED_PURCHASES = int(parameters_dictionary['T'])
    print("D =", NUMBER_OF_DEGREES, "T =", NUMBER_OF_TRACKED_PURCHASES)
    
# Processes batch_log.json for building data structures
def Process_Batch_Log(file_path):
    print("\nprocess ", file_path)    
    file_generator = Get_File_Generator(file_path)
    Extract_Network_Parameters(file_generator)
    Process_Events_From_Batch_Log(file_generator)
    
# Processes stream_log.json for updating data structures and anomaly detection
def Process_Stream_Log(file_path):
    print("\nprocess ", file_path)
    Process_Events_From_Stream_Log(Get_File_Generator(file_path))
    
### End Main Methods

### Begin Main Script Environment
if __name__ == '__main__':
    Process_Batch_Log(sys.argv[1])
    Process_Stream_Log(sys.argv[2])
    
    # test USERS_DICT
    print()
    for user in USERS_DICT.values():
        user.print()
    
### End Main Script Environment