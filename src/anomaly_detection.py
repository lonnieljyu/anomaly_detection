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
Reads batch purchase records and social network friend/unfriend events.
Builds users' purchase history and social networks.
Reads stream events to check for anomalous purchases and updates record histories/social networks.
"""

### Begin Global Variables

NUMBER_OF_DEGREES = 1           # D, the maximum number of degrees of any connection in a social network
NUMBER_OF_TRACKED_PURCHASES = 2 # T, the maximum sample size of tracked purchases in a social network
CURRENT_LOG_INDEX = 0           # Used for assigning each purchase event a log index
USERS = dict()                  # Dictionary of users

### End Global Variables

### Begin Classes

# User:
#   Contains a user id, set of friends, set of distant connections, and DataFrame of purchase history.
#   Friends are defined as social network connections of degree 1.
#   Distant connections are defined as social network connections of degree 2 to D.
#   Each row of the purchase history contains one purchase event information.
#       A DataFrame is a Pandas data structure for tabular data with easy slicing and sorting.
class User:
    def __init__(self, id):
        self.id = id
        self.friends = set()
        self.distant_connections = set()
        self.purchase_history = DataFrame()
        
    def Add_Purchase(event_dictionary):
        self.purchase_history.append(event_dictionary)
        
    def Add_Friend(friend):
        self.friends.add(friend)
        
    def Remove_Friend(not_friend):
        self.friends.remove(not_friend)
        
    def Add_Distant_Connection(connection):
        self.distant_connections.add(connection)
        
    def Remove_Distant_Connection(not_connection):
        self.distant_connections.remove(not_connection)
        
    # Prints class contents for debugging
    def print(self):
        print(  "User Id:", self.id, 
                "Friends:", self.friends, 
                "Distant connections:", self.distant_connections)
        print("Purchase history:", self.purchase_history)
        
### End Classes

### Begin Public Methods

# Convert timestamp formatted string to datetime object.
def Convert_To_Datetime(timestamp):
    pass
    
# Create input file stream
def Get_File_Generator(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            yield line
            
# Process streaming events 
def Process_Events_From_Input_Stream(input_stream):
    for line in input_stream:
        event_dictionary = json.loads(line)
        print(event_dictionary)
        
        # Case purchase event: 
        #   Add purchase event to user's purchase history
        #   Update user's social network connections' statistics
        
        # Case befriend event:
        #   
        
        # Case unfriend event
            
# Processes batch_log.json for building data structures
def Process_Batch_Log(file_path):
    print()
    print("process ", file_path)    
    
    file_generator = Get_File_Generator(file_path)
    
    # Extract 'D' and 'T' parameters from first line
    parameters_dictionary = json.loads(file_generator.__next__())
    global NUMBER_OF_DEGREES
    global NUMBER_OF_TRACKED_PURCHASES
    NUMBER_OF_DEGREES = int(parameters_dictionary['D'])
    NUMBER_OF_TRACKED_PURCHASES = int(parameters_dictionary['T'])
    print("D =", NUMBER_OF_DEGREES, "T =", NUMBER_OF_TRACKED_PURCHASES)
    
    Process_Events_From_Input_Stream(file_generator)
    
# Processes stream_log.json for updating data structures
def Process_Stream_Log(file_path):
    print()
    print("process ", file_path)
    
    Process_Events_From_Input_Stream(Get_File_Generator(file_path))
    
### End Public Methods

### Begin Main Script Environment
if __name__ == '__main__':
    Process_Batch_Log(sys.argv[1])
    Process_Stream_Log(sys.argv[2])
    
    # test user class
    print()
    user = User(1)
    user.print()
    
### End Main Script Environment