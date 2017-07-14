from pandas import DataFrame

### Begin Global Variables

NUMBER_OF_DEGREES = 1           # D, the maximum number of degrees of any connection in a social network
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
        
### End Classes

### Begin Methods
    
# Create user if not found in USERS_DICT
def Verify_User_In_Users(user_id):
    global USERS_DICT
    if user_id not in USERS_DICT:
        USERS_DICT[user_id] = User(user_id)
        
# Verifify multiple users
def Verify_Users_In_Users(user_ids):
    for user_id in user_ids:
        Verify_User_In_Users(user_id)
        
# Handle purchase event in batch log
def Handle_Batch_Purchase_Event(event_dictionary):
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
    
# Handle friendship event
def Handle_Friendship_Event(event_dictionary):
    user_id1 = event_dictionary['id1']
    user_id2 = event_dictionary['id2']
    Verify_Users_In_Users([user_id1, user_id2])
    if event_dictionary['event_type'] == 'befriend':
        Handle_Befriend_Event(user_id1, user_id2)
    else:
        Handle_Unfriend_Event(user_id1, user_id2)
            
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
        
### End Methods

### Begin Main

if __name__ == '__main__':
    pass
    
### End Main