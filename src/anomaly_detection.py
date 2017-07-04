import sys
import json

"""
anomaly_detection.py

Usage: 
python anomaly_detection.py batch_log.json stream_log.json

Description:
Detects user's anomalous purchases based on user's social media's purchase statistics.
Reads batch purchase records and social media friend/unfriend events and builds a social network.
Reads stream events to check for anomalous purchases and to update the social network.
"""

# Initialize global variables
NUMBER_DEGREES = 1
NUMBER_TRACKED_PURCHASES = 2

# Create input file stream
def Get_File_Generator(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            yield line

# Processes batch_log.json for building data structures
def Process_Batch_Log(file_path):
    print()
    print("process ", file_path)
    
    # Create file generator to stream data
    file_generator = Get_File_Generator(file_path)
    
    # Extract 'D' and 'T' parameters from first line
    global NUMBER_DEGREES
    global NUMBER_TRACKED_PURCHASES
    parameters_dictionary = json.loads(file_generator.__next__())
    NUMBER_DEGREES = int(parameters_dictionary['D'])
    NUMBER_TRACKED_PURCHASES = int(parameters_dictionary['T'])
    print("D =", NUMBER_DEGREES, "T =", NUMBER_TRACKED_PURCHASES)
    
    # Process each line in batch log
    for line in file_generator:
        event_dictionary = json.loads(line)
        print(event_dictionary)
        
        # Case purchase event 
        
        # Case befriend event
        
        # Case unfriend event
    
# Processes stream_log.json to update data structures
def Process_Stream_Log(file_path):
    print()
    print("process ", file_path)
    
    # Process each line in stream log
    for line in Get_File_Generator(file_path):
        event_dictionary = json.loads(line)
        print(event_dictionary)
        
        # Case purchase event 
        
        # Case befriend event
        
        # Case unfriend event
    

    
if __name__ == '__main__':
    Process_Batch_Log(sys.argv[1])
    Process_Stream_Log(sys.argv[2])
    