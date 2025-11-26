import os
# Replace 'path/to/top/folder' with your actual path
TOP_FOLDER = 'output' 

result = [int(f.replace('day_', '')) for f in os.listdir(TOP_FOLDER) if os.path.isdir(os.path.join(TOP_FOLDER, f)) and f.startswith('day_') and len([i for i in os.listdir(os.path.join(TOP_FOLDER, f)) if os.path.isfile(os.path.join(TOP_FOLDER, f, i))]) != 4]
print (result)
