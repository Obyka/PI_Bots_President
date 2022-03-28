import json
import os

def save_to_JSON_file(data, filename='output.json'):
    final_path = os.path.join('data/', filename)
    with open(final_path, 'w') as outfile:
        json.dump(data, outfile, default=str)

def remove_temp_files():
    dirs = ['merged','output']
    for dir in dirs:
        final_path = os.path.join('data/', dir)
        for f in os.listdir(final_path):
            os.remove(os.path.join(final_path, f))