import os

def make_folder(_files_path):
    # Make extracted folder if it doesn't exist
    if not os.path.exists(_files_path):
        os.mkdir(_files_path)