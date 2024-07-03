import os

def delete_jpg_files(base_dir):
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.lower().endswith('.jpg'):
                file_path = os.path.join(root, file)
                os.remove(file_path)
                print(f"Deleted {file_path}")

        # Recursively call the function for subdirectories
        for dir in dirs:
            subdir_path = os.path.join(root, dir)
            delete_jpg_files(subdir_path)

# Adjust the base directory path as necessary
base_dir = "your_base_directory_path_here"

delete_jpg_files("joradp_pdfs")
