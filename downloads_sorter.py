import os

folder = r"#INSERT_YOUR_DOWNLOADS_FOLDER#"
files = os.scandir(folder)

# Loop through files in downloads folder
for file in files:
    extention = str(file.name).split(".")[-1]
    
    # Create extention folder is not exists
    if os.path.exists(folder + "\\" + extention) == False:
        os.makedirs(folder + "\\" + extention)
    
    # Move file into extention folder 
    if os.path.isfile(file.path):   
        os.rename(file.path, folder + "\\" + extention + "\\" + file.name)
        
# Report of download contents
print("=====Download folder contents=====")
sub_directories = os.scandir(folder)
for directory in sub_directories:
    sub_files = os.scandir(directory)
    num_of_files = 0
    for file in sub_files: num_of_files += 1
    print("Subfolder:", directory.name, "Number of files:", num_of_files)