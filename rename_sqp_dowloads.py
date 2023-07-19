import os
import re
import amazon


sqp_downloads = os.listdir('SQP Downloads')

def rename_sqp():
    for file in sqp_downloads:
        if 'ASIN' in file and file.startswith('B0'):
            asin, full_filename = file.split('_', maxsplit=1)
            filename, file_extension = full_filename.split('.')
            new_filename = filename + f' [{asin}].{file_extension}'
            new_filepath = os.path.join('SQP Downloads', new_filename)
            old_filepath = os.path.join('SQP Downloads', file)
            print(old_filepath)
            print(new_filepath)
            os.rename(old_filepath, new_filepath)
        elif file.startswith('Brand'):
            new_filename = file.split('_', maxsplit=1)[1]
            new_filepath = os.path.join('SQP Downloads', new_filename)
            old_filepath = os.path.join('SQP Downloads', file)
            print(old_filepath)
            print(new_filepath)
            os.rename(old_filepath, new_filepath)

def insert_sqp_brands():
    for file in sqp_downloads:
        if 'Brand' in file and 'US' in file and '2023_05_27' in file:
            try:
                print(f"Inserting {file}")
                filepath = os.path.join('SQP Downloads', file)
                amazon.insert_sqp_reports(filepath)
            except Exception as e:
                print("\tError on inserting")
                print(e)
                pass


if __name__ == '__main__':
    insert_sqp_brands()