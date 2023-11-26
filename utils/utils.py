import os
import pandas as pd

def convert_excel_to_csv(source_directory, target_directory):
    if not os.path.exists(target_directory):
        os.makedirs(target_directory)
    for filename in os.listdir(source_directory):
        if filename.endswith(('.xls', '.xlsx')):
            file_path = os.path.join(source_directory, filename)
            df = pd.read_excel(file_path)
            csv_filename = filename.rsplit('.', 1)[0] + '.csv'
            csv_file_path = os.path.join(target_directory, csv_filename)
            df.to_csv(csv_file_path, index=False)
            print(f'Converted {filename} to {csv_filename}')



if __name__ == '__main__':
    source_dir = '/Users/alexanderfournier/PycharmProjects/medtronic_battery_poc/data/preprocessed'
    target_dir = '/Users/alexanderfournier/PycharmProjects/medtronic_battery_poc/data/postprocessed'
    convert_excel_to_csv(source_dir, target_dir)