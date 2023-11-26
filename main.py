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

def check_column_names(file1, file2):
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    columns1 = df1.columns
    columns2 = df2.columns

    if columns1.equals(columns2):
        print("The files have the same column names in the same order.")
    else:
        print("The files do NOT have the same column names in the same order.")


if __name__ == '__main__':

    file_path1 = '/Users/alexanderfournier/PycharmProjects/medtronic_battery_poc/data/postprocess/lot_info_a.csv'
    file_path2 = '/Users/alexanderfournier/PycharmProjects/medtronic_battery_poc/data/postprocess/lot_info_b.csv'

