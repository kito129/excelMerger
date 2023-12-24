import os, csv, sys
from datetime import datetime
import pandas as pd
import openpyxl

# global variables
INPUT = "./inputData"
OUTPUT = "./outputData"

# function that open the path file and sheet name and return a dataframe
def file_reader(path, sheet_name):
    xls = pd.ExcelFile(os.path.join(INPUT, path))
    return pd.read_excel(xls, sheet_name)

def merge_visualizza(input_datas):
    return pd.concat(input_datas)

def save_dataframe_to_excel(path, dataframe, sheet_name):
    print(path)
    with pd.ExcelWriter(path, mode="a") as writer:
        dataframe.to_excel(writer, sheet_name=sheet_name, index= False)
        print('saved file', path)


def filter_budget(input_dataframe):
    grouped_df = input_dataframe.groupby(['articolo', 'mese'])['qta'].sum()

    # Apply the `agg()` function to calculate the total `Q.TA` per `ARTICOLO`

    # grouped_df = grouped_df.agg({'qta': 'sum'})
    print(grouped_df)
    return grouped_df


# main
date = datetime.now().strftime('%d_%m_%Y_%H_%M_%S')

# check of folders if present
if not os.path.exists(OUTPUT):
    try:
        os.mkdir(OUTPUT)
    except:
        print('Error in output path creation')
        sys.exit(0)

if not os.path.exists(INPUT):
    sys.exit(0)

# list all files in INPUT
files = os.listdir(INPUT)
visual_dataframes = []
for file in files:
     # check if is excel file
    if file.endswith('.xlsx') or file.endswith('.xls'):
        if file.find('Visualizza') != -1:
            visual_dataframes.append(file_reader(file, "Sheet0"))
        elif file.find('Lista') != -1:
            lista_dataframe = file_reader(file, "Sheet0")
            print(lista_dataframe)
        elif file.find('BDG') != -1:
            budget_dataframe = file_reader(file, "Foglio1")
            budget_dataframe = filter_budget(budget_dataframe)
        elif file.find('estrazione') != -1:
            estrazione_dataframe = file_reader(file, "Foglio1")
            print(estrazione_dataframe)


# test pourpose
test_file = os.path.join(os.path.abspath(OUTPUT), f'merged_{date}.xlsx')
df = pd.DataFrame()
df.to_excel(test_file)
workbook = openpyxl.load_workbook(test_file)
temp_sheet = workbook['Sheet1']
temp_sheet.title = 'TempExcelSheetForDeleting'
workbook.save(test_file)



# save visualizza merged
#merged_visualizza = merge_visualizza(visual_dataframes)
#save_dataframe_to_excel(test_file, merged_visualizza, "visualizza")
save_dataframe_to_excel(test_file, budget_dataframe, "budget")
#save_dataframe_to_excel(test_file, lista_dataframe, "lista")
#save_dataframe_to_excel(test_file, estrazione_dataframe, "estrazione")



# delete the first empty sheet
workbook=openpyxl.load_workbook(test_file)
std=workbook["TempExcelSheetForDeleting"]
workbook.remove(std)
workbook.save(test_file)




