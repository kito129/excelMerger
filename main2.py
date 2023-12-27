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

def filter_budget_gou(input_dataframe):
    #grouped_df = input_dataframe.groupby(['articolo', 'mese'])['qta'].sum()
    grouped_df = input_dataframe.groupby(['articolo','mese','channel'], as_index=False).agg({'qta':'sum', 'mese':'first','channel':'first'})
    gou_df = grouped_df[grouped_df['channel']=='GOU       ']
    # Apply the `agg()` function to calculate the total `Q.TA` per `ARTICOLO`

    # grouped_df = grouped_df.agg({'qta': 'sum'})
    print(gou_df)
    return gou_df
def filter_budget_ind(input_dataframe):
    #grouped_df = input_dataframe.groupby(['articolo', 'mese'])['qta'].sum()
    grouped_df = input_dataframe.groupby(['articolo','mese','channel'], as_index=False).agg({'qta':'sum', 'mese':'first','channel':'first'})
    ind_df = grouped_df[grouped_df['channel']=='FOOD      ']
    # Apply the `agg()` function to calculate the total `Q.TA` per `ARTICOLO`

    # grouped_df = grouped_df.agg({'qta': 'sum'})
    print(ind_df)
    return ind_df

def filter_budget_tot(input_dataframe):
    #grouped_df = input_dataframe.groupby(['articolo', 'mese'])['qta'].sum()
    grouped_df = input_dataframe.groupby(['articolo','mese'], as_index=False).agg({'qta':'sum', 'mese':'first'})
    # Apply the `agg()` function to calculate the total `Q.TA` per `ARTICOLO`

    # grouped_df = grouped_df.agg({'qta': 'sum'})
    print(grouped_df)
    return grouped_df

def filter_estrazione_gou(input_dataframe):
    #grouped_df = input_dataframe.groupby(['articolo', 'mese'])['qta'].sum()
    grouped_df = input_dataframe.groupby(['articolo','mese','macro_channel'], as_index=False).agg({'qta':'sum', 'mese':'first','macro_channel':'first'})
    gou_df = grouped_df[grouped_df['macro_channel']=='GOURMET ']
    # Apply the `agg()` function to calculate the total `Q.TA` per `ARTICOLO`

    # grouped_df = grouped_df.agg({'qta': 'sum'})
    print(gou_df)
    return gou_df

def filter_estrazione_ind(input_dataframe):
    #grouped_df = input_dataframe.groupby(['articolo', 'mese'])['qta'].sum()
    filtered_df = input_dataframe[input_dataframe['macro_channel'].isin(['FOOD MANUFACTURERS       ', 'INTERCOMPANY'])]
    grouped_df = filtered_df.groupby(['articolo','mese'], as_index=False).agg({'qta':'sum', 'mese':'first'})
    #gou_df = grouped_df[grouped_df['macro_channel'].isin(['FOOD MANUFACTURERS       ', 'INTERCOMPANY'])]
    # Apply the `agg()` function to calculate the total `Q.TA` per `ARTICOLO`

    # grouped_df = grouped_df.agg({'qta': 'sum'})
    print(grouped_df)
    return grouped_df

def filter_estrazione_tot(input_dataframe):
    #grouped_df = input_dataframe.groupby(['articolo', 'mese'])['qta'].sum()
    grouped_df = input_dataframe.groupby(['articolo','mese'], as_index=False).agg({'qta':'sum', 'mese':'first'})
    # Apply the `agg()` function to calculate the total `Q.TA` per `ARTICOLO`

    # grouped_df = grouped_df.agg({'qta': 'sum'})
    print(grouped_df)
    return grouped_df

def filter_lista_tot(input_dataframe):
    #grouped_df = input_dataframe.groupby(['articolo', 'mese'])['qta'].sum()
    input_dataframe['BU'] = input_dataframe['Customer Group'].fillna(input_dataframe['Bus'])
    input_dataframe[' Val.a'] = pd.to_datetime(input_dataframe[' Val.a'])
    input_dataframe = input_dataframe[input_dataframe['BU'] != 'I80']
    input_dataframe = input_dataframe[input_dataframe['Stt'] == 40]
    dataframe_trimestre1 = input_dataframe[input_dataframe[' Val.a'].dt.to_period('Q') == '2024Q1']
    grouped_df1 = dataframe_trimestre1.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum'})
    dataframe_trimestre2 = input_dataframe[input_dataframe[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_df2 = dataframe_trimestre2.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum'})
    dataframe_trimestre3 = input_dataframe[input_dataframe[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_df3 = dataframe_trimestre3.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum'})
    dataframe_trimestre4 = input_dataframe[input_dataframe[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_df4 = dataframe_trimestre4.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum'})
    merged_df = pd.merge(grouped_df1,grouped_df2, how = 'outer', on = 'Item                ', suffixes=('Q1','Q2'))
    merged_df = pd.merge(merged_df,grouped_df3, how = 'outer', on = 'Item                ', suffixes=('','Q3'))
    merged_df = pd.merge(merged_df, grouped_df4, how='outer', on='Item                ', suffixes=('', 'Q4'))

    # grouped_df = grouped_df.agg({'qta': 'sum'})
    print(merged_df)
    return merged_df

'''def filter_lista_tot(input_dataframe):
    input_dataframe[' Val.a'] = pd.to_datetime(input_dataframe[' Val.a'])
    dataframe_trimestre1 = input_dataframe[input_dataframe[' Val.a'].dt.to_period('Q')== '2024Q1']
    grouped_df1 = dataframe_trimestre1.groupby(['Item                '], as_index=False).agg({'        Qtà conc ':'sum'})
    dataframe_trimestre2 = input_dataframe[input_dataframe[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_df2 = dataframe_trimestre2.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum'})
    dataframe_trimestre3 = input_dataframe[input_dataframe[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_df3 = dataframe_trimestre3.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum'})
    dataframe_trimestre4 = input_dataframe[input_dataframe[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_df4 = dataframe_trimestre4.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum'})
    merged_df = pd.merge(grouped_df1,grouped_df2,grouped_df3,grouped_df4, on='Item                ', how='outer',suffixes=('Q1','Q2','Q3','Q4'))

    # grouped_df = grouped_df.agg({'qta': 'sum'})
    print(merged_df)
    return merged_df'''


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
            lista_dataframe_tot = filter_lista_tot(lista_dataframe)
            print(lista_dataframe)
        elif file.find('BDG') != -1:
            budget_dataframe = file_reader(file, "Foglio1")
            budget_dataframe_gou = filter_budget_gou(budget_dataframe)
            budget_dataframe_ind = filter_budget_ind(budget_dataframe)
            budget_dataframe_tot = filter_budget_tot(budget_dataframe)
        elif file.find('estrazione') != -1:
            estrazione_dataframe = file_reader(file, "Foglio1")
            estrazione_dataframe_tot = filter_estrazione_tot(estrazione_dataframe)
            estrazione_dataframe_gou = filter_estrazione_gou(estrazione_dataframe)
            estrazione_dataframe_ind = filter_estrazione_ind(estrazione_dataframe)
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
save_dataframe_to_excel(test_file, budget_dataframe_gou, "budget_gou")
save_dataframe_to_excel(test_file, budget_dataframe_ind, "budget_ind")
save_dataframe_to_excel(test_file, budget_dataframe_tot, "budget_tot")
save_dataframe_to_excel(test_file, lista_dataframe_tot, "lista")
save_dataframe_to_excel(test_file, estrazione_dataframe_tot, "estrazione_tot")
save_dataframe_to_excel(test_file, estrazione_dataframe_ind, "estrazione_ind")
save_dataframe_to_excel(test_file, estrazione_dataframe_gou, "estrazione_gou")



# delete the first empty sheet
workbook=openpyxl.load_workbook(test_file)
std=workbook["TempExcelSheetForDeleting"]
workbook.remove(std)
workbook.save(test_file)




