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

# function that merge "visualizza files"
def merge_visualizza(input_datas):
    return pd.concat(input_datas)

def save_dataframe_to_excel(path, dataframe, sheet_name):
    with pd.ExcelWriter(path, mode="a") as writer:
        dataframe.to_excel(writer, sheet_name=sheet_name, index= False)
        print('saved file', path)

#function to filter budget files (gourmet version)
def filter_budget_gou(input_dataframe):
    grouped_df = input_dataframe.groupby(['articolo','mese','channel'], as_index=False).agg({'qta':'sum', 'mese':'first','channel':'first'})
    gou_df = grouped_df[grouped_df['channel']=='GOU       ']
    return gou_df

#function to filter budget files (industrial version)
def filter_budget_ind(input_dataframe):
    grouped_df = input_dataframe.groupby(['articolo','mese','channel'], as_index=False).agg({'qta':'sum', 'mese':'first','channel':'first'})
    ind_df = grouped_df[grouped_df['channel']=='FOOD      ']
    return ind_df

#function to filter budget files (total version)
def filter_budget_tot(input_dataframe):
    grouped_df = input_dataframe.groupby(['articolo','mese'], as_index=False).agg({'qta':'sum', 'mese':'first'})
    return grouped_df

#function to filter "estrazione" files (gourmet version)
def filter_estrazione_gou(input_dataframe):
    grouped_df = input_dataframe.groupby(['articolo','mese','macro_channel'], as_index=False).agg({'qta':'sum', 'mese':'first','macro_channel':'first'})
    gou_df = grouped_df[grouped_df['macro_channel']=='GOURMET ']
    return gou_df

#function to filter "estrazione" files (industrial version)
def filter_estrazione_ind(input_dataframe):
    filtered_df = input_dataframe[input_dataframe['macro_channel'].isin(['FOOD MANUFACTURERS       ', 'INTERCOMPANY'])]
    grouped_df = filtered_df.groupby(['articolo','mese'], as_index=False).agg({'qta':'sum', 'mese':'first'})
    return grouped_df

#function to filter "estrazione" files (total version)
def filter_estrazione_tot(input_dataframe):
    grouped_df = input_dataframe.groupby(['articolo','mese'], as_index=False).agg({'qta':'sum', 'mese':'first'})
    return grouped_df

def filter_lista_tot(input_dataframe):
    input_dataframe['BU'] = input_dataframe['Customer Group'].fillna(input_dataframe['Bus'])
    input_dataframe['Fatt'] = input_dataframe ['  Purchase Price ']*input_dataframe['        Qtà conc ']
    input_dataframe[' Val.a'] = pd.to_datetime(input_dataframe[' Val.a'])
    input_dataframe = input_dataframe[input_dataframe['BU'] != 'I80']
    input_dataframe = input_dataframe[input_dataframe['Stt'] == 40]
    input_dataframe1 = input_dataframe[input_dataframe['TiC'] != 'I02']
    dataframe_trimestre1 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q1']
    grouped_df1 = dataframe_trimestre1.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum',
    '         Qtà acq ':'sum', '     Qta Residua ':'sum', '    Qtà ricevuta':'sum','Fatt':'sum'})
    dataframe_trimestre2 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_df2 = dataframe_trimestre2.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum',
    '         Qtà acq ':'sum', '     Qta Residua ':'sum', '    Qtà ricevuta':'sum','Fatt':'sum'})
    dataframe_trimestre3 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_df3 = dataframe_trimestre3.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum',
    '         Qtà acq ':'sum', '     Qta Residua ':'sum', '    Qtà ricevuta':'sum','Fatt':'sum'})
    dataframe_trimestre4 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_df4 = dataframe_trimestre4.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum',
    '         Qtà acq ':'sum', '     Qta Residua ':'sum', '    Qtà ricevuta':'sum','Fatt':'sum'})
    merged_df = pd.merge(grouped_df1,grouped_df2, how = 'outer', on = 'Item                ', suffixes=('Q1', 'Q2'))
    merged_df = pd.merge(merged_df,grouped_df3, how = 'outer', on = 'Item                ', suffixes=('', ' Q3'))
    merged_df = pd.merge(merged_df, grouped_df4, how='outer', on='Item                ', suffixes=('', 'Q4'))
    merged_df['APPQ1'] = merged_df['FattQ1'] / merged_df['        Qtà conc Q1']
    merged_df['APPQ2'] = merged_df['FattQ2'] / merged_df['        Qtà conc Q2']
    merged_df['APPQ3'] = merged_df['Fatt'] / merged_df['        Qtà conc ']
    merged_df['APPQ4'] = merged_df['FattQ4'] / merged_df['        Qtà conc Q4']
    listini_dataframe = input_dataframe[input_dataframe['TiC'] == 'I02']
    listini_trimestre1 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q1']
    grouped_list1 = listini_trimestre1.groupby(['Item                '], as_index=False).agg({'  Purchase Price ': 'max'})
    listini_trimestre2 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_list2 = listini_trimestre2.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre3 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_list3 = listini_trimestre3.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre4 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_list4 = listini_trimestre4.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    merged_dflist = pd.merge(grouped_list1, grouped_list2, how = 'outer', on = 'Item                ', suffixes=('Q1', 'Q2'))
    merged_dflist = pd.merge(merged_dflist, grouped_list3, how='outer', on='Item                ', suffixes=('', ' Q3'))
    merged_dflist = pd.merge(merged_dflist, grouped_list4, how='outer', on='Item                ', suffixes=('', 'Q4'))
    merged_df = pd.merge(merged_df,merged_dflist, how='outer', on = 'Item                ')
    merged_df['  Purchase Price '] = merged_df['  Purchase Price '].fillna(merged_df['  Purchase Price Q4'])
    merged_df['  Purchase Price Q2'] = merged_df['  Purchase Price Q2'].fillna(merged_df['  Purchase Price '])
    merged_df['  Purchase Price Q1'] = merged_df['  Purchase Price Q1'].fillna(merged_df['  Purchase Price Q2'])
    merged_df['APPQ1'] = merged_df['APPQ1'].fillna(merged_df['  Purchase Price Q1'])
    merged_df['APPQ2'] = merged_df['APPQ2'].fillna(merged_df['  Purchase Price Q2'])
    merged_df['APPQ3'] = merged_df['APPQ3'].fillna(merged_df['  Purchase Price '])
    merged_df['APPQ4'] = merged_df['APPQ4'].fillna(merged_df['  Purchase Price Q4'])
    todelete = ['  Purchase Price Q1','  Purchase Price Q2','  Purchase Price ','  Purchase Price Q4']
    merged_df = merged_df.drop(columns=todelete)
    return merged_df


def filter_lista_gou(input_dataframe):
    input_dataframe['BU'] = input_dataframe['Customer Group'].fillna(input_dataframe['Bus'])
    input_dataframe['Fatt'] = input_dataframe ['  Purchase Price ']*input_dataframe['        Qtà conc ']
    input_dataframe[' Val.a'] = pd.to_datetime(input_dataframe[' Val.a'])
    input_dataframe1 = input_dataframe[input_dataframe['BU'] == 'I75']
    input_dataframe1 = input_dataframe1[input_dataframe1['Stt'] == 40]
    input_dataframe1 = input_dataframe1[input_dataframe1['TiC'] != 'I02']
    dataframe_trimestre1 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q1']
    grouped_df1 = dataframe_trimestre1.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum',
    '         Qtà acq ':'sum', '     Qta Residua ':'sum', '    Qtà ricevuta':'sum','Fatt':'sum'})
    dataframe_trimestre2 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_df2 = dataframe_trimestre2.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum',
    '         Qtà acq ':'sum', '     Qta Residua ':'sum', '    Qtà ricevuta':'sum','Fatt':'sum'})
    dataframe_trimestre3 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_df3 = dataframe_trimestre3.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum',
    '         Qtà acq ':'sum', '     Qta Residua ':'sum', '    Qtà ricevuta':'sum','Fatt':'sum'})
    dataframe_trimestre4 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_df4 = dataframe_trimestre4.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum',
    '         Qtà acq ':'sum', '     Qta Residua ':'sum', '    Qtà ricevuta':'sum','Fatt':'sum'})
    merged_df = pd.merge(grouped_df1,grouped_df2, how = 'outer', on = 'Item                ', suffixes=('Q1', 'Q2'))
    merged_df = pd.merge(merged_df,grouped_df3, how = 'outer', on = 'Item                ', suffixes=('', ' Q3'))
    merged_df = pd.merge(merged_df, grouped_df4, how='outer', on='Item                ', suffixes=('', 'Q4'))
    merged_df['APPQ1'] = merged_df['FattQ1'] / merged_df['        Qtà conc Q1']
    merged_df['APPQ2'] = merged_df['FattQ2'] / merged_df['        Qtà conc Q2']
    merged_df['APPQ3'] = merged_df['Fatt'] / merged_df['        Qtà conc ']
    merged_df['APPQ4'] = merged_df['FattQ4'] / merged_df['        Qtà conc Q4']
    listini_dataframe = input_dataframe[input_dataframe['TiC'] == 'I02']
    listini_trimestre1 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q1']
    grouped_list1 = listini_trimestre1.groupby(['Item                '], as_index=False).agg({'  Purchase Price ': 'max'})
    listini_trimestre2 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_list2 = listini_trimestre2.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre3 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_list3 = listini_trimestre3.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre4 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_list4 = listini_trimestre4.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    merged_dflist = pd.merge(grouped_list1, grouped_list2, how = 'outer', on = 'Item                ', suffixes=('Q1', 'Q2'))
    merged_dflist = pd.merge(merged_dflist, grouped_list3, how='outer', on='Item                ', suffixes=('', ' Q3'))
    merged_dflist = pd.merge(merged_dflist, grouped_list4, how='outer', on='Item                ', suffixes=('', 'Q4'))
    merged_df = pd.merge(merged_df,merged_dflist, how='outer', on = 'Item                ')
    merged_df['  Purchase Price '] = merged_df['  Purchase Price '].fillna(merged_df['  Purchase Price Q4'])
    merged_df['  Purchase Price Q2'] = merged_df['  Purchase Price Q2'].fillna(merged_df['  Purchase Price '])
    merged_df['  Purchase Price Q1'] = merged_df['  Purchase Price Q1'].fillna(merged_df['  Purchase Price Q2'])
    merged_df['APPQ1'] = merged_df['APPQ1'].fillna(merged_df['  Purchase Price Q1'])
    merged_df['APPQ2'] = merged_df['APPQ2'].fillna(merged_df['  Purchase Price Q2'])
    merged_df['APPQ3'] = merged_df['APPQ3'].fillna(merged_df['  Purchase Price '])
    merged_df['APPQ4'] = merged_df['APPQ4'].fillna(merged_df['  Purchase Price Q4'])
    todelete = ['  Purchase Price Q1','  Purchase Price Q2','  Purchase Price ','  Purchase Price Q4']
    merged_df = merged_df.drop(columns=todelete)
    return merged_df



def filter_lista_ind(input_dataframe):
    input_dataframe['BU'] = input_dataframe['Customer Group'].fillna(input_dataframe['Bus'])
    input_dataframe['Fatt'] = input_dataframe ['  Purchase Price ']*input_dataframe['        Qtà conc ']
    input_dataframe[' Val.a'] = pd.to_datetime(input_dataframe[' Val.a'])
    input_dataframe1 = input_dataframe[input_dataframe['BU'].isin(['I71', 'I72'])]
    input_dataframe1 = input_dataframe1[input_dataframe1['Stt'] == 40]
    input_dataframe1 = input_dataframe1[input_dataframe1['TiC'] != 'I02']
    dataframe_trimestre1 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q1']
    grouped_df1 = dataframe_trimestre1.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum',
    '         Qtà acq ':'sum', '     Qta Residua ':'sum', '    Qtà ricevuta':'sum','Fatt':'sum'})
    dataframe_trimestre2 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_df2 = dataframe_trimestre2.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum',
    '         Qtà acq ':'sum', '     Qta Residua ':'sum', '    Qtà ricevuta':'sum','Fatt':'sum'})
    dataframe_trimestre3 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_df3 = dataframe_trimestre3.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum',
    '         Qtà acq ':'sum', '     Qta Residua ':'sum', '    Qtà ricevuta':'sum','Fatt':'sum'})
    dataframe_trimestre4 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_df4 = dataframe_trimestre4.groupby(['Item                '], as_index=False).agg({'        Qtà conc ': 'sum',
    '         Qtà acq ':'sum', '     Qta Residua ':'sum', '    Qtà ricevuta':'sum','Fatt':'sum'})
    merged_df = pd.merge(grouped_df1,grouped_df2, how = 'outer', on = 'Item                ', suffixes=('Q1', 'Q2'))
    merged_df = pd.merge(merged_df,grouped_df3, how = 'outer', on = 'Item                ', suffixes=('', ' Q3'))
    merged_df = pd.merge(merged_df, grouped_df4, how='outer', on='Item                ', suffixes=('', 'Q4'))
    merged_df['APPQ1'] = merged_df['FattQ1'] / merged_df['        Qtà conc Q1']
    merged_df['APPQ2'] = merged_df['FattQ2'] / merged_df['        Qtà conc Q2']
    merged_df['APPQ3'] = merged_df['Fatt'] / merged_df['        Qtà conc ']
    merged_df['APPQ4'] = merged_df['FattQ4'] / merged_df['        Qtà conc Q4']
    listini_dataframe = input_dataframe[input_dataframe['TiC'] == 'I02']
    listini_trimestre1 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q1']
    grouped_list1 = listini_trimestre1.groupby(['Item                '], as_index=False).agg({'  Purchase Price ': 'max'})
    listini_trimestre2 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_list2 = listini_trimestre2.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre3 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_list3 = listini_trimestre3.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre4 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_list4 = listini_trimestre4.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    merged_dflist = pd.merge(grouped_list1, grouped_list2, how = 'outer', on = 'Item                ', suffixes=('Q1', 'Q2'))
    merged_dflist = pd.merge(merged_dflist, grouped_list3, how='outer', on='Item                ', suffixes=('', ' Q3'))
    merged_dflist = pd.merge(merged_dflist, grouped_list4, how='outer', on='Item                ', suffixes=('', 'Q4'))
    merged_df = pd.merge(merged_df,merged_dflist, how='outer', on = 'Item                ')
    merged_df['  Purchase Price '] = merged_df['  Purchase Price '].fillna(merged_df['  Purchase Price Q4'])
    merged_df['  Purchase Price Q2'] = merged_df['  Purchase Price Q2'].fillna(merged_df['  Purchase Price '])
    merged_df['  Purchase Price Q1'] = merged_df['  Purchase Price Q1'].fillna(merged_df['  Purchase Price Q2'])
    merged_df['APPQ1'] = merged_df['APPQ1'].fillna(merged_df['  Purchase Price Q1'])
    merged_df['APPQ2'] = merged_df['APPQ2'].fillna(merged_df['  Purchase Price Q2'])
    merged_df['APPQ3'] = merged_df['APPQ3'].fillna(merged_df['  Purchase Price '])
    merged_df['APPQ4'] = merged_df['APPQ4'].fillna(merged_df['  Purchase Price Q4'])
    todelete = ['  Purchase Price Q1','  Purchase Price Q2','  Purchase Price ','  Purchase Price Q4']
    merged_df = merged_df.drop(columns=todelete)
    return merged_df

def groupforBU(input_dataframe):
    input_dataframe['BU'] = input_dataframe['Customer Group'].fillna(input_dataframe['Bus'])
    grouped_df = input_dataframe.groupby(['Contrat'], as_index=False).agg({'BU': 'first'})
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
            lista_dataframe_tot = filter_lista_tot(lista_dataframe)
            lista_dataframe_gou = filter_lista_gou(lista_dataframe)
            lista_dataframe_ind = filter_lista_ind(lista_dataframe)
            grouped_forBU = groupforBU(lista_dataframe)
            #merge_lista1 = pd.merge(lista_dataframe_ind,lista_dataframe_gou, how = 'outer', on = 'Item                ', suffixes=('IND', 'GOU'))
            #merge_lista = pd.merge(merge_lista1,lista_dataframe_tot,how = 'outer', on = 'Item                ', suffixes=('', 'TOT'))
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
        elif file.find('codici') != -1:
            codici_dataframe_total = file_reader(file, "Total")
            codici_dataframe_industrial = file_reader(file, "Industrial")
            codici_dataframe_gourmet = file_reader(file, "Gourmet")


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
#save_dataframe_to_excel(test_file, visual_dataframes, "visualizza")
save_dataframe_to_excel(test_file, budget_dataframe_gou, "budget_gou")
save_dataframe_to_excel(test_file, budget_dataframe_ind, "budget_ind")
save_dataframe_to_excel(test_file, budget_dataframe_tot, "budget_tot")
save_dataframe_to_excel(test_file, lista_dataframe_tot, "listatot")
save_dataframe_to_excel(test_file, lista_dataframe_gou, "listagou")
save_dataframe_to_excel(test_file, lista_dataframe_ind, "listaind")
save_dataframe_to_excel(test_file, grouped_forBU, "BUcontracts")
#save_dataframe_to_excel(test_file, merge_lista,"merge")
save_dataframe_to_excel(test_file, estrazione_dataframe_tot, "estrazione_tot")
save_dataframe_to_excel(test_file, estrazione_dataframe_ind, "estrazione_ind")
save_dataframe_to_excel(test_file, estrazione_dataframe_gou, "estrazione_gou")
save_dataframe_to_excel(test_file, codici_dataframe_total, "codici total")
#save_dataframe_to_excel(test_file, grouped_forBU, "merged_visuaBU")



# delete the first empty sheet
workbook=openpyxl.load_workbook(test_file)
std=workbook["TempExcelSheetForDeleting"]
workbook.remove(std)
workbook.save(test_file)




