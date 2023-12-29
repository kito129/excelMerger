import os, sys
from datetime import datetime
import pandas as pd
import openpyxl


# global variables
INPUT = "./inputData"
OUTPUT = "./outputData"
QUARTERS = ['2024Q1', '2024Q2', '2024Q3', '2024Q4']
QUARTERS_ = ['Q1', 'Q2', 'Q3', 'Q4']

# utils functions
def file_reader(path, sheet_name):
    xls = pd.ExcelFile(os.path.join(INPUT, path))
    return pd.read_excel(xls, sheet_name)

def merge_visualizza_from_all_files(input_datas):
    return pd.concat(input_datas)


def save_dataframe_to_excel(path, dataframe, sheet_name):
    with pd.ExcelWriter(path, mode="a") as writer:
        dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
        print('saved file', path)


# group functions
def group_for_BU(input_dataframe):
    input_dataframe['BU'] = input_dataframe['Customer Group'].fillna(input_dataframe['Bus'])
    return input_dataframe.groupby(['Contrat'], as_index=False).agg({'BU': 'first'})
def filter_visual(input_dataframe, grouped_for_BU):
    input_dataframe['Ns N rif'] = pd.to_numeric(input_dataframe['Ns N rif'], errors='coerce')
    input_dataframe = pd.merge(input_dataframe, grouped_for_BU, how='outer', left_on='Ns N rif', right_on='Contrat')
    input_dataframe['Dtric+cons'] = input_dataframe['Dt ric'].fillna(input_dataframe['DtCons'])
    input_dataframe['Qtàric+ord'] = input_dataframe['Qtà ricev '].fillna(input_dataframe['QtOrdinata'])
    input_dataframe['Fatt'] = input_dataframe['Prz acquis'] * input_dataframe['Qtàric+ord']
    input_dataframe['Dtric+cons'] = pd.to_datetime(input_dataframe['Dtric+cons'])

    merged_df = pd.DataFrame()
    for month in range(1, 13):
        dataframe_month = input_dataframe[input_dataframe['Dtric+cons'].dt.month == month]
        grouped_df = dataframe_month.groupby(['C  parte  '], as_index=False).agg({
            'Qtàric+ord': 'sum', 'Qtà confer': 'sum', 'Qtà ricev ': 'sum', 'Fatt': 'sum'
        })
        grouped_df['APP'] = grouped_df['Fatt'] / grouped_df['Qtàric+ord']
        merged_df = pd.concat([merged_df, grouped_df], axis=0, ignore_index=True)

    return merged_df

def filter_list(input_dataframe, tot=True, gou=False, ind=False):
    # prepare dataframe
    input_dataframe['BU'] = input_dataframe['Customer Group'].fillna(input_dataframe['Bus'])
    input_dataframe['Fatt'] = input_dataframe['  Purchase Price '] * input_dataframe['        Qtà conc ']
    input_dataframe[' Val.a'] = pd.to_datetime(input_dataframe[' Val.a'])

    if tot:
        input_dataframe = input_dataframe[input_dataframe['Stt'] == 40]
        input_dataframe1 = input_dataframe[input_dataframe['TiC'] != 'I02']
    elif gou:
        input_dataframe1 = input_dataframe[input_dataframe['BU'] == 'I75']
        input_dataframe1 = input_dataframe1[input_dataframe1['Stt'] == 40]
        input_dataframe1 = input_dataframe1[input_dataframe1['TiC'] != 'I02']
    elif ind:
        input_dataframe1 = input_dataframe[input_dataframe['BU'].isin(['I71', 'I72'])]
        input_dataframe1 = input_dataframe1[input_dataframe1['Stt'] == 40]
        input_dataframe1 = input_dataframe1[input_dataframe1['TiC'] != 'I02']

    quarters_dataframe = []
    # group by quarter
    for quarter in QUARTERS:
        temp = input_dataframe[input_dataframe[' Val.a'].dt.to_period('Q') == quarter]
        quarters_dataframe.append(
            temp.groupby(['Item                '], as_index=False).agg(
                {'        Qtà conc ': 'sum',
                 '         Qtà acq ': 'sum',
                 '     Qta Residua ': 'sum',
                 '    Qtà ricevuta': 'sum',
                 'Fatt': 'sum'})
        )
    # rename columns
    for i in range(len(quarters_dataframe)):
        quarters_dataframe[i].columns = ['Item                ',
                                         '        Qtà conc ' + QUARTERS[i],
                                         '         Qtà acq ' + QUARTERS[i],
                                         '     Qta Residua ' + QUARTERS[i],
                                         '    Qtà ricevuta' + QUARTERS[i],
                                         'Fatt' + QUARTERS[i]]
    # merge all quarters
    merged_df = []
    for i in range(len(quarters_dataframe)):
        merged_df = pd.merge(merged_df,
                             quarters_dataframe[i], how='outer', on='Item                ',
                             suffix=(QUARTERS_[i - 1]))

    # calculate APP
    for i in range(len(quarters_dataframe)):
        merged_df['APP' + QUARTERS_[i]] = merged_df['Fatt' + QUARTERS_[i]] / merged_df[
            '        Qtà conc ' + QUARTERS_[i]]
    return merged_df
def list_elaborate(lista_file):
    list_dataframe = file_reader(lista_file, "Sheet0")

    lista_dataframe_tot = filter_list(list_dataframe, tot=True)
    lista_dataframe_gou = filter_list(list_dataframe, gou=True)
    lista_dataframe_ind = filter_list(list_dataframe, ind=True)
    # TODO from here
    grouped_forBU = group_for_BU(list_dataframe)

    save_dataframe_to_excel(os.path.join(OUTPUT, 'list.xlsx'), list_dataframe, 'List')
    print('list.xlsx saved')
    return list_dataframe

def visual_elaborate(visual_files):
    visual_dataframes = []
    for file in visual_files:
        visual_dataframe = file_reader(file, "Sheet0")
        visual_dataframe = filter_visual(visual_dataframe)
        visual_dataframes.append(visual_dataframe)
    visual_dataframe = merge_visualizza_from_all_files(visual_dataframes)
    save_dataframe_to_excel(os.path.join(OUTPUT, 'visual.xlsx'), visual_dataframe, 'Visual')
    print('visual.xlsx saved')
    return visual_dataframe



def budget_elaborate(budget_file):
    budget_dataframe = file_reader(budget_file, "Foglio1")


    return budget_dataframe

def extraction_elaborate(extraction_file):
    extraction_dataframe = file_reader(extraction_file, "Foglio1")


    return extraction_dataframe

def code_elaborate(code_file):
    code_dataframe_total = file_reader(code_file, "Total")
    code_dataframe_industrial = file_reader(code_file, "Industrial")
    code_dataframe_gourmet = file_reader(code_file, "Gourmet")


    return code_dataframe_total




#main
if __name__ == "__main__":
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
    # filter files
    files = [file for file in files if file.endswith('.xlsx') or file.endswith('.xls')]
    # find file that ahve substring as "Visualizza"
    visual_files = [file for file in files if file.find('Visualizza') != -1]
    list_file = [file for file in files if file.find('Lista') != -1]
    budget_file = [file for file in files if file.find('BDG') != -1]
    extraction_file = [file for file in files if file.find('estrazione') != -1]
    code_file = [file for file in files if file.find('codici') != -1]

    list_result = list_elaborate(list_file)
    visual_result = visual_elaborate(visual_files)
    budget_result = budget_elaborate(budget_file)
    extraction_result = extraction_elaborate(extraction_file)
    code_result = code_elaborate(code_file)

    # merge result in one xls different sheet
    # save result in OUTPUT folder
    save_dataframe_to_excel(os.path.join(OUTPUT, 'result.xlsx'), list_result, 'List')
    save_dataframe_to_excel(os.path.join(OUTPUT, 'result.xlsx'), visual_result, 'Visual')
    save_dataframe_to_excel(os.path.join(OUTPUT, 'result.xlsx'), budget_result, 'Budget')
    save_dataframe_to_excel(os.path.join(OUTPUT, 'result.xlsx'), extraction_result, 'Extraction')
    save_dataframe_to_excel(os.path.join(OUTPUT, 'result.xlsx'), code_result, 'Code')
    print('result.xlsx saved')

