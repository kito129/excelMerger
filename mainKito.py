import os, sys
from datetime import datetime
import pandas as pd
import openpyxl

# global variables
INPUT_PATH = "./inputData"
OUTPUT_PATH = "./outputData"
QUARTERS = ['2024Q1', '2024Q2', '2024Q3', '2024Q4']
QUARTERS_ = ['Q1', 'Q2', 'Q3', 'Q4']


# utils functions
def file_reader(path, sheet_name):
    xls = pd.ExcelFile(os.path.join(INPUT_PATH, path))
    df = pd.read_excel(xls, sheet_name)
    # remove special characters and spaces from columns
    df.columns = df.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df.columns = df.columns.str.replace(' ', '')
    return df


def merge_visualizza_from_all_files(input_datas):
    return pd.concat(input_datas)


def save_dataframe_to_excel(path, dataframe, sheet_name):
    global date
    with pd.ExcelWriter(path + date, mode="w") as writer:
        dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
        print('saved file', path)


def append_dataframe_to_excel(path, dataframe, sheet_name):
    global date
    with pd.ExcelWriter(path + date, mode='a') as writer:
        dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
        print('saved file', path)


# group functions
def group_for_BU(input_dataframe):
    input_dataframe['BU'] = input_dataframe['CustomerGroup'].fillna(input_dataframe['Bus'])
    return input_dataframe.groupby(['Contrat'], as_index=False).agg({'BU': 'first'})


def filter_visual(input_dataframe, grouped_for_BU):
    input_dataframe['NsNrif'] = pd.to_numeric(input_dataframe['NsNrif'], errors='coerce')
    pd.merge(input_dataframe, grouped_for_BU, how='outer', left_on='NsNrif', right_on='Contrat')
    input_dataframe['Dtric+cons'] = input_dataframe['Dtric'].fillna(input_dataframe['DtCons'])
    input_dataframe['Qtaric+ord'] = input_dataframe['Qtaricev'].fillna(input_dataframe['QtOrdinata'])
    input_dataframe['Fatt'] = input_dataframe['Przacquis'] * input_dataframe['Qtaric+ord']
    input_dataframe['Dtric+cons'] = pd.to_datetime(input_dataframe['Dtric+cons'])

    merged_df = pd.DataFrame()
    for month in range(1, 13):
        dataframe_month = input_dataframe[input_dataframe['Dtric+cons'].dt.month == month]
        grouped_df = dataframe_month.groupby(['Cparte'], as_index=False).agg({
            'Qtaric+ord': 'sum',
            'Qtaconfer': 'sum',
            'Qtaricev': 'sum',
            'Fatt': 'sum'
        })
        grouped_df['APP'] = grouped_df['Fatt'] / grouped_df['Qtaric+ord']
        merged_df = pd.concat([merged_df, grouped_df], axis=0, ignore_index=True)

    return merged_df


def filter_list(input_dataframe, tot=True, gou=False, ind=False):
    # prepare dataframe
    input_dataframe['BU'] = input_dataframe['CustomerGroup'].fillna(input_dataframe['Bus'])
    input_dataframe['Fatt'] = input_dataframe['PurchasePrice'] * input_dataframe['Qtaconc']
    input_dataframe['Val.a'] = pd.to_datetime(input_dataframe['Val.a'])

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
        temp = input_dataframe[input_dataframe['Val.a'].dt.to_period('Q') == quarter]
        quarters_dataframe.append(
            temp.groupby(['Item'], as_index=False).agg(
                {'Qtaconc': 'sum',
                 'Qtaacq': 'sum',
                 'QtaResidua': 'sum',
                 'Qtaricevuta': 'sum',
                 'Fatt': 'sum'})
        )
    # rename columns
    for i in range(len(quarters_dataframe)):
        quarters_dataframe[i].columns = ['Item',
                                         'Qtaconc' + QUARTERS[i],
                                         'Qtaacq' + QUARTERS[i],
                                         'QtaResidua' + QUARTERS[i],
                                         'Qtaricevuta' + QUARTERS[i],
                                         'Fatt' + QUARTERS[i]]

    # new dataframe
    merged_df = quarters_dataframe[0]
    # merge all quarters
    for i in range(len(quarters_dataframe)):
        merged_df = pd.merge(merged_df, quarters_dataframe[i], how='outer', on='Item', suffixes=('', QUARTERS_[i]))

    for i in range(len(quarters_dataframe)):
        merged_df['APP' + QUARTERS_[i]] = merged_df['Fatt' + QUARTERS[i]] / merged_df[
            'Qtaconc' + QUARTERS[i]]
    return merged_df


def filter_budget(input_dataframe, type):
    chanel = ''
    if type == 'tot':
        grouped_df = input_dataframe.groupby(['articolo', 'mese'], as_index=False).agg({'qta': 'sum', 'mese': 'first'})
        pivot_df = grouped_df.pivot_table(index='articolo', columns='mese', values='qta').reset_index()
    else:
        if type == 'gou':
            chanel = 'GOU'
        elif type == 'ind':
            chanel = 'IND'
        grouped_df = input_dataframe.groupby(['articolo', 'mese', 'channel'], as_index=False).agg(
            {'qta': 'sum', 'mese': 'first', 'channel': 'first'})
        ind_df = grouped_df[grouped_df['channel'] == chanel]
        pivot_df = ind_df.pivot_table(index='articolo', columns='mese', values='qta').reset_index()
    return pivot_df


def filter_extraction(input_dataframe, type):
    if type == 'tot':
        grouped_df = input_dataframe.groupby(['articolo', 'mese'], as_index=False).agg({'qta': 'sum', 'mese': 'first'})
        pivot_df = grouped_df.pivot_table(index='articolo', columns='mese', values='qta').reset_index()
    elif type == 'gou':
        grouped_df = input_dataframe.groupby(['articolo', 'mese', 'macro_channel'], as_index=False).agg(
            {'qta': 'sum', 'mese': 'first', 'macro_channel': 'first'})
        gourmet_df = grouped_df[grouped_df['macro_channel'] == 'GOURMET ']
        pivot_df = gourmet_df.pivot_table(index='articolo', columns='mese', values='qta').reset_index()
    elif type == 'ind':
        filtered_df = input_dataframe[input_dataframe['macro_channel'].isin(['FOODMANUFACTURERS', 'INTERCOMPANY'])]
        grouped_df = filtered_df.groupby(['articolo', 'mese'], as_index=False).agg({'qta': 'sum', 'mese': 'first'})
        pivot_df = grouped_df.pivot_table(index='articolo', columns='mese', values='qta').reset_index()
    return pivot_df


def merge_total(code, list, extraction, budget):
    merged_df_tot = pd.merge(code, list, how='outer', left_on='Codiceparte',
                             right_on='Item')
    merged_df_tot_bdg = pd.merge(merged_df_tot, budget, how='outer', left_on='Codiceparte',
                                 right_on='articolo', suffixes=('', 'BDG'))
    merged_df_tot_bdg_act = pd.merge(merged_df_tot_bdg, extraction, how='outer', left_on='Codiceparte',
                                     right_on='articolo', suffixes=('', 'ACT'))
    merged_df_tot_bdg_act['BDGQ1'] = merged_df_tot_bdg_act['1'] + merged_df_tot_bdg_act['2'] + merged_df_tot_bdg_act[
        '3']
    merged_df_tot_bdg_act['BDGQ2'] = merged_df_tot_bdg_act['4'] + merged_df_tot_bdg_act['5'] + merged_df_tot_bdg_act[
        '6']
    merged_df_tot_bdg_act['BDGQ3'] = merged_df_tot_bdg_act['7'] + merged_df_tot_bdg_act['8'] + merged_df_tot_bdg_act[
        '9']
    merged_df_tot_bdg_act['BDGQ4'] = merged_df_tot_bdg_act[10] + merged_df_tot_bdg_act[11] + merged_df_tot_bdg_act[12]
    merged_df_tot_bdg_act['ACTQ1'] = merged_df_tot_bdg_act['1ACT'] + merged_df_tot_bdg_act['2ACT'] + \
                                     merged_df_tot_bdg_act['3ACT']
    merged_df_tot_bdg_act['ACTQ2'] = merged_df_tot_bdg_act['4ACT'] + merged_df_tot_bdg_act['5ACT'] + \
                                     merged_df_tot_bdg_act['6ACT']
    merged_df_tot_bdg_act['ACTQ3'] = merged_df_tot_bdg_act['7ACT'] + merged_df_tot_bdg_act['8ACT'] + \
                                     merged_df_tot_bdg_act['9ACT']
    # why not the same for all quarters?
    new_order = ['Codiceparte', 'Macro', 'Micro', 'Nome', 'Codicepadre', 'Descrizionepadre', 'BaselineGourmet',
                 'BDGQ1', 'ACTQ1', 'QtàconcQ1', 'QtaResiduaQ1', 'QtaricevutaQ1', 'FattQ1', 'APPQ1',
                 'BDGQ2', 'ACTQ2', 'QtaconcQ2', 'QtaacqQ2', 'QtaResiduaQ2', 'QtàricevutaQ2', 'FattQ2', 'APPQ2',
                 'BDGQ3', 'ACTQ3', 'QtaconcQ3', 'QtàacqQ3', 'QtaResiduaQ3', 'QtaricevutaQ3', 'FattQ3', 'APPQ3',
                 'BDGQ4', 'ACTQ4', 'QtaconcQ4', 'QtàacqQ4', 'QtaResiduaQ4', 'QtaricevutaQ4', 'FattQ4', 'APPQ4']
    return merged_df_tot_bdg_act[new_order]


def final_merger(list_dataframe, visual_dataframe, budget_dataframe, extraction_dataframe, code_dataframe):
    list_total = list_dataframe.get('total')
    list_gourmet = list_dataframe.get('gourmet')
    list_industrial = list_dataframe.get('industrial')
    budget_total = budget_dataframe.get('total')
    budget_gourmet = budget_dataframe.get('gourmet')
    budget_industrial = budget_dataframe.get('industrial')
    extraction_total = extraction_dataframe.get('total')
    extraction_gourmet = extraction_dataframe.get('gourmet')
    extraction_industrial = extraction_dataframe.get('industrial')
    code_total = code_dataframe.get('total')
    code_gourmet = code_dataframe.get('gourmet')
    code_industrial = code_dataframe.get('industrial')

    # merge total
    merged_df_tot_bdg_act = merge_total(code_total, list_total, extraction_total, budget_total)
    # TODO: merge gourmet
    # TODO: merge industrial
    # TODO: merge visual


def group_in_obj(total, inustrial, gourmet):
    return {
        "total": total,
        "gourmet": gourmet,
        "industrial": inustrial
    }


def list_elaborate(lista_file):
    global grouped_for_BU
    list_dataframe = file_reader(lista_file[0], "Sheet0")
    grouped_for_BU = group_for_BU(list_dataframe)
    lista_dataframe_tot = filter_list(list_dataframe, tot=True)
    lista_dataframe_gou = filter_list(list_dataframe, gou=True)
    lista_dataframe_ind = filter_list(list_dataframe, ind=True)
    return group_in_obj(lista_dataframe_tot, lista_dataframe_ind, lista_dataframe_gou)


def visual_elaborate(visual_files):
    global grouped_for_BU
    visual_dataframes = []
    for file in visual_files:
        visual_dataframe = file_reader(file, "Sheet0")
        visual_dataframe = filter_visual(visual_dataframe, grouped_for_BU)
        visual_dataframes.append(visual_dataframe)
    visual_dataframe = merge_visualizza_from_all_files(visual_dataframes)
    save_dataframe_to_excel(os.path.join(OUTPUT_PATH, 'visual.xlsx'), visual_dataframe, 'Visual')
    return visual_dataframe


def budget_elaborate(budget_file):
    budget_dataframe = file_reader(budget_file[0], "Foglio1")
    budget_industrial_dataframe = filter_budget(budget_dataframe, 'ind')
    budget_gourmet_dataframe = filter_budget(budget_dataframe, 'gou')
    budget_tot_dataframe = filter_budget(budget_dataframe, 'tot')
    return group_in_obj(budget_tot_dataframe, budget_industrial_dataframe, budget_gourmet_dataframe)


def extraction_elaborate(extraction_file):
    extraction_dataframe = file_reader(extraction_file[0], "Foglio1")
    extraction_industrial_dataframe = filter_extraction(extraction_dataframe, 'ind')
    extraction_gourmet_dataframe = filter_extraction(extraction_dataframe, 'gou')
    extraction_tot_dataframe = filter_extraction(extraction_dataframe, 'tot')
    return group_in_obj(extraction_tot_dataframe, extraction_industrial_dataframe, extraction_gourmet_dataframe)


def code_elaborate(code_file):
    code_dataframe_total = file_reader(code_file[0], "Total")
    code_dataframe_industrial = file_reader(code_file[0], "Industrial")
    code_dataframe_gourmet = file_reader(code_file[0], "Gourmet")
    return group_in_obj(code_dataframe_total, code_dataframe_industrial, code_dataframe_gourmet)


# main
if __name__ == "__main__":
    # main
    date = '_' + datetime.now().strftime('%d_%m_%Y_%H_%M_%S') + '.xlsx'

    # check of folders if present
    if not os.path.exists(OUTPUT_PATH):
        try:
            os.mkdir(OUTPUT_PATH)
        except:
            print('Error in output path creation')
            sys.exit(0)

    if not os.path.exists(INPUT_PATH):
        sys.exit(0)

    # list all files in INPUT
    files = os.listdir(INPUT_PATH)
    visual_dataframes = []
    grouped_for_BU = None
    # filter files
    files = [file for file in files if file.endswith('.xlsx') or file.endswith('.xls')]
    visual_files = [file for file in files if file.find('Visualizza') != -1]
    list_file = [file for file in files if file.find('Lista') != -1]
    budget_file = [file for file in files if file.find('BDG') != -1]
    extraction_file = [file for file in files if file.find('estrazione') != -1]
    code_file = [file for file in files if file.find('codici') != -1]

    # print lenght of files
    print('Visual files: ', len(visual_files))
    print('List files: ', len(list_file))
    print('Budget files: ', len(budget_file))
    print('Extraction files: ', len(extraction_file))
    print('Code files: ', len(code_file))

    list_result = list_elaborate(list_file)
    visual_result = visual_elaborate(visual_files)
    budget_result = budget_elaborate(budget_file)
    extraction_result = extraction_elaborate(extraction_file)
    code_result = code_elaborate(code_file)

    to_save = final_merger(list_result, visual_result, budget_result, extraction_result, code_result)

    # merge result in one xls different sheet
    # save result in OUTPUT folder

    # final save to fix

    resultFile = os.path.join(OUTPUT_PATH, 'result.xlsx')
    # save_dataframe_to_excel(resultFile, list_result, 'List')
    append_dataframe_to_excel(resultFile, visual_result, 'Visual')
    # append_dataframe_to_excel(resultFile, budget_result, 'Budget')
    # append_dataframe_to_excel(resultFile, extraction_result, 'Extraction')
    # append_dataframe_to_excel(resultFile, code_result, 'Code')
    print('result.xlsx saved')
