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
        dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
        print('saved file', path)


# function to filter budget files (gourmet version)
def filter_budget_gou(input_dataframe):
    grouped_df = input_dataframe.groupby(['articolo', 'mese', 'channel'], as_index=False).agg(
        {'qta': 'sum', 'mese': 'first', 'channel': 'first'})
    gou_df = grouped_df[grouped_df['channel'] == 'GOU       ']
    pivot_df = gou_df.pivot_table(index='articolo', columns='mese', values='qta').reset_index()
    return pivot_df


# function to filter budget files (industrial version)
def filter_budget_ind(input_dataframe):
    grouped_df = input_dataframe.groupby(['articolo', 'mese', 'channel'], as_index=False).agg(
        {'qta': 'sum', 'mese': 'first', 'channel': 'first'})
    ind_df = grouped_df[grouped_df['channel'] == 'FOOD      ']
    pivot_df = ind_df.pivot_table(index='articolo', columns='mese', values='qta').reset_index()
    return pivot_df


# function to filter budget files (total version)
def filter_budget_tot(input_dataframe):
    grouped_df = input_dataframe.groupby(['articolo', 'mese'], as_index=False).agg({'qta': 'sum', 'mese': 'first'})
    pivot_df = grouped_df.pivot_table(index='articolo', columns='mese', values='qta').reset_index()
    return pivot_df


# function to filter "estrazione" files (gourmet version)
def filter_estrazione_gou(input_dataframe):
    grouped_df = input_dataframe.groupby(['articolo', 'mese', 'macro_channel'], as_index=False).agg(
        {'qta': 'sum', 'mese': 'first', 'macro_channel': 'first'})
    gou_df = grouped_df[grouped_df['macro_channel'] == 'GOURMET ']
    pivot_df = gou_df.pivot_table(index='articolo', columns='mese', values='qta').reset_index()
    return pivot_df


# function to filter "estrazione" files (industrial version)
def filter_estrazione_ind(input_dataframe):
    filtered_df = input_dataframe[input_dataframe['macro_channel'].isin(['FOOD MANUFACTURERS       ', 'INTERCOMPANY'])]
    grouped_df = filtered_df.groupby(['articolo', 'mese'], as_index=False).agg({'qta': 'sum', 'mese': 'first'})
    pivot_df = grouped_df.pivot_table(index='articolo', columns='mese', values='qta').reset_index()
    return pivot_df


# function to filter "estrazione" files (total version)
def filter_estrazione_tot(input_dataframe):
    grouped_df = input_dataframe.groupby(['articolo', 'mese'], as_index=False).agg({'qta': 'sum', 'mese': 'first'})
    pivot_df = grouped_df.pivot_table(index='articolo', columns='mese', values='qta').reset_index()
    return pivot_df


def filter_lista_tot(input_dataframe):
    input_dataframe['BU'] = input_dataframe['Customer Group'].fillna(input_dataframe['Bus'])
    input_dataframe['Fatt'] = input_dataframe['  Purchase Price '] * input_dataframe['        Qtà conc ']
    input_dataframe[' Val.a'] = pd.to_datetime(input_dataframe[' Val.a'])
    input_dataframe = input_dataframe[input_dataframe['Stt'] == 40]
    input_dataframe1 = input_dataframe[input_dataframe['TiC'] != 'I02']
    dataframe_trimestre1 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q1']
    grouped_df1 = dataframe_trimestre1.groupby(['Item                '], as_index=False).agg(
        {'        Qtà conc ': 'sum',
         '         Qtà acq ': 'sum', '     Qta Residua ': 'sum', '    Qtà ricevuta': 'sum', 'Fatt': 'sum'})
    dataframe_trimestre2 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_df2 = dataframe_trimestre2.groupby(['Item                '], as_index=False).agg(
        {'        Qtà conc ': 'sum',
         '         Qtà acq ': 'sum', '     Qta Residua ': 'sum', '    Qtà ricevuta': 'sum', 'Fatt': 'sum'})
    dataframe_trimestre3 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_df3 = dataframe_trimestre3.groupby(['Item                '], as_index=False).agg(
        {'        Qtà conc ': 'sum',
         '         Qtà acq ': 'sum', '     Qta Residua ': 'sum', '    Qtà ricevuta': 'sum', 'Fatt': 'sum'})
    dataframe_trimestre4 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_df4 = dataframe_trimestre4.groupby(['Item                '], as_index=False).agg(
        {'        Qtà conc ': 'sum',
         '         Qtà acq ': 'sum', '     Qta Residua ': 'sum', '    Qtà ricevuta': 'sum', 'Fatt': 'sum'})
    merged_df = pd.merge(grouped_df1, grouped_df2, how='outer', on='Item                ', suffixes=('Q1', 'Q2'))
    merged_df = pd.merge(merged_df, grouped_df3, how='outer', on='Item                ', suffixes=('', ' Q3'))
    merged_df = pd.merge(merged_df, grouped_df4, how='outer', on='Item                ', suffixes=('', 'Q4'))
    merged_df['APPQ1'] = merged_df['FattQ1'] / merged_df['        Qtà conc Q1']
    merged_df['APPQ2'] = merged_df['FattQ2'] / merged_df['        Qtà conc Q2']
    merged_df['APPQ3'] = merged_df['Fatt'] / merged_df['        Qtà conc ']
    merged_df['APPQ4'] = merged_df['FattQ4'] / merged_df['        Qtà conc Q4']
    listini_dataframe = input_dataframe[input_dataframe['TiC'] == 'I02']
    listini_trimestre1 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q1']
    grouped_list1 = listini_trimestre1.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre2 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_list2 = listini_trimestre2.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre3 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_list3 = listini_trimestre3.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre4 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_list4 = listini_trimestre4.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    merged_dflist = pd.merge(grouped_list1, grouped_list2, how='outer', on='Item                ',
                             suffixes=('Q1', 'Q2'))
    merged_dflist = pd.merge(merged_dflist, grouped_list3, how='outer', on='Item                ', suffixes=('', ' Q3'))
    merged_dflist = pd.merge(merged_dflist, grouped_list4, how='outer', on='Item                ', suffixes=('', 'Q4'))
    merged_df = pd.merge(merged_df, merged_dflist, how='outer', on='Item                ')
    merged_df['  Purchase Price '] = merged_df['  Purchase Price '].fillna(merged_df['  Purchase Price Q4'])
    merged_df['  Purchase Price Q2'] = merged_df['  Purchase Price Q2'].fillna(merged_df['  Purchase Price '])
    merged_df['  Purchase Price Q1'] = merged_df['  Purchase Price Q1'].fillna(merged_df['  Purchase Price Q2'])
    merged_df['APPQ1'] = merged_df['APPQ1'].fillna(merged_df['  Purchase Price Q1'])
    merged_df['APPQ2'] = merged_df['APPQ2'].fillna(merged_df['  Purchase Price Q2'])
    merged_df['APPQ3'] = merged_df['APPQ3'].fillna(merged_df['  Purchase Price '])
    merged_df['APPQ4'] = merged_df['APPQ4'].fillna(merged_df['  Purchase Price Q4'])
    todelete = ['  Purchase Price Q1', '  Purchase Price Q2', '  Purchase Price ', '  Purchase Price Q4']
    merged_df = merged_df.drop(columns=todelete)
    return merged_df


def filter_lista_gou(input_dataframe):
    input_dataframe['BU'] = input_dataframe['Customer Group'].fillna(input_dataframe['Bus'])
    input_dataframe['Fatt'] = input_dataframe['  Purchase Price '] * input_dataframe['        Qtà conc ']
    input_dataframe[' Val.a'] = pd.to_datetime(input_dataframe[' Val.a'])
    input_dataframe1 = input_dataframe[input_dataframe['BU'] == 'I75']
    input_dataframe1 = input_dataframe1[input_dataframe1['Stt'] == 40]
    input_dataframe1 = input_dataframe1[input_dataframe1['TiC'] != 'I02']
    dataframe_trimestre1 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q1']
    grouped_df1 = dataframe_trimestre1.groupby(['Item                '], as_index=False).agg(
        {'        Qtà conc ': 'sum',
         '         Qtà acq ': 'sum', '     Qta Residua ': 'sum', '    Qtà ricevuta': 'sum', 'Fatt': 'sum'})
    dataframe_trimestre2 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_df2 = dataframe_trimestre2.groupby(['Item                '], as_index=False).agg(
        {'        Qtà conc ': 'sum',
         '         Qtà acq ': 'sum', '     Qta Residua ': 'sum', '    Qtà ricevuta': 'sum', 'Fatt': 'sum'})
    dataframe_trimestre3 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_df3 = dataframe_trimestre3.groupby(['Item                '], as_index=False).agg(
        {'        Qtà conc ': 'sum',
         '         Qtà acq ': 'sum', '     Qta Residua ': 'sum', '    Qtà ricevuta': 'sum', 'Fatt': 'sum'})
    dataframe_trimestre4 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_df4 = dataframe_trimestre4.groupby(['Item                '], as_index=False).agg(
        {'        Qtà conc ': 'sum',
         '         Qtà acq ': 'sum', '     Qta Residua ': 'sum', '    Qtà ricevuta': 'sum', 'Fatt': 'sum'})
    merged_df = pd.merge(grouped_df1, grouped_df2, how='outer', on='Item                ', suffixes=('Q1', 'Q2'))
    merged_df = pd.merge(merged_df, grouped_df3, how='outer', on='Item                ', suffixes=('', ' Q3'))
    merged_df = pd.merge(merged_df, grouped_df4, how='outer', on='Item                ', suffixes=('', 'Q4'))
    merged_df['APPQ1'] = merged_df['FattQ1'] / merged_df['        Qtà conc Q1']
    merged_df['APPQ2'] = merged_df['FattQ2'] / merged_df['        Qtà conc Q2']
    merged_df['APPQ3'] = merged_df['Fatt'] / merged_df['        Qtà conc ']
    merged_df['APPQ4'] = merged_df['FattQ4'] / merged_df['        Qtà conc Q4']
    listini_dataframe = input_dataframe[input_dataframe['TiC'] == 'I02']
    listini_trimestre1 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q1']
    grouped_list1 = listini_trimestre1.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre2 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_list2 = listini_trimestre2.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre3 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_list3 = listini_trimestre3.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre4 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_list4 = listini_trimestre4.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    merged_dflist = pd.merge(grouped_list1, grouped_list2, how='outer', on='Item                ',
                             suffixes=('Q1', 'Q2'))
    merged_dflist = pd.merge(merged_dflist, grouped_list3, how='outer', on='Item                ', suffixes=('', ' Q3'))
    merged_dflist = pd.merge(merged_dflist, grouped_list4, how='outer', on='Item                ', suffixes=('', 'Q4'))
    merged_df = pd.merge(merged_df, merged_dflist, how='outer', on='Item                ')
    merged_df['  Purchase Price '] = merged_df['  Purchase Price '].fillna(merged_df['  Purchase Price Q4'])
    merged_df['  Purchase Price Q2'] = merged_df['  Purchase Price Q2'].fillna(merged_df['  Purchase Price '])
    merged_df['  Purchase Price Q1'] = merged_df['  Purchase Price Q1'].fillna(merged_df['  Purchase Price Q2'])
    merged_df['APPQ1'] = merged_df['APPQ1'].fillna(merged_df['  Purchase Price Q1'])
    merged_df['APPQ2'] = merged_df['APPQ2'].fillna(merged_df['  Purchase Price Q2'])
    merged_df['APPQ3'] = merged_df['APPQ3'].fillna(merged_df['  Purchase Price '])
    merged_df['APPQ4'] = merged_df['APPQ4'].fillna(merged_df['  Purchase Price Q4'])
    todelete = ['  Purchase Price Q1', '  Purchase Price Q2', '  Purchase Price ', '  Purchase Price Q4']
    merged_df = merged_df.drop(columns=todelete)
    return merged_df


def filter_lista_ind(input_dataframe):
    input_dataframe['BU'] = input_dataframe['Customer Group'].fillna(input_dataframe['Bus'])
    input_dataframe['Fatt'] = input_dataframe['  Purchase Price '] * input_dataframe['        Qtà conc ']
    input_dataframe[' Val.a'] = pd.to_datetime(input_dataframe[' Val.a'])
    input_dataframe1 = input_dataframe[input_dataframe['BU'].isin(['I71', 'I72'])]
    input_dataframe1 = input_dataframe1[input_dataframe1['Stt'] == 40]
    input_dataframe1 = input_dataframe1[input_dataframe1['TiC'] != 'I02']
    dataframe_trimestre1 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q1']
    grouped_df1 = dataframe_trimestre1.groupby(['Item                '], as_index=False).agg(
        {'        Qtà conc ': 'sum',
         '         Qtà acq ': 'sum', '     Qta Residua ': 'sum', '    Qtà ricevuta': 'sum', 'Fatt': 'sum'})
    dataframe_trimestre2 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_df2 = dataframe_trimestre2.groupby(['Item                '], as_index=False).agg(
        {'        Qtà conc ': 'sum',
         '         Qtà acq ': 'sum', '     Qta Residua ': 'sum', '    Qtà ricevuta': 'sum', 'Fatt': 'sum'})
    dataframe_trimestre3 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_df3 = dataframe_trimestre3.groupby(['Item                '], as_index=False).agg(
        {'        Qtà conc ': 'sum',
         '         Qtà acq ': 'sum', '     Qta Residua ': 'sum', '    Qtà ricevuta': 'sum', 'Fatt': 'sum'})
    dataframe_trimestre4 = input_dataframe1[input_dataframe1[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_df4 = dataframe_trimestre4.groupby(['Item                '], as_index=False).agg(
        {'        Qtà conc ': 'sum',
         '         Qtà acq ': 'sum', '     Qta Residua ': 'sum', '    Qtà ricevuta': 'sum', 'Fatt': 'sum'})
    merged_df = pd.merge(grouped_df1, grouped_df2, how='outer', on='Item                ', suffixes=('Q1', 'Q2'))
    merged_df = pd.merge(merged_df, grouped_df3, how='outer', on='Item                ', suffixes=('', ' Q3'))
    merged_df = pd.merge(merged_df, grouped_df4, how='outer', on='Item                ', suffixes=('', 'Q4'))
    merged_df['APPQ1'] = merged_df['FattQ1'] / merged_df['        Qtà conc Q1']
    merged_df['APPQ2'] = merged_df['FattQ2'] / merged_df['        Qtà conc Q2']
    merged_df['APPQ3'] = merged_df['Fatt'] / merged_df['        Qtà conc ']
    merged_df['APPQ4'] = merged_df['FattQ4'] / merged_df['        Qtà conc Q4']
    listini_dataframe = input_dataframe[input_dataframe['TiC'] == 'I02']
    listini_trimestre1 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q1']
    grouped_list1 = listini_trimestre1.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre2 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q2']
    grouped_list2 = listini_trimestre2.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre3 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q3']
    grouped_list3 = listini_trimestre3.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    listini_trimestre4 = listini_dataframe[listini_dataframe[' Val.a'].dt.to_period('Q') == '2024Q4']
    grouped_list4 = listini_trimestre4.groupby(['Item                '], as_index=False).agg(
        {'  Purchase Price ': 'max'})
    merged_dflist = pd.merge(grouped_list1, grouped_list2, how='outer', on='Item                ',
                             suffixes=('Q1', 'Q2'))
    merged_dflist = pd.merge(merged_dflist, grouped_list3, how='outer', on='Item                ', suffixes=('', ' Q3'))
    merged_dflist = pd.merge(merged_dflist, grouped_list4, how='outer', on='Item                ', suffixes=('', 'Q4'))
    merged_df = pd.merge(merged_df, merged_dflist, how='outer', on='Item                ')
    merged_df['  Purchase Price '] = merged_df['  Purchase Price '].fillna(merged_df['  Purchase Price Q4'])
    merged_df['  Purchase Price Q2'] = merged_df['  Purchase Price Q2'].fillna(merged_df['  Purchase Price '])
    merged_df['  Purchase Price Q1'] = merged_df['  Purchase Price Q1'].fillna(merged_df['  Purchase Price Q2'])
    merged_df['APPQ1'] = merged_df['APPQ1'].fillna(merged_df['  Purchase Price Q1'])
    merged_df['APPQ2'] = merged_df['APPQ2'].fillna(merged_df['  Purchase Price Q2'])
    merged_df['APPQ3'] = merged_df['APPQ3'].fillna(merged_df['  Purchase Price '])
    merged_df['APPQ4'] = merged_df['APPQ4'].fillna(merged_df['  Purchase Price Q4'])
    todelete = ['  Purchase Price Q1', '  Purchase Price Q2', '  Purchase Price ', '  Purchase Price Q4']
    merged_df = merged_df.drop(columns=todelete)
    return merged_df


def groupforBU(input_dataframe):
    input_dataframe['BU'] = input_dataframe['Customer Group'].fillna(input_dataframe['Bus'])
    grouped_df = input_dataframe.groupby(['Contrat'], as_index=False).agg({'BU': 'first'})
    return grouped_df


def filter_visual(input_dataframe):
    input_dataframe['Ns N rif'] = pd.to_numeric(input_dataframe['Ns N rif'], errors='coerce')
    input_dataframe = pd.merge(input_dataframe, grouped_forBU, how='outer', left_on='Ns N rif', right_on='Contrat')
    input_dataframe['Dtric+cons'] = input_dataframe['Dt ric'].fillna(input_dataframe['DtCons'])
    input_dataframe['Qtàric+ord'] = input_dataframe['Qtà ricev '].fillna(input_dataframe['QtOrdinata'])
    input_dataframe['Fatt'] = input_dataframe['Prz acquis'] * input_dataframe['Qtàric+ord']
    input_dataframe['Dtric+cons'] = pd.to_datetime(input_dataframe['Dtric+cons'])
    input_dataframe1 = input_dataframe[input_dataframe['Sta'] != 99]
    dataframe_gen = input_dataframe1[input_dataframe1['Dtric+cons'].dt.month == 1]
    grouped_df1 = dataframe_gen.groupby(['C  parte  '], as_index=False).agg(
        {'Qtàric+ord': 'sum', 'Qtà confer': 'sum', 'Qtà ricev ': 'sum', 'Fatt': 'sum'})
    grouped_df1['APP'] = grouped_df1['Fatt'] / grouped_df1['Qtàric+ord']
    dataframe_feb = input_dataframe1[input_dataframe1['Dtric+cons'].dt.month == 2]
    grouped_df2 = dataframe_feb.groupby(['C  parte  '], as_index=False).agg(
        {'Qtàric+ord': 'sum', 'Qtà confer': 'sum', 'Qtà ricev ': 'sum', 'Fatt': 'sum'})
    grouped_df2['APP'] = grouped_df2['Fatt'] / grouped_df2['Qtàric+ord']
    dataframe_mar = input_dataframe1[input_dataframe1['Dtric+cons'].dt.month == 3]
    grouped_df3 = dataframe_mar.groupby(['C  parte  '], as_index=False).agg(
        {'Qtàric+ord': 'sum', 'Qtà confer': 'sum', 'Qtà ricev ': 'sum', 'Fatt': 'sum'})
    grouped_df3['APP'] = grouped_df3['Fatt'] / grouped_df3['Qtàric+ord']
    dataframe_apr = input_dataframe1[input_dataframe1['Dtric+cons'].dt.month == 4]
    grouped_df4 = dataframe_apr.groupby(['C  parte  '], as_index=False).agg(
        {'Qtàric+ord': 'sum', 'Qtà confer': 'sum', 'Qtà ricev ': 'sum', 'Fatt': 'sum'})
    grouped_df4['APP'] = grouped_df4['Fatt'] / grouped_df4['Qtàric+ord']
    dataframe_mag = input_dataframe1[input_dataframe1['Dtric+cons'].dt.month == 5]
    grouped_df5 = dataframe_mag.groupby(['C  parte  '], as_index=False).agg(
        {'Qtàric+ord': 'sum', 'Qtà confer': 'sum', 'Qtà ricev ': 'sum', 'Fatt': 'sum'})
    grouped_df5['APP'] = grouped_df5['Fatt'] / grouped_df5['Qtàric+ord']
    dataframe_giu = input_dataframe1[input_dataframe1['Dtric+cons'].dt.month == 6]
    grouped_df6 = dataframe_giu.groupby(['C  parte  '], as_index=False).agg(
        {'Qtàric+ord': 'sum', 'Qtà confer': 'sum', 'Qtà ricev ': 'sum', 'Fatt': 'sum'})
    grouped_df6['APP'] = grouped_df6['Fatt'] / grouped_df6['Qtàric+ord']
    dataframe_lug = input_dataframe1[input_dataframe1['Dtric+cons'].dt.month == 7]
    grouped_df7 = dataframe_lug.groupby(['C  parte  '], as_index=False).agg(
        {'Qtàric+ord': 'sum', 'Qtà confer': 'sum', 'Qtà ricev ': 'sum', 'Fatt': 'sum'})
    grouped_df7['APP'] = grouped_df7['Fatt'] / grouped_df7['Qtàric+ord']
    dataframe_ago = input_dataframe1[input_dataframe1['Dtric+cons'].dt.month == 8]
    grouped_df8 = dataframe_ago.groupby(['C  parte  '], as_index=False).agg(
        {'Qtàric+ord': 'sum', 'Qtà confer': 'sum', 'Qtà ricev ': 'sum', 'Fatt': 'sum'})
    grouped_df8['APP'] = grouped_df8['Fatt'] / grouped_df8['Qtàric+ord']
    dataframe_set = input_dataframe1[input_dataframe1['Dtric+cons'].dt.month == 9]
    grouped_df9 = dataframe_set.groupby(['C  parte  '], as_index=False).agg(
        {'Qtàric+ord': 'sum', 'Qtà confer': 'sum', 'Qtà ricev ': 'sum', 'Fatt': 'sum'})
    grouped_df9['APP'] = grouped_df9['Fatt'] / grouped_df9['Qtàric+ord']
    dataframe_ott = input_dataframe1[input_dataframe1['Dtric+cons'].dt.month == 10]
    grouped_df10 = dataframe_ott.groupby(['C  parte  '], as_index=False).agg(
        {'Qtàric+ord': 'sum', 'Qtà confer': 'sum', 'Qtà ricev ': 'sum', 'Fatt': 'sum'})
    grouped_df10['APP'] = grouped_df10['Fatt'] / grouped_df10['Qtàric+ord']
    dataframe_nov = input_dataframe1[input_dataframe1['Dtric+cons'].dt.month == 11]
    grouped_df11 = dataframe_nov.groupby(['C  parte  '], as_index=False).agg(
        {'Qtàric+ord': 'sum', 'Qtà confer': 'sum', 'Qtà ricev ': 'sum', 'Fatt': 'sum'})
    grouped_df11['APP'] = grouped_df11['Fatt'] / grouped_df11['Qtàric+ord']
    dataframe_dic = input_dataframe1[input_dataframe1['Dtric+cons'].dt.month == 12]
    grouped_df12 = dataframe_dic.groupby(['C  parte  '], as_index=False).agg(
        {'Qtàric+ord': 'sum', 'Qtà confer': 'sum', 'Qtà ricev ': 'sum', 'Fatt': 'sum'})
    grouped_df12['APP'] = grouped_df12['Fatt'] / grouped_df12['Qtàric+ord']
    merged_df = pd.merge(grouped_df1, grouped_df2, how='outer', on='C  parte  ', suffixes=('Gen', 'Feb'))
    merged_df1 = pd.merge(merged_df, grouped_df3, how='outer', on='C  parte  ', suffixes=('', 'Mar'))
    merged_df2 = pd.merge(merged_df1, grouped_df4, how='outer', on='C  parte  ', suffixes=('', 'Apr'))
    merged_df3 = pd.merge(merged_df2, grouped_df5, how='outer', on='C  parte  ', suffixes=('', 'Mag'))
    merged_df4 = pd.merge(merged_df3, grouped_df6, how='outer', on='C  parte  ', suffixes=('', 'Giu'))
    merged_df5 = pd.merge(merged_df4, grouped_df7, how='outer', on='C  parte  ', suffixes=('', 'Lug'))
    merged_df6 = pd.merge(merged_df5, grouped_df8, how='outer', on='C  parte  ', suffixes=('', 'Ago'))
    merged_df7 = pd.merge(merged_df6, grouped_df9, how='outer', on='C  parte  ', suffixes=('', 'Set'))
    merged_df8 = pd.merge(merged_df7, grouped_df10, how='outer', on='C  parte  ', suffixes=('', 'Ott'))
    merged_df9 = pd.merge(merged_df8, grouped_df11, how='outer', on='C  parte  ', suffixes=('', 'Nov'))
    merged_df10 = pd.merge(merged_df9, grouped_df12, how='outer', on='C  parte  ', suffixes=('', 'Dic'))
    return merged_df10


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
            merged_visualizza = merge_visualizza(visual_dataframes)
            visual_dataframes1 = filter_visual(merged_visualizza)
        elif file.find('Lista') != -1:
            lista_dataframe = file_reader(file, "Sheet0")
            lista_dataframe_tot = filter_lista_tot(lista_dataframe)
            lista_dataframe_gou = filter_lista_gou(lista_dataframe)
            lista_dataframe_ind = filter_lista_ind(lista_dataframe)
            grouped_forBU = groupforBU(lista_dataframe)
            # merge_lista1 = pd.merge(lista_dataframe_ind,lista_dataframe_gou, how = 'outer', on = 'Item                ', suffixes=('IND', 'GOU'))
            # merge_lista = pd.merge(merge_lista1,lista_dataframe_tot,how = 'outer', on = 'Item                ', suffixes=('', 'TOT'))
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

# merge Totale
merged_df_tot = pd.merge(codici_dataframe_total, lista_dataframe_tot, how='outer', left_on='Codice parte',
                         right_on='Item                ')
merged_df_tot_bdg = pd.merge(merged_df_tot, budget_dataframe_tot, how='outer', left_on='Codice parte',
                             right_on='articolo', suffixes=('', ' BDG'))
merged_df_tot_bdg_act = pd.merge(merged_df_tot_bdg, estrazione_dataframe_tot, how='outer', left_on='Codice parte',
                                 right_on='articolo', suffixes=('', ' ACT'))
merged_df_tot_bdg_act['BDGQ1'] = merged_df_tot_bdg_act['1'] + merged_df_tot_bdg_act['2'] + merged_df_tot_bdg_act['3']
merged_df_tot_bdg_act['BDGQ2'] = merged_df_tot_bdg_act['4'] + merged_df_tot_bdg_act['5'] + merged_df_tot_bdg_act['6']
merged_df_tot_bdg_act['BDGQ3'] = merged_df_tot_bdg_act['7'] + merged_df_tot_bdg_act['8'] + merged_df_tot_bdg_act['9']
merged_df_tot_bdg_act['BDGQ4'] = merged_df_tot_bdg_act[10] + merged_df_tot_bdg_act[11] + merged_df_tot_bdg_act[12]
merged_df_tot_bdg_act['ACTQ1'] = merged_df_tot_bdg_act['1 ACT'] + merged_df_tot_bdg_act['2 ACT'] + \
                                 merged_df_tot_bdg_act['3 ACT']
merged_df_tot_bdg_act['ACTQ2'] = merged_df_tot_bdg_act['4 ACT'] + merged_df_tot_bdg_act['5 ACT'] + \
                                 merged_df_tot_bdg_act['6 ACT']
merged_df_tot_bdg_act['ACTQ3'] = merged_df_tot_bdg_act['7 ACT'] + merged_df_tot_bdg_act['8 ACT'] + \
                                 merged_df_tot_bdg_act['9 ACT']
new_order = ['Codice parte', 'Macro', 'Micro', 'Nome', 'Codice padre', 'Descrizione padre', 'Baseline Gourmet', 'BDGQ1',
             'ACTQ1', '        Qtà conc Q1', '     Qta Residua Q1',
             '    Qtà ricevutaQ1', 'FattQ1', 'APPQ1', 'BDGQ2', 'ACTQ2', '        Qtà conc Q2', '         Qtà acq Q2',
             '     Qta Residua Q2',
             '    Qtà ricevutaQ2', 'FattQ2', 'APPQ2', 'BDGQ3', 'ACTQ3', '        Qtà conc ', '         Qtà acq ',
             '     Qta Residua ',
             '    Qtà ricevuta', 'Fatt', 'APPQ3', 'BDGQ4', '        Qtà conc Q4', '         Qtà acq Q4',
             '     Qta Residua Q4', '    Qtà ricevutaQ4', 'FattQ4', 'APPQ4']
merged_df_tot_bdg_act = merged_df_tot_bdg_act[new_order]

# merge ind
merged_df_ind = pd.merge(codici_dataframe_industrial, lista_dataframe_ind, how='outer', left_on='Codice parte',
                         right_on='Item                ')
merged_df_ind_bdg = pd.merge(merged_df_ind, budget_dataframe_ind, how='outer', left_on='Codice parte',
                             right_on='articolo', suffixes=('', ' BDG'))
merged_df_ind_bdg_act = pd.merge(merged_df_ind_bdg, estrazione_dataframe_ind, how='outer', left_on='Codice parte',
                                 right_on='articolo', suffixes=('', ' ACT'))
merged_df_ind_bdg_act['BDGQ1'] = merged_df_ind_bdg_act['1'] + merged_df_ind_bdg_act['2'] + merged_df_ind_bdg_act['3']
merged_df_ind_bdg_act['BDGQ2'] = merged_df_ind_bdg_act['4'] + merged_df_ind_bdg_act['5'] + merged_df_ind_bdg_act['6']
merged_df_ind_bdg_act['BDGQ3'] = merged_df_ind_bdg_act['7'] + merged_df_ind_bdg_act['8'] + merged_df_ind_bdg_act['9']
merged_df_ind_bdg_act['BDGQ4'] = merged_df_ind_bdg_act[10] + merged_df_ind_bdg_act[11] + merged_df_ind_bdg_act[12]
merged_df_ind_bdg_act['ACTQ1'] = merged_df_ind_bdg_act['1 ACT'] + merged_df_ind_bdg_act['2 ACT'] + \
                                 merged_df_ind_bdg_act['3 ACT']
merged_df_ind_bdg_act['ACTQ2'] = merged_df_ind_bdg_act['4 ACT'] + merged_df_ind_bdg_act['5 ACT'] + \
                                 merged_df_ind_bdg_act['6 ACT']
merged_df_ind_bdg_act['ACTQ3'] = merged_df_ind_bdg_act['7 ACT'] + merged_df_ind_bdg_act['8 ACT'] + \
                                 merged_df_ind_bdg_act['9 ACT']
new_order = ['Codice parte', 'Macro', 'Micro', 'Nome', 'Codice padre', 'Descrizione padre', 'BDGQ1', 'ACTQ1',
             '        Qtà conc Q1', '     Qta Residua Q1',
             '    Qtà ricevutaQ1', 'FattQ1', 'APPQ1', 'BDGQ2', 'ACTQ2', '        Qtà conc Q2', '         Qtà acq Q2',
             '     Qta Residua Q2',
             '    Qtà ricevutaQ2', 'FattQ2', 'APPQ2', 'BDGQ3', 'ACTQ3', '        Qtà conc ', '         Qtà acq ',
             '     Qta Residua ',
             '    Qtà ricevuta', 'Fatt', 'APPQ3', 'BDGQ4', '        Qtà conc Q4', '         Qtà acq Q4',
             '     Qta Residua Q4', '    Qtà ricevutaQ4', 'FattQ4', 'APPQ4']
merged_df_ind_bdg_act = merged_df_ind_bdg_act[new_order]

# merge gou
merged_df_gou = pd.merge(codici_dataframe_gourmet, lista_dataframe_gou, how='outer', left_on='Codice parte',
                         right_on='Item                ')
merged_df_gou_bdg = pd.merge(merged_df_gou, budget_dataframe_gou, how='outer', left_on='Codice parte',
                             right_on='articolo', suffixes=('', ' BDG'))
merged_df_gou_bdg_act = pd.merge(merged_df_gou_bdg, estrazione_dataframe_gou, how='outer', left_on='Codice parte',
                                 right_on='articolo', suffixes=('', ' ACT'))
merged_df_gou_bdg_act['BDGQ1'] = merged_df_gou_bdg_act['1'] + merged_df_gou_bdg_act['2'] + merged_df_gou_bdg_act['3']
merged_df_gou_bdg_act['BDGQ2'] = merged_df_gou_bdg_act['4'] + merged_df_gou_bdg_act['5'] + merged_df_gou_bdg_act['6']
merged_df_gou_bdg_act['BDGQ3'] = merged_df_gou_bdg_act['7'] + merged_df_gou_bdg_act['8'] + merged_df_gou_bdg_act['9']
merged_df_gou_bdg_act['BDGQ4'] = merged_df_gou_bdg_act[10] + merged_df_gou_bdg_act[11] + merged_df_gou_bdg_act[12]
merged_df_gou_bdg_act['ACTQ1'] = merged_df_gou_bdg_act['1 ACT'] + merged_df_gou_bdg_act['2 ACT'] + \
                                 merged_df_gou_bdg_act['3 ACT']
merged_df_gou_bdg_act['ACTQ2'] = merged_df_gou_bdg_act['4 ACT'] + merged_df_gou_bdg_act['5 ACT'] + \
                                 merged_df_gou_bdg_act['6 ACT']
merged_df_gou_bdg_act['ACTQ3'] = merged_df_gou_bdg_act['7 ACT'] + merged_df_gou_bdg_act['8 ACT'] + \
                                 merged_df_gou_bdg_act['9 ACT']
new_order = ['Codice parte', 'Macro', 'Micro', 'Nome', 'Codice padre', 'Descrizione padre', 'Baseline Gourmet', 'BDGQ1',
             'ACTQ1', '        Qtà conc Q1', '     Qta Residua Q1',
             '    Qtà ricevutaQ1', 'FattQ1', 'APPQ1', 'BDGQ2', 'ACTQ2', '        Qtà conc Q2', '         Qtà acq Q2',
             '     Qta Residua Q2',
             '    Qtà ricevutaQ2', 'FattQ2', 'APPQ2', 'BDGQ3', 'ACTQ3', '        Qtà conc ', '         Qtà acq ',
             '     Qta Residua ',
             '    Qtà ricevuta', 'Fatt', 'APPQ3', 'BDGQ4', '        Qtà conc Q4', '         Qtà acq Q4',
             '     Qta Residua Q4', '    Qtà ricevutaQ4', 'FattQ4', 'APPQ4']
merged_df_gou_bdg_act = merged_df_gou_bdg_act[new_order]

# merge visua
merged_df_visua_tot = pd.merge(codici_dataframe_total, visual_dataframes1, how='outer', left_on='Codice parte',
                               right_on='C  parte  ', suffixes=('', ' PO'))

# test pourpose
test_file = os.path.join(os.path.abspath(OUTPUT), f'merged_{date}.xlsx')
df = pd.DataFrame()
df.to_excel(test_file)
workbook = openpyxl.load_workbook(test_file)
temp_sheet = workbook['Sheet1']
temp_sheet.title = 'TempExcelSheetForDeleting'
workbook.save(test_file)

# save visualizza merged
save_dataframe_to_excel(test_file, visual_dataframes1, "visualizza")
save_dataframe_to_excel(test_file, budget_dataframe_gou, "budget_mensile_gou")
save_dataframe_to_excel(test_file, budget_dataframe_ind, "budget_mensile_ind")
save_dataframe_to_excel(test_file, budget_dataframe_tot, "budget_mensile_tot")
# save_dataframe_to_excel(test_file, lista_dataframe_tot, "listatot")
# save_dataframe_to_excel(test_file, lista_dataframe_gou, "listagou")
# save_dataframe_to_excel(test_file, lista_dataframe_ind, "listaind")
# save_dataframe_to_excel(test_file, grouped_forBU, "BUcontracts")
# save_dataframe_to_excel(test_file, merge_lista,"merge")
save_dataframe_to_excel(test_file, estrazione_dataframe_tot, "estrazione_tot")
save_dataframe_to_excel(test_file, estrazione_dataframe_ind, "estrazione_ind")
save_dataframe_to_excel(test_file, estrazione_dataframe_gou, "estrazione_gou")
save_dataframe_to_excel(test_file, merged_df_tot_bdg_act, "codici total")
save_dataframe_to_excel(test_file, merged_df_ind_bdg_act, "codici ind")
save_dataframe_to_excel(test_file, merged_df_gou_bdg_act, "codici gou")
# save_dataframe_to_excel(test_file, grouped_forBU, "merged_visuaBU")


# delete the first empty sheet
workbook = openpyxl.load_workbook(test_file)
std = workbook["TempExcelSheetForDeleting"]
workbook.remove(std)
workbook.save(test_file)
