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