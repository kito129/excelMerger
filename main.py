from datetime import datetime
import os, csv, sys
import openpyxl
import logging
import pandas as pd

# global variables
INPUT_PATH = "./inputData/"
EXPORT_PATH = "./exportData/"

# logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# main
dateOfRun = datetime.now().strftime('%d_%m_%Y_%H_%M_%S')    
logging.info("Start main")

# check for path
if not os.path.exists(EXPORT_PATH):
    try:
        os.mkdir(EXPORT_PATH)
    except:
        logging.error("Export Path not exists and unable to Create")
        sys.exit(0)

if not os.path.exists(INPUT_PATH):
    logging.error("Input Path not exists")
    sys.exit(0)

# create merged file with a temp empty sheet
files = os.listdir(INPUT_PATH)  
out_file = os.path.join(os.path.abspath(EXPORT_PATH), f'merged_{dateOfRun}.xlsx')
df_total = pd.DataFrame()
df_total.to_excel(out_file)
workbook=openpyxl.load_workbook(out_file)
temp_sheet = workbook['Sheet1']
temp_sheet.title = 'TempExcelSheetForDeleting'
workbook.save(out_file)

# copy all sheets from all files to the merged file
for file in files:
    if file.endswith('.xls') or file.endswith('.xlsx'):
        excel_file = pd.ExcelFile(os.path.join(INPUT_PATH, file))
        sheets = excel_file.sheet_names
        logging.info("Open file: %s", file)
        # copy all sheets from the file
        for sheet in sheets:
            df = excel_file.parse(sheet_name = sheet)
            sheet = sheet + "_" + file.split(".")[0]
            # write to merged file
            with pd.ExcelWriter(out_file,mode='a') as writer:  
                df.to_excel(writer, sheet_name=f"{sheet}", index=False)
                logging.info("Sheet to copy: %s", sheet)
        
# delete the first empty sheet
workbook=openpyxl.load_workbook(out_file)
std=workbook["TempExcelSheetForDeleting"]
workbook.remove(std)
workbook.save(out_file)

# end
logging.info("Saved to file: %s", out_file)
logging.info("Done")