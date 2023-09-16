import csv
import  decimal
import pandas as pd
import numpy as np

import os.path 
from enum import Enum

from dateutil import parser as date_parser
from datetime import datetime
from timeit import timeit



import re




FORMAT_DATE = "DD-MM-YYYY"

path = os.getcwd()


# pathCSV = path.join("data/template.csv")
pathCSV = "C:\\Code\\redcap-etl-transformation-lambda\\data\\template.csv"

pathNewCSV = "C:\\Code\\redcap-etl-transformation-lambda\\data\\newfile.csv"

# pathExcel = path.join("./data/")
#pathExcel = "C:\\Code\\redcap-etl-transformation-lambda\\data\\SampleDataTbi1987-2022Lite.xlsx"
pathExcel = "C:\\Code\\redcap-etl-transformation-lambda\\data\\SampleDataTbi1987-2022New.xlsx"

PATTERN_FIELD_CALCULATED = "if [FIELD][OPERATOR][EXPRESSION],[VALUE]" # p.e (if epcont_biv=0,0)

PATTERN_PREFIX = "if "

# VALORES A LIMPIAR O IGNORAR , MEJOR HACER LIMPIEZA EN DATAFRAME 
PATTERN_VALUES_ERRORS_TARGET = ["#NULL!"]


pattern_calculated_expression = "if +([0-9,a-z,A-Z,-,_]+)([,>,<,=]+)([0-9]+)(,+)([0-9]+)"




class operations(Enum):
     VALIDATE = "VALIDATE"
     FILL = "FILL"

class operators(Enum):
     EQUAL = "="
     LESS = "<"
     GREATER = ">"
     GREATERIGUAL = ">="
     LESSIGUAL = "<="

assert os.path.isfile(pathCSV)
assert os.path.isfile(pathExcel)

dfTemplateDataCVS = pd.read_csv(pathCSV,encoding='latin-1', sep = ";")

# COPIAMOS EL NUEVO DATAFRAME SIN FILAS, SOLO LA CABECERA
dfFinalDataCVS = dfTemplateDataCVS.copy()
dfFinalDataCVS[dfFinalDataCVS.columns[1:]] = ''

 


dfSampleData = pd.read_excel(pathExcel,decimal=',' ,thousands='.')
column_list  = dfSampleData.columns.to_list()


def is_date_parsing(date_str):
     try:
         
         return bool(date_parser.parse(date_str))
     except ValueError:
         return False


def dropzeros(number):
    mynum = decimal.Decimal(number).normalize()
    # e.g 22000 --> Decimal('2.2E+4')
    return mynum.__trunc__() if not mynum % 1 else float(mynum)

def date_format_from_timestamp(timestamp_str):
    returnDate= ""
    try:
        if isinstance(timestamp_str, str): #STRING FROM DATAFRAME , TRYING DATE CONVERSIN 
            if is_date_parsing(timestamp_str):
               returnDate = date_parser.parse(timestamp_str).strftime('%Y-%m-%d')      
        else:  # TIMESTAMP
            returnDate = timestamp_str.date().strftime('%Y-%m-%d')        
        return  returnDate           
    except ValueError:
        return ''

# RETURN:
# field = groups[0]
# condition = groups[1]
# value_condition = groups[2]
# else_condition = groups[3]

def splitCalculatedField(fieldData):
    if PATTERN_PREFIX in fieldData:                    
        resultformulafield = re.search(pattern_calculated_expression, fieldData)
        if resultformulafield is  None or resultformulafield.groups() is None:                                
            return None , None, None, None                                     
        else:           
            groups = resultformulafield.groups()
            # print (groups) 
            field = groups[0]
            condition = groups[1]
            value_condition = groups[2]
            else_condition = groups[3]
            return field, condition,value_condition,else_condition
    else:
         return None , None, None, None

# TEMPLATE CONTIENE EL NOMBRE DE LOS CAMPOS 
# SEGUNDA FILA , LOS DATOS DE LOS CAMPOS DEL EXCEL A IMPORTAR 
def listSearchItems(value, listToSearch):
    bFound = False
    if value is not None:
        for item in listToSearch:
            if str(value)==item:
                    bFound = True
                    break
    return bFound


new_row_dict_columns_mapping = dict()



if dfTemplateDataCVS is not None:
    #for index,row in dfTemplateDataCVS.iterrows():   ## SOLO COGEMOS EL ROW == 1 QUE ES EL QUE MARCA LOS NOMBRES A BUSCAR 
    #     if row is not None:        # empty rows avoid 

    MappingColumnsCSVRow = dfTemplateDataCVS.iloc[[0]] #FIRST ROW ONLY     


    for col_name in dfTemplateDataCVS.columns:
        # search por column in data 
        
        
        #searchValueList = lambda value,listSearch: [True if value in item else False for item in listSearch]  
        #bColumnExists = searchValueList(col_name,column_list)
        #print (row[col_name])
        failedFields = 0
        # new_row = {'Name': 'David', 'Age': 40}
        


        if dfTemplateDataCVS[col_name] is not None:           
            # isNa = pd.isna(MappingColumnsCSVRow[col_name])
            newColumnFieldName = col_name # VALOR DE COLUMNA PARA INSERTAR EN EL NUEVO DICT
            newColumnFieldValue = ""  # VALOR DEL EXCEL CON EL DATA 
            
            if bool(pd.isna(MappingColumnsCSVRow[col_name])[0]) is  False:    # hay columna de mapeo         
                # [0]  --> Object to native type
                searchValueColumn =  MappingColumnsCSVRow[col_name][0]

                bColumnExists = listSearchItems(searchValueColumn, column_list)
                if bColumnExists is False: 
                    # VERIFICAMOS SI ES UN PATRON VALIDO EN CASO DE QUE SEA CALCULADO
                    #       
                    fieldData,conditionData,value_conditionData,else_conditionData = None,None,None,None
                    fieldData,conditionData,value_conditionData,else_conditionData =  splitCalculatedField(searchValueColumn)                          
                
                    # print (f"resultformulafield {resultformulafield} row[col_name] {row[col_name]}")                                                                    
                    if fieldData is  None:                                
                        print (f"Columna {searchValueColumn} pattern  is not valid (ex. if epcont_biv=0,0)")                                    
                        failedFields = failedFields+1
                    else:                       
                        bCalculatedColumnExists = listSearchItems(fieldData,column_list)
                        if bCalculatedColumnExists is False:
                            print (f"Columna {searchValueColumn} or {fieldData} does no exists")   
                            failedFields = failedFields+1                        
                # else: # DIRECT COLUMN EXISTS
                #     newColumnFieldValue = MappingColumnsCSVRow[col_name]
                if failedFields==0:
                        newColumnFieldValue = searchValueColumn
                        columnItemDict = {newColumnFieldName :newColumnFieldValue}
                        new_row_dict_columns_mapping.update(columnItemDict)
            else:
                columnItemDict = {newColumnFieldName :''} # columna vacia
                new_row_dict_columns_mapping.update(columnItemDict)

                
    new_list_values =  [] # dict() 
    controws = 0;               
    if failedFields == 0:   ## va correcto el proceso 
        for index,row in dfSampleData.iterrows():   ## SOLO COGEMOS EL ROW == 1 QUE ES EL QUE MARCA LOS NOMBRES A BUSCAR 
        # if row is not None:        # empty rows avoid 
            new_dict_values = dict()
            for itemkey  in new_row_dict_columns_mapping:
                    
                    # VERIFICAMOS SI ES UN CAMPO CALCULADO  (debug)
                    #if itemkey == "ocular_admision":
                        #print ("f{itemkey}")
                    if PATTERN_PREFIX in new_row_dict_columns_mapping[itemkey]:
                        fData,cData,vconditionData,falseconditionData = None,None,None,None

                        calculatedFieldValue = new_row_dict_columns_mapping[itemkey]

                        fData,cData,vconditionData,falseconditionData =  splitCalculatedField(calculatedFieldValue) 

                        calculatedFieldValue = 0 
                        if cData == operators.EQUAL:
                            if vconditionData == row[fData]:
                                calculatedFieldValue = vconditionData 
                            else:
                                 calculatedFieldValue = falseconditionData 
                        if cData == operators.GREATER:
                            if row[fData] > vconditionData:
                                calculatedFieldValue = vconditionData 
                            else:
                                 calculatedFieldValue = falseconditionData 
                        if cData == operators.GREATERIGUAL:
                            if row[fData] >= vconditionData:
                                calculatedFieldValue = vconditionData 
                            else:
                                 calculatedFieldValue = falseconditionData 
                        if cData == operators.LESS:
                            if row[fData] < vconditionData:
                                calculatedFieldValue = vconditionData 
                            else:
                                 calculatedFieldValue = falseconditionData 
                        if cData == operators.LESSIGUAL:
                            if vconditionData <= row[fData]:
                                calculatedFieldValue = vconditionData 
                            else:
                                 calculatedFieldValue = falseconditionData                                           
                        final_data_redcap_row_column = {itemkey : calculatedFieldValue} # TO CHANGED WITH EVALUATE EXPRESSION 
                    else:

                        # DD-MM-YYYY (NO HOURS) 
                        # . FOR DECIMAL SEPARATOR 
                        # 
                        if new_row_dict_columns_mapping[itemkey] == ''  or pd.isna(row[new_row_dict_columns_mapping[itemkey]]) or row[new_row_dict_columns_mapping[itemkey]] in PATTERN_VALUES_ERRORS_TARGET:  ## contador field 
                            final_data_redcap_row_column = {itemkey : controws if itemkey=='record_id' else  ''}
                        else:
                            if isinstance(row[new_row_dict_columns_mapping[itemkey]], float): # 
                                rounded  = row[new_row_dict_columns_mapping[itemkey]] #  round(row[new_row_dict_columns_mapping[itemkey]], 6)                                
                                rounded = dropzeros(rounded)
                                # roundedDecimalFormat = str(rounded).replace(".",",")
                                final_data_redcap_row_column = {itemkey : rounded  }
                            else:
                                if isinstance(row[new_row_dict_columns_mapping[itemkey]], int): #                                   
                                    final_data_redcap_row_column = {itemkey : row[new_row_dict_columns_mapping[itemkey]]  }
                                else: #timestamp
                                    dateformatValue = date_format_from_timestamp(row[new_row_dict_columns_mapping[itemkey]])
                                    if dateformatValue !='':                                    
                                        final_data_redcap_row_column = {itemkey : dateformatValue}
                                    else:
                                        final_data_redcap_row_column = {itemkey : ''}    
                    new_dict_values.update(final_data_redcap_row_column)                                

            # END OF DATA ROW, UPDATING TO THE ROW OF REDCAP TEMPLATE 
            # INDEX IS REQUIRED WITH 
            new_list_values.append(new_dict_values)
            controws = controws + 1;                       
        index_data = np.arange(controws)
        dfFinalDataCVS = pd.DataFrame.from_dict(new_list_values)
        dfFinalDataCVS.to_csv(pathNewCSV,sep = ';', index=False)

