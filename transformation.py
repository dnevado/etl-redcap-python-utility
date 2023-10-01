# COMPROBAR
# row[fData] PUEDE SER ' ' O NUMERICO 
# vconditionData == row[fData]:





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

COMPLETED_STR = "2"
UNVERIFIED_STR = "1"

path = os.getcwd()


# pathCSV = path.join("data/template.csv")
# pathCSV = "C:\\Code\\redcap-etl-transformation-lambda\\data\\template.csv"
pathCSV = os.path.abspath(input("Relative File path with the REDCAP Project Template with updated mapping files on Row 2 (Default data/template.csv)") or "data\\template.csv")
pathCSV = pathCSV.strip()
pathNewCSV = os.path.abspath(input("Relative  CSV  File Path Converted  (Default data/newfile.csv)") or "data\\newfile.csv")
pathNewCSV = pathNewCSV.strip()
# pathExcel = path.join("./data/")
#pathExcel = "C:\\Code\\redcap-etl-transformation-lambda\\data\\SampleDataTbi1987-2022Lite.xlsx"
# pathExcel = "C:\\Code\\redcap-etl-transformation-lambda\\data\\SampleDataTbi1987-2022New.xlsx"
pathExcel = os.path.abspath(input("Relative  Excel Dataset File (Default \data\SampleData.xlsx)" ) or "data\\SampleData.xlsx")
pathExcel = pathExcel.strip()

print (pathNewCSV)
print (pathCSV)
print (pathExcel)

PATTERN_FIELD_CALCULATED = "if [FIELD][OPERATOR][EXPRESSION],[VALUE]" # p.e (if epcont_biv=0,0)

PATTERN_PREFIX = "if "

# VALORES A LIMPIAR O IGNORAR , MEJOR HACER LIMPIEZA EN DATAFRAME 
PATTERN_VALUES_ERRORS_TARGET = ["#NULL!", ' ']


pattern_calculated_expression = "if +([0-9,a-z,A-Z,-,_]+)([,>,<,=]+)([0-9]+)(,+)([0-9]+)"

DEFAULT_CALCULATED_FIELDS_VALUES_FALSE_CONDITION = 0


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
# true_condition = groups[3]
# 'if subd_1tc=1,1'

def splitCalculatedField(fieldData):
    if PATTERN_PREFIX in fieldData:                    
        resultformulafield = re.search(pattern_calculated_expression, fieldData)
        if resultformulafield is  None or resultformulafield.groups() is None:                                
            return None , None, None, None, None                                   
        else:           
            groups = resultformulafield.groups()
            # print (groups) 
            field = groups[0]
            condition = groups[1]
            value_condition = str(groups[2])
            true_condition = str(groups[4])
            return field, condition,value_condition,true_condition, DEFAULT_CALCULATED_FIELDS_VALUES_FALSE_CONDITION
    else:
         return None , None, None, None, None

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


print ("Starting validation entry data..")
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
                    fieldData,conditionData,value_conditionData,true_condition,else_conditionData = None,None,None,None,None
                    fieldData,conditionData,value_conditionData,true_condition,else_conditionData =  splitCalculatedField(searchValueColumn)                          
                
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
        print ("No wrong fields, creating mapping process")
        for index,row in dfSampleData.iterrows():   ## SOLO COGEMOS EL ROW == 1 QUE ES EL QUE MARCA LOS NOMBRES A BUSCAR 
        # if row is not None:        # empty rows avoid 
            new_dict_values = dict()
            for itemkey  in new_row_dict_columns_mapping:
                    
                    
                    if PATTERN_PREFIX in new_row_dict_columns_mapping[itemkey]:
                        fData,cData,vconditionData,true_condition,falseconditionData = None,None,None,None, None

                        calculatedFieldValue = new_row_dict_columns_mapping[itemkey]

                        fData,cData,vconditionData,true_condition,falseconditionData =  splitCalculatedField(calculatedFieldValue) 
                        
                        if fData is not None and row[fData] is not None and str(row[fData]).strip()!='': 
                           
                            mappingFieldValue = str(row[fData])

                            if cData == operators.EQUAL.value:
                                if vconditionData == mappingFieldValue:
                                    calculatedFieldValue = true_condition 
                                else:
                                    calculatedFieldValue = falseconditionData 
                            if cData == operators.GREATER.value:
                                if mappingFieldValue > vconditionData:
                                    calculatedFieldValue = true_condition 
                                else:
                                    calculatedFieldValue = falseconditionData 
                            if cData == operators.GREATERIGUAL.value:
                                if mappingFieldValue >= vconditionData:
                                    calculatedFieldValue = true_condition 
                                else:
                                    calculatedFieldValue = falseconditionData 
                            if cData == operators.LESS.value:
                                if mappingFieldValue < vconditionData:
                                    calculatedFieldValue = true_condition 
                                else:
                                    calculatedFieldValue = falseconditionData 
                            if cData == operators.LESSIGUAL.value:
                                if vconditionData <= mappingFieldValue:
                                    calculatedFieldValue = true_condition 
                                else:
                                    calculatedFieldValue = falseconditionData                                           
                            final_data_redcap_row_column = {itemkey : calculatedFieldValue} # TO CHANGED WITH EVALUATE EXPRESSION 
                    else:

                        # DD-MM-YYYY (NO HOURS) 
                        # . FOR DECIMAL SEPARATOR 
                        # 

                        if new_row_dict_columns_mapping[itemkey] == ''  or pd.isna(row[new_row_dict_columns_mapping[itemkey]]) or row[new_row_dict_columns_mapping[itemkey]] in PATTERN_VALUES_ERRORS_TARGET:  ## contador field 
                            final_data_redcap_row_column = {itemkey : controws+1 if itemkey=='record_id' else  ''}
                        else:
                            if itemkey == "dias_isquemia" :
                               debug  = 1
                            if isinstance(row[new_row_dict_columns_mapping[itemkey]], float): # 
                                rounded  = row[new_row_dict_columns_mapping[itemkey]] #  round(row[new_row_dict_columns_mapping[itemkey]], 6)                                
                                rounded = dropzeros(rounded)
                                # roundedDecimalFormat = str(rounded).replace(".",",")
                                final_data_redcap_row_column = {itemkey : rounded  }
                            else:
                                if isinstance(row[new_row_dict_columns_mapping[itemkey]], int)  or (isinstance(row[new_row_dict_columns_mapping[itemkey]], str) and row[new_row_dict_columns_mapping[itemkey]].isnumeric()): #  isinstance(row[new_row_dict_columns_mapping[itemkey]], int): #                                   
                                    final_data_redcap_row_column = {itemkey : row[new_row_dict_columns_mapping[itemkey]]  }
                                else: #timestamp
                                    dateformatValue = date_format_from_timestamp(row[new_row_dict_columns_mapping[itemkey]])
                                    if dateformatValue.strip() !='':                                    
                                        final_data_redcap_row_column = {itemkey : dateformatValue}
                                    else:
                                        final_data_redcap_row_column = {itemkey : ''}    
                    new_dict_values.update(final_data_redcap_row_column)                                
                    # VERIFICAMOS SI ES UN CAMPO CALCULADO  (debug)
                    # if itemkey == "dias_isquemia" and new_row_dict_columns_mapping[itemkey].strip()!='':
                    #    print (f"{itemkey}  {new_row_dict_columns_mapping[itemkey]}  {row[new_row_dict_columns_mapping[itemkey]]} {final_data_redcap_row_column}")
            # END OF DATA ROW, UPDATING TO THE ROW OF REDCAP TEMPLATE 
            # INDEX IS REQUIRED WITH 
            new_list_values.append(new_dict_values)
            controws = controws + 1;                       
        index_data = np.arange(1,controws-1)
        dfFinalDataCVS = pd.DataFrame.from_dict(new_list_values)
        dfFinalDataCVS.loc[:, "hoja_general_recogida_datos_complete"] = COMPLETED_STR
        dfFinalDataCVS.loc[:,"primer_tc_complete"] = COMPLETED_STR
        dfFinalDataCVS.loc[:,"peor_tc_complete"] = COMPLETED_STR
        dfFinalDataCVS.loc[:,"rm_complete"] = UNVERIFIED_STR
        dfFinalDataCVS.loc[:,"lesin_cerebrovascular_traumatica_complete"] = UNVERIFIED_STR
        dfFinalDataCVS.loc[:,"tratamiento_complete"] = COMPLETED_STR
        dfFinalDataCVS.loc[:,"evolucin_complete"] = UNVERIFIED_STR

        print (f"Writing file to {pathNewCSV}") 
        dfFinalDataCVS.to_csv(pathNewCSV,sep = ';', index=False)

