from itertools import count
from wsgiref import validate
import pandas as pd
import numpy as np
import GlobalDF as gdf


pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', True)
pd.set_option('max_colwidth', None)

#excel_sheet_name="SourceFile/FormulaV5.xlsx"

#df_formula = pd.DataFrame()
#df_formula = pd.read_excel(excel_sheet_name, sheet_name="Allocation Formula XXXX",skiprows=2,header=None)
#df_coa_instance = pd.read_excel(excel_sheet_name, sheet_name="CoA Instance")
#df_variables = pd.read_excel(excel_sheet_name, sheet_name="Variables")
#df_environment = pd.read_excel(excel_sheet_name, sheet_name="Environment")
nan_value = float("NaN")
#df_coa_instance.replace(nan_value,"NA", inplace=True)
#indexCol=df_formula[0]



def DataFilter(fileName):

 try:
  excel_sheet_name = "SourceFile/" + fileName
  df_formula = pd.DataFrame()
  df_formula = pd.read_excel(excel_sheet_name, sheet_name="Allocation Formula XXXX", skiprows=2, header=None)
  gdf.df_coa_instance = pd.read_excel(excel_sheet_name, sheet_name="CoA Instance")
  gdf.df_variables = pd.read_excel(excel_sheet_name, sheet_name="Variables")
  gdf.df_environment = pd.read_excel(excel_sheet_name, sheet_name="Environment")
  # df_coa_instance.replace(nan_value,"NA", inplace=True)
  indexCol = df_formula[0]
  gdf.log.debug("Starting Dataframe generation from  excel - " + excel_sheet_name)
  table_count = 0
  index_var = 0
  for i in range(len(df_formula.columns)):
    if df_formula.iloc[:,i].isna().all():
      if table_count == 0:
        gdf.df_header = df_formula.iloc[:, index_var: i ]
        table_count = table_count +1   
        index_var = i+1
        new_header = gdf.df_header.iloc[0] #grab the first row for the header
        gdf.df_header = gdf.df_header[1:] #take the data less the header row
        gdf.df_header.columns = new_header #set the header row as the df header
        #gdf.df_header=gdf.df_header.replace(nan_value,"", regex=True)
        gdf.df_header.insert(5, 'Status', float("NaN"))
        gdf.df_header.insert(6, 'ErrorDetails', float("NaN"))
        gdf.df_header.name = "Header"
        
        continue
        
      
      if table_count == 1:
        gdf.df_pov_properties = df_formula.iloc[:,index_var: i ]
        gdf.df_pov_properties.insert(0,'Sl.No',indexCol)
        table_count = table_count +1   
        index_var = i+1
        new_header = gdf.df_pov_properties.iloc[0] #grab the first row for the header
        gdf.df_pov_properties = gdf.df_pov_properties[1:] #take the data less the header row
        gdf.df_pov_properties.columns = new_header #set the header row as the df header
        #gdf.df_pov_properties=gdf.df_pov_properties.replace(nan_value,"", regex=True)
        gdf.df_pov_properties.name = "POV Properties"
        continue

      if table_count == 2:
        gdf.df_overall_rule_pov = df_formula.iloc[:,index_var: i]
        gdf.df_overall_rule_pov.insert(0,'Sl.No',indexCol)
        table_count = table_count +1   
        index_var = i+1
        new_header = gdf.df_overall_rule_pov.iloc[0] #grab the first row for the header
        gdf.df_overall_rule_pov = gdf.df_overall_rule_pov[1:] #take the data less the header row
        gdf.df_overall_rule_pov.columns = new_header #set the header row as the df header
        #gdf.df_overall_rule_pov=gdf.df_overall_rule_pov.replace(nan_value,"", regex=True)
        gdf.df_overall_rule_pov.name = "Overall Rule POV"
        continue

      if table_count == 3:
        gdf.df_formula_properties = df_formula.iloc[:,index_var: i]
        gdf.df_formula_properties.insert(0,'Sl.No',indexCol)
        table_count = table_count +1   
        index_var = i+1
        new_header = gdf.df_formula_properties.iloc[0] #grab the first row for the header
        gdf.df_formula_properties = gdf.df_formula_properties[1:] #take the data less the header row
        gdf.df_formula_properties.columns = new_header #set the header row as the df header
        #gdf.df_formula_properties=gdf.df_formula_properties.replace(nan_value,"", regex=True)
        gdf.df_formula_properties.name = "Formula Properties"
        continue

      if table_count == 4:
        gdf.df_source = df_formula.iloc[:,index_var: i]
        gdf.df_source.insert(0,'Sl.No',indexCol)
        table_count = table_count +1   
        index_var = i+1
        new_header = gdf.df_source.iloc[0] #grab the first row for the header
        gdf.df_source = gdf.df_source[1:] #take the data less the header row
        gdf.df_source.columns = new_header #set the header row as the df header
        #gdf.df_source=gdf.df_source.replace(nan_value,"", regex=True)
        gdf.df_source.name = "Source"
        continue

      if table_count == 5:
        gdf.df_target = df_formula.iloc[:,index_var: i]
        gdf.df_target.insert(0,'Sl.No',indexCol)
        table_count = table_count +1   
        index_var = i+1
        new_header = gdf.df_target.iloc[0] #grab the first row for the header
        gdf.df_target = gdf.df_target[1:] #take the data less the header row
        gdf.df_target.columns = new_header #set the header row as the df header
        #gdf.df_target=gdf.df_target.replace(nan_value,"", regex=True)
        gdf.df_target.name = "Target"
        continue

      if table_count == 6:
        gdf.df_offset = df_formula.iloc[:,index_var: i]
        gdf.df_offset.insert(0,'Sl.No',indexCol)
        table_count = table_count +1   
        index_var = i+1
        new_header = gdf.df_offset.iloc[0] #grab the first row for the header
        gdf.df_offset = gdf.df_offset[1:] #take the data less the header row
        gdf.df_offset.columns = new_header #set the header row as the df header
        #gdf.df_offset=gdf.df_offset.replace(nan_value,"", regex=True)
        gdf.df_offset.name = "Offset"
        continue

      if table_count == 7:
        gdf.df_rounding_digits = df_formula.iloc[:,index_var: i]
        gdf.df_rounding_digits.insert(0,'Sl.No',indexCol)
        table_count = table_count +1   
        index_var = i+1
        new_header = gdf.df_rounding_digits.iloc[0] #grab the first row for the header
        gdf.df_rounding_digits = gdf.df_rounding_digits[1:] #take the data less the header row
        gdf.df_rounding_digits.columns = new_header #set the header row as the df header
        #gdf.df_rounding_digits=gdf.df_rounding_digits.replace(nan_value,"", regex=True)
        gdf.df_rounding_digits.name = "Rounding digits"
        continue
    
    if (table_count == 8 and i == len(df_formula.columns)-1):
        df_comments = df_formula.iloc[:, i]
        table_count = table_count +1   
        index_var = i+1
        continue  
    
  gdf.df_comments=df_comments.to_frame()
  gdf.df_comments.insert(0,'Sl.No',indexCol)
  new_header = gdf.df_comments.iloc[0] #grab the first row for the header
  gdf.df_comments = gdf.df_comments[1:] #take the data less the header row
  gdf.df_comments.columns = new_header #set the header row as the df header
  #gdf.df_comments=gdf.df_comments.replace(nan_value,"", regex=True)
  gdf.df_comments.name = "Comments"
  gdf.log.debug("Dataframes generation complete")
 except:
  gdf.log.error("Error in generating Dataframes")

 else:
  gdf.log.info("Dataframes created successfully.Starting Validations")
  gdf.log.info("Starting Validations")



    


def UpdateHeaders():

  gdf.log.debug("Updating Dataframe headers...")
  new_header = gdf.df_pov_properties.iloc[0] #grab the first row for the header
  gdf.df_pov_properties = gdf.df_pov_properties[1:] #take the data less the header row
  gdf.df_pov_properties.columns = new_header #set the header row as the df header
  gdf.df_pov_properties=gdf.df_pov_properties.replace(nan_value,"", regex=True)
  gdf.log.debug("Updating Dataframe headers - Complete...")


def overwritepovwithsource():
  gdf.df_pov_new=gdf.df_overall_rule_pov.copy()
  df_pov_seg = gdf.df_overall_rule_pov.loc[:, "SEGMENT1": "Scenario"].drop(columns=["Scenario"])
  df_source_seg = gdf.df_source.loc[:, "SEGMENT1":"Scenario"].drop(columns=["Scenario"])
  df_pov_seg.update(df_source_seg)
  gdf.df_pov_new.update(df_pov_seg)


def UpdateNan(df):
  # To replace NaN values with blank
  df.replace(nan_value,"", inplace=True)



 
 



     
 
    