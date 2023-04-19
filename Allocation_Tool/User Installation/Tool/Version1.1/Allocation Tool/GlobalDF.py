import pandas as pd
import logging as log
import datetime

#log.basicConfig(filename="log_1.txt", level=log.DEBUG, format="%(asctime)s %(message)s ", filemode="a")
log.basicConfig(filename='logs/log_' + datetime.datetime.now().strftime("%d-%m-%Y") + '.log', format='%(asctime)s - %(levelname)s - %(message)s', level=log.DEBUG,filemode="a")



df_header=pd.DataFrame()

df_pov_properties=pd.DataFrame()

df_overall_rule_pov=pd.DataFrame()

df_formula_properties=pd.DataFrame()  

df_source=pd.DataFrame()

df_target=pd.DataFrame()

df_offset=pd.DataFrame()

df_rounding_digits=pd.DataFrame()

df_comments=pd.DataFrame()

df_coa_instance = pd.DataFrame()

df_variables = pd.DataFrame()

df_environment = pd.DataFrame()

df_pov_new=pd.DataFrame()

df_error_report=pd.DataFrame()



    
