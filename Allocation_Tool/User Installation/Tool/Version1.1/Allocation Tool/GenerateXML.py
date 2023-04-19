from itertools import count
from operator import index
#import pandas as pd
import math as mt
import numpy as np
import xml.etree.ElementTree as gfg
import GlobalDF as gdf
import GenerateData as gd
import calendar
import time
import os
import platform
import socket
import datetime
import re




now = datetime.datetime.now()
date_time_str = now.strftime("%d%m%Y%H%M%S")
sys_name=platform.node()
id_var=sys_name[2:9]
id_var=id_var+""+str(date_time_str)
#version=gdf.df_environment.iloc[0,0]

#Regex compile
paramRgx = re.compile('".*"')
funcNameRgx = re.compile('@.*\(')




def removeDuplicates(df):
    df.drop_duplicates(keep='first',ignore_index=True,inplace=True)


def setIndex(df):
    df.set_index('Sl.No',inplace=True)


def addQoutes(value):
    return('"' + str(value) + '"')

def checkFunctionPattern(value):
    boolFlag = x = re.search('@.*\(".*"\)', value)
    return boolFlag

def getSegmentValue(value):
    if checkFunctionPattern(value):
        res = paramRgx.search(value)
        return res.group(0).replace('"' , '')
    else:
        return value

def generateXML(fileName):
 gdf.log.info("XML generation started") 
 try:
     df_uniqueHeaders=gdf.df_header

     df_uniqueHeaders=df_uniqueHeaders[df_uniqueHeaders['Status']=='Success']
     #df_uniqueHeaders.set_index('Sl.No',inplace=True)

     
     row_header_list=df_uniqueHeaders.index.values.tolist()
     var_index_list=gdf.df_variables.index.values.tolist()
     var_list = ["{" + i + "}" for i in list(gdf.df_variables.loc[: , "Name"])]



     #temporary changes , will be doing same for other dfs in a function
     #gdf.df_overall_rule_pov= gdf.df_overall_rule_pov.drop_duplicates(keep='first',ignore_index=True)
     #gdf.df_overall_rule_pov.set_index('Sl.No',inplace=True)
     overall_rule_pov_cols_list=gdf.df_overall_rule_pov.columns.tolist()

     version = gdf.df_environment.iloc[0, 0]
     root = gfg.Element("HBRRepo",attrib={'version':version})
     variables = gfg.Element("variables")
     root.append (variables)
     rule_id=0

     #Variables Section________________________________________________
     for var in var_index_list:
        #print("Var index"+str(var))
        rule_id=id_var+str(var)
        variable=gfg.SubElement(variables,"variable",attrib={'id':rule_id,'name':gdf.df_variables.loc[var,'Name'],'product':'Essbase','type':'member','usage':'const'})
        property_description=gfg.SubElement(variable,'property',attrib={'name':'description'})
        property_description.text=gdf.df_variables.loc[var,'Description']

        property_application=gfg.SubElement(variable,'property',attrib={'name':'application'})
        application_db_var=str(gdf.df_variables.loc[var,'Scope'])
        scope= application_db_var.split(".")
        property_application.text=scope[0]

        property_plantype=gfg.SubElement(variable,'property',attrib={'name':'plantype'})
        property_plantype.text=scope[1]

        property_scope=gfg.SubElement(variable,'property',attrib={'name':'scope'})
        property_scope.text="ruleset"

        property_array_type=gfg.SubElement(variable,'property',attrib={'name':'array_type'})
        property_array_type.text="string"

        property_prompt_text=gfg.SubElement(variable,'property',attrib={'name':'prompt_text'})
        property_prompt_text.text=gdf.df_variables.loc[var,'RTP Text']

        property_Dimension=gfg.SubElement(variable,'property',attrib={'name':'dimension'})
        property_Dimension.text=gdf.df_variables.loc[var,'Dimension'].replace(" ", "")

        value=gfg.SubElement(variable,"value")
        value.text=addQoutes(gdf.df_variables.loc[var,'Default Value'])

     rulesets = gfg.Element("rulesets")
     root.append (rulesets)
     rules = gfg.Element("rules")
     root.append (rules)
     #print(gdf.df_rounding_digits)
     

     for val in row_header_list:

        rule=gfg.SubElement(rules,"rule",attrib={'id':id_var+str(val),'name':df_uniqueHeaders.loc[val,'Rule Name:'],'product':'Essbase'})  #pending for attributes

        property_description=gfg.SubElement(rule,"property",attrib={'name':'description'}) #field value ?
        try:
         property_description.text=df_uniqueHeaders.loc[val,'Rule Name:']
        except:
         print("Exception Occured : Description property value out of bounds \n")

        property_application=gfg.SubElement(rule,"property",attrib={'name':'application'})
        try:
         property_application.text=df_uniqueHeaders.loc[val,'COA Instance']
         application_name=df_uniqueHeaders.loc[val,'COA Instance']
        except:
         print("Exception Occured : Application property value out of bounds \n")

        property_plantype=gfg.SubElement(rule,"property",attrib={'name':'plantype'})
        try:
         property_plantype.text='db'
        except:
         print("Exception Occured : Plantype property value out of bounds \n")

        property_display_label=gfg.SubElement(rule,"property",attrib={'name':'display_label'})
        try:
         property_display_label.text=df_uniqueHeaders.loc[val,'Rule Name:']

        except:
         print("Exception Occured : Display_Label property value out of bounds \n")

        if(df_uniqueHeaders.loc[val,'Caption'] != ""):
            property_comment=gfg.SubElement(rule,'property',attrib={'name':'comment'})
            property_comment.text=df_uniqueHeaders.loc[val,'Caption']

        variable_references=gfg.SubElement(rule,"variable_references")
        for var in var_index_list:
            if(application_name ==scope[0]):
                #print("Var index"+str(var))
                rule_id=id_var+str(var)
                variable_reference=gfg.SubElement(variable_references,"variable_reference",attrib={'id':rule_id,'name':gdf.df_variables.loc[var,'Name']})
                    
                property_seq=gfg.SubElement(variable_reference,'property',attrib={'name':'seq'})
                property_seq.text="1"
                
                property_application=gfg.SubElement(variable_reference,'property',attrib={'name':'application'})
                application_db_var=str(gdf.df_variables.loc[var,'Scope'])

                property_application.text=scope[0]
                property_type=gfg.SubElement(variable_reference,'property',attrib={'name':'type'})
                property_type.text="3"

                property_hidden=gfg.SubElement(variable_reference,'property',attrib={'name':'hidden'})
                property_hidden.text="false"

                property_plantype=gfg.SubElement(variable_reference,'property',attrib={'name':'plantype'})
                property_plantype.text=scope[1]

                property_hasValue=gfg.SubElement(variable_reference,'property',attrib={'name':'hasvalue'})
                property_hasValue.text='true'

                property_rulename=gfg.SubElement(variable_reference,'property',attrib={'name':'rule_name'})
                property_rulename.text=df_uniqueHeaders.loc[val,'Rule Name:']

            else:
                print("okay")
                



                





        statement=gfg.SubElement(rule,"statement",attrib={'seq':'1','type':'loop'})


        loop=gfg.SubElement(statement,"loop",attrib={'product':'Essbase','type':'data'})
        property_app=gfg.SubElement(loop,"property",attrib={'name':'application'})
        try:
         property_app.text= df_uniqueHeaders.loc[val,'COA Instance']
        except:
            print("Exception Occured : Statement/Loop/Application property value out of bounds \n")


        property_plan=gfg.SubElement(loop,"property",attrib={'name':'plantype'})
        property_plan.text= 'db'
        property_desc=gfg.SubElement(loop,"property",attrib={'name':'description'})
        property_desc.text = gdf.df_pov_properties.loc[val, "Description"]
        property_display_height=gfg.SubElement(loop,"property",attrib={'name':'display_height'})
        property_display_height.text= '2'
        property_disabled=gfg.SubElement(loop,"property",attrib={'name':'disabled'})
        property_disabled.text= 'false'
        property_caption=gfg.SubElement(loop,"property",attrib={'name':'caption'})
        property_caption.text= 'POV'
        property_display_width=gfg.SubElement(loop,"property",attrib={'name':'display_width'})
        property_display_width.text= '3'
        property_Tile_Key=gfg.SubElement(loop,"property",attrib={'name':'Tile_Key'})
        property_Tile_Key.text= 'x1y0'
        comment_val = gdf.df_pov_properties.loc[val ,"Comment"]
        if comment_val != "":
            property_comment = gfg.SubElement(loop, "property", attrib={'name': 'comment'})
            property_comment.text = comment_val
        test=gfg.SubElement(loop,"test")
        expression=gfg.SubElement(test,"expression")
        source=gfg.SubElement(expression,"source")
        slice=gfg.SubElement(source,"slice")


        dimension=gfg.SubElement(slice,"dimension",attrib={'name':(overall_rule_pov_cols_list[0]).replace(" ", ""),'seq':'1'})
        if(gdf.df_overall_rule_pov.loc[val,'Accounting Period'] != ''):
         expression=gfg.SubElement(dimension,"expression")
         userinput=gfg.SubElement(expression,"user_inp")
         if gdf.df_overall_rule_pov.loc[val,'Accounting Period'] in var_list:
             userinput.text = gdf.df_overall_rule_pov.loc[val, 'Accounting Period']
         else:
             userinput.text='"'+gdf.df_overall_rule_pov.loc[val,'Accounting Period']+'"'

        dimension=gfg.SubElement(slice,"dimension",attrib={'name':overall_rule_pov_cols_list[1].capitalize(),'seq':'2'})
        expression=gfg.SubElement(dimension,"expression")
        userinput=gfg.SubElement(expression,"user_inp")
        userinput.text='"'+overall_rule_pov_cols_list[1].capitalize()+'-'+gdf.df_overall_rule_pov.loc[val,'LEDGER']+'"'

        ## dimension=gfg.SubElement(slice,"dimension",attrib={'name':})
        temp_df=gdf.df_coa_instance.loc[gdf.df_coa_instance['CoA Instance Name'] == df_uniqueHeaders.loc[val,'COA Instance']]

        temp_df=temp_df.replace("", gd.nan_value, inplace=False)
        temp_df.dropna(axis=1,inplace=True)
        cols_list=temp_df.iloc[0]
        seq=2
        ##offset@
        for cols in temp_df.columns[1:]:
          seq+=1
          #print(cols_list.loc[cols])
          dimension=gfg.SubElement(slice,"dimension",attrib={'name':cols_list.loc[cols],'seq':str(seq)})
          if(gdf.df_overall_rule_pov.loc[val,cols] != ''):
            expression=gfg.SubElement(dimension,"expression")
            userinput=gfg.SubElement(expression,"user_inp")
            columnval = getSegmentValue(str(gdf.df_overall_rule_pov.loc[val,cols]))
            outval = '"'+str(cols.split("T")[1])+'-'+str(columnval)+',All '+cols_list.loc[cols]+' Values"'
            if checkFunctionPattern(str(gdf.df_overall_rule_pov.loc[val,cols])):
                userinput.text = funcNameRgx.search(str(gdf.df_overall_rule_pov.loc[val,cols])).group(0) + outval + ')'
            else:
                userinput.text= outval
        ###
        seq+=1
        dimension=gfg.SubElement(slice,"dimension",attrib={'name':overall_rule_pov_cols_list[-3],'seq':str(seq)})
        expression=gfg.SubElement(dimension,"expression")
        userinput=gfg.SubElement(expression,"user_inp")
        userinput.text='"'+gdf.df_overall_rule_pov.loc[val,str(overall_rule_pov_cols_list[-3])]+'"'
        seq+=1
        dimension=gfg.SubElement(slice,"dimension",attrib={'name':overall_rule_pov_cols_list[-2],'seq':str(seq)})
        expression=gfg.SubElement(dimension,"expression")
        userinput=gfg.SubElement(expression,"user_inp")
        userinput.text='"'+gdf.df_overall_rule_pov.loc[val,str(overall_rule_pov_cols_list[-2])]+'"'
        seq+=1
        dimension=gfg.SubElement(slice,"dimension",attrib={'name':overall_rule_pov_cols_list[-1],'seq':str(seq)})
        expression=gfg.SubElement(dimension,"expression")
        userinput=gfg.SubElement(expression,"user_inp")
        userinput.text='"'+gdf.df_overall_rule_pov.loc[val,str(overall_rule_pov_cols_list[-1])]+'"'


        df_tempSource=gdf.df_source.loc[[val] , :]
        #print(df_tempSource)
        df_tempSource.replace("", gd.nan_value, inplace=True)
        df_tempSource["Currency Type"] = df_tempSource["Currency Type"].fillna("")
        df_tempSource.dropna(how='all', axis=1, inplace=True)
        #
        #print(df_tempSource)

        #print(df_tempSource)
        #df_tempSource_1 = df_tempSource.drop('Currency Type', axis=1)
        #print(df_tempSource)
        #currency_type_dict = df_tempSource['Currency Type'].to_dict()
        #print(currency_type_dict)

        df_tempTarget=gdf.df_target.loc[[val] , :]
        df_tempTarget.replace("", gd.nan_value, inplace=True)
        df_tempTarget.dropna(how='all', axis=1, inplace=True)

        df_tempOffset = gdf.df_offset.loc[val].astype(str)
        df_tempOffset.replace("", gd.nan_value, inplace=True)
        df_tempOffset.dropna(how='all' , inplace=True)


        coa_instance = df_uniqueHeaders.loc[val, "COA Instance"]
        coa_instance_seg = gdf.df_coa_instance.loc[gdf.df_coa_instance["CoA Instance Name"] == coa_instance].to_dict(orient="records")[0]

        statement=gfg.SubElement(loop,"statement", attrib={'kind':'formula','name':'','seq':'1','type':'component'})
        component=gfg.SubElement(statement,"component", attrib={'id':'-18','name':'','product':'Essbase','type':'formula'})
        property_application=gfg.SubElement(component,"property", attrib={'name':'application'})
        property_application.text=df_uniqueHeaders.loc[val,'COA Instance']

        property_plantype=gfg.SubElement(component,"property", attrib={'name':'plantype'})
        property_plantype.text='db'

        property_isASO=gfg.SubElement(component,"property", attrib={'name':'isASO'})
        property_isASO.text='true'

        property_disabled=gfg.SubElement(component,"property", attrib={'name':'disabled'})
        property_disabled.text='false'

        # Updating value for display_label in component
        property_component_display_label=gfg.SubElement(component,"property",attrib={'name':'display_label'})
        first_seg = next(i for i in df_tempSource.columns if "SEGMENT" in i)
        first_seg_val = df_tempSource.loc[[val], first_seg].iloc[0]
        #first_seg_val = df_tempSource.loc[val, first_seg].to_list()[0]
        property_component_display_label.text = addQoutes(first_seg[-1] + '-' + str(first_seg_val) + ',All ' + str(coa_instance_seg[first_seg]) + ' Values')

        property_Tile_Key=gfg.SubElement(component,"property",attrib={'name':'Tile_Key'})
        property_Tile_Key.text= 'x1y0'

        formulae=gfg.SubElement(component,"formulae")
        condition_block=gfg.SubElement(formulae,"conditionblock")

        # property_offset_member=gfg.SubElement(condition_block,"property",attrib={'name':'offsetmember'})
        # dict_offset = df_tempOffset.to_dict()
        # l = [addQoutes(i[-1] + "-" + str(dict_offset[i]) + ",All " + str(coa_instance_seg[i]) + " Values") for i in dict_offset.keys()]
        # property_offset_member.text = "->".join(l)

        property_offset_member=gfg.SubElement(condition_block,"property",attrib={'name':'offsetmember'})
        dict_offset = df_tempOffset.to_dict()
        l = list()
        for s in dict_offset.keys():
            if "SEGMENT" in s:
                idx = s[-1] + '-'
                coa_val = ",All " + str(coa_instance_seg[s]) + " Values"
                colval = getSegmentValue(str(dict_offset[s]))
                v = addQoutes(idx + str(colval) + coa_val)
                if checkFunctionPattern(str(dict_offset[s])):
                    l.append(funcNameRgx.search(str(dict_offset[s])).group(0) + v + ')')
                else:
                    l.append(v)
            elif dict_offset[s] in var_list:

                l.append(dict_offset[s])
            else:
                l.append(addQoutes(dict_offset[s]))
        #l = [addQoutes(i[-1] + "-" + str(dict_offset[i]) + ",All " + str(coa_instance_seg[i]) + " Values") for i in dict_offset.keys()]
        property_offset_member.text = "->".join(l)

        property_rounding_digits=gfg.SubElement(condition_block,"property",attrib={'name':'roundingdigits'})
        property_rounding_digits.text=str(gdf.df_rounding_digits.loc[val,"Rounding digits (Applicable for all 3)"])

        property_comment_block=gfg.SubElement(condition_block,"property",attrib={'name':'commentblock'})
        property_comment_block.text=str(gdf.df_comments.loc[val,"Comment"])


        #coa_instance = df_uniqueHeaders.loc[val, "COA Instance"]
        #coa_instance_seg = gdf.df_coa_instance.loc[gdf.df_coa_instance["CoA Instance Name"] == coa_instance].to_dict(orient = "records")[0]

        #print(gdf.df_overall_rule_pov.to_dict(orient="records"))
        
        currencyType = gdf.df_overall_rule_pov.loc[val].to_dict()["Currency Type"]
        #Create formula block
        




        for row_s , row_t,  in zip(df_tempSource.to_dict(orient = "records"), df_tempTarget.to_dict(orient = "records")  ):
            formula_block = gfg.SubElement(condition_block, "formula")
            property_formula_type = gfg.SubElement(formula_block, "property", attrib={'name': 'formulatype'})
            property_formula_type.text = "Formula"
            destination_block = gfg.SubElement(formula_block, "destination")
            expression_block = gfg.SubElement(formula_block, "expression")

            expression_val = list()
            

            #expression_end_string = addQoutes(currencyType)
            expression_end_string = ""

           
            #print("===========================================================")
            for s in row_s.keys() :
                

                if "SEGMENT" in s:
                    #print("1")
                    idx = s[-1] + '-'
                    coa_val = ",All " + str(coa_instance_seg[s]) + " Values"
                    colval = getSegmentValue(str(row_s[s]))
                    v = addQoutes(idx + str(colval) + coa_val)
                    if checkFunctionPattern(str(row_s[s])):
                        expression_val.append(funcNameRgx.search(str(row_s[s])).group(0) + v + ')')
                    else:
                        expression_val.append(v)
                elif s in ["Currency Type"]:
                    print(row_s[s])
                    if(row_s[s] == ""):
                        expression_end_string = addQoutes(currencyType)
                        
                    else:
                        expression_end_string=expression_end_string+(addQoutes(row_s[s]))
                        

                           

                elif s in ["Function (*,/,+,- )", "Unit (Positive/Negative value)"]:
                   

                    expression_end_string = expression_end_string + str(row_s[s])
                     
                elif row_s[s] in var_list:
                   
                    expression_val.append(row_s[s])
                else:
                   
                    expression_val.append(addQoutes(row_s[s]))

            #print("++++++++++++++++++++++++++++++++++")
            #print(expression_val)
            expression_val.append(expression_end_string)
            expression_user_inp_block = gfg.SubElement(expression_block, "user_inp")
            expression_user_inp_block.text =  '->'.join(expression_val)



            destination_val = list()
            for t in row_t.keys():
                if "SEGMENT" in t:
                    idx = t[-1] + '-'
                    coa_val = ",All " + str(coa_instance_seg[t]) + " Values"
                    colval = getSegmentValue(str(row_t[t]))
                    v = addQoutes(idx + str(colval) + coa_val)
                    if checkFunctionPattern(str(row_t[t])):
                        destination_val.append(funcNameRgx.search(str(row_t[t])).group(0) + v + ')')
                    else:
                        destination_val.append(v)
                elif row_t[t] in var_list:
                    destination_val.append(row_t[t])
                else:
                    destination_val.append(addQoutes(row_t[t]))
            destination_user_inp_block = gfg.SubElement(destination_block, "user_inp")
            destination_user_inp_block.text = '->'.join(destination_val)
            #print('-&gt;'.join(destination_val))


     components=gfg.Element("components")
     root.append (components)
     deployObjects=gfg.Element("deployobjects")
     root.append (deployObjects)
     for val in row_header_list:
        deployObject=gfg.SubElement(deployObjects,"deployobject",attrib={"application":df_uniqueHeaders.loc[val,'COA Instance'].lower(),'name':df_uniqueHeaders.loc[val,'Rule Name:'],'obj_id':id_var+str(val),'obj_type':str("1"),'plantype':'db','product':'3'})

   

     #xmlTools.enableXmlTreeView
     tree =gfg.ElementTree(root)
     #xmlstr = minidom.parseString(gfg.tostring(root)).toprettyxml(indent="   ")
     with open (fileName, "wb") as files :
         #files.write(xmlstr)
         tree.write(files,xml_declaration=True,encoding='UTF-8')
         gdf.log.info("XML generation success. Name: "+fileName)
         gdf.log.info("======================================================================================================================")  
         print("XML generation Finished-->"+fileName)

 except:
        gdf.log.error("XML generation failed",exc_info=True)
        gdf.log.info("======================================================================================================================") 





 

        
        
           
         




    



