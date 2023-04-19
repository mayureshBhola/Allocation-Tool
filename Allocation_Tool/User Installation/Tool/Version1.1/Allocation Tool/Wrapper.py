#No Module Should Import Wrapper.py
import datetime

import Validate as vd
import GlobalDF as gdf
import GenerateData as gd
import GenerateXML  as gxml
import os
import shutil


arr = os.listdir("SourceFile")
#print(arr)
for file in arr:
    try:
        gdf.log.info("======================================================================================================================")
        gdf.log.info("Started processing file - {}".format(file))
        outFileName = file.replace('.xlsx', '')  +'-' + datetime.datetime.now().strftime("%d%m%Y%H%M%S%f")
        gd.DataFilter(file)
        vd.Validate(gdf.df_header,gdf.df_pov_properties,gdf.df_overall_rule_pov,gdf.df_formula_properties ,gdf.df_source,gdf.df_target,gdf.df_offset,gdf.df_rounding_digits,gdf.df_comments)

     #Update Staus and ErrorDetails in Header Dataframe
        gdf.log.debug("Updating Rule validation status to Header dataframe...")
        for i in vd.rule_status_dict.keys():
            res = not ([ j["Status"] for j in vd.rule_status_dict[i]].count(False) > 0)
            gdf.df_header.loc[gdf.df_header["Sl.No"] == i ,"Status"] = res
            if res:
                gdf.df_header.loc[gdf.df_header["Sl.No"] == i, "Status"] = "Success"
                gdf.log.debug("Rule {} Validation Status - Success . ".format(i))
            else:
                gdf.df_header.loc[gdf.df_header["Sl.No"] == i, "Status"] = "Failure"
                gdf.df_header.loc[gdf.df_header["Sl.No"] == i, "ErrorDetails"] = " | ".join([ j["Message"] for j in vd.rule_status_dict[i] if j["Status"] == False])
                gdf.log.error("Rule {} Validation Status - Failure . ".format(i))










        gdf.log.debug("Updating NaN values to blank in Dataframes..")
        for df in [gdf.df_header, gdf.df_pov_properties, gdf.df_overall_rule_pov, gdf.df_formula_properties, gdf.df_source,gdf.df_target, gdf.df_offset, gdf.df_rounding_digits, gdf.df_comments, gdf.df_coa_instance,gdf.df_variables]:
            gd.UpdateNan(df)

        for df in [gdf.df_header, gdf.df_pov_properties, gdf.df_overall_rule_pov, gdf.df_formula_properties,gdf.df_offset, gdf.df_rounding_digits, gdf.df_comments]:
            gxml.removeDuplicates(df)

        for df in [gdf.df_header, gdf.df_pov_properties, gdf.df_overall_rule_pov, gdf.df_formula_properties,gdf.df_source,gdf.df_target,gdf.df_offset, gdf.df_rounding_digits, gdf.df_comments]:
            gxml.setIndex(df)

        gxml.generateXML("GeneratedXML/" + outFileName + ".xml")

        #gdf.df_error_report=gdf.df_header
        #print(gdf.df_header)
        if 'Failure' in list(gdf.df_header.loc[ :,'Status']):
            gdf.df_error_report=gdf.df_header.copy(deep=True)
            gdf.df_error_report=gdf.df_error_report.loc[gdf.df_error_report['Status'] == 'Failure']
            gdf.df_error_report.to_csv('Reports/ErrorReport-' + outFileName +'.csv', encoding='utf-8')

        shutil.move('SourceFile/' + file , 'Archive/' + outFileName + '.xlsx')

    except Exception as e:
        gdf.log.error(e,exc_info=True)





