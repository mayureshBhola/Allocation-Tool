from itertools import count
from wsgiref import validate
import pandas as pd
import numpy as np
import GlobalDF as gdf
import GenerateData as pa


pd.options.mode.chained_assignment = None





rule_status_dict = dict()



def Validate(*args):
    gdf.log.debug("Starting Rule Validations")
    segmentsCount=len(gdf.df_coa_instance.columns)-1
    df_list_tab=[7,5,segmentsCount+6,7,segmentsCount+10,segmentsCount+2,segmentsCount+2,2,2]
    count=0
    j=0

    for i in args:
        if(len(i.columns)==df_list_tab[j]):
            j+=1
            count+=1
    if(count==9):
        gdf.log.info("Validation Success:DataFrames structure and segment count validated")
        ValidateRules(args)
    else:
        #gdf.log.error("Validation failed: Number of DataFrames are incorrect")
        raise Exception("Validation failed: Number of DataFrames are incorrect")


def ValidateRules(*args):
    #Generate Rule Status list
    for i in gdf.df_header.loc[:, "Sl.No"].unique():
        #rule_status[i] = []
        rule_status_dict[i] = []

    # Validate sequence segments
    gdf.log.debug("Checking segments are in sequence in COA Instance")
    checkSequenceSegments()

    # Validate Source target segments comparison rule
    gdf.log.debug("Checking Source Target Comparison")
    checkSourceTargetSegmentsRule(gdf.df_source, gdf.df_target)

    #Validate SEGMENTS having data in POV table should be empty in Target table
    gdf.log.debug("Checking POV Target Comparison")
    checkPOVTargetSegmentsRule(gdf.df_overall_rule_pov, gdf.df_target)

    # Validate SEGMENTS having data in POV table should be empty in Offset table
    gdf.log.debug("Checking POV Offset Comparison")
    checkPOVOffsetSegmentsRule(gdf.df_overall_rule_pov, gdf.df_offset)

    #Validate rows in Dataframe having same values:
    gdf.log.debug("Checking rows in all Dataframes except Source and Traget have same values for Rules")
    #checkDataframeRowsRedundency(gdf.df_offset)
    for df in [gdf.df_header, gdf.df_pov_properties, gdf.df_overall_rule_pov, gdf.df_formula_properties, gdf.df_offset,gdf.df_rounding_digits, gdf.df_comments]:
        checkDataframeRowsRedundency(df)

    #Validate Segments count is same as COA instance
    gdf.log.debug("Checking combined Segments count having value is same as COA instance for pov-offset , pov-source & pov-target")
    checkSegmentsCountinRule(gdf.df_overall_rule_pov,gdf.df_source,gdf.df_target, gdf.df_offset)



    gdf.log.debug("Rule Validations Completed")

# Validate Source target segments comparison rule
def checkSourceTargetSegmentsRule(df1, df2):
    df_source_seg = df1.loc[:, "SEGMENT1": "Scenario"].drop(columns=["Scenario"])
    df_target_seg = df2.loc[:, "SEGMENT1":]
    pa.UpdateNan(df_source_seg), pa.UpdateNan(df_target_seg)
    df_comp = (df_source_seg == df_target_seg)
    df_comp["Rule"] = gdf.df_source.iloc[:, 0]
    df_status = pd.DataFrame(columns=["Rule", "Status"])
    for row in df_comp.itertuples():
        df_status.loc[row[0], ["Rule", "Status"]] = [ row[-1], ( not all(list(row[1:-1])))]
    for i in df_status.iloc[:, 0].unique():
        #rule_status[i] = rule_status[i] + [df_status.loc[df_status["Rule"] == i].loc[:, "Status"].all()]
        if df_status.loc[df_status["Rule"] == i].loc[:, "Status"].all():
            rule_status_dict[i].append({"Status" : True, "Message" : "Success"})
            #gdf.log.info("Rule {} validation success. Validation Criteria: Target SEGMENTS and Source SEGMENTS cannot have same values.".format(i))
        else:
            rule_status_dict[i].append({"Status" : False, "Message" : "Rule {} Failed. Target SEGMENTS and Source SEGMENTS cannot have same values".format(i)})
            gdf.log.error("Rule {} validation Failed. Target SEGMENTS and Source SEGMENTS cannot have same values".format(i))

#Validate SEGMENTS having data in POV table should be empty in Target table
def checkPOVTargetSegmentsRule(df_pov , df_target):
    df_pov_seg = df_pov.loc[:, "SEGMENT1": "Scenario"].drop(columns=["Scenario"])
    df_target_seg = df_target.loc[:, "SEGMENT1":]
    df_pov_seg["Rule"] = df_pov.iloc[:, 0]
    df_target_seg["Rule"] = df_target.iloc[:, 0]
    for i in df_pov_seg.loc[:, "Rule"].unique():
        df_rh = df_target_seg.loc[df_target_seg["Rule"] == i].drop(columns=["Rule"]).isna()
        df_lh = df_pov_seg.loc[df_pov_seg["Rule"] == i].drop(columns=["Rule"]).isna()
        df_comp2 = df_rh | df_lh
        #rule_status[i] = rule_status[i] + [df_comp2.all().all()]
        if df_comp2.all().all():
            rule_status_dict[i].append({"Status": True, "Message": "Success"})
            #gdf.log.info("Rule {} validation success. Validation Criteria: SEGMENTS having data in POV table should be empty in Target table.".format(i))
        else:
            rule_status_dict[i].append({"Status": False, "Message": "Rule {} Failed. SEGMENTS having data in POV table should be empty in Target table.".format(i)})
            gdf.log.error("Rule {} validation Failed. SEGMENTS having data in POV table should be empty in Target table.".format(i))


#Validate SEGMENTS having data in POV table should be empty in Offset table
def checkPOVOffsetSegmentsRule(df_pov , df_offset):
    df_pov_seg = df_pov.loc[:, "SEGMENT1": "Scenario"].drop(columns=["Scenario"])
    df_offset_seg = df_offset.loc[:, "SEGMENT1":]
    df_pov_seg["Rule"] = df_pov.iloc[:, 0]
    df_offset_seg["Rule"] = df_offset.iloc[:, 0]
    for i in df_pov_seg.loc[:, "Rule"].unique():
        df_rh = df_offset_seg.loc[df_offset_seg["Rule"] == i].drop(columns=["Rule"]).isna()
        df_lh = df_pov_seg.loc[df_pov_seg["Rule"] == i].drop(columns=["Rule"]).isna()
        df_comp2 = df_rh | df_lh
        #rule_status[i] = rule_status[i] + [df_comp2.all().all()]
        if df_comp2.all().all():
            rule_status_dict[i].append({"Status": True, "Message": "Success"})
            #gdf.log.info("Rule {} validation success. Validation Criteria: SEGMENTS having data in POV table should be empty in Offset table.".format(i))
        else:
            rule_status_dict[i].append({"Status": False, "Message": "Rule {} Failed. SEGMENTS having data in POV table should be empty in Offset table.".format(i)})
            gdf.log.error("Rule {} validation Failed. SEGMENTS having data in POV table should be empty in Offset table.".format(
                    i))


# Check if all the formulas in a rule have same offset
def checkDataframeRowsRedundency(df):
    df_name = df.name

    df1 = df.drop_duplicates()
    for i in df1.iloc[:, 0].unique():
        if (len(df1.loc[df1["Sl.No"] == i])) == 1 :
            #rule_status[i] = rule_status[i] + [True]
            rule_status_dict[i].append({"Status": True, "Message": "Success"})
            #gdf.log.info("Rule {} validation success. Validation Criteria: All the formulas in a rule should have a same values in {}.".format(i , df_name))
        else:
            #rule_status[i] = rule_status[i] + [False]
            rule_status_dict[i].append({"Status": False,
                                        "Message": "Rule {} Failed. All the formulas in a rule should have a same values in {}.".format(i, df_name)})
            gdf.log.error("Rule {} validation Failed. All the formulas in a rule should have a same values in {}".format(i, df_name))


def checkSegmentsCountinRule(df_pov , df_src , df_trgt , df_offset):
    #Get SEGMENTS having value in COA Instance
    #coaSegmentsCount = list(gdf.df_coa_instance.loc[0, "SEGMENT1": ].notnull()).count(True)
    coaSegmentsCount = dict()
    df_coa_seg = gdf.df_coa_instance.loc[:, 'SEGMENT1':].notnull()
    df_coa_seg["CoA Instance Name"] = gdf.df_coa_instance.loc[:, "CoA Instance Name"]
 
    for row in df_coa_seg.itertuples():
        coaSegmentsCount[row[-1]] = row[1:-1].count(True)


    # Get SEGMENTS columns from Dataframes with cells containing value as True
    df_pov_seg = df_pov.loc[:, "SEGMENT1": "Scenario"].drop(columns=["Scenario"]).notnull()
    df_offset_seg = df_offset.loc[:, "SEGMENT1":].notnull()
    df_src_seg = df_src.loc[:, "SEGMENT1": "Scenario"].drop(columns=["Scenario"]).notnull()
    df_trgt_seg = df_trgt.loc[:, "SEGMENT1":].notnull()

    #Define Status Dataframe to store status per Rule/formula
    df_status= pd.DataFrame(columns = ["Rule", "pov_src", "pov_trgt", "pov_offset"])
    df_status["Rule"] = df_pov.loc[:, "Sl.No"]

    #Combine dataframes with POV table
    df_pov_offset_comb = df_pov_seg | df_offset_seg
    df_pov_offset_comb["coaInstance"] = gdf.df_header["COA Instance"]
    df_pov_src_comb = df_pov_seg | df_src_seg
    df_pov_src_comb["coaInstance"] = gdf.df_header["COA Instance"]
    df_pov_trgt_comb = df_pov_seg | df_trgt_seg
    df_pov_trgt_comb["coaInstance"] = gdf.df_header["COA Instance"]



    #Store status per rule after checking condition for Segment Count
    for row in df_pov_offset_comb.itertuples():
        df_status.loc[row[0], ["pov_offset"]] = list(row[1:]).count(True) == coaSegmentsCount[row[-1]]
    for row in df_pov_src_comb.itertuples():
        df_status.loc[row[0], ["pov_src"]] = list(row[1:]).count(True) == coaSegmentsCount[row[-1]]
    for row in df_pov_trgt_comb.itertuples():
        df_status.loc[row[0], ["pov_trgt"]] = list(row[1:]).count(True) == coaSegmentsCount[row[-1]]


    #Update final rule status in rule_status checks
    for i in df_status.loc[:, "Rule"].unique():
        #status_list = list(df_status.loc[df_status["Rule"] == i].iloc[:, 1:].all())
        status_list2 = dict(df_status.loc[df_status["Rule"] == i].iloc[:, 1:].all())

        #rule_status[i] = rule_status[i] + status_list

        #Update pov_src status
        if status_list2["pov_src"] == True:
            rule_status_dict[i].append({"Status": True, "Message": "Success"})
            #gdf.log.info("Rule {} validation success. Validation Criteria: Total count of SEGMENTS having data in POV and Source should match the count of SEGMENTS having data in COA instance.".format(i))
        else:
            rule_status_dict[i].append({"Status": False,
                                        "Message": "Rule {} Failed. Total count of SEGMENTS having data in POV and Source should match the count of SEGMENTS having data in COA instance.".format(
                                            i)})
            gdf.log.error("Rule {} validation Failed. Total count of SEGMENTS having data in POV and Source should match the count of SEGMENTS having data in COA instance.".format(i))

        #Update pov_trgt status
        if status_list2["pov_trgt"] == True:
            rule_status_dict[i].append({"Status": True, "Message": "Success"})
            #gdf.log.info("Rule {} validation success. Validation Criteria: Total count of SEGMENTS having data in POV and Target should match the count of SEGMENTS having data in COA instance.".format(i))
        else:
            rule_status_dict[i].append({"Status": False,
                                        "Message": "Rule {} Failed. Total count of SEGMENTS having data in POV and Target should match the count of SEGMENTS having data in COA instance.".format(
                                            i)})
            gdf.log.error(
                "Rule {} validation Failed. Total count of SEGMENTS having data in POV and Target should match the count of SEGMENTS having data in COA instance.".format(
                    i))

        # Update pov_offset status
        if status_list2["pov_offset"] == True:
            rule_status_dict[i].append({"Status": True, "Message": "Success"})
            #gdf.log.info("Rule {} validation success. Validation Criteria: Total count of SEGMENTS having data in POV and Offset should match the count of SEGMENTS having data in COA instance.".format(i))
        else:
            rule_status_dict[i].append({"Status": False,
                                        "Message": "Rule {} Failed. Total count of SEGMENTS having data in POV and Offset should match the count of SEGMENTS having data in COA instance.".format(
                                                i)})
            gdf.log.error(
                "Rule {} validation Failed. Total count of SEGMENTS having data in POV and Offset should match the count of SEGMENTS having data in COA instance.".format(
                    i))


def checkSequenceSegments():
    df_coa_seg = gdf.df_coa_instance.loc[:, 'SEGMENT1':].notnull()
    df_coa_seg["CoA Instance Name"] = gdf.df_coa_instance.loc[:, "CoA Instance Name"]
    segmentSeq = dict()
    for row in df_coa_seg.itertuples():
        ls = [str(int(i)) for i in list(row[1:-1])]
        res = sum([ "1" in j for j in "".join(ls).split("0")])
        if res > 1:
            segmentSeq[row[-1]] = False
        else:
            segmentSeq[row[-1]] = True

    for row in gdf.df_header.drop_duplicates().itertuples():
        if segmentSeq[row[2]]:
            rule_status_dict[row[1]].append({"Status": True, "Message": "Success"})
            #gdf.log.info("Rule {} validation success. Validation Criteria: Segments in COA instance should have values in sequential order .".format(row[1]))
        else:
            rule_status_dict[row[1]].append({"Status": False, "Message": "Rule {} Failed. Segments in COA instance should have values in sequential order .".format(row[1])})
            gdf.log.error("Rule {} Failed. Segments in COA instance Sheet should have values in sequential order for {}.".format(row[1], row[2]))

