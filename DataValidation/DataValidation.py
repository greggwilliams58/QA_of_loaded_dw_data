from CommonFunctions import GetMetaData,GetSourceData,lookupTOCdata, individualranges,output_to_excel,setandsortindex
from DWSource import getDWdata,getSourceItemId,getDWdimension
from plotting_data import plot_the_data
import pandas as pd
import pprint as pp
import numpy as np
import xlsxwriter


def main():
    pd.options.mode.chained_assignment = 'raise'
    pd.set_option("display.precision",16)
    FNum = '119'
    FName = 'TARGETS'
    lowerdatefilter = 2019202001
    upperdatefilter = 2019202013
    
    #testing for changes
    #metadata for source and metadatafiles
    #FNum = '313'
    #FName = 'DISAGGPPMCASL'
    
    #lists holding exceptional case information
    toobigforexport = ['104DELAYS','205LENNON']
    notoclookup = ['106TSR','202SRA','207GOVTSUP','209NRTINFRA','224APPEALS']
    

    #dictionary holding the key-pathtometadata 0) schema, 1)table_name, 2)index fields,3)source TOC lookup fields,4)dimt_toc_lookup field, 5) date_type field
    #test here for new repo 


    unique_feed_features = {   
                    '104DELAYS':['NR','factt_104_delays',['financial_period_key','route','delay_type','responsible_org_code','toc_affected','incident_category','area'],['responsible_org_code','toc_affected'],'train_operating_company_id','financial_period_key'],
                    '104INCIDENTCOUNT':['NR','factt_104_incidentcount',['financial_period_key','route','delay_type','responsible_org_code','incident_category','area'],['responsible_org_code'],'train_operating_company_id','financial_period_key'],
                    '105TMILEAGE':['NR','factt_105_train_mileage',['financial_period_key','train_operating_company_key','operator_type','sector'],['train_operating_company_key'],'train_operating_company_key','financial_period_key'],
                    '105FMILEAGE':['NR','factt_105_freight_mileage',['financial_period_key','train_operating_company_key','provisional'],['train_operating_company_key'],'train_operating_company_key','financial_period_key'] ,
                    '106TSR':['NR', 'factt_106_tsr',['route','classification','financial_period_key'],['route'],'route','financial_period_key' ],
                    '114NRAVAILABILITY':['NR','factt_114_nravailability_freight',['financial_period_key','train_operating_company_key'] ,['train_operating_company_key'],'train_operating_company_key','financial_period_key'],
                    '119TARGETS':['NR','factt_119_targets',['Financial_period_key','TOC_key','Target_Name','Target_Group','Target_Scope','Target_Purpose'],['TOC_key'],'train_operating_company_key','Financial_period_key'],
                    '202SRA':['DFT','factt_202_sra',['date_key','financial_period_key'],['NA'],'NA','financial_period_key' ],
                    '203COMMERCIALTRAINMOVES':['DFT','factt_203_commercialtrainmoves',['financial_year_key','financial_period_key','chargeable'],['train_operating_company_key'],'train_operating_company_key','financial_period_key'],
                    '206ROLLINGSTOCK':['DFT','factt_206_rollingstock_annual',['financial_year_key','toc_key'],['toc_key'],'train_operating_company_key','financial_year_key'],
                    '207GOVTSUPTOC':['TS','factt_207_govtsuptoc',['source','Financial_year_of Publication','Funder_Type','TOC_207_Key','Measure_Name'],['TOC_207_Key'],'toc_ref','Financial_year_of Publication'],
                    '207GOVTSUP':['TS','factt_207_govtsup_pivoted',['financial_year_key','country','funding_category'],['NA'],'NA','financial_year_key'],
                    '208NRTINVESTMENT':['ONS','factt_208_NRTInvestment_FY',['financial_year_key','train_operating_company_key','category','measure_name'],['train_operating_company_key'],'train_operating_company_key','financial_year_key'],
                    '209NRTINFRA':['NR','factt_209_nrtinfra',['financial_year_key'],['NA'],'NA','financial_year_key'],
                    '224SECTIONA':['TOCs','factt_224_sectiona',['financial_period_key','train_operating_company_key','Level_1_Category','Level_2_Category','Level_3_Category'],['train_operating_company_key'],'train_operating_company_key','financial_period_key'],
                    '224SECTIONB':['TOCs','factt_224_sectionb',['financial_period_key','train_operating_company_key','Metric','Contact_method'],['train_operating_company_key'],'train_operating_company_key','financial_period_key'],
                    '224SECTIOND':['TOCs','factt_224_sectiond',['financial_period_key','TOC_Key','Booking_Type','Metric_Reference','Measure_Name'],['TOC_Key'],'train_operating_company_key','financial_period_key'],
                    '224SECTIONG':['TOCs','factt_224_SectionG',['Financial_Period','TOC_key','Category_Name','Measure_Name'],['TOC_key'],'train_operating_company_key','Financial_Period'],
                    '224SECTIONH':['TOCs','factt_224_SectionH',['Financial_Period','TOC_key','Category_Name','Measure_Name'],['TOC_key'],'train_operating_company_key','Financial_Period'],
                    '224SECTIONI':['TOCs','factt_224_SectionI',['Financial_Period','TOC_key','Category_Name','Measure_Name'],['TOC_key'],'train_operating_company_key','Financial_Period'],
                    '224SECTIONDRDG':['TOCs','factt_224_SectiondRDG',['Financial_period_key','Financial_year_key','Datasource','Train_Operating_Company_key','Measure'],['Train_Operating_Company_key'],'train_operating_company_key','Financial_period_key'],
                    '224APPEALS':['NETL','factt_224_LTW_TF_Appeals_Closed_Stats_Release',['financial_quarter','publication_status','Organisation','Operator','Franchise type'],['NA'],'NA','financial_quarter']
                    }

    #metadata for DW data
    schema = unique_feed_features[FNum+FName][0]
    table_name = unique_feed_features[FNum+FName][1]
    source_item_id = getSourceItemId(schema,table_name)
    source_item_id.sort()
    
    pp.pprint(source_item_id)

    if schema != 'NETL':
        MD = GetMetaData(FNum,FName)

    #SD = GetSourceData (FNum,FName,MD)

    #check if more than one load in table
    if len(source_item_id) == 1:
    
        latestSID = source_item_id[-1]
        previousSID = source_item_id[-1]

    else:
    
        latestSID = source_item_id[-1]
        previousSID = source_item_id[-2]

    #latestSID = 9084
    #previousSID = 7996
    
    #datasets too large for DW_output
    print(f"The latest SID = {latestSID}")
    print(f"The lowest SID - {previousSID}")
    


    print("getting DW data")   
    DWnew = getDWdata(schema,table_name,latestSID)
    print(DWnew)
    print(DWnew.info())
    


    DWold = getDWdata(schema,table_name,previousSID)

    if FNum+FName not in notoclookup:
        print("looking up TOC info")
        DWnew = lookupTOCdata(DWnew, unique_feed_features[FNum+FName][2],unique_feed_features[FNum+FName][3],unique_feed_features[FNum+FName][4] )
        DWold = lookupTOCdata(DWold,unique_feed_features[FNum+FName][2],unique_feed_features[FNum+FName][3],unique_feed_features[FNum+FName][4] )
    
    else:
        print("no lookup for TOC needed")
        DWnew = setandsortindex(DWnew,unique_feed_features[FNum+FName][2])
        DWold = setandsortindex(DWold,unique_feed_features[FNum+FName][2])
        
    

    print(DWnew)
    #only get data greater than  2018201901
    print("filtering by dates")
    
    DWfiltered =    DWnew.loc[(DWnew.index.get_level_values(unique_feed_features[FNum+FName][5]) >= lowerdatefilter) & (DWnew.index.get_level_values(unique_feed_features[FNum+FName][5]) <= upperdatefilter) ]
    DWoldfiltered = DWold.loc[(DWold.index.get_level_values(unique_feed_features[FNum+FName][5]) >= lowerdatefilter) & (DWold.index.get_level_values(unique_feed_features[FNum+FName][5]) <= upperdatefilter) ]

    print("getting individual ranges for PPC")
    DWPPC = individualranges(DWfiltered,unique_feed_features[FNum+FName][2],'PPC',FNum) 
    print("getting individual range for YPC")
    DWYPC = individualranges(DWfiltered,unique_feed_features[FNum+FName][2],'YPC',FNum)
    
    filteredDWPPC = DWPPC[DWPPC.index.get_level_values(unique_feed_features[FNum+FName][5])>= upperdatefilter]

    #absolute variance by subtraction
    print("getting raw variance")
    variance_raw = DWfiltered.subtract(DWold)

    print("getting raw individual variance")
    variance = individualranges(variance_raw, unique_feed_features[FNum+FName][2],'individual',FNum)
    
    #percentage change by subtraction and then division
    print("getting % variance")
    PCvariance_raw = (( DWfiltered - DWoldfiltered)/ DWold)*100
    print("getting * individual variances")
    PCvariance = individualranges(PCvariance_raw,unique_feed_features[FNum+FName][2],'individual',FNum)

    #export various dataframes to excel
    print("exporting to excel")
    with pd.ExcelWriter(f"data validation for {FNum}_{FName}.xlsx",engine='openpyxl') as writer:
        print("writing to xls")
        
        
        #SD.to_excel(writer,sheet_name='Source_data')
        if FNum + FName not in toobigforexport:
            print("exporting previous DW_load")
            DWoldfiltered.to_excel(writer,sheet_name="Previous_DW_load")
            print("exporting latest load")
            DWfiltered.to_excel(writer,sheet_name="Latest_DW_load")
        else:
            print("dataset is too big for export")
            too_big_text_previous = pd.DataFrame({f"{FNum}_{FName} is too big for export":[]})
            too_big_text_previous.to_excel(writer,sheet_name="Previous_DW_load",startrow=0,startcol=0)
            too_big_text_latest = pd.DataFrame({f"{FNum}_{FName} is too big for export":[]})
            too_big_text_latest.to_excel(writer,sheet_name="Latest_DW_load",startrow=0,startcol=0)        
        
        
        
        #TOC_Names.to_excel(writer,sheet_name="dimt_TOC")
        
        #generic outputs being sent safely to excel
        
        output_to_excel(variance,'No data shows as being revised since previous load',writer,"absolute revisions")
        
        output_to_excel(PCvariance,'No data shows as being revised since previous load',writer, "percentage revisions")
        output_to_excel(filteredDWPPC,f'No data shows as having significant Period on Period change for {upperdatefilter}',writer,f"PonP change for {upperdatefilter}")
        output_to_excel(DWYPC,'No data shows as being outside 95% confidence interval for Year on Year change',writer,f"YonY change for {str(upperdatefilter)[:8]}")

        describe_current = DWfiltered.describe()
        summary_text_new = pd.DataFrame({f"Latest Load is source item id {latestSID}":[]})
        summary_text_new.to_excel(writer,sheet_name="Summary_Data",startrow=0,startcol=0)
        describe_current.to_excel(writer,sheet_name="Summary_Data",startrow=1,startcol=0)

        describe_old = DWoldfiltered.describe()
        print("Summary of old data\n")
        summary_text_old = pd.DataFrame({f"Previous Load is source item id: {previousSID}": [ ]})
        print("Summary of new data\n")
        summary_text_old.to_excel(writer,sheet_name="Summary_Data", startrow=15,startcol=0)
        describe_old.to_excel(writer,sheet_name="Summary_Data", startrow=16,startcol=0)
 
    writer.save()

if __name__ == '__main__':
    main()
