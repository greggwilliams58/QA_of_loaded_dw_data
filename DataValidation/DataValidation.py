from CommonFunctions import GetMetaData,GetSourceData,lookupTOCdata, individualranges,output_to_excel
from DWSource import getDWdata,getSourceItemId,getDWdimension
from plotting_data import plot_the_data
import pandas as pd
import pprint as pp
import numpy as np
import xlsxwriter


def main():
    pd.options.mode.chained_assignment = 'raise'
    pd.set_option("display.precision",16)
    FNum = '104'
    FName = 'DELAYS'
    lowerdatefilter = 2018201901
    upperdatefilter = 2019202005
    
    #testing for changes
    #metadata for source and metadatafiles
    #FNum = '313'
    #FName = 'DISAGGPPMCASL'
    
    #lists holding exceptional case information
    toobigforexport = ['104DELAYS']
    notoclookup = ['106TSR']

    #dictionary holding the 0) schema, 1)table_name, 2)index fields,3)source TOC lookup fields,4)dimt_toc_lookup field



    unique_feed_features = {   
                    '104DELAYS':['NR','factt_104_delays',['financial_period_key','route','delay_type','responsible_org_code','toc_affected','incident_category','area'],['responsible_org_code','toc_affected'],'train_operating_company_id'],
                    '105TMILEAGE':['NR','factt_105_train_mileage',['financial_period_key','train_operating_company_key','operator_type','sector'],['train_operating_company_key'],'train_operating_company_key'],
                    '105FMILEAGE':['NR','factt_105_freight_mileage',['financial_period_key','train_operating_company_key','provisional'],['train_operating_company_key'],'train_operating_company_key'] 
                     
                     }

    #metadata for DW data
    schema = unique_feed_features[FNum+FName][0]
    table_name = unique_feed_features[FNum+FName][1]
    source_item_id = getSourceItemId(schema,table_name)
    #pp.pprint(source_item_id)

    MD = GetMetaData(FNum,FName)
    #SD = GetSourceData (FNum,FName,MD)


    latestSID = source_item_id[-1]
    previousSID = source_item_id[-2]
    #latestSID = 8947
    #previousSID = 8047
    #datasets too large for DW_output
    
    


    print("getting DW data")   
    DW = getDWdata(schema,table_name,latestSID)
    DWold = getDWdata(schema,table_name,previousSID)


    print("looking up TOC info")
    DW = lookupTOCdata(DW, unique_feed_features[FNum+FName][2],unique_feed_features[FNum+FName][3],unique_feed_features[FNum+FName][4]   )
    DWold = lookupTOCdata(DWold,unique_feed_features[FNum+FName][2],unique_feed_features[FNum+FName][3],unique_feed_features[FNum+FName][4] )

    #only get data greater than  2018201901
    print("filtering by dates")
    DWfiltered =    DW.loc[(DW.index.get_level_values('financial_period_key') >= lowerdatefilter) & (DW.index.get_level_values('financial_period_key') <= upperdatefilter) ]
    DWoldfiltered = DWold.loc[(DWold.index.get_level_values('financial_period_key') >= lowerdatefilter) & (DWold.index.get_level_values('financial_period_key') <= upperdatefilter) ]

    print("getting individual ranges for PPC")
    DWPPC = individualranges(DWfiltered,unique_feed_features[FNum+FName][2],'PPC') 
    print("getting individual range for YPC")
    DWYPC = individualranges(DWfiltered,unique_feed_features[FNum+FName][2],'YPC')
    
    filteredDWPPC = DWPPC[DWPPC.index.get_level_values('financial_period_key')>= upperdatefilter]

    #absolute variance by subtraction
    print("getting raw variance")
    variance_raw = DWfiltered.subtract(DWold)
    print("getting raw individual variance")
    variance = individualranges(variance_raw, unique_feed_features[FNum+FName][2],'individual')
    
    #percentage change by subtraction and then division
    print("getting % variance")
    PCvariance_raw = (( DWfiltered - DWoldfiltered)/ DWold)*100
    print("getting * individual variances")
    PCvariance = individualranges(PCvariance_raw,unique_feed_features[FNum+FName][2],'individual')

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
