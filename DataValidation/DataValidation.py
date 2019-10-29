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
    FNum = '105'
    FName = 'TMILEAGE'
    lowerdatefilter = 2018201901
    upperdatefilter = 2019202005
    
    #metadata for source and metadatafiles
    #FNum = '313'
    #FName = 'DISAGGPPMCASL'
    
    #dictionary holding the 0) schema, 1)table_name, 2)index fields
    unique_feed_features = {   
                    
                    '105TMILEAGE':['NR','factt_105_train_mileage',['financial_period_key','train_operating_company_key','train_operating_company_name','operator_type','sector']],
                    '105FMILEAGE':['NR','factt_105_freight_mileage',['financial_period_key','train_operating_company_key','train_operating_company_name','provisional']] 
                     
                     }

    #metadata for DW data
    schema = unique_feed_features[FNum+FName][0]
    table_name = unique_feed_features[FNum+FName][1]
    source_item_id = getSourceItemId(schema,table_name)
    #pp.pprint(source_item_id)

    MD = GetMetaData(FNum,FName)
    SD = GetSourceData (FNum,FName,MD)

    #pp.pprint(MD)

    #pp.pprint(SD)

    latestSID = source_item_id[-1]
    previousSID = source_item_id[-2]
    #latestSID = 8947
    #previousSID = 8047
   
    DW = getDWdata(schema,table_name,latestSID)
    DWold = getDWdata(schema,table_name,previousSID)

    DW = lookupTOCdata(DW, unique_feed_features[FNum+FName][2])
    DWold = lookupTOCdata(DWold,unique_feed_features[FNum+FName][2])


    #only get data greater than  2018201901
    DWfiltered =    DW.loc[(DW.index.get_level_values('financial_period_key') >= lowerdatefilter) & (DW.index.get_level_values('financial_period_key') <= upperdatefilter) ]
    DWoldfiltered = DWold.loc[(DWold.index.get_level_values('financial_period_key') >= lowerdatefilter) & (DWold.index.get_level_values('financial_period_key') <= upperdatefilter) ]


    DWPPC = individualranges(DWfiltered,unique_feed_features[FNum+FName][2],'PPC') 
    DWYPC = individualranges(DWfiltered,unique_feed_features[FNum+FName][2],'YPC')
    
    filteredDWPPC = DWPPC[DWPPC.index.get_level_values('financial_period_key')>= upperdatefilter]


    #absolute variance by subtraction
    variance_raw = DWfiltered.subtract(DWold)
    variance = individualranges(variance_raw, unique_feed_features[FNum+FName][2],'individual')
    
    #percentage change by subtraction and then division
    PCvariance_raw = (( DWfiltered - DWoldfiltered)/ DWold)*100
    PCvariance = individualranges(PCvariance_raw,unique_feed_features[FNum+FName][2],'individual')



    #export various dataframes to excel
    with pd.ExcelWriter(f"data validation for {FNum}_{FName}.xlsx",engine='openpyxl') as writer:

        SD.to_excel(writer,sheet_name='Source_data')
        DWoldfiltered.to_excel(writer,sheet_name="Previous_DW_load")
        DWfiltered.to_excel(writer,sheet_name="Latest_DW_load")
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
        summary_text_old = pd.DataFrame({f"Previous Load is source item id: {previousSID}": [ ]})
        summary_text_old.to_excel(writer,sheet_name="Summary_Data", startrow=15,startcol=0)
        describe_old.to_excel(writer,sheet_name="Summary_Data", startrow=16,startcol=0)

       
    writer.save()




if __name__ == '__main__':
    main()
