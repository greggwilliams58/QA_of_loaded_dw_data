from CommonFunctions import GetMetaData,GetSourceData,lookupTOCdata, individualranges,output_to_excel,setandsortindex
from DWSource import getDWdata,getSourceItemId,getDWdimension
from plotting_data import plot_the_data
import pandas as pd
import pprint as pp
import numpy as np
import xlsxwriter
import tkinter

def main():
    pd.options.mode.chained_assignment = 'raise'
    pd.set_option("display.precision",16)
    FNum = '353'
    FName = 'ComplaintsOutliersA'
    lowerdatefilter = 2018201901
    upperdatefilter = 2019202008
    
    #testing for changes
    #metadata for source and metadatafiles
    #FNum = '313'
    #FName = 'DISAGGPPMCASL'
    
    #lists holding exceptional case information
    toobigforexport = ['104DELAYS','205LENNON','332PPMCaSLFailure','353ComplaintsOutliersA']
    notoclookup = ['106TSR','202SRA','207GOVTSUP','209NRTINFRA','224APPEALS','311ASR','321OH','326FDMbySFC','327ReliabilityandSustainability','330MaintenanceVols','338FREIGHT16VALUED','339TRAFFICMONTHLYVALUED','346NRComplaintsData','351SDCNational']
    

    #dictionary holding the key-pathtometadata 0) schema, 1)table_name, 2)index fields,3)source TOC lookup fields,4)dimt_toc_lookup field, 5) date_type field

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
                    '224APPEALS':['NETL','factt_224_LTW_TF_Appeals_Closed_Stats_Release',['financial_quarter','publication_status','Organisation','Operator','Franchise type'],['NA'],'NA','financial_quarter'],
                    #'224COMPLAINTS':['NETL','factt_224_LTW_TF_Complaints_Received_Stats_Release',['financial_quarter','publication_status','Organisation','Operator','Franchise type','ORR_Complaint_Category','NRPS Complaint Category'],['NA'],'NA','financial_quarter']
                    '225FREIGHTLIFTED':['ORR','factt_225_freightlifted',['financial_quarter_key','Year','train_operating_company_id'],['train_operating_company_id'],'train_operating_company_id','financial_quarter_key'],
                    '226FREIGHTMOVED':['NR','factt_226_freightmoved',['financial_period_key','period_number','week_number','train_operating_company_key','service_group' ,'commodity_name' ,'service_code','service_code_chargeable','electric','electricity_supply'],['train_operating_company_key'],'train_operating_company_key','financial_period_key'],
                    #303_Delay_targets not loaded since 2018201913
                    '311ASR':['RSSB','factt_311_ASR',['Data_Supplier','Table','Date_key','Injury','Category'],['NA'],'NA','Date_key'],
                    #'312Top50incidents':['NR','factt_312_Top_50_incidents',['Financial_Period_key','Incident_Period_key' ,'Rank','Incident_Date','Incident_Number' ,'Incident_Category' ,'Incident_Description','Incident_Location','Network_Rail_Route','Responsible_Organisation','Attribution_Status'],['NA'],'NA','Financial_Period_key'],
                    '313DISAGGPPMCASL':['NR','factt_313_DISAGGPPMCaSL',['financial_period','train_operating_company_key','Financial Year & Period','Financial Year','Financial Period Number','TOC Sector','Operator Name','Sector','v_ORR sub-operator PPM split'],['train_operating_company_key'],'train_operating_company_key','financial_period'],
                    #'321OH':['ORR','factt_321_OH',['Data_Supplier','Scope','Operator','Date_key_with_Quarters','Disease_Type','Category'],['NA'],'NA','Date_key_with_Quarters']
                    '324AverageLateness':['NR','factt_324_Average_Lateness',['financial_period_key','TOC_key'],['TOC_key'],'train_operating_company_key','financial_period_key'],
                    '326FDMbySFC':['NR','factt_326_Freight_delivery_matrix',['financial_period_key','SFC'],['NA'],'NA','financial_period_key'],
                    '327ReliabilityandSustainability':['NR','factt_327_ReliabilityAndSustainability',['Financial_Period','route_id','Measure','Snapshot','Normalised','Time_Period','Type'],['NA'],'NA','Financial_Period'],
                    #'329RenewalsVols':['NR','factt_329_RenewalVolumes',['Time_Period_Key','Time_Period_Flag','Route_Name','Measure_Name','Measure_Type','Measure_sub_group','Measure_group'],['NA'],'NA','Time_Period_Key'],
                    '330MaintenanceVols':['NR','factt_330_Maintenance_Volumes',['Financial_Quarter_Key','Financial_Quarter_Name','Financial_Year_Key','Route_code','Route_name','Measure_Code','Measure_Description','Published_UOM','Ellipse_UOM'],['NA'],'NA','Financial_Quarter_Key'],
                    '332PPMCaSLFailure':['NR','factt_332_PPM_CASL_Failures',['Financial_Period','National_or_Route_Data','TOC_Victim_Key','Sector_Victim','Sector_Victim_key','TOC_Perpetrator_Key','Sector_Perperator','Sector_Perperator_key','Delay_Type','Incident_Category','Route_Name','Measure_Name'],['TOC_Victim_Key','TOC_Perpetrator_Key'],'train_operating_company_key','Financial_Period'],
                    '335TrainMiles':['NR','factt_335_TrainMiles',['Financial_Period_Key','Financial_Year_Key','Route_Key','TOC_Key'],['TOC_Key'],'train_operating_company_key','Financial_Period_Key'],
                    '336DRPCandAssist':['ATOC','factt_336_DRPCandAssist',['Financial_Period_key','Financial_year_key','TOC_key','Measure_group','Metric'],['TOC_key'],'train_operating_company_key','Financial_Period_key'],
                    '338FREIGHT16VALUED':['ORR','factt_338_EuroFreightData',['calendar_month_key','summary_or_detailed','vehicle_type','national_grouping','nation_name','nation_code','CAFO_FOCA'],['NA'],'NA','calendar_month_key'],
                    '339TRAFFICMONTHLYVALUED':['ORR','factt_339_EuroTunnelTraffic',['calendar_month_key','Travel_Direction','Vehicle_category'],['NA'],'NA','calendar_month_key'],
                    '340SECTIONCATT':['TOCs','factt_340_sectionC',['financial_period_key','TOC_Key','Measure_Name'],['TOC_Key'],'train_operating_company_key','financial_period_key'],
                    #'343ATOCSafetyKPIs': only one load in the warehouse
                    '346NRComplaintsData':['NR','factt_346_NRComplaintsData',['Financial_Period_Key','TOC_Name','Section','Section_description','Section_detailed_description','Level 1 Metric','Level 2 Metric','Level 3 Metric'],['NA'],'NA','Financial_Period_Key'],
                    '348FreightMiles':['NR','factt_348_FreightMiles',['Financial_Period_Key','Financial_Year_Key','Route_Key','TOC_Key'],['TOC_Key'],'train_operating_company_key','Financial_Period_Key'],
                    '350DLRS':['NR','factt_350_DLRS',['financial_period_key','toc_key','measure'],['toc_key'],'train_operating_company_key','financial_period_key'],
                    #only one load and 2.5 million rows'350DLRSSector':['NR','factt_350_DLRSSector',['financial_period_key','toc_key','Service_Group_Code','Service_Group_Description','Geography_Code','Geography_Description','measure'],['toc_key'],'train_operating_company_key','financial_period_key']
                    '351SDCNATIONAL':['NR','factt_351_SDC_national',['financial_period_key','Data_category','measure'],['NA'],'NA','financial_period_key'],
                    '351SDCSUBTOC':['NR','factt_351_SDC_subtoc',['financial_period_key','toc_key','toc_subtoc_data','sub_operator_key','data_description'],['toc_key'],'train_operating_company_key','financial_period_key'],
                    '351SDCTOC':['NR','factt_351_SDC_toc',['region_key','financial_period_key','toc_key','toc_subtoc_data','data_description','measure'],['toc_key'],'train_operating_company_key','financial_period_key'],
                    '353ComplaintsOutliersA':['TOCs','factt_353_sectiona',['financial_period_key','train_operating_company_key','Level_1_Category','Level_2_Category'],['train_operating_company_key'],'train_operating_company_key','financial_period_key']
                    }

    #metadata for DW data
    schema = unique_feed_features[FNum+FName][0]
    table_name = unique_feed_features[FNum+FName][1]
    source_item_id = getSourceItemId(schema,table_name)
    source_item_id.sort()
    
    pp.pprint(source_item_id)

    if schema not in ['NETL','RSSB']:
        MD = GetMetaData(FNum,FName)

    #SD = GetSourceData (FNum,FName,MD)

    #check if more than one load in table
    if len(source_item_id) == 1:
    
        latestSID = source_item_id[-1]
        previousSID = source_item_id[-1]

    else:
    
        latestSID = source_item_id[-1]
        previousSID = source_item_id[-2]

    #latestSID = 8995
    #previousSID = 7882
    
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
    
    
    try:
        print(f"filtering latest load by date values {lowerdatefilter} and {upperdatefilter}")
        DWfiltered = DWnew.loc[(DWnew.index.get_level_values(unique_feed_features[FNum+FName][5]) >= lowerdatefilter) & (DWnew.index.get_level_values(unique_feed_features[FNum+FName][5]) <= upperdatefilter) ]
    except KeyError:
        print(f"dates supplied {lowerdatefilter} and {upperdatefilter} not in data\n")
        DWfiltered = DWnew
    
    try:
        print(f"filtering previous load by date values {lowerdatefilter} and {upperdatefilter}")
        DWoldfiltered = DWold.loc[(DWold.index.get_level_values(unique_feed_features[FNum+FName][5]) >= lowerdatefilter) & (DWold.index.get_level_values(unique_feed_features[FNum+FName][5]) <= upperdatefilter) ]
    except KeyError:
        print(f"dates supplied {lowerdatefilter} and {upperdatefilter} not in data\n")
        DWoldfiltered = DWold


    print("getting individual ranges for PPC")
    DWPPC = individualranges(DWfiltered,unique_feed_features[FNum+FName][2],'PPC',FNum) 
    print("This is DWPPC")
    print(DWPPC)  
    
    print("getting individual range for YPC")
    DWYPC = individualranges(DWfiltered,unique_feed_features[FNum+FName][2],'YPC',FNum)
    
    try:
        print(f"filtering for latest date only on {upperdatefilter}")
        filteredDWPPC = DWPPC[DWPPC.index.get_level_values(unique_feed_features[FNum+FName][5])>= upperdatefilter]
    except KeyError:
        print(f"dates supplied {upperdatefilter} not in data\n")
        filteredDWPCC = DWPPC

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
        
        print(DWoldfiltered)
        print(DWoldfiltered.info())
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
