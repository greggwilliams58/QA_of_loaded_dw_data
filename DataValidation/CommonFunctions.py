import sqlalchemy
import pandas as pd
import numpy as np
import pprint as pp
import os
import glob
import re
from DWSource import getDWdimension
from scipy import stats
import xlsxwriter



def GetMetaData(feednumber, feedname):
    """
    This gets MetaData information from metadata folder.  It returns an ordered dictionary holding the metadata info.
    
    Parameters:
    feed number:    A string holding the Feed Number
    feed name:      A string holding the Feed Name

    Returns:
    An ordered dictionary holding the metadata info for later use
    """
    #check for 
    if feednumber == '351':
        feedname = 'SDC'
    
    
    if feedname not in ['APPEALS','COMPLAINTS','SDCNATIONAL']:
    

        rootOfMetaData = '//orrdwfs1.file.core.windows.net/feeds/LIVE/Metadata/'
    
        filepathToMetaData = rootOfMetaData + feednumber + '/' + feedname + '/' + 'Metadata_' + feednumber + '_' + feedname + '.xls'
   
    
        metadata = pd.read_excel(filepathToMetaData,header=1,sheet_name = ['Feed','Feed Parts','Feed Sub Parts','Feed Sub Part Area','Feed Sub Part Area Groupby','Feed Sub Part Area Columns'])
        #pp.pprint(metadata)
        #pp.pprint(metadata['Feed Sub Parts']['Feed Sub Part Code'])
        return metadata
    else:
        pass


def GetSourceData(feednumber,feedname,metadata):
    """
    This gets the source data as placed into the ETL System and returns the data as a dataframe

    Parameters
    feednumber:     An int representing the feed number being searched for
    feedname:       A string representing the feed_name being searched for
    sourcefilename: A string representing the name of the file being searched for
    metadata:       A dictionary holding the metadata information about the source file

    Returns
    sourcedata:     A dataframe holding the source data
    """
    
    
    rootOfSourceData = '//orrdwfs1.file.core.windows.net/feeds/LIVE/Process/'
    
    #get containing folder
    foldersToSearch = ['ATOC','DFT','LUL','NR','ONS','ORR','TOCs','TS']
    for folder in foldersToSearch:
        if os.path.isdir(rootOfSourceData + folder + '/' + feednumber):
            feedfolder = rootOfSourceData + folder + '/' + feednumber
    
    listOfLoadFolders = glob.glob(feedfolder+'/*')

    relevantfiles = []
    #only get matching feednames
    for folder in listOfLoadFolders:
        for file in glob.glob(folder+'/'+'*'+feedname+'*.xls*'):
            relevantfiles.append(file)

    latestload = relevantfiles[-1]

    #get useable metadata here
    sheetname = str(metadata['Feed Sub Parts']['Feed Sub Part Code'][0]).replace('$','')
    rangefrom = metadata['Feed Sub Part Area']['Range From'][0]
    rangeto = metadata['Feed Sub Part Area']['Range To'][0]


    #get columns here, strip out the numbers
    columnsrange = (rangefrom +":" +rangeto)
    columns = re.sub(r'\d+',"",columnsrange)
    print(f"columns: {columns}")

    #get upper range here
    startofrows = int(re.sub(r'[^\d]+','',rangefrom))-2
    print(f"header: {startofrows}")

    #get lower range here
    endofrows = int(re.sub(r'[^\d]+','',rangeto))
    print(f"number of rows: {endofrows}")

    #print(numofrows)
    #get raw_data loaded
    sourcedata = pd.read_excel(latestload,sheet_name = sheetname, use_cols=columns,nrows=endofrows,header = startofrows)

    #pp.pprint(sourcedata)
    #print(sourcedata.info())

    return sourcedata


def lookupTOCdata(source,key_elements,sourcereference,dimtref):
    """
    This looks up TOC names against the dimt_train_operating_company table.  If no TOC lookup is needed, the placeholder 'NA' is used to return the original source value unaltered.
    This leaves the main program flow undisturbed

    Parameters:
    source:             A dataframe holding the source data from the DW
    key_elements:       A list holding the non-numeric, non-source_item_id, non-toc fields
    sourcereference:    A list holding strings representing the name of the TOC_reference
    dimtref:            A string holding the name of the relevant dimt name

    Returns:
    swt:                A dataframe with the TOC names and the TOC_Id removed

    """
    #if dataset has no toc values to lookup, just return the data as is
    if dimtref == 'NA':
        swt = source
        
    else:

        #if dataset is 207_GOVTSUPbyTOC use the specialsed reference table
        if dimtref == 'toc_ref':
            TOC_Names = getDWdimension('dbo','dimt_207_toc')
            dimtlookupname = 'Output_Name'
        
        else:
            TOC_Names = getDWdimension('dbo','dimt_train_operating_company')
            dimtlookupname = 'train_operating_company_name'
            
        swt = source

        #force conversion of TOC_Key saved as text into int, if not using the toc_key instead
        
        #print(f"the sourcereference is {sourcereference}")
        if sourcereference == 'train_operating_company_id':
            swt[sourcereference] = swt[sourcereference].apply(pd.to_numeric)

        for counter,lookup in enumerate(sourcereference):
            #print(f"This is the sourcereference: {sourcereference}\n")
            #print(f"This is the lookup value: {lookup}\n")
            #print(f"This is the dim reference: {dimtref}\n")

            temp_df = swt.merge(TOC_Names[[dimtref,dimtlookupname]],how='left',left_on=lookup,right_on=dimtref)
            temp_df = temp_df.rename(columns={dimtlookupname:lookup + '_toc_name'})
            temp_df = temp_df.rename(columns={lookup + '_toc_name': 'toc_name'})

            if lookup + 'toc_name' not in key_elements:
                key_elements.append(lookup + '_toc_name')
                #move toc to second place
                key_elements.insert(1,key_elements.pop())
                print(f"key elments inside loop{key_elements}")
            else:
                pass
            
            if 'train_operating_company_key_toc_name' in key_elements:
                key_elements.replace('train_oeprating_company_key_toc_name','toc_name')
            #print("This is temp_data")
            #print(temp_df.info())
            
            swt = temp_df

            print("This is swt_data")
            print(swt.info())

        #remove the unnecessary linking fields from the merge
        swt = swt.loc[:,~swt.columns.str.startswith('train_operating_company_id_')]
        
        swt = swt.loc[:,~swt.columns.str.startswith('train_operating_company_key')] 
        
        #this removes keys from 332_PPM_CaSL failures
        if 'TOC_Victim_Key' in swt.columns:
            swt = swt.drop(['TOC_Victim_Key'],axis=1)

        if 'TOC_Perpetrator_Key' in swt.columns:
            swt = swt.drop(['TOC_Perpetrator_Key'],axis=1)

        #this removes keys from the key elements list
        if 'train_operating_company_key' in key_elements:
            key_elements.remove('train_operating_company_key')
        #this relates to 332_PPM_CaSLFailures
        if 'TOC_Victim_Key' in key_elements:
            key_elements.remove('TOC_Victim_Key')
        if 'TOC_Perpetrator_Key' in key_elements:
            key_elements.remove('TOC_Perpetrator_Key')
            
        
    #remove duplicates from key_elements
    key_elements = list(set(key_elements))



    swt = setandsortindex(swt,key_elements)


    return swt


def setandsortindex(source,key_elements):
    if 'source_item_id' in source:
        del source['source_item_id']

    if 'load_id' in source:
        del source['load_id']
    
    #na values are replaced with text placeholder
    source = source.fillna(value="nothing")



    print("this is the source information in setandsort")
    print(source.info())
    print("This are the key elements in setandsort")
    print(key_elements)

    source.set_index(key_elements,inplace=True)

    #source = source.reindex(columns=key_elements,fill_value="missing")

    source.sort_index(axis=0,level=key_elements, inplace=True)
    
    print("this is the source information after setandsort")
    print(source)
 

    return source
    

def individualranges(df, key_elements,change_type,feed_number):
    """
    This deduces the number of key/levels in the full dataset and converts each column and key combination into a series
    This series has 0 values and NULL removed
    The series is then recombined into a dataframe and exported



    """
    #remove temporal element if not 209_Infrastructure
    if 'financial_period_key' in key_elements:
        key_elements.remove('financial_period_key')
    elif 'Financial_Period_Key' in key_elements:
        key_elements.remove('Financial_Period_Key')
    elif 'financial_year_key' in key_elements and feed_number != '209':
        key_elements.remove('financial_year_key')
    elif 'Financial_year_of Publication' in key_elements:
        key_elements.remove('Financial_year_of Publication')
    elif 'Financial_Period_key' in key_elements and feed_number in ['312','336','335']:
        key_elements.remove('Financial_Period_key')
    elif feed_number == '321':
        key_elements.remove('Date_key_with_Quarters')
    elif feed_number == '338' and 'calendar_month_key' in key_elements:
       key_elements.remove('calendar_month_key')
    #elif feed_number == '329':
    #    key_elements.remove('Time_Period_Key')
        
    else:    
        pass
    
    #this relates to the 224_sectiona complaints dataset
    if 'Complaint_category_id' in key_elements:
        key_elements.remove('Complaint_category_id')

    if 'sectiona_id' in key_elements:
        key_elements.remove('sectiona_id')
    
    #this relates to the 119 targets file
    if 'Target_Group' in key_elements:
        key_elements.remove('Target_Group')

    if 'Target_Purpose' in key_elements:
        key_elements.remove('Target_Purpose')

    number_of_index_levels = df.index.nlevels
   

    measure_list = []
    print("Looping through ranges of individuals")
    print(f"within individual ranges key elements: {key_elements}")
    

    #print(df.info())
    for (colname,coldata) in df.iteritems():
        nozerocoldata = coldata.replace(0,np.NaN)

        nonullcoldata = nozerocoldata.dropna()

        #print("This is no nan data")
        #print(nonullcoldata)
        for group_level,new_series in nonullcoldata.groupby(key_elements):
            #replace NaN in index here
            

            print(f"new series here: {group_level}")
            if change_type == 'PPC':

                nonull_series = new_series.pct_change().dropna()
                filtered_series = set_boundaries(nonull_series)
                measure_list.append(filtered_series)

            elif change_type == 'YPC':

                nonull_series = new_series.pct_change(13).dropna()
                filtered_series = set_boundaries(nonull_series)
                measure_list.append(filtered_series)
          
            else:
                measure_list.append(new_series)

    final_df = series_to_df(measure_list,key_elements,number_of_index_levels)

    return final_df



def set_boundaries(raw_series):
    """
    This filters a given series by a confidence interval of 95%
    """
    #print(raw_series)
    print("setting boundaries now")
    series_stdev = raw_series.std()

    series_mean = raw_series.mean()
    boundary = stats.norm.interval(0.95,loc=series_mean,scale=series_stdev)

    filter_cond = raw_series.apply(lambda x : x < boundary[0] or x > boundary[1])

    #print(f"series name is {raw_series.name} and boundaries are {boundary[0]} and {boundary[1]}")

    return raw_series[filter_cond]


def series_to_df(measure_list,index_keys,index_levels):
    """
    This loops through a list of series and turns the series into a concatinated dataframe with the names of the series converted to DataFrame Columns

    Parameters:
    measure_list:       A list of series objects holding the indvidual measures for each toc and each named measure
    index_key:          A list of field names from the dataset holding the index values
    index_levels:       An integer holding the number of key fields

    Returns:
    final_stacked_df:   A dataframe holding the data with each measure as a column

    """
    print("converting series to df")
    #set up empty dataframes to hold output
    final_stacked_df = pd.DataFrame()
    interim_df = pd.DataFrame()

    #add the name to index of the series
    reorder_index = list(range(1,index_levels+1))
    reorder_index.append(0)
 
    #loop through the list of series objects
    for counter,series_data in enumerate(measure_list):
        #convert series into a data frame with the series name added to the index as a measure
        df1 = (pd.concat([measure_list[counter]],keys=[series_data.name])).reorder_levels(reorder_index).to_frame('observations')

        #append the current df to the cumulative df
        interim_df = pd.concat([df1,interim_df])
        
        #convert  the measures into columns
        final_stacked_df = (interim_df.set_index(interim_df.groupby(interim_df.index).cumcount(),append=True)['observations']
                                .unstack([reorder_index[-2]])
                                .reset_index(level=reorder_index[-2], drop=True))
    
    print(final_stacked_df)
    return final_stacked_df



def output_to_excel(df,txt,w,sname):
    """
    Helper function to safely export outputs to excel
        
    Parameters
    df:     dataframe holding output from processing data
    txt:    string hold text for "no data message"
    w:      writer object used be ExcelWriter
    sname:  Name of sheet
    """
    print(f"Exporting {sname} to Excel\n")
    if df.empty == True:
        v = [txt]    
        text = pd.DataFrame(v,columns=['a'])
        text.to_excel(w,sheet_name=sname)
    else:
        df.to_excel(w,sheet_name=sname)
