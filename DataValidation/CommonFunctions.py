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
    
    rootOfMetaData = '//orrdwfs1.file.core.windows.net/feeds/LIVE/Metadata/'
    
    filepathToMetaData = rootOfMetaData + feednumber + '/' + feedname + '/' + 'Metadata_' + feednumber + '_' + feedname + '.xls'
   

    metadata = pd.read_excel(filepathToMetaData,header=1,sheet_name = ['Feed','Feed Parts','Feed Sub Parts','Feed Sub Part Area','Feed Sub Part Area Groupby','Feed Sub Part Area Columns'])
    #pp.pprint(metadata)
    #pp.pprint(metadata['Feed Sub Parts']['Feed Sub Part Code'])
    return metadata


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
    foldersToSearch = ['ATOC','DFT','LUL','NR','ONS','ORR','TOCs']
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

    TOC_Names = getDWdimension('dbo','dimt_train_operating_company')
 
    TOC_Names.set_index(dimtref, inplace=True)


    for lookup in sourcereference:
        print(lookup)
        print("source before join\n")
        print(source.info())
        
        source.set_index(lookup,inplace=True)

        source_with_toc = source.join(TOC_Names['train_operating_company_name'])


        print("source after join\n")
        print(source.info())
        print("source after index reset")
        swt = source_with_toc.reset_index()
        print(swt.info())
        
        #source_with_toc[lookup] = source_with_toc.index



        swt.set_index(key_elements,inplace=True)

        swt.sort_index(axis=0,level=key_elements, inplace=True)
    

    return swt

    
def individualranges(df, key_elements,change_type):
    """
    This deduces the number of key/levels in the full dataset and converts each column and key combination into a series
    This series has 0 values and NULL removed
    The series is then recombined into a dataframe and exported

    """
    #remove temporal element
    if 'financial_period_key' in key_elements:
        key_elements.remove('financial_period_key')
    else:
        pass


    number_of_index_levels = df.index.nlevels
    
    measure_list = []

    for (colname,coldata) in df.iteritems():
        nozerocoldata = coldata.replace(0,np.NaN)
        nonullcoldata = nozerocoldata.dropna()

        for group_level,new_series in nonullcoldata.groupby(key_elements):

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
    if df.empty == True:
        v = [txt]    
        text = pd.DataFrame(v,columns=['a'])
        text.to_excel(w,sheet_name=sname)
    else:
        df.to_excel(w,sheet_name=sname)
