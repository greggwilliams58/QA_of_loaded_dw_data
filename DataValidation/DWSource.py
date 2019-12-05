import pandas as pd
from pandas import DataFrame
import pyodbc
import sqlalchemy 
from sqlalchemy import create_engine, MetaData, Table, select, inspect
from sqlalchemy.orm import sessionmaker
import pprint as pp



def getDWdimension(schema_name,table_name):
    """
    This uses SQL Alchemy to connect to SQL Server via a trusted connection and extract a table, which is then coverted into a dataframe.
    This is intended for getting the whole table for dimensional data.

    Parameters
    schema_name:    A string represetnting the schema of the table
    table_name:     A string representing the name of the table

    Returns:
    df:             A dataframe containing the table
    """
    engine = sqlalchemy.create_engine('mssql+pyodbc://AZORRDWSC01/ORR_DW?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes')
    
    conn = engine.connect()

    metadata = MetaData()

    example_table = Table(table_name, metadata,autoload=True, autoload_with=engine, schema=schema_name)

    #get raw table data, filtered by source_item_id
    query = select([example_table])

    df = pd.read_sql(query, conn)
    return df


def getDWdata(schema_name,table_name,source_item_id):
    """
    This uses SQL Alchemy to connect to SQL Server via a trusted connection and extract a filtered table, which is then coverted into a dataframe.
    This is intended for getting the partial table for fact data.

    Parameters
    schema_name:    A string represetnting the schema of the table
    table_name:     A string representing the name of the table
    source_item_id: An integer representing the source_item_id needed

    returns:        A dataframe containing the table   
    """
    engine = sqlalchemy.create_engine('mssql+pyodbc://AZORRDWSC01/ORR_DW?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes')
    
    conn = engine.connect()

    metadata = MetaData()

    example_table = Table(table_name, metadata,autoload=True, autoload_with=engine, schema=schema_name)

    #get raw table data, filtered by source_item_id
    if schema_name == 'NETL':
        query = select([example_table]).where(example_table.c.load_id == source_item_id)
    else:
        query = select([example_table]).where(example_table.c.source_item_id == source_item_id)

    df = pd.read_sql(query, conn)

    #delete unnecessary columns
    #this related to 224_sectiona
    if 'sectiona_id' in df.columns:
        del df['sectiona_id']
    
    #this related to 224_sectiona
    if 'sectionb_id' in df.columns:
        del df['sectionb_id']

    if 'Complaint_category_id' in df.columns:
        del df['Complaint_category_id']

    #this related to 311_ASR
    if 'ASR_ID' in df.columns:
        del df['ASR_ID']

    if 'Scope' in df.columns:
        del df['Scope']

    return df


def getSourceItemId(schema_name,table_name):
    """
    This uses SQL Alchemy to connect to SQL Server via a trusted connection and extract a distinct list of source_item_id in the table
    , which is then coverted into a list.
    This is intended for getting the loads within the source table.

    Parameters
    schema_name:    A string represetnting the schema of the table
    table_name:     A string representing the name of the table
    source_item_id: An integer representing the source_item_id needed

    returns:        
    listSID:        A dataframe containing a distinct list of source_item_ids   

    Possibly add a filter for "draft,approved, published"  The SQL Code is 

    #SELECT distinct TM.[source_item_id]
    #, feeds.status_description
    #FROM [ORR_DW].[NR].[factt_105_train_mileage] as TM
    #INNER JOIN [ORR_DW].[dbo].[uvw_latest_feed_part_version] as feeds
    #ON TM.source_item_id = feeds.source_item_id

    sid = sid.select_from(table_name.join([ORR_DW].[dbo].[uvw_latest_feed_part_version])  )

    """    
    engine = sqlalchemy.create_engine('mssql+pyodbc://AZORRDWSC01/ORR_DW?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes')
    conn = engine.connect()

    metadata = MetaData()

    example_table = Table(table_name, metadata,autoload=True, autoload_with=engine, schema=schema_name)
    
    #feeds_table = Table('uvw_latest_feed_part_version',metadata,autoload=True,autoload_with_engine=engine, schema='dbo')

    #standard path for ETL data
    if schema_name != 'NETL': 
        sid = select([example_table.c.source_item_id.distinct()])
        dfSID = pd.read_sql(sid,conn)
        listSID = dfSID['source_item_id'].tolist()

    #extract load_is from NETL table itself
    else:
        sid = select([example_table.c.load_id.distinct()])
        dfSID = pd.read_sql(sid,conn)
        listSID = dfSID['load_id'].tolist()
    
        #dfSID = pd.read_sql(sid,conn)
    #listSID = dfSID['source_item_id'].tolist()
    
    # to add join here
    #full_data = select([example_table, sid])

    #full_data = full_data.select_from(
    #    example_table.join(sid,example_table.source_item_id = sid.source_item_id))
    return listSID