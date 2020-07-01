import pandas as pd
import sqlalchemy 
import numpy as np
from sqlalchemy import create_engine, MetaData, Table, select, inspect
from glob import glob
import os
from ETL_Of_Data_Warehouse_Data import exportfile

def main():
    """
    This code takes in a csv file representing delay minutes and converts it to the required formats
    """
    combined_df = get_raw_data("Input\\NODW\\NODW_103_DELAY_MINUTES\\")
    
    #import raw csv file
    #raw_data = pd.read_csv("input\Detailed Delay Minutes 2014-15 to 2019-20 All Regions.csv",encoding='cp1252')
    
    #drop unnecessary columns and concat three columns into one
    raw_data = shapecolumns(combined_df)

    #get the financial_period lookup info
    dimt_fp = getfpdata('dbo','dimt_financial_period')

    #lookup up last day of period and add to dataset
    dated_data = handledates(raw_data,dimt_fp)

    #stack the data into a single column for data
    pivoted_data = stackminmaxvalues(dated_data)

    #add new static columns
    prepared_data = addnewcolumns(pivoted_data)

    #export finished dataset
    exportfile(prepared_data,'output//NonDW_based_data//NONDW_103_DELAY_MINUTES//','NON_DW_103_DELAY_MINUTES')

def get_raw_data(originfilepath):
    """
    This takes csv files from a specified folder in the DM format, takes the necessary columns, converts to appropriate datatypes and
    appends them as a combined dataset

    Parameters
    originfilepath:     A string representing the file path to the required folder

    Returns
    combined_data:      A dataframe holding the full range of DM data

    """
    dataframes = []
    filepathsandnames = glob(f'{originfilepath}*.csv')
    numberoffiles = len(filepathsandnames)

    for count, file in enumerate(filepathsandnames,1):
        print(f"Loading {os.path.basename(file)} into memory.")
        print(f"That's {count} out of {numberoffiles}, or {str(int((count/numberoffiles)*100))} percent loaded.\n")
        
        
        datatypes = {'v_Incident Count':'int32','v_PfPI Minutes':'float64'}
        #'v_Incident Count':'int32','v_PfPI Minutes':'float64'
        temp = pd.read_csv(file,encoding='Windows-1252',usecols=['Financial Year & Period','Route','Route Name',
                                                                 'Area','Area Name','Delivery Unit','Delivery Unit Name',
                                                                 'Incident Summary Group','Incident Category','Incident Category Description',
                                                                 'Incident Reason','Incident Reason Description','Responsible Organisation',
                                                                 'Responsible Organisation Name','Responsible Manager','Responsible Manager Name',
                                                                 'Responsible Function Level 3 Desc','Responsible Function Level 3 Name',
                                                                 'v_Incident Count','v_PfPI Minutes'],dtype=datatypes)

        


        temp.rename(columns={'Financial Year & Period':'Financial.Year...Period','Route':'Route','Route Name':'Route.Name Original',
                             'Area':'Area','Area Name':'Area.Name','Delivery Unit':'Delivery.Unit','Delivery Unit Name':'Delivery.Unit.Name',
                             'Incident Summary Group':'Incident.Summary.Group','Incident Category':'Incident.Category','Incident Category Description':'Incident.Category.Description',
                             'Incident Reason':'Incident.Reason','Incident Reason Description':'Incident.Reason.Description','Responsible Organisation':'Responsible.Organisation',
                             'Responsible Organisation Name':'Responsible.Organisation.Name','Responsible Manager':'Responsible.Manager','Responsible Manager Name':'Responsible.Manager.Name',
                             'Responsible Function Level 3 Desc':'Responsible.Function.Level.3.Desc','Responsible Function Level 3 Name':'Responsible.Function.Level.3.Name',
                             'v_Incident Count':'v_Incident.Count','v_PfPI Minutes':'v_PfPI.Minutes'},inplace=True)

        dataframes.append(temp)
    
    combined_data = pd.concat(dataframes)
        
    #print('this is combined info')
    #print(combined_data.info())

    return combined_data


def shapecolumns(df):
    """
    This procedure drops unnecessary columns and creates option_1 column by concatting three columns together

    Parameter:
    df          A dataframe holding the raw data

    Returns:
    df          A dataframe holding the transformed dataframe
    """
    print("dropping unnecessary columns\n")

    #get rid of uncessary columns from separate file
    df = df.drop(['Route','Route.Name Original','Area','Area.Name',
                              'Delivery.Unit','Incident.Summary.Group','Incident.Category.Description',
                              'Responsible.Organisation','Responsible.Manager','Responsible.Manager.Name',
                              'Responsible.Function.Level.3.Desc','Responsible.Function.Level.3.Name'],axis=1)


    #join the three columns together
    df['Option_1'] = df['Incident.Reason.Description'] + "[" + df['Incident.Category'] + "]" + "[" + df['Incident.Reason'] + "]"

    #drop unnecessary columns
    df = df.drop(['Incident.Reason.Description','Incident.Category','Incident.Reason'],axis= 1 )

    return df


def handledates(raw_dataset,fp):
    """
    This procedure takes the financial period column and converts it to a timedate format representing the last day of the period.

    Parameters
    raw_dataset:        A dataframe holding the data
    fp:                 A dataframe holding the financial_period_key and last_day_of_period information

    Returns
    joined_data:        A dataframe holding the date information
    """
    print("adding date information in required format\n")
    #format the date into fp key format as an int
    
    #drop na dates (caused by blank rows at end of dataset
    raw_dataset.dropna(subset=['Financial.Year...Period'],inplace=True)


    raw_dataset['Financial.Year...Period'] = raw_dataset['Financial.Year...Period'].str.replace('/','20')
    raw_dataset['Financial.Year...Period'] = raw_dataset['Financial.Year...Period'].str.replace('_P','')

    raw_dataset = raw_dataset[raw_dataset['Financial.Year...Period'] != 'Financial Year & Period']
    

    raw_dataset['Financial.Year...Period'] = raw_dataset['Financial.Year...Period'].astype('int32')

    joined_data = pd.merge(raw_dataset,fp,how='inner',left_on='Financial.Year...Period',
                           right_on='financial_period_key')

    joined_data['Financial.Year...Period'] = joined_data['financial_period_end_date']
    del joined_data['financial_period_end_date']
    
    return joined_data



def getfpdata(schema_name,table_name):
    """
    This uses SQL Alchemy to connect to SQL Server via a trusted connection and extract a filtered table, which is then coverted into a dataframe.
    This is intended for getting financial_period_data

    Parameters
    schema_name:    A string represetnting the schema of the table
    table_name:     A string representing the name of the table


    returns:        
    df:             A dataframe containing columns financial_period_key and financial_period_end_date 
    """

    print(f"getting DW data from {table_name}\n")

    engine = sqlalchemy.create_engine('mssql+pyodbc://AZORRDWSC01/ORR_DW?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes')
    
    conn = engine.connect()

    metadata = MetaData()

    example_table = Table(table_name, metadata,autoload=True, autoload_with=engine, schema=schema_name)

    #get raw table data, filtered by source_item_id
    query = select([example_table.c.financial_period_key,example_table.c.financial_period_end_date])
    
    df = pd.read_sql(query, conn)
    
    return df



def stackminmaxvalues(unpivoted_data):
    """
    This takes the DM data and stacks the data into a single column.  It also calculates merges the min/max values for DM and Count by 
    converting the irrelevant column into 0 and then adding that to the relevant column.  It also increments max value by 1 where max
    and min are equal.

    Parameter
    unpivoted_data:     A dataframe holding the data in wide format

    Returns
    new_data:           A dataframe holding the data in long format, with combined min/max values
    """
    print("calculating the min and max values\n")
    
    #calculate min and max for both minutes and incident count
    unpivoted_data['max_inc_count'] = unpivoted_data.groupby(['Delivery.Unit.Name','Responsible.Organisation.Name','Option_1'])['v_Incident.Count'].transform(np.max)
    unpivoted_data['min_inc_count'] = unpivoted_data.groupby(['Delivery.Unit.Name','Responsible.Organisation.Name','Option_1'])['v_Incident.Count'].transform(np.min)

    unpivoted_data['max_minutes'] = unpivoted_data.groupby(['Delivery.Unit.Name','Responsible.Organisation.Name','Option_1'])['v_PfPI.Minutes'].transform(np.max)
    unpivoted_data['min_minutes'] = unpivoted_data.groupby(['Delivery.Unit.Name','Responsible.Organisation.Name','Option_1'])['v_PfPI.Minutes'].transform(np.min)

    print("reshaping the data\n ")
    #this takes the data and stacks it with a single values column
    new_data = pd.melt(unpivoted_data,id_vars=['Financial.Year...Period','Delivery.Unit.Name','Responsible.Organisation.Name','Option_1','min_inc_count','max_inc_count','min_minutes','max_minutes'],value_vars=['v_Incident.Count','v_PfPI.Minutes'] )
    
    print("reshaping min and max values\n")
    #filter the data by variable column to get distinct list of min/max for counts/minutes
    new_data.loc[new_data.variable == 'v_Incident.Count', 'min_minutes'] = 0
    new_data.loc[new_data.variable == 'v_Incident.Count', 'max_minutes'] = 0

    new_data.loc[new_data.variable == 'v_PfPI.Minutes', 'min_inc_count'] = 0
    new_data.loc[new_data.variable == 'v_PfPI.Minutes', 'min_inc_count'] = 0
    
    #adding the columns together into a new combined column
    new_data['min_value'] = new_data['min_minutes'] + new_data['min_inc_count']
    new_data['max_value'] = new_data['max_minutes'] + new_data['max_inc_count']


    #where min and max match, increment max by 1
    new_data['max_value'] = np.where(new_data['min_value']==new_data['max_value'],
                                           new_data['max_value']+1,new_data['max_value'])
    #dropping the irrelvant columns
    del new_data['min_inc_count']
    del new_data['max_inc_count']
    del new_data['min_minutes']
    del new_data['max_minutes']

    return new_data


def addnewcolumns(dataset):
    """
    This renames the columns into their final name, adds the static columns and places them into the appropriate order
    """
    print("adding the new static columns\n")

    dataset.rename(columns={'Financial.Year...Period':'Date','variable':'Option_2','Responsible.Organisation.Name':'Option_3','value':'Value'}
                   ,inplace=True)
    dataset['Base_Location'] = dataset['Delivery.Unit.Name']
    dataset['TOC'] = 'x'
    dataset['Criticality'] = 'x'
    dataset['Location'] = dataset['Delivery.Unit.Name']
    dataset['Location_Type'] = 'MDU'
    dataset['Natural_Frequency'] = 'Period'
    dataset['Datatype'] = 'Delay_Minutes'
    dataset['Option_4'] = 'x'
    dataset['Option_5'] = 'x'

    dataset = dataset[['Base_Location', 'TOC','Criticality' ,'Location' ,'Location_Type','Natural_Frequency' ,'Datatype','Option_1','Option_2','Option_3',
                       'Option_4','Option_5','min_value','max_value','Date','Value']]
    
    #copy made of dataset to avoid the "setting on copy warning"
    final_data = dataset.copy()

    final_data['Option_2'].replace(['v_Incident.Count','v_PfPI.Minutes'],['Incident_Count','Delay_Minutes'],inplace=True)
    

    return final_data


if __name__ == '__main__':
    main()
