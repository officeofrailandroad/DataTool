import pandas as pd
import os
from pandas import DataFrame
import pyodbc
import sqlalchemy 
from sqlalchemy import create_engine, MetaData, Table, select, inspect
from sqlalchemy.orm import sessionmaker
from glob import glob
import datetime
import numpy as np
from blob_export import export_to_blob


def main():
      
    start_period =datetime.datetime(2006,1,1)
    
    #dictionary holding the output file path and file name
    dwtables = {
        'factv_106_TSR_DT':['output\\DW_based_data\\DW106_TSR\\','DW_106_TSR'],
                'factv_326_FDM_Final_DT':['output\\DW_based_data\\DW326_FDM\\','DW_326_FDM'],
                'factv_327_CP6ReliabilityandSustainability_DT':['output\\DW_based_data\\DW327_RELIABILITY\\','DW_327_RELIABILITY'],
                'factv_318_PPM_Periodic_DT':['output\\DW_based_data\\DW318_PPMCASL\\','DW_318_PPMCASL']}

    #temp_dw_list = list()
    
    for table in dwtables.keys():
        df = getDWdata('NR',table,start_period)

        print(f"transforming data from {table}")
        transformed_dw_data = transform_dw_data(df)
        
        ##temp_dw_list.append(transformed_dw_data)
    
    ##final_data = pd.concat(temp_dw_list)

        exportfile(transformed_dw_data,dwtables[table][0],dwtables[table][1])    

        export_to_blob(dwtables[table][0],dwtables[table][1]+'.csv')


def transform_104(csv_data,start_period):
    """
    This is a function to take in 104_Delay minutes as a custom file and confirm it to a standard format.
    """
    
    #convert fp into right shape
    csv_data['financial_period_key']= csv_data['Financial Year & Period'].str.slice(0,4) + str(20) + csv_data['Financial Year & Period'].str.slice(5,7) + csv_data['Financial Year & Period'].str.slice(9,11)
    csv_data.drop(columns=['Financial Year & Period'],inplace=True)
    
    #add option columns
    csv_data['Location'] = csv_data['Route Name']
    csv_data['Location_Type'] = 'Route-based'
    csv_data['Data_Type'] = 'Performance'
    csv_data['Option_1'] = 'Daily Minutes_type2'
    csv_data['Option_2'] = csv_data['Area']
    csv_data['Option_3'] = csv_data['Delivery Unit Name']
    csv_data['Option_4'] = csv_data['Incident Category Description']
    csv_data['Option_5'] = csv_data['Incident Reason Description']
    csv_data['Option_6'] = csv_data['Responsible Organisation Name']

    #drop unncessary columns converted into options
    csv_data.drop(columns=['Route Name','Route','Area','Delivery Unit Name','Incident Category Description','Incident Reason Description','Responsible Organisation Name'], inplace=True)

    #drop unnecessary columns in general
    csv_data.drop(columns=['Area Name','Delivery Unit','Incident Summary Group','Incident Category','Incident Reason','Responsible Organisation','Responsible Manager','Responsible Function Level 3 Desc','Responsible Function Level 3 Name','Responsible Manager Name'],inplace=True)

    #drop rows where financial_period is null
    csv_data.dropna(subset=['financial_period_key'],inplace=True)
    
    #convert fp to int64 to enable a merge
    csv_data['financial_period_key'] = pd.to_numeric(csv_data['financial_period_key'])
    
    #get the date dimension information
    datedimt = getDWdata('dbo','dimt_financial_period',start_period)
    
    #join the data to date metadata
    print("merging CSV with Date data")
    csv_with_dates = csv_data.merge(datedimt,left_on='financial_period_key',right_on='financial_period_key',suffixes=('csv_','date_'))
    
    #drop unnecessary columns from merge
    csv_with_dates.drop(columns=['financial_period_id','financial_period_name','financial_period_ordinal','financial_year_key','create_date','amend_date','previous_financial_period_key','prior_year_financial_period_key'], inplace=True)
    
    #repeat rows by number of days
    repeated_csv_data = csv_with_dates.loc[csv_with_dates.index.repeat(csv_with_dates.financial_period_day_count)]

    #add daily dates to frame
    repeated_csv_data['start_date']=pd.to_datetime(repeated_csv_data['financial_period_start_date'], dayfirst=True)
    td = pd.to_timedelta(repeated_csv_data.groupby(level=0).cumcount(), unit='d')
    repeated_csv_data['daily_date'] = repeated_csv_data['start_date'] + td

    #drop unnnecessary columns
    repeated_csv_data.drop(columns=['start_date'],inplace=True)

    #rename columns
    repeated_csv_data.rename(columns={'financial_period_start_date':'start_date','financial_period_end_date':'end_date'},inplace=True)

    #move measures to end of dataframe
    repeated_csv_data = repeated_csv_data[[col for col in repeated_csv_data if col not in ['v_Incident Count','v_PfPI Minutes']] + ['v_Incident Count','v_PfPI Minutes']] 

    repeated_csv_data = pd.melt(repeated_csv_data,
                                id_vars=['financial_period_key','Location','Location_Type','Data_Type','Option_1','Option_2','Option_3','Option_4','Option_5','Option_6'
                                         ,'start_date',	'end_date',	'financial_period_day_count','daily_date'],
                                value_vars=['v_Incident Count','v_PfPI Minutes'],
                                var_name='Measure',
                                value_name='value')

    #pivot the data
    pivoted_data = repeated_csv_data.pivot_table(index=['Location','Location_Type','Data_Type','Option_1','Option_2','Option_3','Option_4','Option_5','Option_6','Measure'],
                                                columns=['financial_period_key','start_date','end_date','daily_date'],
                                                values=['value'],
                                                aggfunc=np.sum).reset_index(['Location','Location_Type','Data_Type','Option_1','Option_2','Option_3','Option_4','Option_5','Option_6','Measure'])

    #this needs the reset index function at the end of the pivot to ensure that the pivot returns these fields, but it ends up 
    #creating a format different from DW that doesn't play nicely with moving columns around!
    
    
    #create a min and max column
    pivoted_data['min_value'] = pivoted_data.min(axis=1)
    pivoted_data['max_value'] = pivoted_data.max(axis=1)
    
    for col in pivoted_data.columns:
        print(col)

    #move min and max columns
    #pivoted_data= movecol(pivoted_data,['min_value','max_value'],'Location','After')

    return pivoted_data

    



def transform_dw_data(dw_data):
    """
    This function takes a dataframe and convert date to timeseries, repeats rows by number of days and then generates a list of daily dates which
    is added to the dataframe.  The dataframe is then pivoted by the daily date, with min and max values for each row added.

    Parameters
    dw_data:        A pandas dataframe holding data extracted from data warehouse

    returns:        A pandas dataframe holding the transformed data
    """

    # repeat rows by the number of days
    #repeated_dw_data = dw_data.loc[dw_data.index.repeat(dw_data.number_of_days)]

    #add daily dates to frame
    #repeated_dw_data['start_date']=pd.to_datetime(repeated_dw_data['start_date'], dayfirst=True)

    #td = pd.to_timedelta(repeated_dw_data.groupby(level=0).cumcount(), unit='d')
    #repeated_dw_data['daily_date'] = repeated_dw_data['start_date'] + td

    #next step is to pivot the data by the daily date column.
    pivoted_data = pd.pivot_table(dw_data,
                                  index=['Base_Location','TOC','Criticality','Location','Location_Type','Natural_Frequency','Data_Type','Option_1','Option_2','Option_3','Option_4','Option_5'],
                                                 columns=['Date'],
                                                 values='Value',
                                                 aggfunc=np.sum)

    #create a min and max column
    pivoted_data['min_value'] = pivoted_data.min(axis=1)
    pivoted_data['max_value'] = pivoted_data.max(axis=1)

    #move min and max columns
    #colstomove = ['max_value','min_value']

    #for cols in colstomove:
    #    pivoted_data= movecolumnstofront(pivoted_data,cols)

    
    data_unpivoted = pivoted_data.reset_index().melt(id_vars=['Base_Location','TOC','Criticality','Location','Location_Type','Natural_Frequency','Data_Type','Option_1','Option_2','Option_3','Option_4','Option_5','min_value','max_value'],var_name= "Date",value_name='value')

    data_unpivoted.dropna(inplace=True)
    
    #where min and max match, increment max by 1
    data_unpivoted['max_value'] = np.where(data_unpivoted['min_value']==data_unpivoted['max_value'],
                                           data_unpivoted['max_value']+1,data_unpivoted['max_value'])
    

    return data_unpivoted


def movecolumnstofront(df,fieldname):
    """
    A helper function for the transform_data function.  It moves a moves a given column to the first index of the dataframe

    Parameters
    df:         A dataframe holding the dataset from transform_data
    fieldname:  A string holding the name of the field to be moved

    Output
    df:         A transformed dataframe
    """
    field = df[fieldname]
    df.drop(labels=[fieldname],axis=1,level=0, inplace=True)
    df.insert(0,fieldname,field)

    return df


def getDWdata(schema_name,table_name,fp_key):
    """
    This uses SQL Alchemy to connect to SQL Server via a trusted connection and extract a filtered table, which is then coverted into a dataframe.
    This is intended for getting the partial table for fact data.

    Parameters
    schema_name:    A string represetnting the schema of the table
    table_name:     A string representing the name of the table
    fp_key:         An integer representing the lower limit of the financial period

    returns:        
    df:             A dataframe containing the table   
    """
    print(f"getting DW data from {table_name}\n\n")

    engine = sqlalchemy.create_engine('mssql+pyodbc://AZORRDWSC01/ORR_DW?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes')
    
    conn = engine.connect()

    metadata = MetaData()

    example_table = Table(table_name, metadata,autoload=True, autoload_with=engine, schema=schema_name)

    #get raw table data, filtered by source_item_id
    query = select([example_table]).where(example_table.c.Date >= fp_key)
    


    df = pd.read_sql(query, conn)
    
    return df


def exportfile(df,destinationpath,filename,numberoffiles=1):
    """
    This procedure exports the finalised file as a CSV file with a datetime stamp in filename

    Parameters:
    df        - a dataframe containing the finalised data
    destinationpath     - a string providing the filepath for the csv file
    numberoffiles       - an int with the number of files being processed
    
    Returns:
    None, but does export dataframe df as a csv object
    """
     
    #formatted_date = datetime.datetime.now().strftime('%Y%m%d_%H-%M')
    #destinationfilename = f'{filename}_{formatted_date}.csv'
    destinationfilename = filename +'.csv'
    print(f"Exporting {filename} to {destinationpath}{destinationfilename}\n")
    checkmessage = "If you want to check on progress, refresh the folder "+ destinationpath + " and check the size of the " + filename + ".csv file. \n"  

    if filename == 'superfile':
        if numberoffiles < 9:
            print("This is less than 9 files so should be ok")
            print(checkmessage)
    
        elif numberoffiles > 10 and numberoffiles < 29:
            print("This may take a few minutes.  Why not go and have a nice cup of tea?\n")
            print(checkmessage)

        elif numberoffiles > 30:
            print("This may possibly hang the PC due to memory issues.  If it hangs, turn off IE, Outlook and any other memory/resource hungry applications and try again.\n")
            print(checkmessage)

        else:
            pass
    else:
        print(f"the {filename} file should be quick.")
    
    #delete all previous files in folder
    files_to_delete = glob(destinationpath + '*')
    for f in files_to_delete:
        os.remove(f)
    
    #export the source_data
    df.to_csv(destinationpath + destinationfilename,index=False)


def getcsvdata(csvinput):
    """
    This procedure reads in a series of csv files using the glob method, the data (as a dataframe) and file names are passed into a dictionary

    Parameter:
    originfilepath     - a string containing the filepath where the files are stored

    Returns:
    namesanddata       - a dictionary containing the file name (key) and a dataframe (data) for each individual csv dataset
    numberoffiles      - an int with the number file files held on the dataframe list 
    """
    filepathsandnames = glob(f'{csvinput}*.csv')
    numberoffiles = len(filepathsandnames)
    
    print(f"{numberoffiles} files need to be processed. \n")
    # printout names of the files to be loaded
    print(f"reading in CSV files from {csvinput}\n\n")

    dataframes = []
    filenames = []
    #dtypedictionary = {'Route Code':str}
    for count, file in enumerate(filepathsandnames,1):
        print(f"Loading {os.path.basename(file)} into memory.")
        print(f"That's {count} out of {numberoffiles}, or {str(int((count/numberoffiles)*100))} percent loaded.\n")
        temp = pd.read_csv(file, encoding = 'cp1252')

        filenames.append(os.path.basename(file))
        dataframes.append(temp)

        namesanddata = dict(zip(filenames,dataframes))


    
    return namesanddata, numberoffiles


def combinecsvfiles(toc_list,file_count):
    """
    This procedure take a list of dataframe and combines them into a single dataframe


    Parameters:
    toc_list        - a list of dataframes containing indivdual toc data 
    file_count      - an int with the total number of dataframes in the list

    Returns:
    tocs            - a single dataframe
    """

    print(f"appending {file_count} files into single datafile.  Please wait\n\n")

    data_df = pd.concat(toc_list, axis=0,ignore_index=True,verify_integrity=True, sort=False)
    #data_df.reset_index(drop=True, inplace=True)
    #data_df.index.name='id_code'

    return data_df


def movecol(df, cols_to_move=[], ref_col='', place='After'):
    """
    Helper function used to move columns around.  Lifted from https://towardsdatascience.com/reordering-pandas-dataframe-columns-thumbs-down-on-standard-solutions-1ff0bc2941d5

    Parameters
    df:             a pandas dataframe to manipulate
    cols_to_move:   a list holding column names to be moved
    ref_col:        a string holding the name of the column to be used as reference point
    place:          After/Before.  A flag in indicate direction of movement

    Returns
    df:             a reorderded dataframe
    """
    cols = df.columns.tolist()
    if place == 'After':
        seg1 = cols[:list(cols).index(ref_col) + 1]
        seg2 = cols_to_move
    if place == 'Before':
        seg1 = cols[:list(cols).index(ref_col)]
        seg2 = cols_to_move + [ref_col]
    
    seg1 = [i for i in seg1 if i not in seg2]
    seg3 = [i for i in cols if i not in seg1 + seg2]
    
    return(df[seg1 + seg2 + seg3])


if __name__  == '__main__':
    main()