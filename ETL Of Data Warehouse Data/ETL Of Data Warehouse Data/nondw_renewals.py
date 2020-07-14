import pandas as pd
from glob import glob
import os
import numpy as np
import datetime as dt
from ETL_Of_Data_Warehouse_Data import exportfile
from blob_export import export_to_blob

def main():
    """
    This processes a series of files within a defined folder structure and outputs a csv file which hold in the agreed format
    the renewals data for all years in 8 route format for all categories
    """
    sourcefilepath = 'Input\\NODW\\NODW_102_RENEWALS\\'
    temp_data = []
    
    #collect the paths and names of files to be loaded
    renewal_filepathsandnames = glob(f'{sourcefilepath}*.xlsx')
    numberoffiles = len(renewal_filepathsandnames)    

    #This holds the tabs we want from the sheet
    routes = ['Scotland','Western','Wales','Wessex','Sussex','Kent','WCMLS','North West','Central','North&Eastern','East Midlands','East Coast','Anglia']

    #This holds start and end point for different measure groups
    measure_group_ranges = {'Track':[5,66],'Signalling':[76,44],'Structures':[125,77],'Earthworks':[207,25],'Buildings':[237,55],
                            'Electrification_and_Fixed_Plant':[297,54],'Drainage':[356,54],'Telecoms':[415,25]}

    #loop through the files in the input folder and process them
    for count, file in enumerate(renewal_filepathsandnames,1):
        print(file)
    
        fy,fyn = derive_fy(file)
        
        print(f"Loading {os.path.basename(file)} into memory.")

        df = get_excel_data(file,routes,measure_group_ranges,fy,fyn)

        print(f"That's {count} out of {numberoffiles}, or {str(int((count/numberoffiles)*100))} percent loaded.\n")

        df_remapped = remap_routes(df)

        temp_data.append(df_remapped)

    #join the individual files together
    final_data = pd.concat(temp_data)
    
    #calculate the min and max values
    final_data_minmax = get_min_and_max(final_data)

    #reshape the data into the final format
    final_data = add_metadata(final_data_minmax)
    
    
    #export the final dataset
    exportfile(final_data,'output//NonDW_based_data//NONDW_102_RENEWALS//','NONDW_102_RENEWALS')
    
    #export the final dataset to azure
    export_to_blob('output//NonDW_based_data//NONDW_102_RENEWALS//','NONDW_102_RENEWALS.csv')



def add_metadata(df):
    """
    This reforms the dataframe into a required shape, by adding/dropping columns as well as converting min/max columns
    into rows.

    Parameter
    df:     A python dataframe hold data with mix max values

    Returns
    df:     A python dataframe with the appropriate format
    """

    #melt the data to get actual/budget on different rows
    id_vars = ['Measure','measure_group','route','Date','min','max']
    df = df.melt(id_vars = id_vars, value_vars=['Actual', 'Budget'])

    #add the required columns for the final format
    df['Base_Location'] = df['route']
    df['TOC'] = 'x'
    df['Criticality'] =  'x'
    df['Location'] = df['route']
    df['Location_Type'] = 'Route'
    df['Natural_Frequency'] = 'Annual'
    df['Data_Type'] = 'Renewal_Volumes'
    df['Option_1'] = df['measure_group']
    df['Option_2'] = df['Measure']
    df['Option_3'] = df['variable']
    df['Option_4'] = 'x'
    df['Option_5'] = 'x'
    df['min_value'] = df['min']
    df['max_value'] = df['max']
    df['value'] = df['value']

    #drop unnecessary columns
    df.drop(columns= ['Measure','measure_group','route'],inplace=True)

    #reshape the columns into the required order
    df = df [['Base_Location','TOC','Criticality','Location','Location_Type','Natural_Frequency','Data_Type','Option_1',
          'Option_2','Option_3','Option_4','Option_5','min_value','max_value','Date','value']]

    return df


def get_min_and_max(df):
    """
    This takes the joined dataframe and melts the 'actual' and 'budget' columns, so that min/max values can be
    calculated as though they were one item.  Chaining is used to group and aggregate and return a dataframe which is then
    merged with the original dataframe

    Parameters
    df:                 A dataframe holding data for all years data.

    Returns
    df_with_min_max:    A dataframe holding all years data with min/max values as two new columns
    """

    id_vars = ['Measure', 'measure_group', 'route']

    df1 = (df.melt(id_vars=id_vars, value_vars=['Actual', 'Budget'])
         .groupby(id_vars)['value']
         .agg(['min', 'max']))

    df_with_min_max = df.merge(df1, how='left', on=id_vars)

    #where min and max match, increment max by 1
    df_with_min_max['max'] = np.where(df_with_min_max['min']==df_with_min_max['max'],
                                           df_with_min_max['max']+1,df_with_min_max['max'])

    return df_with_min_max


def remap_routes(df):
    """
    This collapses 13 NR routes into 8 routes.  The method is to map new route names over old ones.  The resulting frame is 
    then grouped and summed to join the 13 routes into 8

    Parameter
    df:         A python dataframe holding a year's worth of data

    Returns
    summed_df:  A python dataframe holding 8 route equivalents, rather than 12
    """
    print("remapping routes")
    new_routes = {'Sussex':'Southeast','Kent':'Southeast','WCMLS':'LNW','North West':'LNW','Central':'LNW','North&Eastern':'LNEEM','East Midlands':'LNEEM','East Coast':'LNEEM'}

    df['route'] = df['route'].map(new_routes).fillna(df['route'])

    summed_df = df.groupby(['Measure','measure_group','route','Date'],as_index=False).agg({'Actual':'sum','Budget':'sum'})

    return summed_df


def derive_fy(filename):
    """
    This takes the year description of the file name, derive the previous year and then an end of financial year date which 
    is then later added to the final dataframe

    Parameter
    filename:       A string holding the filepath and filename of the file being processed

    Returns
    fy_key          An datetime value holding the end of the financial year
    fy_name         A string holding the name of the financial year in the format "YYYY-YYYY"
    """
    cfy =  int('20' + filename[31:33])
    pfy =  str(cfy-1)
    fy_name = str(pfy) + '-'  + str(cfy)

    fy_key = dt.date(cfy,3,31)

    return fy_key, fy_name
    

def get_excel_data(source,routes,measure_ranges,fin_year,fy_name):
    """
    This reads in data from an xlsx file through a series of tabs and ranges defined by lists and dictionaries.
    It depends on data being held in each spreadsheet in the same way.  The each measure group is joined into one dataframe
    representing the whole route.  Fields for the Date, Measure_Group and Routes are added to the final joined set

    Parameters
    source:         A string holding the filepath and file name of the xlsx file to be process
    routes:         A list holding the names of tabs which are to be processed
    measure_ranges: A dictionary holding the names of measure_groups and ranges of each group.  The format is
                    measuregroup:[row to start loading data from, number of rows to take in]
    fin_year:       An int holding the name of the financial year

    Returns
    joined_data:    A dataframe holding the raw data for a given route
    """
    route_list =[]
    range_list =[]
    
    for route in routes:
        
        for measure_group, ranges in measure_ranges.items():    
            print(f'getting {measure_group} for {route} in {fy_name}')

            temp_df = pd.read_excel(
                source,route,
                skiprows=ranges[0],
                nrows=ranges[1],
                usecols='C:E'
            )

            temp_df.rename(columns={'Unnamed: 2':'Measure'},inplace=True)
            temp_df['measure_group'] = measure_group
            temp_df['route'] = route

            range_list.append(temp_df)
    
        all_ranges = pd.concat(range_list)
    
    route_list.append(all_ranges)
    
    joined_data = pd.concat(route_list)

    joined_data['Date'] = fin_year

    return joined_data

if __name__ == '__main__':
    main()
