import pandas as pd


def main():
    filename = 'FY20 Renewals Volume Tables.xlsx'
    sourcefilepath = 'input/renewal_vols/'+filename

    fy = derive_fy(filename)
    
    #This holds the tabs we want from the sheet
    routes = ['Scotland','Western','Wales','Wessex','Sussex','Kent','WCMLS','North West','Central','North&Eastern','East Midlands','East Coast','Anglia']

    #This holds start and end point for different measure groups
    measure_group_ranges = {'track':[5,66],'signalling':[76,44],'structures':[125,77],'earthworks':[207,25],'buildings':[237,55],
                            'electrification_and_fixed_plant':[297,54],'drainage':[356,54],'telecoms':[415,25]}

    df = get_excel_data(sourcefilepath,routes,measure_group_ranges,fy)

    df_remapped = remap_routes(df)

    df_remapped.to_csv('output/renewals.csv',index=False)

    print(df_remapped)


def remap_routes(df):
    print("remapping routes")
    new_routes = {'Sussex':'Southeast','Kent':'Southeast','WCMLS':'LNW','North West':'LNW','Central':'LNW','North&Eastern':'LNEEM','East Midlands':'LNEEM','East Coast':'LNEEM'}

    df['route'] = df['route'].map(new_routes).fillna(df['route'])

    summed_df = df.groupby(['Measure','measure_group','route','Date'],as_index=False).agg({'Actual':'sum','Budget':'sum'})

    return summed_df


def derive_fy(filename):
    cfy = int(filename[2:4])
    pfy = cfy-1

    fy_key = int('20' + str(pfy) + '20' + str(cfy))

    return fy_key
    

def get_excel_data(source,routes,measure_ranges,fin_year):

    route_list =[]
    range_list =[]
    
    for route in routes:
        
        for measure_group, ranges in measure_ranges.items():    
            print(f'getting {measure_group} for {route}')

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
    
    #print(temp_list)
    joined_data = pd.concat(route_list)

    joined_data['Date'] = fin_year

    return joined_data


if __name__ == '__main__':
    main()
