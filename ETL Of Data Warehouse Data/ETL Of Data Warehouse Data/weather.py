from urllib.request import Request,urlopen
import pandas as pd
from io import StringIO
import xlsxwriter
import calendar
import numpy as np
import datetime


def main():
    
    urls = get_list_of_urls()

    full_dataset = list()

    for url in urls:

        targeturl = url
        listofurl = targeturl.split('/')
        data = get_weather_data(targeturl,listofurl[9],listofurl[11].replace('.txt',''))
        full_dataset.append(data)

    all_data = pd.concat(full_dataset)

    all_data.to_csv('output/formatted_weather.csv', index=False)


def get_list_of_urls():
    urls = pd.read_excel('input/Weather data source (MetOffice).xlsx')
    list_of_urls = urls['DATA SOURCE']

    return list_of_urls


def get_weather_data(webaddress, weather_type, region):
    """
    This takes a web address from the met office and extracts a .txt file which is converted into a string,
    removing the first five introductory lines then convert the file into a pandas dataframe. It then drops
    the unnecessary seasonal aggregations.  The data is then stacked by year and month using melt.

    Parameters
    webaddress:     A string holding the url for the txt file
    weathertype:    A string holding the type of data held in dataset
    region:         A string holding the region under discussion

    
    """
    req = Request(webaddress,headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()
    txt_string = webpage.decode('utf-8')
        #remove unnecessary five lines of introduction
    postString = txt_string.split("\n",5)[5]

    parse_column = ['year']
    weather_data = pd.read_csv(StringIO(postString),delimiter=r'\s{1,5}',parse_dates=parse_column, engine='python',usecols=['year','jan','feb','mar','apr','may','jun','jul','aug','sep','oct',	'nov','dec'])
    
    print(weather_data)
    # add region and weather type information here
    weather_data['Location'] = region
    weather_data['Option_1'] = weather_type
    
    print("before min and max")
    print(weather_data)

    weather_data = pd.melt(weather_data, id_vars=['year','Location','Option_1'],value_vars=['jan','feb','mar','apr','may','jun','jul','aug','sep','oct',	'nov','dec'])
    
    print("this is melted data")
    print(weather_data)
    print(weather_data.info())
    
    #get min and max values
    weather_data['max'] = weather_data.max(axis=0)['value']
    weather_data['min'] = weather_data.min(axis=0)['value']

    #where min and max match, increment max by 1
    weather_data['max'] = np.where(weather_data['min']==weather_data['max'],
                                           weather_data['max']+1,weather_data['max'])


    print("weather_data")

    weather_data = handle_dates(weather_data)

    formatted_data = format_full_dataset(weather_data,weather_type,region)

    return formatted_data


def handle_dates(weather):
    weather['year'] = weather['year'].dt.year

    #filter out years before 1900 that can't be parsed
    weather = weather[weather['year']>=1900]

    #convert month to int 
    month_lookup = {'jan':int('01'),'feb':int('02'),'mar':int('03'),'apr':int('04'),'may':int('05'),'jun':int('06'),'jul':int('07')
                    ,'aug':int('08'),'sep':int('09'),'oct':int('10'),'nov':int('11'),'dec':int('12')}
    weather['month']= weather['variable'].apply(lambda x:month_lookup[x])
    
    #convert day to int
    day_lookup = {int('01'):int('31'), int('02'):int('28'),int('03'):int('31'),int('04'):int('30'),int('05'):int('31'),int('06'):int('30'),
                  int('07'):int('31'),int('08'):int('31'),int('09'):int('30'),int('10'):int('31'),int('11'):int('30'),int('12'):int('31')}

    weather['day'] = weather['month'].apply(lambda x:day_lookup[x])

    #handle leap year issue
    weather['leap_year'] = weather['year'].apply(lambda x: calendar.isleap(x))
    
    conditions = [(weather['variable'] == 'feb') & (weather['leap_year'] == True)  ]

    choices = [weather['day']+1]

    weather['day'] = np.select(conditions,choices,default = weather['day'])

    #concatinate and convert to date format
    weather['date'] = weather['year'].astype(str) +'/'+ weather['month'].astype(str) + '/'+ weather['day'].astype(str)
    #weather['date'] = datetime.datetime.strptime(weather['date'],'%Y%m%d')
    pd.to_datetime(weather['date'],format = '%Y%m%d',errors='coerce')

    #drop unnecessary columns
    del weather['leap_year']
    del weather['year']
    del weather['month']
    del weather['day']

    return weather


def format_full_dataset(raw_data,weather_type,region):
    

    #set weather_type correct
    if weather_type == 'Tmax':
        weather_type = 'Max Temperature'
    elif weather_type == 'Tmin':
        weather_type = 'Min Temperature'
    elif weather_type == 'Rainfall':
        weather_type = 'Rainfall'
    
    #assign new fields to datasets
    raw_data['Base_Location'] = region
    #raw_data['Location'] = region 
    raw_data['TOC'] = 'x'
    raw_data['Criticality'] = 'x'
    raw_data['Location_Type'] = 'Met Office Region'
    raw_data['Natural_Frequency'] = 'Monthly'
    raw_data['Data_Type'] = 'Weather'
    #raw_data['Option_1'] = weather_type
    raw_data['Option_2'] = 'x'
    raw_data['Option_3'] = 'x'
    raw_data['Option_4'] = 'x'
    raw_data['Option_5'] = 'x'
    raw_data['Date'] = raw_data['date']
    raw_data['min_value'] = raw_data['min']
    raw_data['max_value'] = raw_data['max']
    raw_data['Value'] = raw_data['value']


    columntitles =['Base_Location','TOC','Criticality','Location','Location_Type','Natural_Frequency','Data_Type','Option_1','Option_2','Option_3','Option_4','Option_5','min_value','max_value','Date','value']

    raw_data = raw_data.reindex(columns=columntitles)

    
    return raw_data

if __name__ == '__main__':
    main()
