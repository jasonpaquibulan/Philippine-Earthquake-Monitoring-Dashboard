from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
from dateutil import parser
from dateutil.parser import parse
import numpy as np   
# import psycopg2
# from sqlalchemy import create_engine

#extracting data from 2018 to 2022
#extraction of data for 2023 will be separated since its url structure is different from 2018 to 2022

months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
years = ["2018", "2019", "2020", "2021", "2022"]


for year in years:
    for month in months:
        data = []
        url = f"https://earthquake.phivolcs.dost.gov.ph/EQLatest-Monthly/{year}/{year}_{month}.html"

        page_request = requests.get(url, verify = False)

        print(page_request)

        soup = BeautifulSoup(page_request.content, 'html.parser' )

        trs = soup.find_all('tr')
        
        for tr in trs:
            values = [td.text for td in tr.find_all('td')]
            data.append(values)

        print(f"Done looping for {month}-{year}")

        # Create a Pandas DataFrame from the data list
        df = pd.DataFrame(data)

        # Save the DataFrame to a CSV file
        df.to_csv(f'Phivolcs_{year}_{month}.csv', index=False)

        print(f"CSV created for {month}-{year}")


# #Extracting Data for January 2023

# url = "https://earthquake.phivolcs.dost.gov.ph/EQLatest-Monthly/2023/2023_January.html"

# page_request = requests.get(url, verify = False)

# data1 = []
# soup = BeautifulSoup(page_request.content, 'html.parser' )

# trs = soup.find_all('tr')

# for tr in trs:
#     values = [td.text for td in tr.find_all('td')]
#     data1.append(values)

# # Create a Pandas DataFrame from the data list
# df = pd.DataFrame(data1)

# # Save the DataFrame to a CSV file
# df.to_csv(f'Phivolcs_2023_January.csv', index=False)

# #Extracting Data for February 2023

# url = "https://earthquake.phivolcs.dost.gov.ph/2023_February.html"

# page_request = requests.get(url, verify = False)

# data2 = []
# soup = BeautifulSoup(page_request.content, 'html.parser' )

# trs = soup.find_all('tr')

# for tr in trs:
#     values = [td.text for td in tr.find_all('td')]
#     data2.append(values)

# # Create a Pandas DataFrame from the data list
# df = pd.DataFrame(data2)

# # Save the DataFrame to a CSV file
# df.to_csv(f'Phivolcs_2023_February.csv', index=False)

# #Extracting Data for March 2023

# url = "https://earthquake.phivolcs.dost.gov.ph/2023_March.html"

# page_request = requests.get(url, verify = False)

# data3 = []
# soup = BeautifulSoup(page_request.content, 'html.parser' )

# trs = soup.find_all('tr')

# for tr in trs:
#     values = [td.text for td in tr.find_all('td')]
#     data3.append(values)

# # Create a Pandas DataFrame from the data list
# df = pd.DataFrame(data3)

# # Save the DataFrame to a CSV file
# df.to_csv(f'Phivolcs_2023_March.csv', index=False)

# #Extracting Data for April 2023

# url = "https://earthquake.phivolcs.dost.gov.ph"

# page_request = requests.get(url, verify = False)

# data4 = []
# soup = BeautifulSoup(page_request.content, 'html.parser' )

# trs = soup.find_all('tr')

# for tr in trs:
#     values = [td.text for td in tr.find_all('td')]
#     data4.append(values)

# # Create a Pandas DataFrame from the data list
# df = pd.DataFrame(data4)

# # Save the DataFrame to a CSV file
# df.to_csv(f'Phivolcs_2023_April.csv', index=False)

months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
years = ["2018", "2019", "2020", "2021", "2022"]

data_cleaned = []

for year in years:
    for month in months:
        df = pd.read_csv(rf"C:\Users\jason.paquibulan\Desktop\Git&Github\Philippine-Earthquake-Monitoring-Dashboard\Phivolcs_{year}_{month}.csv")

        #Dropping Excess Column
        column_list = df.shape[1]
        df = df.iloc[:,0:6]

        df = df.rename(columns={'0': 'date_time'})
        df = df.rename(columns={'1': 'latitude_n'})
        df = df.rename(columns={'2': 'longitude_e'})
        df = df.rename(columns={'3': 'depth_km'})
        df = df.rename(columns={'4': 'magnitude'})
        df = df.rename(columns={'5': 'location'})

        #Removing Special Characters
        def remove_special_chars(text):
            """
            Removes special characters (\r, \n, \t) from a given string if they exist.
            """
            if isinstance(text, str):
                if "\r" in text:
                    text = text.replace("\r", "")
                if "\n" in text:
                    text = text.replace("\n", "")
                if "\t" in text:
                    text = text.replace("\t", "")
                if "ï" in text:
                    text = text.replace("ï", "")
                if "»" in text:
                    text = text.replace("»", "")
                if "¿" in text:
                    text = text.replace("¿", "")
            return text
        
        column_names = df.columns.tolist()

        for column_name in column_names:
            df[column_name] = df[column_name].apply(remove_special_chars)

        #Dropping rows with irrelevant data
        df.drop(df[df['latitude_n'].isna()].index, inplace=True)
        df.drop(df[df['date_time'].str.contains('Jan(?!\w)')].index, inplace=True)
        df.drop(df[df['date_time'].str.contains('Date')].index, inplace=True)
        df.drop(df[df['date_time'].str.contains('APRIL')].index, inplace=True)
        df.drop(df[df['date_time'].str.contains('FEBRUARY')].index, inplace=True)
        df.drop(df[df['date_time'].str.contains('MARCH')].index, inplace=True)
        df.drop(df[df['date_time'].str.contains('MAY')].index, inplace=True)
        
        #Making Additional Column
        df['date_only'] = df['date_time'].str.split('-').str[0].str.strip()
        df['time_only'] = df['date_time'].str.split('-').str[1].str.strip()

        #Dropping Null

        df.drop(df[df['time_only'].isna()].index, inplace=True)

        #Dropping Duplicates
        df = df.drop_duplicates()

        data_cleaned.append(df)

        print(f"Done {year}_{month}")
        print(df.shape)

months_2023 = ["January", "February", "March", "April"]
for month_2023 in months_2023:
        df = pd.read_csv(rf"C:\Users\jason.paquibulan\Desktop\Git&Github\Philippine-Earthquake-Monitoring-Dashboard\Phivolcs_2023_{month_2023}.csv")

        #Dropping Excess Column
        column_list = df.shape[1]
        df = df.iloc[:,0:6]

        df = df.rename(columns={'0': 'date_time'})
        df = df.rename(columns={'1': 'latitude_n'})
        df = df.rename(columns={'2': 'longitude_e'})
        df = df.rename(columns={'3': 'depth_km'})
        df = df.rename(columns={'4': 'magnitude'})
        df = df.rename(columns={'5': 'location'})

        #Removing Special Characters
        def remove_special_chars(text):
            """
            Removes special characters (\r, \n, \t) from a given string if they exist.
            """
            if isinstance(text, str):
                if "\r" in text:
                    text = text.replace("\r", "")
                if "\n" in text:
                    text = text.replace("\n", "")
                if "\t" in text:
                    text = text.replace("\t", "")

            return text
        
        column_names = df.columns.tolist()

        for column_name in column_names:
            df[column_name] = df[column_name].apply(remove_special_chars)

        #Dropping rows with irrelevant data
        df.drop(df[df['latitude_n'].isna()].index, inplace=True)
        df.drop(df[df['date_time'].str.contains('Jan(?!\w)')].index, inplace=True)
        df.drop(df[df['date_time'].str.contains('Date')].index, inplace=True)
        df.drop(df[df['date_time'].str.contains('APRIL')].index, inplace=True)
        df.drop(df[df['date_time'].str.contains('FEBRUARY')].index, inplace=True)
        df.drop(df[df['date_time'].str.contains('MARCH')].index, inplace=True)
        df.drop(df[df['date_time'].str.contains('MAY')].index, inplace=True)

        df['date_only'] = df['date_time'].str.split('-').str[0].str.strip()
        df['time_only'] = df['date_time'].str.split('-').str[1].str.strip() 

        #Dropping Null

        df.drop(df[df['time_only'].isna()].index, inplace=True)

        #Dropping Duplicates
        df = df.drop_duplicates()

        data_cleaned.append(df)

        print(f"Done 2023_{month}")
        print(df.shape)

merged_data_cleaned = pd.concat(data_cleaned)

df = pd.DataFrame(merged_data_cleaned, columns = ["date_time","latitude_n","longitude_e","depth_km","magnitude", "location","date_only", "time_only"])
#merged_data_cleaned_df = pd.DataFrame(merged_data_cleaned)

dates = list(df['date_only'])
date_formats = ["%d%B %Y", "%d %B %Y"]
date_not_parsed = []

for date_str in dates:
    parsed = False
    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            parsed = True
            break  # stop trying other formats once a match is found
        except ValueError:
            pass  # continue to next format if parsing fails
    if not parsed:
        print(f"Could not parse date string: {date_str}")
        date_not_parsed.append(date_str)

for date_remain in date_not_parsed:
    date_remain = date_remain.replace('\ufeff', '')  # replace BOM with empty string
    try:
        date_obj = parser.parse(date_remain)
        print(f"Successfully parse date remain {date_remain}")
    except ParserError:
        print(f"Could not parse date string: {date_remain}")

df['date_only'] = pd.to_datetime(df['date_only'], errors='coerce')
df['date_only'].dtype

dates = list(df['time_only'])
date_formats = ["%I:%M %p"]
date_not_parsed = []

for date_str in dates:
    parsed = False
    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            parsed = True
            break  # stop trying other formats once a match is found
        except ValueError:
            pass  # continue to next format if parsing fails
    if not parsed:
        print(f"Could not parse date string: {date_str}")
        date_not_parsed.append(date_str)

for date_remain in date_not_parsed:
    date_remain = date_remain.replace('\ufeff', '')  # replace BOM with empty string
    try:
        date_obj = parser.parse(date_remain)
        print(f"Successfully parse date remain {date_remain}")
    except ParserError:
        print(f"Could not parse date string: {date_remain}")

# # extract date and time components
df['time_only'] = pd.to_datetime(df['time_only'], errors='coerce')
df['time_only'].dtype

print("successful parsed date_only")

#replacing values
df['latitude_n'] = df['latitude_n'].str.strip().replace("-", "")

#replacing empty str with mean of latitude

df['latitude_n'] = df['latitude_n'].replace("", np.nan)
df['latitude_n'] = df['latitude_n'].astype(float)

print(df.dtypes)

df['longitude_e'] = df['longitude_e'].replace(" - ","").replace("-","")
df['longitude_e'] = df['longitude_e'].replace("",np.nan)
df['longitude_e'] = df['longitude_e'].astype(float)

df['depth_km'] = df['depth_km'].replace("nan", "").replace("-","").replace("\xa0","").replace(" - ","").replace("  ","").replace("", np.nan)

df['depth_km'] = df['depth_km'].replace("1.9  32 km from Ibajay (Aklan)","").replace("", np.nan)

df['depth_km'] = df['depth_km'].astype(float)

print(df.dtypes)

df['depth_km'] = df['depth_km'].fillna(df['depth_km'].mean())
df = df.dropna()

df['magnitude'] = df['magnitude'].astype(float)

#Connecting to PostgreSQL and loading the scraped data

# conn = psycopg2.connect(
#     host="localhost",
#     database="postgres",
#     user="postgres",
#     password="8888"
# )

# engine = create_engine('postgresql+psycopg2://postgres:8888@localhost:5432/postgres')

df.to_csv('phivolcs.csv', index=True)

# df.to_sql('temp_table_name', engine, if_exists='replace')

# # Open a cursor to perform database operations
# cur = conn.cursor()

# cur.execute("""
#     DROP TABLE phivolcs2_dataset;
# """)

# # Create the  table

# cur.execute("""
#     CREATE TABLE IF NOT EXISTS phivolcs2_dataset (
#         "date_time" VARCHAR ,
#         "latitude_n" FLOAT ,
#         "longitude_e" FLOAT ,
#         "depth_km" FLOAT ,
#         "magnitude" FLOAT ,
#         "location" VARCHAR, 
#         "date_only" DATE ,
#         "time_only" TIME 
#     );
# """)

# cur.execute("""
#     INSERT INTO phivolcs2_dataset ("date_time","latitude_n","longitude_e","depth_km","magnitude", "location","date_only", "time_only")
#     SELECT "date_time","latitude_n","longitude_e","depth_km","magnitude", "location","date_only", "time_only"
#     FROM temp_table_name """)

# cur.close()

# conn.commit()
# conn.close()

# print("ETL SUCCESSFULL")
