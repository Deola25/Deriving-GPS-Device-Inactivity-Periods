# -*- coding: utf-8 -*-
"""
Created on Fri Dec 21 15:02:40 2018

@author: Adeola
"""

'''
This project examines the about 6000 GPS devices on vendor vehicles to determine those which are deemed inactive because they haven't
reported any recorded events in over 48 hours. To achieve this the first and last pings of each day are noted and subtracted to
determine periods when no events where recorded.
'''
import pandas as pd
import numpy as np
import datetime

#Read in the GPS data
data= pd.read_csv(r'C:\Users\Yemmy\Downloads\GPS_Report_Sept_2018_Sample.csv')

#Data prep and analysis

#Convert the date columns to datetime format
data['ActivityDateTimeEST'] = pd.to_datetime(data['ActivityDateTimeEST'])
#Data prep and analysis

#Convert the date columns to datetime format
data['ActivityDateTimeEST'] = pd.to_datetime(data['ActivityDateTimeEST'])

#create a new date column which would be used to monitor first and last pings of the day and vehicle ID that wont be an index
data['ActivityDateTimeEST_add']=data['ActivityDateTimeEST']
data['VehicleID_add']=data['VehicleID']

#Create an indexed dataframes of the last and first events of each day events 
indexed_data_dailyfirst= data.groupby(['VehicleID', pd.Grouper(key='ActivityDateTimeEST', freq='D')])['ActivityDateTimeEST_add',
                                    'VehicleID_add','EventSubTypeID','Longitude', 'Latitude','Registration', 'UniqueVehicleNumber',
                                    'vendor'].min()
indexed_data_dailylast= data.groupby(['VehicleID', pd.Grouper(key='ActivityDateTimeEST', freq='D')])['ActivityDateTimeEST_add', 
                                    'VehicleID_add','EventSubTypeID','Longitude', 'Latitude','Registration', 'UniqueVehicleNumber',
                                    'vendor'].max()

#Rename the time columns for the daily first and last events
indexed_data_dailyfirst.rename(columns={'ActivityDateTimeEST_add':'ActivityDateTimeEST_First','VehicleID_add':'VehicleID_addF' ,
                                        'EventSubTypeID':'EventSubTypeID_First','Registration':'Registration_F', 
                                        'UniqueVehicleNumber':'UniqueVehicleNumber_F' ,'Longitude':'Longitude_First','Latitude':'Latitude_First',
                                        'vendor':'Vendor_First','VehicleID':'VehicleID_F'},inplace= True)
indexed_data_dailylast.rename(columns={'ActivityDateTimeEST_add':'ActivityDateTimeEST_Last','VehicleID_add':'VehicleID_addL',
                                       'EventSubTypeID':'EventSubTypeID_Last','Registration':'Registration_L', 
                                       'UniqueVehicleNumber':'UniqueVehicleNumber_L','Longitude':'Longitude_Last','Latitude':'Latitude_Last',
                                       'vendor':'Vendor_Last','VehicleID':'VehicleID_L'},inplace= True)

#Merge the first and last daily events dataframes
Merged_daily_data = pd.concat([indexed_data_dailylast, indexed_data_dailyfirst], axis=1, join_axes=[indexed_data_dailylast.index])

#To change the datetime from '<M8[ns]' and create bthe interval column which is in hours
Merged_daily_data['ActivityDateTimeEST_First']= pd.to_datetime(Merged_daily_data['ActivityDateTimeEST_First'])
Merged_daily_data['ActivityDateTimeEST_Last']= pd.to_datetime(Merged_daily_data['ActivityDateTimeEST_Last'])
Merged_daily_data['Daily_check'] = (Merged_daily_data.groupby(level=0,sort=False).apply(lambda x: x.ActivityDateTimeEST_First-x.ActivityDateTimeEST_Last.shift(1)) / pd.Timedelta('1 hour')).values

#Set the time variable for the end of the month 
Month_end=datetime.datetime(2018,9,30,23,59,59)   #Has to be set manually

#Get the the last event logged by each device in the month
Last_event= data.groupby(['VehicleID', pd.Grouper(key='ActivityDateTimeEST', freq='M')])['ActivityDateTimeEST_add',
                        'VehicleID_add', 'EventSubTypeID','Longitude', 'Latitude','Registration', 
                        'UniqueVehicleNumber','vendor'].max()
Last_event.rename(columns={'Vehicle_add':'Vehicle_add_Last'})
Last_event['Monthly_check'] = (Last_event.groupby(level=0,sort=False).apply(lambda x: Month_end - x.ActivityDateTimeEST_add) / pd.Timedelta('1 hour')).values
Last_event= Last_event.reset_index()
Last_event_only = Last_event.drop_duplicates(subset='VehicleID', keep='last')

#Assign Binary column to determine if its a month end checker
Merged_daily_data['Monthlystuffs']=0
Last_event_only['Monthlystuffs']=1

#Get the daily an monthly periods of inactivity
daily_inactivity= Merged_daily_data.loc[Merged_daily_data['Daily_check'] >= 48]
monthly_check= Last_event_only.loc[Last_event_only['Monthly_check'] >= 48]

#Create columns with same schema as the last activity recorded befor period of inactivity
monthly_check['ActivityDateTimeEST_Last'] = monthly_check['ActivityDateTimeEST_add']
monthly_check['VehicleID_addL'] = monthly_check['VehicleID_add']
monthly_check['EventSubTypeID_Last'] = monthly_check['EventSubTypeID']
monthly_check['Longitude_Last'] = monthly_check['Longitude'] 
monthly_check['Latitude_Last'] = monthly_check['Latitude']
monthly_check['Registration_L'] = monthly_check['Registration']
monthly_check['UniqueVehicleNumber_L'] = monthly_check['UniqueVehicleNumber']
monthly_check['Vendor_Last'] = monthly_check['vendor']
monthly_check['Daily_check'] = monthly_check['Monthly_check']

#Remove index from the daily and monthly tables and then concatenate them
unindexed_daily = daily_inactivity.reset_index()
unindexed_monthly = monthly_check.reset_index()
dataframes= [unindexed_daily,unindexed_monthly]
unindexed_inactivity = pd.concat(dataframes, sort = True)

#Select the needed columns and give them appropraite names
InactivityTable = unindexed_inactivity[['VehicleID','UniqueVehicleNumber_L','ActivityDateTimeEST',
   'EventSubTypeID_Last',  'ActivityDateTimeEST_Last','Latitude_Last','Longitude_Last', 'Vendor_Last',
    'EventSubTypeID_First','ActivityDateTimeEST_First','Latitude_First','Longitude_First','Vendor_First',
    'Monthlystuffs','Daily_check']].set_index(['VehicleID','ActivityDateTimeEST']).sort_values(by=['VehicleID'])
InactivityTable.rename(columns = {'ActivityDateTimeEST':'Date','EventSubTypeID_Last':'Last_Event' ,
                       'ActivityDateTimeEST_Last':'Last_EventTime',' Latitude_Last':'Last_Latitude', 
                       'Longitude_Last':'Last_Longitude' ,'Vendor_Last':'Last_Vendor',
                       'EventSubTypeID_First':'Reactvation_Event','ActivityDateTimeEST_First':'Reactivatiopn_EventTime',
                       'Latitude_First':'Reactvation_Latitude','Longitude_First':'Reactivation_Longitude',
                       'Vendor_First':'Reactivation vendor','Daily_check':'Inactivity(hours)',
                        'Monthlystuffs':'Final_check','UniqueVehicleNumber_L':'UniqueVIN '},inplace= True)

#Make column for inactivity in days
InactivityTable['Inactivity(days)']= round((InactivityTable['Inactivity(hours)'] / 24), 0)