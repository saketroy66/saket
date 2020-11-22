import pandas as pd
import warnings
import datetime as dp
warnings.filterwarnings("ignore")

dt = pd.read_excel('Response Time & MTTR.xlsx',sheet_name='Incidents (CTS, BTS, DCNOC, SD)')

#for Sev1:
sev1 = dt[(dt['Priority'] == 'Critical') | (dt['Group']=='DCNOC') & (dt['Group']=="CTS")]

#For Response Time
sev1['Response times'] = sev1['INC.Response Time']-sev1['Reported Date']
sev1['Responsetimeinseconds'] = sev1['Response times'].dt.total_seconds()
sev1['Response times'] = pd.to_datetime(sev1['Responsetimeinseconds'], unit='s').dt.strftime("%H:%M:%S")


#For MTTR
sev1['MTTRs']= sev1['Last Resolved Date']-sev1['Reported Date']
sev1['MTTRs_inseconds']= sev1['MTTRs'].dt.total_seconds()
sev1['MTTRs'] = pd.to_datetime(sev1['MTTRs_inseconds'], unit='s').dt.strftime("%H:%M:%S")

#For Week
sev1['Weeks'] = sev1['Last Resolved Date'].dt.isocalendar().week

#for Month
sev1['Months'] = pd.DatetimeIndex(sev1["Last Resolved Date"]).month

#For sev1 Response time avg per month:
groupmonthresponseinsec = sev1.groupby(['Months']).mean()[['Responsetimeinseconds']]
groupmonthresponseinsec['Responsetimeinseconds'] = groupmonthresponseinsec.round(0)
groupmonthresponseinsec.rename(columns = {'Responsetimeinseconds':'Critical'}, inplace = True)
groupmonthresponseinsec['Critical'] = pd.to_datetime(groupmonthresponseinsec["Critical"], unit='s').dt.strftime("%H:%M:%S")


#For sev1 MTTRs avg per month:
groupmonthMttrsinsec = sev1.groupby(['Months']).mean()[['MTTRs_inseconds']]
groupmonthMttrsinsec['MTTRs_inseconds'] = groupmonthMttrsinsec.round(0)
groupmonthMttrsinsec.rename(columns = {'MTTRs_inseconds':'Critical'}, inplace = True)
groupmonthMttrsinsec['Critical'] = pd.to_datetime(groupmonthMttrsinsec["Critical"], unit='s').dt.strftime("%H:%M:%S")
#print(groupmonthMttrsinsec)


# For Sev2:
sev2 = dt[(dt['Priority'] == 'High') | (dt['Group']=='DCNOC') & (dt['Group']=="CTS")]
#print (sev2)

#For Response Time
sev2['Response times'] = sev2['INC.Response Time']-sev2['Reported Date']
sev2['Responsetimeinseconds']= sev2['Response times'].dt.total_seconds()
sev2['Response times'] = pd.to_datetime(sev2['Responsetimeinseconds'], unit='s').dt.strftime("%H:%M:%S")


#For MTTR
sev2['MTTRs']= sev2['Last Resolved Date']-sev2['Reported Date']
sev2['MTTRs_inseconds']= sev2['MTTRs'].dt.total_seconds()
sev2['MTTRs'] = pd.to_datetime(sev2['MTTRs_inseconds'], unit='s').dt.strftime("%H:%M:%S")

#For Week
sev2['Weeks'] = sev2['Last Resolved Date'].dt.isocalendar().week

#for Month
sev2['Months'] = pd.DatetimeIndex(sev2["Last Resolved Date"]).month

# incident counts
#ic = sev2.groupby(sev2['Assigned Group']).count()[['Incident ID']].sum()
#print(ic)


#For sev2 Response time avg per month:
sev2_1 = sev2[['Priority','Months','Responsetimeinseconds']]
#print(sev2_1)
y = sev2_1.groupby(['Months']).mean()
y.rename(columns = {'Responsetimeinseconds':'High'}, inplace = True)
y['High'] = pd.to_datetime(y["High"], unit='s').dt.strftime("%H:%M:%S")

b= pd.merge(y,groupmonthresponseinsec,on=['Months'], how='outer')
b = b.fillna('')


#For sev2 MTTRs avg per weeks:
sev2_2 = sev2[['Priority','Months','MTTRs_inseconds']]
y1 = sev2_2.groupby(['Months']).mean()
#print(y1.round())
y1.rename(columns = {'MTTRs_inseconds':'High'}, inplace = True)
y1['High'] = pd.to_datetime(y1["High"], unit='s').dt.strftime("%H:%M:%S")

b1= pd.merge(y1,groupmonthMttrsinsec,on=['Months'], how='outer')
b1 = b1.fillna('')




#for Sev3 :
sev3 = dt[(dt['Priority'] == 'Medium') | (dt['Group'] == 'DCNOC') & (dt['Group'] == "CTS") & (dt['Group'] == 'BTS')]

#For Response Time
sev3['Response times'] = sev3['INC.Response Time']-sev3['Reported Date']
sev3['Responsetimeinseconds']= sev3['Response times'].dt.total_seconds()
sev3['Response times'] = pd.to_datetime(sev3['Responsetimeinseconds'], unit='s').dt.strftime("%H:%M:%S")

#For MTTR
sev3['MTTRs']= sev3['Last Resolved Date']-sev3['Reported Date']
sev3['MTTRs_inseconds']= sev3['MTTRs'].dt.total_seconds()
sev3['MTTRs'] = pd.to_datetime(sev3['MTTRs_inseconds'], unit='s').dt.strftime("%H:%M:%S")

#For Week
sev3['Weeks'] = sev3['Last Resolved Date'].dt.isocalendar().week

#for Month
sev3['Months'] = pd.DatetimeIndex(sev3["Last Resolved Date"]).month

#ic = sev3.groupby(sev3['Assigned Group']).count()[['Incident ID']].sum()
#print(ic)


#For sev3 Response time avg per month:
sev3_1 = sev3[['Priority','Months','Responsetimeinseconds']]
z = sev3_1.groupby(['Months']).mean()

z.rename(columns = {'Responsetimeinseconds':'Medium'}, inplace = True)
z['Medium'] = pd.to_datetime(z["Medium"], unit='s').dt.strftime("%H:%M:%S")

ResponseTable= pd.merge(z,b,on=['Months'], how='outer')
ResponseTable = ResponseTable.fillna('')


#For sev3 MTTRs avg per month:
sev3_2 = sev3[['Priority','Months','MTTRs_inseconds']]
z1 = sev3_2.groupby(['Months']).mean()

z1.rename(columns = {'MTTRs_inseconds':'Medium'}, inplace = True)
z1['Medium'] = pd.to_datetime(z1["Medium"], unit='s').dt.strftime("%H:%M:%S")

MttrTable= pd.merge(z1,b1,on=['Months'], how='outer')
MttrTable = MttrTable.fillna('')



#for sev4:
sev4 = dt[(dt['Priority'] == 'Low') | (dt['Group'] == 'DCNOC') & (dt['Group'] == "CTS") & (dt['Group'] == 'BTS')]

#For Response Time
sev4['Response times'] = sev4['INC.Response Time']-sev4['Reported Date']
sev4['Responsetimeinseconds']= sev4['Response times'].dt.total_seconds()
sev4['Response times'] = pd.to_datetime(sev4['Responsetimeinseconds'], unit='s').dt.strftime("%H:%M:%S")

#For MTTR
sev4['MTTRs']= sev4['Last Resolved Date']-sev4['Reported Date']
sev4['MTTRs_inseconds']= sev4['MTTRs'].dt.total_seconds()
sev4['MTTRs'] = pd.to_datetime(sev4['MTTRs_inseconds'], unit='s').dt.strftime("%H:%M:%S")

#For Week
sev4['Weeks'] = sev4['Last Resolved Date'].dt.isocalendar().week

#for Month
sev4['Months'] = pd.DatetimeIndex(sev4["Last Resolved Date"]).month



#For sev4 Response time avg per month:
sev4_1 = sev4[['Priority','Months','Responsetimeinseconds']]
p = sev4_1.groupby(['Months']).mean()

p.rename(columns = {'Responsetimeinseconds':'Low'}, inplace = True)
p['Low'] = pd.to_datetime(p["Low"], unit='s').dt.strftime("%H:%M:%S")

ResponseTablefinal= pd.merge(p,ResponseTable,on=['Months'], how='outer')
ResponseTablefinal = ResponseTablefinal.fillna('')
#print(ResponseTablefinal)

# sum_row=ResponseTablefinal[['Low',"Medium","High","Critical"]].sum()
# print(sum_row)


#For sev4 MTTRs avg per month:
sev4_2 = sev4[['Priority','Months','MTTRs_inseconds']]
p1 = sev4_2.groupby(['Months']).mean()
#print(y.round())
p1.rename(columns = {'MTTRs_inseconds':'Low'}, inplace = True)
p1['Low'] = pd.to_datetime(p1["Low"], unit='s').dt.strftime("%H:%M:%S")

MttrTablefinal= pd.merge(p1,MttrTable,on=['Months'], how='outer')
MttrTablefinal = MttrTablefinal.fillna('')
print(MttrTablefinal)





#####Today implimentation#####
df6 =pd.DataFrame({'Low': list(MttrTablefinal["Low"])})
df7 = pd.to_timedelta(df6.Low).sum()
print ('Low: '+str(df7))

df4 = pd.DataFrame({'Medium': list(MttrTablefinal["Medium"])})
df5 = pd.to_timedelta(df4.Medium).agg("sum")
print ('Medium: '+str(df5))

df8 =pd.DataFrame({'High': list(MttrTablefinal["High"])})
df9 = pd.to_timedelta(df8.High).sum()
print ('High: '+str(df9))

df10 =pd.DataFrame({'Critical': list(MttrTablefinal["Critical"])})
df11 = pd.to_timedelta(df10.Critical).sum()
print ('Critical: '+str(df11))

#MttrTablefinal.loc['Total'] = pd.Series(df5, index = ['Medium'])




MttrTablefinal.to_excel('MttrTable Without Hold Time.xlsx', engine='xlsxwriter')
