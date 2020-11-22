import pandas as pd
import warnings
warnings.filterwarnings("ignore")

dt = pd.read_excel('Response Time & MTTR.xlsx',sheet_name='Incidents (CTS, BTS, DCNOC, SD)')

#for Sev1:
sev1 = dt[(dt['Priority'] == 'Critical') | (dt['Group']=='DCNOC') & (dt['Group']=="CTS")]

#For Response Time
sev1['Response times'] = sev1['INC.Response Time']-sev1['Reported Date']
sev1['Responsetimeinseconds']= sev1['Response times'].dt.total_seconds()
sev1['Response times'] = pd.to_datetime(sev1['Responsetimeinseconds'], unit='s').dt.strftime("%H:%M:%S")

#For MTTR
sev1['MTTRs']= sev1['Last Resolved Date']-sev1['Reported Date']
sev1['MTTRs_inseconds']= sev1['MTTRs'].dt.total_seconds()- sev1['Accumulated SLAHOLD Time (sec)']
sev1['MTTRs'] = pd.to_datetime(sev1['MTTRs_inseconds'], unit='s').dt.strftime("%H:%M:%S")

#For Week
sev1['Weeks'] = sev1['Last Resolved Date'].dt.isocalendar().week

#for Month
sev1['Months'] = pd.DatetimeIndex(sev1["Last Resolved Date"]).month


#For sev1 Response time avg per month:
groupweekresponseinsec = sev1.groupby(['Months']).mean()[['Responsetimeinseconds']]
groupweekresponseinsec['Responsetimeinseconds'] = groupweekresponseinsec.round(0)

groupweekresponseinsec.rename(columns = {'Responsetimeinseconds':'Critical'}, inplace = True)
groupweekresponseinsec['Critical'] = pd.to_datetime(groupweekresponseinsec["Critical"], unit='s').dt.strftime("%H:%M:%S")



#For sev1 MTTRs avg per month:
groupweekMttrsinsec = sev1.groupby(['Months']).mean()[['MTTRs_inseconds']]
groupweekMttrsinsec['MTTRs_inseconds'] = groupweekMttrsinsec.round(0)

groupweekMttrsinsec.rename(columns = {'MTTRs_inseconds':'Critical'}, inplace = True)

groupweekMttrsinsec['Critical'] = pd.to_datetime(groupweekMttrsinsec["Critical"], unit='s').dt.strftime("%H:%M:%S")
#print(groupweekMttrsinsec)



# For Sev2:
sev2 = dt[(dt['Priority'] == 'High') | (dt['Group']=='DCNOC') & (dt['Group']=="CTS")]

#For Response Time
sev2['Response times'] = sev2['INC.Response Time']-sev2['Reported Date']
sev2['Responsetimeinseconds']= sev2['Response times'].dt.total_seconds()
sev2['Response times'] = pd.to_datetime(sev2['Responsetimeinseconds'], unit='s').dt.strftime("%H:%M:%S")

#For MTTR
sev2['MTTRs']= sev2['Last Resolved Date']-sev2['Reported Date']
sev2['MTTRs_inseconds']= sev2['MTTRs'].dt.total_seconds()- sev2['Accumulated SLAHOLD Time (sec)']
sev2['MTTRs'] = pd.to_datetime(sev2['MTTRs_inseconds'], unit='s').dt.strftime("%H:%M:%S")

#For Week
sev2['Weeks'] = sev2['Last Resolved Date'].dt.isocalendar().week

#for Month
sev2['Months'] = pd.DatetimeIndex(sev2["Last Resolved Date"]).month

# incident counts
ic = sev2.groupby(sev2['Assigned Group']).count()[['Incident ID']].sum()
#print(ic)


#For sev2 Response time avg per month:
sev2_1 = sev2[['Priority','Months','Responsetimeinseconds']]
#print(sev2_1)
y = sev2_1.groupby(['Months']).mean()
#print(y.round())

y.rename(columns = {'Responsetimeinseconds':'High'}, inplace = True)
y['High'] = pd.to_datetime(y["High"], unit='s').dt.strftime("%H:%M:%S")

b= pd.merge(y,groupweekresponseinsec,on=['Months'], how='outer')
b = b.fillna('')




#For sev2 MTTRs avg per month:
sev2_2 = sev2[['Priority','Months','MTTRs_inseconds']]
y1 = sev2_2.groupby(['Months']).mean()
#print(y1.round())

y1.rename(columns = {'MTTRs_inseconds':'High'}, inplace = True)
y1['High'] = pd.to_datetime(y1["High"], unit='s').dt.strftime("%H:%M:%S")
# y1 = y1.drop('MTTRs_inseconds',axis=1)
#print(y)

b1= pd.merge(y1,groupweekMttrsinsec,on=['Months'], how='outer')
#print(b)

b1 = b1.fillna('')
#print(b1)




#for Sev3 :
sev3 = dt[(dt['Priority'] == 'Medium') | (dt['Group'] == 'DCNOC') & (dt['Group'] == "CTS") & (dt['Group'] == 'BTS')]

#For Response Time
sev3['Response times'] = sev3['INC.Response Time']-sev3['Reported Date']
sev3['Responsetimeinseconds']= sev3['Response times'].dt.total_seconds()
sev3['Response times'] = pd.to_datetime(sev3['Responsetimeinseconds'], unit='s').dt.strftime("%H:%M:%S")

#For MTTR
sev3['MTTRs']= sev3['Last Resolved Date']-sev3['Reported Date']
#MTTRs_inseconds= sev3['MTTRs'].dt.total_seconds()-sev3['Accumulated SLAHOLD Time (sec)']
sev3['MTTRs_inseconds']= sev3['MTTRs'].dt.total_seconds()-sev3['Accumulated SLAHOLD Time (sec)']
sev3['MTTRs'] = pd.to_datetime(sev3['MTTRs_inseconds'], unit='s').dt.strftime("%H:%M:%S")


#For Week
sev3['Weeks'] = sev3['Last Resolved Date'].dt.isocalendar().week

#for Month
sev3['Months'] = pd.DatetimeIndex(sev3["Last Resolved Date"]).month

#ic = sev3.groupby(sev3['Assigned Group']).count()[['Incident ID']].sum()



#For sev3 Response time avg per months:
sev3_1 = sev3[['Priority','Months','Responsetimeinseconds']]
#print(sev2_1)
z = sev3_1.groupby(['Months']).mean()

z.rename(columns = {'Responsetimeinseconds':'Medium'}, inplace = True)
z['Medium'] = pd.to_datetime(z["Medium"], unit='s').dt.strftime("%H:%M:%S")

ResponseTable= pd.merge(z,b,on=['Months'], how='outer')
ResponseTable = ResponseTable.fillna('')
#print(ResponseTable)




#For sev3 MTTRs avg per month:
sev3_2 = sev3[['Priority','Months','MTTRs_inseconds']]
z1 = sev3_2.groupby(['Months']).mean()
#print(y.round())

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
#MTTRs_inseconds= sev4['MTTRs'].dt.total_seconds()-sev4['Accumulated SLAHOLD Time (sec)']
sev4['MTTRs_inseconds']= sev4['MTTRs'].dt.total_seconds()-sev4['Accumulated SLAHOLD Time (sec)']
sev4['MTTRs'] = pd.to_datetime(sev4['MTTRs_inseconds'], unit='s').dt.strftime("%H:%M:%S")


#For Week
sev4['Weeks'] = sev4['Last Resolved Date'].dt.isocalendar().week

#for Month
sev4['Months'] = pd.DatetimeIndex(sev4["Last Resolved Date"]).month

ic = sev4.groupby(sev4['Assigned Group']).count()[['Incident ID']].sum()



#For sev4 Response time avg per month:
sev4_1 = sev4[['Priority','Months','Responsetimeinseconds']]
#print(sev4_1)
p = sev4_1.groupby(['Months']).mean()
#print(y.round())

p.rename(columns = {'Responsetimeinseconds':'Low'}, inplace = True)
p['Low'] = pd.to_datetime(p["Low"], unit='s').dt.strftime("%H:%M:%S")

ResponseTablefinal= pd.merge(p,ResponseTable,on=['Months'], how='outer')
#print(ResponseTablefinal)

ResponseTablefinal = ResponseTablefinal.fillna('')
#print(ResponseTablefinal)




#For sev4 MTTRs avg per month:
sev4_2 = sev4[['Priority','Months','MTTRs_inseconds']]
#print(sev4_1)
p1 = sev4_2.groupby(['Months']).mean()
#print(y.round())

p1.rename(columns = {'MTTRs_inseconds':'Low'}, inplace = True)
p1['Low'] = pd.to_datetime(p1["Low"], unit='s').dt.strftime("%H:%M:%S")
MttrTablefinal= pd.merge(p1,MttrTable,on=['Months'], how='outer')
#print(MttrTablefinal)

MttrTablefinal = MttrTablefinal.fillna('')
print(MttrTablefinal)






# sum_row=MttrTablefinal[["Months"]].sum()
# print(sum_row)
#
# MttrTablefinal1= pd.DataFrame(data=sum_row).T
# print(MttrTablefinal1)



MttrTablefinal.append(pd.DataFrame(MttrTablefinal['Low'].sum(), index = ["Total"], columns=["Low"]))
print(MttrTablefinal)


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


df4 = pd.DataFrame({'Medium': list(MttrTablefinal["Medium"])})
df5 = pd.to_timedelta(df4.Medium).sum()
#print (df5)
#
df6 = pd.DataFrame({'Low': list(MttrTablefinal["Low"])})
df7 = pd.to_timedelta(df6.Low).sum()
#print (df7)

MttrTablefinal.loc['Total'] = pd.Series(df5, index = ['Medium'])
MttrTablefinal.loc['Total'] = pd.Series(df7, index = ['Low'])
#print(MttrTablefinal)




MttrTablefinal.to_excel('MttrTable with Hold time.xlsx', engine='xlsxwriter')
