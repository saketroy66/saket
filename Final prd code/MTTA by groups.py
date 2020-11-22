import pandas as pd
import warnings
import mysql.connector
warnings.filterwarnings("ignore")

dt = pd.read_excel('Response Time & MTTR.xlsx',sheet_name='Incidents (CTS, BTS, DCNOC, SD)')
#for Sev1:
sev1 = dt[(dt['Priority'] == 'Critical') | (dt['Group']=='DCNOC') & (dt['Group']=="CTS")]
#print (sev1)

#For Response Time
sev1['Response times'] = sev1['INC.Response Time']-sev1['Reported Date']
sev1['Responsetimeinseconds']= sev1['Response times'].dt.total_seconds()
sev1['Response times'] = pd.to_datetime(sev1['Responsetimeinseconds'], unit='s').dt.strftime("%H:%M:%S")
#print(sev1.head(6))

#For MTTR
sev1['MTTRs']= sev1['Last Resolved Date']-sev1['Reported Date']
sev1['MTTRs_inseconds']= sev1['MTTRs'].dt.total_seconds()
sev1['MTTRs'] = pd.to_datetime(sev1['MTTRs_inseconds'], unit='s').dt.strftime("%H:%M:%S")
#print(sev1.head(6))

#For Week
sev1['Weeks'] = sev1['Last Resolved Date'].dt.isocalendar().week
#print (sev1.head(6))



#For sev1 Response time avg per teams:

groupteamresponseinsec = sev1.groupby(['Assigned Group']).mean()[['Responsetimeinseconds']]
groupteamresponseinsec['Responsetimeinseconds'] = groupteamresponseinsec.round(0)
print(groupteamresponseinsec)
#groupweekresponseinsec = groupweekresponseinsec.drop('Responsetimeinseconds',axis=1)

groupteamresponseinsec.rename(columns = {'Responsetimeinseconds':'Critical'}, inplace = True)
#print(groupweekresponseinsec)
groupteamresponseinsec['Critical'] = pd.to_datetime(groupteamresponseinsec["Critical"], unit='s').dt.strftime("%H:%M:%S")
#print(groupteamresponseinsec)
# x = x.drop('resp_sec',axis=1)



#For sev1 MTTRs avg per team:
groupteamMttrsinsec = sev1.groupby(['Assigned Group']).mean()[['MTTRs_inseconds']]
groupteamMttrsinsec['MTTRs_inseconds'] = groupteamMttrsinsec.round(0)
#print(groupteamMttrsinsec)

groupteamMttrsinsec.rename(columns = {'MTTRs_inseconds':'Critical'}, inplace = True)

groupteamMttrsinsec['Critical'] = pd.to_datetime(groupteamMttrsinsec["Critical"], unit='s').dt.strftime("%H:%M:%S")
#print(groupteamMttrsinsec)


# For Sev2:

sev2 = dt[(dt['Priority'] == 'High') | (dt['Group']=='DCNOC') & (dt['Group']=="CTS")]
#print (sev2)

#For Response Time
sev2['Response times'] = sev2['INC.Response Time']-sev2['Reported Date']
sev2['Responsetimeinseconds']= sev2['Response times'].dt.total_seconds()
sev2['Response times'] = pd.to_datetime(sev2['Responsetimeinseconds'], unit='s').dt.strftime("%H:%M:%S")
#print(sev2.head(6))

#For MTTR
sev2['MTTRs']= sev2['Last Resolved Date']-sev2['Reported Date']
sev2['MTTRs_inseconds']= sev2['MTTRs'].dt.total_seconds()
sev2['MTTRs'] = pd.to_datetime(sev2['MTTRs_inseconds'], unit='s').dt.strftime("%H:%M:%S")
#print(sev2.head(6))

#For Week
sev2['Weeks'] = sev2['Last Resolved Date'].dt.isocalendar().week
#print (sev2.head(6))

# incident counts
#ic = sev2.groupby(sev2['Assigned Group']).count()[['Incident ID']].sum()
#print(ic)



#For sev2 Response time avg per teams:
sev2_1 = sev2[['Priority','Assigned Group','Responsetimeinseconds']]
#print(sev2_1)
y = sev2_1.groupby(['Assigned Group']).mean()
#print(y.round())

y.rename(columns = {'Responsetimeinseconds':'High'}, inplace = True)
y['High'] = pd.to_datetime(y["High"], unit='s').dt.strftime("%H:%M:%S")
# y = y.drop('Responsetimeinseconds',axis=1)
#print(y)

b= pd.merge(y,groupteamresponseinsec,on=['Assigned Group'], how='outer')
#print(b)

b = b.fillna('')
#print(b)




#For sev2 MTTRs avg per teams:
sev2_2 = sev2[['Priority','Assigned Group','MTTRs_inseconds']]
y1 = sev2_2.groupby(['Assigned Group']).mean()
#print(y1.round())

y1.rename(columns = {'MTTRs_inseconds':'High'}, inplace = True)
y1['High'] = pd.to_datetime(y1["High"], unit='s').dt.strftime("%H:%M:%S")

b1= pd.merge(y1,groupteamMttrsinsec,on=['Assigned Group'], how='outer')
#print(b1)

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
sev3['MTTRs_inseconds']= sev3['MTTRs'].dt.total_seconds()
sev3['MTTRs'] = pd.to_datetime(sev3['MTTRs_inseconds'], unit='s').dt.strftime("%H:%M:%S")


#For Week
sev3['Weeks'] = sev3['Last Resolved Date'].dt.isocalendar().week
#print(sev3.head(6))

#ic = sev3.groupby(sev3['Assigned Group']).count()[['Incident ID']].sum()
#print(ic)


#For sev3 Response time avg per teams:
sev3_1 = sev3[['Priority','Assigned Group','Responsetimeinseconds']]
#print(sev2_1)
z = sev3_1.groupby(['Assigned Group']).mean()
#print(y.round())

z.rename(columns = {'Responsetimeinseconds':'Medium'}, inplace = True)
z['Medium'] = pd.to_datetime(z["Medium"], unit='s').dt.strftime("%H:%M:%S")


ResponseTable= pd.merge(z,b,on=['Assigned Group'], how='outer')
#print(ResponseTable)

ResponseTable = ResponseTable.fillna('')
#print(ResponseTable)




#For sev3 MTTRs avg per weeks:
sev3_2 = sev3[['Priority','Assigned Group','MTTRs_inseconds']]
z1 = sev3_2.groupby(['Assigned Group']).mean()
#print(y.round())

z1.rename(columns = {'MTTRs_inseconds':'Medium'}, inplace = True)
z1['Medium'] = pd.to_datetime(z1["Medium"], unit='s').dt.strftime("%H:%M:%S")
# z1 = z1.drop('Responsetimeinseconds',axis=1)
#print(z1)

MttrTable= pd.merge(z1,b1,on=['Assigned Group'], how='outer')
#print(MttrTable)

MttrTable = MttrTable.fillna('')
#print(MttrTable)







#for sev4:

sev4 = dt[(dt['Priority'] == 'Low') | (dt['Group'] == 'DCNOC') & (dt['Group'] == "CTS") & (dt['Group'] == 'BTS')]
#print (sev4)

#For Response Time
sev4['Response times'] = sev4['INC.Response Time']-sev4['Reported Date']
sev4['Responsetimeinseconds']= sev4['Response times'].dt.total_seconds()
sev4['Response times'] = pd.to_datetime(sev4['Responsetimeinseconds'], unit='s').dt.strftime("%H:%M:%S")
#print(sev4.head(6))

#For MTTR
sev4['MTTRs']= sev4['Last Resolved Date']-sev4['Reported Date']
#MTTRs_inseconds= sev4['MTTRs'].dt.total_seconds()-sev4['Accumulated SLAHOLD Time (sec)']
sev4['MTTRs_inseconds']= sev4['MTTRs'].dt.total_seconds()
sev4['MTTRs'] = pd.to_datetime(sev4['MTTRs_inseconds'], unit='s').dt.strftime("%H:%M:%S")



#For Week
sev4['Weeks'] = sev4['Last Resolved Date'].dt.isocalendar().week

#ic = sev4.groupby(sev4['Assigned Group']).count()[['Incident ID']].sum()
#print(ic)


#For sev4 Response time avg per teams:
sev4_1 = sev4[['Priority','Assigned Group','Responsetimeinseconds']]
#print(sev4_1)
p = sev4_1.groupby(['Assigned Group']).mean()
#print(y.round())

p.rename(columns = {'Responsetimeinseconds':'Low'}, inplace = True)
p['Low'] = pd.to_datetime(p["Low"], unit='s').dt.strftime("%H:%M:%S")

# For Response Table based on severity:

ResponseTablefinal= pd.merge(p,ResponseTable,on=['Assigned Group'], how='outer')
#print(ResponseTablefinal)

ResponseTablefinal = ResponseTablefinal.fillna('')
print(ResponseTablefinal)


ResponseTablefinal.to_excel('MTTA by Group.xlsx', engine='xlsxwriter')