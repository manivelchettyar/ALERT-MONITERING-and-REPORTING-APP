from __future__ import print_function
import pandas as pd
import cx_Oracle
import os
import pymssql
from datetime import date,timedelta
from re  import search


cx_Oracle.init_oracle_client(lib_dir=r"C:\oracleClient\instantclient_19_6")


tv_offered=0
tv_transferred=0
JIO_MART_V2_offered=0
MOJO_ONNET_V3_offered=0
BALANCE_MISSCALL_V1_offered=0
MerchantFlow_V4_offered=0
FTTX_IVR_V2_offered=0
FTTX_IVR_V2_transferred=0
AssistedCareONNET_V5_offered=0
AssistedCareONNET_V5_transferred=0
AssistedCareOFFNET_V5_offered=0
AssistedCareOFFNET_V5_transferred=0

tv_offered_1=0
tv_transferred_1=0
JIO_MART_V2_offered_1=0
MOJO_ONNET_V3_offered_1=0
BALANCE_MISSCALL_V1_offered_1=0
MerchantFlow_V4_offered_1=0
FTTX_IVR_V2_offered_1=0
FTTX_IVR_V2_transferred_1=0
AssistedCareONNET_V5_offered_1=0
AssistedCareONNET_V5_transferred_1=0
AssistedCareOFFNET_V5_offered_1=0
AssistedCareOFFNET_V5_transferred_1=0



def main():
    #cx_Oracle.init_oracle_client(lib_dir=r"C:\oracleClient\instantclient_19_6")
    oh="C:\oracleClient\instantclient_19_6"
    os.environ["ORACLE_HOME"]=oh
    os.environ["PATH"]=oh+os.pathsep+os.environ["PATH"]

    print("File:", cx_Oracle.__file__)
    print("Client Version:", ".".join(str(i) for i in cx_Oracle.clientversion()))

    conn1 = cx_Oracle.connect('prod_ivr_config/Szyvr7#u@10.141.53.242:1521/PJIVRDB')
    cursor1 = conn1.cursor()
    query1 = "SELECT COUNT(*)Total_Call,SUM(case when FLDIVREXIT is not null then 1 else 0 end ) Transfer, FLDAPPNAME FROM TBLCALLDETAIL WHERE FLDCALLSTARTTIME >= TRUNC(SYSDATE-1) AND FLDCALLSTARTTIME    < TRUNC(SYSDATE) GROUP BY FLDAPPNAME"
    cursor1.execute(query1)
    df1 = pd.DataFrame(cursor1.fetchall())
    cursor1.close()
    print(df1)

    conn2 = cx_Oracle.connect('prod_ivr_config/Szyvr7#u@10.141.53.242:1521/PJIVRDB')
    cursor2 = conn2.cursor()
    query2 = "SELECT COUNT(*)Total_Call,SUM(case when FLDIVREXIT is not null then 1 else 0 end ) Transfer, FLDAPPNAME FROM TBLCALLDETAIL WHERE FLDCALLSTARTTIME >= TRUNC(SYSDATE-2) AND FLDCALLSTARTTIME< TRUNC(SYSDATE-1) GROUP BY FLDAPPNAME"
    cursor2.execute(query2)
    df2 = pd.DataFrame(cursor2.fetchall())
    cursor2.close()
    print(df2)




    conn3 = cx_Oracle.connect('SI_PROD_INFOMART/Aghtjh#gh7@10.141.193.22/PJVOICE')
    cursor4 = conn3.cursor()
    qry1 = "SELECT TD.TECHNICAL_RESULT_CODE,Count(1) FROM MEDIATION_SEGMENT_FACT MSF INNER JOIN DATE_TIME DT1 ON DT1.DATE_TIME_KEY = MSF.START_DATE_TIME_KEY INNER JOIN INTERACTION_FACT INF ON INF.INTERACTION_ID = MSF.INTERACTION_ID AND INF.START_DATE_TIME_KEY = MSF.INTERACTION_SDT_KEY INNER JOIN DATE_TIME DT ON DT.DATE_TIME_KEY = INF.START_DATE_TIME_KEY LEFT OUTER JOIN IRF_USER_DATA_VOICE_1  VUD1 ON VUD1.INTERACTION_RESOURCE_ID = MSF.MEDIATION_SEGMENT_ID AND VUD1.START_DATE_TIME_KEY = MSF.START_DATE_TIME_KEY LEFT OUTER JOIN IRF_USER_DATA_VOICE_2  VUD2 ON VUD2.INTERACTION_RESOURCE_ID = MSF.MEDIATION_SEGMENT_ID AND VUD2.START_DATE_TIME_KEY = MSF.START_DATE_TIME_KEY LEFT OUTER JOIN IRF_USER_DATA_VOICE_3  VUD3 ON VUD3.INTERACTION_RESOURCE_ID = MSF.MEDIATION_SEGMENT_ID AND VUD3.START_DATE_TIME_KEY = MSF.START_DATE_TIME_KEY left outer join IRF_USER_DATA_CUST_2 UD2 ON UD2.INTERACTION_RESOURCE_ID = MSF.MEDIATION_SEGMENT_ID AND UD2.START_DATE_TIME_KEY= MSF.START_DATE_TIME_KEY INNER JOIN INTERACTION_TYPE IT ON IT.INTERACTION_TYPE_KEY = MSF.INTERACTION_TYPE_KEY INNER JOIN MEDIA_TYPE MT ON MT.MEDIA_TYPE_KEY = MSF.MEDIA_TYPE_KEY INNER JOIN RESOURCE_ QR ON QR.RESOURCE_KEY = MSF.RESOURCE_KEY inner join TECHNICAL_DESCRIPTOR  td  on(MSF.TECHNICAL_DESCRIPTOR_KEY = td.TECHNICAL_DESCRIPTOR_KEY) LEFT OUTER JOIN INTERACTION_RESOURCE_FACT IRF ON MSF.TARGET_IXN_RESOURCE_ID = IRF.INTERACTION_RESOURCE_ID AND MSF.TARGET_IXN_RESOURCE_SDT_KEY = IRF.START_DATE_TIME_KEY LEFT OUTER JOIN RESOURCE_ AR ON AR.RESOURCE_KEY = IRF.RESOURCE_KEY where  MT.MEDIA_NAME_CODE = 'VOICE' and dt1.cal_date >= to_date(trunc(sysdate-1)) and dt1.cal_date < to_date(trunc(Sysdate)) and substr(VUD1.CUSTOM_DATA_1,-4)  in (2014,2001,2000) and QR.RESOURCE_NAME like '%_VQ' and QR.RESOURCE_NAME not like '%XTRAI%' group by TD.TECHNICAL_RESULT_CODE"
    cursor4.execute(qry1)
    df3 = pd.DataFrame(cursor4.fetchall())
    cursor4.close()
    df3.columns = ['status', 'count']
    gen_acd_offered_1 = df3['count'].sum()
    gen_diverted_1 =int(df3.query('status=="DIVERTED"')['count'])
    print(df3)
    print(gen_acd_offered_1)
    print(gen_diverted_1)

    conn4 = cx_Oracle.connect('SI_PROD_INFOMART/Aghtjh#gh7@10.141.193.22/PJVOICE')
    cursor5 = conn4.cursor()
    qry2 = "SELECT TD.TECHNICAL_RESULT_CODE,Count(1) FROM MEDIATION_SEGMENT_FACT MSF INNER JOIN DATE_TIME DT1 ON DT1.DATE_TIME_KEY = MSF.START_DATE_TIME_KEY INNER JOIN INTERACTION_FACT INF ON INF.INTERACTION_ID = MSF.INTERACTION_ID AND INF.START_DATE_TIME_KEY = MSF.INTERACTION_SDT_KEY INNER JOIN DATE_TIME DT ON DT.DATE_TIME_KEY = INF.START_DATE_TIME_KEY LEFT OUTER JOIN IRF_USER_DATA_VOICE_1  VUD1 ON VUD1.INTERACTION_RESOURCE_ID = MSF.MEDIATION_SEGMENT_ID AND VUD1.START_DATE_TIME_KEY = MSF.START_DATE_TIME_KEY LEFT OUTER JOIN IRF_USER_DATA_VOICE_2  VUD2 ON VUD2.INTERACTION_RESOURCE_ID = MSF.MEDIATION_SEGMENT_ID AND VUD2.START_DATE_TIME_KEY = MSF.START_DATE_TIME_KEY LEFT OUTER JOIN IRF_USER_DATA_VOICE_3  VUD3 ON VUD3.INTERACTION_RESOURCE_ID = MSF.MEDIATION_SEGMENT_ID AND VUD3.START_DATE_TIME_KEY = MSF.START_DATE_TIME_KEY left outer join IRF_USER_DATA_CUST_2 UD2 ON UD2.INTERACTION_RESOURCE_ID = MSF.MEDIATION_SEGMENT_ID AND UD2.START_DATE_TIME_KEY= MSF.START_DATE_TIME_KEY INNER JOIN INTERACTION_TYPE IT ON IT.INTERACTION_TYPE_KEY = MSF.INTERACTION_TYPE_KEY INNER JOIN MEDIA_TYPE MT ON MT.MEDIA_TYPE_KEY = MSF.MEDIA_TYPE_KEY INNER JOIN RESOURCE_ QR ON QR.RESOURCE_KEY = MSF.RESOURCE_KEY inner join TECHNICAL_DESCRIPTOR  td  on(MSF.TECHNICAL_DESCRIPTOR_KEY = td.TECHNICAL_DESCRIPTOR_KEY) LEFT OUTER JOIN INTERACTION_RESOURCE_FACT IRF ON MSF.TARGET_IXN_RESOURCE_ID = IRF.INTERACTION_RESOURCE_ID AND MSF.TARGET_IXN_RESOURCE_SDT_KEY = IRF.START_DATE_TIME_KEY LEFT OUTER JOIN RESOURCE_ AR ON AR.RESOURCE_KEY = IRF.RESOURCE_KEY where  MT.MEDIA_NAME_CODE = 'VOICE' and dt1.cal_date >= to_date(trunc(sysdate-2)) and dt1.cal_date < to_date(trunc(Sysdate-1)) and substr(VUD1.CUSTOM_DATA_1,-4)  in (2014,2001,2000) and QR.RESOURCE_NAME like '%_VQ' and QR.RESOURCE_NAME not like '%XTRAI%' group by TD.TECHNICAL_RESULT_CODE"
    cursor5.execute(qry2)
    df4 = pd.DataFrame(cursor5.fetchall())
    cursor5.close()
    df4.columns = ['status', 'count']
    gen_acd_offered_2 = df4['count'].sum()
    gen_diverted_2 = int(df4.query('status=="DIVERTED"')['count'])
    print(df4)
    print(gen_acd_offered_2)
    print(gen_diverted_2)




    server2 = '10.141.49.227:1433'
    database2 = 'IVRReports_db'
    username2 = 'ro_ivruser'
    password2 = 'u$ro_ivruser'
    con2 = pymssql.connect(server2, username2, password2, database2)
    cursor_2 = con2.cursor()

    IVR_TRANSFERRED227_1 = "select count(*) from TBL_IVRCALLACTIVITY(nolock) where  AGENT_TRANSFERED_STATUS='Y' and DISPOSITION='XA'and  STARTDATETIME >= (CONVERT(varchar(11),GetDate()-1,120)) and STARTDATETIME < (CONVERT(varchar(11),GetDate(),120))"
    IVR_TRANSFERRED227_2 = "select count(*) from TBL_IVRCALLACTIVITY(nolock) where  AGENT_TRANSFERED_STATUS='Y' and DISPOSITION='XA'and  STARTDATETIME >= (CONVERT(varchar(11),GetDate()-2,120)) and STARTDATETIME < (CONVERT(varchar(11),GetDate()-1,120))"
    IVR_OFFERED227_1 = "select count(*) from  TBL_IVRCALLACTIVITY(nolock) where STARTDATETIME >= (CONVERT(varchar(11),GetDate()-1,120)) and STARTDATETIME < (CONVERT(varchar(11),GetDate(),120)) "
    IVR_OFFERED227_2 = "select count(*) from  TBL_IVRCALLACTIVITY(nolock) where STARTDATETIME >= (CONVERT(varchar(11),GetDate()-2,120)) and STARTDATETIME < (CONVERT(varchar(11),GetDate()-1,120)) "

    queries2 = [IVR_TRANSFERRED227_1, IVR_TRANSFERRED227_2, IVR_OFFERED227_1, IVR_OFFERED227_2]

    result2 = []
    for i in queries2:
        cursor_2.execute(i)
        resultA = cursor_2.fetchall()
        result2.append(resultA)
    cursor_2.close()

    print(result2)

    server3 = '10.141.49.230:1433'
    database3 = 'IVRReports_db'
    username3 = 'ro_ivruser'
    password3 = 'u$ro_ivruser'
    con3 = pymssql.connect(server3, username3, password3, database3)
    cursor_3 = con3.cursor()

    IVR_TRANSFERRED230_1 = "select count(*) from TBL_IVRCALLACTIVITY(nolock) where  AGENT_TRANSFERED_STATUS='Y' and DISPOSITION='XA'and  STARTDATETIME >= (CONVERT(varchar(11),GetDate()-1,120)) and STARTDATETIME < (CONVERT(varchar(11),GetDate(),120))"
    IVR_TRANSFERRED230_2 = "select count(*) from TBL_IVRCALLACTIVITY(nolock) where  AGENT_TRANSFERED_STATUS='Y' and DISPOSITION='XA'and  STARTDATETIME >= (CONVERT(varchar(11),GetDate()-2,120)) and STARTDATETIME < (CONVERT(varchar(11),GetDate()-1,120))"
    IVR_OFFERED230_1 = "select count(*) from  TBL_IVRCALLACTIVITY(nolock) where STARTDATETIME >= (CONVERT(varchar(11),GetDate()-1,120)) and STARTDATETIME < (CONVERT(varchar(11),GetDate(),120)) "
    IVR_OFFERED230_2 = "select count(*) from  TBL_IVRCALLACTIVITY(nolock) where STARTDATETIME >= (CONVERT(varchar(11),GetDate()-2,120)) and STARTDATETIME < (CONVERT(varchar(11),GetDate()-1,120)) "

    queries3 = [IVR_TRANSFERRED230_1, IVR_TRANSFERRED230_2, IVR_OFFERED230_1, IVR_OFFERED230_2]

    result3 = []
    for i in queries3:
        cursor_3.execute(i)
        resultB = cursor_3.fetchall()
        result3.append(resultB)
    cursor_3.close()

    print(result3)

    server4 = '10.142.116.32:1433'
    database4 = 'jpag1_awdb'
    username4 = 'Dashboard'
    password4 = 'Admin#1234'
    con4 = pymssql.connect(server4, username4, password4, database4)
    cursor_4 = con4.cursor()

    CUIC_OFF_TRANS_1 = "select TotalCallsOffered = sum(isnull(CTSG.RouterCallsAbandQ,0)) + sum(isnull(CTSG.RouterCallsAbandToAgent,0)) + sum(isnull( CTSG.CallsAnswered,0))+ sum(isnull( CTSG.ShortCalls,0)), callsAnswered = sum(isnull( CTSG.CallsAnswered,0)) FROM  Call_Type_SG_Interval (nolock) CTSG inner join Skill_Group (nolock) SKG on CTSG.SkillGroupSkillTargetID=SKG.SkillTargetID,Call_Type (nolock) Where ( SKG.EnterpriseName not Like ('%OUTB%') AND SKG.EnterpriseName not Like ('%L2ESC%') AND SKG.EnterpriseName not Like ('%NWFEE%') ) and  (SKG.SkillTargetID = CTSG.SkillGroupSkillTargetID) and Call_Type.CallTypeID = CTSG.CallTypeID and CTSG.DateTime >=CONVERT(varchar(11),GETDATE()-1,120) and CTSG.DateTime < CONVERT(varchar(11),GETDATE(),120)"
    CUIC_OFF_TRANS_2 = "select TotalCallsOffered = sum(isnull(CTSG.RouterCallsAbandQ,0)) + sum(isnull(CTSG.RouterCallsAbandToAgent,0)) + sum(isnull( CTSG.CallsAnswered,0))+ sum(isnull( CTSG.ShortCalls,0)), callsAnswered = sum(isnull( CTSG.CallsAnswered,0)) FROM  Call_Type_SG_Interval (nolock) CTSG inner join Skill_Group (nolock) SKG on CTSG.SkillGroupSkillTargetID=SKG.SkillTargetID,Call_Type (nolock) Where ( SKG.EnterpriseName not Like ('%OUTB%') AND SKG.EnterpriseName not Like ('%L2ESC%') AND SKG.EnterpriseName not Like ('%NWFEE%') ) and  (SKG.SkillTargetID = CTSG.SkillGroupSkillTargetID) and Call_Type.CallTypeID = CTSG.CallTypeID and CTSG.DateTime >=CONVERT(varchar(11),GETDATE()-2,120) and CTSG.DateTime < CONVERT(varchar(11),GETDATE()-1,120)"

    queries4 = [CUIC_OFF_TRANS_1, CUIC_OFF_TRANS_2]

    result4 = []
    for i in queries4:
        cursor_4.execute(i)
        resultC = cursor_4.fetchall()
        result4.append(resultC)
    cursor_4.close()
    print(result4)

    df1.columns = ['offered', 'transferred', 'APPNAME']
    df2.columns = ['offered_1', 'transferred_1', 'APPNAME_1']
    print(df1)
    print(df2)

    a = df1['offered'].values.tolist()
    b = df1['transferred'].values.tolist()
    c = df1['APPNAME'].values.tolist()

    e = df2['offered_1'].values.tolist()
    f = df2['transferred_1'].values.tolist()
    g = df2['APPNAME_1'].values.tolist()


    #string1='AUTO_TV'
    #string2='JIO_MART'
    #string3='MOJO_ONNET'
    #string4=
    #string3=''
    for i in c:
        if i == "AUTO_TV_V2":
        #if string1 in i:
         #if search(string1,i):
            global tv_offered
            global tv_transferred
            tv_offered = a[c.index(i)]
            tv_transferred = b[c.index(i)]
         #elif search(string2, i):
        elif i == "JIO_MART_V2":
        #elif string2 in i:
            global JIO_MART_V2_offered
            JIO_MART_V2_offered = a[c.index(i)]
        elif i == "MOJO_ONNET_V3":
            global MOJO_ONNET_V3_offered
            MOJO_ONNET_V3_offered = a[c.index(i)]
        elif i == "BALANCE_MISSCALL_V1":
            global BALANCE_MISSCALL_V1_offered
            BALANCE_MISSCALL_V1_offered = a[c.index(i)]
        elif i == "MerchantFlow_V4":
            global MerchantFlow_V4_offered
            MerchantFlow_V4_offered = a[c.index(i)]
        elif i == "FTTX_IVR_V2":
            global FTTX_IVR_V2_offered
            global FTTX_IVR_V2_transferred
            FTTX_IVR_V2_offered = a[c.index(i)]
            FTTX_IVR_V2_transferred = b[c.index(i)]
        elif i == "AssistedCareONNET_V5":
            global AssistedCareONNET_V5_offered
            global AssistedCareONNET_V5_transferred
            AssistedCareONNET_V5_offered = a[c.index(i)]
            AssistedCareONNET_V5_transferred = b[c.index(i)]
        elif i == "AssistedCareOFFNET_V5":
            global AssistedCareOFFNET_V5_offered
            global AssistedCareOFFNET_V5_transferred
            AssistedCareOFFNET_V5_offered = a[c.index(i)]
            AssistedCareOFFNET_V5_transferred = b[c.index(i)]

    for i in g:
        if i == "AUTO_TV_V2":
            global tv_offered_1
            global tv_transferred_1
            tv_offered_1 = e[g.index(i)]
            tv_transferred_1 = f[g.index(i)]
        elif i == "JIO_MART_V2":
            global JIO_MART_V2_offered_1
            JIO_MART_V2_offered_1 = e[g.index(i)]
        elif i == "MOJO_ONNET_V3":
            global MOJO_ONNET_V3_offered_1
            MOJO_ONNET_V3_offered_1 = e[g.index(i)]
        elif i == "BALANCE_MISSCALL_V1":
            global BALANCE_MISSCALL_V1_offered_1
            BALANCE_MISSCALL_V1_offered_1 = e[g.index(i)]
        elif i == "MerchantFlow_V4":
            global MerchantFlow_V4_offered_1
            MerchantFlow_V4_offered_1 = e[g.index(i)]
        elif i == "FTTX_IVR_V2":
            global FTTX_IVR_V2_offered_1
            global FTTX_IVR_V2_transferred_1
            FTTX_IVR_V2_offered_1 = e[g.index(i)]
            FTTX_IVR_V2_transferred_1= f[g.index(i)]
        elif i == "AssistedCareONNET_V5":
            global AssistedCareONNET_V5_offered_1
            global AssistedCareONNET_V5_transferred_1
            AssistedCareONNET_V5_offered_1 = e[g.index(i)]
            AssistedCareONNET_V5_transferred_1 = f[g.index(i)]
        elif i == "AssistedCareOFFNET_V5":
            global AssistedCareOFFNET_V5_offered_1
            global AssistedCareOFFNET_V5_transferred_1
            AssistedCareOFFNET_V5_offered_1 = e[g.index(i)]
            AssistedCareOFFNET_V5_transferred_1 = f[g.index(i)]

    print(tv_offered, tv_transferred, JIO_MART_V2_offered, MOJO_ONNET_V3_offered, BALANCE_MISSCALL_V1_offered,result2[2][0][0], result3[2][0][0] )
    print(tv_offered_1, tv_transferred_1, JIO_MART_V2_offered_1, MOJO_ONNET_V3_offered_1, BALANCE_MISSCALL_V1_offered_1,result2[3][0][0],result3[3][0][0])

    SS_OFFERED_D_1 = tv_offered + JIO_MART_V2_offered + MOJO_ONNET_V3_offered +MerchantFlow_V4_offered + result2[2][0][0] + result3[2][0][0]+FTTX_IVR_V2_offered+AssistedCareONNET_V5_offered+AssistedCareOFFNET_V5_offered
    SS_OFFERED_D_2 = tv_offered_1 + JIO_MART_V2_offered_1 + MOJO_ONNET_V3_offered_1 + MerchantFlow_V4_offered_1 +result2[3][0][0] + result3[3][0][0]+FTTX_IVR_V2_offered_1+AssistedCareONNET_V5_offered_1+AssistedCareOFFNET_V5_offered_1
    ACD_ODDERED_1 = result4[0][0][0]+gen_acd_offered_1
    ACD_ODDERED_2 = result4[1][0][0]+gen_acd_offered_2
    ACD_ANSWERED_1 = result4[0][0][1]+gen_diverted_1
    ACD_ANSWERED_2 = result4[1][0][1]+gen_diverted_2
    MISCALL_D_1 = BALANCE_MISSCALL_V1_offered
    MISCALL_D_2 = BALANCE_MISSCALL_V1_offered_1
    IVR_TRANSFERRED_D_1 = result2[0][0][0] + result3[0][0][0] +tv_transferred+FTTX_IVR_V2_transferred+AssistedCareONNET_V5_transferred+AssistedCareOFFNET_V5_transferred

    IVR_TRANSFERRED_D_2 = result2[1][0][0] + result3[1][0][0] +tv_transferred_1+FTTX_IVR_V2_transferred_1+AssistedCareONNET_V5_transferred_1+AssistedCareOFFNET_V5_transferred_1

    DAILY_COUNT = {
        'PARAMETER': ['SS_OFFERED', 'ACD OFFERED', 'ACD ANSWERED', 'BALANCE_MISCALL', 'GENESYS_MOJO', 'IVR_TRANSFERRED',
                      'TV_OFFERED', 'TV_TRANSFERRED'],
        'DAY-1': [SS_OFFERED_D_1, ACD_ODDERED_1, ACD_ANSWERED_1, BALANCE_MISSCALL_V1_offered, MOJO_ONNET_V3_offered,
                  IVR_TRANSFERRED_D_1,
                  tv_offered, tv_transferred],
        'DAY-2': [SS_OFFERED_D_2, ACD_ODDERED_2, ACD_ANSWERED_2, BALANCE_MISSCALL_V1_offered_1, MOJO_ONNET_V3_offered_1,
                  IVR_TRANSFERRED_D_2,
                  tv_offered_1, tv_transferred_1]
    }

    df = pd.DataFrame(DAILY_COUNT, columns=['PARAMETER', 'DAY-1', 'DAY-2'])

    df["DELTA"] = df["DAY-1"] - df["DAY-2"]
    df["DELTA_PERCENTAGE"] = (df["DELTA"] / df["DAY-2"]) * 100

    df_final = df.round(2)
    print(df_final)

    return df_final

main()




def previous_date():
    PREVIOUS_DATE = date.today() - timedelta(days=1)
    print(PREVIOUS_DATE)
    return(PREVIOUS_DATE)

previous_date()





