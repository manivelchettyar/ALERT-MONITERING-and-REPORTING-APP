from testing import *
from flask import *
import pymssql
import cx_Oracle

app = Flask(__name__)


@app.route('/DAILYCOUNT')

def MANIVEL(): return render_template('view.html', tables=[main().to_html()],previous_date=previous_date())

@app.route('/CPS')
def CPS():
    server5 = '10.141.42.220:1433'
    database5 = 'ListDB'
    username5 = 'Genesys_Report'
    password5 = 'Genesys@1234'
    con5 = pymssql.connect(server5, username5, password5, database5)
    cursor_5 = con5.cursor()
    query_5 = "Select (case when b.name = 'SIPSwitch01' then 'AHM' when b.name = 'SIPSwitch08' then 'BLR2' when b.name = 'SIPSwitch02' then 'BLR' when b.name = 'SIPSwitch03' then 'MOH' when b.name = 'SIPSwitch04' then 'HYD' when b.name = 'SIPSwitch05' then 'KOL' when b.name = 'SIPSwitch06' then 'MUM' when b.name = 'SIPSwitch07' then 'NOI' else b.name end) Circle ,count(1)[Total], sum(case when call_result=13 then 1 else 0 end )[Dial_Error], sum(case when Call_Result=35 then 1 else 0 end )[No_DialTone], sum(case when call_result=37 then 1 else 0 end )[No_Ring Back Tone], sum(case when call_result=7 then 1 else 0 end )[No Answer],sum(case when call_result=6 then 1 else 0 end )[Busy], sum(case when call_result=33 then 1 else 0 end )[Answered], sum(case when call_result=14 then 1 else 0 end )[SIT Unknown], sum(case when  call_result=3 then 1 else 0 end )[General Error], sum(case when call_result=4 then 1 else 0 end )[System Error], sum(case when call_result=11 then 1 else 0 end )[Invalid Number] from  TRG_JIO_CALLHISTORY(nolock) a inner join ConfigDB..cfg_switch(Nolock) b on a.switch_id = b.dbid where  Call_date >= DATEADD(MINUTE, -5, CONVERT(DATETIME, CONVERT(VARCHAR(16), GETDATE(), 120) + ':00:00'))  group by  (case when b.name = 'SIPSwitch01' then 'AHM' when b.name = 'SIPSwitch08' then 'BLR2' when b.name = 'SIPSwitch02' then 'BLR' when b.name = 'SIPSwitch03' then 'MOH' when b.name = 'SIPSwitch04' then 'HYD' when b.name = 'SIPSwitch05' then 'KOL' when b.name = 'SIPSwitch06' then 'MUM' when b.name = 'SIPSwitch07' then 'NOI' else b.name end)  order by 1"
    cursor_5.execute(query_5)
    result5 = pd.DataFrame(cursor_5.fetchall())
    cursor_5.close()
    result5.columns = ['CIRCLE', 'TOTAL', 'DIAL_ERROR', 'NO_DIAL_TONE', 'NO_RING_BACK', 'NO_ANS', 'BUSY', 'ANSWERED',
                       'SIT_ERROR', 'GENERAL_ERROR', 'SYSTEM_ERROR', 'INVALID_NUMBER']
    result5["CPS"] = result5["TOTAL"] / 300

    print("CPS Calculated")
    return render_template('view.html', tables=[result5.to_html()], previous_date=previous_date())


@app.route('/HOUR')
def HOUR():
    server6 = '10.141.42.220:1433'
    database6 = 'ListDB'
    username6 = 'Genesys_Report'
    password6 = 'Genesys@1234'
    con6 = pymssql.connect(server6, username6, password6, database6)
    cursor_6 = con6.cursor()
    query_6 = "select DATEPART(HH,Call_Date) as HOUR,COUNT(1) as COUNT from TRG_JIO_CALLHISTORY(nolock) where Call_Date>=cast(getdate() as Date) group by DATEPART(HH,Call_Date) order by 1"
    cursor_6.execute(query_6)
    result6 = pd.DataFrame(cursor_6.fetchall())
    result6.columns=["hour","count"]
    cursor_6.close()
    print("HOUR Calculated")
    return render_template('view.html', tables=[result6.to_html()], previous_date=previous_date())




#def cps(): return render_template('view.html', tables=[CPS().to_html()],previous_date=previous_date())

if __name__ == '__main__':
  app.run(host='10.141.52.105',port='8080',debug="true")



