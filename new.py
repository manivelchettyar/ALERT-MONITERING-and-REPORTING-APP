from __future__ import print_function
import pandas as pd
import cx_Oracle
import pymssql
from datetime import datetime
import schedule
import time
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib

def mains():

    #text='HI ,mani'
    data = {'Name': ['Tom', 'nick', 'krish', 'jack'],
            'Age': [20, 21, 19, 18]}

    # Create DataFrame
    df = pd.DataFrame(data)
    #recipients = ['manivel.chettyar@ril.com', 'Krishna.Balu@ril.com', 'pooja.nayak@ril.com', 'pratik6.singh@ril.com',
                 # 'vinay2.jain@ril.com', 'Shakeeb.Anwar@ril.com', 'Jio.TopsRCCSupport@ril.com']
    recipients = ['manivel.chettyar@ril.com']
    #emaillist = [elem.strip().split(',') for elem in recipients]
    #CC = ['Manoj.Baheti@ril.com', 'Sameer2.Shaikh@ril.com', 'Vinay.Tyagi@ril.com']
    msg = MIMEMultipart()
    msg['Subject'] = "IVR ACD OFFERED TRANSFERRED"
    msg['From'] = 'alert.ivr@ril.com'
    msg['To'] = ", ".join(recipients)
    # msg['Cc']=['Manoj.Baheti@ril.com','Sameer2.Shaikh@ril.com','Vinay.Tyagi@ril.com']
    #msg['Cc'] = ", ".join(CC)
    df1_html = df.to_html()

    html = """\
       <html>
         <head></head>
         <body>
           {0}
         </body>
       </html>
       """.format(df1_html)
    part1 = MIMEText(html, 'html')
    msg.attach(part1)
    server = smtplib.SMTP('smtp.bss.jio.com', 25)
    server.ehlo()
    server.sendmail(msg['From'], recipients, msg.as_string())
    server.quit()

    print('Email sent!')
    print("email sent at",datetime.now())
    print("============================================")

mains()
