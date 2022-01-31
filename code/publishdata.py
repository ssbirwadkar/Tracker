import smtplib
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import rating as rating
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

def sendmail(cal_month, f, mx, resourcelist):
    for i in resourcelist.index:
        if resourcelist['Active'][i] == 1:
            g = ''
            g = f[f['Resolution Analyst'] == resourcelist['ResourceName'][i]]
            df = g.groupby(['Resolution Analyst', 'Type', 'CreationMonth'])['Total'].sum().unstack(fill_value=0).reset_index().rename_axis(None, axis=1)
            #Do not include TAT Ideal and Actual in calculation
            #df = g[g['Type'].isin(['TAT Incident Ideal', 'TAT Incident Actual', 'TAT']) == False].groupby(['Resolution Analyst', 'Type', 'CreationMonth'])['Total'].sum().unstack(
            #    fill_value=0).reset_index().rename_axis(None, axis=1)
            getrate = df.copy()

            # Calculate resource rating
            #print(getrate)
            rating.individualrating(cal_month, df, mx, getrate)
            #getrate.to_csv("C:\\Users\\birwadsa\\PycharmProjects\\PerformanceTracker\\data\\result2.csv")
            getrate.loc['Total', :] = getrate.sum(axis=0)
            #getrate.loc['Total',:] = getrate[getrate['Type'].isin(['TAT Incident Ideal', 'TAT Incident Actual', 'TAT']) == False].sum(axis=0)
            #totalrate = getrate[[1,2,3,4,5,6,7,8,9,10,11,12]]
            rows = ['Total']
            getrate.rename(
                columns={1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug',
                         9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}, inplace=True)
            src = getrate[['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']].copy()
            scr1 = src.loc[rows]

            df.rename(
                columns={'Type': 'Task', 1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug',
                         9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}, inplace=True)
            ff = df[['Task', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']].copy()

            #scr1.to_csv("C:\\Users\\birwadsa\\PycharmProjects\\PerformanceTracker\\data\\result2.csv")
            rname = resourcelist['ResourceName'][i]
            rnamearray = rname.split(' ')
            lname = resourcelist['LeadName'][i]
            lnamearray = lname.split(' ')
            mname = resourcelist['ManagerName'][i]

            to = resourcelist['ResourceEmail'][i]
            frm = 'DONOTREPLY_performance.report@travelex.com'
            msg = MIMEMultipart('alternative')
            msg['Subject'] = "Mirror Image"
            msg['From'] = frm
            msg['To'] = resourcelist['ResourceEmail'][i]
            msg['Cc'] = resourcelist['LeadEmail'][i]
            msg['Bcc'] = resourcelist['ManagerEmail'][i]
            cc = resourcelist['LeadEmail'][i]
            bcc = resourcelist['ManagerEmail'][i]
            #image_path = mpimg.imread('C:\\Users\\birwadsa\\PycharmProjects\\PerformanceTracker\\data\\test.png')
            html_string = '''
                                <html>
                                  <head><title>Mirror Image</title></head>
                                  <link rel="stylesheet" type="text/css" href="df_style.css"/>
                                  <body>
                                    <table>
                                    <tr>Hi {toname},</tr>
                                    <tr></tr>
                                    <tr>Thank you for your tremendous efforts and contribution.</tr>
                                    <tr>Monthly KPI numbers are a mirror image of involvement and envisioned to assist in concentrating on improvement areas.</tr>
                                    <tr></tr>
                                    <tr>If you feel like lagging without any hesitation, reach out to {lead} , together we will work out the best possible alternative, </tr>
                                    <tr>and you can expedite and turn weakness to strength.</tr>
                                    <tr></tr>
                                    <tr>Have a look at your performance report below.</tr>
                                    <tr></tr>
                                    <tr></tr>
                                    <tr>Task Wise Count</tr>
                                    <tr>===================================================================================================</tr>
                                    </table>
                                    {table}                                
                                    <table>                                
                                    <tr>---------------------------------------------------------------------------------------------------</tr>
                                    <tr></tr>
                                    <tr>Month Wise Score</tr>                                
                                    </table>
                                    {table1}
                                    <table>
                                    <tr>===================================================================================================</tr>
                                    <tr></tr>
                                    <tr>Thanks & Regards,</tr>
                                    <tr>{manager}</tr>
                                    <tr><img src="cid:Mailtrapimage"></tr>
                                    <tr></tr><tr></tr>
                                    <tr>Please contact your team management with any questions or concerns about this mail.</tr>
                                    <tr>This email was automatically generated by Application Support Team.</tr>
                                    <tr></tr>
                                    </table>
                                  </body>
                                </html>.
                                '''
            html = html_string.format(table=ff.to_html(classes='mystyle'), table1=scr1.to_html(classes='mystyle'),
                                      toname=rnamearray[0], lead=lnamearray[0], manager=mname)
            part = MIMEText(html, 'html')
            msg.attach(part)

            #fp = open('C:\\Users\\birwadsa\\PycharmProjects\\PerformanceTracker\\data\\test.jpg', 'rb')
            fp = open('E:\\Satish\\PerformanceTracker\\data\\test.jpg', 'rb')
            image = MIMEImage(fp.read())
            fp.close()
            image.add_header('Content-ID', '<Mailtrapimage>')
            msg.attach(image)

            # Open mail connection
            server = smtplib.SMTP('145.224.216.21', 25)
            server.connect('145.224.216.21')
            server.sendmail(frm, [to, cc, bcc], msg.as_string())
            server.quit()
            #print(html)
            html_string = ''


