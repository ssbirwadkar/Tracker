import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import os
import shutil
#import seaborn as sns
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import rating as rating
import publishdata as publishdata
import configparser
import io

#Column list
taskcol = ['Task ID','Parent Ticket Type','Parent Ticket ID','Priority','Status','Created Date & Time',
           'Completed Date Time','Task Resolver Group','Resolution Analyst', 'Last Updated Date & Time']
incidentcol = ['Incident','Created Date/Time','Modified Date/Time','Status','Source','Resolution Date/Time',
               'Priority','Team','Owner']
avoidcol = ['ID','Avoided By','Date & Time identified']
inncol = ['Proposer','Implementation Date', 'Implemented']
surcol = ['Analyst','Ticket Type', 'Ticket Number', 'Service score', 'Submitted Date']
projcol = ['Timesheet Owner','Hours Worked', 'Start Date']


#Configuration Information
#datapath = 'C:\\Users\\birwadsa\\PycharmProjects\\PerformanceTracker\\data\\'
datapath = 'E:\\Satish\\PerformanceTracker\\data\\'
resultpath = datapath + 'result\\'
month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
task_resolver_groups = ['Global IT Support Functions','Global Bliss Support','Global Retail Support',
                        'Global Currency Solutions','Global Datacontrol','Global Ecom Support','Global EMEA Apps Support',
                        'Global Foxweb Support','Global Frimley Support','Global Netherlands App Support','Global Service Manager Support',
                        'Global Sharepoint Support','Global TRS Support','Global WNP Application Support']


#Data Load
tk1 = pd.read_csv(datapath + 'TaskYTD.csv', usecols=taskcol)
tempir1 = pd.read_csv(datapath + 'IncidentYTD.csv', usecols=incidentcol)
av1 = pd.read_csv(datapath + 'IncidentAvoidance.csv', usecols=avoidcol)
inn1 = pd.read_csv(datapath + 'Innovation.csv', usecols=inncol)
sur1 = pd.read_csv(datapath + 'Survey.csv', usecols=surcol)
proj1 = pd.read_csv(datapath + 'Project.csv', usecols=projcol)
resourcelist = pd.read_csv(datapath + 'ResourceList.csv')

#Include additional columns in resource list
#resourcelist['ServiceGraph'] = ''
#resourcelist['IncidentGraph'] = ''

#Get all ticket details except incident from task
tk = tk1[tk1['Parent Ticket Type'].isin(['Change','Problem','ServiceReq'])].copy()

#--------------------
#Get incidents which are missing in task extract
dd1 = tempir1.merge(tk1, how='outer', left_on=['Incident','Team','Owner'],
                   right_on=['Parent Ticket ID','Task Resolver Group','Resolution Analyst'],
                   indicator=True).loc[lambda x: x['_merge'] == 'left_only']
#Filter out dataframe with available resource list
dd2 = dd1[dd1['Owner'].isin(resourcelist['ResourceName'])].copy()
#Get selected columns only
dd3 = dd2[['Incident','Status_x','Priority_x','Team','Owner','Created Date/Time','Modified Date/Time','Resolution Date/Time']].copy()
#Create missing columns in incident frame w.r.t task frame
#Dummy task id column
dd3['Task ID'] = dd3.apply((lambda x: str(x['Incident']) + '01'), axis=1)
dd3['Parent Ticket Type'] = 'Incident'
#dd2.apply((lambda x: 'Incident'), axis=1)

#Rename incident frame columns and match with task frame
dd4 = dd3.rename(columns={'Incident': 'Parent Ticket ID', 'Status_x': 'Status', 'Priority_x': 'Priority',
                          'Team': 'Task Resolver Group', 'Owner': 'Resolution Analyst', 'Created Date/Time': 'Created Date & Time',
                          'Modified Date/Time': 'Last Updated Date & Time', 'Resolution Date/Time': 'Completed Date Time'})

#Get Incident data from task
tempir1 = tk1[tk1['Parent Ticket Type'].isin(['Incident'])].copy()
tempir2 = tempir1.drop_duplicates().copy()

#Concat both task and incident data frames to gather all incident data
tempir = pd.concat([tempir2,dd4], axis=0, ignore_index=True)
#tempir.to_csv("C:\\Users\\birwadsa\\PycharmProjects\\PerformanceTracker\\data\\incidentresult.csv")
#--------------------

#Get calculation month and year
curmonth = datetime.now().month
curyear = datetime.now().year

if curmonth == 1:
    #Previous year and month
    cal_month = 12
    cal_year = curyear - 1
else:
    #Current year and last month
    cal_month = curmonth - 1
    cal_year = curyear

def prepare_incident_data(irlist, task_resolver_groups, resourcelist):

    #Derive incident breach and resolution breach date for TAT
    irlist['Resolution Breach to Date'] = irlist.apply(lambda x: datetime.strptime(x['Created Date & Time'], '%d-%m-%Y %H:%M') + timedelta(hours=priority_hours(x['Priority'])), axis=1)
    #Task yet to be completed then consider resolution date as today's date - TAT calculation
    irlist['ResolutionCalDate'] = irlist.apply(lambda x: resdate_calculate(x['Completed Date Time']), axis=1)

    irlist['IsBreach'] = irlist.apply((lambda x: 1 if x['ResolutionCalDate'] > x['Resolution Breach to Date'] else 0), axis=1)

    #Ideal TAT (Incontext of SLA) = Resolution Breach to Date - Created Date & Time
    irlist['TAT Incident Ideal'] = irlist.apply(lambda x: ideal_tat(x['Resolution Breach to Date'],datetime.strptime(x['Created Date & Time'], '%d-%m-%Y %H:%M')), axis=1)
    #Actual TAT = ResolutionCalDate - Created Date & Time
    irlist['TAT Incident Actual'] = irlist.apply(lambda x: actual_tat(x['ResolutionCalDate'], datetime.strptime(x['Created Date & Time'], '%d-%m-%Y %H:%M')), axis=1)
    #TAT calculation
    irlist['TAT'] = irlist.apply(lambda x: x['TAT Incident Ideal'] - x['TAT Incident Actual'], axis=1)

    #print(irlist)

    #Selected group details
    ir = irlist[irlist['Resolution Analyst'].isin(resourcelist['ResourceName'])].copy()
    #ir = irlist[irlist['Task Resolver Group'].isin(task_resolver_groups)].copy()
    #print(ir)
    #day and month value needs to be adjusted first
    ir['Created Date & Time'] = ir['Created Date & Time'].convert_dtypes(convert_string=True)
    ir['Created Date & Time'] = pd.to_datetime(ir['Created Date & Time'], format='%d-%m-%Y %H:%M')

    #Sort both data frames
    #ir.sort_values(by='Incident ID')
    #tk.sort_values(by='Parent Ticket ID')

    #Merge task data with incident data
    #result = pd.merge(ir, tk, how='left', left_on=['Incident ID','Task Resolver Group','Resolution Analyst'],
    #                  right_on=['Parent Ticket ID','Task Resolver Group','Resolution Analyst'])

    # day and month value needs to be adjusted for task creation date
    irlist['Created Date & Time'] = irlist['Created Date & Time'].convert_dtypes(convert_string=True)
    irlist['Created Date & Time'] = pd.to_datetime(irlist['Created Date & Time'], format='%d-%m-%Y %H:%M')

    irlist['CreationDay'] = irlist['Created Date & Time'].dt.day
    irlist['CreationMonth'] = irlist['Created Date & Time'].dt.month
    irlist['CreationYear'] = irlist['Created Date & Time'].dt.year

    #result.to_csv("C:\\Satish\\Reporting Application\\Lead Task\\Functional Review\\Automation\\incidentresult.csv")

    #Filter data for calculation year
    result1 = irlist[irlist['CreationYear'] == cal_year].copy()
    #result1.to_csv("C:\\Users\\birwadsa\\PycharmProjects\\PerformanceTracker\\data\\incidentresult.csv")
    return result1

def prepare_all_service_data(tasks, task_resolver_groups, resourcelist):
    #print(resourcelist['ResourceName'])
    #Selected group details
    taskdetails = tasks[tasks['Resolution Analyst'].isin(resourcelist['ResourceName'])].copy()
    #taskdetails = tasks[tasks['Task Resolver Group'].isin(task_resolver_groups)].copy()

    # day and month value needs to be adjusted first
    taskdetails['Created Date & Time'] = taskdetails['Created Date & Time'].convert_dtypes(convert_string=True)
    taskdetails['Created Date & Time'] = pd.to_datetime(taskdetails['Created Date & Time'], format='%d-%m-%Y %H:%M')

    taskdetails['Completed Date Time'] = taskdetails['Completed Date Time'].convert_dtypes(convert_string=True)
    taskdetails['Completed Date Time'] = pd.to_datetime(taskdetails['Completed Date Time'], format='%d-%m-%Y %H:%M')

    taskdetails.sort_values(by='Task ID')
    taskdetails.sort_values(by='Parent Ticket ID')

    #Create reporting columns day, month and year
    taskdetails['CreationDay'] = taskdetails['Created Date & Time'].dt.day
    taskdetails['CreationMonth'] = taskdetails['Created Date & Time'].dt.month
    taskdetails['CreationYear'] = taskdetails['Created Date & Time'].dt.year

    taskdetails['LastUpdatedDay'] = taskdetails['Completed Date Time'].dt.day
    taskdetails['LastUpdatedMonth'] = taskdetails['Completed Date Time'].dt.strftime('%b')
    taskdetails['LastUpdatedYear'] = taskdetails['Completed Date Time'].dt.year

    #Filter data for calculation year
    taskdetails1 = taskdetails[taskdetails['CreationYear'] == cal_year].copy()

    rq = taskdetails1.groupby(['Task Resolver Group','Resolution Analyst', 'CreationMonth','Parent Ticket Type'])['Task ID'].count().reset_index()
    rq.sort_values(by=['Task Resolver Group','Resolution Analyst','CreationMonth','Parent Ticket Type'])
    #print(rq)
    #rq.sort_values(by=['Resolver Analyst','CreationMonth','Parent Ticket Type'])
    #rq.to_excel('output.xlsx')
    #rq.to_csv("C:\\Users\\birwadsa\\PycharmProjects\\PerformanceTracker\\data\\serviceresult.csv")
    return rq

def incident_avoidance(av, task_resolver_groups, resourcelist):
    #avdetails = av[av['Avoided by Group'].isin(task_resolver_groups)].copy()
    #print(av)
    #print(avdetails)
    #av_members_list = avdetails['Avoided By'].drop_duplicates()

    avdetails = av.rename(columns={'Avoided By': 'Resolution Analyst'})

    avdetails['Date & Time identified'] = avdetails['Date & Time identified'].convert_dtypes(convert_string=True)
    avdetails['Date & Time identified'] = pd.to_datetime(avdetails['Date & Time identified'], format='%d-%m-%Y %H:%M')

    avdetails.sort_values(by='ID')

    #Create reporting columns day, month and year
    avdetails['CreationDay'] = avdetails['Date & Time identified'].dt.day
    avdetails['CreationMonth'] = avdetails['Date & Time identified'].dt.month #strftime('%b')
    avdetails['CreationYear'] = avdetails['Date & Time identified'].dt.year

    #Filter data for calculation year
    avdetails1 = avdetails[avdetails['CreationYear'] == cal_year].copy()

    return avdetails1

def innovation(inn1):

    inn = inn1.rename(columns={'Proposer': 'Resolution Analyst'})

    inn['Implementation Date'] = inn['Implementation Date'].convert_dtypes(convert_string=True)
    inn['Implementation Date'] = pd.to_datetime(inn['Implementation Date'], format='%d-%m-%Y')

    #Create reporting columns day, month and year
    inn['CreationDay'] = inn['Implementation Date'].dt.day
    inn['CreationMonth'] = inn['Implementation Date'].dt.month #strftime('%b')
    inn['CreationYear'] = inn['Implementation Date'].dt.year

    #Filter data for calculation year
    inn1 = inn[inn['CreationYear'] == cal_year].copy()

    return inn1

def project(proj1):

    proj = proj1.rename(columns={'Timesheet Owner': 'Resolution Analyst'})

    proj['Start Date'] = proj['Start Date'].convert_dtypes(convert_string=True)
    proj['Start Date'] = pd.to_datetime(proj['Start Date'], format='%d-%m-%Y')

    #Create reporting columns day, month and year
    proj['CreationDay'] = proj['Start Date'].dt.day
    proj['CreationMonth'] = proj['Start Date'].dt.month #strftime('%b')
    proj['CreationYear'] = proj['Start Date'].dt.year

    #Filter data for calculation year
    proj1 = proj[proj['CreationYear'] == cal_year].copy()

    return proj1

def survey_data(sur, tks, incidents, task_resolver_groups):
    sur['Submitted Date'] = sur['Submitted Date'].convert_dtypes(convert_string=True)
    sur['Submitted Date'] = pd.to_datetime(sur['Submitted Date'], format='%d-%m-%Y %H:%M')

    # Create reporting columns day, month and year
    sur['CreationDay'] = sur['Submitted Date'].dt.day
    sur['CreationMonth'] = sur['Submitted Date'].dt.month  # strftime('%b')
    sur['CreationYear'] = sur['Submitted Date'].dt.year

    #Filter data for calculation year
    sur1 = sur[sur['CreationYear'] == cal_year].copy()

    #Service Request survey
    #Get all request rows where analyst name is blank
    a1 = sur1[sur1['Ticket Type'].isin(['ServiceReq'])].copy()
    sur2 = a1[a1['Analyst'].isnull()].copy()
    #print(sur2)
    t1 = tks[tks['Parent Ticket Type'].isin(['ServiceReq'])].copy()
    t2 = t1[t1['Task Resolver Group'].isin(task_resolver_groups)].copy().reset_index()
    t3 = t2[['Parent Ticket ID','Resolution Analyst']].drop_duplicates()
    p1 = pd.merge(sur2, t2, how='left', left_on=['Ticket Number'], right_on=['Parent Ticket ID'])
    #-------------------------------------------------------------------------------------------
    # Get all incident rows where analyst name is blank
    a1 = sur1[sur1['Ticket Type'].isin(['Incident'])].copy()
    sur2 = a1[a1['Analyst'].isnull()].copy()
    #print(incidents)
    #t1 = tks[tks['Parent Ticket Type'].isin(['Incident'])].copy()
    t2 = incidents[incidents['Task Resolver Group'].isin(task_resolver_groups)].copy()
    t3 = t2[['Parent Ticket ID', 'Resolution Analyst']].drop_duplicates()
    #print(t3)
    p2 = pd.merge(sur2, t2, how='left', left_on=['Ticket Number'], right_on=['Parent Ticket ID'])
    #print(p2)
    p3 = pd.concat([p1, p2])
    return p3

def print_graph(incidents, services, task_resolver_groups):
    #print(services)

    #Incident graph
    # Loop through each team
    #for eachgroup in task_resolver_groups:

    # Team wise data filtered
    team_incidents = incidents[incidents['Task Resolver Group'].isin(task_resolver_groups)].copy()
    team_services = services[services['Task Resolver Group'].isin(task_resolver_groups)].copy()

    # Team member wise data filtered
    incidents_team_members_list = team_incidents['Resolution Analyst'].drop_duplicates()
    services_team_members_list = team_services['Resolution Analyst'].drop_duplicates()

    # Create incident graph of each team member
    for team_member in incidents_team_members_list:
        # Create team member folder
        member_folder = team_member
        parent_folder = resultpath

        path = os.path.join(parent_folder, member_folder)
        if os.path.isdir(path):
            os.chdir(path)
        else:
            os.makedirs(path)

        incidents_team_member_data = team_incidents[team_incidents['Resolution Analyst'] == team_member]
        incidents_team_member_data.groupby('CreationMonth')['Incident ID'].count().plot(kind='bar')
        plt.xlabel('Incident Count')
        plt.ylabel('CreationMonth')
        #piv = incidents_team_member_data.pivot(columns='Resolution Analyst', index='CreationMonth', values='Incident ID')
        #ax = plt.plot(kind='bar')
        #ax.set_xticklabels(month_labels, rotation=45)
        #rects = ax.patches()

        parent_dir = resultpath
        leaf_dir = team_member
        path = os.path.join(parent_dir, leaf_dir)
        os.makedirs(path, exist_ok=True)
        if os.path.isfile(path + '\IncidentGraph.png'):
            os.remove(path + '\IncidentGraph.png')
        plt.savefig(path + '\IncidentGraph.png')

        # Append incident graph to each team member
        pathstr = '<img src="' + path + '\\IncidentGraph.png' + '"/>'
        resourcelist.loc[resourcelist.ResourceName == team_member, 'IncidentGraph'] = pathstr

        # Individual html
        #html = resourcelist.loc[resourcelist.ResourceName == team_member].to_html()
        #text_file = open(path + "\\index.html", "w")
        #text_file.write(html)
        #text_file.close()

    #----------------------------------------------------------------------------------------------------
    # Create service graph of each team member
    for team_member in services_team_members_list:
        #if not pd.isnull(team_member):
        # Create team member folder
        member_folder = team_member
        parent_folder = resultpath
        # Team member folder
        path = os.path.join(parent_folder, member_folder)
        # print(path)
        if os.path.isdir(path):
            os.chdir(path)
        else:
            os.makedirs(path)

        services_team_member_data = team_services[team_services['Resolution Analyst'] == team_member]

        #df = services_team_member_data.groupby(['CreationMonth','Parent Ticket Type'])['Task ID'].sum().unstack()
        #df.columns = df.columns.droplevel()
        #pivot = df.groupby(['CreationMonth']).sum()
        #for type1, pivot_df in pivot:
        #    print(type1)
        #    print(pivot_df)
        services_team_member_data.groupby(['CreationMonth', 'Parent Ticket Type'])['Task ID'].sum().unstack()\
            .reset_index().plot.bar(x='CreationMonth')
        plt.ylabel('Count')
        plt.xlabel('Month')
        plt.text = team_member
        #plt.show()

        parent_dir = resultpath
        leaf_dir = team_member
        path = os.path.join(parent_dir, leaf_dir)
        os.makedirs(path, exist_ok=True)
        if os.path.isfile(path + '\\ServiceGraph.png'):
            os.remove(path + '\\ServiceGraph.png')
        plt.savefig(path + '\\ServiceGraph.png')

        # Append graph of each team member
        pathstr = '<img src="' + path + '\\ServiceGraph.png' + '"/>'
        resourcelist.loc[resourcelist.ResourceName == team_member, 'ServiceGraph'] = pathstr

        # Individual html
        #html = resourcelist.loc[resourcelist.ResourceName == team_member].to_html()
        #text_file = open(path + "\\index.html", "w")
        #text_file.write(html)
        #text_file.close()

        #pivot = services_team_member_data.groupby(['Parent Ticket Type','CreationMonth']).sum()
        #print(pivot)
        #x = pivot.loc[:,'Task ID']
        #print(x)
        #x.plot(kind='bar')
        #plt.show()


        #data1 = services_team_member_data.groupby('Parent Ticket Type')
        #ax = pd.pivot_table(services_team_member_data, index=['Parent Ticket Type','CreationMonth'],
        #                    values=['Parent Ticket Type','CreationMonth','Task ID'], aggfunc=np.sum())
        #services_team_member_data.pivot_table(['Parent Ticket Type', 'CreationMonth'],'Task ID')
        #ax = services_team_member_data.pivot_table(values=['Parent Ticket Type', 'CreationMonth', 'Task ID'], index='Parent Ticket Type')

        #df = pd.DataFrame(services_team_member_data, columns=['Parent Ticket Type','CreationMonth'])
        #ax = df.groupby('Parent Ticket Type')

        #ax.plot(kind='bar', xlim=(0,10))
        #ax = services_team_member_data.groupby(['Parent Ticket Type','CreationMonth'])['Task ID'].count().plot(kind='bar')
        #piv = s1.pivot(columns=['Parent Ticket Type','CreationMonth'], values='Task ID')
        #ax.plot(kind='bar')
        #s1.show()
        #services_team_member_data.to_excel(resultpath + '\\ + ' + team_member + '_output.xlsx')

def final_result(incidents, services, ir_avoided, inno, survcount, projcount, resourcelist):

    #Create resource and month template data frame
    #c = pd.DataFrame({'CreationMonth': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
    #                  'Total': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]})
    c = pd.DataFrame({'CreationMonth': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                      'Total': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]})
    d = resourcelist['ResourceName'].to_frame('Resolution Analyst').reset_index()
    p = c.assign(foo=1).merge(d.assign(foo=1)).drop('foo', 1)

    m = pd.DataFrame({'Type': ['Incident', 'ServiceReq', 'Change', 'Problem', 'IncidentAvoided', 'Innovation', 'Survey',
                               'Project', 'SLA', 'TAT Incident Ideal','TAT Incident Actual', 'TAT']})
    e = p.assign(foo=1).merge(m.assign(foo=1)).drop('foo', 1)
    #print(e)

    # Incident details
    #print(incidents)
    n = incidents.groupby(['Resolution Analyst', 'CreationMonth'])['Task ID'].count().to_frame('Total').reset_index()
    n['Type'] = 'Incident'
    #print(n)

    # Services details
    services.rename(columns={'Parent Ticket Type': 'Type'}, inplace=True)
    r = services.groupby(['Resolution Analyst', 'CreationMonth', 'Type'])['Task ID'].sum().to_frame('Total').reset_index()

    # Incident Avoided details
    ir_avoided['Type'] = 'IncidentAvoided'
    av1 = ir_avoided.groupby(['Resolution Analyst','CreationMonth','Type'])['ID'].count().to_frame('Total').reset_index()

    # Innovation details
    inno['Type'] = 'Innovation'
    inno1 = inno.groupby(['Resolution Analyst','CreationMonth','Type'])['Implemented'].count().to_frame('Total').reset_index()

    # Project details
    projcount['Type'] = 'Project'
    projcount1 = projcount.groupby(['Resolution Analyst','CreationMonth','Type'])['Hours Worked'].sum().to_frame('Total').reset_index()

    # Survey details
    survcount['Type'] = 'Survey'
    s1 = survcount[['Resolution Analyst','CreationMonth', 'Service score', 'Type']]
    surv = s1.groupby(['Resolution Analyst','CreationMonth','Type'])['Service score'].mean().to_frame('Total').reset_index()
    #print(surv)

    # SLA details
    sla = incidents[incidents['IsBreach'] == 1].groupby(['Resolution Analyst', 'CreationMonth'])['Task ID'].count().to_frame('Breach').reset_index()
    #get copy of incident data frame for processing
    s = n[['Resolution Analyst', 'CreationMonth', 'Total']]
    ss = s.rename(columns={'Total': 'TotalCount'})
    slaresult = pd.merge(sla, ss, how='left', left_on=['Resolution Analyst', 'CreationMonth'],
                      right_on=['Resolution Analyst', 'CreationMonth'])

    slaresult['Total'] = slaresult.apply(lambda row: sla_calculate(row), axis=1)
    slaresult['Type'] = 'SLA'
    slafinal = slaresult[['Resolution Analyst', 'CreationMonth', 'Total', 'Type']]
    slafinal['Total'].apply(np.floor)

    #TAT
    # get copy of incident data frame for processing
    irt = incidents.groupby(['Resolution Analyst', 'CreationMonth'])['TAT Incident Ideal'].mean().to_frame('Total').reset_index()
    irt['Type'] = 'TAT Incident Ideal'

    art = incidents.groupby(['Resolution Analyst', 'CreationMonth'])['TAT Incident Actual'].mean().to_frame('Total').reset_index()
    art['Type'] = 'TAT Incident Actual'

    tat = incidents.groupby(['Resolution Analyst', 'CreationMonth'])['TAT'].mean().to_frame(
        'Total').reset_index()
    tat['Type'] = 'TAT'

    #incidents.to_csv("C:\\Users\\birwadsa\\PycharmProjects\\PerformanceTracker\\data\\incidentresult1.csv")

    #Consolidate output
    frames = [e, n, r, av1, inno1, surv, projcount1, slafinal, irt, art, tat]
    f1 = pd.concat(frames)

    f = f1[f1['Resolution Analyst'].isin(resourcelist['ResourceName'])].groupby(['Resolution Analyst', 'CreationMonth', 'Type'])['Total'].sum().astype(int).to_frame('Total').reset_index()

    #f.to_csv("C:\\Users\\birwadsa\\PycharmProjects\\PerformanceTracker\\data\\result1.csv")
    #Get maximum count per type and month
    mx = rating.maxcount(f)
    #mx.to_csv("C:\\Users\\birwadsa\\PycharmProjects\\PerformanceTracker\\data\\result1.csv")
    #Publish data to individual resource via email
    publishdata.sendmail(cal_month, f, mx, resourcelist)

def sla_calculate(row):
    if row['TotalCount'] != 0 or row['TotalCount'] != 'nan':
        return ((row['TotalCount'] - row['Breach'])/row['TotalCount'])*100
    else:
        return 0

def resdate_calculate(rdate):

    #blanck value from excel is treated as float
    if type(rdate) == float:
        # yet to be resolved then current date
        return datetime.now()
    else:
        return datetime.strptime(rdate,'%d-%m-%Y %H:%M')

def priority_hours(p):
    #p = row['Priority']
    switcher = {
        'Travelex P1': 1,
        'Travelex P2': 2,
        'Travelex P3': 24,
        'Travelex P4': 72,
        'Travelex P5': 96,
        'Sainsburys P1': 1,
        'Sainsburys P2': 2,
        'Sainsburys P3': 24,
        'Sainsburys P4': 72,
        'Sainsburys P5': 96
    }
    return switcher.get(p, lambda: 0)

def ideal_tat(rbdate, cdate):
    # in hours
    diff = rbdate - cdate
    return diff.total_seconds()/60**2

def actual_tat(rcdate, cdate):
    # in hours
    diff = rcdate - cdate
    return diff.total_seconds()/60**2

def main():
    incidents = prepare_incident_data(tempir, task_resolver_groups, resourcelist)
    services = prepare_all_service_data(tk, task_resolver_groups, resourcelist)
    ir_avoided = incident_avoidance(av1, task_resolver_groups, resourcelist)
    inno = innovation(inn1)
    projcount = project(proj1)
    survcount = survey_data(sur1, tk, incidents, task_resolver_groups)
    final_result(incidents, services, ir_avoided, inno, survcount, projcount, resourcelist)

if __name__ == "__main__":
    main()