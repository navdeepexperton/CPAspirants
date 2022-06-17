from flask import Flask,redirect
from datetime import datetime
from datetime import timedelta
import numpy as np
import pandas as pd
import gspread
import json
import requests
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
app = Flask(__name__)
@app.route('/')
def home():
    #today's date
    today = datetime.today()
    #considering yesterday as a lag of 3 days from today (n_help)
    yesterday = today - timedelta(days=3)
    #changing format of today
    today1 = today.strftime("%Y-%m-%d %H:%M:%S")
    #changing format of yesterday
    yesterday1 = yesterday.strftime("%Y-%m-%d %H:%M:%S")
    #Creating new dataframe named Lead
    Lead = pd.DataFrame()
    #For loop for appending the data in columns in dataframe Lead
    for i in range(1,5):
        #key and URL for accessing the data for LSQ
        lead_squared_access_key = 'u$r9d756da850aa2e17ae61f497b2bc99c3'
        lead_squared_secret_key = '2b8db837b12d22937d2e95e2cb257af189ab0342'
        url = 'https://api-in21.leadsquared.com/v2/LeadManagement.svc/Leads.RecentlyModified?accessKey=%s&secretKey=%s' % (
        lead_squared_access_key, lead_squared_secret_key)
        headers = {"Content-Type": "application/json; charset=utf-8"}
        #pre assign variables(parameters) for requests.post function
        dic = {
            "Parameter": {
                "FromDate": yesterday1,
                "ToDate": today1
            },
            "Columns": {
                "Include_CSV": "FirstName,LastName,EmailAddress,Phone,CreatedOn,mx_CP_Code,mx_CP_Name,ProspectStage,mx_Last_Activity_Sub_Outcome,mx_Last_Activity_Outcome,mx_Activity_Disposition,mx_City,mx_Career_Stage,mx_Preferred_domain,mx_Date_of_Birth,mx_Highest_Education_Qualification,mx_Work_Experience,mx_Which_organization_do_you_see_yourself_working_for,mx_Which_Job_role_do_you_want,mx_Which_of_the_following_factors_you_need_help_with,mx_How_many_interviews_have_you_attended,mx_Device_for_training,mx_Rate_your_Proficiency_in_the_English_Language,mx_English_Test,mx_Can_You_Please_Upload_Your_Updated_Resume,mx_Final_English_Test_Result,mx_Written_Question,mx_Introduce_Yourself_in_2_3_Line,mx_Verbal_Test_Script,mx_Verbal_Test_Audio,mx_Verbal_Question,mx_Overall_Score"
            },
            #n_help- why we are running this for 4 pages
            "Paging": {
                "PageIndex": i,
                "PageSize": 5000
            }

        }
        #requesting data from the web page(url) with paramaters explained above
        res = requests.post(url, headers=headers, json=dic)
        #converting x1 in json format
        x1 = res.json()
        #chossing leads column from json data
        x = x1['Leads']
        #checking whether the list is empty the code wont run and if it contains data then it will go to else condition
        if not x:
            print(f"list is  empty{i}")
        # 11 lists are created for append
        else:
            lst = []
            lst1 = []
            lst2 = []
            lst3 = []
            lst4 = []
            lst5 = []
            lst6 = []
            lst7 = []
            lst8 = []
            lst9 = []
            lst10 = []
            lst11 = []
            lst12 = [] #1
            lst13 = []
            lst14 = []
            lst15 = []
            lst16 = []
            lst17 = []
            lst18 = []
            lst19 = []
            lst20 = []
            lst21 = []
            lst22 = []
            lst23 = []
            lst24 = []
            lst25 = []
            lst26 = []
            lst27 = []
            lst28 = []
            lst29 = []
            lst30 = []
            lst31 = []
            lst32 = []
            #appending data from values to the lists (selecting top 10 values according to the schema)
            for i in range(len(x)):
                #FIRST two line use to select first(i) collection of entry
                y = x[i]
                y = y['LeadPropertyList']
                #append all the entry present in each collectiobn
                k = y[0]
                k = k['Value']
                lst.append(k)
                l = y[1]
                l = l['Value']
                lst1.append(l)
                m = y[2]
                m = m['Value']
                lst2.append(m)
                n = y[3]
                n = n['Value']
                lst3.append(n)
                o = y[4]
                o = o['Value']
                lst4.append(o)
                p = y[5]
                p = p['Value']
                lst5.append(p)
                q = y[6]
                q = q['Value']
                lst6.append(q)
                r = y[7]
                r = r['Value']
                lst7.append(r)
                s = y[8]
                s = s['Value']
                lst8.append(s)
                t = y[9]
                t = t['Value']
                lst9.append(t)
                u = y[10]
                u = u['Value']
                lst10.append(u)
                a = y[11]
                a = a['Value']
                lst11.append(a)
                b = y[12]
                b = b['Value']
                lst12.append(b)
                c = y[13]
                c = c['Value']
                lst13.append(c)
                d = y[14]
                d = d['Value']
                lst14.append(d)
                e = y[15]
                e = e['Value']
                lst15.append(e)
                f = y[12]
                f = f['Value']
                lst16.append(f)
                g = y[17]
                g = g['Value']
                lst17.append(g)
                h = y[18]
                h = h['Value']
                lst18.append(h)
                i = y[19]
                i = i['Value']
                lst19.append(i)
                j = y[20]
                j = j['Value']
                lst20.append(j)
                v = y[21]
                v = v['Value']
                lst21.append(v)
                w = y[22]
                w = w['Value']
                lst22.append(w)
                ah = y[23]
                ah = ah['Value']
                lst23.append(ah)
                ai = y[24]
                ai = ai['Value']
                lst24.append(ai)
                z = y[25]
                z = z['Value']
                lst25.append(z)
                aa = y[26]
                aa = aa['Value']
                lst26.append(aa)
                ab = y[27]
                ab = ab['Value']
                lst27.append(ab)
                ac = y[28]
                ac = ac['Value']
                lst28.append(ac)
                ad = y[29]
                ad = ad['Value']
                lst29.append(ad)
                ae = y[30]
                ae = ae['Value']
                lst30.append(ae)
                af = y[31]
                af = af['Value']
                lst31.append(af)
                ag = y[32]
                ag = ag['Value']
                lst32.append(ag)
            #creating a dataframe and adding the appended lists to the respective columns where lists are features
            LSQ = pd.DataFrame(list(zip(lst, lst1, lst2, lst3, lst4, lst5, lst6, lst7, lst8, lst9, lst10,lst11,lst12,lst13,lst14,lst15,lst16,lst17,lst18,lst19,lst20,lst21,lst22,lst23,lst24,lst25,lst26,lst27,lst28,lst29,lst30,lst31)),
                                columns=['FirstName', 'LastName', 'EmailAddress', 'Mobile Number', 'Created On',
                                        'CP_Code', 'CP Name', 'Lead Stage', 'Last_Activity_Sub_Outcome',
                                        'Last_Activity_Outcome', 'Activity_Disposition', 'City', 'Career Stage',
                                         'Your Preferred Domain to land a job', 'Date of Birth', 'Highest Education Qualification',
                                         'Work Experience', 'Which organisation do you see yourself working for',
                                         'Which Job role do you want', 'Which of the following factors you need help with',
                                         'How many interviews have you attended', 'Device for Training', 'Rate your proficiency in the English language',
                                         'English Test', 'Can you please upload your updated Resume', 'Final English Test Result', 'Written Question',
                                         'Introduce yourself in 2-3 lines', 'Verbal Test Script', 'Verbal Test Audio', 'Verbal Question', 'Overall Score'])
            #if lastname has null value then fille it with ''
            LSQ['LastName'].fillna('', inplace=True)
            #creating a full name column with first name and last name column
            LSQ['Full Name'] = LSQ['FirstName'] + LSQ['LastName']
            #dropping columns First Name and Last name
            LSQ.drop(['FirstName', 'LastName'], inplace=True, axis=1)
            #only taking 10 columns total from LSQ and rearranging the columns
            LSQ = LSQ[['Full Name', 'EmailAddress', 'Mobile Number', 'CP_Code', 'CP Name', 'Lead Stage',
                        'Last_Activity_Sub_Outcome', 'Last_Activity_Outcome', 'Activity_Disposition', 'Created On','City', 'Career Stage',
                                         'Your Preferred Domain to land a job', 'Date of Birth', 'Highest Education Qualification',
                                         'Work Experience', 'Which organisation do you see yourself working for',
                                         'Which Job role do you want', 'Which of the following factors you need help with',
                                         'How many interviews have you attended', 'Device for Training', 'Rate your proficiency in the English language',
                                         'English Test', 'Can you please upload your updated Resume', 'Final English Test Result', 'Written Question',
                                         'Introduce yourself in 2-3 lines', 'Verbal Test Script', 'Verbal Test Audio', 'Verbal Question', 'Overall Score']]
            #Concating Lead Datframe with LSQ Dataframe
            Lead = pd.concat([Lead, LSQ])
    #Removing NAT values from 'created on' column
    Lead['Created On'] = pd.to_datetime(Lead['Created On'], format='%Y/%m/%d %H:%M:%S')
    #removing entries where null values of CP codes are present
    Lead = Lead[Lead['CP_Code'].isnull() == False]
    #Filling Null values with "Fresh Lead" of the LIST lst
    lst = ['Activity_Disposition', 'Last_Activity_Outcome', 'Last_Activity_Sub_Outcome']
    for i in lst:
        Lead[i].fillna('Fresh Lead', inplace=True)
    #Reseting index of the dataframe Lead
    Lead.reset_index(inplace=True)
    #Dropping "Index" from the dataframe Lead
    Lead.drop('index', inplace=True, axis=1)

    #Connecting Google sheets by API .Here,credentials is (sample.json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_file('sample.json', scopes=scopes)
    gc = gspread.authorize(credentials)

    #Importing all the data from google sheets
    spreadsheet = gc.open_by_key('13U5SZo9a5K29o-XculKFFUJ-USEkZ_8YXlwGadmN7E8')
    worksheet = spreadsheet.worksheet("CP Order User Journey")
    CP = pd.DataFrame(worksheet.get_all_records())

    #Merging Lead and CP dataframe on basis of mobile number(outer join)
    Lead = Lead.merge(CP, on='Mobile Number', how="outer")

    #After merging the x values contains new updates where as the y values contains already stored disposition.
    #In the below code, if the x value is null then that means there was no update on that lead
    #so we replace it wiht the existing data
    Lead['Full Name_x'] = Lead['Full Name_x'].fillna(Lead['Full Name_y'])
    Lead['EmailAddress_x'] = Lead['EmailAddress_x'].fillna(Lead['EmailAddress_y'])
    Lead['CP_Code_x'] = Lead['CP_Code_x'].fillna(Lead['CP_Code_y'])
    Lead['CP Name_x'] = Lead['CP Name_x'].fillna(Lead['CP Name_y'])
    Lead['Lead Stage_x'] = Lead['Lead Stage_x'].fillna(Lead['Lead Stage_y'])
    Lead['Last_Activity_Sub_Outcome_x'] = Lead['Last_Activity_Sub_Outcome_x'].fillna(
        Lead['Last_Activity_Sub_Outcome_y'])
    Lead['Last_Activity_Outcome_x'] = Lead['Last_Activity_Outcome_x'].fillna(Lead['Last_Activity_Outcome_y'])
    Lead['Activity_Disposition_x'] = Lead['Activity_Disposition_x'].fillna(Lead['Activity_Disposition_y'])
    Lead['Created On_x'] = Lead['Created On_x'].fillna(Lead['Created On_y'])
    Lead['City_x'] = Lead['City_x'].fillna(Lead['City_y'])
    Lead['Career Stage_x'] = Lead['Career Stage_x'].fillna(Lead['Career Stage_y'])
    Lead['Your Preferred Domain to land a job_x'] = Lead['Your Preferred Domain to land a job_x'].fillna(Lead['Your Preferred Domain to land a job_y'])
    Lead['Date of Birth_x'] = Lead['Date of Birth_x'].fillna(Lead['Date of Birth_y'])
    Lead['Highest Education Qualification_x'] = Lead['Highest Education Qualification_x'].fillna(Lead['Highest Education Qualification_y'])
    Lead['Work Experience_x'] = Lead['Work Experience_x'].fillna(Lead['Work Experience_y'])
    Lead['Which organisation do you see yourself working for_x'] = Lead['Which organisation do you see yourself working for_x'].fillna(Lead['Which organisation do you see yourself working for_y'])
    Lead['Which Job role do you want_x'] = Lead['Which Job role do you want_x'].fillna(Lead['Which Job role do you want_y'])
    Lead['Which of the following factors you need help with_x'] = Lead['Which of the following factors you need help with_x'].fillna(Lead['Which of the following factors you need help with_y'])
    Lead['How many interviews have you attended_x'] = Lead['How many interviews have you attended_x'].fillna(Lead['How many interviews have you attended_y'])
    Lead['Device for Training_x'] = Lead['Device for Training_x'].fillna(Lead['Device for Training_y'])
    Lead['Rate your proficiency in the English language_x'] = Lead['Rate your proficiency in the English language_x'].fillna(Lead['Rate your proficiency in the English language_y'])
    Lead['English Test_x'] = Lead['English Test_x'].fillna(Lead['English Test_y'])
    Lead['Can you please upload your updated Resume_x'] = Lead['Can you please upload your updated Resume_x'].fillna(Lead['Can you please upload your updated Resume_y'])
    Lead['Final English Test Result_x'] = Lead['Final English Test Result_x'].fillna(Lead['Final English Test Result_y'])
    Lead['Written Question_x'] = Lead['Written Question_x'].fillna(Lead['Written Question_y'])
    Lead['Introduce yourself in 2-3 lines_x'] = Lead['Introduce yourself in 2-3 lines_x'].fillna(Lead['Introduce yourself in 2-3 lines_y'])
    Lead['Verbal Test Script_x'] = Lead['Verbal Test Script_x'].fillna(Lead['Verbal Test Script_y'])
    Lead['Verbal Test Audio_x'] = Lead['Verbal Test Audio_x'].fillna(Lead['Verbal Test Audio_y'])
    Lead['Verbal Question_x'] = Lead['Verbal Question_x'].fillna(Lead['Verbal Question_y'])
    Lead['Overall Score_x'] = Lead['Overall Score_x'].fillna(Lead['Overall Score_y'])


    #Renaming the columns for uniformity
    Lead.rename(columns={'Full Name_x': 'Full Name', 'EmailAddress_x': 'EmailAddress', 'CP_Code_x': 'CP_Code',
                          'CP Name_x': 'CP Name', 'Lead Stage_x': 'Lead Stage',
                          'Last_Activity_Sub_Outcome_x': 'Last_Activity_Sub_Outcome',
                          'Last_Activity_Outcome_x': 'Last_Activity_Outcome',
                          'Activity_Disposition_x': 'Activity_Disposition', 'Created On_x': 'Created On','City_x':'City', 'Career Stage_x':'Career Stage',
                         'Your Preferred Domain to land a job_x':'Your Preferred Domain to land a job', 'Date of Birth_x':'Date of Birth',
                         'Highest Education Qualification_x':'Highest Education Qualification', 'Work Experience_x':'Work Experience',
                         'Which organisation do you see yourself working for_x':'Which organisation do you see yourself working for',
                         'Which Job role do you want_x':'Which Job role do you want',
                         'Which of the following factors you need help with_x':'Which of the following factors you need help with',
                         'How many interviews have you attended_x':'How many interviews have you attended', 'Device for Training_x':'Device for Training',
                         'Rate your proficiency in the English language_x':'Rate your proficiency in the English language', 'English Test_x':'English Test',
                          'Can you please upload your updated Resume_x':'Can you please upload your updated Resume', 'Final English Test Result_x':'Final English Test Result',
                          'Written Question_x':'Written Question', 'Introduce yourself in 2-3 lines_x':'Introduce yourself in 2-3 lines', 'Verbal Test Script_x':'Verbal Test Script',
                         'Verbal Test Audio_x':'Verbal Test Audio', 'Verbal Question_x':'Verbal Question', 'Overall Score_x':'Overall Score' }, inplace=True)

    #droping CP dataframe columns
    Lead.drop(['Full Name_y', 'EmailAddress_y', 'CP_Code_y','CP Name_y','Lead Stage_y','Last_Activity_Sub_Outcome_y','Last_Activity_Outcome_y','Activity_Disposition_y', 'Created On_y','City_y', 'Career Stage_y',
                    'Your Preferred Domain to land a job_y', 'Date of Birth_y', 'Highest Education Qualification_y', 'Work Experience_y', 'Which organisation do you see yourself working for_y',
                'Which Job role do you want_y', 'Which of the following factors you need help with_y', 'How many interviews have you attended_y', 'Device for Training_y', 'Rate your proficiency in the English language_y', 'English Test_y',
                'Can you please upload your updated Resume_y', 'Final English Test Result_y', 'Written Question_y', 'Introduce yourself in 2-3 lines_y', 'Verbal Test Script_y', 'Verbal Test Audio_y', 'Verbal Question_y', 'Overall Score_y'], axis=1, inplace=True)
    for _ in range(100):
      Lead = Lead.append(pd.Series(), ignore_index=True)
    #Coverting Lead Dataframe to csv file
    Lead.to_csv('Lead.csv', index=False)
    #using rb mode to read non-text file
    with open('Lead.csv', 'rb') as file_obj:
        #saving file in content mode
        content = file_obj.read()
        #importing data into the spreadsheet CP LEAD <> LSQ
        gc.import_csv(spreadsheet.id, data=content)
    return redirect("https://docs.google.com/spreadsheets/d/1Bbv1NmnAxXTC8Gm4A10hLcQGtYWsaHYgHmGbPHplbog/edit#gid=0")
if __name__ == "__main__":
    app.run(debug=True)
