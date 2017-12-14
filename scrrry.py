#
#   scrrry
#   (a scraping framework)
#   version 0.2.1
#
#   https://github.com/DGalbichek/scrrry/
#
#   MIT License, Copyright (c) 2017 David Galbicsek
#

from __future__ import print_function
from lxml import html
import datetime
import json
import pandas as pd
import re
import requests
import sqlite3
import time

RE_EMAIL = r'''([a-zA-Z0-9\._%+-]+@[a-zA-Z0-9\.-]+(?:\.[a-zA-Z]{2,4})+)'''

class Scrape_Db():
    def __init__(self,task_name):
        self.tim=[[time.time(),],]
        self.task_name=task_name
        self.db = sqlite3.connect(self.task_name+'-db.sqlite')
        self.cursor = self.db.cursor()
        try:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS variables( id INTEGER PRIMARY KEY, variable_name TEXT, variable_content TEXT);
                ''')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS scrapedata( id INTEGER PRIMARY KEY, added_date TIMESTAMP,
                        scrape_task_uid TEXT, scrape_task_type TEXT, scrape_task_content TEXT, skip BOOLEAN,
                        scrape_date TIMESTAMP, content TEXT);
                ''')
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise e

    ##
    ##  VARIABLES
    ##
    def setVariable(self,var,val):
        """Either creates variable or updates the value of the one that already exists with the name.
        """
        if not self.cursor.execute('''SELECT variable_name FROM variables WHERE variable_name=?;''',(var,)).fetchone():
            self.cursor.execute('''INSERT INTO variables(variable_name, variable_content)
                                VALUES (?,?);''', (var,json.dumps(val)))
        elif var:
            self.cursor.execute('''UPDATE variables SET variable_content=?
                                WHERE variable_name=?;''', (json.dumps(val),var))
        self.db.commit()


    def getVariable(self,var,val='---'):
        """Retreives value of variable or returns a default if doesn't exist yet.
        """
        if self.cursor.execute('''SELECT variable_content FROM variables WHERE variable_name=?;''',(var,)).fetchone():
            #print self.cursor.execute('''SELECT variable_content FROM variables WHERE variable_name=?;''',(var,)).fetchone()[0]
            return json.loads(self.cursor.execute('''SELECT variable_content FROM variables WHERE variable_name=?;''',(var,)).fetchone()[0])
        else:
            return val


    def listVariables(self):
        """Returns list of ALL variables.
        """
        return [x[0] for x in self.cursor.execute('''SELECT variable_name FROM variables;''').fetchall()]




    ##
    ##  MANAGEMENT
    ##
    def tick(self,total=False,display=True,newcycle=False,scripttotal=False):
        """Timekeeping function.
        """
        if newcycle or scripttotal:
            self.tim.append([time.time(),])
        else:
            self.tim[-1].append(time.time())
        if total:
            t=self.tim[-1][-1]-self.tim[-1][0]
        else:
            if len(self.tim[-1])==1 and not scripttotal:
                t=-5
            elif scripttotal:
                t=self.tim[-1][-1]-self.tim[0][0]
            else:
                t=self.tim[-1][-1]-self.tim[-1][-2]
        th=int(t/3600)
        tm=int(t/60)%60
        ts=t%60
        if display and t!=-5:
            return "%ih %im %.2fs" % (th,tm,ts)
        else:
            return ''


    def tc_done(self,standalone=False):
        if standalone:
            print('DONE',end=' ')
            return
        self.tc_ndone+=1
        if self.tc_disptype=='verbose':
            return 'DONE'+'\n'
        elif self.tc_disptype=='brief':
            return '+'

    def tc_wasdone(self,plus='',standalone=False):
        if standalone:
            print('was already done',end=' ')
            return
        self.tc_nwasdone+=1
        if self.tc_disptype=='verbose':
            return 'was already done '+plus+'\n'
        elif self.tc_disptype=='brief':
            return '='

    def tc_skipped(self,plus='',standalone=False):
        if standalone:
            print('SKIPPED',end=' ')
            return
        self.tc_nskipped+=1
        if self.tc_disptype=='verbose':
            return 'is SKIPPED '+plus+'\n'
        elif self.tc_disptype=='brief':
            return 's'

    def tc_nodata(self,plus='',standalone=False):
        if standalone:
            print('NO DATA',end=' ')
            return
        self.tc_nnodata+=1
        if self.tc_disptype=='verbose':
            return 'NO DATA obtained '+plus+'\n'
        elif self.tc_disptype=='brief':
            return 'X'

    def tc_unfold(self,dic,key):
        kkk=[]
        g=dict(dic)
        if dic[key]:
            del g[key]
            for s in dic[key]:
                kkk.append(dict(g.items()+s.items()))
        else:
            kkk.append(dict(g.items()))
        return kkk


    def taskCycle(self,algofunct,iterr='def',display={'type':'verbose','freq':1,'tick':0},unfold='',checktodo=False,nosubmit=False):
        """Wrapper for gather and scrape tasks. Includes time and output management.
        """
        self.tick(display=False,newcycle=True)
        if iterr=='def':
            iterr=self.toDo()
        print()
        if 'name' in display.keys() and display['name']:
            print('## <[',display['name'],']>',sep=' ')
        print('##',algofunct.__name__,' cycle begins -',len(iterr),'elements -',sep=' ')
        print('##')

        self.tc_ndone,self.tc_nwasdone,self.tc_nskipped,self.tc_nnodata=0,0,0,0
        self.tc_disptype=display['type']
        data=[]

        for n,it in enumerate(iterr):
            if display['freq']==1 or (n+1)%display['freq']==0:
                if display['type']=='verbose':
                    print(n+1,it,sep=' ',end=' ')
                elif display['type']=='brief':
                    pass


            # GATHER
            if algofunct.__name__=='gatherTask':
                if not checktodo or (checktodo and it not in self.toDo()):
                    pr=algofunct(it)
                else:
                    pr=self.tc_wasdone()

            # SCRAPE
            elif algofunct.__name__=='scrapeTask':
                wasdone=self.istoDo(it)
                if wasdone and wasdone!='SKIP':
                    if unfold:
                        for d in self.tc_unfold(self.content(it),unfold):
                            data.append(d)
                    else:
                        data.append(self.content(it))
                    pr=self.tc_wasdone(wasdone)
                elif wasdone=='SKIP':
                    pr=self.tc_skipped(wasdone)
                else:
                    dii=algofunct(it,n)
                    if dii:
                        if unfold:
                            for d in self.tc_unfold(dii,unfold):
                                data.append(d)
                        else:
                            data.append(dii)
                        if not nosubmit:
                            self.done(it,dii)
                            pr=self.tc_done()
                        else:
                            pr=self.tc_nodata()
                    else:
                        pr=self.tc_nodata()

            if display['type']!='none' and (display['freq']==1 or (n+1)%display['freq']==0):
                print(pr,end=' ')


            if display['tick']!=0 and n%display['tick']==display['tick']-1:
                if display['type']=='verbose':
                    print('##',self.tick(),sep=' ',end=' ')
                elif display['type']=='brief':
                    print(str(n+1)+'('+self.tick()+')',end=' ')


        #print 'eo cyc'
        if display['type']=='brief':
            print()
        print('##\n##',algofunct.__name__,'task cycle complete -',sep=' ',end=' ')
        stats='t'+str(len(iterr))
        if self.tc_nwasdone:
            stats+='/='+str(self.tc_nwasdone)
        if self.tc_ndone:
            stats+='/+'+str(self.tc_ndone)
        if self.tc_nskipped:
            stats+='/s'+str(self.tc_nskipped)
        if self.tc_nnodata:
            stats+='/x'+str(self.tc_nnodata)
        print(stats,'-',self.tick(total=True),sep=' ',end=' ')

        #list of column names
        if data:
            a=[x.keys() for x in data]
            b=set()
            for aa in a:
                for aaa in aa:
                    b.add(aaa)
            print('##',list(b),sep=' ')
        return data


    def clearDoneTask(self,uid,feedback=True):
        """Delete specified completed tasks but keeping tha tasks themselves.
        """
        self.cursor.execute('UPDATE scrapedata SET scrape_date=NULL,content=NULL WHERE scrape_task_uid=?;',(uid,))
        self.db.commit()
        if feedback:
            print('## CLEARED',uid,sep=' ')
        return True


    def clearAllDone(self,feedback=True):
        """Delete all completed tasks but keeping tha tasks themselves.
        """
        self.cursor.execute('UPDATE scrapedata SET scrape_date=NULL,content=NULL;')
        self.db.commit()
        if feedback:
            print('## Completed tasks cleared.')
        return True


    def to_excel(self):
        """Dumps scraping database in an excel sheet.
        """
        filename=self.task_name+'-dbexport.xlsx'
        print('## ... Dumping DB to Excel spreadsheet >',filename,sep=' ')

        writer = pd.ExcelWriter(filename, engine='xlsxwriter')

        for ddd in ['scrapedata','variables']:
            data = pd.read_sql("SELECT * FROM "+ddd+";",self.db)
            data.to_excel(writer,sheet_name=self.task_name+ddd)

        writer.save()




    ##
    ##  TOOLS
    ##
    def emailInText(self,text,context=0):
        em=list(set(re.findall(RE_EMAIL,text)))
        r=[]
        if em and context>0:
            for e in em:
                p=text.index(e)
                r.append({'contactemail':e,'emailcontext':text[p-context:p+context]})
            return r
        else:
            return em

    def stripUrlTracking(self,u):
        rem=['?trk','?ref','?hc_ref','?business_id']
        for r in rem:
            if r in u:
                u=u.split(r)[0]
        return u



    ##
    ##  GATHERING
    ##
    def addTask(self,uid,ctype='url',content='',testing=False,standalone=False):
        """Adds task and optional content to the scraping todo list.
        """
        if testing:
            return self.tc_nodata(standalone=standalone)
        elif self.cursor.execute('''SELECT id FROM scrapedata WHERE scrape_task_uid=?;''',(uid,)).fetchone():
            return self.tc_wasdone(standalone=standalone)
        elif uid:
            self.cursor.execute('''INSERT INTO scrapedata(added_date,scrape_task_uid,scrape_task_type,scrape_task_content,skip)
                                VALUES (?,?,?,?,?);''', (datetime.datetime.now(),uid,ctype,content,False))
            self.db.commit()
            return self.tc_done(standalone=standalone)









    ##
    ##  SCRAPING
    ##

    def setSkip(self,uid,boo):
        """Switch whether a task should be skipped.
        """
        self.cursor.execute('UPDATE scrapedata SET skip=? WHERE scrape_task_uid=?;', (boo,uid))
        self.db.commit()

    def istoDo(self,uid):
        """Checks if task is to be skipped and if it was done or not and returns date string or an empty one if not done yet.
        """
        wasdoneorskip=self.cursor.execute('''SELECT scrape_date,skip FROM scrapedata WHERE scrape_task_uid=?;''',(uid,)).fetchone()
        #print wasdoneorskip
        if bool(wasdoneorskip[1]):
            return 'SKIP'
        else:
            if wasdoneorskip[0]:
                return wasdoneorskip[0].split(' ')[0]
            else:
                return ''

    def content(self,uid):
        """Returns dumped data for given uid in dict format.
        """
        return json.loads(self.cursor.execute('''SELECT content FROM scrapedata WHERE scrape_task_uid=?;''',(uid,)).fetchone()[0])

    def rawContent(self,uid):
        """Returns raw data for given uid.
        """
        return self.cursor.execute('''SELECT scrape_task_content FROM scrapedata WHERE scrape_task_uid=?;''',(uid,)).fetchone()[0]

    def parse_content(self,uid):
        return html.fromstring(self.rawContent(uid))



    def toDo(self):
        """Returns list of ALL tasks.
        """
        return [x[0] for x in self.cursor.execute('''SELECT scrape_task_uid FROM scrapedata;''').fetchall()]

    def done(self,uid,content):
        """Add gathered data dict to an existing task.
        """
        self.cursor.execute('UPDATE scrapedata SET scrape_date=?,content=? WHERE scrape_task_uid=?;',
                            (datetime.datetime.now(),json.dumps(content),uid))
        self.db.commit()



    def selenium(self,driver,header=[],options=[]):
        from selenium import webdriver
        if driver=='chrome':
            from selenium.webdriver.chrome.options import Options
            opts = Options()
            chromeOptions = webdriver.ChromeOptions()
            if header:
                for h in header:
                    opts.add_argument(h+'='+header[h])
            if options:
                if 'noimages' in options:
                    prefs = {"profile.managed_default_content_settings.images":2}
                    chromeOptions.add_experimental_option("prefs",prefs)
            return webdriver.Chrome(r'C:\Users\d.galbicsek\pywork\chromedriver.exe',chrome_options=chromeOptions)

        elif driver=='phantomjs':
            service_args=[]
            if options:
                if 'noimages' in options:
                    service_args=['--load-images=no']
            return webdriver.PhantomJS(executable_path=r'C:\Users\d.galbicsek\pywork\phantomjs-2.1.1-windows\bin\phantomjs.exe',service_args=service_args)

        else:
            print('!! Provide valid web driver.')
            return False

    def selenium_waitfor(self,driver,xpath,visibility=False):
        while not driver.find_elements_by_xpath(xpath):
            print('waiting for',xpath,sep=' ')
            time.sleep(1)
        if visibility:
            while not driver.find_elements_by_xpath(xpath)[0].is_displayed():
                print('waiting for visibility of',xpath,sep=' ')
                time.sleep(1)


    def parse_page(self,uid):
        """By default parses page first by trying to do it with lxml.thml.parse,
        then with lxml.html.fromstring(requests.get).
        """
        try:
            return html.parse(uid)
        except:
            return html.fromstring(requests.get(uid).text)


    def parse_task(self,uid):
        """Parses task content from url or stored html/json, depending on task's type.
        scrape_task_type is set by ctype param of addTask
        currently supported values are: url, html and json
        """
        dtype = self.cursor.execute('''SELECT scrape_task_type FROM scrapedata WHERE scrape_task_uid=?;''',(uid,)).fetchone()[0]
        if dtype=='url':
            return self.parse_page(uid)
        elif dtype=='html':
            return self.parse_content(uid)
        elif dtype=='json':
            return json.loads(self.rawContent(uid))
        else:
            print('Unsupported task type! (',dtype,')',sep=' ')

##########
##########


