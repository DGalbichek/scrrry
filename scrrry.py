from __future__ import print_function

#
#   scrrry
#   (a scraping framework)
#
VERSION='0.2.4'
#
#   https://github.com/DGalbichek/scrrry/
#
#   MIT License, Copyright (c) 2017 David Galbicsek
#

from lxml import html

import datetime
import json
import math
import pandas as pd
import re
import requests
import sqlite3
import time

RE_EMAIL = r'''([a-zA-Z0-9\._%+-]+@[a-zA-Z0-9\.-]+(?:\.[a-zA-Z]{2,4})+)'''
RE_PHONE = r'''([0-9\._+()-][0-9\._+() -]{5,}[0-9\._+()-])'''

SURVEY_KEYWORDS = ['email-protection', 'ld+json', 'schema.org']


def algofunctMultiWrapper(tup):
    return [tup[0](tup[1][x]) for x in range(tup[2][0],tup[2][1])]


def survey_page(pagetext):
    """Find recurring characteristics in a page's sourcecode."""
    return [x for x in SURVEY_KEYWORDS if x in str(pagetext)]

	
def decode_email(ee):
    """Decode email protection."""
    try:
        r = int(ee[:2], 16)
        email = ''.join([chr(int(ee[i:i+2], 16) ^ r) for i in range(2, len(ee), 2)])
        return email
    except (ValueError):
        return ''

		
class Scrape_Db():
    def __init__(self,task_name,ver_check=True,multicall=False):
        if not multicall:
            self.tim=[[time.time(),],]
            self.currenttimestamp=datetime.datetime.now().strftime("%c")

        #check version
        if ver_check:
            print('## -= scrrry v'+VERSION+' =-',end=' ')
            try:
                v=requests.get('https://raw.githubusercontent.com/DGalbichek/scrrry/master/scrrry.py').text.split("VERSION='")[1].split("'")[0]
                if v==VERSION:
                    print('(Up to date.)')
                else:
                    print('\n!! current/latest version discrepancy:',VERSION,'/',v)
                    print('!! (https://raw.githubusercontent.com/DGalbichek/scrrry/master/scrrry.py)')
            except:
                print('(Online version check failed.)')

        self.task_name=task_name
        self.db = sqlite3.connect(self.task_name+'-db.sqlite')
        self.cursor = self.db.cursor()
        self.proxylist=[]
        self.proxypos=0

        if not multicall:
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

            if self.getVariable('scrrryMeta')=='---':
                self.setVariable('scrrryMeta',{'versionCreatedWith':VERSION,'creationDateTime':datetime.datetime.now().strftime("%c")})
            self.setVariable('scrrryLog-'+self.currenttimestamp,json.dumps(self.tim))


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


    def getVariable(self,var,novar='---'):
        """Retreives value of variable or returns a default if doesn't exist yet.
        """
        if self.cursor.execute('''SELECT variable_content FROM variables WHERE variable_name=?;''',(var,)).fetchone():
            #print self.cursor.execute('''SELECT variable_content FROM variables WHERE variable_name=?;''',(var,)).fetchone()[0]
            return json.loads(self.cursor.execute('''SELECT variable_content FROM variables WHERE variable_name=?;''',(var,)).fetchone()[0])
        else:
            return novar


    def listVariables(self):
        """Returns list of ALL variables.
        """
        return [x[0] for x in self.cursor.execute('''SELECT variable_name FROM variables;''').fetchall()]




    ##
    ##  MANAGEMENT
    ##
    def tick(self,total=False,display=True,newcycle=False,scripttotal=False,currenttime=False):
        """Timekeeping function.
        """

        if newcycle or scripttotal: # tick in new time segment
            self.tim.append([time.time(),])
        else: # tick in ongoing segment
            self.tim[-1].append(time.time())

        # saving time log
        self.setVariable('scrrryLog-'+self.currenttimestamp,json.dumps(self.tim))

        if total: # segment total
            t=self.tim[-1][-1]-self.tim[-1][0]
        else:
            if len(self.tim[-1])==1 and not scripttotal: # no diff for single value segment
                display=False
            elif scripttotal: # script total
                t=self.tim[-1][-1]-self.tim[0][0]
            else: # time since last tick
                t=self.tim[-1][-1]-self.tim[-1][-2]

        if display:
            th=int(t/3600)
            tm=int(t/60)%60
            ts=t%60
            r = "%ih %im %.2fs" % (th,tm,ts)
        else:
            r= ''

        if currenttime:
            r+=datetime.datetime.now().strftime(" (%c)")
        return r.strip()


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
        def merge_two_dicts(x, y):
            z = x.copy()
            z.update(y)
            return z

        kkk=[]
        g=dict(dic)
        if dic[key]:
            del g[key]
            for s in dic[key]:
                kkk.append(merge_two_dicts(g,s))
        else:
            kkk.append(dic)
        return kkk


    def _dataColumnsAvailable(self,data):
        a=[x.keys() for x in data if x]
        b=set()
        for aa in a:
            for aaa in aa:
                b.add(aaa)
        return list(b)


    def taskCycle(self,algofunct,iterr='def',display={'type':'verbose','freq':1,'tick':0},
                  unfold='',checktodo=False,nosubmit=False,multi={}):
        """Wrapper for gather and scrape tasks. Deals with time and output management.
        """

        # Cycle start
        ti=self.tick(display=False,newcycle=True,currenttime=True)
        if iterr=='def':
            iterr=self.toDo()
        print()
        if 'name' in display.keys() and display['name']: # optional name for cycle
            print('## <[',display['name'],']>',sep=' ')
        print('##',algofunct.__name__,' cycle begins -',len(iterr),'elements -',ti,sep=' ')
        print('##')

        self.tc_ndone,self.tc_nwasdone,self.tc_nskipped,self.tc_nnodata=0,0,0,0
        self.tc_disptype=display['type']
        data=[]

        def preprint(n,it):
            # whether or not display task name
            if display['freq']==1 or (n+1)%display['freq']==0:
                if display['type']=='verbose':
                    print(n+1,it,sep=' ',end=' ')
                elif display['type']=='brief':
                    pass

        def postprint(n,multi=False):
            # outputting prompt of completion
            if not multi:
                if display['type']!='none' and (display['freq']==1 or (n+1)%display['freq']==0):
                    print(pr,end=' ')


            if multi or display['tick']!=0 and n%display['tick']==display['tick']-1:
                if display['type']=='verbose':
                    print('##',self.tick(),sep=' ',end=' ')
                elif display['type']=='brief':
                    perc='['+str(int(float(n+1)/len(iterr)*100))+'%]' # percentage
                    print(str(n+1)+perc+'('+self.tick()+')',end=' ')



        # Core loop - GATHER
        if algofunct.__name__=='gatherTask':
            for n,it in enumerate(iterr):
                preprint(n,it)

                if not checktodo or (checktodo and it not in self.toDo()):
                    pr=algofunct(it)
                else:
                    pr=self.tc_wasdone()

                postprint(n)


        # Core loop - SCRAPE - single thread
        elif algofunct.__name__=='scrapeTask' and not multi:
            for n,it in enumerate(iterr):
                preprint(n,it)

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
                    dii=algofunct(it)
                    if dii:
                        if unfold:
                            for d in self.tc_unfold(dii,unfold):
                                data.append(d)
                        else:
                            data.append(dii)
                        if not nosubmit and dii:
                            self.done(it,dii)
                            pr=self.tc_done()
                        else:
                            pr=self.tc_nodata()
                    else:
                        pr=self.tc_nodata()

                postprint(n)


        # Core loop - SCRAPE - multi thread
        elif algofunct.__name__=='scrapeTask' and multi:
            LIMIT=len(iterr)
            BATCHSIZE=multi['batchsize']
            NOOFPROC=multi['noofproc']
            print('** POOL PARTY! ** '+str(BATCHSIZE)+'/'+str(NOOFPROC),end='')

            from multiprocessing import Pool
            pool = Pool(processes=NOOFPROC)
            #print('limit',LIMIT,'batch',BATCHSIZE,'noofproc',NOOFPROC,'-',math.ceil(LIMIT/BATCHSIZE))
            # batch creation
            for nn in range(int(LIMIT/BATCHSIZE)+1):
                #print(nn,(nn*BATCHSIZE,nn*BATCHSIZE+BATCHSIZE,))
                b1,b2=[],[]
                for rrr in range(NOOFPROC):
                    st=nn*BATCHSIZE+BATCHSIZE/NOOFPROC*rrr
                    if st<LIMIT:
                        b1.append(st)
                        en=nn*BATCHSIZE+BATCHSIZE/NOOFPROC*(rrr+1)
                        if en>LIMIT:
                            b2.append(LIMIT)
                        else:
                            if rrr+1==NOOFPROC:
                                b2.append((nn+1)*BATCHSIZE)
                            else:
                                b2.append(en)
                b1=[int(x) for x in b1]
                b2=[int(x) for x in b2]
                #print(b1,b2)
                print()
                print(list(zip(b1,b2)),end=' === ')

                res=pool.map_async(algofunctMultiWrapper, [(algofunct, iterr, x) for x in zip(b1,b2)])
                for r in res.get():
                    if unfold:
                        for rr in r:
                            for d in self.tc_unfold(rr,unfold):
                                data.append(d)
                    else:
                        for rr in r:
                            data.append(rr)

                postprint(min((nn+1)*BATCHSIZE-1,LIMIT-1), multi=True)


        # Cycle end
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
        print(stats,'-',self.tick(total=True, currenttime=True),sep=' ')

        #list of column names
        if data:
            print('## Available columns:', self._dataColumnsAvailable(data),sep=' ')
        return data


    def unusedColumns(self,data,columns):
        print('\n## Unused columns:', [x for x in self._dataColumnsAvailable(data) if x not in columns])


    def deleteTask(self,uid,feedback=True):
        """Delete specified tasks.
        """
        if not isinstance(uid,list):
            uid=[uid,]
        for uu in uid:
            self.cursor.execute('DELETE FROM scrapedata WHERE scrape_task_uid=?;',(uu,))
            self.db.commit()
            if feedback:
                print('## DELETED',uu,sep=' ')
        return True


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
    def removeBlocks(self,lxmlhtml,blockstoremove=[['<!--', '-->'],['<script', '</script>'],['<style', '</style>']]):
        ht=html.tostring(lxmlhtml)
        for blk in blockstoremove:
            while blk[0] in ht and blk[1] in ht and blk[1] in ht[ht.index(blk[0]):]:
                ht=ht[:ht.index(blk[0])]+ht[ht.index(blk[1],ht.index(blk[0]))+len(blk[1]):]
        return html.fromstring(ht)

        
    def getLDjson(self,lxmlhtml):
        c=lxmlhtml.xpath('//script[@type="application/ld+json"]')
        return c[0].text if c else ''


    def getContactFromText(self,text,what,context=0):
        whatd={'email':RE_EMAIL,'phone':RE_PHONE}
        text=str(text)
        try:
            cd=[x.strip('.- ') for x in set(re.findall(whatd[what],unicode(text)))]
        except:
            cd=[x.strip('.- ') for x in set(re.findall(whatd[what],str(text)))]
        if cd and context>0:
            r=[]
            for c in cd:
                p=text.index(c)
                r.append([c, text[p-context:p+context].replace('\t','').replace('\n','').replace('\r','')])
            return r
        else:
            return [[c,''] for c in cd]


    def emailInText(self,text,context=0):
        """Masking method for backwards comp."""
        return self.getContactFromText(text,'email',context)


    def routineFindings(self,lxmlhtml,what=['ldjson','email','phone'],context=100,exclusionlist=[]):
        findings=[]
        ishtmlelement=isinstance(lxmlhtml,type(html.fromstring('<div></div>')))
        if 'ldjson' in what and ishtmlelement:
            f=self.getLDjson(lxmlhtml)
            if f:
                findings.append({'detail':f,'context':'ld+json'})
        for wh in [w for w in what if w in ['email','phone']]:
            fi=self.getContactFromText('|'.join([x for x in lxmlhtml.itertext()]) if ishtmlelement else lxmlhtml,wh,context)
            if wh=='email' and ishtmlelement:
                fi+=self.getContactFromText(html.tostring(lxmlhtml),'email',context)
            if fi:
                for n,f in enumerate(fi):
                    # check if already in findings or if on the exclusion list
                    if not [x for x in findings if x['detail']==f[0]] and not [x for x in exclusionlist if f[0] in x]:
                        findings.append({'detail':f[0],'context':f[1]})
        if context==0:
            return [x['detail'] for x in findings if x['detail']]
        else:
            return findings


    def stripUrlTracking(self,u):
        rem=['?trk','?ref','?hc_ref','?business_id']
        for r in rem:
            if r in u:
                u=u.split(r)[0]
        return u


    def _get_proxies(self, filtr={
                    #'code':['US','CA'],
                    'https':['yes',],
                    'anonimity':['anonymous','elite proxy']
                    }):
        h=html.fromstring(requests.get('https://free-proxy-list.net/').text)

        proxies=[]

        for row in h.xpath('//div[@class="table-responsive fpl-list"]/table/tbody/tr'):
            proxies.append({})
            try:
                d=[x.text.strip() if x.text else '' for x in row.xpath('.//td')]
            except:
                break
            w=['ip','port','code','country','anonimity','google','https','last checked']
            
            for n,ww in enumerate(w):
                proxies[-1][ww]=d[n]

            if filtr:
                for f in filtr:
                    if proxies[-1][f] not in filtr[f]:
                        proxies.pop(-1)
                        break

        return proxies


    def _requests_with_rotating_proxies(self, url, method='get', headers={}, data={}, timeout=20):
        params={'timeout':timeout}
        if headers:
            params['headers']=headers
        if data:
            params['data']=data

        no_of_proxy_setups = 0
        while True:
            if not self.proxylist:
                self.proxylist = self._get_proxies()
                self.proxypos = 0
                no_of_proxy_setups += 1
            try:
                params['proxies']={
                    'http':'http://'+self.proxylist[self.proxypos]['ip']+':'+self.proxylist[self.proxypos]['port'],
                    'https':'http://'+self.proxylist[self.proxypos]['ip']+':'+self.proxylist[self.proxypos]['port'],
                }

                if method=='get':
                    r=requests.get(url, **params)
                elif method=='post':
                    r=requests.post(url, **params)
                    print('post',r.status_code)

                if r and r.status_code==200:
                    break
                elif r and r.status_code==404:
                    print('!404!',url)
                    return None
                elif r and r.status_code == 500:
                    print('!500!',url)
                    return None

                self.proxypos = (self.proxypos + 1) % len(self.proxylist)
            except:
                r=None
                self.proxylist.pop(self.proxypos)
                if self.proxylist and len(self.proxylist) == self.proxypos:
                    self.proxypos = 0
                print('.',end=' ')
            
            if not self.proxylist and no_of_proxy_setups > 1:
                r=None
                break
        
        return r


    def get_with_rotating_proxies(self, url, headers={}, timeout=20):
        return self._requests_with_rotating_proxies(url, method='get', headers=headers, timeout=timeout)


    def post_with_rotating_proxies(self, url, headers={}, data={}, timeout=20):
        return self._requests_with_rotating_proxies(url, method='post', headers=headers, data=data, timeout=timeout)


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


    def removeTask(self,uid,feedback=True):
        if self.cursor.execute('''SELECT id FROM scrapedata WHERE scrape_task_uid=?;''',(uid,)).fetchone():
            self.cursor.execute('''DELETE FROM scrapedata WHERE scrape_task_uid=?;''',(uid,)).fetchone()
            self.db.commit()
            if feedback:
                print('## '+uid+' REMOVED')
        else:
            if feedback:
                print('## '+uid+' not found!')



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



    def selenium(self,driver,path,header=[],options=[]):
        from selenium import webdriver
        if driver=='chrome':
            from selenium.webdriver.chrome.options import Options
            opts = Options()
            chromeOptions = webdriver.ChromeOptions()
            if header:
                for h in header:
                    opts.add_argument(h+'='+header[h])
            if options:
                if 'incognito' in options:
                    chromeOptions.add_argument("--incognito")
                if 'noimages' in options:
                    prefs = {"profile.managed_default_content_settings.images":2}
                    chromeOptions.add_experimental_option("prefs",prefs)
            return webdriver.Chrome(path,chrome_options=chromeOptions)

        elif driver=='phantomjs':
            service_args=[]
            if options:
                if 'noimages' in options:
                    service_args=['--load-images=no']
            return webdriver.PhantomJS(executable_path=path,service_args=service_args)

        else:
            print('!! Provide valid web driver.')
            return False


    def selenium_waitfor(self,driver,xpath,visibility=False,scrollto=True,report=False,timeout=20):
        c=0
        element=driver.find_elements_by_xpath(xpath)
        while not element:
            element=driver.find_elements_by_xpath(xpath)
            if report:
                print('waiting for',xpath,sep=' ')
            time.sleep(1)
            c+=1
            if c==timeout:
                return None
        c=0
        if visibility:
            while not element[0].is_displayed():
                if report:
                    print('waiting for visibility of',xpath,sep=' ')
                time.sleep(1)
                c+=1
                if c==timeout:
                    return None
        if scrollto:
            driver.execute_script("arguments[0].scrollIntoView();", element[0])
        return element[0]


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
            
