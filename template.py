#
# tested and works with scrrry v0.2.2
#
#

from lxml import html

import os
import pandas as pd
import requests
import scrrry as sc


## init
TASK_NAME=os.path.basename(__file__).replace('.pyc','').replace('.py','')

# all code below is function definition or 'hidden' under an if statement
# to prevent them from running when multi-threading
if __name__ == '__main__':
    scr=sc.Scrape_Db(TASK_NAME)
else:
    scr=sc.Scrape_Db(TASK_NAME,ver_check=False,multicall=True)


## Gathering    ##              ##
##              ##              ##
def gatherTask(it):
    '''This is the algofunct passed in to task cycle management for gathering.
    Has to return scr.addTask()'''
    
    # could do all sorts of things in here, just remember this is for individual tasks
    hh=html.fromstring(requests.get(it).text)

    # only take source code of section that matters
    content=html.tostring(hh.xpath('//whatever the xpath is')[0])
    return scr.addTask(it,ctype='html',content=content)#,testing=True) # enable testing to avoid submitting data


## CYCLE        ##              ##
def gather():
    h=html.fromstring(requests.get('https://whatever the website is').text)
    iterr=[]

    # there's room for all sorts of mumbo-jumbo here just make sure you
    # fill up iterr with the urls that will become your individual tasks
    subpages=[x.attrib['href'] for x in h.xpath('//whatever the xpath is')]
        for sp in subpages:
            hh=html.fromstring(requests.get(sp).text)
            for x in hh.xpath('//whatever the xpath is')]:
                iterr.append(x.attrib['href'])

    # set display options here
    display={'type':'brief','freq':1,'tick':20}
    # and execute the Gather Cycle using the above gatherTask
    scr.taskCycle(gatherTask,iterr,display=display,checktodo=True)


if __name__ == '__main__':
    #gather() # toggle here whether you need gather or not at all
    pass


## Scraping     ##              ##
##              ##              ##
def scrapeTask(p):
    '''This is the algofunct passed in to task cycle management for scraping.
    Has to return a dictionary.'''
    try:
        h=scr.parse_task(p) # loads the raw data
        dat={}
    except:
        print('@#!$%')
        return {}

    # do all your magic here and store your results in the dictionary
    dat['whatever']=h.xpath('//whatever the xpath is')[0].text

    #print('\n',dat)
    return dat


## CYCLE        ##              ##
if __name__ == '__main__':
    # set display options here
    display={'type':'brief','freq':10,'tick':100}
    iterr='def' # iterate over all tasks
    #iterr=scr.toDo()[:1000] # iterate over a subset (ordering is based on when tasks were added)

    #scr.clearAllDone() # this can clear all tasks in case you made some modifications to what you want to extract and need to rerun on all tasks

    # execute the Scrape Cycle using the above scrapeTask
    data=scr.taskCycle(scrapeTask,iterr,display=display) # comment this out to toggle on/off
    #   ,nosubmit=True) # extracted data gets saved in the db and will be used on next run instead of being reextracted again
    #   ,multi={'noofproc':4,'batchsize':200}) # this bit enables multi-threading


# WRITE TO EXCEL
#scr.to_excel() # dumps the sqlite Db in a spreadsheet if need be

# exporting data to a spreadsheet
def writeexcel():
    # specify column names you want to be exported (and their order)
    columns=['whateverfield','whatever']

    print('\n## ... Writing to Excel spreadsheet > '+ TASK_NAME+'.xlsx',end=' ')
    pd.DataFrame(data, columns=columns).to_excel(TASK_NAME+'.xlsx')
    print('Done.')

if __name__ == '__main__':
    writeexcel()

    # WRAPUP
    print('\n##\n## Script total runtime: '+scr.tick(scripttotal=True))
    scr.db.close()
