scrrry
======

A Python data scraping framework. Works with both 2.7.13 and 3.6.3.

### Intro

I've created this framework and will maintain it to aid my day-to-day data scraping at work. The workflow consists of a *GatherTask* followed by a *ScrapeTask*. The first one is to collect a somewhat trimmed version of the webpages (whatever source really) and store them as individual tasks, while the second one is the *offline* data extraction. ```template.py``` is now included in the repo for reference.

It runs like this:

```
## -= scrrry v0.2.2 =- 

## gatherTask  cycle begins - 44 elements - (Tue Jan 16 13:16:34 2018)
##
+ = + = + + = = + = = = + = + = + + + = 20[45%](0h 0m 16.83s) + + + + + + + + + + + + + + + + + + + + 40[90%](0h 0m 14.21s) + + + + 
##
## gatherTask task cycle complete - t44/=10/+34 - 0h 0m 33.85s (Tue Jan 16 13:17:08 2018)

## scrapeTask  cycle begins - 44 elements - (Tue Jan 16 13:17:08 2018)
##
+ + + + + + + + + + + + + + + + + + + + 20[45%](0h 0m 0.25s) + + + + + + + + + + + + + + + + + + + + 40[90%](0h 0m 0.23s) + + + + 
##
## scrapeTask task cycle complete - t44/+44 - 0h 0m 0.55s (Tue Jan 16 13:17:09 2018)
## ['product', 'company', 'country', 'website']

## ... Writing to Excel spreadsheet > taskname.xlsx

##
## Script total runtime: 0h 0m 37.58s
```

What happened above? The script looked at 44 pages, already had 10, gathered 34. Then scraped all 44, listed all the available columns and exported to a spreadsheet.

### Features:
* sqlite db to store raw data in
* a task management framework that helps with keeping track of what needs doing and how much time things are taking
* 16/1/18 - new in **v0.2.2**: multi-threaded scraping (not for gathering)

### Packages used

* lxml
* Pandas
* requests
* Selenium

---
This code is licensed under the MIT license.
Copyright (c) 2017-2018 David Galbicsek,
See [here](https://github.com/DGalbichek/scrrry/blob/master/LICENSE) for details.
