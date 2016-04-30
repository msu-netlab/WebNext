#!/usr/bin/python

from lxml import html
import mysql.connector as mariadb
import sys

ListIP = "select ip, count(1) from `dns` group by 1 order by 2"
DataByIP = "SELECT hour, min, SUBSTRING(domain, LOCATE('.', domain) + 1, LENGTH(domain) - LOCATE('.', domain)) from `dns` where ip='%s' group by hour, min, SUBSTRING(domain, LOCATE('.', domain) + 1, LENGTH(domain) - LOCATE('.', domain)) order by hour, min, sec, msec"

DataMorning = "select distinct ip from `new_data_5am_to_8am` where SUBSTRING_INDEX(new_domain, '.', -1) not in ('edu') and domain not regexp 'montana|spam|aka|akamai' and hour = 6"
DataEvening = "select distinct ip from `new_data_5pm_to_5am` where SUBSTRING_INDEX(new_domain, '.', -1) not in ('edu') and domain not regexp 'montana|spam|aka|akamai' and hour = 21"
DataWorking = "select distinct ip from `new_data_8am_to_5pm` where SUBSTRING_INDEX(new_domain, '.', -1) not in ('edu') and domain not regexp 'montana|spam|aka|akamai' and hour = 12"

DataMorningByIP = "SELECT hour, min, new_domain from `new_data_5am_to_8am` where ip='%s' and SUBSTRING_INDEX(new_domain, '.', -1) not in ('edu') and domain not regexp 'montana|spam|aka|akamai' and hour = 6 group by min, new_domain order by min, sec, msec"
DataEveningByIP = "SELECT hour, min, new_domain from `new_data_5pm_to_5am` where ip='%s' and SUBSTRING_INDEX(new_domain, '.', -1) not in ('edu') and domain not regexp 'montana|spam|aka|akamai' and hour = 21 group by min, new_domain order by min, sec, msec"
DataWorkingByIP = "SELECT hour, min, new_domain from `new_data_8am_to_5pm` where ip='%s' and SUBSTRING_INDEX(new_domain, '.', -1) not in ('edu') and domain not regexp 'montana|spam|aka|akamai' and hour = 12 group by min, new_domain order by min, sec, msec"



def RunQuery(cnx, query):
    cursor = cnx.cursor()
    cursor.execute(query)
    result = [x for x in cursor]
    cursor.close()
    return result

def main(daterbase):
    cnx = mariadb.connect()
    try:
        cnx = mariadb.connect(
            user='', 
            password='', 
            host='', 
            database='')
    except mariadb.Error as error:
        print("Error: {}".format(error))

    FinalList = []
    ResultDict = {}

    print(daterbase)

    if daterbase == 'DataMorning': 
        ans = RunQuery(cnx, DataMorning)
    elif daterbase == 'DataEvening': 
        ans = RunQuery(cnx, DataEvening)
    elif daterbase == 'DataWorking': 
        ans = RunQuery(cnx, DataWorking)
    else: 
        ans = RunQuery(cnx, ListIP)
    IPlist = [str(x[0]) for x in ans] # string conversion of first element of tuple of returned list

    print("Got IP List")

    for y in IPlist:
        if IPlist.index(y)%100 == 0: print(IPlist.index(y))
        if daterbase == 'DataMorning': 
            res = RunQuery(cnx, DataMorningByIP % y)
        elif daterbase == 'DataEvening': 
            res = RunQuery(cnx, DataEveningByIP % y)
        elif daterbase == 'DataWorking': 
            res = RunQuery(cnx, DataWorkingByIP % y)
        else: 
            res = RunQuery(cnx, DataByIP % y)
        fin = [(x[0], x[1], x[2].encode('raw_unicode_escape', 'ignore').decode("cp1252")) for x in res]
        FinalList.append(fin)

    print("Got Final List\n")

    for z in FinalList:
        for i in range(0,len(z)-3):
            #if z[i][2] != z[i+1][2] and z[i+1][2] != z[i+2][2] and z[i+2][2] != z[i+3][2]:
            if z[i][2] != z[i+1][2] and z[i+1][2] != z[i+2][2]:
            #if z[i][2] != z[i+1][2]:
                #key = (z[i][2], z[i+1][2], z[i+2][2], z[i+3][2])
                key = (z[i][2], z[i+1][2], z[i+2][2])
                #key = (z[i][2], z[i+1][2])
                if key in ResultDict:
                    ResultDict[key] += 1
                else:
                    ResultDict[key] = 1
    
    print ''
    for key, value in ResultDict.iteritems():
        if value > 1:
            #print "%s,%s,%s,%s,%d" % (key[0].strip(), key[1].strip(), key[2].strip(), key[3].strip(), value)
            print "%s,%s,%s,%d" % (key[0].strip(), key[1].strip(), key[2].strip(), value)
            #print "%s,%s,%d" % (key[0].strip(), key[1].strip(), value)

if __name__ == '__main__':
    main(sys.argv[1])
