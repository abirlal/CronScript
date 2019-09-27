import datetime
import os, re
import threading
import time 


#Global Veriuables
CRONLIST_FILE = '../etc/cronlist.dat'


def main():
    with open('pid.txt', 'a') as f:
        f.write(str(os.getpid()) + '\n')
    cronItems = list()
    cronItems = getCronList()
    continueLoop(cronItems)

def getCronList(): 
    cronList = list()
    with open(CRONLIST_FILE) as f:
        lines = f.readlines()
    for line in lines:
        if line[0] != '#':
            cronList = cronEntryParser(cronList, line)
    #print cronList
    return cronList

def cronEntryParser(cronList, line):
    cronDetails = dict()
    scheduleItems = line.split(' ', 5)

    if scheduleItems[0] == "*":
        cronDetails['minute'] = "00" 
    elif int(scheduleItems[0]) < 10:
        cronDetails['minute'] = "0" + scheduleItems[0]
    else:
        cronDetails['minute'] = scheduleItems[0]

    if scheduleItems[1] == "*":
        cronDetails['hour'] = "00" 
    elif int(scheduleItems[1]) < 10:
        cronDetails['hour'] = "0" + scheduleItems[1]
    else:
        cronDetails['hour'] = scheduleItems[1]

    if scheduleItems[2] == "*":
        cronDetails['day'] = "00" 
    elif int(scheduleItems[2]) < 10:
        cronDetails['day'] = "0" + scheduleItems[2]
    else:
        cronDetails['day'] = scheduleItems[2]

    if scheduleItems[3] == "*":
        cronDetails['month'] = "00" 
    elif int(scheduleItems[3]) < 10:
        cronDetails['month'] = "0" + scheduleItems[3]
    else:
        cronDetails['month'] = scheduleItems[3]
    
    
    if scheduleItems[4] == "*":
        cronDetails['weekday'] = "-"
    cronDetails['cmd'] = scheduleItems[5].rstrip()
    cronDetails['hourMinute'] = int(cronDetails['hour']  + cronDetails['minute'])
    cronList = insertInList(cronList, cronDetails)
    return cronList

def insertInList(list, details): 
    l = len(list)
    if l == 0:
        list.append(details)
    else:
        index = False
        for i in range(l): 
            if list[i]['hourMinute'] > details['hourMinute']: 
                index = True
                break
        if index:
            list = list[:i] + [details] + list[i:]
        else:
            list = list + [details]
    return list


def continueLoop(cronItems):
    year, month, day, hour, minute, second = getCurrentDateTime()
    #print year, month, day, hour, minute, second
    nextSleepTime = 60 - int(second)
    print " Sleeping to tuning for next minute (seconds) ...  " + str(nextSleepTime)
    time.sleep(nextSleepTime) 
    while True:
        if len(cronItems) > 0:
            #cronItem = cronItems[0]
            nextSleepTime = calculateWaitingTime(cronItems[0])
            print 'Sleeping (in Seconds) .... ' + str(nextSleepTime)
            time.sleep(nextSleepTime)
            cronItem = cronItems.pop(0)
            createTaskThread(cronItem)
            print "----------------------------------------------"
        else:
            cronItem['hour'] = "23"
            cronItem['minute'] = "59"
            cronItem['hourMinute'] = 2359
            nextSleepTime = calculateWaitingTime(cronItem) + 60
            print 'Sleeping (in Seconds) .... ' + str(nextSleepTime)
            time.sleep(nextSleepTime)
            cronItems = getCronList()
            print "Cron list is reloaded..."
            print "----------------------------------------------"
    
def calculateWaitingTime(cronItem):
    waitTime = 0
    year, month, day, hour, minute, second = getCurrentDateTime()
    print "Current Time :: ", year, month, day, hour, minute
    hourMinute = int(hour + minute)
    if cronItem['hourMinute'] > hourMinute :
        print "Next execution hour minute :: " + cronItem['hour'] + ':' + cronItem['minute'] 
        cronTime = cronItem['hour'] + ':' + cronItem['minute'] + ":00"
        sysTime = hour + ':' + minute + ":00"
        cronDt = datetime.datetime.strptime(cronTime, '%H:%M:%S')
        sysDt = datetime.datetime.strptime(sysTime, '%H:%M:%S')
        diff = (cronDt - sysDt)
        print "Next Execution Waiting time :: " + str(diff)
        waitTime = diff.seconds
    else:
        print "Next execution hour minute :: " + cronItem['hour'] + ':' + cronItem['minute'] + "  But this is already passed.. "

    return waitTime


def createTaskThread(cronItem):
    print "Calling task thread func..."
    year, month, day, hour, minute, second = getCurrentDateTime()
    onlyMinute = '000000' + minute
    hourMinute = '0000' + hour + minute
    dayHourMinute = '00' + day + hour + minute
    monthDayHourMinute = month + day + hour + minute
    timeStr = cronItem['month'] + cronItem['day'] + cronItem['hour'] + cronItem['minute']
    print "Time string :: " + timeStr + "  " + monthDayHourMinute
    if timeStr == monthDayHourMinute:
        createThread(cronItem['cmd'])
    elif timeStr == dayHourMinute:
        createThread(cronItem['cmd'])
    elif timeStr == hourMinute:
        createThread(cronItem['cmd'])
    elif timeStr == onlyMinute:
        createThread(cronItem['cmd'])
    else:
        pass
    

def createThread(commandStr):
    t = threading.Thread(target=doExecute, args=[commandStr])
    t.start()

def doExecute(commandStr):
    print " @@@@@@@@  Trigered Command :: " + commandStr
    os.system(commandStr)

def getCurrentDateTime():
    match = re.match(r'(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+).*', str(datetime.datetime.now()))
    items = match.groups()
    return items[0], items[1], items[2], items[3], items[4], items[5]


if __name__ == '__main__':
    main()