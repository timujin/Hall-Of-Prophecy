import pymysql
import json
#from datetime import datetime, timedelta
import datetime
import lib.db
import lib.util
import urllib
import urllib.request

mysql = {"server":"127.0.0.1",
         "user":"hallofprophecy",
         "password":"hallofprophecy123",
         "port":"3306",
         "database":"hallofprophecy",
         }

conn = 0

def rescheduleDue(due):
        #t = datetime.datetime.utcfromtimestamp()
        #d = datetime.timedelta(days=1)
        #due["dueDate"] += d
        lib.db.addTwitterDue(due, conn)

def scheduleConfirm(due):
        due["confirm"] = 1
        lib.db.addTwitterDue(due, conn)

def confirmDue(due):
        try:
                post_params = {
                        "key":due["key"],"tweetID":due["tweetID"],"arbiter":due["arbiterHandle"],
                }
                params = json.dumps(post_params).encode('utf-8')
                req = urllib.request.Request("http://hallofprophecy.xyz:8080/confirm/twitter/confirm", data=params,
                             headers={'content-type': 'application/json'})
                response = urllib.request.urlopen(req)
                print(response)
                return True
        except:
                return False

def checkTwitterDues():
        dues = lib.db.popUpcomingTwitterDues(num=20, connection=conn)
        success = None #print("!"+ str(dues))
        for due in dues:
                if (due["confirm"]):
                        success = confirmDue(due)
                else:
                        success = askDue(due)
                        if (success):
                                scheduleConfirm(due)
                if (not success):
                        rescheduleDue(due)

def askDue(due):
        try:
                post_params = {
                        "key":due["key"],"tweetID":due["tweetID"],"arbiter":due["arbiterHandle"],"dueDate":due["dueDate"],
                }
                params = json.dumps(post_params).encode('utf-8')
                req = urllib.request.Request("http://hallofprophecy.xyz:8080/confirm/twitter/ask", data=params,
                             headers={'content-type': 'application/json'})
                response = urllib.request.urlopen(req)
                print(response)
                return True
        except:
                return False

if __name__ == "__main__":
    server = mysql["server"]
    user = mysql["user"]
    password = mysql["password"]
    database = mysql["database"]
    conn = pymysql.connect(host=server, user=user, password=password, db=database,cursorclass=pymysql.cursors.DictCursor)

    checkTwitterDues()


