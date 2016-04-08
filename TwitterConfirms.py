import tornado.auth
import tornado.ioloop
import tornado.web
from tornado.options import options, define
from tornado.httpserver import HTTPServer

import pymysql
import json
#from datetime import datetime
import datetime
import lib.db
import lib.util

class AskTwitterPrediction(tornado.web.RequestHandler, 
                    tornado.auth.TwitterMixin):
    @tornado.gen.coroutine
    def post(self):
        input = self.request.body
        input = input.decode("utf-8")
        inputDict = json.loads(input)
        invalidRequest = False
        requestErrors = {}
        if not(all(x in inputDict.keys() for x in ["key", "tweetID", "arbiter", "dueDate"])):
            print("Lack of keys" + str(inputDict))
            self.send_error(400)
            return
        user = lib.db.getUserByKey(inputDict['key'], options.connection)
        if not user:
            invalidRequest = True
            requestErrors['key'] = "User key not found"
        #dueDate = datetime.utcfromtimestamp(int(inputDict['dueDate']))
        #dueDateDelta = dueDate - datetime.utcnow()
        if invalidRequest:
            print("invalid user")
            self.set_status(400)      
            self.finish(requestErrors)
            return

        text = "@" + inputDict['arbiter'] + r", did it happen? @" + user["screen_name"]
        try:
            post = yield self.twitter_request(
                        "/statuses/update",
                        post_args={"status":text,"in_reply_to_status_id":inputDict["tweetID"]},
                        access_token = user,
                        )
        except tornado.auth.AuthError as e:
            print(str(e))
            self.set_status(403)
            self.finish()
            return
        self.set_status(200)
        status = {"responseTweetID":post["id_str"]}
        self.finish(status)

class ConfirmTwitterPrediction(tornado.web.RequestHandler, 
                    tornado.auth.TwitterMixin):
    @tornado.gen.coroutine
    def post(self):
        input = self.request.body
        input = input.decode("utf-8")
        inputDict = json.loads(input)
        invalidRequest = False
        requestErrors = {}
        if not(all(x in inputDict.keys() for x in ["key", "tweetID", "arbiter","responseTweetID"])):
            self.send_error(400)
            return
        user = lib.db.getUserByKey(inputDict['key'], options.connection)
        if not user:
            invalidRequest = True
            requestErrors['key'] = "User key not found"
        #dueDate = datetime.utcfromtimestamp(int(inputDict['dueDate']))
        #dueDateDelta = dueDate - datetime.utcnow()
        if invalidRequest:
            self.set_status(400)      
            self.finish(requestErrors)
            return
        print("->" + inputDict["tweetID"])
        success = None;
        try:
            print(inputDict["arbiter"] + str(user))
            args = {}
            args['screen_name'] = inputDict["arbiter"] 
            replies = yield self.twitter_request(
                        "/statuses/user_timeline",
                        access_token = user, 
                        **args
                        )
            for reply in replies:
                print(reply["in_reply_to_status_id"])
                if str(reply["in_reply_to_status_id"]) == str(inputDict["responseTweetID"]):
                         res = self.parseReply(reply)
                         if res != "":
                                 success = res
                                 break
            else:
                         success =  ""
        except tornado.auth.AuthError as e:
            self.set_status(403)
            print(str(e))
            self.finish({"text":str(e)})
            return
        print(success)
        self.set_status(200 if success else 403)
        status = {}
        print(success)
        status['success'] = success
        self.finish(status)

    def parseReply(self, reply):
                print("Parse reply")
                text = reply["text"]
                if any(x in text for x in ["True", "true", "Yes", "yes", "Confirm", "confirm"]):
                        return "Yes"
                if any(x in text for x in ["False", "false", "No", "no", "Not", "not"]):
                        return "No"
                else:
                        return ""

