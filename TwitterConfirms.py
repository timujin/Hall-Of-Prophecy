import tornado.auth
import tornado.ioloop
import tornado.web
from tornado.options import options, define
from tornado.httpserver import HTTPServer

import pymysql
import json
from datetime import datetime

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
            self.set_status(403)
            self.finish()
            return
        self.set_status(200)
        status = {}
        self.finish()

class ConfirmTwitterPrediction(tornado.web.RequestHandler, 
                    tornado.auth.TwitterMixin):
    @tornado.gen.coroutine
    def post(self):
        input = self.request.body
        input = input.decode("utf-8")
        inputDict = json.loads(input)
        invalidRequest = False
        requestErrors = {}
        if not(all(x in inputDict.keys() for x in ["key", "tweetID", "arbiter"])):
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

        success = None;
        try:
            replies = yield self.twitter_request(
                        "statuses/user_timeline",
                        post_args={"screen_name ":inputDict["arbiter"],"count":200},
                        access_token = user,
                        )
            for reply in replies:
                if reply["in_reply_to_status_id"] == tweetID:
                         success = self.parseReply(reply)
            else:
                         success =  "Unconfirmed"
        except tornado.auth.AuthError as e:
            self.set_status(403)
            self.finish(e.args)
            return
        self.set_status(200)
        status = {}
        print(success)
        status['success'] = success
        self.finish(status)

    def parseReply(self, reply):
                text = reply["text"]
                if any(x in text for x in ["True", "true", "Yes", "yes", "Confirm", "confirm"]):
                        return "True"
                if any(x in text for x in ["False", "false", "No", "no", "Not", "not"]):
                        return "False"
                else:
                        return "Unconfirmed"
