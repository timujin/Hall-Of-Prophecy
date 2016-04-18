import tornado.auth
import tornado.ioloop
import tornado.web
from tornado.options import options, define
from tornado.httpserver import HTTPServer

import pymysql
import json
from datetime import datetime
import calendar

import lib.db
import lib.util
import TwitterConfirms

class MainHandler(tornado.web.RequestHandler):
    def get(self, args):
        self.write("Hello, world " + args)


class TwitterLoginHandler(tornado.web.RequestHandler,
                          tornado.auth.TwitterMixin):
    @tornado.gen.coroutine
    def get(self):
        if self.get_argument("oauth_token", None):
            user = yield self.get_authenticated_user()
            # Save the user using e.g. set_secure_cookie()
            #print(user)
            #define("twitter_access_token", user['access_token'])
            conn = options.connection
            lib.db.saveUser(user, conn)
            #define("twitter_username", user['username'])
            #self.write("Logged in " + options.twitter_username )
            if ('auth_redirect' in options):
                self.redirect(options.auth_redirect)
        else:
            next = self.get_argument("next", None)
            if next:
                define("auth_redirect", next)
            yield self.authenticate_redirect()

class RegisterAppClient(tornado.web.RequestHandler,
                        tornado.auth.TwitterMixin):
    @tornado.gen.coroutine
    def post(self):
        input = self.request.body
        input = input.decode("utf-8")
        inputDict = json.loads(input)
        if (all(x in inputDict.keys() for x in ["key", "secret", "user_id"])):
            existingUser = lib.db.getUserByUserID(inputDict['user_id'], options.connection)
            access = inputDict
            needUpdate = False
            if existingUser:
                access = existingUser
                try:
                    new_user = yield self.twitter_request("/account/verify_credentials", 
                               access_token=access)
                    self.finish("OK")
                    return
                except tornado.auth.AuthError:
                    access = inputDict
                    needUpdate = True
            try:
                new_user = yield self.twitter_request("/account/verify_credentials",
                                 access_token=access)
            except tornado.auth.AuthError:
                self.send_error(403)
                return
            user = dict()
            inputDict['screen_name']=new_user['screen_name']
            inputDict['x_auth_expires']=0
            user['access_token']=inputDict
            user['name']=new_user['name']
            if (needUpdate):
                lib.db.updateUser(user, options.connection)
            else:
                lib.db.saveUser(user, options.connection)
            self.finish("OK")
        else:
            self.send_error(400)
            return


class AddTwitterPrediction(tornado.web.RequestHandler, 
                    tornado.auth.TwitterMixin):
    @tornado.gen.coroutine
    def post(self):
        input = self.request.body
        try:
            input = input.decode("utf-8")
            inputDict = json.loads(input)
        except:
            self.set_status(400)
            self.finish("Failed to parse JSON")
            return
        invalidRequest = False
        requestErrors = {}
        if not(all(x in inputDict.keys() for x in ["key", "text", "dueDate", "arbiterHandle"])):
            self.send_error(400)
            return
        user = lib.db.getUserByKey(inputDict['key'], options.connection)
        if not user:
            invalidRequest = True
            requestErrors['key'] = "User key not found"
        dueDate = datetime.utcfromtimestamp(int(inputDict['dueDate']))
        dueDateDelta = dueDate - datetime.utcnow()
        if dueDateDelta.total_seconds() < 120:
            invalidRequest = True
            requestErrors['dueDate'] = "Date Too Close to Now" + str(dueDateDelta.total_seconds())
        if len(inputDict['text']) > 130:
            invalidRequest = True
            requestErrors['text'] = "Post text too long"
        if invalidRequest:
            self.set_status(400)      
            self.finish(requestErrors)
            return
        """ try:
            testHandle = yield self.twitter_request(
                        "/users/show", access_token = user, 
                        args = {
                            'screen_name':inputDict['arbiterHandle'],
                            'include_entities':'false',
                               }
                        )
        except torando.auth.AuthError as e:
            print(e)
            #self.send_error(403)
            return
        """
        text = inputDict['text']
        try:
            post = yield self.twitter_request(
                        "/statuses/update",
                        post_args={"status":text + r" #Prophecy"},
                        access_token = user,
                        )
        except tornado.auth.AuthError as e:
            self.set_status(403)
            self.finish(e.args)
            return
        prediction = {}
        prediction['authorID']=user['id']
        prediction['text'] = text
        prediction['tweetID'] = post['id_str']
        prediction['arbiterHandle'] = inputDict['arbiterHandle']
        prediction['dueDate'] = inputDict['dueDate']
        prediction['url'] = lib.util.generateURL()
        while True:
            try:
                lib.db.saveTwitterPrediction(prediction, options.connection)
                break
            except e:
                print('Twitter URL Colision found. Regenerating url')
                prediction['url'] = lib.util.generateURL()
        id = lib.db.getTwitterPredictionByURL(prediction["url"], options.connection)["id"]
        timestamp = datetime.utcnow()
        wager = {
                'author':user['id'],
                'prediction':id,
                'wager':'1',
                'time':calendar.timegm(timestamp.utctimetuple()),
                }
        lib.db.saveTwitterWager(wager, options.connection)
        lib.db.addTwitterDue({"predictionID":id,"dueDate":inputDict["dueDate"],"confirm":False} ,options.connection)
        self.set_status(200)
        status = {}
        status['url'] = prediction['url']
        self.finish(status)
        
        
class ShowTwitterPrediction(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, url):
        prediction = lib.db.getTwitterPredictionByURL(url, options.connection)
        if prediction:
            prediction['comments'] = lib.db.getTwitterPredictionComments(prediction['id'], options.connection)
            prediction['wagers'] = lib.db.getTwitterPredictionWagers(prediction['id'], options.connection)
            self.finish(prediction)
            return
        self.send_error(404)

class ShowUserProfile(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, userID):
        user = lib.db.getUserByUserID(userID, options.connection)
        if not user:
            user = lib.db.getUserByHandle(userID, options.connection)
            if not user:
                self.send_error(404)
                return
        predictions = lib.db.getUserTwitterPredictions(user['id'], options.connection)
        result = {}
        result['predictions'] = predictions
        result['handle'] = user['screen_name']
        result['name'] = user['name']
        result['id'] = user['user_id']
        self.finish(result)

class ShowUserProfileWithWagers(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, userID):
        user = lib.db.getUserByUserID(userID, options.connection)
        if not user:
            user = lib.db.getUserByHandle(userID, options.connection)
            if not user:
                self.send_error(404)
                return
        predictions = lib.db.getUserTwitterPredictionsWithWagers(user['id'], options.connection)
        result = {}
        result['predictions'] = predictions
        result['handle'] = user['screen_name']
        result['name'] = user['name']
        result['id'] = user['user_id']
        self.finish(result)

class ShowUserProfileOnlyUndecided(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, userID):
        user = lib.db.getUserByUserID(userID, options.connection)
        if not user:
            user = lib.db.getUserByHandle(userID, options.connection)
            if not user:
                self.send_error(404)
                return
        predictions = lib.db.getUserTwitterPredictionsOnlyUndecided(user['id'], options.connection)
        result = {}
        result['predictions'] = predictions
        result['handle'] = user['screen_name']
        result['name'] = user['name']
        result['id'] = user['user_id']
        self.finish(result)

class AddTwitterPredictionComment(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def post(self, url):
        input = self.request.body
        invalidRequest = False
        requestErrors = {}
        prediction = lib.db.getTwitterPredictionByURL(url, options.connection)
        if not prediction:
            self.send_error(404)
            return
        try:
            input = input.decode("utf-8")
            inputDict = json.loads(input)
        except:
            self.set_status(400)
            self.finish("Failed to parse JSON")
            return
            
        if not(all(x in inputDict.keys() for x in ["author", "text"])):
            self.set_status(400)
            self.finish("Some required parameters were not found")
            return
            
        user = lib.db.getUserByKey(inputDict['author'], options.connection)
        if not user:
            requestErrors['author'] = "User not found"
            invalidRequest = True
            
        if len(inputDict['text']) > 140 or len(inputDict['text']) < 1:
            requestErrors['text'] = "Comment text should exist and be less then 140 symbols long"
            invalidRequest = True
            
        if invalidRequest:
            self.set_status(400)
            self.finish(requestErrors)
            return
        #print(inputDict['text'])
        timestamp = datetime.utcnow()
        comment = {
                'author':user['id'],
                'prediction':prediction['id'],
                'text':inputDict['text'],
                'time':calendar.timegm(timestamp.utctimetuple()),
                }
        lib.db.saveTwitterComment(comment, options.connection)
        self.set_status(200)
        self.finish()
        return


class AddTwitterPredictionWager(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def post(self, url):
        input = self.request.body
        invalidRequest = False
        requestErrors = {}
        prediction = lib.db.getTwitterPredictionByURL(url, options.connection)
        if not prediction:
            self.send_error(404)
            return
        try:
            input = input.decode("utf-8")
            inputDict = json.loads(input)
        except:
            self.set_status(400)
            self.finish("Failed to parse JSON")
            return
        if not(all(x in inputDict.keys() for x in ["author", "wager"])):
            self.set_status(400)
            self.finish("Some required parameters were not found")
            return
        user = lib.db.getUserByKey(inputDict['author'], options.connection)
        if not user:
            requestErrors['author'] = "User not found"
            invalidRequest = True
        s = inputDict['wager']
        if not (s == "0" or s == "1"):
            requestErrors['wager'] = "Wager should be either 1 or 0"
            invalidRequest = True
            
        if invalidRequest:
            self.set_status(400)
            self.finish(requestErrors)
            return 
        wager = lib.db.getTwitterPredictionAuthorWager(prediction['id'], user['id'], options.connection)
        if wager:
            self.send_error(403)
            return
        timestamp = datetime.utcnow()
        wager = {
                'author':user['id'],
                'prediction':prediction['id'],
                'wager':s,
                'time':calendar.timegm(timestamp.utctimetuple()),
                }
        lib.db.saveTwitterWager(wager, options.connection)
        self.set_status(200)
        self.finish()
        return

def make_app(settings):
    return tornado.web.Application([
        (r"/register", RegisterAppClient),
        (r"/prediction/twitter", AddTwitterPrediction),
        (r"/prediction/twitter/wager/(.*)", AddTwitterPredictionWager),
        (r"/prediction/twitter/comment/(.*)", AddTwitterPredictionComment),
        (r"/prediction/twitter/(.*)", ShowTwitterPrediction),
        (r"/user/withwagers/(.*)", ShowUserProfileWithWagers),
        (r"/user/onlyundecided/(.*)", ShowUserProfileOnlyUndecided),
        (r"/user/(.*)", ShowUserProfile),
        (r"/confirm/twitter/ask", TwitterConfirms.AskTwitterPrediction),
        (r"/confirm/twitter/confirm", TwitterConfirms.ConfirmTwitterPrediction),
        (r"(.*)", MainHandler),
       #(r"/testPost/(.*)", TwitterTestPoster),
    ],**settings)

if __name__ == "__main__":
    lib.util.parse_config_file("config.conf")
    server = options.mysql["server"]
    user = options.mysql["user"]
    password = options.mysql["password"]
    database = options.mysql["database"]
    conn = pymysql.connect(host=server, user=user, password=password, db=database,cursorclass=pymysql.cursors.DictCursor, charset='utf8')
    define("connection", conn)
    app = make_app(options.as_dict())
    app.listen(8080)
    tornado.ioloop.IOLoop.current().start()

