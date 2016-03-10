import tornado.auth
import tornado.ioloop
import tornado.web
from tornado.options import options, define
from tornado.httpserver import HTTPServer

import pymysql
import json

from lib.util import parse_config_file
import lib.db

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

class TwitterPoster(tornado.web.RequestHandler,
                        tornado.auth.TwitterMixin):
    @tornado.gen.coroutine
    def prepare(self):
        self.current_user = lib.db.getUserByHandle("hallofprophecy", options.connection)
        # if 'twitter_access_token' in options:
         #    self.current_user = options.twitter_access_token

    @tornado.gen.coroutine
    @tornado.web.authenticated
    def post(self):
        #access_token = self.current_user
        try:
            new_entry = yield self.twitter_request(
            "/statuses/update",
            post_args={"status": arg},
            access_token=access_token)
        except tornado.auth.AuthError:
            self.finish("Message already posted")
            return
        if not new_entry:
            # Call failed; perhaps missing permission?
            yield self.authorize_redirect()
            return
        self.finish("Posted a message!")

def make_app(settings):
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/POSTtest", RegisterAppClient),
       #(r"/testPost/(.*)", TwitterTestPoster),
    ],**settings)

if __name__ == "__main__":
    parse_config_file("config.conf")
    server = options.mysql["server"]
    user = options.mysql["user"]
    password = options.mysql["password"]
    database = options.mysql["database"]
    conn = pymysql.connect(host=server, user=user, password=password, db=database,cursorclass=pymysql.cursors.DictCursor)
    define("connection", conn)
    app = make_app(options.as_dict())
    app.listen(8080)
    tornado.ioloop.IOLoop.current().start()

