from datetime import date

def saveUser(user, connection):
    with connection.cursor() as cursor:
        sql = "INSERT INTO `Users` (`name`,`screen_name`, `key`,\
            `user_id`, `secret`, `x_auth_expires`, `regid`) VALUES (%s,%s,%s,%s,%s,%s)"
        token = user['access_token']
        cursor.execute(sql, (user['name'], token['screen_name'], token['key'],\
                             token['user_id'], token['secret'], token['x_auth_expires'], user['regid']))
    connection.commit()

def updateUser(user, connection):
    with connection.cursor() as cursor:
        sql = "UPDATE `Users` SET `name`=%s, `screen_name`=%s, `key`=%s, `user_id`=%s, `secret`=%s, \
              `x_auth_expires`=%s, WHERE `user_id`=%s LIMIT 1"
        token = user['access_token']
        a = cursor.execute(sql, (user['name'], token['screen_name'], token['key'],
                             token['user_id'], token['secret'], token['x_auth_expires'], token['user_id'], user['regid']))
    connection.commit()

def updateUserNotificationID(user, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "UPDATE `Users` SET `regid`=%s WHERE `user_id`=%s LIMIT 1"
        a = cursor.execute(sql, (user['regid'], user['user_id']))
    connection.commit()

def saveTwitterPrediction(pred, connection):
    with connection.cursor() as cursor:
        sql = "INSERT INTO `twitterPredictions` (`authorID`, `text`, `tweetID`, `arbiterHandle`, `dueDate`, `url`) \
                VALUES (%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, (pred['authorID'], pred['text'], pred['tweetID'], pred['arbiterHandle'],
                            pred['dueDate'], pred['url']))
    connection.commit()

def getUserByHandle(handle, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT `id`,`name`, `screen_name`, `key`, `user_id`, `secret`, `x_auth_expires`, `regid`\
               FROM `Users` WHERE `screen_name` = %s"
        cursor.execute(sql, (handle))
        return cursor.fetchone()

def getUserByKey(key, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT `id`, `name`, `screen_name`, `key`, `user_id`, `secret`, `x_auth_expires`\
               FROM `Users` WHERE `key` = %s"
        cursor.execute(sql, (key))
        return cursor.fetchone()

def getUserByUserID(user_id, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT `id`, `name`, `screen_name`, `key`, `user_id`, `secret`, `x_auth_expires`\
               FROM `Users` WHERE `user_id` = %s"
        cursor.execute(sql, (user_id))
        return cursor.fetchone()

def getTwitterPredictionByURL(url, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT `twitterPredictions`.`id`,`Users`.`name`, `text`, `arbiterHandle`, `dueDate`, `result`, `resultTweetID`\
                FROM `twitterPredictions` LEFT JOIN `Users` ON `twitterPredictions`.`authorID`=`Users`.`id`\
                WHERE url = %s"
        cursor.execute(sql, (url))
        return cursor.fetchone()

def getTwitterPredictionByPredictionID(predictionID, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT `twitterPredictions`.`id`,`name`, `text`, `arbiterHandle`, `dueDate`, `result`, `resultTweetID`, `url`\
                FROM `twitterPredictions` LEFT JOIN `Users` ON `twitterPredictions`.`authorID`=`Users`.`id`\
                WHERE `twitterPredictions`.`id` = %s"
        cursor.execute(sql, (predictionID))
        return cursor.fetchone()

####################

def addTwitterDue(due, connection):
    with connection.cursor() as cursor:
        sql = "INSERT INTO `TwitterDues` (`predictionID`, `dueDate`, `confirm`,`responseTweetID`) \
                VALUES (%s,%s,%s,%s)"
        if "responseTweetID" not in due.keys():
             due["responseTweetID"] = "0"
        cursor.execute(sql, (due["predictionID"], due["dueDate"], due["confirm"], due["responseTweetID"]))
    connection.commit()

def popUpcomingTwitterDues(num, connection):
    tday = 9999999999999999 #= date.today()
    with connection.cursor() as cursor:
        sql = "SELECT `TwitterDues`.`id`,`TwitterDues`.`confirm`,`TwitterDues`.`responseTweetID`,`TwitterDues`.`predictionID`, `TwitterDues`.`dueDate`,\
         `Users`.`key`, `twitterPredictions`.`authorID`, `twitterPredictions`.`tweetID`,`twitterPredictions`.`arbiterHandle`\
               FROM `TwitterDues` INNER JOIN `twitterPredictions` ON `TwitterDues`.`predictionID`=`twitterPredictions`.`id` INNER JOIN `Users` ON `Users`.`id`=`twitterPredictions`.`authorID`\
                WHERE `TwitterDues`.`dueDate` <= %s ORDER BY `TwitterDues`.`dueDate` DESC LIMIT %s"
        cursor.execute(sql, (tday, num))
        result = cursor.fetchall()
        removeTwitterDues(result, connection)
        return result

def removeTwitterDues(dues, connection):
    print(dues)
    with connection.cursor() as cursor:
        sql = "DELETE FROM `TwitterDues` WHERE `id`=%s"
        for due in dues:
            cursor.execute(sql, (due["id"]))
    connection.commit()

def passJudgement(id, judgement, connection):
    with connection.cursor() as cursor:
        sql = "UPDATE `twitterPredictions` SET `result`=%s WHERE `id`=%s"
        cursor.execute(sql, (1 if judgement else -1, id))
    connection.commit()

##################

def getUserTwitterPredictions(userID, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT `twitterPredictions`.`url`,`name`, `text`, `arbiterHandle`, `dueDate`, `result`, `resultTweetID`\
               FROM `twitterPredictions` LEFT JOIN `Users` ON `twitterPredictions`.`authorID`=`Users`.`id`\
               WHERE `twitterPredictions`.`authorID` = %s\
               ORDER BY `dueDate` ASC"
        cursor.execute(sql, (userID))
        return cursor.fetchall()

def getUserTwitterPredictionsOnlyUndecided(userID, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT `twitterPredictions`.`url`, `name`, `text`, `arbiterHandle`, `dueDate`, `result`, `resultTweetID`\
               FROM `twitterWagers` LEFT JOIN `twitterPredictions` ON `twitterWagers`.`predictionID`=`twitterPredictions`.`id`\
               LEFT JOIN `Users` ON `twitterWagers`.`authorID`=`Users`.`id`\
               WHERE `twitterWagers`.`authorID`=%s AND `result` IS NULL\
               ORDER BY `dueDate` ASC"
        cursor.execute(sql, (userID))
        return cursor.fetchall()

def getUserTwitterPredictionsWithWagers(userID, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT `twitterPredictions`.`url`, `name`, `text`, `arbiterHandle`, `dueDate`, `result`, `resultTweetID`\
               FROM `twitterWagers` LEFT JOIN `twitterPredictions` ON `twitterWagers`.`predictionID`=`twitterPredictions`.`id`\
               LEFT JOIN `Users` ON `twitterWagers`.`authorID`=`Users`.`id`\
               WHERE `twitterWagers`.`authorID`=%s\
               ORDER BY `dueDate` ASC"
        cursor.execute(sql, (userID))
        return cursor.fetchall()

def saveTwitterComment(comment, connection):
    with connection.cursor() as cursor:
        sql = "INSERT INTO `hallofprophecy`.`twitterComments` (`authorID`, `predictionID`, `timestamp`,  `text`)\
               VALUES (%s,%s,%s,%s)"
        cursor.execute(sql, (comment['author'], comment['prediction'], comment['time'], comment['text']))
    connection.commit()


def saveTwitterWager(wager, connection):
    with connection.cursor() as cursor:
        sql = "INSERT INTO `hallofprophecy`.`twitterWagers` (`authorID`, `predictionID`, `timestamp`,  `wager`)\
               VALUES (%s,%s,%s,%s)"
        cursor.execute(sql, (wager['author'], wager['prediction'], wager['time'], wager['wager']))
    connection.commit()

def getTwitterPredictionWagers(predictionID, connection):
    with connection.cursor() as cursor:
        sql = "SELECT `timestamp`, `Users`.`name`, `wager`, `Users`.`user_id`, `Users`.`screen_name` as 'handle'\
               FROM `twitterWagers` LEFT JOIN `Users` ON `twitterWagers`.`authorID` = `Users`.`id`\
               WHERE `predictionID` = %s"
        cursor.execute(sql, (predictionID))
        return cursor.fetchall()

def getTwitterPredictionWagersLocal(predictionID, connection):
    with connection.cursor() as cursor:
        sql = "SELECT `timestamp`, `Users`.`name`, `wager`, `Users`.`user_id`, `Users`.`regid`, `Users`.`screen_name` as 'handle'\
               FROM `twitterWagers` LEFT JOIN `Users` ON `twitterWagers`.`authorID` = `Users`.`id`\
               WHERE `predictionID` = %s"
        cursor.execute(sql, (predictionID))
        return cursor.fetchall()

def getTwitterPredictionAuthorWager(predictionID, authorID, connection):
    with connection.cursor() as cursor:
        sql = "SELECT `timestamp`, `Users`.`name`, `wager`, `Users`.`user_id`, `Users`.`screen_name` as 'handle'\
                FROM `twitterWagers` LEFT JOIN `Users` ON `twitterWagers`.`authorID` = `Users`.`id`\
                WHERE `predictionID` = %s AND `authorID` = %s"
        cursor.execute(sql, (predictionID, authorID))
        return cursor.fetchall()


def getTwitterPredictionComments(predictionID, connection):
    with connection.cursor() as cursor:
        sql = "SELECT `timestamp`, `Users`.`name`, `text`, `Users`.`user_id`, `Users`.`screen_name` as 'handle'\
              FROM `twitterComments` LEFT JOIN `Users` ON `twitterComments`.`authorID` = `Users`.`id`\
              WHERE `predictionID` = %s"
        cursor.execute(sql, (predictionID))
        return cursor.fetchall()



def testDB(connection):
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `Users` (`accessToken`, `name`, `handler`) VALUES (%s, %s, %s)"
            cursor.execute(sql, ('webmaster@python.org', 'very-secret', '@secret'))

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()

        with connection.cursor() as cursor:
        # Read a single record
            sql = "SELECT * FROM `Users`"
            cursor.execute(sql)
            result = cursor.fetchone()
            print(result)
    finally:
        connection.close()
