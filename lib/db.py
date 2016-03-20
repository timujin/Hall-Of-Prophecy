from datetime import date

def saveUser(user, connection):
    with connection.cursor() as cursor:
        sql = "INSERT INTO `Users` (`name`,`screen_name`, `key`,\
            `user_id`, `secret`, `x_auth_expires`) VALUES (%s,%s,%s,%s,%s,%s)"
        token = user['access_token']
        cursor.execute(sql, (user['name'], token['screen_name'], token['key'],\
                             token['user_id'], token['secret'], token['x_auth_expires']))
    connection.commit()

def saveTwitterPrediction(pred, connection):
    with connection.cursor() as cursor:
        sql = "INSERT INTO `twitterPredictions` (`authorID`, `text`, `tweetID`, `arbiterHandle`, `dueDate`, `url`) \
                VALUES (%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, (pred['authorID'], pred['text'], pred['tweetID'], pred['arbiterHandle'], 
                            pred['dueDate'], pred['url']))
    connection.commit()

def getUserByHandle(handle, connection):
    with connection.cursor() as cursor:
        sql = "SELECT `id`,`name`, `screen_name`, `key`, `user_id`, `secret`, `x_auth_expires`\
               FROM `Users` WHERE `screen_name` = %s"
        cursor.execute(sql, (handle))
        return cursor.fetchone()

def getUserByKey(key, connection):
    with connection.cursor() as cursor:
        sql = "SELECT `id`, `name`, `screen_name`, `key`, `user_id`, `secret`, `x_auth_expires`\
               FROM `Users` WHERE `key` = %s"
        cursor.execute(sql, (key))
        return cursor.fetchone()
        
def getUserByUserID(user_id, connection):
    with connection.cursor() as cursor:
        sql = "SELECT `id`, `name`, `screen_name`, `key`, `user_id`, `secret`, `x_auth_expires`\
               FROM `Users` WHERE `user_id` = %s"
        cursor.execute(sql, (user_id))
        return cursor.fetchone()

def getTwitterPredictionByURL(url, connection):
    with connection.cursor() as cursor:
        sql = "SELECT `twitterPredictions`.`id`,`name`, `text`, `arbiterHandle`, `dueDate`, `result`, `resultTweetID`\
                FROM `twitterPredictions` LEFT JOIN `Users` ON `twitterPredictions`.`authorID`=`Users`.`id`\
                WHERE url = %s"
        cursor.execute(sql, (url))
        return cursor.fetchone()
        

def updateUser(user, connection):
    with connection.cursor() as cursor:
        sql = "UPDATE `Users` SET `name`=%s, `screen_name`=%s, `key`=%s, `user_id`=%s, `secret`=%s, \
              `x_auth_expires`=%s WHERE `user_id`=%s LIMIT 1"
        token = user['access_token']
        a = cursor.execute(sql, (user['name'], token['screen_name'], token['key'],
                             token['user_id'], token['secret'], token['x_auth_expires'], token['user_id']))                                   
    connection.commit()

####################

def addTwitterDue(due, connection):
    with connection.cursor() as cursor:
        sql = "INSERT INTO `TwitterDues` (`predictionID`, `dueDate`, `confirm`) \
                VALUES (%s,%s,%s)"
        cursor.execute(sql, (due["predictionID"], due["dueDate"], due["confirm"]))
    connection.commit()

def popUpcomingTwitterDues(num, connection):
    tday = 9999999999999999 #= date.today()
    with connection.cursor() as cursor:
        sql = "SELECT `TwitterDues`.`id`,`TwitterDues`.`confirm`,`TwitterDues`.`predictionID`, `TwitterDues`.`dueDate`,\
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

##################

def getUserTwitterPredictions(userID, connection):
    with connection.cursor() as cursor:
        sql = "SELECT `twitterPredictions`.`id`,`name`, `text`, `arbiterHandle`, `dueDate`, `result`, `resultTweetID`\
               FROM `twitterPredictions` LEFT JOIN `Users` ON `twitterPredictions`.`authorID`=`Users`.`id`\
               WHERE `twitterPredictions`.`authorID` = %s"
        cursor.execute(sql, (userID))
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

