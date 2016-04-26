tableName   = lambda title: title + "Predictions"
stringData  = lambda pred : (', '.join(["`{}`"]*len(pred.keys()))).format(*(pred.keys()))
stringFormat= lambda pred : ','.join(["%s"]*len(pred.keys()))
stringValues= lambda pred : tuple(str(pred[x]) for x in pred.keys())
stringConditions= lambda pred: " AND ".join(['`' + x + '`' + " = '" + str(pred[x]) + "'" for x in pred.keys()])
tableWager= lambda title: title + "Wagers"
createTableFields = lambda pred: (', '.join(["`{}` {}"]*len(pred.keys()))).format(*(list(sum([(k,v) for k,v in pred.items()],()))))

defaultPredictionFields = {
    "result": "int(11) DEFAULT NULL",
    "url": "varchar(60) DEFAULT NULL",
}
defaultPredictionNetworkFields = {
    "dueDate": "bigint(16) NOT NULL",
}    

defaultWagerFields = {
    "timestamp": "bigint(16) NOT NULL",
    "wagerResult": "int(11) DEFAULT NULL",
}

defaultWagerNetworkFields = {} #NOT IMPLEMENTED

def createDatasetTables(title, predictionFields, wagerFields, judgementFields, connection):
    with connection.cursor() as cursor:
        sql = """CREATE TABLE IF NOT EXISTS `{0}` (
              `id` int(11) NOT NULL AUTO_INCREMENT,
              `authorID` int(11) NOT NULL,
              {3},
              {1},
              {2},
              PRIMARY KEY (`id`),
              UNIQUE KEY `{0}_url_UNIQUE` (`url`),
              KEY `{0}_author_idx` (`authorID`),
              KEY `{0}_dueDate_idx` (`dueDate`),
              CONSTRAINT `{0}_id` FOREIGN KEY (`authorID`) REFERENCES `Users` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
              ) ENGINE=InnoDB AUTO_INCREMENT=72 DEFAULT CHARSET=utf8;
              """.format(
                tableName(title),
                createTableFields(predictionFields), createTableFields(judgementFields),
                createTableFields(defaultPredictionFields)
                )
        print (sql)
        cursor.execute(sql, ())

        sql = """CREATE TABLE IF NOT EXISTS `{0}` (
                  `id` int(11) NOT NULL AUTO_INCREMENT,
                  `authorID` int(11) NOT NULL,
                  `predictionID` int(11) NOT NULL,
                  {3},
                  {1},
                  PRIMARY KEY (`id`),
                  KEY `{0}_authorID_idx` (`authorID`),
                  KEY `{0}_predictionID_idx` (`predictionID`),
                  CONSTRAINT `{0}_authorID` FOREIGN KEY (`authorID`) REFERENCES `Users` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
                  CONSTRAINT `{0}_predictionID` FOREIGN KEY (`predictionID`) REFERENCES `{2}` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
                ) ENGINE=InnoDB AUTO_INCREMENT=29 DEFAULT CHARSET=utf8;
                """.format(
                tableWager(title),
                createTableFields(wagerFields),
                tableName(title),
                createTableFields(defaultWagerFields)
                )
        print(sql)
        cursor.execute(sql, ())
    connection.commit()

def saveDatasetPrediction(title, pred, connection):
    with connection.cursor() as cursor:
        sql = "INSERT INTO `{}` ({}) VALUES ({})".format(
        	tableName(title), stringData(pred), stringFormat(pred))
        cursor.execute(sql, stringValues(pred))
    connection.commit()


def getDatasetPredictionByDataSet(title, valuesDict, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT `{0}`.`id`, {3}, {1}\
                FROM `{0}` LEFT JOIN `Users` ON `{0}`.`authorID`=`Users`.`id`\
                WHERE {2}".format(tableName(title), stringData(valuesDict), 
                                  stringConditions(valuesDict), stringData(defaultPredictionFields)) 
        print(sql)
        cursor.execute(sql, ())
        return cursor.fetchone()

    
def getDatasetPredictionByURL(title, valuesDict, url, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT `{0}`.`id`, {2}, {1}\
                FROM `{0}` LEFT JOIN `Users` ON `{0}`.`authorID`=`Users`.`id`\
                WHERE url = %s".format(tableName(title), stringData(valuesDict), stringData(defaultPredictionFields))
        cursor.execute(sql, (url))
        return cursor.fetchone()

def getUserDatasetPredictions(title, valuesDict, userID, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT {2}, {1}\
               FROM `{0}` LEFT JOIN `Users` ON `{0}`.`authorID`=`Users`.`id`\
               WHERE `{0}`.`authorID` = %s".format(tableName(title), stringData(valuesDict), stringData(defaultPredictionFields))
        cursor.execute(sql, (userID))
        return cursor.fetchall()

def getUserDatasetPredictionsWithWagers(title, valuesDict, userID, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT {3}, {1}\
               FROM `{2}` LEFT JOIN `{0}` ON `{2}`.`predictionID`=`{0}`.`id`\
               LEFT JOIN `Users` ON `{2}`.`authorID`=`Users`.`id`\
               WHERE `{2}`.`authorID`=%s".format(tableName(title), stringData(valuesDict), 
                                                 tableWager(title), stringData(defaultPredictionFields))
        cursor.execute(sql, (userID))
        return cursor.fetchall()

def getUserDatasetPredictionsOnlyUndecided(title, valuesDict, userID, connection):
    connection.commit()
    with connection.cursor() as cursor:
        sql = "SELECT {2}, {1}\
               FROM `{0}` LEFT JOIN `Users` ON `{0}`.`authorID`=`Users`.`id`\
               WHERE `{0}`.`authorID` = %s AND `result` IS NULL".format(tableName(title), 
                                                                        stringData(valuesDict), stringData(defaultPredictionFields))
        cursor.execute(sql, (userID))
        return cursor.fetchall()

def saveDatasetWager(title, wager, connection):
    with connection.cursor() as cursor:
        sql = "INSERT INTO `{}` ({}) VALUES ({})".format(
        	tableWager(title), stringData(wager), stringFormat(wager))
        print(sql)
        cursor.execute(sql, stringValues(wager))
    connection.commit()

def getDatasetWagers(title, valuesDict, predictionID, connection):
    with connection.cursor() as cursor:
        sql = "SELECT {2}, `Users`.`name`, `Users`.`user_id`, `Users`.`screen_name` as 'handle', {1}\
               FROM `{0}` LEFT JOIN `Users` ON `{0}`.`authorID` = `Users`.`id`\
               WHERE `predictionID` = %s".format(tableWager(title), stringData(valuesDict), stringData(defaultWagerFields))
        cursor.execute(sql, (predictionID))
        return cursor.fetchall()        
        
def getDatasetUserWager(title, valuesDict, predictionID, authorID, connection):
    with connection.cursor() as cursor:
        sql = "SELECT {2}, `Users`.`name`, `Users`.`user_id`, `Users`.`screen_name` as 'handle', {1}\
               FROM `{0}` LEFT JOIN `Users` ON `{0}`.`authorID` = `Users`.`id`\
               WHERE `predictionID` = %s AND `{0}`.`authorID` = %s".format(tableWager(title), 
                                                                           stringData(valuesDict),
                                                                           stringData(defaultWagerFields))
        print(sql)
        cursor.execute(sql, (predictionID, authorID))
        return cursor.fetchone()
