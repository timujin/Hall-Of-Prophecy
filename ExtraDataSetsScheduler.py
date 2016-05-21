import ExtraDataSets
import lib.db_datasets
import lib.util

from tornado.options import options
import pymysql
import gcm


def main(connection):
    gcm_service = gcm.GCM(options.gcm_key)
    descriptions = ExtraDataSets.getDatasetsDescriptions()
    for description in descriptions.values():
        print(description)
        predictionsToRate = lib.db_datasets.getDatasetDuePredictions(description.title, description.predictionFields, 10, connection) #Change number to rate to a more dynamic value
        print(predictionsToRate, sep='\n')
        for prediction in predictionsToRate:
            judgement = description.getJudgement(prediction)
            print(judgement)
            if judgement:
                regids = []
                message = {"message" : "Your prediction have been judged",
                            "link": r"https://" + options.hostname + r"/" + r"prediction/" + description.title + r"/" + prediction['url'],
                          }
                print("notification: ", message)
                judgement["result"] = 1
                lib.db_datasets.updateDatasetPredictionJudgement(description.title, prediction['id'], judgement, connection)
                wagers = lib.db_datasets.getDatasetWagersLocal(description.title, description.wagerFields, prediction['id'], connection)
                for wager in wagers:
                    isRight = description.decideJudgement(wager, judgement)
                    wager['wagerResult'] = isRight
                    print(wager)
                    if wager['regid']:
                        regids.append(wager['regid'])
                    lib.db_datasets.updateDatasetWagerJudgement(description.title, wager['authorID'], wager['predictionID'],wager['wagerResult'], connection)
                try:
                    gcm_service.json_request(registration_ids=regids, data = message, priority='high')
                except gcm.GCMException:
                    print('regid was not found')
                    pass
                connection.commit()

if __name__ == "__main__":
    lib.util.parse_config_file("config.conf")
    mysql = options.mysql
    server = mysql["server"]
    user = mysql["user"]
    password = mysql["password"]
    database = mysql["database"]
    conn = pymysql.connect(host=server, user=user, password=password, db=database,cursorclass=pymysql.cursors.DictCursor)
    main(conn)
