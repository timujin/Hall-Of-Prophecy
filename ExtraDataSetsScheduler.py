import ExtraDataSets
import lib.db_datasets
import pymysql

mysql = {"server":"127.0.0.1",
         "user":"hallofprophecy",
         "password":"hallofprophecy123",
         "port":"3306",
         "database":"hallofprophecy",
        }


def main(connection):
    descriptions = ExtraDataSets.getDatasetsDescriptions()
    for description in descriptions.values():
        print(description)
        predictionsToRate = lib.db_datasets.getDatasetDuePredictions(description.title, description.predictionFields, 10, connection) #Change number to rate to a more dynamic value
        print(predictionsToRate, sep='\n')
        for prediction in predictionsToRate:
            judgement = description.getJudgement(prediction)
            print(judgement)
            if judgement:
                judgement["result"] = 1
                lib.db_datasets.updateDatasetPredictionJudgement(description.title, prediction['id'], judgement, connection)
                wagers = lib.db_datasets.getDatasetWagers(description.title, description.wagerFields, prediction['id'], connection)
                for wager in wagers:
                    isRight = description.decideJudgement(wager, judgement)
                    wager['wagerResult'] = isRight
                    print(wager)
                    lib.db_datasets.updateDatasetWagerJudgement(description.title, wager['authorID'], wager['predictionID'],wager['wagerResult'], connection)
                connection.commit()

if __name__ == "__main__":
    server = mysql["server"]
    user = mysql["user"]
    password = mysql["password"]
    database = mysql["database"]
    conn = pymysql.connect(host=server, user=user, password=password, db=database,cursorclass=pymysql.cursors.DictCursor)
    main(conn)
