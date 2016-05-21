import tornado.web
from tornado.options import options

import json
from datetime import datetime
import calendar
import traceback

import lib.db
import lib.util
import importlib

import os
import sys

dataSets = []
endpoints = []


def loadDataSet(dataSetFile):
    dataSetDescription = importlib.import_module("dataSets."+dataSetFile)

    dataSetDescription.predictionFields.update(lib.datasets.defaultPredictionNetworkFields)
    dataSetDescription.wagerFields.update(lib.datasets.defaultWagerNetworkFields)
    dataSetDescription.judgementFields.update(lib.datasets.defaultJudgementNetworkFields)

    class AddDataSetPrediction(tornado.web.RequestHandler):
        @tornado.gen.coroutine
        def post(self):
            input = self.request.body
            try:
                input = input.decode("utf-8")
                inputDict = json.loads(input)
            except:
                self.set_status(400)
                print("Failed to parser JSON")
                self.finish("Failed to parse JSON")
                return
            invalidRequest = False
            requestErrors = {}
            properKeys = ["key", "dueDate"] + list(dataSetDescription.predictionFields.keys()) + list(dataSetDescription.wagerFields.keys())
            if not(all(x in inputDict.keys() for x in properKeys)):
                self.send_error(400)
                print("All required fields not found")
                return
            user = lib.db.getUserByKey(inputDict['key'], options.connection)
            if not user:
                invalidRequest = True
                requestErrors['key'] = "User key not found"

            predictionData = dict([(x,inputDict[x]) for x in dataSetDescription.predictionFields.keys()])
            wagerData = dict([(x,inputDict[x]) for x in dataSetDescription.wagerFields.keys()])
            #print(predictionData)
            predictionData = dataSetDescription.processPrediction(predictionData)
            wagerData = dataSetDescription.processWager(wagerData)
            if not predictionData or not wagerData:
                invalidRequest = True
                print(predictionData)
                print(wagerData)
                requestErrors['data'] = "Invalid prediction data"
            if invalidRequest:
                self.set_status(400)
                self.finish(requestErrors)
                print(requestErrors)
                return
            prediction = lib.db_datasets.getDatasetPredictionByDataSet(dataSetDescription.title, predictionData, dataSetDescription.judgementFields, options.connection)
            if (not prediction):
                prediction = predictionData
                prediction['authorID']=user['id']
                prediction['url'] = lib.util.generateURL()
                while True:
                    try:
                        lib.db_datasets.saveDatasetPrediction(dataSetDescription.title, prediction, options.connection)
                        break
                    except Exception as e:
                        print('URL Colision found. Regenerating url')
                        print(e)
                        prediction['url'] = lib.util.generateURL()
                prediction["id"] = lib.db_datasets.getDatasetPredictionByURL(dataSetDescription.title, predictionData, prediction["url"],
                                                                             dataSetDescription.judgementFields, options.connection)["id"]

            wager = lib.db_datasets.getDatasetUserWager(dataSetDescription.title, dataSetDescription.wagerFields,
                                                        prediction['id'], user['id'], options.connection)
            if wager:
                print("Such wager and prediction already exists")
                self.set_status(200)
                status = {}
                status['url'] = prediction['url']
                self.finish(status)
                return
            timestamp = datetime.utcnow()
            wager = wagerData
            wager["authorID"] = user["id"]
            wager["predictionID"] = prediction["id"]
            wager["timestamp"] = calendar.timegm(timestamp.utctimetuple())
            
            lib.db_datasets.saveDatasetWager(dataSetDescription.title, wager, options.connection)
            self.set_status(200)
            status = {}
            status['url'] = prediction['url']
            self.finish(status)

    class AddDataSetWager(tornado.web.RequestHandler):
        @tornado.gen.coroutine
        def post(self, url):
            input = self.request.body
            invalidRequest = False
            requestErrors = {}
            prediction = lib.db_datasets.getDatasetPredictionByURL(dataSetDescription.title,
                                                                   dataSetDescription.predictionFields, url,
                                                                   dataSetDescription.judgementFields, options.connection)
            if not prediction:
                self.send_error(404)
                return
            try:
                input = input.decode("utf-8")
                inputDict = json.loads(input)
            except:
                self.set_status(400)
                print("Failed to parse JSON")
                self.finish("Failed to parse JSON")
                return
            properKeys = ["key"] + list(dataSetDescription.wagerFields.keys())
            if not(all(x in inputDict.keys() for x in properKeys)):
                self.set_status(400)
                print("Some required parameters were not found")
                print(inputDict.keys(), " != ",  properKeys)
                self.finish("Some required parameters were not found")
                return
            user = lib.db.getUserByKey(inputDict['key'], options.connection)
            if not user:
                requestErrors['author'] = "User not found"
                invalidRequest = True
            wagerData = dict([(x,inputDict[x]) for x in dataSetDescription.wagerFields.keys()])
            wagerData = dataSetDescription.processWager(wagerData);
            if not wagerData:
                self.set_status(400)
                self.finish(requestErrors)
                print(requestErrors)
                return
            wager = lib.db_datasets.getDatasetUserWager(dataSetDescription.title, dataSetDescription.wagerFields,
                                                        prediction['id'], user['id'], options.connection)
            if wager:
                self.send_error(403)
                return
            timestamp = datetime.utcnow()
            wager = wagerData
            wager["authorID"] = user["id"]
            wager["predictionID"] = prediction["id"]
            wager["timestamp"] = calendar.timegm(timestamp.utctimetuple())
            lib.db_datasets.saveDatasetWager(dataSetDescription.title, wager, options.connection)
            self.set_status(200)
            self.finish()
            return

    class ShowDataSetPrediction(tornado.web.RequestHandler):
        @tornado.gen.coroutine
        def get(self, url):
            prediction = lib.db_datasets.getDatasetPredictionByURL(dataSetDescription.title,
                                                                   dataSetDescription.predictionFields, url,
                                                                   dataSetDescription.judgementFields, options.connection)
            if prediction:
                prediction['wagers'] = lib.db_datasets.getDatasetWagers(dataSetDescription.title, dataSetDescription.wagerFields,
                                                                        prediction['id'], options.connection)
                self.finish(prediction)
                return
            self.send_error(404)

    class GetDataSetData(tornado.web.RequestHandler):
        @tornado.gen.coroutine
        def get(self):
            ret = dataSetDescription.getData()
            if ret:
                self.finish(ret)
                return
            else:
                self.send_error(404)

    dataSetDict = {
        "title": dataSetDescription.title,
        "description": dataSetDescription,
        "AddPredction": AddDataSetPrediction,
        "AddWager": AddDataSetWager,
        "GetPrediction": ShowDataSetPrediction,
        "GetData" : GetDataSetData,
    }

    lib.db_datasets.createDatasetTables(dataSetDescription.title, dataSetDescription.predictionFields,
                                        dataSetDescription.wagerFields, dataSetDescription.judgementFields, options.connection)

    dataSets.append(dataSetDict)


def loadDataSetDescription(dataSetFile):
    dataSetDescription = importlib.import_module("dataSets."+dataSetFile)

    dataSetDescription.predictionFields.update(lib.datasets.defaultPredictionNetworkFields)
    dataSetDescription.wagerFields.update(lib.datasets.defaultWagerNetworkFields)
    dataSetDescription.judgementFields.update(lib.datasets.defaultJudgementNetworkFields)

    return dataSetDescription


def turnDataSetsIntoTornadoEndpoints():
    for dataSet in dataSets:
        print(dataSet)
        endpoints.append((r"/prediction/" + dataSet["title"], dataSet["AddPredction"]))
        endpoints.append((r"/prediction/" + dataSet["title"] + "/wager/(.*)", dataSet["AddWager"]))
        endpoints.append((r"/prediction/" + dataSet["title"] + "/(.*)", dataSet["GetPrediction"]))
        endpoints.append((r"/" + dataSet["title"], dataSet["GetData"]))

def init():
    files = os.listdir("dataSets")
    for file in files:
        if file != "__init__.py":
            try:
                loadDataSet(file[:-3])
            except Exception as e:
                print(sys.exc_info())
                traceback.format_exc()
                pass

    turnDataSetsIntoTornadoEndpoints()

def getDatasetsDescriptions():
    ret = {}
    files = os.listdir("dataSets")
    for file in files:
        if file != "__init__.py":
            try:
                ret[file[:-3]] = loadDataSetDescription(file[:-3])
            except Exception as e:
                print(sys.exc_info())
                traceback.format_exc()
                pass
    return ret
