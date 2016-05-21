from yahoo_finance import Currency
import datetime

import lib.datasets

availableCurrencies = ["USDRUB",
                      "EURUSD",
                      "EURRUB",
                      "USDEUR",
                      "GBPRUB",
                      "JPYRUB",
                      "CADRUB",]

title = "yahooFinance"

bidDirections = {
    "g": (lambda a,b: a > b),
    "ge": (lambda a,b: a >= b),
    "l": (lambda a,b: a < b),
    "le": (lambda a,b: a <= b),
    "e": (lambda a,b: a == b),
}

predictionFields = {
        "currencies":"VARCHAR(10) NOT NULL",
    }

wagerFields = {
        "targetBid":"FLOAT NOT NULL",
        "bidDirection":"VARCHAR(3) NOT NULL",
    }


judgementFields = {
        "judgementBid":"FLOAT DEFAULT NULL",
        "judgementDate":"BIGINT(16) DEFAULT NULL",
    }

def processPrediction(prediction):
    if (prediction['currencies'] not in availableCurrencies):
        return None
    try:
        dueDate = datetime.datetime.utcfromtimestamp(prediction['dueDate'])
    except ValueError:
        return None
    dueDate = dueDate.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
    dueDateDelta = dueDate.date() - datetime.datetime.utcnow().date()
    if (dueDateDelta.days < 1):
        return None
    prediction['dueDate'] = dueDate.timestamp()
    return prediction

def processWager(wager):
    try:
        bid = int(wager['targetBid'])
    except ValueError:
        return None
    if (wager['bidDirection'] not in bidDirections):
        return None
    return wager

def getJudgement(prediction):
    value = _getValue(prediction['currencies'])
    judgement = {}
    if value:
        judgement['judgementBid'] = value['rate']
        judgement['judgementDate'] = value['tradeDateTime']
    else:
        return None
    return judgement


def decideJudgement(wager, judgement):
    if judgement:
        return bidDirections[wager['bidDirection']](float(wager['targetBid']), float(judgement['judgementBid']))


def getData():
    ret = {}
    ret["currencies"] = availableCurrencies
    return ret


def _getValue(symbol):
    ret = {}
    cur = Currency(symbol)
    ret['rate'] = cur.get_rate()
    if ret['rate']:
        time = datetime.datetime.strptime(cur.get_trade_datetime(), '%Y-%m-%d %H:%M:%S %Z%z')
        ret['tradeDateTime'] = int(time.timestamp())
        return ret
    return None
