# General Libraries
import json
import time
import gspread
import platform
from gspread_formatting import *
from datetime import datetime, timedelta
from config import SERVICE_ACCOUNT_CREDENTIAL, TELEGRAM_CHANNEL_BOT_TOKEN, CHANNEL_IDS, SUPERUSERS, DUTY_GRP_ID, CHARLIE_Y2_ID, ID_INSTANCE, TOKEN_INSTANCE, CHARLIE_DUTY_CMDS, PERM_DUTY_CMDS, TIMETREE_USERNAME, TIMETREE_PASSWORD, TIMETREE_CALENDAR_ID
import traceback
import copy

# Google Drive API
# PyDrive library has been depracated since 2021
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Telegram API
import telegram
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackContext, ConversationHandler, MessageHandler, filters
import asyncio
import nest_asyncio
nest_asyncio.apply() # patch asyncio
import multiprocessing
import threading
MAX_MESSAGE_LENGTH = 4096

# Pytesseract OCR + Super Resolution Libraries
import pytesseract
import cv2
import numpy as np
from io import BytesIO
import io
from pdf2image import convert_from_bytes
import pyheif
from PIL import Image
import re

# WhatsApp API
import requests as rq
from whatsapp_api_client_python import API

# Intercepting TimeTree Responses
from pyppeteer import launch
from pyppeteer.errors import TimeoutError
from zoneinfo import ZoneInfo
foundResponse = False # Timetree response interception
responseContent = None

# Telegram Channel
telegram_bot = telegram.Bot(token=TELEGRAM_CHANNEL_BOT_TOKEN)

monthConversion = {"Jan":"01", "January":"01", "Feb":"02", "February":"02", "Mar":"03", "March":"03", "Apr":"04", "April":"04", "May":"05", "Jun":"06", "June":"06", "Jul":"07", "July":"07", "Aug":"08", "August":"08", "Sep":"09", "September":"09", "Oct":"10", "October":"10", "Nov":"11", "November":"11", "Dec":"12", "December":"12"} 
trooperRanks = ['PTE', 'PFC', 'LCP', 'CPL', 'CFC']
wospecRanks = ['3SG', '2SG', '1SG', 'SSG', 'MSG', '3WO', '2WO', '1WO', 'MWO', 'SWO', 'CWO']
officerRanks = ['2LT', 'LTA', 'CPT', 'MAJ', 'LTC', 'SLTC', 'COL', 'BG', 'MG', 'LG']

ENABLE_WHATSAPP_API = True # Flag to enable live whatsapp manipulation

masterUserRequests = dict()
rateLimit = 1 # number of seconds between commands per user
 
def send_tele_msg(msg, receiver_id = None,  parseMode = None, replyMarkup = None):

    """
        receiver_id -> SUPERUSERS/ALL/individual ID. None -> Send to everyone
        parseMode = 'MarkdownV2'
        replyMarkup for keyboards
    """
    if receiver_id is not None and not isinstance(receiver_id, str): receiver_id = str(receiver_id)
    
    if receiver_id is None:
        for _, value in CHANNEL_IDS.items():
            asyncio.run(send_telegram_bot_msg(msg, value, parseMode, replyMarkup))
    else:
        if receiver_id == "SUPERUSERS":  
            for _, value in SUPERUSERS.items():
                asyncio.run(send_telegram_bot_msg(msg, value, parseMode, replyMarkup))
        elif receiver_id == "ALL": 
            for _, value in CHANNEL_IDS.items():
                asyncio.run(send_telegram_bot_msg(msg, value, parseMode, replyMarkup))
        elif receiver_id in list(CHANNEL_IDS.values()): asyncio.run(send_telegram_bot_msg(msg, receiver_id, parseMode, replyMarkup))

async def send_telegram_bot_msg(msg, channel_id, parseMode, replyMarkup):
    try: 
        if parseMode is None: await telegram_bot.send_message(chat_id = channel_id, text = msg, read_timeout=5, reply_markup=replyMarkup)
        else: await telegram_bot.send_message(chat_id = channel_id, text = msg, read_timeout=5, parse_mode=parseMode, reply_markup=replyMarkup)
    except telegram.error.TimedOut:
        await asyncio.sleep(5)
        if parseMode is None: await telegram_bot.send_message(chat_id = channel_id, text = msg, read_timeout=5, reply_markup=replyMarkup)
        else: await telegram_bot.send_message(chat_id = channel_id, text = msg, read_timeout=5, parse_mode=parseMode, reply_markup=replyMarkup)

async def intercept_response(response):
    global foundResponse, responseContent
    if response.url == "https://timetreeapp.com/api/v1/calendar/{}/events/sync".format(TIMETREE_CALENDAR_ID):
        try: responseContent = await response.text()
        except Exception as e: print(f"Error fetching response body: {e}")
        foundResponse = True

async def timetreeResponses():
    global foundResponse
    if platform.system() == "Darwin": # macOS
        browser = await launch(headless=True)
    elif platform.system() == "Linux": # ubuntu
        # custom exec as oracle cloud uses ARM ubuntu
        # custom user data dir due to permission issues for default location 
        # headless mode does not work on oracle cloud linux for some reason       
        browser = await launch(headless=False, executablePath='/snap/bin/chromium', userDataDir='/home/pyppeteer')
    # Incognito to force login to be able to intercept
    context = await browser.createIncognitoBrowserContext()
    page = await context.newPage()
    
    # Intercept network responses
    page.on('response', lambda response: asyncio.ensure_future(intercept_response(response)))

    await page.goto('https://timetreeapp.com/signin?locale=en', timeout=10000)
    try:
        await page.waitForSelector('input[name="email"]', timeout=10000)
        await page.type('input[name="email"]', TIMETREE_USERNAME)
        await page.waitForSelector('input[name="password"]', timeout=10000)
        await page.type('input[name="password"]', TIMETREE_PASSWORD)
        await page.waitForSelector('button[type="submit"]', timeout=10000)
        await page.click('button[type="submit"]')
        await page.waitForNavigation(timeout=10000)
    except TimeoutError as e: print(f"Error: {e}")
    
    while not foundResponse: await asyncio.sleep(1)
    await browser.close()

def convertTimestampToDatetime(timestamp, tzinfo=ZoneInfo("Asia/Singapore")):
    
    """
        timestamp: Int or str object
        Returns datetime object.
    """
    
    if isinstance(timestamp, str): timestamp = int(timestamp)

    if timestamp >= 0:
        timestamp = timestamp/1000
        return datetime.fromtimestamp(timestamp, tzinfo)
    return datetime.fromtimestamp(0, tzinfo) + timedelta(seconds=int(timestamp))

def insertConductTracking(conductDate: str, conductName: str, conductColumn: int):
    
    gc = gspread.service_account_from_dict(SERVICE_ACCOUNT_CREDENTIAL)
    sheet = gc.open("Charlie Conduct Tracking")
    conductTrackingSheet = sheet.worksheet("CONDUCT TRACKING")

    startRow = 5
    endRow = 129

    def columnIndexToLetter(index):
        letter = ''
        while index > 0:
            index, remainder = divmod(index - 1, 26)
            letter = chr(65 + remainder) + letter
        return letter

    actualLetter = columnIndexToLetter(conductColumn)
    adjLetter = columnIndexToLetter(conductColumn+1)

    # clear the two columns to be written
    conductTrackingSheet.batch_clear(["{}:{}".format(actualLetter, adjLetter)])
    requests = []
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": conductTrackingSheet.id,  # Replace with your sheet ID
                "startRowIndex": 0,
                "endRowIndex": 1000,  # Adjust as needed
                "startColumnIndex": conductColumn-1,
                "endColumnIndex": conductColumn+1
            },
            "cell": {
                "userEnteredFormat": {}
            },
            "fields": "userEnteredFormat"
        }
    })
    rulesToRemove = []
    rules = get_conditional_format_rules(conductTrackingSheet)
    for index, rule in enumerate(rules, start = 0):
        ranges = rule.ranges
        if len(ranges) == 1 and ranges[0].startColumnIndex == conductColumn-1 and ranges[0].endColumnIndex == conductColumn: rulesToRemove.append(index)
    rulesToRemove.reverse()
    for idx in rulesToRemove:
        requests.append({
            "deleteConditionalFormatRule": {
                "sheetId": conductTrackingSheet.id,
                "index": idx
            }
        })
    body = {'requests': requests}
    creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_CREDENTIAL, ['https://www.googleapis.com/auth/spreadsheets'])
    service = build('sheets', 'v4', credentials=creds)
    response = service.spreadsheets().batchUpdate(spreadsheetId=conductTrackingSheet.spreadsheet_id, body=body).execute()

    grayCellBackground = CellFormat(backgroundColor=Color(0.85, 0.85, 0.85, 1))  
    redCellBackground = CellFormat(backgroundColor=Color(0.92, 0.27, 0.2, 1))
    greenCellBackground = CellFormat(backgroundColor=Color(0.2, 0.66, 0.33, 1))

    cellFormat = CellFormat(textFormat=TextFormat(bold=True), horizontalAlignment="CENTER", wrapStrategy="WRAP")
    format_cell_range(conductTrackingSheet, "{}2:{}4".format(actualLetter, actualLetter), cellFormat)
    conductTrackingSheet.update_cells([gspread.cell.Cell(2, conductColumn, conductDate),
                                       gspread.cell.Cell(4, conductColumn, conductName)])
    conductTrackingSheet.update_cell(3, conductColumn, '=IF(REGEXMATCH({}4, "HAPT"), "HAPT", "NON_HAPT")'.format(actualLetter))

    set_data_validation_for_cell_range(conductTrackingSheet, "{}{}:{}{}".format(actualLetter, startRow, actualLetter, endRow), DataValidationRule(BooleanCondition("BOOLEAN", []), showCustomUi=True)) 
    conductTrackingSheet.update(range_name="{}{}:{}{}".format(actualLetter, startRow, actualLetter, endRow), values=[[False] for _ in range(endRow - startRow + 1)])
    
    requests = []
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "booleanRule": {
                    "condition": {
                        "type": "TEXT_EQ",
                        "values": [{"userEnteredValue": "TRUE"}]
                    },
                    "format": {
                        "backgroundColor": {"red": 0.2, "green": 0.66, "blue": 0.33}  # Green color 0.2, 0.66, 0.33
                    }
                },
                "ranges": [{
                    "sheetId": conductTrackingSheet.id,
                    "startRowIndex": startRow-1,
                    "endRowIndex": endRow,
                    "startColumnIndex": conductColumn - 1,
                    "endColumnIndex": conductColumn
                }]
            },
            "index": 0
        }
    })
    # Rule to turn cells red when FALSE
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "booleanRule": {
                    "condition": {
                        "type": "TEXT_EQ",
                        "values": [{"userEnteredValue": "FALSE"}]
                    },
                    "format": {
                        "backgroundColor": {"red": 0.92, "green": 0.27, "blue": 0.2}  # Red color 0.92, 0.27, 0.2
                    }
                },
                "ranges": [{
                    "sheetId": conductTrackingSheet.id,
                    "startRowIndex": startRow-1,
                    "endRowIndex": endRow,
                    "startColumnIndex": conductColumn - 1,
                    "endColumnIndex": conductColumn
                }]
            },
            "index": 1
        }
    })
    ranges = [
        {
            "sheetId": conductTrackingSheet.id,  
            "startRowIndex": 1,
            "endRowIndex": 2,
            "startColumnIndex": conductColumn-1,
            "endColumnIndex": conductColumn
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 2,
            "endRowIndex": 3,
            "startColumnIndex": conductColumn-1,
            "endColumnIndex": conductColumn
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 3,
            "endRowIndex": 4,
            "startColumnIndex": conductColumn-1,
            "endColumnIndex": conductColumn
        },
        {
            "sheetId": conductTrackingSheet.id,  
            "startRowIndex": 1,
            "endRowIndex": 2,
            "startColumnIndex": conductColumn,
            "endColumnIndex": conductColumn+1
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 2,
            "endRowIndex": 3,
            "startColumnIndex": conductColumn,
            "endColumnIndex": conductColumn+1
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 3,
            "endRowIndex": 4,
            "startColumnIndex": conductColumn,
            "endColumnIndex": conductColumn+1
        }
    ]

    border_settings = {
        "top": {
            "style": "SOLID",
            "color": {
                "red": 0,
                "green": 0,
                "blue": 0
            }
        },
        "bottom": {
            "style": "SOLID",
            "color": {
                "red": 0,
                "green": 0,
                "blue": 0
            }
        },
        "left": {
            "style": "SOLID",
            "color": {
                "red": 0,
                "green": 0,
                "blue": 0
            }
        },
        "right": {
            "style": "SOLID",
            "color": {
                "red": 0,
                "green": 0,
                "blue": 0
            }
        }
    }

    for rng in ranges:
        requests.append({
            "updateBorders": {
                "range": rng,
                **border_settings
            }
        })
    
    ranges = [
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 17,
            "endRowIndex": 18,
            "startColumnIndex": conductColumn-1,
            "endColumnIndex": conductColumn
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 17,
            "endRowIndex": 18,
            "startColumnIndex": conductColumn,
            "endColumnIndex": conductColumn+1
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 42,
            "endRowIndex": 43,
            "startColumnIndex": conductColumn-1,
            "endColumnIndex": conductColumn
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 42,
            "endRowIndex": 43,
            "startColumnIndex": conductColumn,
            "endColumnIndex": conductColumn+1
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 67,
            "endRowIndex": 68,
            "startColumnIndex": conductColumn-1,
            "endColumnIndex": conductColumn
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 67,
            "endRowIndex": 68,
            "startColumnIndex": conductColumn,
            "endColumnIndex": conductColumn+1
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 93,
            "endRowIndex": 94,
            "startColumnIndex": conductColumn-1,
            "endColumnIndex": conductColumn
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 93,
            "endRowIndex": 94,
            "startColumnIndex": conductColumn,
            "endColumnIndex": conductColumn+1
        }
    ]

    border_settings = {
        "bottom": {
            "style": "SOLID",
            "color": {
                "red": 0,
                "green": 0,
                "blue": 0
            }
        }
    }
    for rng in ranges:
        requests.append({
            "updateBorders": {
                "range": rng,
                **border_settings
            }
        })

    body = {'requests': requests}
    response = service.spreadsheets().batchUpdate(spreadsheetId=conductTrackingSheet.spreadsheet_id, body=body).execute()
    format_cell_range(conductTrackingSheet, "{}{}:{}{}".format(adjLetter, startRow, adjLetter, endRow), grayCellBackground)

def updateConductTracking(receiver_id = None):
    try: 
        global foundResponse, responseContent
        foundResponse = False
        asyncio.get_event_loop().run_until_complete(timetreeResponses())

        if responseContent is not None: 
            responseContent = json.loads(responseContent) # conversion from str response to a dict
            events = responseContent['events']
            futureEvents = dict()
            pattern = r'(?<!\d)/(?!\d)'
            for event in events:
                # Look for events that are labelled PT activities (id:8) starting from today
                if event['label_id'] == 8 and convertTimestampToDatetime(event['start_at']).date()>=datetime.now(tz=ZoneInfo("Asia/Singapore")).date():
                    startDateTime = convertTimestampToDatetime(event['start_at'])
                    startDateTime = "{}{}{}".format(("0" + str(startDateTime.day)) if startDateTime.day < 10 else (str(startDateTime.day)), (("0" + str(startDateTime.month)) if startDateTime.month < 10 else (str(startDateTime.month))), str(startDateTime.year).replace("20", ""))
                    slave = re.split(pattern, event['title']) # if there are multiple conducts i.e make up training with main body training
                    if startDateTime not in futureEvents: futureEvents[startDateTime] = []
                    for i in slave:
                        futureEvents[startDateTime].append(i)

        changesMade = True
        gc = gspread.service_account_from_dict(SERVICE_ACCOUNT_CREDENTIAL)
        sheet = gc.open("Charlie Conduct Tracking")
        while changesMade:
            changesMade = False
            conductTrackingSheet = sheet.worksheet("CONDUCT TRACKING")
            allDates = conductTrackingSheet.row_values(2)
            allConducts = conductTrackingSheet.row_values(4)
            prevDateTimeObject = None
            currentIndex = None
            futureEvents = dict(sorted(futureEvents.items(), key=lambda item: datetime.strptime(item[0], "%d%m%y")))
            for timetreeDate, timetreeConducts in futureEvents.items():
                for timetreeConduct in timetreeConducts:
                    timetreeDateObject = datetime.strptime(timetreeDate.replace(" ", ""), "%d%m%y")
                    correctConduct = False
                    for index, date in enumerate(allDates, start = 0):
                        if date == '': continue
                        if currentIndex is not None and index<=currentIndex: continue
                        dateObject = datetime.strptime(date.replace(" ", ""), "%d%m%y")
                        if dateObject.date() < datetime.now().date(): continue
                        else: currentIndex = index

                        conduct = allConducts[index]
                        if "SST" in conduct: continue # ignore SSTs

                        conduct = conduct.replace(" (HAPT)", "")
                        conduct = conduct.replace("(HAPT)", "")
                        conduct = conduct.replace("\n", "")
                        slave = copy.deepcopy(conduct)
                        conduct = conduct.replace(" ", "")
                        if date == timetreeDate and conduct in timetreeConduct.replace(" ", ""): 
                            # print("Correct: ", slave, date) # no actions needed
                            correctConduct = True
                            break
                        elif date == timetreeDate and conduct not in timetreeConduct.replace(" ", ""):
                            # print("Not on timetree: ", slave, date) # conduct that is not on timetree
                            send_tele_msg("Removing {} on {}".format(slave, date), receiver_id="SUPERUSERS")
                            conductTrackingSheet.delete_columns(index+1, index+2)
                            changesMade = True
                            break
                        elif dateObject > timetreeDateObject: # conduct not added to conduct tracking sheets
                            # print("Missing conducts: ", timetreeConduct, timetreeDate)
                            send_tele_msg("Adding {} on {}".format(timetreeConduct, timetreeDate), receiver_id="SUPERUSERS")
                            requests = [{
                                'insertDimension': {
                                    'range': {
                                        'sheetId': conductTrackingSheet.id,
                                        'dimension': 'COLUMNS',
                                        'startIndex': index, 
                                        'endIndex': index+1
                                    },
                                    'inheritFromBefore': False
                                }
                            }]
                            requests.append({
                                'insertDimension': {
                                    'range': {
                                        'sheetId': conductTrackingSheet.id,
                                        'dimension': 'COLUMNS',
                                        'startIndex': index+1, 
                                        'endIndex': index+2
                                    },
                                    'inheritFromBefore': False
                                }
                            })
                            body = {'requests': requests}
                            creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_CREDENTIAL, ['https://www.googleapis.com/auth/spreadsheets'])
                            service = build('sheets', 'v4', credentials=creds)
                            response = service.spreadsheets().batchUpdate(spreadsheetId=conductTrackingSheet.spreadsheet_id, body=body).execute()
                            insertConductTracking(timetreeDate, timetreeConduct+"(HAPT)", index+1)
                            changesMade = True
                            break
                        elif date != timetreeDate or conduct not in timetreeConduct.replace(" ", ""):
                            # print("Not on timetree: ", slave, date) # conduct that is not on timetree
                            send_tele_msg("Removing {} on {}".format(slave, date), receiver_id="SUPERUSERS")
                            conductTrackingSheet.delete_columns(index+1, index+2)
                            changesMade = True
                            # do not break here 
                        
                    if changesMade: break
                    
                    # latest conduct is before current date or is already correct
                    if currentIndex is None: currentIndex = len(allDates)-1 # never make changes during first pass
                    if not correctConduct and currentIndex is not None and currentIndex+1 == len(allDates): # never make changes during subsequent passes
                        # print("Missing conducts: ", timetreeConduct, timetreeDate)
                        send_tele_msg("Adding {} on {}".format(timetreeConduct, timetreeDate), receiver_id="SUPERUSERS")
                        requests = [{
                            'insertDimension': {
                                'range': {
                                    'sheetId': conductTrackingSheet.id,
                                    'dimension': 'COLUMNS',
                                    'startIndex': currentIndex+2, 
                                    'endIndex': currentIndex+3
                                },
                                'inheritFromBefore': False
                            }
                        }]
                        requests.append({
                            'insertDimension': {
                                'range': {
                                    'sheetId': conductTrackingSheet.id,
                                    'dimension': 'COLUMNS',
                                    'startIndex': currentIndex+3, 
                                    'endIndex': currentIndex+4
                                },
                                'inheritFromBefore': False
                            }
                        })
                        body = {'requests': requests}
                        creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_CREDENTIAL, ['https://www.googleapis.com/auth/spreadsheets'])
                        service = build('sheets', 'v4', credentials=creds)
                        response = service.spreadsheets().batchUpdate(spreadsheetId=conductTrackingSheet.spreadsheet_id, body=body).execute()
                        insertConductTracking(timetreeDate, timetreeConduct+"(HAPT)", currentIndex+3)
                        changesMade = True
                        break
                    prevDateTimeObject = dateObject
                if changesMade: break
        send_tele_msg("Finished", receiver_id="SUPERUSERS")
    except Exception as e:
        print("Encountered exception:\n{}".format(traceback.format_exc()))
        send_tele_msg("Encountered exception:\n{}".format(traceback.format_exc()), receiver_id="SUPERUSERS")

def checkMcStatus(receiver_id = None):

    startTm = time.time()
    try:
        # Get Coy MC/Status list from parade state
        gc = gspread.service_account_from_dict(SERVICE_ACCOUNT_CREDENTIAL)
        sheet = gc.open("3GDS CHARLIE PARADE STATE")
        cCoySheet = sheet.worksheet("C COY")
        allValues = cCoySheet.get_all_values()
        allValues = list(zip(*allValues))
        platoonMc = allValues[5] # column F
        sectionMc = allValues[6] # column G
        sheetMcList = allValues[8] # column I
        mcStartDates = allValues[9] # column J
        mcEndDates = allValues[10] # column K
        mcReason = allValues[11] # column L
        platoonStatus = allValues[26] # column AA
        sectionStatus = allValues[27] # column AB
        sheetStatusList = allValues[29] # column AD
        statusStartDates = allValues[30] # column AE
        statusEndDates = allValues[31] # column AF
        statusReason = allValues[32] # column AG
        # assert len(sheetMcList) == len(mcStartDates) == len(mcEndDates), "Num of names and MC dates do not tally"
        # assert len(sheetStatusList) == len(statusStartDates) == len(statusEndDates), "Num of names and status dates do not tally"
        foundHeader = False
        mcList = []
        for index, name in enumerate(sheetMcList, start = 0):
            if not foundHeader and name == 'NAME': 
                foundHeader = True
                continue
            if foundHeader and name != '' and mcStartDates[index] != '#REF!' and mcEndDates[index] != '#REF!' and mcReason[index] != '#REF!': 
                mcList.append((name, mcStartDates[index], (mcEndDates[index] if mcEndDates[index] != '' else '-'), platoonMc[index], sectionMc[index], "MC", mcReason[index]))
        foundHeader = False
        statusList = []
        for index, name in enumerate(sheetStatusList, start = 0):
            if not foundHeader and name == 'NAME': 
                foundHeader = True
                continue
            if foundHeader and name != '' and statusStartDates[index] != '#REF!' and statusEndDates[index] != '#REF!' and statusReason[index] != '#REF!': 
                statusList.append((name, statusStartDates[index], (statusEndDates[index] if statusEndDates[index] != '' else '-'), platoonStatus[index], sectionStatus[index], "Status", statusReason[index]))
        
        paradeStateMcList = copy.deepcopy(mcList)
        paradeStateMasterList = copy.deepcopy(statusList)
        paradeStateMasterList.extend(paradeStateMcList)

        # read existing MC/Status entries from mc lapse sheet
        mcStatusLapseSheet = gc.open("MC/Status Lapse Tracking")
        mcLapse = mcStatusLapseSheet.worksheet("MC")
        allValues = mcLapse.get_all_values()
        allValues = list(zip(*allValues))
        sheetMcList = list(filter(None, allValues[0])) # column A
        mcStartDates = list(filter(None, allValues[1])) # column B
        mcEndDates = list(filter(None, allValues[2])) # column C
        platoon = list(filter(None, allValues[3])) # column D
        section = list(filter(None, allValues[4])) # column E
        mcReason = list(filter(None, allValues[5])) # column F
        assert len(sheetMcList) == len(mcStartDates) == len(mcEndDates), "Num of names and MC dates do not tally"
        foundHeader = False
        existingMcList = []
        for index, name in enumerate(sheetMcList, start = 0):
            if not foundHeader and name == 'NAME': 
                foundHeader = True
                continue
            if foundHeader: existingMcList.append((name, mcStartDates[index], mcEndDates[index], platoon[index], section[index], "MC", mcReason[index]))
        mcList.extend(existingMcList)
        mcList = list(set(mcList)) # remove duplicate entries

        statusLapse = mcStatusLapseSheet.worksheet("Status")
        allValues = statusLapse.get_all_values()
        allValues = list(zip(*allValues))
        sheetStatusList = list(filter(None, allValues[0])) # column A
        statusStartDates = list(filter(None, allValues[1])) # column B
        statusEndDates = list(filter(None, allValues[2])) # column C
        platoon = list(filter(None, allValues[3])) # column D
        section = list(filter(None, allValues[4])) # column E
        statusReason = list(filter(None, allValues[5])) # column F
        assert len(sheetStatusList) == len(statusStartDates) == len(statusEndDates), "Num of names and status dates do not tally"
        foundHeader = False
        existingStatusList = []
        for index, name in enumerate(sheetStatusList, start = 0):
            if not foundHeader and name == 'NAME': 
                foundHeader = True
                continue
            if foundHeader: existingStatusList.append((name, statusStartDates[index], statusEndDates[index], platoon[index], section[index], "Status", statusReason[index]))
        statusList.extend(existingStatusList)
        statusList = list(set(statusList)) # remove duplicate entries

        # Get already checked MC/Status entries
        mcStatusChecked = mcStatusLapseSheet.worksheet("Checked")
        allValues = mcStatusChecked.get_all_values()
        allValues = list(zip(*allValues))
        sheetMcStatusList = list(filter(None, allValues[0])) # column A
        mcStatusStartDates = list(filter(None, allValues[1])) # column B
        mcStatusEndDates = list(filter(None, allValues[2])) # column C
        platoon = list(filter(None, allValues[3])) # column D
        section = list(filter(None, allValues[4])) # column E
        type = list(filter(None, allValues[5])) # column F
        mcStatusReason = list(filter(None, allValues[6])) # column G
        foundHeader = False
        checkedMcStatus = []
        for index, name in enumerate(sheetMcStatusList, start = 0):
            if not foundHeader and name == 'NAME': 
                foundHeader = True
                continue
            if foundHeader: checkedMcStatus.append((name, mcStatusStartDates[index], mcStatusEndDates[index], platoon[index], section[index], type[index], mcStatusReason[index]))

        # get MC/Status files in google drive
        gauth = GoogleAuth()
        creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_CREDENTIAL, ['https://www.googleapis.com/auth/drive'])
        service = build('drive', 'v3', credentials=creds)
        gauth.credentials = creds
        drive = GoogleDrive(gauth)

        masterList = copy.deepcopy(mcList)
        masterList.extend(statusList)

        lapseMcList = []
        lapseStatusList = []
        possibleMcList = []
        possibleStatusList = []
        foundMcStatusFiles = []
        for count, mcStatus in enumerate(masterList, start = 1):
            rank = mcStatus[0].split(' ')[0]
            folderName = mcStatus[0].replace(rank + " ", "") # remove rank from name
            folderList = drive.ListFile({'q': f"title='{folderName}' and trashed=false"}).GetList()
            assert len(folderList) != 0, "No MC folder of the name {} is present".format(folderName)
            assert len(folderList) == 1, "More than one MC folder of the name {} is present".format(folderName)
            folderId = folderList[0]['id']
            if mcStatus in checkedMcStatus: 
                foundMcStatusFiles.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                continue
            driveMcStatusList = drive.ListFile({'q': f"'{folderId}' in parents and trashed=false"}).GetList()
            if mcStatus[1] == "#REF!": continue # parade state ref errors. skip iteration
            tmp = mcStatus[1].split(' ')
            try: tmp[1] = monthConversion[tmp[1]] # convert MMM to MM
            except IndexError: 
                send_tele_msg("Unable to perform month conversion for {}".format(mcStatus[1]))
                raise IndexError
            startDate = ''.join(tmp)
            startDateTime = datetime.strptime(startDate, "%d%m%y").date()
            if mcStatus[2] != '-': # no end date/permanent
                tmp = mcStatus[2].split(' ')
                try: tmp[1] = monthConversion[tmp[1]] # convert MMM to MM
                except IndexError: 
                    send_tele_msg("Unable to perform month conversion for {}".format(mcStatus[1]))
                    raise IndexError
                endDate = ''.join(tmp)
            else: endDate = mcStatus[2]
            pattern1 = r"(?<!\d)(\d{1,2}/\d{1,2}/\d{4})(?!\d)"
            pattern2 = r"(?<!\d)(\d{1,2}-(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)-\d{4})(?!\d)"
            foundMcStatusFile = False
            for driveMcStatus in driveMcStatusList:
                tmp = driveMcStatus['createdDate'].split('T')[0].split('-')
                tmp.reverse()
                tmp[2] = tmp[2].replace("20", "")
                uploadDate = "".join(tmp)
                uploadDateTime = datetime.strptime(uploadDate, "%d%m%y").date()
                if startDate in driveMcStatus['title'] and endDate in driveMcStatus['title']: # found MC file
                    foundMcStatusFile = True
                    break
                elif uploadDateTime >= startDateTime: # possible MC file with upload date later than start of MC date
                    request = service.files().get_media(fileId=driveMcStatus['id'])
                    imageIo = io.BytesIO()
                    downloader = MediaIoBaseDownload(imageIo, request)
                    done = False
                    while done is False:
                        status, done = downloader.next_chunk()
                    imageIo.seek(0)
                    if driveMcStatus['fileExtension'].upper() == 'PDF': # PDF Formats
                        images = convert_from_bytes(imageIo.read())
                        pil_image = images[0]  # Convert the first page only
                        img = cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGB2BGR)
                    elif driveMcStatus['fileExtension'].upper() == 'HEIC': # HEIC Formats
                        heif_file = pyheif.read(imageIo.read())
                        image = Image.frombytes(
                            heif_file.mode, 
                            heif_file.size, 
                            heif_file.data,
                            "raw",
                            heif_file.mode,
                            heif_file.stride,
                        )
                        img = np.array(image) # Ensure it's in RGB mode
                    else: #jpg/jpeg formats
                        file_data = BytesIO(request.execute())
                        imageArray = np.asarray(bytearray(file_data.read()), dtype="uint8")
                        img = cv2.imdecode(imageArray, cv2.IMREAD_COLOR)

                    imageText = pytesseract.image_to_string(img)
                    dates_format1 = re.findall(pattern1, imageText)
                    dates_format2 = re.findall(pattern2, imageText)
                    allDates = []
                    for date in dates_format1:
                        tmp = date.replace("/", "")
                        tmp = tmp.replace("2023", "23")
                        tmp = tmp.replace("2024", "24")
                        tmp = tmp.replace("2025", "25")
                        allDates.append(tmp)
                    for date in dates_format2:
                        tmp = date.split('-')
                        try: tmp[1] = monthConversion[tmp[1]]
                        except KeyError: continue
                        tmp[2] = tmp[2].replace("2023", "23")
                        tmp[2] = tmp[2].replace("2024", "24")
                        tmp[2] = tmp[2].replace("2025", "25")
                        allDates.append("".join(tmp))
                    if len(allDates) < 2: # not enough dates detected. try image processing
                        img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
                        img = cv2.GaussianBlur(img, (3, 3), 0)
                        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 1))
                        img = cv2.dilate(img, kernel, iterations=1)
                        img = cv2.erode(img, kernel, iterations=1)
                        imageText = pytesseract.image_to_string(img)
                        dates_format1 = re.findall(pattern1, imageText)
                        dates_format2 = re.findall(pattern2, imageText)
                        allDates = []
                        for date in dates_format1:
                            tmp = date.replace("/", "")
                            tmp = tmp.replace("2023", "23")
                            tmp = tmp.replace("2024", "24")
                            tmp = tmp.replace("2025", "25")
                            allDates.append(tmp)
                        for date in dates_format2:
                            tmp = date.split('-')
                            try: tmp[1] = monthConversion[tmp[1]]
                            except KeyError: continue
                            tmp[2] = tmp[2].replace("2023", "23")
                            tmp[2] = tmp[2].replace("2024", "24")
                            tmp[2] = tmp[2].replace("2025", "25")
                            allDates.append("".join(tmp))
                    if startDate in allDates and endDate in allDates: 
                        foundMcStatusFile = True
                        break
                    else: 
                        if mcStatus[5] == "MC": possibleMcList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                        elif mcStatus[5] == "Status": possibleStatusList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))

            if not foundMcStatusFile: 
                if mcStatus[5] == "MC": lapseMcList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId))) 
                elif mcStatus[5] == "Status": lapseStatusList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))

            else: foundMcStatusFiles.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))

            timeElapsed = time.time()-startTm
            if timeElapsed > 120: # 2 minutes
                send_tele_msg("Current progress: {:.1f}%".format((count/len(masterList))*100), receiver_id=receiver_id)
                startTm = time.time() 

        # write lapsed mc/status list to mc/status lapse tracking sheet
        mcLapse.batch_clear(['A2:G1000'])
        statusLapse.batch_clear(['A2:G1000'])
        if len(lapseMcList) == 0: send_tele_msg("No MC lapses", receiver_id=receiver_id)
        else:
            lapseMcList = sorted(lapseMcList, key=lambda x: datetime.strptime(x[1], "%d %b %y"), reverse=True)
            tele_msg = "Lapsed MC List:"
            for index, mc in enumerate(lapseMcList, start = 2):
                mcLapse.update_cells([gspread.cell.Cell(index, 1, mc[0]),
                                      gspread.cell.Cell(index, 2, mc[1]),
                                      gspread.cell.Cell(index, 3, mc[2]),
                                      gspread.cell.Cell(index, 4, mc[3]),
                                      gspread.cell.Cell(index, 5, mc[4]),
                                      gspread.cell.Cell(index, 6, mc[6]),
                                      gspread.cell.Cell(index, 7, mc[7])])
                if mc in possibleMcList: tele_msg = "\n".join([tele_msg, "{}".format(mc[0]) + ((" (P{}S{})".format(mc[3], mc[4])) if mc[3] != "HQ" else (" (HQ)")), "{} - {} (Possible MC found)\n{}\n{}\n".format(mc[1], mc[2], mc[6], mc[7])])
                else: tele_msg = "\n".join([tele_msg, "{}".format(mc[0]) + ((" (P{}S{})".format(mc[3], mc[4])) if mc[3] != "HQ" else (" (HQ)")), "{} - {}\n{}\n{}\n".format(mc[1], mc[2], mc[6], mc[7])])
            send_tele_msg(tele_msg, receiver_id=receiver_id)
        
        if len(lapseStatusList) == 0: send_tele_msg("No Status lapses", receiver_id=receiver_id)
        else:
            lapseStatusList = sorted(lapseStatusList, key=lambda x: datetime.strptime(x[1], "%d %b %y"), reverse=True)
            tele_msg = "Lapsed Status List:"
            for index, status in enumerate(lapseStatusList, start = 2):
                statusLapse.update_cells([gspread.cell.Cell(index, 1, status[0]),
                                          gspread.cell.Cell(index, 2, status[1]),
                                          gspread.cell.Cell(index, 3, status[2]),
                                          gspread.cell.Cell(index, 4, status[3]),
                                          gspread.cell.Cell(index, 5, status[4]),
                                          gspread.cell.Cell(index, 6, status[6]),
                                          gspread.cell.Cell(index, 7, status[7])])
                if status in possibleStatusList: tele_msg = "\n".join([tele_msg, "{}".format(status[0]) + ((" (P{}S{})".format(status[3], status[4])) if status[3] != "HQ" else (" (HQ)")), "{} - {} (Possible status found)\n{}\n{}\n".format(status[1], status[2], status[6], status[7])])
                else: tele_msg = "\n".join([tele_msg, "{}".format(status[0]) + ((" (P{}S{})".format(status[3], status[4])) if status[3] != "HQ" else (" (HQ)")), "{} - {}\n{}\n{}\n".format(status[1], status[2], status[6], status[7])])
                if len(tele_msg) > MAX_MESSAGE_LENGTH-1000:
                    send_tele_msg(tele_msg, receiver_id=receiver_id)
                    tele_msg = "Lapsed Status List:"
            send_tele_msg(tele_msg, receiver_id=receiver_id)
    
        # Write checked mc/status files to avoid repeated checks
        mcStatusChecked.batch_clear(['A2:H1000'])
        for index, status in enumerate(foundMcStatusFiles, start = 2):
            mcStatusChecked.update_cells([gspread.cell.Cell(index, 1, status[0]), 
                                          gspread.cell.Cell(index, 2, status[1]),
                                          gspread.cell.Cell(index, 3, status[2]),
                                          gspread.cell.Cell(index, 4, status[3]),
                                          gspread.cell.Cell(index, 5, status[4]),
                                          gspread.cell.Cell(index, 6, status[5]),
                                          gspread.cell.Cell(index, 7, status[6]),
                                          gspread.cell.Cell(index, 8, status[7])])

    except Exception as e:
        print("Encountered exception:\n{}".format(traceback.format_exc()))
        send_tele_msg("Encountered exception:\n{}".format(traceback.format_exc()))

def checkConductTracking(receiver_id = None):

    try:
        gc = gspread.service_account_from_dict(SERVICE_ACCOUNT_CREDENTIAL)
        sheet = gc.open("Charlie Conduct Tracking")
        conductTrackingSheet = sheet.worksheet("CONDUCT TRACKING")
        allDates = conductTrackingSheet.row_values(2)
        currentDate = "{}{}{}".format(("0" + str(datetime.now().day)) if datetime.now().day < 10 else (str(datetime.now().day)), (("0" + str(datetime.now().month)) if datetime.now().month < 10 else (str(datetime.now().month))), str(datetime.now().year).replace("20", ""))
        colIndexes = []
        foundIndexes = False
        for index, date in enumerate(allDates, start = 1):
            if date == currentDate: 
                colIndexes.append(index)
                foundIndexes = True
            elif foundIndexes and date != currentDate and date != "": break
        if len(colIndexes) == 0: send_tele_msg("No conducts today", receiver_id=receiver_id)

        # for each conduct TODAY
        for index in colIndexes:
            colValues = conductTrackingSheet.col_values(index)
            ajColValues = conductTrackingSheet.col_values(index+1) # reasons for absence
            #1-Date, 2-HAPT/NON HAPT, 3-Conduct, 4-17-HQ, 18-42-p7, 43-67-p8, 68-93-p9, 94-146-cmd
            conductDate = colValues[1]
            conductName = colValues[3]
            hqTrackingStatus = []
            p7TrackingStatus = []
            p8TrackingStatus = []
            p9TrackingStatus = []
            for row, colValue in enumerate(colValues, start = 0):
                if len(ajColValues)-1 < row: ajColValues.append("")
                if row<3:continue
                if row>=4 and row<=17: # HQ
                    if (colValue != 'FALSE' or ajColValues[row] != ""): hqTrackingStatus.append(True)
                    else: hqTrackingStatus.append(False)
                elif row>=18 and row<=42:
                    if (colValue != 'FALSE' or ajColValues[row] != ""): p7TrackingStatus.append(True)
                    else: p7TrackingStatus.append(False)
                elif row>=43 and row<=67:
                    if (colValue != 'FALSE' or ajColValues[row] != ""): p8TrackingStatus.append(True)
                    else: p8TrackingStatus.append(False)
                elif row>=68 and row<=93:
                    if (colValue != 'FALSE' or ajColValues[row] != ""): p9TrackingStatus.append(True)
                    else: p9TrackingStatus.append(False)
            updatedMsg = "{} {}: ".format(conductDate, conductName)
            if set(hqTrackingStatus) == {True} and set(p7TrackingStatus) == {True} and set(p8TrackingStatus) == {True} and set(p9TrackingStatus) == {True}: updatedMsg = "".join([updatedMsg, "All updated"])
            else:
                if set(hqTrackingStatus) == {True}: updatedMsg = "\n".join([updatedMsg, "HQ updated"])
                if not set(hqTrackingStatus) == {True} and not set(hqTrackingStatus) == {False}: updatedMsg = "\n".join([updatedMsg, "HQ partially updated"])
                if set(hqTrackingStatus) == {False}: updatedMsg = "\n".join([updatedMsg, "HQ not updated"])
                if set(p7TrackingStatus) == {True}: updatedMsg = "\n".join([updatedMsg, "P7 updated"])
                if not set(p7TrackingStatus) == {True} and not set(p7TrackingStatus) == {False}: updatedMsg = "\n".join([updatedMsg, "P7 partially updated"])
                if set(p7TrackingStatus) == {False}: updatedMsg = "\n".join([updatedMsg, "P7 not updated"])
                if set(p8TrackingStatus) == {True}: updatedMsg = "\n".join([updatedMsg, "P8 updated"])
                if not set(p8TrackingStatus) == {True} and not set(p8TrackingStatus) == {False}: updatedMsg = "\n".join([updatedMsg, "P8 partially updated"])
                if set(p8TrackingStatus) == {False}: updatedMsg = "\n".join([updatedMsg, "P8 not updated"])
                if set(p9TrackingStatus) == {True}: updatedMsg = "\n".join([updatedMsg, "P9 updated"])
                if not set(p9TrackingStatus) == {True} and not set(p9TrackingStatus) == {False}: updatedMsg = "\n".join([updatedMsg, "P9 partially updated"])
                if set(p9TrackingStatus) == {False}: updatedMsg = "\n".join([updatedMsg, "P9 not updated"])
                if "not updated" in updatedMsg or "partially updated" in updatedMsg: updatedMsg = "\n".join([updatedMsg, "https://docs.google.com/spreadsheets/d/1TBHzKqmEHmyONaMQJoqt4HWwdsoY0pRSEv8WSoXDmyw/edit?gid=1000647342#gid=1000647342"])
            send_tele_msg(updatedMsg, receiver_id=receiver_id)
        
    except Exception as e:
        print("Encountered exception:\n{}".format(traceback.format_exc()))
        send_tele_msg("Encountered exception:\n{}".format(traceback.format_exc()))

def updateWhatsappGrp(cet, receiver_id = None):
    
    dutyGrpId = DUTY_GRP_ID
    greenAPI = API.GreenAPI(ID_INSTANCE, TOKEN_INSTANCE)

    # Getting duty commanders and date and FP timing from CET
    try: 
        cetSegments = cet.split('\n')
        CDS = None
        PDS7 = None
        PDS8 = None
        PDS9 = None
        newDate = None
        fpTime = None
        noFPTimeFound = True
        for segment in cetSegments:
            if "Duty Personnel" in segment: newDate = segment.split('[')[1].split('/')[0].replace(" ", "")
            if 'FP' in segment: fpTime = segment.split(" -")[0]
            if 'CDS' in segment: CDS = segment.split(': ')[-1].replace(" ", "").replace("3SG", "").replace("2SG", "")
            elif 'PDS7' in segment: PDS7 = segment.split(': ')[-1].replace(" ", "").replace("3SG", "").replace("2SG", "")
            elif 'PDS8' in segment: PDS8 = segment.split(': ')[-1].replace(" ", "").replace("3SG", "").replace("2SG", "")
            elif 'PDS9' in segment: PDS9 = segment.split(': ')[-1].replace(" ", "").replace("3SG", "").replace("2SG", "")
            if fpTime is not None: 
                noFPTimeFound = False
                cetQueue.put((newDate, fpTime, receiver_id))
        if noFPTimeFound: 
            cetQueue.put(None)
            send_tele_msg("No FP time found. CDS reminder not scheduled.", receiver_id="SUPERUSERS")
        if (CDS is None and PDS7 is None and PDS8 is None and PDS9 is None) or newDate is None: raise Exception
    except Exception as e: 
        send_tele_msg("Unrecognized CET", receiver_id="SUPERUSERS")
        return
    
    # Renaming of group name
    if ENABLE_WHATSAPP_API: greenAPI.groups.updateGroupName(dutyGrpId, "{} DUTY CDS/PDS".format(newDate))
        
    # Removal of previous duty members not in next duty 
    url = "https://api.green-api.com/waInstance{}/getGroupData/{}".format(ID_INSTANCE, TOKEN_INSTANCE)
    payload = {
        "groupId": dutyGrpId  
    }
    response = rq.post(url, json=payload)
    if response.status_code == 200: group_data = response.json()
    else: 
        send_tele_msg("Failed to retrieve group data: {}\nAborting updating duty group.".format(response.json()), receiver_id="SUPERUSERS")
        group_data = None
        return
    if group_data is not None: 
        nextDutyCmds = []
        try: nextDutyCmds.append(CHARLIE_DUTY_CMDS[CDS])
        except KeyError: send_tele_msg("Unknown CDS: {}".format(CDS), receiver_id="SUPERUSERS")
        try: nextDutyCmds.append(CHARLIE_DUTY_CMDS[PDS7])
        except KeyError: send_tele_msg("Unknown PDS7: {}".format(PDS7), receiver_id="SUPERUSERS")
        try: nextDutyCmds.append(CHARLIE_DUTY_CMDS[PDS8])
        except KeyError: send_tele_msg("Unknown PDS8: {}".format(PDS8), receiver_id="SUPERUSERS")
        try: nextDutyCmds.append(CHARLIE_DUTY_CMDS[PDS9])
        except KeyError: send_tele_msg("Unknown PDS9: {}".format(PDS9), receiver_id="SUPERUSERS")
        allMembers = group_data['participants']
        for member in allMembers:
            memberId = member['id'].split('@c.us')[0][2:]
            if memberId not in list(PERM_DUTY_CMDS.values()) and memberId not in nextDutyCmds: 
                if ENABLE_WHATSAPP_API: greenAPI.groups.removeGroupParticipant(dutyGrpId, member['id']) 

    # Adding new duty members
    if CDS in CHARLIE_DUTY_CMDS and ENABLE_WHATSAPP_API: greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(CHARLIE_DUTY_CMDS[CDS]))
    if PDS7 in CHARLIE_DUTY_CMDS and ENABLE_WHATSAPP_API: greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(CHARLIE_DUTY_CMDS[PDS7]))
    if PDS8 in CHARLIE_DUTY_CMDS and ENABLE_WHATSAPP_API: greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(CHARLIE_DUTY_CMDS[PDS8]))
    if PDS9 in CHARLIE_DUTY_CMDS and ENABLE_WHATSAPP_API: greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(CHARLIE_DUTY_CMDS[PDS9]))

    # Sending new CET
    if ENABLE_WHATSAPP_API: response = greenAPI.sending.sendMessage(dutyGrpId, cet)

    # Checking whether all members were added successfully
    url = "https://api.green-api.com/waInstance{}/getGroupData/{}".format(ID_INSTANCE, TOKEN_INSTANCE)
    payload = {
        "groupId": dutyGrpId  
    }
    response = rq.post(url, json=payload)
    if response.status_code == 200: group_data = response.json()
    else: 
        send_tele_msg("Unable to check whether all members were added successfully: {}.".format(response.json()), receiver_id="SUPERUSERS")
        group_data = None
    if group_data is not None: 
        allMembers = group_data['participants']
        allMemberNumbers = []
        for member in allMembers: allMemberNumbers.append(member['id'].split('@c.us')[0][2:])
        for memberId in nextDutyCmds:
            if memberId not in allMemberNumbers:
                for name, number in CHARLIE_DUTY_CMDS.items():
                    if memberId == number: 
                        send_tele_msg("{} - {} was not added succesfully".format(name.replace("3SG", "").replace("2SG", ""), memberId), receiver_id="SUPERUSERS")
                        break
    send_tele_msg("Updated duty group", receiver_id="SUPERUSERS")

def autoCheckMA():
    try:
        gc = gspread.service_account_from_dict(SERVICE_ACCOUNT_CREDENTIAL)
        sheet = gc.open("3GDS CHARLIE PARADE STATE")
        paradeStateSheet = sheet.worksheet("C COY")
        allValues = paradeStateSheet.get_all_values()
        allValues = list(zip(*allValues))
        mAs = allValues[44] # column AS
        names = allValues[43] # column AR
        foundStart = False
        foundMA = False
        pattern = r'\d{6}' # 6 consecutive digits i.e 6 digit date
        secondPattern = r'\d{2}\s[A-Z][a-z]{2}\s\d{2}' # e.g. 28 Aug 23
        tele_msg = "Medical Appointments today ({}{}{}):".format(datetime.now().day, (("0" + str(datetime.now().month)) if datetime.now().month < 10 else (str(datetime.now().month))), str(datetime.now().year).replace("20", ""))
        for index, ma in enumerate(mAs, start = 0):
            if ma == 'DETAILS': 
                foundStart = True
                continue
            if not foundStart: continue
            if ma == '': continue
            date = re.findall(pattern, ma)
            if len(date) != 0: 
                datetimeObj = datetime.strptime(date[0], '%d%m%y')
            else:
                date = re.findall(secondPattern, ma)
                if len(date) != 0: datetimeObj = datetime.strptime(date[0], '%d %b %y')
            if datetimeObj.day == datetime.now().today().day and datetimeObj.month == datetime.now().today().month and datetimeObj.year == datetime.now().today().year:
                if names[index] == '': continue
                foundMA = True
                tele_msg = "\n".join([tele_msg, "{}\n{}\n".format(names[index], ma)])
        if foundMA: send_tele_msg(tele_msg)
        else: send_tele_msg("No Medical Appointments today")
            
    except Exception as e:
        print("Encountered exception:\n{}".format(traceback.format_exc()))
        send_tele_msg("Encountered exception:\n{}".format(traceback.format_exc()))

def main(cetQ):

    charlieY2Id = CHARLIE_Y2_ID
    greenAPI = API.GreenAPI(ID_INSTANCE, TOKEN_INSTANCE)
    fpDateTime = None
    sentCdsReminder = False
    checkedDailyMcMa = False
    weekDay = [1, 2, 3, 4, 5]
    while True:

        # Auto updating of MC Lapses and MAs everyday at 0900
        if not checkedDailyMcMa and datetime.now().hour == 9 and datetime.now().minute == 0:
            send_tele_msg("Checking for MC and Status Lapses. This might take a while.")
            checkMcStatus()
            send_tele_msg("Checking for MAs...")
            autoCheckMA()
            checkedDailyMcMa = True
        else: checkedDailyMcMa = False

        try: # Auto reminding of CDS to send report sick parade state every morning 
            while not cetQ.empty(): 
                sentCdsReminder = False
                fpDateTime = cetQ.get()
                # got latest CET
                # check whether date and time is correct
                if cetQ.empty(): 
                    if fpDateTime is None: pass
                    elif datetime.strptime(fpDateTime[0]+fpDateTime[1], "%d%m%y%H%M") > datetime.now(): send_tele_msg("CDS reminder for report sick parade state scheduled at {} {}".format(fpDateTime[0], fpDateTime[1]), receiver_id=fpDateTime[2])
                    else: 
                        send_tele_msg("Invalid CET date to schedule CDS reminder.", receiver_id=fpDateTime[2])
                        fpDateTime = None
        except Exception as e:
            print("Encountered exception:\n{}".format(traceback.format_exc()))
            send_tele_msg("Encountered exception while trying to schedule CDS reminder:\n{}".format(traceback.format_exc()))
        # there was a sent CET since the start of the bot
        if fpDateTime is not None:
            # send reminder during weekdays when it hits the FP date and time of sent CET
            if datetime.now().isoweekday() in weekDay and datetime.now().day == int(fpDateTime[0][:2]) and datetime.now().hour == int(fpDateTime[1][:2]) and datetime.now().minute == int(fpDateTime[1][-2:]) and not sentCdsReminder:
                send_tele_msg("Sending automated CDS reminder", receiver_id=fpDateTime[2])
                if ENABLE_WHATSAPP_API: response = greenAPI.sending.sendMessage(charlieY2Id, "This is an automated daily reminder for the CDS to send the REPORT SICK PARADE STATE\nhttps://docs.google.com/spreadsheets/d/1y6q2rFUE_dbb-l_Ps3R3mQVSPJT_DB_kDys1uyFeXRg/edit?gid=802597665#gid=802597665")
                sentCdsReminder = True

        time.sleep(5)

reply_keyboard_all_commands = [["/checkmcstatus", "/checkconduct", "/checkall", "/updatedutygrp", "/updateconducttracking", "/generateIR"]]
ALL_COMMANDS = "Available Commands:\n/checkmcstatus -> Check for MC/Status Lapses\n/checkconduct -> Conduct Tracking Updates\
\n/updatedutygrp -> Update duty group and schedule CDS reminder according to CET\n/updateconducttracking -> Update conduct tracking sheet according to TimeTree\
\n/generateIR -> Help to generate IR"

async def helpHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) in list(CHANNEL_IDS.values()): 
        try: masterUserRequests[str(update.effective_user.id)]
        except KeyError: masterUserRequests[str(update.effective_user.id)] = None
        if masterUserRequests[str(update.effective_user.id)] is None or time.time() - masterUserRequests[str(update.effective_user.id)] > rateLimit:
            masterUserRequests[str(update.effective_user.id)] = time.time()
            await update.message.reply_text(ALL_COMMANDS)
        else: await update.message.reply_text("Sir stop sir. Too many requests at one time. Please try again later.")
    else: await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")

mcStatusUserRequests = dict()
async def checkMcStatusHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) in list(CHANNEL_IDS.values()): 
        try: mcStatusUserRequests[str(update.effective_user.id)]
        except KeyError: mcStatusUserRequests[str(update.effective_user.id)] = None
        try: masterUserRequests[str(update.effective_user.id)]
        except KeyError: masterUserRequests[str(update.effective_user.id)] = None
        if masterUserRequests[str(update.effective_user.id)] is None or time.time() - masterUserRequests[str(update.effective_user.id)] > rateLimit:
            if mcStatusUserRequests[str(update.effective_user.id)] is None or not mcStatusUserRequests[str(update.effective_user.id)].is_alive():
                masterUserRequests[str(update.effective_user.id)] = time.time()
                await update.message.reply_text("Checking for MC and Status Lapses. This might take a while.")
                t1 = threading.Thread(target=checkMcStatus, args=(str(update.effective_user.id),))
                t1.start()
                mcStatusUserRequests[str(update.effective_user.id)] = t1
            else: await update.message.reply_text("Please wait for the current request to finish")
        else: await update.message.reply_text("Sir stop sir. Too many requests at one time. Please try again later.")
    else: await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")

checkConductUserRequests = dict()
async def checkConductHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) in list(CHANNEL_IDS.values()): 
        try: checkConductUserRequests[str(update.effective_user.id)]
        except KeyError: checkConductUserRequests[str(update.effective_user.id)] = None
        try: masterUserRequests[str(update.effective_user.id)]
        except KeyError: masterUserRequests[str(update.effective_user.id)] = None
        if masterUserRequests[str(update.effective_user.id)] is None or time.time() - masterUserRequests[str(update.effective_user.id)] > rateLimit:
            if checkConductUserRequests[str(update.effective_user.id)] is None or not checkConductUserRequests[str(update.effective_user.id)].is_alive():
                masterUserRequests[str(update.effective_user.id)] = time.time()
                await update.message.reply_text("Checking for conduct tracking updates...")
                t1 = threading.Thread(target=checkConductTracking, args=(str(update.effective_user.id),))
                t1.start()
                checkConductUserRequests[str(update.effective_user.id)] = t1
            else: await update.message.reply_text("Please wait for the current request to finish")
        else: await update.message.reply_text("Sir stop sir. Too many requests at one time. Please try again later.")
    else: await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")

updateConductUserRequests = dict()
async def updateConductHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) in list(SUPERUSERS.values()):
        try: updateConductUserRequests[str(update.effective_user.id)]
        except KeyError: updateConductUserRequests[str(update.effective_user.id)] = None
        try: masterUserRequests[str(update.effective_user.id)]
        except KeyError: masterUserRequests[str(update.effective_user.id)] = None
        if masterUserRequests[str(update.effective_user.id)] is None or time.time() - masterUserRequests[str(update.effective_user.id)] > rateLimit:
            if updateConductUserRequests[str(update.effective_user.id)] is None or not updateConductUserRequests[str(update.effective_user.id)].is_alive():
                masterUserRequests[str(update.effective_user.id)] = time.time()
                send_tele_msg("Updating conduct tracking...", receiver_id="SUPERUSERS")
                t1 = threading.Thread(target=updateConductTracking, args=(str(update.effective_user.id),))
                t1.start()
                updateConductUserRequests[str(update.effective_user.id)] = t1
            else: await update.message.reply_text("Please wait for the current request to finish")
        else: await update.message.reply_text("Sir stop sir. Too many requests at one time. Please try again later.")
    elif str(update.effective_user.id) not in list(SUPERUSERS.values()) and str(update.effective_user.id) in list(CHANNEL_IDS.values()):
        await update.message.reply_text("You are not authorised to use this function. Contact Charlie HQ specs for assistance.")
    else: 
        await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")

ASK_CET = 1

updateDutyGrpUserRequests = dict()
async def updateCet(update: Update, context: CallbackContext) -> int:
    if str(update.effective_user.id) in list(SUPERUSERS.values()):
        try: updateDutyGrpUserRequests[str(update.effective_user.id)]
        except KeyError: updateDutyGrpUserRequests[str(update.effective_user.id)] = None
        try: masterUserRequests[str(update.effective_user.id)]
        except KeyError: masterUserRequests[str(update.effective_user.id)] = None
        if masterUserRequests[str(update.effective_user.id)] is None or time.time() - masterUserRequests[str(update.effective_user.id)] > rateLimit:
            if updateDutyGrpUserRequests[str(update.effective_user.id)] is None or not updateDutyGrpUserRequests[str(update.effective_user.id)].is_alive():
                masterUserRequests[str(update.effective_user.id)] = time.time()
                await update.message.reply_text("Send the new CET or send /cancel to cancel.")
                return ASK_CET
            else: 
                await update.message.reply_text("Please wait for the current request to finish")
                return ConversationHandler.END
        else: 
            await update.message.reply_text("Sir stop sir. Too many requests at one time. Please try again later.")
            return ConversationHandler.END
    elif str(update.effective_user.id) not in list(SUPERUSERS.values()) and str(update.effective_user.id) in list(CHANNEL_IDS.values()):
        await update.message.reply_text("You are not authorised to use this function. Contact Charlie HQ specs for assistance.")
        return ConversationHandler.END
    else: 
        await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")
        return ConversationHandler.END
    
async def updateDutyGrp(update: Update, context: CallbackContext) -> int:
    cet = update.message.text
    t1 = threading.Thread(target=updateWhatsappGrp, args=(cet, str(update.effective_user.id),))
    t1.start()
    updateDutyGrpUserRequests[str(update.effective_user.id)] = t1
    return ConversationHandler.END

NEW, CHECK_PREV_IR, PREV_IR, TRAINING, NAME, CHECK_PES, DATE_TIME, LOCATION, DESCRIPTION, STATUS, FOLLOW_UP, NOK, REPORTED_BY = range(13)

async def start(update: Update, context: CallbackContext) -> int:
    if str(update.effective_user.id) not in list(CHANNEL_IDS.values()): 
        await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")
        return ConversationHandler.END
    try: masterUserRequests[str(update.effective_user.id)]
    except KeyError: masterUserRequests[str(update.effective_user.id)] = None
    if masterUserRequests[str(update.effective_user.id)] is None or time.time() - masterUserRequests[str(update.effective_user.id)] > rateLimit:
        masterUserRequests[str(update.effective_user.id)] = time.time()
        context.user_data['usingPrevIR'] = False
        context.user_data['prevIRDetails'] = None
        context.user_data['findingName'] = False
        context.user_data['findingDateTime'] = False
        context.user_data['findingLocation'] = False
        context.user_data['checkingName'] = False
        context.user_data['nameToBeChecked'] = None
        context.user_data['shiftingStatus'] = False
        context.user_data['new'] = None
        context.user_data['training_related'] = None
        context.user_data['name'] = None
        context.user_data['date_time'] = None
        context.user_data['location'] = None
        context.user_data['description'] = None
        context.user_data['status'] = None
        context.user_data['follow_up'] = None
        context.user_data['nok_informed'] = None
        await update.message.reply_text("Send /cancel to cancel the IR generation any point in time.")
        reply_keyboard = [['New', 'Update', 'Final']]
        await update.message.reply_text(
            "Is it a new/update/final report ?",
            reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True),
        )
        return CHECK_PREV_IR
    else: 
        await update.message.reply_text("Sir stop sir. Too many requests at one time. Please try again later.")
        return ConversationHandler.END

async def checkPrevIR(update: Update, context: CallbackContext) -> int:
    if update.message.text.upper() not in ['NEW', 'UPDATE', 'FINAL']: 
        await update.message.reply_text("Unrecognised response: {}. Please enter New or Update or Final.".format(update.message.text))
        return CHECK_PREV_IR
    context.user_data['new'] = update.message.text
    if update.message.text.upper() == "UPDATE" or update.message.text.upper() == "FINAL":
        reply_keyboard = [['Yes', 'No']]
        await update.message.reply_text(
            "Is there a previous IR ? (Yes/No)",
            reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True)
        )
        return PREV_IR
    else: return await new(update, context)

async def prevIR(update: Update, context: CallbackContext) -> int:
    if update.message.text.upper() == "YES":
        await update.message.reply_text("Send the previous IR")
        return LOCATION
    elif update.message.text.upper() == "NO":
        return await new(update, context)
    else:
        await update.message.reply_text("Unrecognised response: {}. Please enter yes or no.".format(update.message.text))
        return PREV_IR

async def new(update: Update, context: CallbackContext) -> int:
    reply_keyboard = [['Training', 'Non-Training']]
    await update.message.reply_text(
        "*Training* or *Non\\-Training* related?",
        reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True),
        parse_mode='MarkdownV2'
    )
    return TRAINING

async def training(update: Update, context: CallbackContext) -> int:
    if not context.user_data['findingName']: 
        if update.message.text.upper() not in ['TRAINING', 'NON TRAINING', 'NON-TRAINING']: 
            await update.message.reply_text("Unrecognised response: {}. Please enter Training or Non-Training.".format(update.message.text))
            return TRAINING
        if update.message.text.upper() == "NON TRAINING":
            context.user_data['training_related'] = "Non-Training"
        else: context.user_data['training_related'] = update.message.text.lower().title()
    if context.user_data['usingPrevIR'] and not context.user_data['findingName']: return await location(update, context)
    await update.message.reply_text("Please provide the name of the personnel involved:")
    return NAME

async def name(update: Update, context: CallbackContext) -> int:
    gc = gspread.service_account_from_dict(SERVICE_ACCOUNT_CREDENTIAL)
    sheet = gc.open("Charlie Nominal Roll")
    cCoyNominalRollSheet = sheet.worksheet("COMPANY ORBAT")
    allValues = cCoyNominalRollSheet.get_all_values()
    formattedAllValues = list(zip(*allValues))[5]
    if not context.user_data['checkingName']: userInput = update.message.text
    else: 
        context.user_data['nameToBeChecked'] = context.user_data['nameToBeChecked'].split(' ')
        del context.user_data['nameToBeChecked'][0] # remove rank
        userInput = "".join(context.user_data['nameToBeChecked'])

    formatteduserInput = userInput.replace(" ", "").upper()
    allMatches = list()
    foundPersonnel = False
    for index, value in enumerate(formattedAllValues, start = 0):
        if formatteduserInput in value.replace(" ", "").upper():
            allMatches.append(allValues[index][5])
            context.user_data['name'] = allValues[index]
            foundPersonnel = True
    if not foundPersonnel: 
        await update.message.reply_text("Unable to find {}. Please provide another name:".format(userInput))
        return NAME
    if len(allMatches) > 1: # more than one match found
        reply_keyboard = [allMatches]
        await update.message.reply_text(
            "Please specify the personnel involved:",
            reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True))
        return NAME
    if context.user_data['name'][15] == "":
        reply_keyboard = [['A', 'B1']]
        await update.message.reply_text(
            "What is the PES status of {} ?".format(userInput),
            reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True))
        return CHECK_PES
    if context.user_data['findingName'] or context.user_data['checkingName']: return await location(update, context)
    await update.message.reply_text("Please provide the date and time of the incident (e.g. 310124 1430):")
    return DATE_TIME

async def checkPes(update: Update, context: CallbackContext) -> int:
    if not context.user_data['findingDateTime']:
        allPesStatus = ['A', 'B1', 'B2', 'B3', 'B4', 'C2', 'C9', 'D', 'E1', 'E9', 'F', 'BP']
        pes = update.message.text
        if pes not in allPesStatus: 
            await update.message.reply_text("Unknown PES Status: {}. Please send another PES status:".format(pes))
            return CHECK_PES
        context.user_data['name'][15] = pes
    if context.user_data['findingName'] or context.user_data['checkingName']: return await location(update, context)
    await update.message.reply_text("Please provide the date and time of the incident (e.g. 310124 1430):")
    return DATE_TIME

async def date_time(update: Update, context: CallbackContext) -> int:
    if not context.user_data['findingLocation']:
        userInput = update.message.text.replace(" ", "")
        if len(userInput) != 10:
            await update.message.reply_text("Unrecognised datetime {}. Please provide another date and time in the format (310124 1430):".format(update.message.text))
            return DATE_TIME
        context.user_data['date_time'] = userInput
        if context.user_data['usingPrevIR']: return await location(update, context)
    reply_keyboard = [["Serviceman's Residence", 'Bedok Camp 2']]
    await update.message.reply_text(
        "Location of incident ?", 
        reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True),)
    return LOCATION

async def location(update: Update, context: CallbackContext) -> int:
    response = update.message.text # could be a previous IR or just purely location
    if context.user_data['usingPrevIR'] and "INCIDENT" not in response and context.user_data['findingLocation']: context.user_data['location'] = response # should be location response
    if "INCIDENT" in response or context.user_data['usingPrevIR']: # previous IR
        context.user_data['usingPrevIR'] = True
        if "INCIDENT" in response: context.user_data['prevIRDetails'] = response
        lines = context.user_data['prevIRDetails'].split('\n')
        try: 
            natureOfIncident = context.user_data['training_related']
            if natureOfIncident is None: raise KeyError
            foundNatureOfIncident = True
        except KeyError:
            foundNatureOfIncident = False
            natureOfIncident = None
        try:
            name_t = context.user_data['name']
            if name_t is None: raise KeyError
            foundName = True
        except KeyError:
            foundName = False
            name_t = None
        try:
            dateTime = context.user_data['date_time']
            if dateTime is None: raise KeyError
            foundDateTime = True
        except KeyError:
            foundDateTime = False
            dateTime = None
        try:
            location = context.user_data['location']
            if location is None: raise KeyError
            foundLocation = True
        except KeyError:
            foundLocation = False
            location = None
        try: 
            description = context.user_data['description']
            if description is None: raise KeyError
            foundDescription = True
        except KeyError: 
            foundDescription = False
            description = None
        try: 
            status = context.user_data['status']
            if status is None: raise KeyError
            foundStatus = True
        except KeyError: 
            foundStatus = False
            status = None

        for line in lines:
            if not foundNatureOfIncident and line.replace("*", "").replace(" ", "").replace(":", "") == "1)NatureandTypeofIncident":
                foundNatureOfIncident = True
                continue
            if not foundNatureOfIncident: continue
            if line == "": continue
            if natureOfIncident is None: 
                context.user_data['training_related'] = line.replace(" Related", "").replace("Related", "")
                natureOfIncident = line.replace(" Related", "").replace("Related", "")
                continue

            if not foundName and line.replace("*", "").replace(" ", "").replace(":", "") == "2)DetailsofPersonnelInvolved":
                foundName = True
                continue
            if not foundName: continue
            if line == "": continue
            if name_t is None: 
                context.user_data['checkingName'] = True
                context.user_data['nameToBeChecked'] = line
                return await name(update, context)
            context.user_data['checkingName'] = False
            context.user_data['nameToBeChecked'] = None

            if not foundDateTime and line.replace("*", "").replace(" ", "").replace(":", "") == "3)Date&TimeofIncident":
                foundDateTime = True
                continue
            if not foundDateTime: continue
            if line == "": continue
            if dateTime is None:
                dateTime = line.replace("/", "").replace(" ", "").replace("hrs", "").replace("hr", "")
                context.user_data['date_time'] = line.replace("/", "").replace(" ", "").replace("hrs", "").replace("hr", "")
                continue

            if not foundLocation and line.replace("*", "").replace(" ", "").replace(":", "") == "4)LocationofIncident":
                foundLocation = True
                continue
            if not foundLocation: continue
            if line == "": continue
            if location is None:
                location = line
                context.user_data['location'] = line
                continue
                
            if not foundDescription and line.replace("*", "").replace(" ", "").replace(":", "") == "5)BriefDescription":
                foundDescription = True
                continue
            if not foundDescription: continue
            if line == "": continue
            if description is None:
                description = line
                context.user_data['description'] = line
                continue

            if not foundStatus and line.replace("*", "").replace(" ", "").replace(":", "") == "6)CurrentStatus":
                foundStatus = True
                continue
            if not foundStatus: continue
            if line == "": continue
            status = line
            break
        
        if natureOfIncident is not None and natureOfIncident in ['Training', 'Non-Training']: context.user_data['training_related'] = natureOfIncident
        else: return await new(update, context)

        if name_t is None: 
            context.user_data['findingName'] = True
            return await training(update, context)
        context.user_data['findingName'] = False
        
        if dateTime is None: 
            context.user_data['findingDateTime'] = True
            return await checkPes(update, context)
        context.user_data['findingDateTime'] = False

        if location is None: 
            context.user_data['findingLocation'] = True
            return await date_time(update, context)
        context.user_data['findingLocation'] = False

        if status is not None and not context.user_data['shiftingStatus'] and description is not None:
            context.user_data['shiftingStatus'] = True
            reply_keyboard = [['Yes', 'No']]
            await update.message.reply_text("Shift the previous status to the description ?",
                                            reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True))
            return LOCATION

        if description is not None:
            reply_keyboard = [['No Changes']]
            await update.message.reply_text("Please update the description following the below text")
            if context.user_data['shiftingStatus'] and response == 'Yes':
                await update.message.reply_text(
                    description + '\n\n' + status, 
                    reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True))
                context.user_data['description'] = description + '\n\n' + status
            else:
                await update.message.reply_text(
                    description, 
                    reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True))
                context.user_data['description'] = description
            context.user_data['shiftingStatus'] = False
            return DESCRIPTION
        else:
            await update.message.reply_text("Please write the description following the below text")
            await update.message.reply_text("On {} at about {}hrs, {} {}...".format(context.user_data['date_time'][:6], context.user_data['date_time'][-4:], context.user_data['name'][4], context.user_data['name'][5]))
            await update.message.reply_text("Refer to the templates below when writing your description:\n\n*Normal Report Sick*\n\\.\\.\\. requested permission from *\\(RANK \\+ NAME\\)* to report sick at *\\(LOCATION\\)* for *\\(REASON\\)*\\.\n\n*Medical Appointment*\n\\.\\.\\. has left *\\(LOCATION\\)* to attend his *\\(TYPE\\)* *\\(medical appointment\\/surgery\\)* at *\\(LOCATION\\)*", parse_mode='MarkdownV2')
            return DESCRIPTION

    else:
        context.user_data['location'] = update.message.text
        await update.message.reply_text("Please write the description following the below text")
        await update.message.reply_text("On {} at about {}hrs, {} {}...".format(context.user_data['date_time'][:6], context.user_data['date_time'][-4:], context.user_data['name'][4], context.user_data['name'][5]))
        await update.message.reply_text("Refer to the templates below when writing your description:\n\n*Normal Report Sick*\n\\.\\.\\. requested permission from *\\(RANK \\+ NAME\\)* to report sick at *\\(LOCATION\\)* for *\\(REASON\\)*\\.\n\n*Medical Appointment*\n\\.\\.\\. has left *\\(LOCATION\\)* to attend his *\\(TYPE\\)* *\\(medical appointment\\/surgery\\)* at *\\(LOCATION\\)*", parse_mode='MarkdownV2')
        return DESCRIPTION

async def description(update: Update, context: CallbackContext) -> int:
    if update.message.text != 'No Changes' and not context.user_data['usingPrevIR']:
        context.user_data['description'] = "On {} at about {}hrs, {} {} ".format(context.user_data['date_time'][:6], context.user_data['date_time'][-4:], context.user_data['name'][4], context.user_data['name'][5]) + update.message.text
    elif update.message.text != 'No Changes' and context.user_data['usingPrevIR']:
        context.user_data['description'] = context.user_data['description'] + update.message.text
    await update.message.reply_text("What is the current status?")
    await update.message.reply_text("Refer to the templates below when writing your status:\n\nServiceman is currently making his way to *\\(LOCATION\\)*\\.\n\n*Normal Report Sick*\nServiceman has received *\\(DURATION OF MC\\/STATUS \\+ WHAT MC\\/STATUS\\)* from *\\(START DATE\\)* to *\\(END DATE\\)* inclusive\\.\n\n*Medical Appointments*\nServiceman has completed his appointment\\.\\.\\.\n\n\
1\\) with no status\\.\n\n\
2\\) and was given *\\(DURATION OF MC\\/STATUS \\+ WHAT MC\\/STATUS\\)* from *\\(START DATE\\)* to *\\(END DATE\\)* inclusive\\.\n\n\
3\\) *\\(If Applicable\\)* and was scheduled a follow up appointment on *\\(DATE OF FOLLOW UP APPT\\)*\\.\n\n\
*\\(If Applicable\\) *Serviceman is currently headed home to consume his MC\\.", parse_mode='MarkdownV2')
    return STATUS

async def status(update: Update, context: CallbackContext) -> int:
    context.user_data['status'] = update.message.text
    if context.user_data['new'] == "Final": reply_keyboard = [['Unit will proceed to close the case.']]
    elif context.user_data['new'] == "New" or context.user_data['new'] == "Update": reply_keyboard = [['Unit will monitor and update accordingly.']]
    else: reply_keyboard = [['Unit will monitor and update accordingly.', 'Unit will proceed to close the case.']]
    await update.message.reply_text(
        "Follow-up actions?", 
        reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True))
    return FOLLOW_UP

async def follow_up(update: Update, context: CallbackContext) -> int:
    context.user_data['follow_up'] = update.message.text
    reply_keyboard = [['Yes', 'No']]
    await update.message.reply_text(
        "Has the NOK (Next of Kin) been informed ? (Yes/No)",
        reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True))
    return NOK

async def nok(update: Update, context: CallbackContext) -> int:
    if update.message.text.upper() != "YES" and update.message.text.upper() != "NO":
        await update.message.reply_text("Unrecognised response: {}. Please enter yes or no.".format(update.message.text))
        return NOK
    context.user_data['nok_informed'] = update.message.text
    await update.message.reply_text("Who is reporting this incident?")
    return REPORTED_BY

async def reported_by(update: Update, context: CallbackContext) -> int:
    userInput = update.message.text
    gc = gspread.service_account_from_dict(SERVICE_ACCOUNT_CREDENTIAL)
    sheet = gc.open("Charlie Nominal Roll")
    cCoyNominalRollSheet = sheet.worksheet("COMPANY ORBAT")
    allValues = cCoyNominalRollSheet.get_all_values()
    formattedAllValues = list(zip(*allValues))[5]
    formatteduserInput = userInput.replace(" ", "").upper()
    reportingPerson = None
    allMatches = list()
    for index, value in enumerate(formattedAllValues, start = 0):
        if formatteduserInput in value.replace(" ", ""):
            allMatches.append(allValues[index][5])
            reportingPerson = allValues[index]
    if len(allMatches) > 1:
        reply_keyboard = [allMatches]
        await update.message.reply_text(
            "Please specify the reporting personnel:",
            reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True))
        return REPORTED_BY
    if reportingPerson is None: 
        await update.message.reply_text("Unable to find {}. Please provide another name:".format(userInput))
        return REPORTED_BY
    context.user_data['reported_by'] = reportingPerson
    await update.message.reply_text("Generating IR...")
    await update.message.reply_text("*INCIDENT REPORT*\n\
*{}: 3 GDS {} RELATED REPORT*\n\
\n\
*1) Nature and Type of Incident:*\n\
{} Related\n\
\n\
*2) Details of Personnel Involved*\n\
{} {}\n\
{}\n\
3GDS/C COY\n\
PES {}\n\
\n\
*3) Date & Time of Incident*\n\
{} / {}hrs\n\
\n\
*4) Location of Incident*\n\
{}\n\
\n\
*5) Brief Description*\n\
{}\n\
\n\
*6) Current Status:*\n\
{}\n\
\n\
*7) Follow Up Action*\n\
{}\n\
\n\
*8) NOK Informed?*\n\
{}\n\
\n\
*9) HHQ/GSOC Informed?*\n\
Verbal Report - No\n\
HQ 7SIB - No\n\
HQ GDS - No\n\
GSOC - No\n\
ASIS Report - No\n\
\n\
*10) Reported By:*\n\
{} {}\n\
(HP: {} {})".format(context.user_data['new'].upper(), context.user_data['training_related'].upper(),
        context.user_data['training_related'],
        context.user_data['name'][4], context.user_data['name'][5],
        context.user_data['name'][3],
        context.user_data['name'][15],
        context.user_data['date_time'][:6], context.user_data['date_time'][-4:],
        context.user_data['location'],
        context.user_data['description'],
        context.user_data['status'],
        context.user_data['follow_up'],
        context.user_data['nok_informed'],
        reportingPerson[4], reportingPerson[5],
        reportingPerson[8][:4], reportingPerson[8][-4:]))
    await update.message.reply_text("Copy and paste the generated IR to WhatsApp")
    return ConversationHandler.END

async def cancel_ir(update: Update, context: CallbackContext) -> int:
    if str(update.effective_user.id) in list(CHANNEL_IDS.values()):
        await update.message.reply_text("IR generation cancelled.")
        return ConversationHandler.END
    else: 
        await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")
        return ConversationHandler.END

async def cancel_dutygrp(update: Update, context: CallbackContext) -> int:
    if str(update.effective_user.id) in list(SUPERUSERS.values()):
        send_tele_msg('Updating cancelled', receiver_id="SUPERUSERS")
        return ConversationHandler.END
    elif str(update.effective_user.id) not in list(SUPERUSERS.values()) and str(update.effective_user.id) in list(CHANNEL_IDS.values()):
        await update.message.reply_text("You are not authorised to use this function. Contact Charlie HQ specs for assistance.")
        return ConversationHandler.END
    else: 
        await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")
        return ConversationHandler.END

async def unknownCommand(update: Update, context: CallbackContext) -> None:
    if str(update.effective_user.id) in list(CHANNEL_IDS.values()):
        await update.message.reply_text("Unrecognised command.")
        await update.message.reply_text(ALL_COMMANDS)
    else: await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")

def telegram_manager() -> None:

    application = Application.builder().token(TELEGRAM_CHANNEL_BOT_TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("help", helpHandler))
    application.add_handler(CommandHandler("checkmcstatus", checkMcStatusHandler))
    application.add_handler(CommandHandler("checkconduct", checkConductHandler))
    application.add_handler(CommandHandler("updateconducttracking", updateConductHandler))

    # Add a conversation handler for the new command
    conv_dutygrp_handler = ConversationHandler(
        entry_points=[CommandHandler('updatedutygrp', updateCet)],
        states={
            ASK_CET: [MessageHandler(filters.TEXT & ~filters.COMMAND, updateDutyGrp)],
        },
        fallbacks=[CommandHandler('cancel', cancel_dutygrp)],
    )

    conv__IR_handler = ConversationHandler(
        entry_points=[CommandHandler('generateIR', start)],
        states={
            NEW: [MessageHandler(filters.TEXT & ~filters.COMMAND, new)],
            CHECK_PREV_IR: [MessageHandler(filters.TEXT & ~filters.COMMAND, checkPrevIR)],
            PREV_IR: [MessageHandler(filters.TEXT & ~filters.COMMAND, prevIR)],
            TRAINING: [MessageHandler(filters.TEXT & ~filters.COMMAND, training)],
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, name)],
            CHECK_PES: [MessageHandler(filters.TEXT & ~filters.COMMAND, checkPes)],
            DATE_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, date_time)],
            LOCATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, location)],
            DESCRIPTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, description)],
            STATUS: [MessageHandler(filters.TEXT & ~filters.COMMAND, status)],
            FOLLOW_UP: [MessageHandler(filters.TEXT & ~filters.COMMAND, follow_up)],
            NOK: [MessageHandler(filters.TEXT & ~filters.COMMAND, nok)],
            REPORTED_BY: [MessageHandler(filters.TEXT & ~filters.COMMAND, reported_by)],
        },
        fallbacks=[CommandHandler('cancel', cancel_ir)],
        allow_reentry=True)

    # Add the conversation handler
    application.add_handler(conv_dutygrp_handler)
    application.add_handler(conv__IR_handler)
    application.add_handler(MessageHandler(filters.COMMAND, unknownCommand))
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':

    send_tele_msg("Welcome to HQ Bot. Strong Alone, Stronger Together.")
    send_tele_msg(ALL_COMMANDS)
    send_tele_msg("Send the latest CET using /updatedutygrp to schedule CDS reminder for report sick parade state during FP.")
    cetQueue = multiprocessing.Queue()
    mainCheckMcProcess = multiprocessing.Process(target=main, args=(cetQueue,))
    mainCheckMcProcess.start()
    telegram_manager()