# General Libraries
import csv
import requests
from requests.exceptions import SSLError
import gc as garbageCollector
import json
import time
import gspread
import platform
from gspread_formatting import *
from datetime import datetime, timedelta
from collections import Counter
from config import SERVICE_ACCOUNT_CREDENTIAL, TELEGRAM_CHANNEL_BOT_TOKEN, CHANNEL_IDS, SUPERUSERS, DUTY_GRP_ID, CHARLIE_Y2_ID, WHATSAPP_ID_INSTANCE, WHATSAPP_TOKEN_INSTANCE, SUPABASE_URL, SUPABASE_KEY, SUPBASE_BACKUP_DRIVE_ID, CHARLIE_DUTY_CMDS, PERM_DUTY_CMDS, TIMETREE_USERNAME, TIMETREE_PASSWORD, TIMETREE_CALENDAR_ID
import traceback
import copy

# Google Drive API
# PyDrive library has been depracated since 2021
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import AuthorizedSession
credentials = Credentials.from_service_account_info(SERVICE_ACCOUNT_CREDENTIAL, scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
session = AuthorizedSession(credentials)
session.verify = False
gc = gspread.authorize(credentials)
# gc = gspread.service_account_from_dict(SERVICE_ACCOUNT_CREDENTIAL, scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])

# Telegram API
import telegram
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackContext, ConversationHandler, MessageHandler, filters
from telegram.error import NetworkError
import asyncio
import nest_asyncio
nest_asyncio.apply() # patch asyncio
import multiprocessing
multiprocessing.set_start_method("spawn", force=True)
import threading
MAX_MESSAGE_LENGTH = 4096

# Image to text detection
import cv2
import numpy as np
from io import BytesIO
import io
from pdf2image import convert_from_bytes
import pyheif
from PIL import Image
import re

from doctr.models import detection, ocr_predictor
detection_model = detection.__dict__["db_resnet50"](
        pretrained=True,
        bin_thresh=0.3,
        box_thresh=0.1,
    )
model = ocr_predictor(detection_model, "crnn_vgg16_bn", pretrained=True, straighten_pages=True)

# WhatsApp API
import requests as rq
from whatsapp_api_client_python import API

# Supabase API
from supabase import create_client, Client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Intercepting TimeTree Responses
from playwright.async_api import async_playwright
from zoneinfo import ZoneInfo
foundResponse = False # Timetree response interception
responseContent = None

# Telegram Channel
telegram_bot = telegram.Bot(token=TELEGRAM_CHANNEL_BOT_TOKEN)

monthConversion = {'January': '01', 'Jan': '01', 'February': '02', 'Feb': '02', 'March': '03', 'Mar': '03', 'April': '04', 'Apr': '04', 'May': '05', 'June': '06', 'Jun': '06', 'July': '07', 'Jul': '07', 'August': '08', 'Aug': '08', 'September': '09', 'Sep': '09', 'October': '10', 'Oct': '10', 'November': '11', 'Nov': '11', 'December': '12', 'Dec': '12'}
trooperRanks = ['PTE', 'PFC', 'LCP', 'CPL', 'CFC']
wospecRanks = ['3SG', '2SG', '1SG', 'SSG', 'MSG', '3WO', '2WO', '1WO', 'MWO', 'SWO', 'CWO']
officerRanks = ['2LT', 'LTA', 'CPT', 'MAJ', 'LTC', 'SLTC', 'COL', 'BG', 'MG', 'LG']

ENABLE_WHATSAPP_API = True # Flag to enable live whatsapp manipulation

masterUserRequests = dict()
rateLimit = 1 # number of seconds between commands per user

tmpDutyCmdsDict = dict()
tmpDutyCmdsList = list()

# supabase
charlieNominalRoll = None
allNames = None
allContacts = None

# google sheet
googleSheetsNominalRoll = None
allPerson = None
 
def send_tele_msg(msg, receiver_id = None,  parseMode = None, replyMarkup = None):

    """
        :param receiver_id (str): SUPERUSERS/NORMALUSERS/ALL/individual ID. None -> Send to everyone
        :param parseMode (str): MarkdownV2
        :param replyMarkup: For onscreen keyboards
    """
    if receiver_id is not None and not isinstance(receiver_id, str): receiver_id = str(receiver_id)

    if receiver_id is None: # send to everyone
        for _, value in CHANNEL_IDS.items():
            asyncio.run(send_telegram_bot_msg(msg, value, parseMode, replyMarkup))
    else:
        if receiver_id == "SUPERUSERS":  
            for _, value in SUPERUSERS.items():
                asyncio.run(send_telegram_bot_msg(msg, value, parseMode, replyMarkup))
        elif receiver_id == "ALL": 
            for _, value in CHANNEL_IDS.items():
                asyncio.run(send_telegram_bot_msg(msg, value, parseMode, replyMarkup))
        elif receiver_id == "NORMALUSERS":
            for _, value in CHANNEL_IDS.items():
                if value not in list(SUPERUSERS.values()): asyncio.run(send_telegram_bot_msg(msg, value, parseMode, replyMarkup))
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
    async with async_playwright() as playwright:
        
        browser = await playwright.chromium.launch(headless=True)
        
        # Create incognito context
        context = await browser.new_context()
        page = await context.new_page()

        # Intercept network responses
        page.on("response", intercept_response)

        # Navigate to the sign-in page
        await page.goto('https://timetreeapp.com/signin?locale=en', timeout=10000)

        try:
            # Log in by filling in the credentials
            await page.wait_for_selector('input[name="email"]', timeout=10000)
            await page.fill('input[name="email"]', TIMETREE_USERNAME)
            await page.wait_for_selector('input[name="password"]', timeout=10000)
            await page.fill('input[name="password"]', TIMETREE_PASSWORD)
            await page.wait_for_selector('button[type="submit"]', timeout=10000)
            await page.click('button[type="submit"]')
            await page.wait_for_load_state(timeout=10000)
        except Exception as e:
            print(f"Error: {e}")

        # Wait for the desired response
        while not foundResponse:
            await asyncio.sleep(1)

        # Close the browser
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
    
    sheet = None
    for attempt in range(5):
        try: 
            sheet = gc.open("Charlie Conduct Tracking")
            break
        except SSLError as e:
            if attempt < 4: time.sleep(5)
            else: raise e
    conductTrackingSheet = sheet.worksheet("CONDUCT TRACKING")

    startRow = 5
    endRow = 139

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
            "startRowIndex": 19,
            "endRowIndex": 20,
            "startColumnIndex": conductColumn-1,
            "endColumnIndex": conductColumn
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 19,
            "endRowIndex": 20,
            "startColumnIndex": conductColumn,
            "endColumnIndex": conductColumn+1
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 44,
            "endRowIndex": 45,
            "startColumnIndex": conductColumn-1,
            "endColumnIndex": conductColumn
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 44,
            "endRowIndex": 45,
            "startColumnIndex": conductColumn,
            "endColumnIndex": conductColumn+1
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 69,
            "endRowIndex": 70,
            "startColumnIndex": conductColumn-1,
            "endColumnIndex": conductColumn
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 69,
            "endRowIndex": 70,
            "startColumnIndex": conductColumn,
            "endColumnIndex": conductColumn+1
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 95,
            "endRowIndex": 96,
            "startColumnIndex": conductColumn-1,
            "endColumnIndex": conductColumn
        },
        {
            "sheetId": conductTrackingSheet.id, 
            "startRowIndex": 95,
            "endRowIndex": 96,
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
        global responseContent
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
        sheet = None
        for attempt in range(5):
            try: 
                sheet = gc.open("Charlie Conduct Tracking")
                break
            except SSLError as e:
                if attempt < 4: time.sleep(5)
                else: raise e
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

def checkMcStatus(receiver_id = None, send_whatsapp = False):

    startTm = time.time()
    try:
        if send_whatsapp: greenAPI = API.GreenAPI(WHATSAPP_ID_INSTANCE, WHATSAPP_TOKEN_INSTANCE)
        # Get Coy MC/Status list from parade state
        sheet = None
        for attempt in range(5):
            try: 
                sheet = gc.open("3GDS CHARLIE PARADE STATE")
                break
            except SSLError as e:
                if attempt < 4: time.sleep(5)
                else: raise e
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
            if foundHeader and name != '' and mcStartDates[index] != '#REF!' and mcStartDates[index] != '' and mcEndDates[index] != '#REF!' and mcEndDates[index] != '' and mcReason[index] != '#REF!' and mcReason[index] != '': 
                mcList.append((name, mcStartDates[index], (mcEndDates[index] if mcEndDates[index] != '' else '-'), platoonMc[index], sectionMc[index], "MC", mcReason[index]))
        foundHeader = False
        statusList = []
        for index, name in enumerate(sheetStatusList, start = 0):
            if not foundHeader and name == 'NAME': 
                foundHeader = True
                continue
            if foundHeader and name != '' and statusStartDates[index] != '#REF!' and statusStartDates[index] != '' and statusEndDates[index] != '#REF!' and statusEndDates[index] != '' and statusReason[index] != '#REF!' and statusReason[index] != '': 
                statusList.append((name, statusStartDates[index], (statusEndDates[index] if statusEndDates[index] != '' else '-'), platoonStatus[index], sectionStatus[index], "Status", statusReason[index]))

        # read existing MC/Status entries from mc lapse sheet
        mcStatusLapseSheet = None
        for attempt in range(5):
            try: 
                mcStatusLapseSheet = gc.open("MC/Status Lapse Tracking")
                break
            except SSLError as e:
                if attempt < 4: time.sleep(5)
                else: raise e
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
        combinedPattern = r"from\s*?(\d{1,2}/\d{1,2}/\d{4}|\d{1,2}-\d{1,2}-\d{4}|(\d{1,2}(?:-?)(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:-?)\d{4})|(\d{1,2}(?:-?)(?:January|February|March|April|May|June|July|August|September|October|November|December)(?:-?)\d{4}))\s*?to\s*?(\d{1,2}/\d{1,2}/\d{4}|\d{1,2}-\d{1,2}-\d{4}|(\d{1,2}(?:-?)(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:-?)\d{4})|(\d{1,2}(?:-?)(?:January|February|March|April|May|June|July|August|September|October|November|December)(?:-?)\d{4}))"
        alternatePattern = r"from\s*?(\d{1,2}/\d{1,2}/\d{4}|\d{1,2}-\d{1,2}-\d{4}|(\d{1,2}(?:-?)(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:-?)\d{4})|(\d{1,2}(?:-?)(?:January|February|March|April|May|June|July|August|September|October|November|December)(?:-?)\d{4}))"
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
            foundMcStatusFile = False
            for driveMcStatus in driveMcStatusList:
                tmp = driveMcStatus['createdDate'].split('T')[0].split('-')
                tmp.reverse()
                tmp[2] = tmp[2].replace("20", "")
                uploadDate = "".join(tmp)
                uploadDateTime = datetime.strptime(uploadDate, "%d%m%y").date()
                if (startDate in driveMcStatus['title'] and endDate != '-' and endDate in driveMcStatus['title']) or (endDate == '-' and startDate in driveMcStatus['title']): # found MC file
                    foundMcStatusFile = True
                    break
                elif uploadDateTime >= startDateTime-timedelta(days=7): # possible file with upload date no earlier than 7 days before start of MC/status
                    request = service.files().get_media(fileId=driveMcStatus['id'])
                    imageIo = io.BytesIO()
                    downloader = MediaIoBaseDownload(imageIo, request)
                    done = False
                    while done is False: status, done = downloader.next_chunk()
                    imageIo.seek(0)
                    if driveMcStatus['fileExtension'].upper() == 'PDF': # PDF Formats
                        try: 
                            images = convert_from_bytes(imageIo.read(), first_page=0, last_page=1)
                            pil_image = images[0]  # Convert the first page only
                            img = cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGB2BGR)
                            del images
                        except Exception as e:
                            if mcStatus[5] == "MC" and (mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)) not in possibleMcList: possibleMcList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                            elif mcStatus[5] == "Status" and (mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)) not in possibleStatusList: possibleStatusList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                            continue
                    elif driveMcStatus['fileExtension'].upper() == 'HEIC': # HEIC Formats
                        try:
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
                            del heif_file, image
                        except Exception as e:
                            if mcStatus[5] == "MC" and (mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)) not in possibleMcList: possibleMcList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                            elif mcStatus[5] == "Status" and (mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)) not in possibleStatusList: possibleStatusList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                            continue
                    elif driveMcStatus['fileExtension'].upper() == 'JPG' or driveMcStatus['fileExtension'].upper() == 'JPEG': #jpg/jpeg formats
                        try:
                            file_data = BytesIO(request.execute())
                            imageArray = np.asarray(bytearray(file_data.read()), dtype="uint8")
                            img = cv2.imdecode(imageArray, cv2.IMREAD_COLOR)
                            del file_data, imageArray
                        except Exception as e:
                            if mcStatus[5] == "MC" and (mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)) not in possibleMcList: possibleMcList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                            elif mcStatus[5] == "Status" and (mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)) not in possibleStatusList: possibleStatusList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                            continue
                    elif driveMcStatus['fileExtension'].upper() == 'PNG': # PNG Formats
                        try:
                            img = Image.open(io.BytesIO(imageIo.read()))
                            img_np = np.array(img)
                            img = cv2.cvtColor(img_np, cv2.COLOR_RGB2BGR)
                        except Exception as e:
                            if mcStatus[5] == "MC" and (mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)) not in possibleMcList: possibleMcList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                            elif mcStatus[5] == "Status" and (mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)) not in possibleStatusList: possibleStatusList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                            continue
                    else: # unknown image type
                        if mcStatus[5] == "MC" and (mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)) not in possibleMcList: possibleMcList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                        elif mcStatus[5] == "Status" and (mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)) not in possibleStatusList: possibleStatusList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                        continue
                    imageText = model([img]).render().replace("\n", "").replace(" ", "")
                    matches = re.findall(combinedPattern, imageText)
                    allDates = list()
                    if matches:
                        for match in matches:
                            start_date = match[2] or match[1] or match[0]
                            end_date = match[5] or match[4] or match[3]
                            if start_date == match[1] or start_date == match[2]:
                                tmp = start_date.split('-')
                                if len(tmp) == 1: # date does not have hyphens
                                    slave = list()
                                    for month, number in monthConversion.items():
                                        matchingMonth = re.findall(month, start_date)
                                        if len(matchingMonth) != 0: # found corresponding month
                                            slave = start_date.split(matchingMonth[0])
                                            slave.insert(1, monthConversion[matchingMonth[0]])
                                            slave[2] = slave[2].replace("2023", "23")
                                            slave[2] = slave[2].replace("2024", "24")
                                            slave[2] = slave[2].replace("2025", "25")
                                            start_date = "".join(slave)
                                            break
                                else:
                                    try: tmp[1] = monthConversion[tmp[1]]
                                    except KeyError: continue
                                    tmp[2] = tmp[2].replace("2023", "23")
                                    tmp[2] = tmp[2].replace("2024", "24")
                                    tmp[2] = tmp[2].replace("2025", "25")
                                    start_date = "".join(tmp)
                            elif start_date == match[0]: 
                                tmp = start_date.replace("-", "")
                                tmp = tmp.replace("/", "")
                                tmp = tmp.replace("2023", "23")
                                tmp = tmp.replace("2024", "24")
                                tmp = tmp.replace("2025", "25")
                                start_date = tmp

                            if end_date == match[4] or end_date == match[5]:
                                tmp = end_date.split('-')
                                if len(tmp) == 1: # date does not have hyphens
                                    slave = list()
                                    for month, number in monthConversion.items():
                                        matchingMonth = re.findall(month, end_date)
                                        if len(matchingMonth) != 0: # found corresponding month
                                            slave = end_date.split(matchingMonth[0])
                                            slave.insert(1, monthConversion[matchingMonth[0]])
                                            slave[2] = slave[2].replace("2023", "23")
                                            slave[2] = slave[2].replace("2024", "24")
                                            slave[2] = slave[2].replace("2025", "25")
                                            end_date = "".join(slave)
                                            break
                                else:
                                    try: tmp[1] = monthConversion[tmp[1]]
                                    except KeyError: continue
                                    tmp[2] = tmp[2].replace("2023", "23")
                                    tmp[2] = tmp[2].replace("2024", "24")
                                    tmp[2] = tmp[2].replace("2025", "25")
                                    end_date = "".join(tmp)
                            elif end_date == match[3]: 
                                tmp = end_date.replace("-", "")
                                tmp = tmp.replace("/", "")
                                tmp = tmp.replace("2023", "23")
                                tmp = tmp.replace("2024", "24")
                                tmp = tmp.replace("2025", "25")
                                end_date = tmp
                            allDates.append((start_date, end_date))
                    else: # no from (startdate) to (enddate) matches. try only from (startdate) matches
                        matches = re.findall(alternatePattern, imageText)
                        if matches:
                            for match in matches:
                                start_date = match[2] or match[1] or match[0]
                                if start_date == match[1] or start_date == match[2]:
                                    tmp = start_date.split('-')
                                    if len(tmp) == 1: # date does not have hyphens
                                        slave = list()
                                        for month, number in monthConversion.items():
                                            matchingMonth = re.findall(month, start_date)
                                            if len(matchingMonth) != 0: # found corresponding month
                                                slave = start_date.split(matchingMonth[0])
                                                slave.insert(1, monthConversion[matchingMonth[0]])
                                                slave[2] = slave[2].replace("2023", "23")
                                                slave[2] = slave[2].replace("2024", "24")
                                                slave[2] = slave[2].replace("2025", "25")
                                                start_date = "".join(slave)
                                                break
                                    else:
                                        try: tmp[1] = monthConversion[tmp[1]]
                                        except KeyError: continue
                                        tmp[2] = tmp[2].replace("2023", "23")
                                        tmp[2] = tmp[2].replace("2024", "24")
                                        tmp[2] = tmp[2].replace("2025", "25")
                                        start_date = "".join(tmp)
                                elif start_date == match[0]: 
                                    tmp = start_date.replace("-", "")
                                    tmp = start_date.replace("/", "")
                                    tmp = tmp.replace("2023", "23")
                                    tmp = tmp.replace("2024", "24")
                                    tmp = tmp.replace("2025", "25")
                                    start_date = tmp
                                allDates.append((start_date, None))
                    if (endDate != '-' and (startDate, endDate) in allDates) or (endDate == '-' and (startDate, None) in allDates): 
                        foundMcStatusFile = True
                        # renaming MC file to include date
                        biggestNum = (0, None)
                        dateRegEx = r"([0-2][0-9]|3[01])([0][1-9]|1[0-2])([0-9]{2})"
                        for driveMcStatusTmp in driveMcStatusList:
                            try:  
                                start = re.findall(dateRegEx, driveMcStatusTmp['title'])
                                dates = [''.join(match) for match in start]
                                allFoundDates = list()
                                for date in dates:
                                    date_obj = datetime.strptime(date, "%d%m%y")
                                    allFoundDates.append(date_obj)
                                if allFoundDates: smallest_date = min(allFoundDates)
                                latestNum = (int(driveMcStatusTmp['title'].split(' ')[0]), smallest_date)
                            except ValueError: latestNum = (0, None)
                            if (latestNum[1] is not None and biggestNum[1] is not None and latestNum[1] > biggestNum[1]) or (biggestNum[1] is None and latestNum[1] is not None): 
                                biggestNum = latestNum
                        fileID = driveMcStatus['id']
                        num = None
                        if (biggestNum[1] is not None and datetime.strptime(startDate, "%d%m%y") > biggestNum[1]) or (biggestNum[1] is None): num = biggestNum[0]+1
                        elif (biggestNum[1] is not None and datetime.strptime(startDate, "%d%m%y") == biggestNum[1]): num = biggestNum[0]
                        else:# submitted MC is not the latest MC on the drive.                         
                            allFiles = list()
                            for driveMcStatusTmp in driveMcStatusList:
                                id = driveMcStatusTmp['id']
                                try:  
                                    int(driveMcStatusTmp['title'].split(' ')[0])
                                    num = driveMcStatusTmp['title'].split(' ')[0]
                                    start = re.findall(dateRegEx, driveMcStatusTmp['title'])
                                    dates = [''.join(match) for match in start]
                                    allFoundDates = list()
                                    for date in dates:
                                        date_obj = datetime.strptime(date, "%d%m%y")
                                        allFoundDates.append(date_obj)
                                    if allFoundDates: smallest_date = min(allFoundDates)
                                    if smallest_date > datetime.strptime(startDate, "%d%m%y"): 
                                        allFiles.append((id, num, smallest_date, driveMcStatusTmp['title']))
                                except ValueError: continue
                            allFiles = sorted(allFiles, key=lambda x: x[2])
                            num = allFiles[0][1] # take the next biggest number
                            for file in allFiles: # rename all number for files that come after current
                                updated_file = service.files().update(
                                    fileId=file[0],
                                    body={'name': file[3].replace(file[1], ("0" if int(file[1]) < 10 else "") + str(int(file[1])+1), 1)},
                                    fields='id, name'
                                ).execute()

                        if mcStatus[5] == "MC": newName = "{} MC {}-{}.{}".format((("0" if num < 10 else "") + str(num)), startDate, endDate, driveMcStatus['fileExtension'])
                        else: newName = "{} {} {}-{}.{}".format((("0" if num < 10 else "") + str(num)), mcStatus[6], startDate, endDate, driveMcStatus['fileExtension'])
                        updated_file = service.files().update(
                            fileId=fileID,
                            body={'name': newName},
                            fields='id, name'
                        ).execute()
                        break
                    else: 
                        if mcStatus[5] == "MC" and (mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)) not in possibleMcList: possibleMcList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                        elif mcStatus[5] == "Status" and (mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)) not in possibleStatusList: possibleStatusList.append((mcStatus[0], mcStatus[1], mcStatus[2], mcStatus[3], mcStatus[4], mcStatus[5], mcStatus[6], "https://drive.google.com/drive/folders/{}".format(folderId)))
                    del imageText, img
                    garbageCollector.collect()

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
        if send_whatsapp and (len(lapseMcList) > 0 or len(lapseStatusList) > 0):
            send_tele_msg("Sending MC & Status Lapses to WhatsApp", receiver_id="SUPERUSERS")
        if len(lapseMcList) == 0: send_tele_msg("No missing MC files", receiver_id=receiver_id)
        else:
            lapseMcList = sorted(lapseMcList, key=lambda x: datetime.strptime(x[1], "%d %b %y"), reverse=True)
            tele_msg = "Missing MC files:"
            cellUpdates = list()
            index = 2
            for mc in lapseMcList:
                startDate = mc[1]
                tmp = startDate.split(' ')
                try: tmp[1] = monthConversion[tmp[1]] # convert MMM to MM
                except IndexError: 
                    send_tele_msg("Unable to perform month conversion for {}".format(mc[1]))
                    raise IndexError
                startDate = ''.join(tmp)
                startDate = datetime.strptime(startDate, "%d%m%y")
                if mc[2] != "-": 
                    endDate = mc[2]
                    tmp = endDate.split(' ')
                    try: tmp[1] = monthConversion[tmp[1]] # convert MMM to MM
                    except IndexError: 
                        send_tele_msg("Unable to perform month conversion for {}".format(mc[1]))
                        raise IndexError
                    endDate = ''.join(tmp)
                    endDate = datetime.strptime(endDate, "%d%m%y")
                else: endDate = None
                if startDate < datetime.now()+timedelta(days=7) and startDate > datetime.now()-timedelta(days=365):
                    if endDate is None or (endDate < datetime.now()+timedelta(days=365) and endDate > datetime.now()-timedelta(days=365)):
                        cellUpdates.append(gspread.cell.Cell(index, 1, mc[0]))
                        cellUpdates.append(gspread.cell.Cell(index, 2, mc[1]))
                        cellUpdates.append(gspread.cell.Cell(index, 3, mc[2]))
                        cellUpdates.append(gspread.cell.Cell(index, 4, mc[3]))
                        cellUpdates.append(gspread.cell.Cell(index, 5, mc[4]))
                        cellUpdates.append(gspread.cell.Cell(index, 6, mc[6]))
                        cellUpdates.append(gspread.cell.Cell(index, 7, mc[7]))
                        index += 1

                if mc in possibleMcList: tele_msg = "\n".join([tele_msg, "{}".format(mc[0]) + ((" (P{}S{})".format(mc[3], mc[4])) if mc[3] != "HQ" else (" (HQ)")), "{} - {} (Possible MC found)\n{}\n".format(mc[1], mc[2], mc[7])])
                else: tele_msg = "\n".join([tele_msg, "{}".format(mc[0]) + ((" (P{}S{})".format(mc[3], mc[4])) if mc[3] != "HQ" else (" (HQ)")), "{} - {}\n{}\n".format(mc[1], mc[2], mc[7])])
                if len(tele_msg) > MAX_MESSAGE_LENGTH-1000:
                    send_tele_msg(tele_msg, receiver_id=receiver_id)
                    if send_whatsapp: response = greenAPI.sending.sendMessage(CHARLIE_Y2_ID, tele_msg)
                    tele_msg = "Missing MC files:"
            if len(cellUpdates) > 0: mcLapse.update_cells(cellUpdates)
            send_tele_msg(tele_msg, receiver_id=receiver_id)
            if send_whatsapp: response = greenAPI.sending.sendMessage(CHARLIE_Y2_ID, tele_msg)
        
        if len(lapseStatusList) == 0: send_tele_msg("No missing status files", receiver_id=receiver_id)
        else:
            lapseStatusList = sorted(lapseStatusList, key=lambda x: datetime.strptime(x[1], "%d %b %y"), reverse=True)
            tele_msg = "Missing status files:"
            cellUpdates = list()
            index = 2
            for status in lapseStatusList:
                startDate = status[1]
                tmp = startDate.split(' ')
                try: tmp[1] = monthConversion[tmp[1]] # convert MMM to MM
                except IndexError: 
                    send_tele_msg("Unable to perform month conversion for {}".format(status[1]))
                    raise IndexError
                startDate = ''.join(tmp)
                startDate = datetime.strptime(startDate, "%d%m%y")
                if status[2] != "-": 
                    endDate = status[2]
                    tmp = endDate.split(' ')
                    try: tmp[1] = monthConversion[tmp[1]] # convert MMM to MM
                    except IndexError: 
                        send_tele_msg("Unable to perform month conversion for {}".format(status[1]))
                        raise IndexError
                    endDate = ''.join(tmp)
                    endDate = datetime.strptime(endDate, "%d%m%y")
                else: endDate = None
                if startDate < datetime.now()+timedelta(days=7) and startDate > datetime.now()-timedelta(days=365):
                    if endDate is None or (endDate < datetime.now()+timedelta(days=365) and endDate > datetime.now()-timedelta(days=365)):
                        cellUpdates.append(gspread.cell.Cell(index, 1, status[0]))
                        cellUpdates.append(gspread.cell.Cell(index, 2, status[1]))
                        cellUpdates.append(gspread.cell.Cell(index, 3, status[2]))
                        cellUpdates.append(gspread.cell.Cell(index, 4, status[3]))
                        cellUpdates.append(gspread.cell.Cell(index, 5, status[4]))
                        cellUpdates.append(gspread.cell.Cell(index, 6, status[6]))
                        cellUpdates.append(gspread.cell.Cell(index, 7, status[7]))
                        index += 1

                if status in possibleStatusList: tele_msg = "\n".join([tele_msg, "{}".format(status[0]) + ((" (P{}S{})".format(status[3], status[4])) if status[3] != "HQ" else (" (HQ)")), "{} - {} (Possible status found)\n{}\n{}\n".format(status[1], status[2], status[6], status[7])])
                else: tele_msg = "\n".join([tele_msg, "{}".format(status[0]) + ((" (P{}S{})".format(status[3], status[4])) if status[3] != "HQ" else (" (HQ)")), "{} - {}\n{}\n{}\n".format(status[1], status[2], status[6], status[7])])
                if len(tele_msg) > MAX_MESSAGE_LENGTH-2000:
                    send_tele_msg(tele_msg, receiver_id=receiver_id)
                    if send_whatsapp: response = greenAPI.sending.sendMessage(CHARLIE_Y2_ID, tele_msg)
                    tele_msg = "Missing status files:"
            if len(cellUpdates) > 0: statusLapse.update_cells(cellUpdates)
            send_tele_msg(tele_msg, receiver_id=receiver_id)
            if send_whatsapp: response = greenAPI.sending.sendMessage(CHARLIE_Y2_ID, tele_msg)
    
        # Write checked mc/status files to avoid repeated checks
        mcStatusChecked.batch_clear(['A2:H1000'])
        cellUpdates = list()
        for index, status in enumerate(foundMcStatusFiles, start = 2):
            cellUpdates.append(gspread.cell.Cell(index, 1, status[0]))
            cellUpdates.append(gspread.cell.Cell(index, 2, status[1]))
            cellUpdates.append(gspread.cell.Cell(index, 3, status[2]))
            cellUpdates.append(gspread.cell.Cell(index, 4, status[3]))
            cellUpdates.append(gspread.cell.Cell(index, 5, status[4]))
            cellUpdates.append(gspread.cell.Cell(index, 6, status[5]))
            cellUpdates.append(gspread.cell.Cell(index, 7, status[6]))
            cellUpdates.append(gspread.cell.Cell(index, 8, status[7]))
        mcStatusChecked.update_cells(cellUpdates)
    except Exception as e:
        print("Encountered exception:\n{}".format(traceback.format_exc()))
        send_tele_msg("Encountered exception:\n{}".format(traceback.format_exc()))

def checkConductTracking(receiver_id = None):

    try:
        sheet = None
        for attempt in range(5):
            try: 
                sheet = gc.open("Charlie Conduct Tracking")
                break
            except SSLError as e:
                if attempt < 4: time.sleep(5)
                else: raise e
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

def updateWhatsappGrp(cet, tmpCmdsQ, receiver_id = None):
    
    dutyGrpId = DUTY_GRP_ID
    greenAPI = API.GreenAPI(WHATSAPP_ID_INSTANCE, WHATSAPP_TOKEN_INSTANCE)

    tmpDutyCmdsDict = dict()
    while not tmpCmdsQ.empty(): tmpDutyCmdsDict = tmpCmdsQ.get()
    # remove any outdated temporarily added duty commanders
    keysToDelete = list()
    for key, value in tmpDutyCmdsDict.items():
        if datetime.strptime(key, "%d%m%y").date() < datetime.now().date():
            keysToDelete.append(key)
    for key in keysToDelete: del tmpDutyCmdsDict[key]
    tmpDutyCmds = list()
    for key, value in tmpDutyCmdsDict.items():
        for name, number in value:
            tmpDutyCmds.append(number)
    # queue should only hold one list at a time.
    tmpCmdsQ.put(tmpDutyCmdsDict)

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
            if ('FP' in segment or "firstparade" in segment.replace(" ", "").lower()) and fpTime is None: fpTime = segment.replace("h", " ").split(" ")[0]
            if 'CDS' in segment: CDS = segment.split(': ')[-1].replace(" ", "").replace("3SG", "").replace("2SG", "").upper()
            elif 'PDS7' in segment: PDS7 = segment.split(': ')[-1].replace(" ", "").replace("3SG", "").replace("2SG", "").upper()
            elif 'PDS8' in segment: PDS8 = segment.split(': ')[-1].replace(" ", "").replace("3SG", "").replace("2SG", "").upper()
            elif 'PDS9' in segment: PDS9 = segment.split(': ')[-1].replace(" ", "").replace("3SG", "").replace("2SG", "").upper()
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
    url = "https://api.green-api.com/waInstance{}/getGroupData/{}".format(WHATSAPP_ID_INSTANCE, WHATSAPP_TOKEN_INSTANCE)
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
        response = supabase.table("profiles").select("*").execute()
        response = response.json()
        response = response.replace('rank', 'Rank').replace('name', 'Name').replace('platoon', 'Platoon').replace('section', 'Section').replace('email', 'Email').replace('contact', 'Contact').replace('appointment', 'Appointment').replace('duty_points', 'Duty points').replace('ration', 'Ration').replace('shirt_size', 'Shirt Size').replace('pants_size', 'Pants Size').replace('pes', 'PES')
        data = json.loads(response)
        charlieNominalRoll = data['data']
        nextDutyCmds = []
        foundCDS = False
        foundPDS7 = False
        foundPDS8 = False
        foundPDS9 = False
        for person in charlieNominalRoll:
            if CDS in person['Name'].replace(" ", "").upper() and person['Contact'] in CHARLIE_DUTY_CMDS: 
                nextDutyCmds.append(person['Contact'])
                foundCDS = True
            elif PDS7 in person['Name'].replace(" ", "").upper() and person['Contact'] in CHARLIE_DUTY_CMDS: 
                nextDutyCmds.append(person['Contact'])
                foundPDS7 = True
            elif PDS8 in person['Name'].replace(" ", "").upper() and person['Contact'] in CHARLIE_DUTY_CMDS: 
                nextDutyCmds.append(person['Contact'])
                foundPDS8 = True
            elif PDS9 in person['Name'].replace(" ", "").upper() and person['Contact'] in CHARLIE_DUTY_CMDS: 
                nextDutyCmds.append(person['Contact'])
                foundPDS9 = True
            if foundCDS and foundPDS7 and foundPDS8 and foundPDS9: break
        if not foundCDS: send_tele_msg("Unknown CDS: {}".format(CDS), receiver_id="SUPERUSERS")
        if not foundPDS7: send_tele_msg("Unknown PDS7: {}".format(PDS7), receiver_id="SUPERUSERS")
        if not foundPDS8: send_tele_msg("Unknown PDS8: {}".format(PDS8), receiver_id="SUPERUSERS")
        if not foundPDS9: send_tele_msg("Unknown PDS9: {}".format(PDS9), receiver_id="SUPERUSERS")
        allMembers = group_data['participants']
        for member in allMembers:
            memberId = member['id'].split('@c.us')[0][2:]
            if memberId not in PERM_DUTY_CMDS and memberId not in nextDutyCmds and memberId not in tmpDutyCmds: 
                if ENABLE_WHATSAPP_API: greenAPI.groups.removeGroupParticipant(dutyGrpId, member['id']) 

    # Adding new duty members if they are not already inside
    if ENABLE_WHATSAPP_API:
        for num in nextDutyCmds:
            greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(num))

    # Checking whether all members were added successfully
    url = "https://api.green-api.com/waInstance{}/getGroupData/{}".format(WHATSAPP_ID_INSTANCE, WHATSAPP_TOKEN_INSTANCE)
    payload = {
        "groupId": dutyGrpId  
    }
    response = rq.post(url, json=payload)
    if response.status_code == 200: group_data = response.json()
    else: 
        send_tele_msg("Unable to check whether all members were added successfully: {}.".format(response.json()), receiver_id="SUPERUSERS")
        group_data = None
    sendCET = True
    if group_data is not None: 
        allMembers = group_data['participants']
        allMemberNumbers = []
        for member in allMembers: allMemberNumbers.append(member['id'].split('@c.us')[0][2:])
        for memberId in nextDutyCmds:
            if memberId not in allMemberNumbers:
                for person in charlieNominalRoll:
                    if memberId == person['Contact']: 
                        name = person['Name'].upper()
                        send_tele_msg("{} - {} was not added succesfully".format(name.replace("3SG", "").replace("2SG", ""), memberId), receiver_id="SUPERUSERS")
                        sendCET = False
                        break
    # Sending new CET if all members were added successfully
    if ENABLE_WHATSAPP_API and sendCET: response = greenAPI.sending.sendMessage(dutyGrpId, cet)
    send_tele_msg("Updated duty group", receiver_id="SUPERUSERS")

def autoCheckMA():
    try:
        sheet = None
        for attempt in range(5):
            try: 
                sheet = gc.open("3GDS CHARLIE PARADE STATE")
                break
            except SSLError as e:
                if attempt < 4: time.sleep(5)
                else: raise e
        paradeStateSheet = sheet.worksheet("C COY")
        allValues = paradeStateSheet.get_all_values()
        allValues = list(zip(*allValues))
        mAs = allValues[44] # column AS
        names = allValues[43] # column AR
        platoons = allValues[40] # column AO
        sections = allValues[41] # column AP
        foundStart = False
        foundMA = False
        pattern = r'\d{6}' # 6 consecutive digits i.e 6 digit date
        secondPattern = r'\d{2}\s[A-Z][a-z]{2}\s\d{2}' # e.g. 28 Aug 23
        tele_msg = "Medical Appointments today ({}{}{}):".format((("0" + str(datetime.now().day)) if datetime.now().day < 10 else (str(datetime.now().day))), (("0" + str(datetime.now().month)) if datetime.now().month < 10 else (str(datetime.now().month))), str(datetime.now().year).replace("20", ""))
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
                tele_msg = "\n".join([tele_msg, "{} ".format(names[index]) + ((" (P{}S{})".format(platoons[index], sections[index])) if platoons[index] != "HQ" else (" (HQ)")) + "\n{}\n".format(ma)])
        if foundMA: send_tele_msg(tele_msg)
        else: send_tele_msg("No Medical Appointments today")
            
    except Exception as e:
        print("Encountered exception:\n{}".format(traceback.format_exc()))
        send_tele_msg("Encountered exception:\n{}".format(traceback.format_exc()))

def backup_charlie_nominal_roll():
    global charlieNominalRoll, allNames, allContacts
    try:
        send_tele_msg("Backing up Charlie Nominal Roll from Supabase onto Google Drive...", receiver_id="SUPERUSERS")
        gauth = GoogleAuth()
        creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_CREDENTIAL, ['https://www.googleapis.com/auth/drive'])
        service = build('drive', 'v3', credentials=creds)
        gauth.credentials = creds
        drive = GoogleDrive(gauth)
        all_backups = drive.ListFile({'q': f"'{SUPBASE_BACKUP_DRIVE_ID}' in parents and trashed=false"}).GetList()
        for backup in all_backups:
            if backup['title'] == '{}_Charlie Nominal Roll.csv'.format(datetime.now().date()):
                send_tele_msg("A backup from today already exists.", receiver_id="SUPERUSERS")
                return
        response = supabase.table("profiles").select("*").execute()
        response = response.json()
        response = response.replace('rank', 'Rank').replace('name', 'Name').replace('platoon', 'Platoon').replace('section', 'Section').replace('email', 'Email').replace('contact', 'Contact').replace('appointment', 'Appointment').replace('duty_points', 'Duty points').replace('ration', 'Ration').replace('shirt_size', 'Shirt Size').replace('pants_size', 'Pants Size').replace('pes', 'PES')
        data = json.loads(response)
        data = data['data']
        charlieNominalRoll = data
        allNames = [person['Name'] for person in charlieNominalRoll]
        allContacts = [person['Contact'] for person in charlieNominalRoll]
        field_order = ['id', 'Rank', 'Name', 'Platoon', 'Section', 'Email', 'Contact', 'Appointment', 'Duty points', 'Ration', 'Shirt Size', 'Pants Size', 'PES']  # Custom order
        data = [{k: v for k, v in row.items() if k in field_order} for row in data]
        csv_data = io.StringIO()
        writer = csv.DictWriter(csv_data, fieldnames=field_order)
        writer.writeheader()
        writer.writerows(data)
        csv_data.seek(0)
        file = drive.CreateFile({'title': '{}_Charlie Nominal Roll.csv'.format(datetime.now().date()), 'parents': [{'id': SUPBASE_BACKUP_DRIVE_ID}], 'mimeType': 'text/csv'}) 
        file.SetContentString(csv_data.getvalue())
        file.Upload()
        send_tele_msg("Done", receiver_id="SUPERUSERS")
    except Exception as e:
        send_tele_msg("Encountered exception trying to backup charlie nominal roll from supabase: {}".format(traceback.format_exc()), receiver_id="SUPERUSERS")

async def backupcharlienominalroll(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) in list(SUPERUSERS.values()):
        try: updateDutyGrpUserRequests[str(update.effective_user.id)]
        except KeyError: updateDutyGrpUserRequests[str(update.effective_user.id)] = None
        try: masterUserRequests[str(update.effective_user.id)]
        except KeyError: masterUserRequests[str(update.effective_user.id)] = None
        if masterUserRequests[str(update.effective_user.id)] is None or time.time() - masterUserRequests[str(update.effective_user.id)] > rateLimit:
            if updateDutyGrpUserRequests[str(update.effective_user.id)] is None or not updateDutyGrpUserRequests[str(update.effective_user.id)].is_alive():
                masterUserRequests[str(update.effective_user.id)] = time.time()
                backup_charlie_nominal_roll()
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

def conductTrackingFactory(haQ, service, oldCellsUpdate = None):
    '''
        :param oldCellsUpdate (list): The old list of cell updates to the sheet. To determine if there is a need to update the sheet again 
    '''
    try:
        try: 
            sheet = None
            for attempt in range(5):
                try: 
                    sheet = gc.open("Charlie Conduct Tracking")
                    break
                except SSLError as e:
                    if attempt < 4: time.sleep(5)
                    else: raise e
            worksheets = sheet.worksheets()             
            conductTrackingSheet = next(ws for ws in worksheets if ws.title == "CONDUCT TRACKING")
            allValues = conductTrackingSheet.get_all_values()
            formattedAllValues = list(zip(*allValues))
            haBuiltUpColumn = formattedAllValues[2]
            namesColumn = formattedAllValues[1]
            allHA = dict()
            cellsUpdate = list()
            cellRequests = list()
            foundHeader = False
            for index, row in enumerate(haBuiltUpColumn, start = 0):
                if row == 'HA BUILT UP': 
                    foundHeader = True
                    continue
                if not foundHeader: continue
                if row == 'TRUE': allHA[index] = list() # HA BUILT UP
                elif row == "FALSE": # HA NOT BUILT UP
                    cellsUpdate.append(gspread.cell.Cell(index+1, 4, ""))
                    cellsUpdate.append(gspread.cell.Cell(index+1, 5, ""))
                else: break
                endingRow = index

            currentDate = datetime.now().date()
            columnNum = len(formattedAllValues)-1
            while True: # for each column
                column = formattedAllValues[columnNum]
                if 'HAPT' in column[3]: 
                    conductDate = datetime.strptime(column[1], "%d%m%y").date()
                    if conductDate > currentDate: # conduct has not happened yet
                        if columnNum == 0: break
                        else: columnNum-=1
                        continue
                    if currentDate-conductDate > timedelta(days=27): break # ignore conducts that are more than 4 weeks ago
                    rowNum = 3
                    while True: # for each row in the column
                        if rowNum not in list(allHA.keys()): 
                            if rowNum == endingRow: break
                            else: rowNum += 1
                            continue
                        if column[rowNum] == 'TRUE': allHA[rowNum].append(conductDate)
                        if rowNum == endingRow: break
                        else: rowNum += 1

                if columnNum == 0: break
                else: columnNum-=1

            atRiskPersonnel = list()
            for row, conductDates in allHA.items():
                if len(conductDates) < 1: 
                    cellsUpdate.append(gspread.cell.Cell(row+1, 4, "NO"))
                    continue
                conductDates.reverse() # set to oldest first to newest
                haMaintainedDate = None
                for index, date in enumerate(conductDates, start = 0):
                    if index == 0: continue
                    if date-conductDates[index-1] <= timedelta(days=7): # 2 conducts within 7 days
                        haMaintainedDate = date+timedelta(days=1)
                
                if haMaintainedDate is None or currentDate - haMaintainedDate > timedelta(days=13): # no 2 conducts within 7 days in the past 14 days = HA broke
                    cellsUpdate.append(gspread.cell.Cell(row+1, 4, "NO"))
                    cellsUpdate.append(gspread.cell.Cell(row+1, 5, ""))
                elif currentDate - haMaintainedDate > timedelta(days=6): # ha maintained but last maintained HA activity is more than 7 days ago
                    cellsUpdate.append(gspread.cell.Cell(row+1, 4, "AT RISK"))
                    numActivities = 2
                    for date in conductDates: 
                        # only require one more activity
                        if date > haMaintainedDate and date <= currentDate: 
                            numActivities = 1
                            break
                    latestDate = haMaintainedDate+timedelta(days=13)
                    cellsUpdate.append(gspread.cell.Cell(row+1, 5, str(latestDate)))
                    if currentDate-haMaintainedDate >= timedelta(days=11): # red colour
                        cellRequests.append({
                            "updateCells": {
                                "range": {
                                    "sheetId": conductTrackingSheet.id,  # Sheet ID, use the gspread object to get the ID
                                    "startRowIndex": row,
                                    "endRowIndex": row+1,
                                    "startColumnIndex": 4,
                                    "endColumnIndex": 5
                                },
                                "rows": [{
                                    "values": [{
                                        "userEnteredFormat": {
                                            "backgroundColor": {
                                                "red": 0.92, 
                                                "green": 0.27,
                                                "blue": 0.2
                                            }
                                        }
                                    }]
                                }],
                                "fields": "userEnteredFormat.backgroundColor"
                            }
                        })
                    else: # orange colour
                        cellRequests.append({
                            "updateCells": {
                                "range": {
                                    "sheetId": conductTrackingSheet.id,  # Sheet ID, use the gspread object to get the ID
                                    "startRowIndex": row,
                                    "endRowIndex": row+1,
                                    "startColumnIndex": 4,
                                    "endColumnIndex": 5
                                },
                                "rows": [{
                                    "values": [{
                                        "userEnteredFormat": {
                                            "backgroundColor": {
                                                "red": 0.98, 
                                                "green": 0.73,
                                                "blue": 0.02
                                            }
                                        }
                                    }]
                                }],
                                "fields": "userEnteredFormat.backgroundColor"
                            }
                        })
                    if row >= 4 and row <= 20: platoon = "HQ"
                    elif row >= 21 and row <= 44: platoon = "7"
                    elif row >= 45 and row <= 69: platoon = "8"
                    elif row >= 70 and row <= 95: platoon = "9"
                    else: platoon = "COMMANDERS"
                    if numActivities == 2: atRiskPersonnel.append((namesColumn[row], platoon, "{} activities latest by {}".format(numActivities, latestDate.strftime("%d%m%y"))))
                    else: atRiskPersonnel.append((namesColumn[row], platoon, "{} activity latest by {}".format(numActivities, latestDate.strftime("%d%m%y"))))
                else: # HA maintained with more than 7 days validity
                    cellsUpdate.append(gspread.cell.Cell(row+1, 4, "YES"))
                    latestDate = haMaintainedDate+timedelta(days=13)
                    cellsUpdate.append(gspread.cell.Cell(row+1, 5, str(latestDate)))
                    cellRequests.append({
                        "updateCells": {
                            "range": {
                                "sheetId": conductTrackingSheet.id,  # Sheet ID, use the gspread object to get the ID
                                "startRowIndex": row,
                                "endRowIndex": row+1,
                                "startColumnIndex": 4,
                                "endColumnIndex": 5
                            },
                            "rows": [{
                                "values": [{
                                    "userEnteredFormat": {
                                        "backgroundColor": {
                                            "red": 0.20, 
                                            "green": 0.65,
                                            "blue": 0.33
                                        }
                                    }
                                }]
                            }],
                            "fields": "userEnteredFormat.backgroundColor"
                        }
                    })

            while not haQ.empty(): haQ.get()
            haQ.put(atRiskPersonnel)
            # only update sheet if there is a need to
            if (oldCellsUpdate is None or 
                (oldCellsUpdate is not None and oldCellsUpdate != cellsUpdate)): 
                conductTrackingSheet.update_cells(cellsUpdate)
                body = {'requests': cellRequests}
                response = service.spreadsheets().batchUpdate(spreadsheetId=conductTrackingSheet.spreadsheet_id, body=body).execute()
                return cellsUpdate
        except (requests.exceptions.JSONDecodeError, gspread.exceptions.APIError): # google API gave up momentarily
            return oldCellsUpdate
    except Exception as e:
        send_tele_msg("Encountered exception while trying to update conduct tracking sheet:\n{}".format(traceback.format_exc()), receiver_id="SUPERUSERS")
        return None

def main(cetQ, tmpCmdsQ, nominalRollQ, haQ, sheetNominalRollQ, googleSheetRequestsQ):
    # this function is executed in a separate process
    # use it to execute automated functions

    greenAPI = API.GreenAPI(WHATSAPP_ID_INSTANCE, WHATSAPP_TOKEN_INSTANCE)
    fpDateTime = None
    sentCdsReminder = False
    Daily = False
    conductTrackingReminder = False
    backedupSupabase = False
    oldCellsUpdate = None
    atRiskPersonnel = None
    weekDay = [1, 2, 3, 4, 5]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_CREDENTIAL, ['https://www.googleapis.com/auth/spreadsheets'])
    service = build('sheets', 'v4', credentials=creds)
    while True:

        # Auto updating of MC Lapses and MAs everyday at 0600
        # Also update charlie nominal roll in memory
        # Also send HA at risk personnel
        if not Daily and datetime.now().hour == 6 and datetime.now().minute == 0:
            send_tele_msg("Checking for MAs...")
            autoCheckMA()

            # Auto sending of temporary duty commanders list if any
            tmpDutyCmdsDict = dict()
            while not tmpCmdsQ.empty(): tmpDutyCmdsDict = tmpCmdsQ.get()
            # remove any outdated temporarily added duty commanders
            keysToDelete = list()
            for key, value in tmpDutyCmdsDict.items():
                if datetime.strptime(key, "%d%m%y").date() < datetime.now().date():
                    keysToDelete.append(key)
            for key in keysToDelete: del tmpDutyCmdsDict[key]
            for date, value in tmpDutyCmdsDict.items():
                tele_msg = "Temporary duty commanders until {}:\n".format(date)
                for index, slave in enumerate(value, start = 0): 
                    name, number = slave
                    if index == 0: tele_msg = "".join([tele_msg, (name if name != "Unknown" else number)])
                    else: tele_msg = ", ".join([tele_msg, (name if name != "Unknown" else number)])
                send_tele_msg(tele_msg, receiver_id = "SUPERUSERS")
            # queue should only hold one list at a time.
            tmpCmdsQ.put(tmpDutyCmdsDict)
            send_tele_msg("Checking for missing MC and Status files. This might take a while.")
            checkMcStatus(send_whatsapp=ENABLE_WHATSAPP_API)
            
            # updating charlie nominal roll in memory once per day
            response = supabase.table("profiles").select("*").execute()
            response = response.json()
            response = response.replace('rank', 'Rank').replace('name', 'Name').replace('platoon', 'Platoon').replace('section', 'Section').replace('email', 'Email').replace('contact', 'Contact').replace('appointment', 'Appointment').replace('duty_points', 'Duty points').replace('ration', 'Ration').replace('shirt_size', 'Shirt Size').replace('pants_size', 'Pants Size').replace('pes', 'PES')
            data = json.loads(response)
            charlieNominalRoll = data['data']
            allNames = [person['Name'] for person in charlieNominalRoll]
            allContacts = [person['Contact'] for person in charlieNominalRoll]
            nominalRollQ.put((charlieNominalRoll, allNames, allContacts))

            sheet = None
            for attempt in range(5):
                try: 
                    sheet = gc.open("Charlie Nominal Roll")
                    break
                except SSLError as e:
                    if attempt < 4: time.sleep(5)
                    else: raise e
            worksheets = sheet.worksheets()
            cCoyNominalRollSheet = next(ws for ws in worksheets if ws.title == "COMPANY ORBAT")
            allPerson = cCoyNominalRollSheet.get_all_values()
            sheetNominalRollQ.put((cCoyNominalRollSheet, allPerson))
            
            # sending of HA at risk personnel
            while atRiskPersonnel is None:
                while not haQ.empty():
                    atRiskPersonnel = haQ.get()
            haQ.put(atRiskPersonnel)
            priority_order = ['HQ', '7', '8', '9', 'COMMANDERS']
            atRiskPersonnel = sorted(atRiskPersonnel, key=lambda x: priority_order.index(x[1]))
            count_by_type = Counter([x[1] for x in atRiskPersonnel])
            numHQ = count_by_type.get("HQ", 0)
            num7 = count_by_type.get("7", 0)
            num8 = count_by_type.get("8", 0)
            num9 = count_by_type.get("9", 0)
            numCOMD = count_by_type.get("COMMANDERS", 0)
            tele_msg = "HA At Risk:"
            startHQ = False
            start7 = False
            start8 = False
            start9 = False
            startCOMD = False
            for person, platoon, details in atRiskPersonnel:
                if not startHQ and numHQ > 0 and platoon == "HQ":
                    tele_msg = "\n\n".join([tele_msg, "Coy HQ: ({})".format(numHQ)])
                    startHQ = True
                if not start7 and num7 > 0 and platoon == "7":
                    tele_msg = "\n\n".join([tele_msg, "P7: ({})".format(num7)])
                    start7 = True
                if not start8 and num8 > 0 and platoon == "8":
                    tele_msg = "\n\n".join([tele_msg, "P8: ({})".format(num8)])
                    start8 = True
                if not start9 and num9 > 0 and platoon == "9":
                    tele_msg = "\n\n".join([tele_msg, "P9: ({})".format(num9)])
                    start9 = True
                if not startCOMD and numCOMD > 0 and platoon == "COMMANDERS":
                    tele_msg = "\n\n".join([tele_msg, "COMMANDERS: ({})".format(numCOMD)])
                    startCOMD = True
                tele_msg = "\n".join([tele_msg, "{}\n{}\n".format(person, details)])
                if len(tele_msg) > MAX_MESSAGE_LENGTH-2000:
                    send_tele_msg(tele_msg)
                    if ENABLE_WHATSAPP_API: response = greenAPI.sending.sendMessage(CHARLIE_Y2_ID, tele_msg)
                    tele_msg = "HA At Risk:"
            if len(atRiskPersonnel) != 0: 
                send_tele_msg(tele_msg)
                if ENABLE_WHATSAPP_API: 
                    response = greenAPI.sending.sendMessage(CHARLIE_Y2_ID, tele_msg)
                    send_tele_msg("Sending HA at risk personnel to WhatsApp", receiver_id="SUPERUSERS")
            Daily = True
        
        elif datetime.now().hour == 6 and datetime.now().minute != 0: Daily = False

        # send reminder to update conduct tracking sheet if any activites were done for the day
        if not conductTrackingReminder and datetime.now().hour == 21 and datetime.now().minute == 0: 
            try:
                sheet = None
                for attempt in range(5):
                    try: 
                        sheet = gc.open("Charlie Conduct Tracking")
                        break
                    except SSLError as e:
                        if attempt < 4: time.sleep(5)
                        else: raise e
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

                # for each conduct TODAY if any
                tele_msg = "Hi all, please be reminded to update the conducts today in the conduct tracking sheet:"
                for index in colIndexes:
                    conductName = conductTrackingSheet.col_values(index)[3]
                    tele_msg = "\n".join([tele_msg, conductName])
                tele_msg = "\n".join([tele_msg, "https://docs.google.com/spreadsheets/d/1TBHzKqmEHmyONaMQJoqt4HWwdsoY0pRSEv8WSoXDmyw/edit?gid=1000647342#gid=1000647342"])
                if ENABLE_WHATSAPP_API and len(colIndexes) > 0: 
                    send_tele_msg("Sending conduct tracking reminder to WhatsApp", receiver_id="SUPERUSERS")
                    response = greenAPI.sending.sendMessage(CHARLIE_Y2_ID, tele_msg)

            except Exception as e:
                send_tele_msg("Encountered exception trying to send update conduct tracking reminder:\n{}".format(traceback.format_exc()))

            conductTrackingReminder = True
        
        elif datetime.now().hour == 21 and datetime.now().minute != 0: conductTrackingReminder = False

        try: # Auto reminding of CDS to send report sick parade state every morning 
            while not cetQ.empty(): 
                sentCdsReminder = False
                fpDateTime = cetQ.get()
                # got latest CET
                # check whether date and time is correct
                if cetQ.empty(): 
                    if fpDateTime is None: pass
                    elif datetime.strptime(fpDateTime[0]+fpDateTime[1], "%d%m%y%H%M") > datetime.now(): send_tele_msg("CDS reminder for report sick parade state scheduled at {} {}".format(fpDateTime[0], fpDateTime[1]), receiver_id="SUPERUSERS")
                    else: 
                        send_tele_msg("Invalid CET date to schedule CDS reminder.", receiver_id=fpDateTime[2])
                        fpDateTime = None
        except Exception as e:
            print("Encountered exception:\n{}".format(traceback.format_exc()))
            send_tele_msg("Encountered exception while trying to schedule CDS reminder:\n{}".format(traceback.format_exc()), receiver_id="SUPERUSERS")
        
        # there was a sent CET since the start of the bot
        if fpDateTime is not None:
            # send reminder during weekdays when it hits the FP date and time of sent CET
            if datetime.now().isoweekday() in weekDay and datetime.now().day == int(fpDateTime[0][:2]) and datetime.now().hour == int(fpDateTime[1][:2]) and datetime.now().minute == int(fpDateTime[1][-2:]) and not sentCdsReminder:
                send_tele_msg("Sending automated CDS reminder", receiver_id="SUPERUSERS")
                if ENABLE_WHATSAPP_API: response = greenAPI.sending.sendMessage(CHARLIE_Y2_ID, "This is an automated reminder for the CDS to send the REPORT SICK PARADE STATE\nhttps://docs.google.com/spreadsheets/d/1y6q2rFUE_dbb-l_Ps3R3mQVSPJT_DB_kDys1uyFeXRg/edit?gid=802597665#gid=802597665")
                sentCdsReminder = True

        # Monthly backup of supabase nominal roll
        if not backedupSupabase and datetime.now().day == 1:
            backup_charlie_nominal_roll()
            backedupSupabase = True
        elif datetime.now().day != 1: backedupSupabase = False

        # update conduct tracking sheet
        oldCellsUpdate = conductTrackingFactory(haQ, service, oldCellsUpdate)

        time.sleep(2)

NORMAL_USER_COMMANDS = "Available Commands:\n/checkmcstatus -> Check for MC/Status files\n/checkconduct -> Conduct Tracking Updates\
\n/generateIR -> Help to generate IR\n/gethaatrisk -> Get list of HA at risk personnel"
ALL_COMMANDS = "Available Commands:\n/checkmcstatus -> Check for MC/Status files\n/checkconduct -> Conduct Tracking Updates\
\n/updatedutygrp -> Update duty group and schedule CDS reminder according to CET\n/addtmpmember -> Add temporary duty commanders to duty group\
\n/resettmpdutycmds -> Reset list of temporary duty commanders\n/gethaatrisk -> Get list of HA at risk personnel\n/updateconducttracking -> Update conduct tracking sheet according to TimeTree\
\n/generateIR -> IR generator\n/backupcharlienominalroll -> Backup charlie nominal roll from supabase to google drive"

async def helpHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) in list(CHANNEL_IDS.values()): 
        try: masterUserRequests[str(update.effective_user.id)]
        except KeyError: masterUserRequests[str(update.effective_user.id)] = None
        if masterUserRequests[str(update.effective_user.id)] is None or time.time() - masterUserRequests[str(update.effective_user.id)] > rateLimit:
            masterUserRequests[str(update.effective_user.id)] = time.time()
            if str(update.effective_user.id) not in list(SUPERUSERS.values()): await update.message.reply_text(NORMAL_USER_COMMANDS)
            else: await update.message.reply_text(ALL_COMMANDS)
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
                await update.message.reply_text("Checking for missing MC and Status files. This might take a while.")
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
    global foundResponse
    if str(update.effective_user.id) in list(SUPERUSERS.values()):
        try: updateConductUserRequests[str(update.effective_user.id)]
        except KeyError: updateConductUserRequests[str(update.effective_user.id)] = None
        try: masterUserRequests[str(update.effective_user.id)]
        except KeyError: masterUserRequests[str(update.effective_user.id)] = None
        if masterUserRequests[str(update.effective_user.id)] is None or time.time() - masterUserRequests[str(update.effective_user.id)] > rateLimit:
            if updateConductUserRequests[str(update.effective_user.id)] is None or not updateConductUserRequests[str(update.effective_user.id)].is_alive():
                masterUserRequests[str(update.effective_user.id)] = time.time()
                send_tele_msg("Updating conduct tracking...", receiver_id="SUPERUSERS")
                foundResponse = False
                asyncio.get_event_loop().run_until_complete(timetreeResponses())
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
    t1 = threading.Thread(target=updateWhatsappGrp, args=(cet, tmpDutyCmdsQueue, str(update.effective_user.id),))
    t1.start()
    updateDutyGrpUserRequests[str(update.effective_user.id)] = t1
    return ConversationHandler.END

ADD_TMP_MEMBER = 0
ADD_TMP_DATE = 1
CONSOLIDATE_TMP_DATE = 2

addtmpmemberUserRequests = dict()
async def addtmpmember(update: Update, context: CallbackContext) -> int:
    global charlieNominalRoll, allNames, allContacts
    if str(update.effective_user.id) in list(SUPERUSERS.values()):
        try: addtmpmemberUserRequests[str(update.effective_user.id)]
        except KeyError: addtmpmemberUserRequests[str(update.effective_user.id)] = None
        try: masterUserRequests[str(update.effective_user.id)]
        except KeyError: masterUserRequests[str(update.effective_user.id)] = None
        if masterUserRequests[str(update.effective_user.id)] is None or time.time() - masterUserRequests[str(update.effective_user.id)] > rateLimit:
            if addtmpmemberUserRequests[str(update.effective_user.id)] is None or not addtmpmemberUserRequests[str(update.effective_user.id)].is_alive():
                masterUserRequests[str(update.effective_user.id)] = time.time()
                await update.message.reply_text("Send the name/number of the temporary member to add. Send /cancel to cancel the request at any time.")
                # Auto sending of temporary duty commanders list if any
                tmpDutyCmdsDict = dict()
                while not tmpDutyCmdsQueue.empty(): tmpDutyCmdsDict = tmpDutyCmdsQueue.get()
                # remove any outdated temporarily added duty commanders
                keysToDelete = list()
                for key, value in tmpDutyCmdsDict.items():
                    if datetime.strptime(key, "%d%m%y").date() < datetime.now().date():
                        keysToDelete.append(key)
                for key in keysToDelete: del tmpDutyCmdsDict[key]
                for date, value in tmpDutyCmdsDict.items():
                    tele_msg = "Temporary duty commanders until {}:\n".format(date)
                    for index, slave in enumerate(value, start = 0): 
                        name, number = slave
                        if index == 0: tele_msg = "".join([tele_msg, (name if name != "Unknown" else number)])
                        else: tele_msg = ", ".join([tele_msg, (name if name != "Unknown" else number)])
                    send_tele_msg(tele_msg, receiver_id = "SUPERUSERS")
                # queue should only hold one list at a time.
                tmpDutyCmdsQueue.put(tmpDutyCmdsDict)
                while not nominalRollQueue.empty(): charlieNominalRoll, allNames, allContacts = nominalRollQueue.get()
                return ADD_TMP_MEMBER
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

async def addmembernames(update: Update, context: CallbackContext) -> int:
    global charlieNominalRoll, allNames, allContacts
    try:
        int(update.message.text)
        num_digits = len(update.message.text)
        if num_digits != 8: 
            await update.message.reply_text("Invalid number {}. Please provide another name/number:".format(update.message.text))
            return ADD_TMP_MEMBER

        tmpname = "Unknown"
        for index, value in enumerate(allContacts, start = 0):
            if value == update.message.text:
                tmpname = allNames[index]
                break
        
        for number in PERM_DUTY_CMDS:
            if number == update.message.text:
                await update.message.reply_text("{} ({}) is already a permanent member of the duty group. Please provide another name/number:".format(tmpname, number))
                return ADD_TMP_MEMBER
        for key, value in tmpDutyCmdsDict.items():
            for name, number in value:
                if number == update.message.text: 
                    await update.message.reply_text("{} is already a temporary member of the duty group. Please provide another name/number:".format(name if name != "Unknown" else number))
                    await update.message.reply_text("If you would like to change how long {} stays as a temporary commander, reset all temporary commanders".format(name if name != "Unknown" else number))
                    return ADD_TMP_MEMBER
        for name, number in tmpDutyCmdsList:
            if number == update.message.text: 
                await update.message.reply_text("{} is already a pending temporary member of the duty group. Please provide another name/number:".format(name if name != "Unknown" else number))
                return ADD_TMP_MEMBER
        tmpDutyCmdsList.append((tmpname, update.message.text))
        reply_keyboard = [['No']]
        await update.message.reply_text("Send the name/number of the next member to add. Otherwise send no.", 
                                        reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True))
        return ADD_TMP_MEMBER

    except ValueError: pass
    
    userInput = update.message.text
    formatteduserInput = userInput.replace(" ", "").upper()
    if formatteduserInput == "NO": return await addtmpdate(update, context)
    allMatches = list()
    foundPersonnel = False
    tmpname = None
    for index, name in enumerate(allNames, start = 0):
        if formatteduserInput in name.replace(" ", "").upper():
            allMatches.append((name, allContacts[index]))
            tmpname = name
            tmpnum = allContacts[index]
            foundPersonnel = True
    if not foundPersonnel: 
        await update.message.reply_text("Unable to find {}. Please provide another name/number:".format(userInput))
        return ADD_TMP_MEMBER
    if len(allMatches) > 1: # more than one match found
        reply_keyboard = [[name[0] for name in allMatches]]
        await update.message.reply_text(
            "Please specify the personnel involved:",
            reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True))
        return ADD_TMP_MEMBER

    for index, name in enumerate(allNames, start = 0):
        if tmpname == name and allContacts[index] in PERM_DUTY_CMDS:
            await update.message.reply_text("{} is already a permanent member of the duty group. Please provide another name/number".format(userInput))
            return ADD_TMP_MEMBER
    for key, value in tmpDutyCmdsDict.items():
        for name, number in value:
            if name == tmpname: 
                await update.message.reply_text("{} is already a temporary member of the duty group. Please provide another name/number:".format(userInput))
                await update.message.reply_text("If you would like to change how long {} stays as a temporary commander, reset all temporary commanders".format(userInput))
                return ADD_TMP_MEMBER
    for name, number in tmpDutyCmdsList:
        if name == tmpname: 
            await update.message.reply_text("{} is already pending temporary member of the duty group. Please provide another name/number:".format(userInput))
            return ADD_TMP_MEMBER
    
    tmpDutyCmdsList.append((tmpname, tmpnum))
    reply_keyboard = [['No']]
    await update.message.reply_text("Send the name/number of the next member to add. Otherwise send no.", 
                                    reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True))
    return ADD_TMP_MEMBER

async def addtmpdate(update: Update, context: CallbackContext) -> int:
    await update.message.reply_text("Send the last date (inclusive) to keep the temporary members (e.g. {}):".format(datetime.now().strftime('%d%m%y')))
    return CONSOLIDATE_TMP_DATE

async def consolidatetmpdate(update: Update, context: CallbackContext) -> int:
    date = update.message.text
    try: date_object = datetime.strptime(date, "%d%m%y").date()
    except Exception as e: 
        await update.message.reply_text("Unable to interpret {}. Please provide another date in the format {}:".format(date, datetime.now().strftime('%d%m%y')))
        return CONSOLIDATE_TMP_DATE
    if date_object >= datetime.now().date():
        try: tmpDutyCmdsDict[date].extend(copy.deepcopy(tmpDutyCmdsList))
        except KeyError: tmpDutyCmdsDict[date] = copy.deepcopy(tmpDutyCmdsList)
        # adding the temporary members to whatsapp group if they are not already inside.
        dutyGrpId = DUTY_GRP_ID
        greenAPI = API.GreenAPI(WHATSAPP_ID_INSTANCE, WHATSAPP_TOKEN_INSTANCE)
        for name, number in tmpDutyCmdsList:
            if ENABLE_WHATSAPP_API: greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(number))
        send_tele_msg("Added {} as temporary duty commanders until {}".format(str([(t[0] if t[0] != "Unknown" else t[1]) for t in tmpDutyCmdsList]).replace("['", "").replace("']", "").replace("'", ""), date), receiver_id="SUPERUSERS")
        tmpDutyCmdsList.clear()
        # flush the queue before adding a new list
        while not tmpDutyCmdsQueue.empty(): tmpDutyCmdsQueue.get()
        tmpDutyCmdsQueue.put(tmpDutyCmdsDict)
        return ConversationHandler.END
    else:
        await update.message.reply_text("Invalid date: {}. Please provide another date:".format(date))
        return CONSOLIDATE_TMP_DATE

async def cancel_tempmembers(update: Update, context: CallbackContext) -> int:
    if str(update.effective_user.id) in list(SUPERUSERS.values()):
        send_tele_msg('Operation cancelled.', receiver_id="SUPERUSERS")
        tmpDutyCmdsList.clear()
        return ConversationHandler.END
    elif str(update.effective_user.id) not in list(SUPERUSERS.values()) and str(update.effective_user.id) in list(CHANNEL_IDS.values()):
        await update.message.reply_text("You are not authorised to use this function. Contact Charlie HQ specs for assistance.")
        return ConversationHandler.END
    else: 
        await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")
        return ConversationHandler.END

async def resettmpdutycmds(update: Update, context: CallbackContext) -> int:
    if str(update.effective_user.id) in list(SUPERUSERS.values()):
        tmpDutyCmdsDict.clear()
        tmpDutyCmdsList.clear()
        while not tmpDutyCmdsQueue.empty(): tmpDutyCmdsQueue.get()
        send_tele_msg("Resetted temporary duty commanders.", receiver_id="SUPERUSERS")
        return ConversationHandler.END
    elif str(update.effective_user.id) not in list(SUPERUSERS.values()) and str(update.effective_user.id) in list(CHANNEL_IDS.values()):
        await update.message.reply_text("You are not authorised to use this function. Contact Charlie HQ specs for assistance.")
        return ConversationHandler.END
    else: 
        await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")
        return ConversationHandler.END

async def gethaatrisk(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) in list(CHANNEL_IDS.values()): 
        try: masterUserRequests[str(update.effective_user.id)]
        except KeyError: masterUserRequests[str(update.effective_user.id)] = None
        if masterUserRequests[str(update.effective_user.id)] is None or time.time() - masterUserRequests[str(update.effective_user.id)] > rateLimit:
            masterUserRequests[str(update.effective_user.id)] = time.time()
            atRiskPersonnel = None
            while atRiskPersonnel is None:
                while not haQueue.empty():
                    atRiskPersonnel = haQueue.get()
            haQueue.put(atRiskPersonnel)
            priority_order = ['HQ', '7', '8', '9', 'COMMANDERS']
            atRiskPersonnel = sorted(atRiskPersonnel, key=lambda x: priority_order.index(x[1]))
            count_by_type = Counter([x[1] for x in atRiskPersonnel])
            numHQ = count_by_type.get("HQ", 0)
            num7 = count_by_type.get("7", 0)
            num8 = count_by_type.get("8", 0)
            num9 = count_by_type.get("9", 0)
            numCOMD = count_by_type.get("COMMANDERS", 0)
            tele_msg = "HA At Risk:"
            startHQ = False
            start7 = False
            start8 = False
            start9 = False
            startCOMD = False
            for person, platoon, details in atRiskPersonnel:
                if not startHQ and numHQ > 0 and platoon == "HQ":
                    tele_msg = "\n\n".join([tele_msg, "Coy HQ: ({})".format(numHQ)])
                    startHQ = True
                if not start7 and num7 > 0 and platoon == "7":
                    tele_msg = "\n\n".join([tele_msg, "P7: ({})".format(num7)])
                    start7 = True
                if not start8 and num8 > 0 and platoon == "8":
                    tele_msg = "\n\n".join([tele_msg, "P8: ({})".format(num8)])
                    start8 = True
                if not start9 and num9 > 0 and platoon == "9":
                    tele_msg = "\n\n".join([tele_msg, "P9: ({})".format(num9)])
                    start9 = True
                if not startCOMD and numCOMD > 0 and platoon == "COMMANDERS":
                    tele_msg = "\n\n".join([tele_msg, "COMMANDERS: ({})".format(numCOMD)])
                    startCOMD = True
                tele_msg = "\n".join([tele_msg, "{}\n{}\n".format(person, details)])
                if len(tele_msg) > MAX_MESSAGE_LENGTH-2000:
                    send_tele_msg(tele_msg, receiver_id=str(update.effective_user.id))
                    tele_msg = "HA At Risk:"
            if len(atRiskPersonnel) != 0: send_tele_msg(tele_msg, receiver_id=str(update.effective_user.id))
            else: send_tele_msg("No HA at risk", receiver_id=str(update.effective_user.id))
        else: await update.message.reply_text("Sir stop sir. Too many requests at one time. Please try again later.")
    else: await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")

NEW, CHECK_PREV_IR, PREV_IR, TRAINING, NAME, CHECK_PES, DATE_TIME, LOCATION, DESCRIPTION, STATUS, FOLLOW_UP, NOK, REPORTED_BY = range(13)

async def start(update: Update, context: CallbackContext) -> int:
    global charlieNominalRoll, allNames, allContacts, googleSheetsNominalRoll, allPerson
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
            reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True)
        )
        while not nominalRollQueue.empty(): charlieNominalRoll, allNames, allContacts = nominalRollQueue.get()
        while not googleSheetNominalRollQueue.empty(): googleSheetsNominalRoll, allPerson = googleSheetNominalRollQueue.get()
        return CHECK_PREV_IR
    else: 
        await update.message.reply_text("Sir stop sir. Too many requests at one time. Please try again later.")
        return ConversationHandler.END

async def checkPrevIR(update: Update, context: CallbackContext) -> int:
    if update.message.text.upper() not in ['NEW', 'UPDATE', 'FINAL']: 
        await update.message.reply_text("Unrecognised response: {}. Please enter New or Update or Final.".format(update.message.text))
        return CHECK_PREV_IR
    context.user_data['new'] = update.message.text.upper()
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
    global charlieNominalRoll, allNames, allContacts, googleSheetsNominalRoll, allPerson
    if not context.user_data['checkingName']: userInput = update.message.text
    else: 
        if isinstance(context.user_data['nameToBeChecked'], list): userInput = update.message.text
        else:
            context.user_data['nameToBeChecked'] = context.user_data['nameToBeChecked'].split(' ')
            del context.user_data['nameToBeChecked'][0] # remove rank
            userInput = " ".join(context.user_data['nameToBeChecked'])

    formatteduserInput = userInput.replace(" ", "").upper()
    allMatches = list()
    foundPersonnel = False
    for index, person in enumerate(charlieNominalRoll, start = 0):
        if formatteduserInput in person['Name'].replace(" ", "").upper():
            allMatches.append(person['Name'])
            context.user_data['name'] = person
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
    if context.user_data['name']['PES'] == "":
        reply_keyboard = [['A', 'B1']]
        await update.message.reply_text(
            "What is the PES status of {} ?".format(userInput),
            reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True))
        return CHECK_PES
    if context.user_data['findingName'] or context.user_data['checkingName']: return await location(update, context)
    await update.message.reply_text("Please provide the date and time of the incident (Current date & time: {}):".format(datetime.now().strftime('%d%m%y %H%M')))
    return DATE_TIME

async def checkPes(update: Update, context: CallbackContext) -> int:
    if not context.user_data['findingDateTime']:
        allPesStatus = ['A', 'B1', 'B2', 'B3', 'B4', 'C2', 'C9', 'D', 'E1', 'E9', 'F', 'BP']
        pes = update.message.text
        if pes not in allPesStatus: 
            await update.message.reply_text("Unknown PES Status: {}. Please send another PES status:".format(pes))
            return CHECK_PES
        context.user_data['name']['PES'] = pes
    if context.user_data['findingName'] or context.user_data['checkingName']: return await location(update, context)
    await update.message.reply_text("Please provide the date and time of the incident (Current date & time: {}):".format(datetime.now().strftime('%d%m%y %H%M')))
    return DATE_TIME

async def date_time(update: Update, context: CallbackContext) -> int:
    if not context.user_data['findingLocation']:
        userInput = update.message.text.replace(" ", "")
        if len(userInput) != 10:
            await update.message.reply_text("Unrecognised datetime {}. Please provide another date and time in the format ({}):".format(update.message.text, datetime.now().strftime('%d%m%y %H%M')))
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
            if description is None or context.user_data['shiftingStatus']: raise KeyError
            foundDescription = True
        except KeyError: 
            foundDescription = False
            description = None
        try: 
            status = context.user_data['status']
            if status is None or context.user_data['shiftingStatus']: raise KeyError
            foundStatus = True
        except KeyError: 
            foundStatus = False
            status = None

        for line in lines:
            if not foundNatureOfIncident and line.replace("*", "").replace(" ", "").replace(":", "").lower() == "1)natureandtypeofincident":
                foundNatureOfIncident = True
                continue
            if not foundNatureOfIncident: continue
            if line == "": continue
            if natureOfIncident is None: 
                context.user_data['training_related'] = line.replace(" Related", "").replace("Related", "")
                natureOfIncident = line.replace(" Related", "").replace("Related", "")
                continue

            if not foundName and line.replace("*", "").replace(" ", "").replace(":", "").lower() == "2)detailsofpersonnelinvolved":
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

            if not foundDateTime and line.replace("*", "").replace(" ", "").replace(":", "").lower() == "3)date&timeofincident":
                foundDateTime = True
                continue
            if not foundDateTime: continue
            if line == "": continue
            if dateTime is None:
                dateTime = line.replace("/", "").replace(" ", "").replace("hrs", "").replace("hr", "")
                context.user_data['date_time'] = line.replace("/", "").replace(" ", "").replace("hrs", "").replace("hr", "")
                continue

            if not foundLocation and line.replace("*", "").replace(" ", "").replace(":", "").lower() == "4)locationofincident":
                foundLocation = True
                continue
            if not foundLocation: continue
            if line == "": continue
            if location is None:
                location = line
                context.user_data['location'] = line
                continue
                
            if not foundDescription and line.replace("*", "").replace(" ", "").replace(":", "").lower() == "5)briefdescription":
                foundDescription = True
                continue
            if not foundDescription: continue
            if line == "": continue
            if description is None or (not foundStatus and line.replace("*", "").replace(" ", "").replace(":", "").lower() != "6)currentstatus"):
                if description is None: description = line
                else: description = description + "\n\n" + line
                context.user_data['description'] = description
                continue

            if not foundStatus and line.replace("*", "").replace(" ", "").replace(":", "").lower() == "6)currentstatus":
                foundStatus = True
                continue
            if not foundStatus: continue
            if line == "": continue
            status = line
            break
        
        if natureOfIncident is not None and natureOfIncident in ['Training', 'Non-Training', 'Non Training']: 
            if natureOfIncident == 'Non Training': context.user_data['training_related'] = 'Non-Training'
            else: context.user_data['training_related'] = natureOfIncident
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
            await update.message.reply_text("Shift the status to the description ?",
                                            reply_markup=telegram.ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True, resize_keyboard=True))
            return LOCATION

        if description is not None:
            reply_keyboard = [['No Changes']]
            await update.message.reply_text("Update the description following the below text")
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
            await update.message.reply_text("On {} at about {}hrs, {} {}...".format(context.user_data['date_time'][:6], context.user_data['date_time'][-4:], context.user_data['name']['Rank'], context.user_data['name']['Name']))
            await update.message.reply_text("Refer to the templates below when writing your description:\n\n*Normal Report Sick*\n\\.\\.\\. requested permission from *\\(RANK \\+ NAME\\)* to report sick at *\\(LOCATION\\)* for *\\(REASON\\)*\\.\n\n*Medical Appointment*\n\\.\\.\\. has left *\\(LOCATION\\)* to attend his medical appointment at *\\(LOCATION\\)* for his *\\(TYPE\\)* *\\(medical appointment\\/surgery\\)*", parse_mode='MarkdownV2')
            return DESCRIPTION

    else: # new IR
        context.user_data['location'] = update.message.text
        await update.message.reply_text("Please write the description following the below text")
        await update.message.reply_text("On {} at about {}hrs, {} {}...".format(context.user_data['date_time'][:6], context.user_data['date_time'][-4:], context.user_data['name']['Rank'], context.user_data['name']['Name']))
        await update.message.reply_text("Refer to the templates below when writing your description:\n\n*Normal Report Sick*\n\\.\\.\\. requested permission from *\\(RANK \\+ NAME\\)* to report sick at *\\(LOCATION\\)* for *\\(REASON\\)*\\.\n\n*Medical Appointment*\n\\.\\.\\. has left *\\(LOCATION\\)* to attend his medical appointment at *\\(LOCATION\\)* for his *\\(TYPE\\)* *\\(medical appointment\\/surgery\\)*", parse_mode='MarkdownV2')
        return DESCRIPTION

async def description(update: Update, context: CallbackContext) -> int:
    if update.message.text != 'No Changes' and not context.user_data['usingPrevIR']:
        context.user_data['description'] = "On {} at about {}hrs, {} {} ".format(context.user_data['date_time'][:6], context.user_data['date_time'][-4:], context.user_data['name']['Rank'], context.user_data['name']['Name']) + update.message.text
    elif update.message.text != 'No Changes' and context.user_data['usingPrevIR']:
        context.user_data['description'] = context.user_data['description'] + "\n\n" + update.message.text
    await update.message.reply_text("What is the current status?")
    if context.user_data['new'] == "NEW": 
        await update.message.reply_text("Refer to the template below when writing your status:\n\nServiceman is currently making his way to *\\(LOCATION\\)*\\.", parse_mode='MarkdownV2')
    else:
        await update.message.reply_text("Refer to the templates below when writing your status:\n\n*Normal Report Sick*\nAs of *\\(TIME\\)hrs*, serviceman has received *\\(DURATION OF MC\\/STATUS \\+ WHAT MC\\/STATUS\\)* from *\\(START DATE\\)* to *\\(END DATE\\)* inclusive\\.\n\n*Medical Appointments*\nAs of *\\(TIME\\)hrs*, serviceman has completed his appointment\\.\\.\\.\n\n\
1\\) with no status\\.\n\n\
2\\) and was given *\\(DURATION OF MC\\/STATUS \\+ WHAT MC\\/STATUS\\)* from *\\(START DATE\\)* to *\\(END DATE\\)* inclusive\\.\n\n\
3\\) *\\(If Applicable\\)* and was scheduled a follow up appointment on *\\(DATE OF FOLLOW UP APPT\\)*\\.\n\n\
*\\(If Applicable\\) *Serviceman is currently headed home to consume his MC\\.", parse_mode='MarkdownV2')
    return STATUS

async def status(update: Update, context: CallbackContext) -> int:
    context.user_data['status'] = update.message.text
    if context.user_data['new'] == "FINAL": reply_keyboard = [['Unit will proceed to close the case.']]
    elif context.user_data['new'] == "NEW" or context.user_data['new'] == "UPDATE": reply_keyboard = [['Unit will monitor and update accordingly.']]
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
    global charlieNominalRoll, allNames, allContacts, googleSheetsNominalRoll, allPerson
    userInput = update.message.text
    formatteduserInput = userInput.replace(" ", "").upper()
    reportingPerson = None
    allMatches = list()
    for index, name in enumerate(allNames, start = 0):
        if formatteduserInput in name.replace(" ", ""):
            allMatches.append(allNames[index])
            reportingPerson = charlieNominalRoll[index]
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
    nric = None
    for person in allPerson:
        if person[8] == context.user_data['name']['Contact']:
            nric = person[3]
            break
    if nric is None: nric = "Unknown NRIC"
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
        context.user_data['name']['Rank'], context.user_data['name']['Name'],
        nric,
        context.user_data['name']['PES'],
        context.user_data['date_time'][:6], context.user_data['date_time'][-4:],
        context.user_data['location'],
        context.user_data['description'],
        context.user_data['status'],
        context.user_data['follow_up'],
        context.user_data['nok_informed'],
        reportingPerson['Rank'], reportingPerson['Name'],
        reportingPerson['Contact'][:4], reportingPerson['Contact'][-4:]))
    await update.message.reply_text("Copy and paste the generated IR to WhatsApp")
    if nric == "Unknown NRIC": await update.message.reply_text("NOTE: NRIC of {} {} is unknown".format(context.user_data['name']['Rank'], context.user_data['name']['Name']))
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
        if str(update.effective_user.id) in list(SUPERUSERS.values()): await update.message.reply_text(ALL_COMMANDS)
        else: await update.message.reply_text(NORMAL_USER_COMMANDS)
    else: await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")

async def timeout(update: Update, context: CallbackContext) -> None:
    if str(update.effective_user.id) in list(CHANNEL_IDS.values()):
        await update.message.reply_text("Conversation timed out.")
    else: await update.message.reply_text("You are not authorised to use this telegram bot. Contact Charlie HQ specs for any issues.")

async def error_handler(update, context):
    try:
        raise context.error
    except NetworkError as e:
        print(f"NetworkError occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}", exc_info=True)

def telegram_manager() -> None:

    application = Application.builder().token(TELEGRAM_CHANNEL_BOT_TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("help", helpHandler))
    application.add_handler(CommandHandler("checkmcstatus", checkMcStatusHandler))
    application.add_handler(CommandHandler("checkconduct", checkConductHandler))
    application.add_handler(CommandHandler("updateconducttracking", updateConductHandler))
    application.add_handler(CommandHandler("resettmpdutycmds", resettmpdutycmds))
    application.add_handler(CommandHandler("gethaatrisk", gethaatrisk))
    application.add_handler(CommandHandler("backupcharlienominalroll", backupcharlienominalroll))

    # Add a conversation handler for the new command
    conv_dutygrp_handler = ConversationHandler(
        entry_points=[CommandHandler('updatedutygrp', updateCet)],
        states={
            ASK_CET: [MessageHandler(filters.TEXT & ~filters.COMMAND, updateDutyGrp)],
        },
        fallbacks=[CommandHandler('cancel', cancel_dutygrp)],
    )

    conv_tempmembers_handler = ConversationHandler(
        entry_points=[CommandHandler('addtmpmember', addtmpmember)],
        states={
            ADD_TMP_MEMBER: [MessageHandler(filters.TEXT & ~filters.COMMAND, addmembernames)],
            ADD_TMP_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, addtmpdate)],
            CONSOLIDATE_TMP_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, consolidatetmpdate)]
        },
        fallbacks=[CommandHandler('cancel', cancel_tempmembers)],
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
            ConversationHandler.TIMEOUT: [MessageHandler(filters.TEXT | filters.COMMAND, timeout)],
        },
        fallbacks=[CommandHandler('cancel', cancel_ir)],
        conversation_timeout=60*5, # 5 minutes
        allow_reentry=True)

    # Add the conversation handler
    application.add_handler(conv_dutygrp_handler)
    application.add_handler(conv__IR_handler)
    application.add_handler(conv_tempmembers_handler)
    application.add_handler(MessageHandler(filters.COMMAND, unknownCommand))
    application.add_error_handler(error_handler)
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':

    updateNotes = "Added Y2 PC HQ as super user"
    # send_tele_msg("Welcome to HQ Bot. Strong Alone, Stronger Together.")
    # send_tele_msg(NORMAL_USER_COMMANDS, receiver_id="NORMALUSERS")
    # send_tele_msg(ALL_COMMANDS, receiver_id="SUPERUSERS")
    # send_tele_msg("Send the latest CET using /updatedutygrp to schedule CDS reminder for report sick parade state during FP.", receiver_id="SUPERUSERS")
    # send_tele_msg("*UPDATE NOTES\\:*\n{}".format(updateNotes), parseMode="MarkdownV2")

    response = supabase.table("profiles").select("*").execute()
    response = response.json()
    response = response.replace('rank', 'Rank').replace('name', 'Name').replace('platoon', 'Platoon').replace('section', 'Section').replace('email', 'Email').replace('contact', 'Contact').replace('appointment', 'Appointment').replace('duty_points', 'Duty points').replace('ration', 'Ration').replace('shirt_size', 'Shirt Size').replace('pants_size', 'Pants Size').replace('pes', 'PES')
    data = json.loads(response)
    charlieNominalRoll = data['data']
    allNames = [person['Name'] for person in charlieNominalRoll]
    allContacts = [person['Contact'] for person in charlieNominalRoll]
    
    sheet = None
    for attempt in range(5):
        try: 
            sheet = gc.open("Charlie Nominal Roll")
            break
        except SSLError as e:
            if attempt < 4: time.sleep(5)
            else: raise e
    worksheets = sheet.worksheets()
    googleSheetsNominalRoll = next(ws for ws in worksheets if ws.title == "COMPANY ORBAT")
    allPerson = googleSheetsNominalRoll.get_all_values()
    
    cetQueue = multiprocessing.Queue()
    tmpDutyCmdsQueue = multiprocessing.Queue()
    nominalRollQueue = multiprocessing.Queue()
    haQueue = multiprocessing.Queue()
    googleSheetNominalRollQueue = multiprocessing.Queue()
    googleSheetRequests = multiprocessing.Queue()
    mainCheckMcProcess = multiprocessing.Process(target=main, args=(cetQueue, tmpDutyCmdsQueue, nominalRollQueue, haQueue, googleSheetNominalRollQueue, googleSheetRequests))
    mainCheckMcProcess.start()
    telegram_manager()