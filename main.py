import time
import gspread
from datetime import datetime, timedelta
from config import SERVICE_ACCOUNT_CREDENTIAL, TELEGRAM_CHANNEL_BOT_TOKEN, CHANNEL_IDS, DUTY_GRP_ID, CHARLIE_Y2_ID, ID_INSTANCE, TOKEN_INSTANCE, CHARLIE_DUTY_CMDS, PERM_DUTY_CMDS
import traceback
import copy

# PyDrive library has been depracated since 2021
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import telegram
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackContext, ConversationHandler, MessageHandler, filters
import asyncio
import nest_asyncio
nest_asyncio.apply() # patch asyncio
import multiprocessing

import pytesseract
import cv2
import numpy as np
from io import BytesIO
import io
from pdf2image import convert_from_bytes
import pyheif
from PIL import Image
import re

import requests
from whatsapp_api_client_python import API

# Telegram Channel
telegram_bot = telegram.Bot(token=TELEGRAM_CHANNEL_BOT_TOKEN)

monthConversion = {"Jan":"01", "January":"01", "Feb":"02", "February":"02", "Mar":"03", "March":"03", "Apr":"04", "April":"04", "May":"05", "Jun":"06", "June":"06", "Jul":"07", "July":"07", "Aug":"08", "August":"08", "Sep":"09", "September":"09", "Oct":"10", "October":"10", "Nov":"11", "November":"11", "Dec":"12", "December":"12"} 
trooperRanks = ['PTE', 'PFC', 'LCP', 'CPL', 'CFC']
wospecRanks = ['3SG', '2SG', '1SG', 'SSG', 'MSG', '3WO', '2WO', '1WO', 'MWO', 'SWO', 'CWO']
officerRanks = ['2LT', 'LTA', 'CPT', 'MAJ', 'LTC', 'SLTC', 'COL', 'BG', 'MG', 'LG']

ENABLE_WHATSAPP_API = False # Flag to enable live whatsapp manipulation
TELE_ALL_MEMBERS = False # Flag to send tele messages to all listed members

def send_tele_msg(msg):
    if TELE_ALL_MEMBERS:
        for _, value in CHANNEL_IDS.items():
            asyncio.run(send_telegram_bot_msg(msg, value))
    else: 
        for _, value in CHANNEL_IDS.items():
            asyncio.run(send_telegram_bot_msg(msg, value))
            break

async def send_telegram_bot_msg(msg, channel_id):
    try: 
        await telegram_bot.send_message(chat_id = channel_id, text = msg, read_timeout=5)
    except telegram.error.TimedOut:
        print("Request timed out. Retrying...")
        await asyncio.sleep(5)
        await telegram_bot.send_message(chat_id = channel_id, text = msg, read_timeout=5)

def checkMcStatus():

    startTm = time.time()
    try:
        # Get Coy MC/Status list from parade state
        gc = gspread.service_account_from_dict(SERVICE_ACCOUNT_CREDENTIAL)
        sheet = gc.open("3GDS CHARLIE PARADE STATE")
        cCoySheet = sheet.worksheet("C COY")
        platoonMc = cCoySheet.col_values(6) # column F
        sectionMc = cCoySheet.col_values(7) # column G
        sheetMcList = cCoySheet.col_values(9) # column I
        mcStartDates = cCoySheet.col_values(10) # column J
        mcEndDates = cCoySheet.col_values(11) # column K
        mcReason = cCoySheet.col_values(12) # column L
        platoonStatus = cCoySheet.col_values(27) # column AA
        sectionStatus = cCoySheet.col_values(28) # column AB
        sheetStatusList = cCoySheet.col_values(30) # column AD
        statusStartDates = cCoySheet.col_values(31) # column AE
        statusEndDates = cCoySheet.col_values(32) # column AF
        statusReason = cCoySheet.col_values(33) # column AG
        assert len(sheetMcList) == len(mcStartDates) == len(mcEndDates), "Num of names and MC dates do not tally"
        assert len(sheetStatusList) == len(statusStartDates) == len(statusEndDates), "Num of names and status dates do not tally"
        foundHeader = False
        mcList = []
        for index, name in enumerate(sheetMcList, start = 0):
            if not foundHeader and name == 'NAME': 
                foundHeader = True
                continue
            if foundHeader: mcList.append((name, mcStartDates[index], mcEndDates[index], platoonMc[index], sectionMc[index], "MC", mcReason[index]))

        foundHeader = False
        statusList = []
        for index, name in enumerate(sheetStatusList, start = 0):
            if not foundHeader and name == 'NAME': 
                foundHeader = True
                continue
            if foundHeader: statusList.append((name, statusStartDates[index], statusEndDates[index], platoonStatus[index], sectionStatus[index], "Status", statusReason[index]))
        
        paradeStateMcList = copy.deepcopy(mcList)
        paradeStateMasterList = copy.deepcopy(statusList)
        paradeStateMasterList.extend(paradeStateMcList)

        # read existing MC/Status entries from mc lapse sheet
        mcStatusLapseSheet = gc.open("MC/Status Lapse Tracking")
        mcLapse = mcStatusLapseSheet.worksheet("MC")
        sheetMcList = mcLapse.col_values(1) # column A
        mcStartDates = mcLapse.col_values(2) # column B
        mcEndDates = mcLapse.col_values(3) # column C
        platoon = mcLapse.col_values(4) # column D
        section = mcLapse.col_values(5) # # column E
        mcReason = mcLapse.col_values(6) # # column F
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
        sheetStatusList = statusLapse.col_values(1) # column A
        statusStartDates = statusLapse.col_values(2) # column B
        statusEndDates = statusLapse.col_values(3) # column C
        platoon = statusLapse.col_values(4) # column D
        section = statusLapse.col_values(5) # # column E
        statusReason = statusLapse.col_values(6) # # column F
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
        sheetMcStatusList = mcStatusChecked.col_values(1) # column A
        mcStatusStartDates = mcStatusChecked.col_values(2) # column B
        mcStatusEndDates = mcStatusChecked.col_values(3) # column C
        platoon = mcStatusChecked.col_values(4) # column D
        section = mcStatusChecked.col_values(5) # # column E
        type = mcStatusChecked.col_values(6) # # column F
        mcStatusReason = mcStatusChecked.col_values(7) # # column G
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
        firstMsg = False
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
            tmp[1] = monthConversion[tmp[1]] # convert MMM to MM
            startDate = ''.join(tmp)
            startDateTime = datetime.strptime(startDate, "%d%m%y").date()
            if mcStatus[2] != '-': # no end date/permanent
                tmp = mcStatus[2].split(' ')
                tmp[1] = monthConversion[tmp[1]] # convert MMM to MM
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
                    # print(allDates)
                    # cv2.imshow("MC/STATUS", img)
                    # cv2.waitKey(0)
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
                if not firstMsg: 
                    send_tele_msg("This is taking longer than expected\nCurrent progress: {:.1f}%".format((count/len(masterList))*100))
                    firstMsg = True
                else: send_tele_msg("Current progress: {:.1f}%".format((count/len(masterList))*100))
                startTm = time.time() 

        # write lapsed mc/status list to mc/status lapse tracking sheet
        mcLapse.batch_clear(['A2:G1000'])
        statusLapse.batch_clear(['A2:G1000'])
        if len(lapseMcList) == 0: send_tele_msg("No MC lapses")
        else:
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
            send_tele_msg(tele_msg)
        
        if len(lapseStatusList) == 0: send_tele_msg("No Status lapses")
        else:
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
            send_tele_msg(tele_msg)
    
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

def conductTracking():

    try:
        gc = gspread.service_account_from_dict(SERVICE_ACCOUNT_CREDENTIAL)
        sheet = gc.open("Charlie Conduct Tracking")
        conductTrackingSheet = sheet.worksheet("CONDUCT TRACKING")
        allDates = conductTrackingSheet.row_values(2)
        currentDate = "{}{}{}".format(str(datetime.now().day), (("0" + str(datetime.now().month)) if datetime.now().month < 10 else (str(datetime.now().month))), str(datetime.now().year).replace("20", ""))
        colIndexes = []
        foundIndexes = False
        for index, date in enumerate(allDates, start = 1):
            if date == currentDate: 
                colIndexes.append(index)
                foundIndexes = True
            elif foundIndexes and date != currentDate and date != "": break
        if len(colIndexes) == 0: send_tele_msg("No conducts today")

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
            send_tele_msg(updatedMsg)
        
    except Exception as e:
        print("Encountered exception:\n{}".format(traceback.format_exc()))
        send_tele_msg("Encountered exception:\n{}".format(traceback.format_exc()))

def updateWhatsappGrp(cet):
    
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
        for segment in cetSegments:
            if "Duty Personnel" in segment: newDate = segment.split('[')[1].split('/')[0].replace(" ", "")
            if 'FP' in segment: fpTime = segment.split(" -")[0]
            if 'CDS' in segment: CDS = segment.split(': ')[-1].replace(" ", "")
            elif 'PDS7' in segment: PDS7 = segment.split(': ')[-1].replace(" ", "")
            elif 'PDS8' in segment: PDS8 = segment.split(': ')[-1].replace(" ", "")
            elif 'PDS9' in segment: PDS9 = segment.split(': ')[-1].replace(" ", "")
            if fpTime is not None: cetQueue.put((newDate, fpTime))
        if (CDS is None and PDS7 is None and PDS8 is None and PDS9 is None) or newDate is None: raise Exception
    except Exception as e: 
        send_tele_msg("Unrecognized CET")
        return
    
    # Renaming of group name
    if ENABLE_WHATSAPP_API: greenAPI.groups.updateGroupName(dutyGrpId, "{} DUTY CDS/PDS".format(newDate))
    
    # if ENABLE_WHATSAPP_API: response = greenAPI.sending.sendMessage(dutyGrpId, "Updating duty group. This is an automated message.")
    
    # Removal of previous duty members not in next duty 
    url = "https://api.green-api.com/waInstance{}/getGroupData/{}".format(ID_INSTANCE, TOKEN_INSTANCE)
    payload = {
        "groupId": dutyGrpId  
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200: group_data = response.json()
    else: 
        send_tele_msg("Failed to retrieve group data: {}\nAborting updating duty group.".format(response.json()))
        group_data = None
        return
    if group_data is not None: 
        nextDutyCmds = []
        nextDutyCmds.append(CHARLIE_DUTY_CMDS[CDS])
        nextDutyCmds.append(CHARLIE_DUTY_CMDS[PDS7])
        nextDutyCmds.append(CHARLIE_DUTY_CMDS[PDS8])
        nextDutyCmds.append(CHARLIE_DUTY_CMDS[PDS9])
        allMembers = group_data['participants']
        for member in allMembers:
            memberId = member['id'].split('@c.us')[0][2:]
            if memberId not in list(PERM_DUTY_CMDS.values()) and memberId not in nextDutyCmds: 
                if ENABLE_WHATSAPP_API: greenAPI.groups.removeGroupParticipant(dutyGrpId, member['id']) 

    # Adding new duty members
    if CDS not in CHARLIE_DUTY_CMDS: send_tele_msg("CDS {} not found".format(CDS))
    else: 
        if ENABLE_WHATSAPP_API: greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(CHARLIE_DUTY_CMDS[CDS]))
    if PDS7 not in CHARLIE_DUTY_CMDS: send_tele_msg("PDS7 {} not found".format(PDS7))
    else: 
        if ENABLE_WHATSAPP_API: greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(CHARLIE_DUTY_CMDS[PDS7]))
    if PDS8 not in CHARLIE_DUTY_CMDS: send_tele_msg("PDS8 {} not found".format(PDS8))
    else: 
        if ENABLE_WHATSAPP_API: greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(CHARLIE_DUTY_CMDS[PDS8]))
    if PDS9 not in CHARLIE_DUTY_CMDS: send_tele_msg("PDS9 {} not found".format(PDS9))
    else: 
        if ENABLE_WHATSAPP_API: greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(CHARLIE_DUTY_CMDS[PDS9]))

    # Sending new CET
    if ENABLE_WHATSAPP_API: response = greenAPI.sending.sendMessage(dutyGrpId, cet)

    # Checking whether all members were added successfully
    url = "https://api.green-api.com/waInstance{}/getGroupData/{}".format(ID_INSTANCE, TOKEN_INSTANCE)
    payload = {
        "groupId": dutyGrpId  
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200: group_data = response.json()
    else: 
        send_tele_msg("Unable to check whether all members were added successfully: {}.".format(response.json()))
        group_data = None
    if group_data is not None: 
        allMembers = group_data['participants']
        allMemberNumbers = []
        for member in allMembers: allMemberNumbers.append(member['id'].split('@c.us')[0][2:])
        for memberId in nextDutyCmds:
            if memberId not in allMemberNumbers:
                for name, number in CHARLIE_DUTY_CMDS.items():
                    if memberId == number: 
                        send_tele_msg("{} was not added succesfully".format(name.replace("3SG", "")))
                        break
    send_tele_msg("Updated duty group")

def main(cetQ):

    charlieY2Id = CHARLIE_Y2_ID
    greenAPI = API.GreenAPI(ID_INSTANCE, TOKEN_INSTANCE)
    fpDateTime = None
    sentCdsReminder = False
    weekDay = [1, 2, 3, 4, 5]

    while True:
        # Auto updating of MC Lapses everyday at 0900
        if datetime.now().hour == 9 and datetime.now().minute == 0:
            send_tele_msg("Checking for MC Lapses...")
            checkMcStatus()
        # target_time_today = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)

        # If the target time is earlier in the day, add a day to the target time
        # if datetime.now() >= target_time_today:
        #     target_time_today += timedelta(days=1)

        # time_difference = (target_time_today - datetime.now()).total_seconds()
        
        # Auto reminding of CDS to send report sick parade state every morning 
        # Default time 0530 if no CET received otherwise during FP of CET

        while not cetQ.empty(): 
            sentCdsReminder = False
            fpDateTime = cetQ.get()
            # got latest CET
            # check whether date and time is correct
            if cetQ.empty(): 
                if datetime.strptime(fpDateTime[0], "%d%m%y") > datetime.now(): send_tele_msg("CDS reminder for report sick parade state scheduled at {} {}".format(fpDateTime[0], fpDateTime[1]))
                else: send_tele_msg("Invalid CET date. CDS reminder for report sick parade state scheduled at 0530")

        # there was a sent CET since the start of the bot
        if fpDateTime is not None:
            # send reminder during weekdays when it hits the FP date and time of sent CET
            if datetime.now().isoweekday() in weekDay and datetime.now().day == int(fpDateTime[0][:2]) and datetime.now().hour == int(fpDateTime[1][:2]) and datetime.now().minute == int(fpDateTime[1][-2:]) and not sentCdsReminder:
                send_tele_msg("Sending automated CDS reminder")
                if ENABLE_WHATSAPP_API: response = greenAPI.sending.sendMessage(charlieY2Id, "This is an automated daily reminder for the CDS to send the REPORT SICK PARADE STATE")
                sentCdsReminder = True
            else:
                # if it is 0530 and the latest sent CET is still not current, send reminder
                if datetime.now().isoweekday() in weekDay and datetime.now().day != int(fpDateTime[0][:2]) and datetime.now().hour == 5 and datetime.now().minute == 30 and not sentCdsReminder:
                    send_tele_msg("Sending automated CDS reminder")
                    if ENABLE_WHATSAPP_API: response = greenAPI.sending.sendMessage(charlieY2Id, "This is an automated daily reminder for the CDS to send the REPORT SICK PARADE STATE")
                    sentCdsReminder = True
        else: 
            # no sent CET since the start of the bot
            # send reminder during weekdays at default timing of 0530
            if datetime.now().isoweekday() in weekDay and datetime.now().hour == 5 and datetime.now().minute == 30 and not sentCdsReminder:
                send_tele_msg("Sending automated CDS reminder")
                if ENABLE_WHATSAPP_API: response = greenAPI.sending.sendMessage(charlieY2Id, "This is an automated daily reminder for the CDS to send the REPORT SICK PARADE STATE")
                sentCdsReminder = True

        time.sleep(5)

async def helpHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Available Commands:\n/checkmcstatus -> Check for MC/Status Lapses\n/checkconduct -> Conduct Tracking Updates\
                                    \n/checkall -> Check everything\n/updatedutygrp -> Update duty group according to CET")

async def checkMcStatusHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Checking for MC and Status Lapses...")
    checkMcStatus()

async def checkConductHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Checking for conduct tracking updates...")
    conductTracking()

async def checkAllHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Checking for MC and Status Lapses...")
    checkMcStatus()
    await update.message.reply_text("Checking for conduct tracking updates...")
    conductTracking()

ASK_CET = 1

async def updateCet(update: Update, context: CallbackContext) -> int:
    await update.message.reply_text("Send the new CET")
    return ASK_CET

async def updateDutyGrp(update: Update, context: CallbackContext) -> int:
    cet = update.message.text
    updateWhatsappGrp(cet)
    return ConversationHandler.END

async def cancel(update: Update, context: CallbackContext) -> int:
    await update.message.reply_text('Updating cancelled')
    return ConversationHandler.END

async def unknownCommand(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text("Unrecognised command.")
    await update.message.reply_text("Available Commands:\n/checkmcstatus -> Check for MC/Status Lapses\n/checkconduct -> Conduct Tracking Updates\
                                    \n/checkall -> Check everything\n/updatedutygrp -> Update duty group according to CET")

def telegram_manager() -> None:

    application = Application.builder().token(TELEGRAM_CHANNEL_BOT_TOKEN).build()

    #Add handlers
    application.add_handler(CommandHandler("help", helpHandler))
    application.add_handler(CommandHandler("checkmcstatus", checkMcStatusHandler))
    application.add_handler(CommandHandler("checkconduct", checkConductHandler))
    application.add_handler(CommandHandler("checkall", checkAllHandler))

    # Add a conversation handler for the new command
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('updatedutygrp', updateCet)],
        states={
            ASK_CET: [MessageHandler(filters.TEXT & ~filters.COMMAND, updateDutyGrp)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    # Add the conversation handler
    application.add_handler(conv_handler)
    application.add_handler(MessageHandler(filters.COMMAND, unknownCommand))
    application.run_polling(allowed_updates=Update.ALL_TYPES, poll_interval=1)

if __name__ == '__main__':

    send_tele_msg("Welcome to HQ Bot. Strong alone, stronger together. Send /help for list of available commands.")
    send_tele_msg("CDS reminder for report sick parade state scheduled at 0530. Send the latest CET using /updatedutygrp to schedule during FP")
    cetQueue = multiprocessing.Queue()
    mainCheckMcProcess = multiprocessing.Process(target=main, args=(cetQueue,))
    mainCheckMcProcess.start()
    telegram_manager()