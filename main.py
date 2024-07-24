import time
import gspread
from datetime import datetime, timedelta
from config import SERVICE_ACCOUNT_CREDENTIAL, TELEGRAM_CHANNEL_BOT_TOKEN, CHANNEL_IDS
import traceback

# PyDrive library has been depracated since 2021
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import telegram
from telegram import Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes, Updater, CallbackContext, ConversationHandler, MessageHandler, filters
import asyncio
import nest_asyncio
nest_asyncio.apply() # patch asyncio
from multiprocessing import Process

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

monthConversion = {"Jan":"01", "Feb":"02", "Mar":"03", "Apr":"04", "May":"05", "Jun":"06", "Jul":"07", "Aug":"08", "Sep":"09", "Oct":"10", "Nov":"11", "Dec":"12"}
trooperRanks = ['PTE', 'PFC', 'LCP', 'CPL', 'CFC']
wospecRanks = ['3SG', '2SG', '1SG', 'SSG', 'MSG', '3WO', '2WO', '1WO', 'MWO', 'SWO', 'CWO']
officerRanks = ['2LT', 'LTA', 'CPT', 'MAJ', 'LTC', 'SLTC', 'COL', 'BG', 'MG', 'LG']
charlieDutyCmds = {"3SGZEYEUNG":"87157835", "3SGLIANGDING":"90282045", "3SGELLIOT":"88110850", "3SGJAVEEN":"82316394", "3SGZACH":"98107933", "3SGILLIYAS":"92300624", "3SGDAMIEN":"98999958", "3SGJOASH":"94787064", "3SGJOEL":"93672953", "3SGJOSEPH":"87785701", "3SGMAD":"98250556", "3SGPATRICK":"83740026", "3SGSHENGJUN":"84096282", "3SGAFIF":"91867127", "3SGIRFAN":"97218155", "3SGVIKNESH":"87862607", "3SGKERWIN":"97734298", "3SGNAWFAL":"84282446", "3SGSKY":"88877846", "3SGSRIRAM":"87002363"}
# 4 PS + 4 PC + 2 HQ Spec
permDutyCmds = {"3SGZEYEUNG":"87157835", "3SGLIANGDING":"90282045", "3SGKEILOK":"91361826", "3SGGREGORY":"84208408", "3SGKAILE":"90882585", "3SGRONGJIN":"97289218", "ETHANCHAN":"90030559", "JEREMIAH":"87207881", "DAEMON":"91553385", "MAX":"93696236"}

def send_tele_msg(msg):
    for _, value in CHANNEL_IDS.items():
        asyncio.run(send_telegram_bot_msg(msg, value))

async def send_telegram_bot_msg(msg, channel_id):
    await telegram_bot.send_message(chat_id = channel_id, text = msg)      

def checkMcStatus():

    try:
        # Get Coy MC list from parade state
        gc = gspread.service_account_from_dict(SERVICE_ACCOUNT_CREDENTIAL)
        sheet = gc.open("3GDS CHARLIE PARADE STATE")
        cCoySheet = sheet.worksheet("C COY")
        Platoons = cCoySheet.col_values(6) # column F
        Sections = cCoySheet.col_values(7) # column G
        SheetMcList = cCoySheet.col_values(9) # column I
        McStartDates = cCoySheet.col_values(10) # column J
        McEndDates = cCoySheet.col_values(11) # column K
        assert len(SheetMcList) == len(McStartDates) == len(McEndDates), "Num of names and MC dates do not tally"
        foundHeader = False
        McList = []
        for index, name in enumerate(SheetMcList, start = 0):
            if not foundHeader and name == 'NAME': 
                foundHeader = True
                continue
            if foundHeader: McList.append((name, McStartDates[index], McEndDates[index], Platoons[index], Sections[index]))

        # read existing MC entries from mc lapse sheet
        mcLapseSheet = gc.open("MC Lapse Tracking")
        mcLapse = mcLapseSheet.worksheet("Sheet1")
        SheetMcList = mcLapse.col_values(1) # column A
        McStartDates = mcLapse.col_values(2) # column B
        McEndDates = mcLapse.col_values(3) # column C
        Platoons = mcLapse.col_values(4) # column D
        Sections = mcLapse.col_values(5) # # column E
        assert len(SheetMcList) == len(McStartDates) == len(McEndDates), "Num of names and MC dates do not tally"
        foundHeader = False
        existingMcList = []
        for index, name in enumerate(SheetMcList, start = 0):
            if not foundHeader and name == 'NAME': 
                foundHeader = True
                continue
            if foundHeader: existingMcList.append((name, McStartDates[index], McEndDates[index], Platoons[index], Sections[index]))
        McList.extend(existingMcList)
        McList = list(set(McList)) # remove duplicate entries

        # get MC files in google drive
        gauth = GoogleAuth()
        creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_CREDENTIAL, ['https://www.googleapis.com/auth/drive'])
        service = build('drive', 'v3', credentials=creds)
        gauth.credentials = creds
        drive = GoogleDrive(gauth)

        lapseMcList = []
        possibleMcList = []
        for mc in McList:
            rank = mc[0].split(' ')[0]
            folderName = mc[0].replace(rank + " ", "") # remove rank from name
            folderList = drive.ListFile({'q': f"title='{folderName}' and trashed=false"}).GetList()
            assert len(folderList) != 0, "No MC folder of the name {} is present".format(folderName)
            assert len(folderList) == 1, "More than one MC folder of the name {} is present".format(folderName)
            folderId = folderList[0]['id']
            driveMcList = drive.ListFile({'q': f"'{folderId}' in parents and trashed=false"}).GetList()
            tmp = mc[1].split(' ')
            tmp[1] = monthConversion[tmp[1]] # convert MMM to MM
            startDate = ''.join(tmp)
            startDateTime = datetime.strptime(startDate, "%d%m%y").date()
            tmp = mc[2].split(' ')
            tmp[1] = monthConversion[tmp[1]] # convert MMM to MM
            endDate = ''.join(tmp)
            foundMcFile = False
            for driveMc in driveMcList:
                tmp = driveMc['createdDate'].split('T')[0].split('-')
                tmp.reverse()
                tmp[2] = tmp[2].replace("20", "")
                uploadDate = "".join(tmp)
                uploadDateTime = datetime.strptime(uploadDate, "%d%m%y").date()
                if startDate in driveMc['title'] and endDate in driveMc['title']: # found MC file
                    foundMcFile = True
                    break
                elif uploadDateTime >= startDateTime: # possible MC file with upload date later than start of MC date
                    request = service.files().get_media(fileId=driveMc['id'])
                    imageIo = io.BytesIO()
                    downloader = MediaIoBaseDownload(imageIo, request)
                    done = False
                    while done is False:
                        status, done = downloader.next_chunk()
                    imageIo.seek(0)
                    if driveMc['fileExtension'].upper() == 'PDF': # PDF Formats
                        images = convert_from_bytes(imageIo.read())
                        pil_image = images[0]  # Convert the first page only
                        img = cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGB2BGR)
                    elif driveMc['fileExtension'].upper() == 'HEIC': # HEIC Formats
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
                    pattern1 = r"\b(\d{1,2}/\d{1,2}/\d{4})\b"
                    pattern2 = r"\b(\d{1,2}-[A-Za-z]{3}-\d{4})\b"
                    dates_format1 = re.findall(pattern1, imageText)
                    dates_format2 = re.findall(pattern2, imageText)
                    allDates = []
                    for date in dates_format1:
                        allDates.append(date.replace("/", ""))
                    for date in dates_format2:
                        tmp = date.split('-')
                        tmp[1] = monthConversion[tmp[1]]
                        tmp[2] = tmp[2].replace("20", "")
                        allDates.append("".join(tmp))
                    if startDate in allDates and endDate in allDates: 
                        foundMcFile = True
                        break
                    else: possibleMcList.append(mc)

            if not foundMcFile: lapseMcList.append(mc) 

        # write lapsed mc list to mc lapse tracking sheet
        mcLapse.batch_clear(["A2:C1000"])
        if len(lapseMcList) == 0: send_tele_msg("No lapses")
        else:
            tele_msg = "Lapsed MC List:"
            for index, mc in enumerate(lapseMcList, start = 2):
                mcLapse.update_acell('A{}'.format(index), mc[0]) 
                mcLapse.update_acell('B{}'.format(index), mc[1]) 
                mcLapse.update_acell('C{}'.format(index), mc[2])
                mcLapse.update_acell('D{}'.format(index), mc[3]) 
                mcLapse.update_acell('E{}'.format(index), mc[4])
                if mc in possibleMcList: tele_msg = "\n".join([tele_msg, "{}".format(mc[0]) + ((" (P{}S{})".format(mc[3], mc[4])) if mc[3] != "HQ" else (" (HQ)")), "{} - {} (Possible MC found)\n".format(mc[1], mc[2])])
                else: tele_msg = "\n".join([tele_msg, "{}".format(mc[0]) + ((" (P{}S{})".format(mc[3], mc[4])) if mc[3] != "HQ" else (" (HQ)")), "{} - {}\n".format(mc[1], mc[2])])
            send_tele_msg(tele_msg)
    
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
    
    dutyGrpId = "120363314173996674@g.us"
    greenAPI = API.GreenAPI("7103960874", "e226a9f2550045dfb03624e95950d4448ff72a4a96774f7cbf")

    # Getting duty commanders and date from CET
    try: 
        cetSegments = cet.split('\n')
        CDS = None
        PDS7 = None
        PDS8 = None
        PDS9 = None
        newDate = None
        for segment in cetSegments:
            if "Duty Personnel" in segment: newDate = segment.split('[')[1].split('/')[0].replace(" ", "")
            if 'CDS' in segment: CDS = segment.split(': ')[-1].replace(" ", "")
            elif 'PDS7' in segment: PDS7 = segment.split(': ')[-1].replace(" ", "")
            elif 'PDS8' in segment: PDS8 = segment.split(': ')[-1].replace(" ", "")
            elif 'PDS9' in segment: PDS9 = segment.split(': ')[-1].replace(" ", "")
        if (CDS is None and PDS7 is None and PDS8 is None and PDS9 is None) or newDate is None: raise Exception
    except Exception as e: 
        send_tele_msg("Unrecognized CET")
        return
    
    # Renaming of group name
    greenAPI.groups.updateGroupName(dutyGrpId, "{} DUTY CDS/PDS".format(newDate))
    
    response = greenAPI.sending.sendMessage(dutyGrpId, "Updating duty group. This is an automated message.")
    
    # Removal of previous duty members not in next duty 
    url = "https://api.green-api.com/waInstance7103960874/getGroupData/e226a9f2550045dfb03624e95950d4448ff72a4a96774f7cbf"
    payload = {
        "groupId": dutyGrpId  
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200: group_data = response.json()
    else: 
        send_tele_msg("Failed to retrieve group data: {}\nAborting updating duty group.".format(response.json()))
        group_data = None
    if group_data is not None: 
        nextDutyCmds = []
        nextDutyCmds.append(charlieDutyCmds[CDS])
        nextDutyCmds.append(charlieDutyCmds[PDS7])
        nextDutyCmds.append(charlieDutyCmds[PDS8])
        nextDutyCmds.append(charlieDutyCmds[PDS9])
        allMembers = group_data['participants']
        for member in allMembers:
            memberId = member['id'].split('@c.us')[0][2:]
            if memberId not in list(permDutyCmds.values()) and memberId not in nextDutyCmds: 
                greenAPI.groups.removeGroupParticipant(dutyGrpId, member['id']) 

    # Adding new duty members
    if CDS not in charlieDutyCmds: send_tele_msg("CDS {} not found".format(CDS))
    else: greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(charlieDutyCmds[CDS]))
    if PDS7 not in charlieDutyCmds: send_tele_msg("PDS7 {} not found".format(PDS7))
    else: greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(charlieDutyCmds[PDS7]))
    if PDS8 not in charlieDutyCmds: send_tele_msg("PDS8 {} not found".format(PDS8))
    else: greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(charlieDutyCmds[PDS8]))
    if PDS9 not in charlieDutyCmds: send_tele_msg("PDS9 {} not found".format(PDS9))
    else: greenAPI.groups.addGroupParticipant(dutyGrpId, "65{}@c.us".format(charlieDutyCmds[PDS9]))

    # Sending new CET
    response = greenAPI.sending.sendMessage(dutyGrpId, cet)

def main():
    while True:
        if datetime.now().hour == 9 and datetime.now().minute == 0:
            send_tele_msg("Checking for MC Lapses...")
            checkMcStatus()
        target_time_today = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)

        # If the target time is earlier in the day, add a day to the target time
        if datetime.now() >= target_time_today:
            target_time_today += timedelta(days=1)

        time_difference = (target_time_today - datetime.now()).total_seconds()
        time.sleep(time_difference)

async def helpHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Available Commands:\n/checkmcstatus -> Check for MC/Status Lapses\n/checkconduct -> Conduct Tracking Updates\
                                    \n/checkall -> Check everything\n/updatedutygrp -> Update duty group according to CET")

async def checkMcStatusHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Checking for MC/Status Lapses...")
    checkMcStatus()

async def checkConductHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Checking for conduct tracking updates...")
    conductTracking()

async def checkAllHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Checking for MC/Status Lapses...")
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
    mainCheckMcProcess = Process(target=main)
    mainCheckMcProcess.start()
    telegram_manager()