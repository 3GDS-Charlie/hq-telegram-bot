import time
import gspread
from datetime import datetime, timedelta
from multiprocessing import Process
from config import SERVICE_ACCOUNT_CREDENTIAL, TELEGRAM_CHANNEL_BOT_TOKEN, CHANNEL_IDS

# PyDrive library has been depracated since 2021
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

import telegram
from telegram import Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes, Updater
import asyncio
import nest_asyncio
nest_asyncio.apply() # patch asyncio

# Telegram Channel
telegram_bot = telegram.Bot(token=TELEGRAM_CHANNEL_BOT_TOKEN)

monthConversion = {"Jan":"01", "Feb":"02", "Mar":"03", "Apr":"04", "May":"05", "Jun":"06", "Jul":"07", "Aug":"08", "Sep":"09", "Oct":"10", "Nov":"11", "Dec":"12"}
trooperRanks = ['PTE', 'PFC', 'LCP', 'CPL', 'CFC']
wospecRanks = ['3SG', '2SG', '1SG', 'SSG', 'MSG', '3WO', '2WO', '1WO', 'MWO', 'SWO', 'CWO']
officerRanks = ['2LT', 'LTA', 'CPT', 'MAJ', 'LTC', 'SLTC', 'COL', 'BG', 'MG', 'LG']

def send_tele_msg(msg):
    for _, value in CHANNEL_IDS.items():
        asyncio.run(send_telegram_bot_msg(msg, value))

async def send_telegram_bot_msg(msg, channel_id):
    await telegram_bot.send_message(chat_id = channel_id, text = msg)

def checkMc():
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
        #TODO: Remove all ranks after standardisation of folder with full names only
        rank = mc[0].split(' ')[0]
        if rank in trooperRanks: folderName = mc[0].replace(rank + " ", "") # remove rank from name if trooper
        else: folderName = mc[0]
        folderList = drive.ListFile({'q': f"title='{folderName}' and trashed=false"}).GetList()
        assert len(folderList) != 0, "No MC folder of the name {} is present".format(folderName)
        assert len(folderList) == 1, "More than one MC folder of the name {} is present".format(folderName)
        folderId = folderList[0]['id']
        driveMcList = drive.ListFile({'q': f"'{folderId}' in parents and trashed=false"}).GetList()
        tmp = mc[1].split(' ')
        tmp[1] = monthConversion[tmp[1]] # convert MMM to MM
        startDate = ''.join(tmp)
        tmp = mc[2].split(' ')
        tmp[1] = monthConversion[tmp[1]] # convert MMM to MM
        endDate = ''.join(tmp)
        foundMcFile = False
        for driveMc in driveMcList:
            tmp = driveMc['createdDate'].split('T')[0].split('-')
            tmp.reverse()
            tmp[2] = tmp[2].replace("20", "")
            if startDate in driveMc['title'] and endDate in driveMc['title']: # found MC file
                foundMcFile = True
                break
            elif "".join(tmp) == startDate: # possible MC file uploaded on same date as start of MC
                possibleMcList.append(mc)

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

def conductTracking():
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
        hqUpdated = False
        p7Updated = False
        p8Updated = False
        p9Updated = False
        for row, colValue in enumerate(colValues, start = 0):
            if len(ajColValues)-1 < row: ajColValues.append("")
            if row<3:continue
            if not hqUpdated and row>=4 and row<=17 and (colValue != 'FALSE' or ajColValues[row] != ""): hqUpdated = True
            elif not p7Updated and row>=18 and row<=42 and (colValue != 'FALSE' or ajColValues[row] != ""): p7Updated = True
            elif not p8Updated and row>=43 and row<=67 and (colValue != 'FALSE' or ajColValues[row] != ""): p8Updated = True
            elif not p9Updated and row>=68 and row<=93 and (colValue != 'FALSE' or ajColValues[row] != ""): p9Updated = True
            if hqUpdated and p7Updated and p8Updated and p9Updated: break
        updatedMsg = "{} {}: ".format(conductDate, conductName)
        if hqUpdated and p7Updated and p8Updated and p9Updated: updatedMsg = "".join([updatedMsg, "All updated"])
        else:
            if not hqUpdated: updatedMsg = "\n".join([updatedMsg, "HQ not updated"])
            if not p7Updated: updatedMsg = "\n".join([updatedMsg, "P7 not updated"])
            if not p8Updated: updatedMsg = "\n".join([updatedMsg, "P8 not updated"])
            if not p9Updated: updatedMsg = "\n".join([updatedMsg, "P9 not updated"])
        send_tele_msg(updatedMsg)

def main():
    while True:
        if datetime.now().hour == 9 and datetime.now().minute == 0:
            send_tele_msg("Checking for MC Lapses...")
            checkMc()
        target_time_today = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0)

        # If the target time is earlier in the day, add a day to the target time
        if datetime.now() >= target_time_today:
            target_time_today += timedelta(days=1)

        time_difference = (target_time_today - datetime.now()).total_seconds()
        time.sleep(time_difference)

async def helpHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Available Commands:\n/checkmc -> Check for MC Lapses\n/checkconduct -> Conduct Tracking Updates\
                                    \n/checkall -> Check everything")

async def checkMcHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Checking for MC Lapses...")
    checkMc()

async def checkConductHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Checking for conduct tracking updates...")
    conductTracking()

async def checkAllHandler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Checking for MC Lapses...")
    checkMc()
    await update.message.reply_text("Checking for conduct tracking updates...")
    conductTracking()

def telegram_manager() -> None:

    application = Application.builder().token(TELEGRAM_CHANNEL_BOT_TOKEN).build()

    #Add handlers
    application.add_handler(CommandHandler("help", helpHandler))
    application.add_handler(CommandHandler("checkmc", checkMcHandler))
    application.add_handler(CommandHandler("checkconduct", checkConductHandler))
    application.add_handler(CommandHandler("checkall", checkAllHandler))

    application.run_polling(allowed_updates=Update.ALL_TYPES, poll_interval=1)

if __name__ == '__main__':
    
    send_tele_msg("Welcome to HQ Bot. Strong alone, stronger together. Send /help for list of available commands.")
    mainCheckMcProcess = Process(target=main)
    mainCheckMcProcess.start()
    telegram_manager()