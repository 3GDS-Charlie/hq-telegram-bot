import os
import json

from dotenv import load_dotenv, dotenv_values

load_dotenv()

# For Google related APIs
SERVICE_ACCOUNT_CREDENTIAL = {
    "type": os.getenv("SERVICE_ACCOUNT_CREDENTIAL_TYPE"),
    "project_id": os.getenv("SERVICE_ACCOUNT_CREDENTIAL_PROJECT_ID"),
    "private_key_id": os.getenv("SERVICE_ACCOUNT_CREDENTIAL_PRIVATE_KEY_ID"),
    "private_key": os.getenv("SERVICE_ACCOUNT_CREDENTIAL_PRIVATE_KEY").replace('\\n', '\n'),
    "client_email": os.getenv("SERVICE_ACCOUNT_CREDENTIAL_CLIENT_EMAIL"),
    "client_id": os.getenv("SERVICE_ACCOUNT_CREDENTIAL_CLIENT_ID"),
    "auth_uri": os.getenv("SERVICE_ACCOUNT_CREDENTIAL_AUTH_URI"),
    "token_uri": os.getenv("SERVICE_ACCOUNT_CREDENTIAL_TOKEN_URI"),
    "auth_provider_x509_cert_url": os.getenv("SERVICE_ACCOUNT_CREDENTIAL_AUTH_PROVIDER_X509_CERT_URL"),
    "client_x509_cert_url": os.getenv("SERVICE_ACCOUNT_CREDENTIAL_CLIENT_X509_CERT_URL"),
    "universe_domain": os.getenv("SERVICE_ACCOUNT_CREDENTIAL_UNIVERSE_DOMAIN"),
}

# For extracting training programme
TIMETREE_USERNAME=os.getenv("TIMETREEUSER")
TIMETREE_PASSWORD=os.getenv("TIMETREEPWD")
TIMETREE_CALENDAR_ID=os.getenv("CALENDARID")

TELEGRAM_CHANNEL_BOT_TOKEN=os.getenv("TELEGRAM_CHANNEL_BOT_TOKEN")

# SUPERUSERS should be a subset of CHANNEL_IDS
SUPERUSERS = {
    "ZE_YEUNG": os.getenv("TELEGRAM_CHANNEL_ID_ZE_YEUNG"),
    "KEI_LOK": os.getenv("TELEGRAM_CHANNEL_ID_KEI_LOK"),
    "LIANG_DING": os.getenv("TELEGRAM_CHANNEL_ID_LIANG_DING")
}

CHANNEL_IDS = {
    "ZE_YEUNG": os.getenv("TELEGRAM_CHANNEL_ID_ZE_YEUNG"),
    "KEI_LOK": os.getenv("TELEGRAM_CHANNEL_ID_KEI_LOK"),
    "LIANG_DING": os.getenv("TELEGRAM_CHANNEL_ID_LIANG_DING"),
    "PATRICK": os.getenv("TELEGRAM_CHANNEL_ID_PATRICK")
}

# WhatsApp related APIs
DUTY_GRP_ID = os.getenv("DUTY_GROUP_ID")
CHARLIE_Y2_ID = os.getenv("CHARLIE_Y2_GROUP_ID")
WHATSAPP_ID_INSTANCE = os.getenv("API_ID_INSTANCE")
WHATSAPP_TOKEN_INSTANCE = os.getenv("API_TOKEN_INSTANCE")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPBASE_BACKUP_DRIVE_ID = os.getenv("SUPBASE_BACKUP_DRIVE_ID")

CHARLIE_DUTY_CMDS = {"ZEYEUNG":os.getenv("ZEYEUNG_NUMBER"), 
                      "LIANGDING":os.getenv("LIANGDING_NUMBER"), 
                      "ELLIOT":os.getenv("ELLIOT_NUMBER"), 
                      "JAVEEN":os.getenv("JAVEEN_NUMBER"), 
                      "ZACH":os.getenv("ZACH_NUMBER"), 
                      "ILLIYAS":os.getenv("ILLIYAS_NUMBER"),
                      "ILLYAS":os.getenv("ILLIYAS_NUMBER"), 
                      "DAMIEN":os.getenv("DAMIEN_NUMBER"), 
                      "JOASH":os.getenv("JOASH_NUMBER"), 
                      "JOEL":os.getenv("JOEL_NUMBER"), 
                      "JOSEPH":os.getenv("JOSEPH_NUMBER"), 
                      "MUHAMMAD":os.getenv("MAD_NUMBER"),
                      "MAD":os.getenv("MAD_NUMBER"), 
                      "PATRICK":os.getenv("PATRICK_NUMBER"), 
                      "SHENGJUN":os.getenv("SHENGJUN_NUMBER"), 
                      "AFIF":os.getenv("AFIF_NUMBER"), 
                      "IRFAN":os.getenv("IRFAN_NUMBER"), 
                      "VIKNES":os.getenv("VIKNESH_NUMBER"),
                      "VIKNESWARAN":os.getenv("VIKNESH_NUMBER"),
                      "VIKNESH":os.getenv("VIKNESH_NUMBER"),
                      "VIKNESWARAN":os.getenv("VIKNESH_NUMBER"),
                      "KERWIN":os.getenv("KERWIN_NUMBER"), 
                      "NAWFAL":os.getenv("NAWFAL_NUMBER"), 
                      "SKY":os.getenv("SKY_NUMBER"), 
                      "SRIRAM":os.getenv("SRIRAM_NUMBER")
}

# 4 PS + PC + 2 HQ Spec
PERM_DUTY_CMDS = {"ZEYEUNG":os.getenv("ZEYEUNG_NUMBER"), 
                  "LIANGDING":os.getenv("LIANGDING_NUMBER"), 
                  "KEILOK":os.getenv("KEILOK_NUMBER"), 
                  "GREGORY":os.getenv("GREGORY_NUMBER"), 
                  "KAILE":os.getenv("KAILE_NUMBER"), 
                  "RONGJIN":os.getenv("RONGJIN_NUMBER"), 
                  "ETHANCHAN":os.getenv("ETHANCHAN_NUMBER"), 
                  "JEREMIAH":os.getenv("JEREMIAH_NUMBER"), 
                  "DAEMON":os.getenv("DAEMON_NUMBER"), 
                  "MAX":os.getenv("MAX_NUMBER"),
                  "AJLOY":os.getenv("AJLOY_NUMBER"),
                  "GAOSHAN":os.getenv("GAOSHAN_NUMBER")
}