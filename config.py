import os
import json

from dotenv import load_dotenv, dotenv_values

load_dotenv()

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

TELEGRAM_CHANNEL_BOT_TOKEN=os.getenv("TELEGRAM_CHANNEL_BOT_TOKEN")

CHANNEL_IDS = {
    "ZE_YEUNG": os.getenv("TELEGRAM_CHANNEL_ID_ZE_YEUNG"),
    "KEI_LOK": os.getenv("TELEGRAM_CHANNEL_ID_KEI_LOK"),
    "LIANG_DING": os.getenv("TELEGRAM_CHANNEL_ID_LIANG_DING"),
}

DUTY_GRP_ID = os.getenv("DUTY_GROUP_ID")
CHARLIE_Y2_ID = os.getenv("CHARLIE_Y2_GROUP_ID")
ID_INSTANCE = os.getenv("API_ID_INSTANCE")
TOKEN_INSTANCE = os.getenv("API_TOKEN_INSTANCE")

CHARLIE_DUTY_CMDS = {"3SGZEYEUNG":os.getenv("ZEYEUNG_NUMBER"), 
                      "3SGLIANGDING":os.getenv("LIANGDING_NUMBER"), 
                      "3SGELLIOT":os.getenv("ELLIOT_NUMBER"), 
                      "3SGJAVEEN":os.getenv("JAVEEN_NUMBER"), 
                      "3SGZACH":os.getenv("ZACH_NUMBER"), 
                      "3SGILLIYAS":os.getenv("ILLIYAS_NUMBER"),
                      "3SGILLYAS":os.getenv("ILLIYAS_NUMBER"), 
                      "3SGDAMIEN":os.getenv("DAMIEN_NUMBER"), 
                      "3SGJOASH":os.getenv("JOASH_NUMBER"), 
                      "3SGJOEL":os.getenv("JOEL_NUMBER"), 
                      "3SGJOSEPH":os.getenv("JOSEPH_NUMBER"), 
                      "3SGMUHAMMAD":os.getenv("MAD_NUMBER"),
                      "3SGMAD":os.getenv("MAD_NUMBER"), 
                      "3SGPATRICK":os.getenv("PATRICK_NUMBER"), 
                      "3SGSHENGJUN":os.getenv("SHENGJUN_NUMBER"), 
                      "3SGAFIF":os.getenv("AFIF_NUMBER"), 
                      "3SGIRFAN":os.getenv("IRFAN_NUMBER"), 
                      "3SGVIKNESH":os.getenv("VIKNESH_NUMBER"),
                      "3SGVIKNESWARAN":os.getenv("VIKNESH_NUMBER"),
                      "3SGKERWIN":os.getenv("KERWIN_NUMBER"), 
                      "3SGNAWFAL":os.getenv("NAWFAL_NUMBER"), 
                      "3SGSKY":os.getenv("SKY_NUMBER"), 
                      "3SGSRIRAM":os.getenv("SRIRAM_NUMBER")
}

# 4 PS + 4 PC + 2 HQ Spec
PERM_DUTY_CMDS = {"3SGZEYEUNG":os.getenv("ZEYEUNG_NUMBER"), 
                  "3SGLIANGDING":os.getenv("LIANGDING_NUMBER"), 
                  "3SGKEILOK":os.getenv("KEILOK_NUMBER"), 
                  "3SGGREGORY":os.getenv("GREGORY_NUMBER"), 
                  "3SGKAILE":os.getenv("KAILE_NUMBER"), 
                  "3SGRONGJIN":os.getenv("RONGJIN_NUMBER"), 
                  "ETHANCHAN":os.getenv("ETHANCHAN_NUMBER"), 
                  "JEREMIAH":os.getenv("JEREMIAH_NUMBER"), 
                  "DAEMON":os.getenv("DAEMON_NUMBER"), 
                  "MAX":os.getenv("MAX_NUMBER"),
                  "AJLOY":os.getenv("AJLOY_NUMBER")
}
