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