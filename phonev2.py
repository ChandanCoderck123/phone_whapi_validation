import phonenumbers
import requests
import mysql.connector
from datetime import datetime
import logging
import os

# LOGGING CONFIGURATION 
LOG_FILE = "phone_validation_cron.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# MYSQL CONFIGURATION 
MYSQL_CONFIG = {
    'host': 'holistique-middleware.c9wdjmzy25ra.ap-south-1.rds.amazonaws.com',
    'user': 'Chandan',
    'password': 'Chandan@#4321',
    'database': 'email_validator_app'
}
API_TOKEN = "mTrC6lq8XjJW4KrwfldSMsTUqK29Hsco"
DEFAULT_REGION = "IN"
BATCH_SIZE = 10

# PHONE NUMBER UTILS 
def smart_format_number(number: str, default_region: str = "IN") -> dict:
    number = number.strip().replace(" ", "")
    if not number:
        return {"valid": False, "error": "Blank number"}
    if number.startswith("+"):
        region_to_use = None
        number_to_parse = number
    elif len(number) == 12 and number.startswith("91"):
        region_to_use = None
        number_to_parse = "+" + number
    else:
        region_to_use = default_region.upper()
        number_to_parse = number
    try:
        parsed_number = phonenumbers.parse(number_to_parse, region_to_use)
        is_valid = phonenumbers.is_valid_number(parsed_number)
        e164_format = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
        return {"valid": is_valid, "e164_format": e164_format}
    except Exception as e:
        return {"valid": False, "error": str(e)}

def check_whatsapp(complete_number: str, api_token: str) -> bool:
    number_digits = complete_number.replace("+", "")
    url_check = "https://gate.whapi.cloud/contacts"
    payload = {
        "blocking": "wait",
        "contacts": [number_digits],
        "force_check": False
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": api_token
    }
    try:
        response = requests.post(url_check, json=payload, headers=headers, timeout=10)
        result = response.json()
        if result.get("contacts") and result["contacts"][0].get("status") == "valid":
            return True
    except Exception as e:
        logging.warning(f"WhatsApp API error for {complete_number}: {e}")
    return False

# MAIN PROCESSING 
def process_and_update_rows():
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        update_cursor = conn.cursor()

        # Fetch up to BATCH_SIZE rows for processing
        cursor.execute(f"""
            SELECT id, phone_no FROM marketing_data
            WHERE phone_no_verification_wa_api = 'Not_Processed'
            LIMIT {BATCH_SIZE}
        """)
        rows = cursor.fetchall()
        logging.info(f"Found {len(rows)} records to process in this batch.")

        for row in rows:
            row_id = row['id']
            phone_no = row['phone_no']
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Default values
            phone_no_verification_wa_api = "Not_Processed"
            phono_no_status_wa = "pending"
            other_validation_phone = None

            # CASE 1: phone_no is null or empty
            if not phone_no or not str(phone_no).strip():
                phone_no_verification_wa_api = "invalid"
                phono_no_status_wa = "successful"
                other_validation_phone = None
                logging.info(f"ID {row_id}: phone_no empty/null. Marked invalid.")
            else:
                phone_result = smart_format_number(str(phone_no), DEFAULT_REGION)
                if not phone_result.get("valid"):
                    phone_no_verification_wa_api = "invalid"
                    phono_no_status_wa = "successful"
                    other_validation_phone = None
                    logging.info(f"ID {row_id}: Invalid format ({phone_no}). Marked invalid.")
                else:
                    if check_whatsapp(phone_result["e164_format"], API_TOKEN):
                        phone_no_verification_wa_api = "valid"
                        phono_no_status_wa = "successful"
                        other_validation_phone = None
                        logging.info(f"ID {row_id}: {phone_no} is on WhatsApp. Marked valid.")
                    else:
                        phone_no_verification_wa_api = "invalid"
                        phono_no_status_wa = "successful"
                        other_validation_phone = "Need to validate one more step"
                        logging.info(f"ID {row_id}: {phone_no} NOT on WhatsApp. Marked invalid, needs further validation.")

            update_query = """
                UPDATE marketing_data
                SET phone_no_verification_wa_api = %s,
                    phono_no_status_wa = %s,
                    phone_created_at = IFNULL(phone_created_at, %s),
                    phone_updated_at = %s,
                    other_validation_phone = %s
                WHERE id = %s
            """
            update_cursor.execute(update_query, (
                phone_no_verification_wa_api,
                phono_no_status_wa,
                now,
                now,
                other_validation_phone,
                row_id
            ))
            conn.commit()

        cursor.close()
        update_cursor.close()
        conn.close()
        logging.info("Batch processing complete.")

    except Exception as e:
        logging.error(f"Exception in processing batch: {e}")

if __name__ == "__main__":
    process_and_update_rows()
