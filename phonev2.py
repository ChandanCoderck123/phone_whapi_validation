import phonenumbers
import requests
import mysql.connector
from datetime import datetime
import logging
import os

# LOGGING CONFIGURATION 
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"phone_validation_{datetime.now().strftime('%Y%m%d')}.log")
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
    'host': '',
    'user': 'Chandan',
    'password': '',
    'database': ''
}

API_TOKEN = ""
DEFAULT_REGION = "IN"
BATCH_SIZE = 5  # Process 200 at a time as required

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

        cursor.execute(f"""
            SELECT id, phone_no FROM marketing_data
            WHERE Phone_Number_Status = 'not_processed'
            LIMIT {BATCH_SIZE}
        """)
        rows = cursor.fetchall()
        logging.info(f"Found {len(rows)} records to process in this batch.")

        for row in rows:
            row_id = row['id']
            phone_no = row['phone_no']
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            phone_number_validated = None
            phone_number_validated_at = None
            is_whatsapp_active = None
            whatsapp_verified_at = None
            Phone_Number_Status = None
            Phone_Number_Status_at = now

            # If phone_no is blank/null, mark invalid and skip checks
            if not phone_no or not str(phone_no).strip():
                phone_number_validated = 'invalid'
                phone_number_validated_at = now
                is_whatsapp_active = None
                whatsapp_verified_at = None
                Phone_Number_Status = 'invalid'
                Phone_Number_Status_at = now
                logging.info(f"ID {row_id}: phone_no empty/null. Marked invalid.")
            else:
                # Format validation
                phone_result = smart_format_number(str(phone_no), DEFAULT_REGION)
                phone_number_validated = 'valid' if phone_result.get('valid') else 'invalid'
                phone_number_validated_at = now
                logging.info(f"ID {row_id}: phone_no '{phone_no}' format validation: {phone_number_validated}")

                # WhatsApp check: Always check, even if format is invalid (try best possible)
                e164_for_wa = None
                if phone_result.get('e164_format'):
                    e164_for_wa = phone_result['e164_format']
                elif phone_no.isdigit():
                    e164_for_wa = "+91" + phone_no if not phone_no.startswith("+") else phone_no
                else:
                    e164_for_wa = phone_no

                if e164_for_wa:
                    is_wa = check_whatsapp(e164_for_wa, API_TOKEN)
                    is_whatsapp_active = 'active' if is_wa else 'inactive'
                    whatsapp_verified_at = now
                    logging.info(f"ID {row_id}: WhatsApp check on '{e164_for_wa}' result: {is_whatsapp_active}")
                else:
                    is_whatsapp_active = 'inactive'
                    whatsapp_verified_at = now
                    logging.info(f"ID {row_id}: WhatsApp check failed due to bad number format.")

                # Final status
                if phone_number_validated == 'valid' and is_whatsapp_active == 'active':
                    Phone_Number_Status = 'valid'
                else:
                    Phone_Number_Status = 'invalid'
                Phone_Number_Status_at = now
                logging.info(f"ID {row_id}: Final Phone_Number_Status: {Phone_Number_Status}")

            update_query = """
                UPDATE marketing_data
                SET phone_number_validated = %s,
                    phone_number_validated_at = %s,
                    is_whatsapp_active = %s,
                    whatsapp_verified_at = %s,
                    Phone_Number_Status = %s,
                    Phone_Number_Status_at = %s
                WHERE id = %s
            """
            update_cursor.execute(update_query, (
                phone_number_validated,
                phone_number_validated_at,
                is_whatsapp_active,
                whatsapp_verified_at,
                Phone_Number_Status,
                Phone_Number_Status_at,
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
