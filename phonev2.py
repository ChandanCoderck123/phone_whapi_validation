import phonenumbers
import requests
import mysql.connector
from datetime import datetime
import logging
import os

# 1. LOGGING CONFIGURATION
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

# 2. MYSQL CONFIGURATION
MYSQL_CONFIG = {
    'host': '',
    'user': 'Chandan',
    'password': 'Chandan@#4321',
    'database': ''
}

API_TOKEN = ""  
DEFAULT_REGION = "IN"
BATCH_SIZE = 5  # Or 200 in production

# 3. PHONE NUMBER FORMAT VALIDATION
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

# 4. WHATSAPP CHECK USING WHAPI CLOUD
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

# 5. MAIN PROCESSING FUNCTION
def process_and_update_rows():
    """
    1. Fetch a batch of unprocessed phone numbers from the MySQL table.
    2. For each:
        a. If blank/null, set phone_number_validated='false', status=invalid.
        b. Otherwise, check format (phone_number_validated='true' or 'false'), always run WhatsApp check.
        c. If phone_number_validated='true' and is_whatsapp_active='active', set Phone_Number_Status=valid, else invalid.
    3. Update the DB with all results and timestamps.
    """
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

            phone_number_validated = None  # 'true' or 'false'
            phone_number_validated_at = None
            is_whatsapp_active = None      # 'active' or 'inactive'
            whatsapp_verified_at = None
            Phone_Number_Status = None     # 'valid' or 'invalid'
            Phone_Number_Status_at = now

            # 5a. If phone_no is blank/null
            if not phone_no or not str(phone_no).strip():
                phone_number_validated = 'false'
                phone_number_validated_at = now
                is_whatsapp_active = None
                whatsapp_verified_at = None
                Phone_Number_Status = 'invalid'
                Phone_Number_Status_at = now
                logging.info(f"ID {row_id}: phone_no empty/null. Marked invalid.")
            else:
                # 5b. Format validation
                phone_result = smart_format_number(str(phone_no), DEFAULT_REGION)
                phone_number_validated = 'true' if phone_result.get('valid') else 'false'
                phone_number_validated_at = now
                logging.info(f"ID {row_id}: phone_no '{phone_no}' format validation: {phone_number_validated}")

                # WhatsApp check: Always check (best effort)
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

                # 5c. Final status logic
                if phone_number_validated == 'true' and is_whatsapp_active == 'active':
                    Phone_Number_Status = 'valid'
                else:
                    Phone_Number_Status = 'invalid'
                Phone_Number_Status_at = now
                logging.info(f"ID {row_id}: Final Phone_Number_Status: {Phone_Number_Status}")

            # 6. Update DB
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
