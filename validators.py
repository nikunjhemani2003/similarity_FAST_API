from datetime import datetime, timedelta

def validate_invoice_date(invoice_date_str: str) -> dict:
    """
    Validates if the given invoice date is:
    1. In correct format (DD/MM/YYYY)
    2. Not in the future
    3. Not older than 30 days from the current date
    
    Args:
        invoice_date_str (str): Invoice date in DD/MM/YYYY format
        
    Returns:
        dict: Validation result with status and message
    """
    if not invoice_date_str:
        return {
            "invoice_date": invoice_date_str,
            "error": "Invoice date is missing"
        }

    # Convert date string (DD/MM/YYYY) to datetime object
    try:
        invoice_date = datetime.strptime(invoice_date_str, "%d/%m/%Y")
    except ValueError:
        return {
            "invoice_date": invoice_date_str,
            "error": "Invalid date format. Expected DD/MM/YYYY"
        }

    # Get current date (without time)
    current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Calculate date 30 days ago
    thirty_days_ago = current_date - timedelta(days=30)

    # Validate future date
    if invoice_date > current_date:
        return {
            "invoice_date": invoice_date_str,
            "error": "Invoice date cannot be in the future"
        }

    # Validate if invoice is older than 30 days
    if invoice_date < thirty_days_ago:
        return {
            "invoice_date": invoice_date_str,
            "error": "Invoice date cannot be older than 30 days"
        }

    # return {
    #     "invoice_date": invoice_date_str,
    #     "error": "Invoice date is valid"
    # }


def validate_invoice_number(invoice_no: str) -> dict:
    """
    Validates if the invoice number is provided (mandatory field).
    
    Args:
        invoice_no (str): Invoice number
        
    Returns:
        dict: Validation result with status and message
    """
    if not invoice_no or invoice_no.strip() == "" or invoice_no == "null":
        return {
            "invoice_no": invoice_no,
            "error": "Invoice number is required"
        }

    # return {
    #     "invoice_no": invoice_no,
    #     "error": "Invoice number is valid"
    # }







import re
import httpx  # Async HTTP client for API calls

GST_REGEX = r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9][Z][0-9A-Z]$'  # GST Format: 12ABCDE1234F1Z5
GST_CHECK_API = "http://localhost:8000/gst-check"  # API endpoint for checking GST

async def validate_gst_number(name: str, address: str, gst_no: str,state_name:str, state_code: str,pan_no:str,entity:str) -> dict | None:
    """
    Validates the format of the GST number (if provided), checks if it exists in the database,
    and verifies that the provided fields match the database records.

    Args:
        entity_details (dict): Dictionary containing GST and associated details
    
    Returns:
        dict | None: Returns a dictionary with errors if invalid; otherwise, returns None
    """
    try:
        entity_gst_no = gst_no

        # If GST number is not provided, return without error
        if not entity_gst_no or entity_gst_no.strip() == "":
            return None

        # Validate GST format
        if not re.match(GST_REGEX, entity_gst_no.strip()):
            return {
                "gst_number": entity_gst_no,
                "error": "Invalid GST number format. Expected format: 12ABCDE1234F1Z5"
            }

        # Validate required fields when GST is provided
        missing_fields = []
        entity_state_name = state_name
        entity_state_code = state_code

        if not entity_state_name or entity_state_name.strip() == "" or entity_state_name == "null" or entity_state_code == 0 or entity_state_code == "0":
            missing_fields.append("State name is required when GST number is provided")
        if entity_state_code is None or entity_state_code == "0":
            missing_fields.append("State code is required when GST number is provided")

            

        # Check GST in database
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    GST_CHECK_API, 
                    json={"gst_no": entity_gst_no.strip()},
                    timeout=10.0  # Add timeout
                )
                response.raise_for_status()  # Raise exception for non-200 status codes
            except httpx.HTTPError as e:
                return {
                    "gst_number": entity_gst_no,
                    "error": f"Error checking GST number: {str(e)}"
                }

        gst_data = response.json()

        # If GST is not found in database
        if gst_data.get("status") == "not_found":
            return {
                "gst_number": entity_gst_no,
                "error": "GST number not found in database"
            }

        # Validate database record matches
        db_entry = gst_data.get("matching_users", [{}])[0]
        errors = []

        # Compare fields (case-insensitive)
        print(name,address,entity_state_name,entity_state_code)
        if db_entry.get("name", "").strip().lower() != name.strip().lower():
            errors.append("Name does not match database record")
        if db_entry.get("address", "").strip().lower() != address.strip().lower():
            errors.append("Address does not match database record")
        if db_entry.get("state_name", "").strip().lower() != entity_state_name.strip().lower():
            errors.append("State name does not match database record")
        if db_entry.get("state_code") != entity_state_code:
            errors.append("State code does not match database record")
        if pan_no and db_entry.get("pan_number", "").strip().lower() != pan_no.strip().lower():
            errors.append("PAN number does not match database record")


        if errors and missing_fields:
            return {"gst_number": entity_gst_no, "error": errors + missing_fields}
        elif errors:
            return {"gst_number": entity_gst_no, "error": errors}
        elif missing_fields:
            return {"gst_number": entity_gst_no, "error": missing_fields}
        

        return None  # All validations passed

    except Exception as e:
        return {
            "gst_number": entity_gst_no if 'entity_gst_no' in locals() else None,
            "error": f"Unexpected error during GST validation: {str(e)}"
        }




from database import get_db_connection
async def validate_name_in_db(table_name: str, input_name: str,gst_no:str) -> dict|None:
    """
    Checks if the given name exists in the specified database table.

    Args:
        table_name (str): The name of the database table to search.
        input_name (str): The name to check.

    Returns:
        dict | None: Returns None if the name exists, otherwise an error message.
    """
    # Check if table_name and input_name are provided
    if not table_name or table_name.strip() == "":
        return {"error": "Table name is required"}
    
    if not input_name or input_name.strip() == "":
        if(table_name == "users"):
            return {"error": "Name is required"}
        elif(table_name == "product"):
            return {"error": "Product name is required"}
        else:
            return {"error": "User/Product name is required"}

    try:
        if gst_no:
            return None
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query to search for the name in a case-insensitive manner
        if(table_name == "users"):
            query = f"SELECT name FROM {table_name} WHERE LOWER(name) = LOWER(%s) LIMIT 1;"
        else:
            query = f"SELECT item_name FROM {table_name} WHERE LOWER(item_name) = LOWER(%s) LIMIT 1;"
        cursor.execute(query, (input_name.strip(),))
        result = cursor.fetchone()


        cursor.close()
        conn.close()

        if not result:
            return {"input_name": input_name, "error": f"Name '{input_name}' not found in table '{table_name}'"}

        return None

    except Exception as e:
        return {"error": f"Database error: {str(e)}"}


from database import get_db_connection

async def validate_address_in_db(input_address: str, gst_no: str) -> dict | None:
    """
    Checks if the given address exists in the 'users' table.

    Args:
        input_address (str): The address to check.
        gst_no (str): The GST number (if provided).

    Returns:
        dict | None: Returns None if the address exists, otherwise an error message.
    """
    # Check if input_address is provided
    if not input_address or input_address.strip() == "":
        return {"error": "Address is required"}

    try:
        # ✅ If GST exists, assume the address is valid
        if gst_no:
            return None

        conn = get_db_connection()
        cursor = conn.cursor()

        # ✅ Query to search for the address (case-insensitive)
        query = "SELECT address FROM users WHERE TRIM(LOWER(address)) = TRIM(LOWER(%s)) LIMIT 1;"
        cursor.execute(query, (input_address.strip(),))
        result = cursor.fetchone()

        cursor.close()
        conn.close()

        # ✅ If no match found, return an error
        if not result:
            return {"input_address": input_address, "error": f"Address '{input_address}' not found in database"}

        return None  # ✅ Address exists, no error

    except Exception as e:
        return {"error": f"Database error: {str(e)}"}




# ✅ PAN Format Regex Pattern (Strictly Enforces Format)
# import re
PAN_REGEX = r'^[A-Z]{5}[0-9]{4}[A-Z]$'

def validate_pan_number(pan_no: str) -> dict | None:
    """
    Validates the format of a PAN number.

    Args:
        pan_no (str): The PAN number to validate.

    Returns:
        dict | None: Returns an error message if invalid; otherwise, returns None.
    """
    # ✅ Ensure PAN number is provided
    if not pan_no or pan_no.strip() == "":
        return None

    # ✅ Validate PAN format using regex
    if not re.match(PAN_REGEX, pan_no.strip()):
        return {"pan_number": pan_no, "error": "Invalid PAN number format. Expected format: ABCDE1234F"}

    # ✅ If PAN format is valid, return None (no error)
    return None
