from datetime import datetime, timedelta

def validate_invoice_date(invoice_date_str: str,entity:str) -> dict | None:
    """
    Validates if the given invoice date is:
    1. In correct format (DD/MM/YYYY)
    2. Not in the future
    3. Not older than 30 days from the current date
    
    Args:
        invoice_date_str (str): Invoice date in DD/MM/YYYY format
        
    Returns:
        dict | None: Returns error dict if invalid; otherwise, returns None
    """

    if not invoice_date_str or invoice_date_str.strip() == "" or invoice_date_str == "null":
        return {
            f"{entity}_invoice_date": "Invoice date is missing"
        }

    try:    
        invoice_date = datetime.strptime(invoice_date_str, "%d/%m/%y")
    except ValueError:
        return {
            f"{entity}_invoice_date": "Invalid date format. Expected DD/MM/YY"
        }

    # Get current date (without time)
    current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    print(current_date)
    
    # Calculate date 30 days ago
    thirty_days_ago = current_date - timedelta(days=30)

    # Validate future date
    if invoice_date > current_date:
        return {
            f"{entity}_invoice_date": "Invoice date cannot be in the future"
        }

    # Validate if invoice is older than 30 days
    if invoice_date < thirty_days_ago:
        print("Invoice date cannot be older than 30 days")
        return {
            f"{entity}_invoice_date": "Invoice date cannot be older than 30 days"
        }

    return None


def validate_invoice_number(invoice_no: str,entity:str) -> dict | None:
    """
    Validates if the invoice number is provided (mandatory field).
    
    Args:
        invoice_no (str): Invoice number
        
    Returns:
        dict | None: Returns error dict if invalid; otherwise, returns None
    """
    if not invoice_no or invoice_no.strip() == "" or invoice_no == "null":
        return {
            f"{entity}_invoice_no": "Invoice number is required"
        }
    # return None







import re
import httpx  # Async HTTP client for API calls

GST_REGEX = r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9][Z][0-9A-Z]$'  # GST Format: 12ABCDE1234F1Z5
GST_CHECK_API = "http://localhost:8000/gst-check"  # API endpoint for checking GST

async def validate_gst_number(name: str, address: str, gst_no: str, state_name:str, state_code: str, pan_no:str, entity:str) -> tuple[dict|None, dict]:
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

        if not entity_gst_no or entity_gst_no == "" or entity_gst_no == "null":
            return None, {}

        # Validate GST format
        if not re.match(GST_REGEX, entity_gst_no.strip()):
            return {
                f"{entity}_gst_number": "Invalid GST number format. Expected format: 12ABCDE1234F1Z5"
            }, {}

        # Check GST in database
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    GST_CHECK_API, 
                    json={"gst_no": entity_gst_no.strip()},
                    timeout=10.0
                )
                response.raise_for_status()
            except httpx.HTTPError as e:
                return {
                    f"{entity}_gst_number": f"Error checking GST number: {str(e)}"
                }, {}

        gst_data = response.json()

        if gst_data.get("status") == "not_found":
            return {
                f"{entity}_gst_number": f"GST number not found in database"
            }, {}

        # Validate database record matches
        db_entry = gst_data.get("matching_users", [{}])[0]
        errors = {}
        recommended_fields = {}

        # Compare fields (case-insensitive)
        if db_entry.get("name", "").strip().lower() != name.strip().lower():
            recommended_fields[f"{entity}_name"] = db_entry.get("name", "").strip()
            errors[f"{entity}_name"] = "Name does not match database record"
        if db_entry.get("address", "").strip().lower() != address.strip().lower():
            recommended_fields[f"{entity}_address"] = db_entry.get("address", "").strip()
            errors[f"{entity}_address"] = "Address does not match database record"
        if db_entry.get("state_name", "").strip().lower() != state_name.strip().lower():
            recommended_fields[f"{entity}_state_name"] = db_entry.get("state_name", "").strip()
            errors[f"{entity}_state_name"] = "State name does not match database record"
        if db_entry.get("state_code") != state_code:
            recommended_fields[f"{entity}_state_code"] = db_entry.get("state_code")
            errors[f"{entity}_state_code"] = "State code does not match database record"
        if not pan_no or db_entry.get("pan_number", "").lower() != pan_no.strip().lower():
            recommended_fields[f"{entity}_pan_number"] = db_entry.get("pan_number", "").strip()
            errors[f"{entity}_pan_number"] = "PAN number does not match database record"

        # Return errors if any exist
        if errors:
            return errors, recommended_fields
        
        return None, {}

    except Exception as e:
        return {
            f"{entity}_gst_number": f"Unexpected error during GST validation: {str(e)}"
        }, {}




from database import get_db_connection
async def validate_name_in_db(table_name: str, input_name: str, gst_no:str,entity:str) -> tuple[dict|None, dict]:
    """
    Checks if the given name exists in the specified database table.

    Args:
        table_name (str): The name of the database table to search.
        input_name (str): The name to check.

    Returns:
        tuple[dict|None, dict]: Returns (None, {}) if valid, otherwise (error_dict, recommendations)
    """
    try:
        # Check if table_name and input_name are provided
        if not table_name or table_name.strip() == "":
            raise ValueError("Table name is required")
            
        if not input_name or input_name.strip() == "":
            if table_name == "users":
                return {f"{entity}_name": "User name is required"}, {}
            elif table_name == "product":
                return {f"{entity}_name": "Product name is required"}, {}
            else:
                return {f"{entity}_name": "User/Product name is required"}, {}
        
        if gst_no:
            return None, {}
        
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
            async with httpx.AsyncClient() as client:
                try:
                    response = await client.post(
                        "http://127.0.0.1:8000/field-recommend",
                        json={"name": input_name.strip(),"table_name":table_name},
                        timeout=10.0
                    )
                    response.raise_for_status()
                    recommendations = response.json()

                    return {
                        f"{entity}_name": f"Name '{input_name}' not found in table '{table_name}'"
                    }, recommendations.get("recommendations", [])
                except httpx.HTTPStatusError as e:
                    return {f"{entity}_name": f"API returned {e.response.status_code}: {e.response.text}"}, {}
                except httpx.RequestError as e:
                    return {f"{entity}_name": f"API request failed: {str(e)}"}, {}

        return None, {}  # Added empty dict as second return value

    except ValueError as ve:
        return {f"{entity}_name": str(ve)}, {}
    except Exception as e:
        return {f"{entity}_name": f"Database error: {str(e)}"}, {}


from database import get_db_connection

# async def validate_address_in_db(input_address: str, gst_no: str) -> dict | None:
#     """
#     Checks if the given address exists in the 'users' table.
#     If not found, uses field-recommend API to get similar addresses.
#     """

#     try:
#         if gst_no:
#             return None
        
#         if not input_address or input_address.strip() == "":
#             return {"error": "Address is required"}

#         conn = get_db_connection()
#         cursor = conn.cursor()

#         # Check for exact match first
#         query = "SELECT address FROM users WHERE TRIM(LOWER(address)) = TRIM(LOWER(%s)) LIMIT 1;"
#         cursor.execute(query, (input_address.strip(),))
#         result = cursor.fetchone()

#         cursor.close()
#         conn.close()

#         # If no exact match, get recommendations
#         if not result:
#             async with httpx.AsyncClient() as client:
#                 response = await client.post(
#                     "http://127.0.0.1:8000/field-recommend",
#                     json={
#                         "name": input_address.strip(),
#                         "table_name": "users"
#                     },
#                     timeout=10.0
#                 )
#                 response.raise_for_status()
#                 recommendations = response.json()

#                 return {
#                     "input_address": input_address,
#                     "error": f"Address '{input_address}' not found in database",
#                     "recommendations": recommendations.get("recommendations", [])
#                 }

#         return None

#     except Exception as e:
#         return {"error": f"Database error: {str(e)}"}

async def validate_address_in_db(input_address: str, gst_no: str,name:str,entity:str) -> tuple[dict|None, dict]:
    """
    Checks if the given address exists in the 'users' table.
    If not found, uses field-recommend API to get similar addresses.
    """
    try:
        if gst_no or name:
            return None,{}
        
        if not input_address or input_address.strip() == "":
            return {f"{entity}_address": "Address is required"},{}

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check for exact match first
        query = "SELECT address FROM users WHERE TRIM(LOWER(address)) = TRIM(LOWER(%s)) LIMIT 1;"
        cursor.execute(query, (input_address.strip(),))
        result = cursor.fetchone()

        cursor.close()
        conn.close()

        # If no exact match, get recommendations
        if not result:
            async with httpx.AsyncClient() as client:
                try:
                    response = await client.post(
                        "http://127.0.0.1:8000/address-recommend",
                        json={"address": input_address.strip()},
                        timeout=10.0
                    )
                    response.raise_for_status()
                    recommendations = response.json()

                    return {
                        f"{entity}_address": f"Address '{input_address}' not found in database, Choose from the following recommendations"
                    },recommendations.get("recommendations", [])
                except httpx.HTTPStatusError as e:
                    return {f"{entity}_address": f"API returned {e.response.status_code}: {e.response.text}"},{}
                except httpx.RequestError as e:
                    return {f"{entity}_address": f"API request failed: {str(e)}"},{}

        return None,{}

    except Exception as e:
        return {f"{entity}_address": f"Database error: {str(e)}"},{}






# import re
PAN_REGEX = r'^[A-Z]{5}[0-9]{4}[A-Z]$'
def validate_pan_number(pan_no: str,entity:str) -> dict | None:
    """
    Validates the format of a PAN number.

    Args:
        pan_no (str): The PAN number to validate.

    Returns:
        dict | None: Returns an error message if invalid; otherwise, returns None.
    """
    # ✅ Ensure PAN number is provided
    if not pan_no or pan_no.strip() == "" or pan_no == "null":
        return None

    # ✅ Validate PAN format using regex
    if not re.match(PAN_REGEX, pan_no.strip()):
        return {f"{entity}_pan_number": f"Invalid PAN number format. Expected format: ABCDE1234F"}

    # ✅ If PAN format is valid, return None (no error)
    return None
