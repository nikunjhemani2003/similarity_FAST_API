from fastapi import FastAPI, HTTPException, Depends, Request,UploadFile,File
from pydantic import BaseModel, validator, Field
from database import get_db_connection
from datetime import datetime, timedelta
import pandas as pd
import magic 
from fastapi.responses import FileResponse
import io
import psycopg2
import re
import validators   
from validators import *  # This imports all functions from validation_utils
import asyncio  # Add this import at the top



app = FastAPI()


class FieldRecommendRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    table_name: str

    @validator('table_name')
    def validate_table_name(cls, v):
        allowed_tables = ["users", "product"]
        if v not in allowed_tables:
            raise ValueError('Invalid table name.')
        return v

class GSTCheckRequest(BaseModel):
    gst_no: str



class UserNameMappingRequest(BaseModel):
    supplier_name: str
    buyer_name: str
    party_name: str
    invoice_data: dict  # Invoice response from extracted data


class InvoiceValidationRequest(BaseModel):
    extracted_data: dict


@app.post("/field-recommend")
async def field_recommend(request: FieldRecommendRequest):
    """
    API endpoint to recommend similar names based on fuzzy matching.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Different queries for each table
        if request.table_name == "users":
            query = """
                SELECT id, name, address, state_name, state_code, gst_no, pan_number, created_at, 
                       similarity(name, %s) AS similarity_score
                FROM users
                ORDER BY similarity_score DESC
                LIMIT 3;
            """
            cursor.execute(query, (request.name,))
        else:  # product table
            query = """
                SELECT id, item_name, hsn_sac, unit, rate, igst, cgst, sgst, cess,
                       similarity(item_name, %s) AS similarity_score
                FROM product
                ORDER BY similarity_score DESC
                LIMIT 3;
            """
            cursor.execute(query, (request.name,))

        results = cursor.fetchall()

        # Close database connection
        cursor.close()
        conn.close()

        # Return JSON response
        return {
            "input_name": request.name, 
            "table": request.table_name, 
            "recommendations": results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/gst-check")
async def gst_check(request: GSTCheckRequest):
    """
    API endpoint to check if a GST number exists in the users table.
    Returns all matching rows.
    """
    try:
        gst_no = request.gst_no.strip()  # Remove any extra spaces

        print(f"🔍 Checking GST Number: '{gst_no}'")  # Debugging log

        # Establish database connection inside the API
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query to fetch all rows matching the given GST number
        query = """
            SELECT id, name, address, state_name, state_code, gst_no, pan_number, created_at
            FROM users
            WHERE gst_no = %s;
        """

        cursor.execute(query, (gst_no,))
        results = cursor.fetchall()
        print(results)

        # Close connection
        cursor.close()
        conn.close()

        # If no matching records are found, return "No data present"
        if not results:
            return {"status": "not_found", "message": "No data present"}

        # ✅ Convert RealDictRow to a JSON-friendly format
        matching_users = [
            {
                "id": row["id"],
                "name": row["name"],
                "address": row["address"],
                "state_name": row["state_name"],
                "state_code": row["state_code"],
                "gst_no": row["gst_no"],
                "pan_number": row["pan_number"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None
            }
            for row in results
        ]

        return {"status": "found", "matching_users": matching_users}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    


@app.post("/user-mapping")
async def name_mapping(request: UserNameMappingRequest):
    """
    API endpoint to compare names with invoice response.
    If different, store them in the user_name_mapping table with the mapped name's user ID.
    Prevents duplicate AI names before inserting.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Extract names from the request body
        req_supplier_name = request.supplier_name.strip()
        req_buyer_name = request.buyer_name.strip()
        req_party_name = request.party_name.strip()

        # Extract AI-generated names from invoice response
        invoice_data = request.invoice_data.get("extracted_data", {}).get("Gemini", {})
        invoice_supplier_name = invoice_data.get("supplier_details", {}).get("supplier_name", "").strip()
        invoice_buyer_name = invoice_data.get("bill_to_details", {}).get("buyer_name", "").strip()
        invoice_party_name = invoice_data.get("ship_to_details", {}).get("party_name", "").strip()

        print(f"🔍 Comparing AI Names with Mapped Names:")
        print(f"AI Names: Supplier={invoice_supplier_name}, Buyer={invoice_buyer_name}, Party={invoice_party_name}")
        print(f"Mapped Names: Supplier={req_supplier_name}, Buyer={req_buyer_name}, Party={req_party_name}")

        # Check if names are different
        mappings_to_insert = []

        if invoice_supplier_name and invoice_supplier_name != req_supplier_name:
            mappings_to_insert.append((invoice_supplier_name, req_supplier_name))
        if invoice_buyer_name and invoice_buyer_name != req_buyer_name:
            mappings_to_insert.append((invoice_buyer_name, req_buyer_name))
        if invoice_party_name and invoice_party_name != req_party_name:
            mappings_to_insert.append((invoice_party_name, req_party_name))

        if mappings_to_insert:
            print("⚠️ Names do not match. Checking for duplicates before inserting...")
            print(mappings_to_insert)

            # Fetch user ID based on the **mapped name** (req_name) from `users` table
            query_get_user_id = "SELECT id FROM users WHERE name = %s;"

            mapping_results = []
            skipped_count = 0
            inserted_count = 0

            for ai_name, mapped_name in mappings_to_insert:
                # Check if ai_name already exists in user_name_mapping
                check_query = "SELECT id FROM user_name_mapping WHERE ai_name = %s;"
                cursor.execute(check_query, (ai_name,))
                existing_mapping = cursor.fetchone()

                if existing_mapping:
                    print(f"⚠️ Duplicate entry found for AI name: {ai_name}. Skipping insertion.")
                    skipped_count += 1
                    continue  # Skip this entry if it already exists

                # Fetch user ID
                cursor.execute(query_get_user_id, (mapped_name,))
                user_result = cursor.fetchone()
                print(user_result)

                if user_result:
                    mapped_user_id = user_result["id"]  # Get the user ID

                    # Insert into mapping table
                    insert_query = """
                        INSERT INTO user_name_mapping (ai_name, mapped_name, mapped_user_id)
                        VALUES (%s, %s, %s)
                        RETURNING id;
                    """
                    cursor.execute(insert_query, (ai_name, mapped_name, mapped_user_id))
                    mapping_results.append(cursor.fetchone())
                    inserted_count += 1

            conn.commit()  # Save changes

            return {
                "status": "mapping_processed",
                "inserted": inserted_count,
                "skipped": skipped_count,
                "mappings": mapping_results
            }

        else:
            print("✅ Names match. No mapping needed.")
            return {"status": "no_mapping_needed"}

    except Exception as e:
        conn.rollback()  # Rollback transaction in case of failure
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()



@app.get("/download-user-aliases")
async def download_user_aliases():
    """
    API endpoint to download the current user alias mappings as a CSV file.
    """
    try:
        
        conn = get_db_connection()
        cursor = conn.cursor()

        # Fetch existing alias mappings
        query = "SELECT ai_name, mapped_name, mapped_user_id FROM user_name_mapping;"
        cursor.execute(query)
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        if not results:
            raise HTTPException(status_code=404, detail="No user alias mappings found.")

        # Convert results to DataFrame
        df = pd.DataFrame(results)
        file_path = "user_alias_mapping.csv"
        df.to_csv(file_path, index=False)

        return FileResponse(file_path, media_type='text/csv', filename="user_alias_mapping.csv")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    



@app.post("/upload-user-aliases")
async def upload_user_aliases(file: UploadFile = File(...)):
    """
    API endpoint to upload a CSV file and update the user alias mappings in bulk.
    - Finds `mapped_user_id` from `users` table.
    - If `ai_name` exists in `user_name_mapping`, updates it.
    - If `ai_name` does not exist, inserts a new entry.
    """
    try:
        print("📂 Receiving file:", file.filename)  # Debug File Name

        # Read uploaded CSV file
        contents = await file.read()
        print(f"📄 File size: {len(contents)} bytes")  # Debug File Size
        
        df = pd.read_csv(io.BytesIO(contents))
        print("✅ CSV successfully read. Columns:", df.columns.tolist())  # Debug Columns

        # Validate required columns
        required_columns = {"ai_name", "mapped_name"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail="CSV file must contain ai_name, mapped_name columns.")

        conn = get_db_connection()
        cursor = conn.cursor()
        print("🔗 Database connection established.")  # Debug DB Connection

        inserted_count = 0
        updated_count = 0
        skipped_count = 0

        for _, row in df.iterrows():
            ai_name = row["ai_name"].strip()
            mapped_name = row["mapped_name"].strip()
            print(f"🔍 Processing: AI Name = '{ai_name}', Mapped Name = '{mapped_name}'")  # Debug Row

            # **Step 1: Find `mapped_user_id` from `users` table**
            cursor.execute("SELECT id FROM users WHERE name = %s;", (mapped_name,))
            user_result = cursor.fetchone()
            print(f"🛠 User ID Lookup for '{mapped_name}':", user_result)  # Debug User ID Fetch

            if not user_result:
                print(f"⚠️ No user found for mapped name: {mapped_name}. Skipping.")
                skipped_count += 1
                continue  # Skip this entry if the mapped user does not exist

            mapped_user_id = user_result["id"]  # Get the user ID

            # **Step 2: Check if `ai_name` already exists in `user_name_mapping`**
            cursor.execute("SELECT id FROM user_name_mapping WHERE ai_name = %s;", (ai_name,))
            existing_mapping = cursor.fetchone()
            print(f"🔍 Existing Mapping Lookup for '{ai_name}':", existing_mapping)  # Debug Existing Mapping Check

            if existing_mapping:
                # **Step 3: Update existing mapping**
                cursor.execute(
                    """
                    UPDATE user_name_mapping 
                    SET mapped_name = %s, mapped_user_id = %s, created_at = NOW()
                    WHERE ai_name = %s;
                    """,
                    (mapped_name, mapped_user_id, ai_name),
                )
                updated_count += 1
                print(f"✅ Updated mapping for '{ai_name}' -> {mapped_name} (User ID: {mapped_user_id})")  # Debug Update
            else:
                # **Step 4: Insert new mapping**
                cursor.execute(
                    """
                    INSERT INTO user_name_mapping (ai_name, mapped_name, mapped_user_id)
                    VALUES (%s, %s, %s);
                    """,
                    (ai_name, mapped_name, mapped_user_id),
                )
                inserted_count += 1
                print(f"✅ Inserted new mapping for '{ai_name}' -> {mapped_name} (User ID: {mapped_user_id})")  # Debug Insert

        conn.commit()
        cursor.close()
        conn.close()
        print("🔗 Database connection closed.")  # Debug DB Close

        return {
            "status": "success",
            "message": f"{inserted_count} new aliases added, {updated_count} aliases updated, {skipped_count} users not found."
        }

    except Exception as e:
        import traceback
        traceback.print_exc()  # Print full error traceback
        print(f"❌ ERROR: {str(e)}")  # Debug Print
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/validate-invoice")
async def validate_invoice(request: InvoiceValidationRequest):
    """
    API endpoint to validate AI generated invoice data
    """
    try:
        error_list = []

        # Extract invoice date from the request
        bill_to_details = request.extracted_data.get("bill_to_details", {})
        bill_to_invoice_date = bill_to_details.get("invoice_date")
        bill_to_invoice_no = bill_to_details.get("invoice_no")
        bill_to_gst_no = bill_to_details.get("buyer_gst_no")
        bill_to_state_name = bill_to_details.get("buyer_state_name")
        bill_to_state_code = bill_to_details.get("buyer_state_code")
        bill_to_name = bill_to_details.get("buyer_name")
        bill_to_address = bill_to_details.get("buyer_address")
        bill_to_pan_no = bill_to_details.get("buyer_pan_no")

        # Extract product details from the request
        sales_of_product_services = request.extracted_data.get("sales_of_product_services", [])  # Note: Default to empty list



        # For bill_to_details
        if (error := validate_invoice_date(bill_to_invoice_date)):
            error_list.append(error)
        if (error := validate_invoice_number(bill_to_invoice_no)):
            error_list.append(error)
        

        gst_validation_task = validate_gst_number(bill_to_name,bill_to_address,bill_to_gst_no,bill_to_state_name,bill_to_state_code,bill_to_pan_no,"bill_to")  
        gst_validation_result = await gst_validation_task
        if (error := gst_validation_result):
            error_list.append(error)


        name_validation_task = validate_name_in_db("users",bill_to_name,bill_to_gst_no)
        name_validation_result = await name_validation_task
        if (error := name_validation_result):
            error_list.append(error)

        address_validation_task = validate_address_in_db(bill_to_address,bill_to_gst_no)
        address_validation_result = await address_validation_task
        if (error := address_validation_result):
            error_list.append(error)

        if(error := validate_pan_number(bill_to_pan_no)):
            error_list.append(error)


        # sales_of_product_services validation
        # item_name validation
        item_name_validation_task = [
            validate_name_in_db("product", data.get("product_service_description"), None) 
            for data in sales_of_product_services
        ]
        
        item_name_validation_result = await asyncio.gather(*item_name_validation_task)
        
        for error in item_name_validation_result:
            if error:
                error_list.append(error)

        # If no errors found, return success message
        if not error_list:
            return {"status": "success", "message": "All validations passed"}
            
        return {"status": "error", "errors": error_list}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

