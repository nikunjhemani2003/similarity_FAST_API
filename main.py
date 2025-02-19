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
class AddressRecommendRequest(BaseModel):
    address:str
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
                SELECT similarity(name, %s) AS similarity_score,name, address, state_name, state_code, gst_no, pan_number
                FROM users
                ORDER BY similarity_score DESC
                LIMIT 3;
            """
            cursor.execute(query, (request.name,))
        else:  # product table
            query = """
                SELECT similarity(item_name, %s) AS similarity_score,item_name, hsn_sac, unit, rate, igst, cgst, sgst, cess
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
            "recommendations": results,
            "table": request.table_name 
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/address-recommend")
async def field_recommend(request: AddressRecommendRequest):
    """
    API endpoint to recommend similar names based on fuzzy matching.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Different queries for each table
        query = """
                SELECT similarity(address, %s) AS similarity_score, name, address, state_name, state_code, gst_no, pan_number
                FROM users
                ORDER BY similarity_score DESC
                LIMIT 3;
            """
        cursor.execute(query, (request.address,))

        results = cursor.fetchall()

        # Close database connection
        cursor.close()
        conn.close()

        # Return JSON response
        return {
            "recommendations": results,
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
        # Extract invoice date from the request
        extracted_data = request.extracted_data


        supplier_details = extracted_data.get("supplier_details", {})
        supplier_details["error_status"]=False
        supplier_details["error"]={}
        supplier_details["recommended_fields"]=[]
        bill_to_details = extracted_data.get("bill_to_details", {})
        bill_to_details["error_status"]=False
        bill_to_details["error"]={}
        bill_to_details["recommended_fields"]=[]
        ship_to_details = extracted_data.get("ship_to_details", {})
        ship_to_details["error_status"]=False
        ship_to_details["error"]={}
        ship_to_details["recommended_fields"]=[]
        transaction_details = extracted_data.get("transaction_details", {})
        other_details = extracted_data.get("other_details", {})
        sales_of_product_services = extracted_data.get("sales_of_product_services", [])  # Note: Default to empty list
        # sales_of_product_services.insert(0,{"error_status":False,"error ":[]})
        # sales_of_product_services.insert(0,{"error_status":False,"error ":[]})
        # sales_of_product_services.insert(1,{"recommended_fields":[]})
        gst_summary = extracted_data.get("gst_summary", [])
        additional_information = extracted_data.get("additional_information", {})

        #Validation for supplier_details
        supplier_name = supplier_details.get("supplier_name")
        supplier_address = supplier_details.get("supplier_address")
        supplier_gst_no = supplier_details.get("supplier_gst_no")
        supplier_state_name = supplier_details.get("supplier_state_name")
        supplier_state_code = supplier_details.get("supplier_state_code")
        supplier_pan_no = supplier_details.get("supplier_pan_no")

        supplier_gst_validation_task = validate_gst_number(supplier_name,supplier_address,supplier_gst_no,supplier_state_name,supplier_state_code,supplier_pan_no,"supplier")
        supplier_error,supplier_recommended_fields = await supplier_gst_validation_task
        if (error := supplier_error):
            supplier_details["error_status"]=True
            if(supplier_recommended_fields):
                supplier_details["recommended_fields"].append(supplier_recommended_fields)
            supplier_details["error"].update(error)


        supplier_name_validation_task = validate_name_in_db("users",supplier_name,supplier_gst_no,"supplier")
        supplier_name_error, supplier_name_recommended_fields = await supplier_name_validation_task

        if (error := supplier_name_error):
            supplier_details["error_status"] = True
            supplier_details["error"].update(error)
            
            if supplier_name_recommended_fields:
                # Ensure the recommended fields remain a flat list
                if isinstance(supplier_name_recommended_fields, list):
                    supplier_details["recommended_fields"].extend(supplier_name_recommended_fields)
                else:
                    supplier_details["recommended_fields"].append(supplier_name_recommended_fields)

        supplier_address_validation_task = validate_address_in_db(supplier_address, supplier_gst_no, supplier_name, "supplier")
        supplier_address_error, supplier_address_recommended_fields = await supplier_address_validation_task

        if (error := supplier_address_error):
            supplier_details["error_status"] = True
            supplier_details["error"].update(error)

            if supplier_address_recommended_fields:
                # Ensure the recommended fields remain a flat list
                if isinstance(supplier_address_recommended_fields, list):
                    supplier_details.setdefault("recommended_fields", []).extend(supplier_address_recommended_fields)
                else:
                    supplier_details.setdefault("recommended_fields", []).append(supplier_address_recommended_fields)

                    
                if(error := validate_pan_number(supplier_pan_no,"supplier")):
                    supplier_details["error_status"]=True
                    supplier_details["error"].update(error)

        # #Validation for bill_to_details

        bill_to_invoice_date = bill_to_details.get("invoice_date")
        bill_to_invoice_no = bill_to_details.get("invoice_no")
        bill_to_gst_no = bill_to_details.get("buyer_gst_no")
        bill_to_state_name = bill_to_details.get("buyer_state_name")
        bill_to_state_code = bill_to_details.get("buyer_state_code")
        bill_to_name = bill_to_details.get("buyer_name")
        bill_to_address = bill_to_details.get("buyer_address")
        bill_to_pan_no = bill_to_details.get("buyer_pan_no")

        # # # Extract product details from the request


        
        # # # # For bill_to_details
        if (error := validate_invoice_date(bill_to_invoice_date,"buyer")):
            bill_to_details["error_status"]=True
            bill_to_details["error"]=error
        if (error := validate_invoice_number(bill_to_invoice_no,"buyer")):
            bill_to_details["error_status"]=True
            bill_to_details["error"].update(error)
        

        gst_validation_task = validate_gst_number(bill_to_name,bill_to_address,bill_to_gst_no,bill_to_state_name,bill_to_state_code,bill_to_pan_no,"buyer")  
        gst_error,gst_recommended_fields = await gst_validation_task
        if (error := gst_error):
            bill_to_details["error_status"]=True
            if(gst_recommended_fields):
                bill_to_details["recommended_fields"].append(gst_recommended_fields)
            bill_to_details["error"].update(error)


        name_validation_task = validate_name_in_db("users",bill_to_name,bill_to_gst_no,"buyer")
        name_error, name_recommended_fields = await name_validation_task

        if (error := name_error):
            bill_to_details["error_status"] = True
            bill_to_details["error"].update(error)

            if name_recommended_fields:
                # Ensure the recommended fields remain a flat list
                if isinstance(name_recommended_fields, list):
                    bill_to_details["recommended_fields"].extend(name_recommended_fields)
                else:
                    bill_to_details["recommended_fields"].append(name_recommended_fields)


        address_validation_task = validate_address_in_db(bill_to_address, bill_to_gst_no, bill_to_name,"buyer")
        address_error, address_recommended_fields = await address_validation_task

        if (error := address_error):
            bill_to_details["error_status"] = True
            bill_to_details["error"].update(error)

            if address_recommended_fields:
                # Ensure the recommended fields remain a flat list
                if isinstance(address_recommended_fields, list):
                    bill_to_details.setdefault("recommended_fields", []).extend(address_recommended_fields)
                else:
                    bill_to_details.setdefault("recommended_fields", []).append(address_recommended_fields)



        if(error := validate_pan_number(bill_to_pan_no,"buyer")):
            bill_to_details["error_status"]=True
            bill_to_details["error"].update(error)



        # # # # validation for Party_name
        party_name = ship_to_details.get("party_name")
        party_address = ship_to_details.get("party_address")
        party_gst_no = ship_to_details.get("party_gst_no")
        party_state_name = ship_to_details.get("party_state_name")
        party_state_code = ship_to_details.get("party_state_code")
        party_pan_no = ship_to_details.get("party_pan_no")

        party_gst_validation_task = validate_gst_number(party_name,party_address,party_gst_no,party_state_name,party_state_code,party_pan_no,"party")
        party_error,party_recommended_fields = await party_gst_validation_task
        if (error := party_error):
            ship_to_details["error_status"]=True
            if(party_recommended_fields):
                ship_to_details["recommended_fields"].append(party_recommended_fields)
            ship_to_details["error"].update(error)    

        party_name_validation_task = validate_name_in_db("users", party_name, party_gst_no, "party")
        party_name_error, party_name_recommended_fields = await party_name_validation_task

        if (error := party_name_error):
            ship_to_details["error_status"] = True
            ship_to_details["error"].update(error)

            if party_name_recommended_fields:
                # Ensure the recommended fields remain a flat list
                if isinstance(party_name_recommended_fields, list):
                    ship_to_details.setdefault("recommended_fields", []).extend(party_name_recommended_fields)
                else:
                    ship_to_details.setdefault("recommended_fields", []).append(party_name_recommended_fields)


        party_address_validation_task = validate_address_in_db(party_address, party_gst_no, party_name, "party")
        party_address_error, party_address_recommended_fields = await party_address_validation_task

        if (error := party_address_error):
            ship_to_details["error_status"] = True
            ship_to_details["error"].update(error)

            if party_address_recommended_fields:
                # Ensure the recommended fields remain a flat list
                if isinstance(party_address_recommended_fields, list):
                    ship_to_details.setdefault("recommended_fields", []).extend(party_address_recommended_fields)
                else:
                    ship_to_details.setdefault("recommended_fields", []).append(party_address_recommended_fields)


                if(error := validate_pan_number(party_pan_no,"party")):
                    ship_to_details["error_status"]=True
                    ship_to_details["error"].update(error)
        
        

        # # # sales_of_product_services validation
        # item_name_validation_tasks = [
        #     validate_name_in_db("product", data.get("product_service_description"), None) 
        #     for data in sales_of_product_services
        # ]

        # # # Gather all validation results
        # item_validation_results = await asyncio.gather(*item_name_validation_tasks)

        # # Process results and collect recommendations
        # for index, (error, recommendations) in enumerate(item_validation_results):
        #     if error:
        #         sales_of_product_services[0]["error"].append(error)
        #         if(recommendations):
        #             sales_of_product_services[1]["recommended_fields"].append(recommendations)
        try:
            # Create validation tasks for each product
            item_name_validation_tasks = [
                validate_name_in_db("product", data.get("product_service_description"), None, "product_service_description") 
                for data in sales_of_product_services
            ]

            # Gather all validation results
            item_validation_results = await asyncio.gather(*item_name_validation_tasks)

            # Process results and collect recommendations for each item separately
            for index, (error, recommendations) in enumerate(item_validation_results):
                sales_of_product_services[index]["error_status"]=False
                sales_of_product_services[index]["error"]={}
                sales_of_product_services[index]["recommended_fields"]=[]
                if error:
                    sales_of_product_services[index]["error_status"]=True
                    sales_of_product_services[index]["error"].update(error)
                    
                    if recommendations:
                        sales_of_product_services[index]["recommended_fields"].extend(recommendations)

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        return {"extracted_data":request.extracted_data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

