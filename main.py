from fastapi import FastAPI, HTTPException, Depends, Request
from pydantic import BaseModel, validator, Field
from database import get_db_connection
from datetime import datetime
import psycopg2

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



class NameMappingRequest(BaseModel):
    supplier_name: str
    buyer_name: str
    party_name: str
    invoice_data: dict  # Invoice response from extracted data


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
    

@app.post("/name-mapping")
async def name_mapping(request: NameMappingRequest):
    """
    API endpoint to compare names with invoice response.
    If different, store them in the user_name_mapping table with the mapped name's user ID.
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
            print("⚠️ Names do not match. Storing in mapping table...")
            print(mappings_to_insert)

            # Fetch user ID based on the **mapped name** (req_name) from `users` table
            query_get_user_id = "SELECT id FROM users WHERE name = %s;"

            mapping_results = []

            for ai_name, mapped_name in mappings_to_insert:
                cursor.execute(query_get_user_id, (mapped_name,))
                user_result = cursor.fetchone()
                print(user_result)

                if user_result:
                    mapped_user_id = user_result["id"]

                    # Insert into mapping table
                    insert_query = """
                        INSERT INTO user_name_mapping (ai_name, mapped_name, mapped_user_id)
                        VALUES (%s, %s, %s)
                        RETURNING id;
                    """
                    cursor.execute(insert_query, (ai_name, mapped_name, mapped_user_id))
                    mapping_results.append(cursor.fetchone())

            conn.commit()  # Save changes

            return {"status": "mapping_created", "mappings": mapping_results}

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
