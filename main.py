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