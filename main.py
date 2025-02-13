from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, validator, Field
from database import get_db_connection
from datetime import datetime

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
