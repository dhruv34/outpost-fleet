import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, Dict, Any
import numpy as np
import os

app = FastAPI(
    title="Get container information",
    description="API to query container information by container number or BOL and consignee",
    version="1.0.0",
)

# Load Happy_ROBOT data
try:
    print("Loading Happy_ROBOT data...")
    # Load both CSV files
    df1 = pd.read_csv("HAPPY_ROBOT_PROCESSED_726.csv")
    df2 = pd.read_csv("HAPPY_ROBOT_PROCESSED_726_part2.csv")

    # Combine both dataframes
    happy_robot_df = pd.concat([df1, df2], ignore_index=True)
    print(
        f"Successfully loaded Happy_ROBOT data with {len(happy_robot_df)} total records"
    )
except Exception as e:
    print(f"Error loading Happy_ROBOT data: {e}")
    happy_robot_df = None


@app.get("/search-container")
async def search_container(
    container_number: Optional[str] = Query(
        None, description="Container number to search for"
    ),
    bol: Optional[str] = Query(None, description="Bill of Lading number to search for"),
    consignee: str = Query(..., description="Consignee to search for"),
):
    """
    Search for container information using either container number and consignee or BOL and consignee

    Args:
        container_number: Optional container number
        bol: Optional Bill of Lading number
        consignee: Consignee name

    Returns:
        Complete container information if found, or error message if not found
    """
    try:
        if happy_robot_df is None:
            raise HTTPException(status_code=500, detail="Database not initialized")

        # Convert consignee to uppercase for case-insensitive search
        consignee = consignee.upper()

        # Filter by consignee first
        filtered_df = happy_robot_df[
            happy_robot_df["CONSIGNEE"].str.upper() == consignee
        ]

        if filtered_df.empty:
            return {"message": "could not find container"}

        # If container number is provided, search by container number
        if container_number:
            result = filtered_df[filtered_df["CONTAINER_NUMBER"] == container_number]
        # If BOL is provided, search by BOL
        elif bol:
            result = filtered_df[filtered_df["BOL"] == bol]
        else:
            return {"message": "Please provide either container_number or bol"}

        if result.empty:
            return {"message": "could not find container"}

        # Return the entire row as a dictionary
        return result.iloc[0].to_dict()

    except Exception as e:
        print(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/")
async def root():
    return {
        "message": "Use /search-container?container_number=XXXX&consignee=YYYY or /search-container?bol=XXXX&consignee=YYYY to search for container information."
    }


# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy", "data_loaded": happy_robot_df is not None}