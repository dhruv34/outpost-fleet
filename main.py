import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
import numpy as np
import os

app = FastAPI(
    title="Carrier Information API",
    description="API to query carrier information by DOT number",
    version="1.0.0"
)

class CarrierResponse(BaseModel):
    DOT_NUMBER: int
    MC_NUMBER: Optional[str]
    COMPANY_NAME: str
    PHY_STREET: str
    PHY_CITY: str
    PHY_STATE: str
    PHY_ZIP: str
    PHONE: str
    CELL_PHONE: Optional[str]
    TRUCK_UNITS: int
    POWER_UNITS: int

def clean_phone_number(value):
    """Convert phone number to string and handle NaN values"""
    if pd.isna(value):
        return None
    # Convert to integer first to remove decimal places, then to string
    return str(int(float(value)))

# Load the data once when the application starts
try:
    print(f"Starting application...")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Files in directory: {os.listdir('.')}")
    print(f"PORT environment variable: {os.getenv('PORT', 'Not set')}")
    
    # List of CSV files to load
    csv_files = ['data1.csv', 'data2.csv', 'data3.csv']
    dataframes = []
    
    # Load each CSV file
    for file in csv_files:
        try:
            print(f"Loading {file}...")
            df_temp = pd.read_csv(file)
            dataframes.append(df_temp)
            print(f"Successfully loaded {file}")
        except Exception as e:
            print(f"Error loading {file}: {e}")
    
    # Combine all dataframes
    if dataframes:
        df = pd.concat(dataframes, ignore_index=True)
        print(f"Successfully combined {len(dataframes)} dataframes")
        print(f"Total records: {len(df)}")
    else:
        print("No data loaded successfully")
        df = None
        
except Exception as e:
    print(f"Error in data loading process: {e}")
    df = None

@app.get("/carrier", response_model=CarrierResponse)
async def get_carrier_by_dot(dot_number: int = Query(..., description="The DOT number to search for")):
    """
    Get carrier information by DOT number
    
    Args:
        dot_number: The DOT number to search for (query parameter)
        
    Returns:
        Carrier information if found
        
    Raises:
        HTTPException: If carrier is not found
    """
    try:
        if df is None:
            raise HTTPException(status_code=500, detail="Database not initialized")
        
        # Filter the dataframe for the specified DOT_NUMBER
        carrier_data = df[df['DOT_NUMBER'] == dot_number]
        
        if carrier_data.empty:
            raise HTTPException(status_code=404, detail=f"Carrier with DOT number {dot_number} not found")
        
        # Convert the first matching row to a dictionary
        carrier_dict = carrier_data.iloc[0].to_dict()
        
        # Clean and convert the data
        carrier_dict['DOT_NUMBER'] = int(carrier_dict['DOT_NUMBER'])
        carrier_dict['TRUCK_UNITS'] = int(carrier_dict['TRUCK_UNITS'])
        carrier_dict['POWER_UNITS'] = int(carrier_dict['POWER_UNITS'])
        carrier_dict['PHONE'] = clean_phone_number(carrier_dict['PHONE'])
        carrier_dict['CELL_PHONE'] = clean_phone_number(carrier_dict['CELL_PHONE'])
        carrier_dict['MC_NUMBER'] = None if pd.isna(carrier_dict['MC_NUMBER']) else str(carrier_dict['MC_NUMBER'])
        
        return carrier_dict
        
    except Exception as e:
        print(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def root():
    return {"message": "Use /carrier?dot_number=XXXX to search for carrier information."}

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy", "data_loaded": df is not None}