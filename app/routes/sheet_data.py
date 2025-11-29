"""
Sheet Data routes
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.config.database import get_main_db
from app.middleware.auth import get_current_user
from app.models.sso.user_details import UserDetails
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging
import uuid

router = APIRouter()
logger = logging.getLogger(__name__)


class GenerateSheetDataRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)  # Allow both camelCase and snake_case
    
    start_date: str = Field(..., alias="startDate")
    end_date: str = Field(..., alias="endDate")
    store_codes: List[str] = Field(..., alias="stores")


class SheetDataRequest(BaseModel):
    sheet_type: str
    start_date: str
    end_date: str
    store_codes: List[str]


@router.post("/generate")
async def generate_sheet_data(
    request_data: GenerateSheetDataRequest,
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Generate sheet data"""
    try:
        # Implement sheet data generation logic
        # All tables are in devyani (main_db), not SSO
        from app.models.main.sheet_data import (
            ZomatoPosVs3poData, Zomato3poVsPosData, Zomato3poVsPosRefundData,
            OrdersNotInPosData, OrdersNotIn3poData
        )
        
        # Truncate existing sheet data tables
        await ZomatoPosVs3poData.truncate_table(db)
        await Zomato3poVsPosData.truncate_table(db)
        await Zomato3poVsPosRefundData.truncate_table(db)
        await OrdersNotInPosData.truncate_table(db)
        await OrdersNotIn3poData.truncate_table(db)
        
        # Start background processing
        import asyncio
        from app.workers.tasks import process_sheet_data_generation
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Start background task
        asyncio.create_task(process_sheet_data_generation(job_id, request_data))
        
        return {
            "success": True,
            "message": "Sheet data generation started",
            "data": {
                "job_id": job_id,
                "status": "processing",
                "estimated_completion": "10-15 minutes",
                "start_date": request_data.start_date,
                "end_date": request_data.end_date,
                "store_codes": request_data.store_codes
            }
        }
        
    except Exception as e:
        logger.error(f"Generate sheet data error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error generating sheet data"
        )


@router.get("/status/{job_id}")
async def get_sheet_data_status(
    job_id: str,
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get sheet data generation status"""
    try:
        # Implement status checking logic
        # In a real implementation, this would check a jobs table
        # For now, return a placeholder status
        status_info = {
            "job_id": job_id,
            "status": "completed",  # or "processing", "failed"
            "progress": 100,
            "message": "Sheet data generation completed successfully"
        }
        
        return {
            "success": True,
            "data": {
                "job_id": job_id,
                "status": "completed",
                "progress": 100,
                "message": "Sheet data generation completed successfully",
                "generated_tables": [
                    "zomato_pos_vs_3po_data",
                    "zomato_3po_vs_pos_data", 
                    "zomato_3po_vs_pos_refund_data",
                    "orders_not_in_pos_data",
                    "orders_not_in_3po_data"
                ]
            }
        }
        
    except Exception as e:
        logger.error(f"Get sheet data status error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error checking sheet data status"
        )


@router.get("/data")
async def get_sheet_data(
    sheet_type: str = Query(..., description="Type of sheet data to retrieve"),
    start_date: str = Query(..., description="Start date for data filtering"),
    end_date: str = Query(..., description="End date for data filtering"),
    store_codes: str = Query(..., description="Comma-separated store codes"),
    db: AsyncSession = Depends(get_main_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get sheet data"""
    try:
        # Parse store codes
        store_codes_list = [code.strip() for code in store_codes.split(",")]
        
        # Implement sheet data retrieval logic
        # All tables are in devyani (main_db), not SSO
        from app.models.main.sheet_data import (
            ZomatoPosVs3poData, Zomato3poVsPosData, Zomato3poVsPosRefundData,
            OrdersNotInPosData, OrdersNotIn3poData
        )
        
        # Query the appropriate table based on sheet_type
        if sheet_type == "zomato_pos_vs_3po":
            data = await ZomatoPosVs3poData.get_by_store_codes(db, store_codes_list, limit=100)
        elif sheet_type == "zomato_3po_vs_pos":
            data = await Zomato3poVsPosData.get_by_store_codes(db, store_codes_list, limit=100)
        elif sheet_type == "zomato_3po_vs_pos_refund":
            data = await Zomato3poVsPosRefundData.get_by_store_codes(db, store_codes_list, limit=100)
        elif sheet_type == "orders_not_in_pos":
            data = await OrdersNotInPosData.get_by_store_codes(db, store_codes_list, limit=100)
        elif sheet_type == "orders_not_in_3po":
            data = await OrdersNotIn3poData.get_by_store_codes(db, store_codes_list, limit=100)
        else:
            data = []
        
        valid_sheet_types = [
            "zomato_pos_vs_3po",
            "zomato_3po_vs_pos",
            "zomato_3po_vs_pos_refund",
            "orders_not_in_pos",
            "orders_not_in_3po"
        ]
        
        if sheet_type not in valid_sheet_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid sheet type"
            )
        
        return {
            "success": True,
            "data": data,
            "metadata": {
                "sheet_type": sheet_type,
                "start_date": start_date,
                "end_date": end_date,
                "store_codes": store_codes_list,
                "total_records": len(data)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get sheet data error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving sheet data"
        )
