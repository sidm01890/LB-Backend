"""
Database Setup and Report Formulas Routes
Handles MongoDB collection setup and report formula management
"""

from fastapi import APIRouter, Depends, HTTPException, status, Body, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from io import BytesIO
import pandas as pd
import logging
import os
from app.middleware.auth import get_current_user
from app.models.sso.user_details import UserDetails

# Import controllers
from app.controllers.db_setup_controller import DBSetupController
from app.controllers.formulas_controller import FormulasController
from app.services.mongodb_service import mongodb_service

router = APIRouter()

# Initialize controllers
db_setup_controller = DBSetupController()
formulas_controller = FormulasController()


# ============================================================================
# DATABASE SETUP ROUTES
# ============================================================================

class CreateCollectionRequest(BaseModel):
    """Request model for creating a MongoDB collection"""
    collection_name: str = Field(
        ...,
        description="Name of the collection to create (will be converted to lowercase)",
        example="zomato",
        min_length=1
    )
    unique_ids: List[str] = Field(
        default_factory=list,
        description="List of field names that form unique identifiers for this collection (can be empty array)",
        example=["order_id", "order_date"]
    )


class CreateCollectionResponse(BaseModel):
    """Response model for collection creation"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message", example="Collection 'zomato' and processed collection 'zomato_processed' created successfully")
    data: Dict[str, Any] = Field(
        ...,
        description="Response data",
        example={
            "collection_name": "zomato",
            "processed_collection_name": "zomato_processed",
            "unique_ids": ["order_id", "order_date"],
            "mongodb_connected": True
        }
    )


@router.post(
    "/setup/new",
    tags=["Database Setup"],
    summary="Create new MongoDB collection",
    response_model=CreateCollectionResponse,
    status_code=status.HTTP_200_OK
)
async def create_collection(
    request: CreateCollectionRequest = Body(...),
    current_user: UserDetails = Depends(get_current_user)
):
    """Create a new MongoDB collection"""
    return await db_setup_controller.create_collection(
        request.collection_name,
        request.unique_ids
    )


class UpdateUniqueIdsRequest(BaseModel):
    """Request model for updating unique_ids"""
    collection_name: str = Field(..., description="Name of the collection", example="zomato", min_length=1)
    unique_ids: List[str] = Field(..., description="List of field names that form unique identifiers", example=["order_id", "order_date"])


class UpdateUniqueIdsResponse(BaseModel):
    """Response model for updating unique_ids"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.put(
    "/setup/new",
    tags=["Database Setup"],
    summary="Update unique_ids for a collection",
    response_model=UpdateUniqueIdsResponse,
    status_code=status.HTTP_200_OK
)
async def update_collection_unique_ids(
    request: UpdateUniqueIdsRequest = Body(...),
    current_user: UserDetails = Depends(get_current_user)
):
    """Update unique_ids for an existing collection"""
    return await db_setup_controller.update_collection_unique_ids(
        request.collection_name,
        request.unique_ids
    )


class GetUniqueIdsResponse(BaseModel):
    """Response model for getting unique_ids"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.get(
    "/setup/new/{collection_name}",
    tags=["Database Setup"],
    summary="Get unique_ids for a collection",
    response_model=GetUniqueIdsResponse,
    status_code=status.HTTP_200_OK
)
async def get_collection_unique_ids(
    collection_name: str,
    current_user: UserDetails = Depends(get_current_user)
):
    """Get unique_ids for an existing collection"""
    return await db_setup_controller.get_collection_unique_ids(collection_name)


class ListCollectionsResponse(BaseModel):
    """Response model for listing collections"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.get(
    "/setup/collections",
    tags=["Database Setup"],
    summary="List all MongoDB collections",
    response_model=ListCollectionsResponse,
    status_code=status.HTTP_200_OK
)
async def list_all_collections(
    current_user: UserDetails = Depends(get_current_user)
):
    """Get all MongoDB collection names from raw_data_collection"""
    return await db_setup_controller.list_all_collections()


class GetCollectionKeysRequest(BaseModel):
    """Request model for getting collection keys"""
    collection_name: str = Field(..., description="Name of the collection", example="zomato", min_length=1)


class GetCollectionKeysResponse(BaseModel):
    """Response model for collection keys"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.post(
    "/setup/collection/keys",
    tags=["Database Setup"],
    summary="Get unique keys from a collection",
    response_model=GetCollectionKeysResponse,
    status_code=status.HTTP_200_OK
)
async def get_collection_keys(
    request: GetCollectionKeysRequest = Body(...),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get unique keys from a MongoDB collection"""
    return await db_setup_controller.get_collection_keys(request.collection_name)


class SaveFieldMappingRequest(BaseModel):
    """Request model for saving field mapping"""
    collection_name: str = Field(..., description="Name of the collection", example="zomato", min_length=1)
    selected_fields: List[str] = Field(..., description="List of field names to use", example=["order_id", "order_amount"], min_items=1)


class SaveFieldMappingResponse(BaseModel):
    """Response model for saving field mapping"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.post(
    "/setup/collection/fields",
    tags=["Database Setup"],
    summary="Save field mapping for a collection",
    response_model=SaveFieldMappingResponse,
    status_code=status.HTTP_200_OK
)
async def save_collection_field_mapping(
    request: SaveFieldMappingRequest = Body(...),
    current_user: UserDetails = Depends(get_current_user)
):
    """Save or update field mapping for a collection"""
    return await db_setup_controller.save_collection_field_mapping(
        request.collection_name,
        request.selected_fields
    )


@router.get(
    "/setup/collection/fields/{collection_name}",
    tags=["Database Setup"],
    summary="Get field mapping for a collection",
    status_code=status.HTTP_200_OK
)
async def get_collection_field_mapping(
    collection_name: str,
    current_user: UserDetails = Depends(get_current_user)
):
    """Get field mapping for a specific collection"""
    return await db_setup_controller.get_collection_field_mapping(collection_name)


@router.get(
    "/setup/collection/fields",
    tags=["Database Setup"],
    summary="List all field mappings",
    status_code=status.HTTP_200_OK
)
async def list_all_field_mappings(
    current_user: UserDetails = Depends(get_current_user)
):
    """List all field mappings for all collections"""
    return await db_setup_controller.list_all_field_mappings()


class ListUploadedFilesResponse(BaseModel):
    """Response model for listing uploaded files"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.get(
    "/setup/uploaded-files",
    tags=["Database Setup"],
    summary="List all uploaded files",
    response_model=ListUploadedFilesResponse,
    status_code=status.HTTP_200_OK
)
async def list_all_uploaded_files(
    current_user: UserDetails = Depends(get_current_user)
):
    """Get all data from the uploaded_files collection"""
    return await db_setup_controller.list_all_uploaded_files()


class HeadersStatusResponse(BaseModel):
    """Response model for headers status check"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.get(
    "/setup/collection/{collection_name}/headers-status",
    tags=["Database Setup"],
    summary="Check if headers file exists for a collection",
    response_model=HeadersStatusResponse,
    status_code=status.HTTP_200_OK
)
async def check_collection_headers_status(
    collection_name: str,
    current_user: UserDetails = Depends(get_current_user)
):
    """Check if headers file has been uploaded for a collection"""
    return await db_setup_controller.check_collection_headers_status(collection_name)


# ============================================================================
# REPORT FORMULAS ROUTES
# ============================================================================

class FormulaField(BaseModel):
    """Model for a single field in a formula"""
    type: str = Field(..., description="Field type", example="data_field")
    dataset_type: str = Field(default="Dataset", description="Dataset type")
    selectedDataSetValue: str = Field(default="", description="Selected dataset value")
    selectedFieldValue: str = Field(default="", description="Selected field value")
    customFieldValue: str = Field(default="", description="Custom field value")
    startBrackets: List[str] = Field(default_factory=list, description="Start brackets")
    endBrackets: List[str] = Field(default_factory=list, description="End brackets")
    selectedTableName: str = Field(default="", description="Selected table name")
    selectedTableColumn: str = Field(default="", description="Selected table column")


class ConditionItem(BaseModel):
    """Model for a single condition"""
    column: str = Field(..., description="Column name", example="order_id")
    operator: str = Field(..., description="Comparison operator", example="not_equal")
    value: Any = Field(..., description="Value to compare against", example="NULL")


class FormulaConditionItem(BaseModel):
    """Model for a single formula condition"""
    conditionType: str = Field(..., description="Type of condition", example="between")
    value1: Optional[str] = Field(default=None, description="First value")
    value2: Optional[str] = Field(default=None, description="Second value")
    formulaValue: Optional[str] = Field(default=None, description="Formula value")


class FormulaItem(BaseModel):
    """Model for a single formula"""
    id: int = Field(..., description="Formula ID", example=1)
    logicName: str = Field(..., description="Logic name", example="Total Amount")
    fields: List[FormulaField] = Field(..., description="List of fields", min_items=1)
    formulaText: str = Field(..., description="Formula text", example="zomato.zvd + zomato.merchant_pack_charge")
    logicNameKey: str = Field(..., description="Logic name key", example="TOTAL_AMOUNT")
    multipleColumn: bool = Field(default=False, description="Whether formula uses multiple columns")
    conditions: List[FormulaConditionItem] = Field(default_factory=list, description="List of conditions")
    active_group_index: int = Field(default=0, description="Active group index")
    excelFormulaText: str = Field(default="", description="Excel formula text")


class SaveReportFormulasRequest(BaseModel):
    """Request model for saving report formulas"""
    report_name: str = Field(..., description="Name of the report", example="zomato_vs_pos_summary", min_length=1)
    formulas: List[FormulaItem] = Field(..., description="List of formulas", default_factory=list)
    mapping_keys: Dict[str, List[str]] = Field(default_factory=dict, description="Mapping keys for joining collections")
    conditions: Dict[str, List[ConditionItem]] = Field(default_factory=dict, description="Conditions to filter data")


class SaveReportFormulasResponse(BaseModel):
    """Response model for saving report formulas"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.post(
    "/reports/formulas",
    tags=["Report Formulas"],
    summary="Save report formulas",
    response_model=SaveReportFormulasResponse,
    status_code=status.HTTP_200_OK
)
async def save_report_formulas(
    request: SaveReportFormulasRequest = Body(...),
    current_user: UserDetails = Depends(get_current_user)
):
    """Save report formulas to a MongoDB collection"""
    # Convert FormulaItem Pydantic models to dictionaries
    formulas_dict = [f.model_dump() for f in request.formulas]
    # Convert ConditionItem Pydantic models to dictionaries
    conditions_dict = {
        key: [c.model_dump() for c in conditions_list]
        for key, conditions_list in request.conditions.items()
    }
    
    return await formulas_controller.save_report_formulas(
        request.report_name,
        formulas_dict,
        request.mapping_keys,
        conditions_dict
    )


class GetReportFormulasResponse(BaseModel):
    """Response model for getting report formulas"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.get(
    "/reports/{report_name}",
    tags=["Report Formulas"],
    summary="Get report formulas",
    response_model=GetReportFormulasResponse,
    status_code=status.HTTP_200_OK
)
async def get_report_formulas(
    report_name: str,
    current_user: UserDetails = Depends(get_current_user)
):
    """Get report formulas by report name"""
    return await formulas_controller.get_report_formulas(report_name)


class UpdateReportFormulasRequest(BaseModel):
    """Request model for updating report formulas"""
    formulas: List[FormulaItem] = Field(..., description="List of formulas", default_factory=list)
    mapping_keys: Dict[str, List[str]] = Field(default_factory=dict, description="Mapping keys")
    conditions: Dict[str, List[ConditionItem]] = Field(default_factory=dict, description="Conditions")


class UpdateReportFormulasResponse(BaseModel):
    """Response model for updating report formulas"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.put(
    "/reports/{report_name}/formulas",
    tags=["Report Formulas"],
    summary="Update report formulas",
    response_model=UpdateReportFormulasResponse,
    status_code=status.HTTP_200_OK
)
async def update_report_formulas(
    report_name: str,
    request: UpdateReportFormulasRequest = Body(...),
    current_user: UserDetails = Depends(get_current_user)
):
    """Update report formulas in an existing MongoDB collection"""
    # Convert FormulaItem Pydantic models to dictionaries
    formulas_dict = [f.model_dump() for f in request.formulas]
    # Convert ConditionItem Pydantic models to dictionaries
    conditions_dict = {
        key: [c.model_dump() for c in conditions_list]
        for key, conditions_list in request.conditions.items()
    }
    
    return await formulas_controller.update_report_formulas(
        report_name,
        formulas_dict,
        request.mapping_keys,
        conditions_dict
    )


class DeleteReportResponse(BaseModel):
    """Response model for deleting report formulas"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.delete(
    "/reports/{report_name}",
    tags=["Report Formulas"],
    summary="Delete report formulas",
    response_model=DeleteReportResponse,
    status_code=status.HTTP_200_OK
)
async def delete_report_collection(
    report_name: str,
    current_user: UserDetails = Depends(get_current_user)
):
    """Delete a report document from the 'formulas' collection"""
    return await formulas_controller.delete_report_collection(report_name)


class GetAllFormulasResponse(BaseModel):
    """Response model for getting all formulas"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.get(
    "/reports/formulas/all",
    tags=["Report Formulas"],
    summary="Get all report formulas",
    response_model=GetAllFormulasResponse,
    status_code=status.HTTP_200_OK
)
async def get_all_formulas(
    current_user: UserDetails = Depends(get_current_user)
):
    """Get all report formulas from the 'formulas' collection"""
    return await formulas_controller.get_all_formulas()


class GetDeltaColumnsResponse(BaseModel):
    """Response model for getting delta columns"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.get(
    "/reports/{report_name}/delta-columns",
    tags=["Report Formulas"],
    summary="Get delta columns",
    response_model=GetDeltaColumnsResponse,
    status_code=status.HTTP_200_OK
)
async def get_delta_columns(
    report_name: str,
    current_user: UserDetails = Depends(get_current_user)
):
    """Get delta columns by report name"""
    return await formulas_controller.get_delta_columns(report_name)


class DeltaColumnItem(BaseModel):
    """Model for a single delta column"""
    delta_column_name: str = Field(..., description="Name of the delta column", example="net_amount_delta")
    first_formula: str = Field(..., description="First formula name", example="NET_AMOUNT")
    second_formula: str = Field(..., description="Second formula name", example="CALCULATED_NET_AMOUNT")
    value: str = Field(..., description="Delta calculation formula", example="NET_AMOUNT - CALCULATED_NET_AMOUNT")




class UpdateDeltaColumnsResponse(BaseModel):
    """Response model for updating delta columns"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.put(
    "/reports/{report_name}/delta-columns",
    tags=["Report Formulas"],
    summary="Update delta columns",
    response_model=UpdateDeltaColumnsResponse,
    status_code=status.HTTP_200_OK
)
async def update_delta_columns(
    report_name: str,
    delta_columns: List[DeltaColumnItem] = Body(...),
    current_user: UserDetails = Depends(get_current_user)
):
    """Update delta columns in an existing MongoDB collection"""
    # Convert DeltaColumnItem Pydantic models to dictionaries
    delta_columns_dict = [dc.model_dump() for dc in delta_columns]
    
    return await formulas_controller.update_delta_columns(
        report_name,
        delta_columns_dict
    )


class ReasonItem(BaseModel):
    """Model for a single reason"""
    reason: str = Field(..., description="Reason name", example="High Variance")
    description: str = Field(default="", description="Description of the reason", example="Difference exceeds threshold")
    delta_column: str = Field(..., description="Delta column name", example="net_amount_delta")
    threshold: float = Field(..., description="Threshold value", example=1000.0)
    must_check: bool = Field(..., description="Must check flag", example=True)


class GetReasonsResponse(BaseModel):
    """Response model for getting reasons"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.get(
    "/reports/{report_name}/reasons",
    tags=["Report Formulas"],
    summary="Get reasons",
    response_model=GetReasonsResponse,
    status_code=status.HTTP_200_OK
)
async def get_reasons(
    report_name: str,
    current_user: UserDetails = Depends(get_current_user)
):
    """Get reasons by report name"""
    return await formulas_controller.get_reasons(report_name)


class UpdateReasonsResponse(BaseModel):
    """Response model for updating reasons"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.put(
    "/reports/{report_name}/reasons",
    tags=["Report Formulas"],
    summary="Update reasons",
    response_model=UpdateReasonsResponse,
    status_code=status.HTTP_200_OK
)
async def update_reasons(
    report_name: str,
    reasons: List[ReasonItem] = Body(...),
    current_user: UserDetails = Depends(get_current_user)
):
    """Update reasons in an existing MongoDB collection"""
    # Convert ReasonItem Pydantic models to dictionaries
    reasons_dict = [r.model_dump() for r in reasons]
    
    return await formulas_controller.update_reasons(
        report_name,
        reasons_dict
    )


class GetReportCollectionKeysResponse(BaseModel):
    """Response model for getting collection keys by report name"""
    status: int = Field(..., description="HTTP status code", example=200)
    message: str = Field(..., description="Response message")
    data: Dict[str, Any] = Field(..., description="Response data")


@router.get(
    "/reports/{report_name}/collection-keys",
    tags=["Report Formulas"],
    summary="Get all keys from collection matching report name",
    response_model=GetReportCollectionKeysResponse,
    status_code=status.HTTP_200_OK
)
async def get_report_collection_keys(
    report_name: str,
    current_user: UserDetails = Depends(get_current_user)
):
    """
    Get all keys from a MongoDB collection that matches the report name.
    The report name is matched to a MongoDB collection name (case-insensitive).
    Returns all keys including _id and system fields.
    
    Example:
    - Input: zomato_vs_pos
    - Matches collection: zomato_vs_pos
    - Returns: ["_id", "zomato_mapping_key", "commission_percentage", ...]
    """
    return await formulas_controller.get_report_collection_keys(report_name)


class GenerateSummaryReportExcelRequest(BaseModel):
    """Request model for generating summary report Excel file"""
    report_name: str = Field(..., description="Name of the report", example="zomato_vs_pos", min_length=1)
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format", example="2024-01-01")
    end_date: str = Field(..., description="End date in YYYY-MM-DD format", example="2024-01-31")


@router.post(
    "/reports/generate-summary-excel",
    tags=["Report Formulas"],
    summary="Generate summary Excel file for report with auto-selected columns in sequence (async background processing)",
    status_code=status.HTTP_200_OK
)
async def generate_summary_report_excel(
    request: GenerateSummaryReportExcelRequest = Body(...),
    current_user: UserDetails = Depends(get_current_user)
):
    """
    Generate a summary Excel file for a report with automatically selected columns in sequence.
    Returns immediately with generationId. Excel generation happens in background.
    
    The summary report will:
    1. Include all base columns (non-delta columns)
    2. For each delta column: include first_formula, second_formula, delta_column_name in sequence
    3. Include reconciliation_status and reason columns at the end
    4. Create an additional "Summary" sheet with date range, total orders, and reconciliation counts
    
    Inputs:
    1. report_name: Name of the report (matches MongoDB collection)
    2. start_date: Start date (YYYY-MM-DD)
    3. end_date: End date (YYYY-MM-DD)
    
    Output:
    - Returns generationId immediately
    - Excel file generated in background with two sheets: "Report" and "Summary"
    - Status can be checked via /api/reconciliation/generation-status
    """
    logger = logging.getLogger(__name__)
    
    try:
        from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
        
        # Validate date format
        date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]
        start_date_dt = None
        end_date_dt = None
        
        for fmt in date_formats:
            try:
                start_date_dt = datetime.strptime(request.start_date, fmt).date()
                break
            except ValueError:
                continue
        
        for fmt in date_formats:
            try:
                end_date_dt = datetime.strptime(request.end_date, fmt).date()
                break
            except ValueError:
                continue
        
        if not start_date_dt or not end_date_dt:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Expected: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS"
            )
        
        if start_date_dt > end_date_dt:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Start date must be before or equal to end date"
            )
        
        collection_name = request.report_name.lower().strip()
        reports_dir = "reports"
        
        # Convert date objects to datetime for MongoDB
        start_datetime = datetime.combine(start_date_dt, datetime.min.time())
        end_datetime = datetime.combine(end_date_dt, datetime.max.time())
        
        # Create MongoDB record
        store_code_label = f"SummaryReport_{request.report_name}"
        generation_record = await ExcelGeneration.create(
            None,  # db parameter not needed for MongoDB
            store_code=store_code_label,
            start_date=start_datetime,
            end_date=end_datetime,
            status=ExcelGenerationStatus.PENDING,
            progress=0,
            message="Initializing summary report generation...",
            metadata={
                "report_name": request.report_name,
                "report_type": "summary"
            }
        )
        
        # Use multiprocessing.Process for isolation
        import multiprocessing
        from app.workers.process_worker import run_summary_report_excel_generation
        
        task_params = {
            "report_name": request.report_name,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "start_date_dt": start_date_dt.isoformat(),
            "end_date_dt": end_date_dt.isoformat(),
            "reports_dir": reports_dir
        }
        
        # Start separate process
        try:
            ctx = multiprocessing.get_context('spawn')
            process = ctx.Process(
                target=run_summary_report_excel_generation,
                args=(generation_record.id, task_params),
                daemon=False
            )
            process.start()
            logger.info(f"✅ Started multiprocessing worker for summary report generation {generation_record.id}, PID: {process.pid}")
            
            if not process.is_alive() and process.exitcode is not None:
                logger.error(f"❌ Process {generation_record.id} exited immediately with code {process.exitcode}")
                await ExcelGeneration.update_status(
                    None,
                    generation_record.id,
                    ExcelGenerationStatus.FAILED,
                    message="Process failed to start",
                    error=f"Process exited with code {process.exitcode}"
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to start summary report generation process"
                )
        except Exception as process_error:
            logger.error(f"❌ Error starting process: {process_error}", exc_info=True)
            await ExcelGeneration.update_status(
                None,
                generation_record.id,
                ExcelGenerationStatus.FAILED,
                message="Failed to start generation process",
                error=str(process_error)
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to start summary report generation: {str(process_error)}"
            )
        
        logger.info(f"✅ Summary report generation process started: {generation_record.id} (PID: {process.pid})")
        return {
            "success": True,
            "message": "Summary report generation started",
            "generationId": generation_record.id,
            "status": "PENDING"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error starting summary report generation: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start summary report generation: {str(e)}"
        )


class GenerateReportExcelRequest(BaseModel):
    """Request model for generating report Excel file"""
    report_name: str = Field(..., description="Name of the report", example="zomato_vs_pos", min_length=1)
    columns: List[str] = Field(..., description="Array of column names for the Excel file", example=["zomato_mapping_key", "commission_percentage", "net_amount"], min_items=1)
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format", example="2024-01-01")
    end_date: str = Field(..., description="End date in YYYY-MM-DD format", example="2024-01-31")


@router.post(
    "/reports/generate-excel",
    tags=["Report Formulas"],
    summary="Generate Excel file for report with specified columns and date range (async background processing)",
    status_code=status.HTTP_200_OK
)
async def generate_report_excel(
    request: GenerateReportExcelRequest = Body(...),
    current_user: UserDetails = Depends(get_current_user)
):
    """
    Generate an Excel file for a report with specified columns and date range.
    Returns immediately with generationId. Excel generation happens in background.
    
    Inputs:
    1. report_name: Name of the report (matches MongoDB collection)
    2. columns: Array of strings (column names for Excel)
    3. start_date: Start date (YYYY-MM-DD)
    4. end_date: End date (YYYY-MM-DD)
    
    Output:
    - Returns generationId immediately
    - Excel file generated in background and saved to reports/ directory
    - Status can be checked via /api/reconciliation/generation-status
    """
    logger = logging.getLogger(__name__)
    
    try:
        from app.models.main.excel_generation import ExcelGeneration, ExcelGenerationStatus
        
        # Validate date format
        date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]
        start_date_dt = None
        end_date_dt = None
        
        for fmt in date_formats:
            try:
                start_date_dt = datetime.strptime(request.start_date, fmt).date()
                break
            except ValueError:
                continue
        
        for fmt in date_formats:
            try:
                end_date_dt = datetime.strptime(request.end_date, fmt).date()
                break
            except ValueError:
                continue
        
        if not start_date_dt or not end_date_dt:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Expected: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS"
            )
        
        if start_date_dt > end_date_dt:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Start date must be before or equal to end date"
            )
        
        # Validate columns array
        if not request.columns or len(request.columns) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one column must be specified"
            )
        
        # 🔥 OPTIMIZATION: Remove collection existence check - let background worker validate
        # This check was blocking the response. Background worker will handle validation.
        collection_name = request.report_name.lower().strip()
        
        # 🔥 OPTIMIZATION: Move directory creation to background worker
        # Don't create directory here - let background worker handle it
        reports_dir = "reports"
        
        # Convert date objects to datetime for MongoDB (fast operation)
        start_datetime = datetime.combine(start_date_dt, datetime.min.time())
        end_datetime = datetime.combine(end_date_dt, datetime.max.time())
        
        # 🔥 CRITICAL: Create MongoDB record (this is the only blocking operation we need)
        # This is fast (<50ms typically) and we need the ID to return
        store_code_label = f"CustomReport_{request.report_name}"
        generation_record = await ExcelGeneration.create(
            None,  # db parameter not needed for MongoDB
            store_code=store_code_label,
            start_date=start_datetime,
            end_date=end_datetime,
            status=ExcelGenerationStatus.PENDING,
            progress=0,
            message="Initializing Excel generation...",
            metadata={
                "report_name": request.report_name,
                "columns": request.columns
            }
        )
        
        # 🔥 KEY CHANGE: Use multiprocessing.Process for TRUE isolation
        # Similar to Node.js fork() - runs in completely separate process
        # Main application is NEVER blocked - completely isolated execution
        # This ensures the main thread stays responsive for other requests
        import multiprocessing
        from app.workers.process_worker import run_report_excel_generation
        
        task_params = {
            "report_name": request.report_name,
            "columns": request.columns,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "start_date_dt": start_date_dt.isoformat(),
            "end_date_dt": end_date_dt.isoformat(),
            "reports_dir": reports_dir
        }
        
        # 🔥 MULTIPROCESSING: Start completely separate Python process
        # This is equivalent to Node.js fork() - main app continues immediately
        # The child process:
        # - Has its own memory space (not shared)
        # - Has its own CPU time (doesn't block main process)
        # - Has its own database connections (separate MongoDB connection)
        # - Runs completely independently
        # - Can process 783K+ records without blocking main app
        
        # Use 'spawn' method for better isolation (default on Windows, Mac)
        # This creates a fresh Python interpreter for the child process
        try:
            ctx = multiprocessing.get_context('spawn')
            process = ctx.Process(
                target=run_report_excel_generation,
                args=(generation_record.id, task_params),  # generation_record.id is now a string (ObjectId)
                daemon=False  # Don't kill when main process exits (let it finish)
            )
            process.start()
            logger.info(f"✅ Started multiprocessing worker for generation {generation_record.id}, PID: {process.pid}")
            
            # Don't wait for process, but check if it started successfully
            if not process.is_alive() and process.exitcode is not None:
                logger.error(f"❌ Process {generation_record.id} exited immediately with code {process.exitcode}")
                # Update status to failed
                await ExcelGeneration.update_status(
                    None,  # db parameter not needed for MongoDB
                    generation_record.id,
                    ExcelGenerationStatus.FAILED,
                    message=f"Process failed to start (exit code: {process.exitcode})",
                    error="Process worker failed to initialize"
                )
        except Exception as process_error:
            logger.error(f"❌ Failed to start multiprocessing worker: {process_error}", exc_info=True)
            # Update status to failed
            await ExcelGeneration.update_status(
                None,  # db parameter not needed for MongoDB
                generation_record.id,
                ExcelGenerationStatus.FAILED,
                message="Failed to start background process",
                error=str(process_error)
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to start background process"
            )
        
        # 🔥 IMPORTANT: Don't wait for process - return immediately
        # The process runs in completely separate memory space and CPU
        # Main application continues normally - other requests are NOT blocked
        logger.info(f"✅ Excel generation process started: {generation_record.id} (PID: {process.pid})")
        return {
            "success": True,
            "message": "Excel generation started",
            "generationId": generation_record.id,
            "status": "PENDING"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error starting Excel generation: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start Excel generation: {str(e)}"
        )

