"""
Database Setup and Report Formulas Routes
Handles MongoDB collection setup and report formula management
"""

from fastapi import APIRouter, Depends, HTTPException, status, Body, Query
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from app.middleware.auth import get_current_user
from app.models.sso.user_details import UserDetails

# Import controllers
from app.controllers.db_setup_controller import DBSetupController
from app.controllers.formulas_controller import FormulasController

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

