"""
Organization management routes
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.config.database import get_sso_db
from app.middleware.auth import get_current_user
from app.models.sso.user_details import UserDetails
from app.config.security import get_password_hash
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


class CreateOrganizationRequest(BaseModel):
    organization_unit_name: str
    organization_full_name: str
    domain_name: Optional[str] = None
    address: Optional[str] = None
    logo_url: Optional[str] = None
    status: Optional[int] = 1
    username: str
    email: str
    mobile: Optional[str] = None
    password: str


class UpdateOrganizationRequest(BaseModel):
    id: int
    organization_unit_name: Optional[str] = None
    organization_full_name: Optional[str] = None
    domain_name: Optional[str] = None
    address: Optional[str] = None
    logo_url: Optional[str] = None
    status: Optional[int] = None


class DeleteOrganizationRequest(BaseModel):
    id: int


class AssignToolsRequest(BaseModel):
    organization_id: int
    tool_ids: List[int]
    module_ids: List[int]


class GetOrganizationModulesRequest(BaseModel):
    organization_id: int


class UpdateOrganizationModuleMappingRequest(BaseModel):
    organization_id: int
    organization_module_mapping: Dict[str, List[int]]


class DashboardStatsRequest(BaseModel):
    organization_id: int
    tool_id: Optional[int] = None


class OrganizationResponse(BaseModel):
    id: int
    organization_unit_name: str
    organization_full_name: str
    domain_name: Optional[str] = None
    address: Optional[str] = None
    logo_url: Optional[str] = None
    status: int
    created_by: Optional[int] = None
    updated_by: Optional[int] = None
    created_date: Optional[str] = None
    updated_date: Optional[str] = None


@router.post("/create")
async def create_organization(
    org_data: CreateOrganizationRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Create a new organization"""
    try:
        # Check if organization unit name already exists
        # Check if organization unit name already exists
        from app.models.sso import Organization
        existing_org = await Organization.get_by_unit_name(db, org_data.organization_unit_name)
        if existing_org:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Organization unit already exists",
                headers={"field": "organization_unit_name"}
            )
        
        # Check if username already exists
        existing_user = await UserDetails.get_by_username(db, org_data.username)
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Username already exists",
                headers={"field": "username"}
            )
        
        # Check if email already exists
        existing_email = await UserDetails.get_by_email(db, org_data.email)
        if existing_email:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Email already exists",
                headers={"field": "email"}
            )
        
        # Create organization record
        organization = await Organization.create(
            db,
            organization_unit_name=org_data.organization_unit_name,
            organization_full_name=org_data.organization_full_name,
            domain_name=org_data.domain_name,
            address=org_data.address,
            logo_url=org_data.logo_url,
            status=True,
            created_by=current_user.username,
            updated_by=current_user.username
        )
        
        # Hash password
        hashed_password = get_password_hash(org_data.password)
        
        # Create admin user for the organization
        user_data = {
            "username": org_data.username,
            "password": hashed_password,
            "raw_password": org_data.password,  # Note: Not recommended in production
            "name": org_data.organization_full_name,
            "email": org_data.email,
            "mobile": org_data.mobile,
            "active": True,
            "organization_id": organization.id,
            "role_name": 1,
            "user_label": "Admin",
            "level": "1",
            "created_by": current_user.username,
            "updated_by": current_user.username
        }
        
        user = await UserDetails.create(db, **user_data)
        
        return {
            "message": "Organization created successfully",
            "organization_id": organization.id,
            "user_id": user.id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create organization error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating organization"
        )


@router.get("/all")
async def get_all_organizations(
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get all organizations"""
    try:
        # Get all organizations
        from app.models.sso import Organization
        organizations = await Organization.get_all(db, skip=0, limit=100)
        
        return {
            "message": "Organization fetched successfully",
            "status": 200,
            "Data": organizations
        }
        
    except Exception as e:
        logger.error(f"Get organizations error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving organizations"
        )


@router.post("/update")
async def update_organization(
    update_data: UpdateOrganizationRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Update organization"""
    try:
        # Implement organization update logic
        from app.models.sso import Organization
        organization = await Organization.get_by_id(db, update_data.id)
        if not organization:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Organization not found"
            )
        
        update_dict = update_data.dict(exclude_unset=True)
        update_dict['updated_by'] = current_user.username
        await Organization.update(db, update_data.id, **update_dict)
        
        return {"message": "Organization was updated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update organization error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating organization"
        )


@router.delete("/delete")
async def delete_organization(
    delete_data: DeleteOrganizationRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Delete organization"""
    try:
        # Implement organization deletion logic
        from app.models.sso import Organization, UserDetails
        
        organization = await Organization.get_by_id(db, delete_data.id)
        if not organization:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Organization not found"
            )
        
        # Check if organization has users
        org_users = await UserDetails.get_all_by_organization(db, delete_data.id)
        if org_users:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot delete organization. Organization has active users. Please remove users first."
            )
        
        await Organization.delete(db, delete_data.id)
        
        return {"message": "Organization was deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete organization error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error deleting organization"
        )


@router.post("/tools/assign")
async def assign_tools(
    assign_data: AssignToolsRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Assign tools to organization"""
    try:
        # Implement tool assignment logic
        from app.models.sso import OrganizationTool, Organization, Tool, Module
        
        # Check if organization exists
        organization = await Organization.get_by_id(db, request_data.organization_id)
        if not organization:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Organization not found"
            )
        
        # Process tool assignments
        assigned_tools = []
        for tool_assignment in request_data.tool_assignments:
            # Check if tool exists
            tool = await Tool.get_by_id(db, tool_assignment.tool_id)
            if not tool:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Tool with ID {tool_assignment.tool_id} not found"
                )
            
            # Check if module exists
            module = await Module.get_by_id(db, tool_assignment.module_id)
            if not module:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Module with ID {tool_assignment.module_id} not found"
                )
            
            # Create or update organization tool mapping
            existing_mapping = await OrganizationTool.get_by_organization_and_tool(
                db, request_data.organization_id, tool_assignment.tool_id
            )
            
            if existing_mapping:
                await OrganizationTool.update(
                    db, existing_mapping.id,
                    module_id=tool_assignment.module_id,
                    status=True,
                    updated_by=current_user.id
                )
            else:
                await OrganizationTool.create(
                    db,
                    organization_id=request_data.organization_id,
                    tool_id=tool_assignment.tool_id,
                    module_id=tool_assignment.module_id,
                    status=True,
                    created_by=current_user.id,
                    updated_by=current_user.id
                )
            
            assigned_tools.append({
                "tool_id": tool_assignment.tool_id,
                "tool_name": tool.tool_name,
                "module_id": tool_assignment.module_id,
                "module_name": module.module_name
            })
        
        return {
            "message": "Tools assigned successfully",
            "added": [],  # Placeholder
            "removed": []  # Placeholder
        }
        
    except Exception as e:
        logger.error(f"Assign tools error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error assigning tools"
        )


@router.get("/tools/{organization_id}")
async def get_organization_tools(
    organization_id: int,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get all tools for an organization"""
    try:
        # Implement organization tools query
        from app.models.sso import OrganizationTool, Tool, Module
        
        org_tools = await OrganizationTool.get_by_organization(db, organization_id)
        
        tools = []
        for org_tool in org_tools:
            tool = await Tool.get_by_id(db, org_tool.tool_id)
            module = await Module.get_by_id(db, org_tool.module_id)
            
            if tool:
                tools.append({
                    "id": org_tool.id,
                    "tool_id": tool.id,
                    "tool_name": tool.tool_name,
                    "tool_logo": tool.tool_logo,
                    "tool_url": tool.tool_url,
                    "module_id": module.id if module else None,
                    "module_name": module.module_name if module else None,
                    "status": org_tool.status,
                    "created_date": org_tool.created_date.isoformat() if org_tool.created_date else None
                })
        
        return {
            "message": "Tools fetched successfully",
            "data": tools
        }
        
    except Exception as e:
        logger.error(f"Get organization tools error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching organization tools"
        )


@router.post("/dashboard")
async def get_dashboard_stats(
    dashboard_data: DashboardStatsRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get dashboard statistics"""
    try:
        # Implement dashboard statistics logic
        from app.models.sso import UserDetails, OrganizationTool, Organization
        
        # Get organization
        organization = await Organization.get_by_id(db, request_data.organization_id)
        if not organization:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Organization not found"
            )
        
        # Get user count for organization
        users = await UserDetails.get_all_by_organization(db, request_data.organization_id)
        user_count = len(users)
        
        # Get active user count
        active_users = [user for user in users if user.active]
        active_user_count = len(active_users)
        
        # Get tool count for organization
        org_tools = await OrganizationTool.get_by_organization(db, request_data.organization_id)
        tool_count = len(org_tools)
        
        # Calculate statistics
        stats = {
            "total_users": user_count,
            "active_users": active_user_count,
            "inactive_users": user_count - active_user_count,
            "total_tools": tool_count,
            "organization_name": organization.organization_full_name,
            "organization_status": "Active" if organization.status else "Inactive"
        }
        
        return {
            "message": "Dashboard statistics fetched successfully",
            "Data": stats
        }
        
    except Exception as e:
        logger.error(f"Get dashboard stats error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving dashboard statistics"
        )


@router.post("/getOrganizationModules")
async def get_organization_modules(
    request_data: GetOrganizationModulesRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get organization modules"""
    try:
        # Implement organization modules query
        from app.models.sso import OrganizationTool, Module, Tool
        
        # Get organization tools
        org_tools = await OrganizationTool.get_by_organization(db, request_data.organization_id)
        
        modules = []
        module_ids = set()
        
        for org_tool in org_tools:
            if org_tool.module_id not in module_ids:
                module = await Module.get_by_id(db, org_tool.module_id)
                tool = await Tool.get_by_id(db, org_tool.tool_id)
                
                if module:
                    modules.append({
                        "module_id": module.id,
                        "module_name": module.module_name,
                        "tool_id": tool.id if tool else None,
                        "tool_name": tool.tool_name if tool else None,
                        "status": org_tool.status
                    })
                    module_ids.add(org_tool.module_id)
        
        return {
            "Message": "Tools mapping fetched successfully",
            "Status": 200,
            "Data": modules
        }
        
    except Exception as e:
        logger.error(f"Get organization modules error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching tools mapping"
        )


@router.post("/updateOrganizationModules")
async def update_organization_module_mapping(
    mapping_data: UpdateOrganizationModuleMappingRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Update organization module mapping"""
    try:
        if not mapping_data.organization_id or not mapping_data.organization_module_mapping:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Organization ID and organization_module are required"
            )
        
        # Implement organization module mapping update logic
        from app.models.sso import OrganizationTool, Organization, Tool, Module
        
        # Check if organization exists
        organization = await Organization.get_by_id(db, request_data.organization_id)
        if not organization:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Organization not found"
            )
        
        # Get existing mappings
        existing_mappings = await OrganizationTool.get_by_organization(db, request_data.organization_id)
        existing_mapping_ids = {mapping.id for mapping in existing_mappings}
        
        # Process new mappings
        updated_mappings = []
        for mapping_data in request_data.module_mappings:
            # Check if tool exists
            tool = await Tool.get_by_id(db, mapping_data.tool_id)
            if not tool:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Tool with ID {mapping_data.tool_id} not found"
                )
            
            # Check if module exists
            module = await Module.get_by_id(db, mapping_data.module_id)
            if not module:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Module with ID {mapping_data.module_id} not found"
                )
            
            # Check if mapping already exists
            existing_mapping = await OrganizationTool.get_by_organization_and_tool(
                db, request_data.organization_id, mapping_data.tool_id
            )
            
            if existing_mapping:
                # Update existing mapping
                await OrganizationTool.update(
                    db, existing_mapping.id,
                    module_id=mapping_data.module_id,
                    status=True,
                    updated_by=current_user.id
                )
            else:
                # Create new mapping
                await OrganizationTool.create(
                    db,
                    organization_id=request_data.organization_id,
                    tool_id=mapping_data.tool_id,
                    module_id=mapping_data.module_id,
                    status=True,
                    created_by=current_user.id,
                    updated_by=current_user.id
                )
            
            updated_mappings.append({
                "tool_id": mapping_data.tool_id,
                "tool_name": tool.tool_name,
                "module_id": mapping_data.module_id,
                "module_name": module.module_name
            })
        
        return {
            "Message": "Organization mapping updated successfully",
            "Status": 200
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update organization module mapping error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating organization module mapping"
        )
