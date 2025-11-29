"""
Permission management routes
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.config.database import get_sso_db
from app.middleware.auth import get_current_user
from app.models.sso.permission import Permission
from app.models.sso.module import Module
from app.models.sso.tool import Tool
from app.models.sso.user_details import UserDetails
from pydantic import BaseModel
from typing import List, Optional
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


class CreatePermissionRequest(BaseModel):
    permission_name: str
    permission_code: str
    module_id: int
    tool_id: int
    id: Optional[int] = None  # For update operations


class GetAllPermissionsRequest(BaseModel):
    module_id: int


class DeletePermissionRequest(BaseModel):
    id: int


class PermissionResponse(BaseModel):
    id: int
    permission_name: str
    permission_code: str
    module_id: int
    tool_id: int
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@router.post("/createPermission")
async def create_permission(
    permission_data: CreatePermissionRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Create or update a permission"""
    try:
        if not permission_data.permission_name or not permission_data.permission_code or not permission_data.module_id or not permission_data.tool_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Permission name, code, module ID, and tool ID are required"
            )
        
        # Check if module and tool exist
        module = await Module.get_by_id(db, permission_data.module_id)
        tool = await Tool.get_by_id(db, permission_data.tool_id)
        
        if not module:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Module not found"
            )
        
        if not tool:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Tool not found"
            )
        
        # Check for duplicates
        is_duplicate = await Permission.check_duplicate(
            db, permission_data.permission_name, permission_data.permission_code,
            permission_data.module_id, permission_data.tool_id
        )
        if is_duplicate:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Permission already exists"
            )
        
        if permission_data.id:
            # Update existing permission
            permission = await Permission.update(db, permission_data.id,
                permission_name=permission_data.permission_name,
                permission_code=permission_data.permission_code,
                module_id=permission_data.module_id,
                tool_id=permission_data.tool_id
            )
        else:
            # Create new permission
            permission = await Permission.create(db,
                permission_name=permission_data.permission_name,
                permission_code=permission_data.permission_code,
                module_id=permission_data.module_id,
                tool_id=permission_data.tool_id
            )
        
        return PermissionResponse(
            id=permission.id,
            permission_name=permission.permission_name,
            permission_code=permission.permission_code,
            module_id=permission.module_id,
            tool_id=permission.tool_id,
            created_at=permission.created_at.isoformat() if permission.created_at else None,
            updated_at=permission.updated_at.isoformat() if permission.updated_at else None
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create permission error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating permission"
        )


@router.post("/getAllPermissions")
async def get_all_permissions(
    request_data: GetAllPermissionsRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get all permissions"""
    try:
        permissions = await Permission.get_all_by_module(db, request_data.module_id)
        
        # Format response
        permission_list = []
        for permission in permissions:
            permission_list.append({
                "id": permission.id,
                "permission_name": permission.permission_name,
                "permission_code": permission.permission_code,
                "module_id": permission.module_id,
                "tool_id": permission.tool_id,
                "created_at": permission.created_at.isoformat() if permission.created_at else None,
                "updated_at": permission.updated_at.isoformat() if permission.updated_at else None,
                "modules": {
                    "id": permission.module.id if permission.module else None,
                    "module_name": permission.module.module_name if permission.module else None
                } if permission.module else None,
                "tools": {
                    "id": permission.tool.id if permission.tool else None,
                    "tool_name": permission.tool.tool_name if permission.tool else None
                } if permission.tool else None
            })
        
        return {
            "Message": "Permission fetched successfully",
            "Status": 200,
            "Data": permission_list
        }
        
    except Exception as e:
        logger.error(f"Get permissions error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching permissions"
        )


@router.post("/deletePermission")
async def delete_permission(
    delete_data: DeletePermissionRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Delete permission"""
    try:
        permission = await Permission.get_by_id(db, delete_data.id)
        
        if not permission:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Permission not found"
            )
        
        # Check if permission is assigned to someone
        from app.models.sso import UserModuleMapping, GroupModuleMapping
        user_assignments = await UserModuleMapping.get_by_user_and_permission(db, None, delete_data.id)
        group_assignments = await GroupModuleMapping.get_by_group_and_module(db, None, None)
        
        # Filter group assignments that use this permission
        permission_assignments = [assignment for assignment in group_assignments if assignment.permission_id == delete_data.id]
        
        if user_assignments or permission_assignments:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot delete permission. Permission is assigned to users or groups. Please remove assignments first."
            )
        
        await Permission.delete(db, delete_data.id)
        return {"message": "Permission deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete permission error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error deleting permission"
        )