"""
Module management routes
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.config.database import get_sso_db
from app.middleware.auth import get_current_user
from app.models.sso.module import Module
from app.models.sso.tool import Tool
from app.models.sso.user_details import UserDetails
from pydantic import BaseModel
from typing import List, Optional
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


class CreateModuleRequest(BaseModel):
    module_name: str
    tool_id: int
    id: Optional[int] = None  # For update operations


class GetAllModulesRequest(BaseModel):
    tool_id: int
    organization_id: Optional[int] = None


class DeleteModuleRequest(BaseModel):
    id: int


class ModuleResponse(BaseModel):
    id: int
    module_name: str
    tool_id: int
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@router.post("/createModule")
async def create_module(
    module_data: CreateModuleRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Create or update a module"""
    try:
        if not module_data.module_name or not module_data.tool_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Module name and tool ID are required"
            )
        
        # Check if tool exists
        tool = await Tool.get_by_id(db, module_data.tool_id)
        if not tool:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Tool not found"
            )
        
        # Check for duplicates
        is_duplicate = await Module.check_duplicate(db, module_data.module_name, module_data.tool_id)
        if is_duplicate:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Module already exists"
            )
        
        if module_data.id:
            # Update existing module
            module = await Module.update(db, module_data.id,
                module_name=module_data.module_name,
                tool_id=module_data.tool_id
            )
        else:
            # Create new module
            module = await Module.create(db,
                module_name=module_data.module_name,
                tool_id=module_data.tool_id
            )
        
        return ModuleResponse(
            id=module.id,
            module_name=module.module_name,
            tool_id=module.tool_id,
            created_at=module.created_at.isoformat() if module.created_at else None,
            updated_at=module.updated_at.isoformat() if module.updated_at else None
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create module error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating module"
        )


@router.post("/getAllModules")
async def get_all_modules(
    request_data: GetAllModulesRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get all modules"""
    try:
        modules = await Module.get_all_by_tool(
            db, request_data.tool_id, request_data.organization_id
        )
        
        # Format response
        module_list = []
        for module in modules:
            module_list.append({
                "id": module.id,
                "module_name": module.module_name,
                "tool_id": module.tool_id,
                "created_at": module.created_at.isoformat() if module.created_at else None,
                "updated_at": module.updated_at.isoformat() if module.updated_at else None,
                "tools": {
                    "id": module.tool.id if module.tool else None,
                    "tool_name": module.tool.tool_name if module.tool else None
                } if module.tool else None,
                "permissions": [permission.to_dict() for permission in module.permissions] if hasattr(module, 'permissions') else [],
                "organization_tool": [org_tool.to_dict() for org_tool in module.organization_tools] if hasattr(module, 'organization_tools') else []
            })
        
        return {
            "Message": "Modules fetched successfully",
            "Status": 200,
            "Data": module_list
        }
        
    except Exception as e:
        logger.error(f"Get modules error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching modules"
        )


@router.post("/deleteModule")
async def delete_module(
    delete_data: DeleteModuleRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Delete module"""
    try:
        module = await Module.get_by_id(db, delete_data.id)
        
        if not module:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Module not found"
            )
        
        # Check if module has permissions
        from app.models.sso import Permission
        existing_permissions = await Permission.get_by_module_id(db, delete_data.id)
        if existing_permissions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot delete module. Module has active permissions. Please remove permissions first."
            )
        
        await Module.delete(db, delete_data.id)
        return {"message": "Module deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete module error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error deleting module"
        )