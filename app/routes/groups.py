"""
Group management routes
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.config.database import get_sso_db
from app.middleware.auth import get_current_user
from app.models.sso.group import Group
from app.models.sso.tool import Tool
from app.models.sso.user_details import UserDetails
from pydantic import BaseModel
from typing import List, Optional, Dict
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


class CreateGroupRequest(BaseModel):
    group_name: str
    tool_id: int
    organization_id: Optional[int] = None
    id: Optional[int] = None  # For update operations


class GetAllGroupsRequest(BaseModel):
    tool_id: int
    organization_id: int


class DeleteGroupRequest(BaseModel):
    id: int


class GetGroupModulesRequest(BaseModel):
    group_id: int


class UpdateGroupModuleMappingRequest(BaseModel):
    group_id: int
    module_permission_mapping: Dict[str, List[int]]


class GroupResponse(BaseModel):
    id: int
    group_name: str
    tool_id: int
    organization_id: Optional[int] = None
    created_by: Optional[int] = None
    updated_by: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@router.post("/createGroup")
async def create_group(
    group_data: CreateGroupRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Create or update a group"""
    try:
        if not group_data.group_name or not group_data.tool_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Group name and tool ID are required"
            )
        
        # Check if tool exists
        tool = await Tool.get_by_id(db, group_data.tool_id)
        if not tool:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Tool not found"
            )
        
        # Check for duplicates
        if group_data.id:
            # Update existing group
            existing_group = await Group.get_by_id(db, group_data.id)
            if not existing_group:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Group not found"
                )
            
            # Check if new name conflicts with other groups
            if group_data.group_name != existing_group.group_name:
                is_duplicate = await Group.check_duplicate(
                    db, group_data.group_name, group_data.tool_id, group_data.organization_id
                )
                if is_duplicate:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="Group already exists"
                    )
            
            # Update group
            group = await Group.update(db, group_data.id,
                group_name=group_data.group_name,
                tool_id=group_data.tool_id,
                organization_id=group_data.organization_id,
                created_by=current_user.id,
                updated_by=current_user.id
            )
        else:
            # Create new group
            is_duplicate = await Group.check_duplicate(
                db, group_data.group_name, group_data.tool_id, group_data.organization_id
            )
            if is_duplicate:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Group already exists"
                )
            
            group = await Group.create(db,
                group_name=group_data.group_name,
                tool_id=group_data.tool_id,
                organization_id=group_data.organization_id,
                created_by=current_user.id,
                updated_by=current_user.id
            )
        
        return GroupResponse(
            id=group.id,
            group_name=group.group_name,
            tool_id=group.tool_id,
            organization_id=group.organization_id,
            created_by=group.created_by,
            updated_by=group.updated_by,
            created_at=group.created_at.isoformat() if group.created_at else None,
            updated_at=group.updated_at.isoformat() if group.updated_at else None
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create group error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating group"
        )


@router.post("/getAllGroups")
async def get_all_groups(
    request_data: GetAllGroupsRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get all groups"""
    try:
        groups = await Group.get_all_by_tool_and_org(
            db, request_data.tool_id, request_data.organization_id
        )
        
        # Format response
        group_list = []
        for group in groups:
            group_list.append({
                "id": group.id,
                "group_name": group.group_name,
                "tool_id": group.tool_id,
                "organization_id": group.organization_id,
                "created_by": group.created_by,
                "updated_by": group.updated_by,
                "created_at": group.created_at.isoformat() if group.created_at else None,
                "updated_at": group.updated_at.isoformat() if group.updated_at else None,
                "tools": {
                    "id": group.tool.id if group.tool else None,
                    "tool_name": group.tool.tool_name if group.tool else None
                } if group.tool else None,
                "group_module_mapping": [mapping.to_dict() for mapping in group.group_module_mappings] if hasattr(group, 'group_module_mappings') else [],
                "users": []  # Users relationship would need to be implemented in UserDetails model
            })
        
        return {
            "Message": "Groups fetched successfully",
            "Status": 200,
            "Data": group_list
        }
        
    except Exception as e:
        logger.error(f"Get groups error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching groups"
        )


@router.post("/deleteGroup")
async def delete_group(
    delete_data: DeleteGroupRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Delete group"""
    try:
        group = await Group.get_by_id(db, delete_data.id)
        
        if not group:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Group not found"
            )
        
        # Check if group has users assigned
        from app.models.sso import UserDetails
        users = await UserDetails.get_all_by_organization(db, delete_data.organization_id)
        group_users = [user for user in users if user.group_id == delete_data.id]
        if group_users:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot delete group. Group has active users. Please reassign users first."
            )
        
        await Group.delete(db, delete_data.id)
        return {"message": "Group deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete group error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error deleting group"
        )


@router.post("/getGroupModules")
async def get_group_modules(
    request_data: GetGroupModulesRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get group modules"""
    try:
        # Implement group module mapping query
        from app.models.sso import GroupModuleMapping, Module, Permission
        
        group_mappings = await GroupModuleMapping.get_by_group(db, request_data.group_id)
        
        modules = []
        for mapping in group_mappings:
            module = await Module.get_by_id(db, mapping.module_id)
            permission = await Permission.get_by_id(db, mapping.permission_id)
            
            if module:
                modules.append({
                    "module_id": module.id,
                    "module_name": module.module_name,
                    "permission_id": permission.id if permission else None,
                    "permission_name": permission.permission_name if permission else None,
                    "permission_code": permission.permission_code if permission else None
                })
        
        return {
            "Message": "Group mapping fetched successfully",
            "Status": 200,
            "Data": modules
        }
        
    except Exception as e:
        logger.error(f"Get group modules error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching group mapping"
        )


@router.post("/updateGroupModuleMapping")
async def update_group_module_mapping(
    mapping_data: UpdateGroupModuleMappingRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Update group module mapping"""
    try:
        if not mapping_data.group_id or not mapping_data.module_permission_mapping:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Group ID and module permissions are required"
            )
        
        # Implement group module mapping update logic
        from app.models.sso import GroupModuleMapping, Module, Permission
        
        # Get existing mappings
        existing_mappings = await GroupModuleMapping.get_by_group(db, request_data.group_id)
        existing_mapping_ids = {mapping.id for mapping in existing_mappings}
        
        # Process new mappings
        updated_mappings = []
        for mapping_data in request_data.module_permissions:
            # Check if module exists
            module = await Module.get_by_id(db, mapping_data.module_id)
            if not module:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Module with ID {mapping_data.module_id} not found"
                )
            
            # Check if permission exists
            permission = await Permission.get_by_id(db, mapping_data.permission_id)
            if not permission:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Permission with ID {mapping_data.permission_id} not found"
                )
            
            # Check if mapping already exists
            existing_mapping = await GroupModuleMapping.get_by_group_and_module(
                db, request_data.group_id, mapping_data.module_id
            )
            
            if existing_mapping:
                # Update existing mapping
                await GroupModuleMapping.update(
                    db, existing_mapping.id,
                    permission_id=mapping_data.permission_id
                )
            else:
                # Create new mapping
                await GroupModuleMapping.create(
                    db,
                    group_id=request_data.group_id,
                    module_id=mapping_data.module_id,
                    permission_id=mapping_data.permission_id
                )
            
            updated_mappings.append({
                "module_id": mapping_data.module_id,
                "module_name": module.module_name,
                "permission_id": mapping_data.permission_id,
                "permission_name": permission.permission_name
            })
        
        return {
            "Message": "Group mapping updated successfully",
            "Status": 200
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update group module mapping error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating group module mapping"
        )