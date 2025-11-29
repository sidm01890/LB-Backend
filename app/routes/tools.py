"""
Tool management routes
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.config.database import get_sso_db
from app.middleware.auth import get_current_user
from app.models.sso.tool import Tool
from app.models.sso.user_details import UserDetails
from pydantic import BaseModel
from typing import List, Optional
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


class CreateToolRequest(BaseModel):
    tool_name: str
    tool_logo: Optional[str] = None
    tool_url: Optional[str] = None
    tool_status: Optional[int] = 1


class UpdateToolRequest(BaseModel):
    id: int
    tool_name: Optional[str] = None
    tool_logo: Optional[str] = None
    tool_url: Optional[str] = None
    tool_status: Optional[int] = None


class DeleteToolRequest(BaseModel):
    id: int


class ToolResponse(BaseModel):
    id: int
    tool_name: str
    tool_logo: Optional[str] = None
    tool_url: Optional[str] = None
    tool_status: int
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@router.post("/createTool")
async def create_tool(
    tool_data: CreateToolRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Create a new tool"""
    try:
        if not tool_data.tool_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Tool name is required"
            )
        
        # Check if tool already exists
        existing_tool = await Tool.get_by_name(db, tool_data.tool_name)
        if existing_tool:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Tool already exists"
            )
        
        # Create tool
        tool = await Tool.create(db, **tool_data.dict())
        
        return ToolResponse(
            id=tool.id,
            tool_name=tool.tool_name,
            tool_logo=tool.tool_logo,
            tool_url=tool.tool_url,
            tool_status=tool.tool_status,
            created_at=tool.created_at.isoformat() if tool.created_at else None,
            updated_at=tool.updated_at.isoformat() if tool.updated_at else None
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create tool error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating tool"
        )


@router.get("/getAllTools")
async def get_all_tools(
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get all tools"""
    try:
        tools = await Tool.get_all(db)
        
        # Format response
        tool_list = []
        for tool in tools:
            tool_list.append({
                "id": tool.id,
                "tool_name": tool.tool_name,
                "tool_logo": tool.tool_logo,
                "tool_url": tool.tool_url,
                "tool_status": tool.tool_status,
                "created_at": tool.created_at.isoformat() if tool.created_at else None,
                "updated_at": tool.updated_at.isoformat() if tool.updated_at else None,
                "modules": [module.to_dict() for module in tool.modules] if hasattr(tool, 'modules') else []
            })
        
        return {
            "message": "Tools fetched successfully",
            "status": 200,
            "Data": tool_list
        }
        
    except Exception as e:
        logger.error(f"Get tools error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching tools"
        )


@router.get("/getToolById")
async def get_tool_by_id(
    tool_id: int,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get tool by ID"""
    try:
        tool = await Tool.get_by_id(db, tool_id)
        
        if not tool:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Tool not found"
            )
        
        return ToolResponse(
            id=tool.id,
            tool_name=tool.tool_name,
            tool_logo=tool.tool_logo,
            tool_url=tool.tool_url,
            tool_status=tool.tool_status,
            created_at=tool.created_at.isoformat() if tool.created_at else None,
            updated_at=tool.updated_at.isoformat() if tool.updated_at else None
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get tool error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching tool"
        )


@router.post("/updateTool")
async def update_tool(
    update_data: UpdateToolRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Update tool"""
    try:
        tool = await Tool.get_by_id(db, update_data.id)
        
        if not tool:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Tool not found"
            )
        
        # Check if new name conflicts with existing tool
        if update_data.tool_name and update_data.tool_name != tool.tool_name:
            existing_tool = await Tool.get_by_name(db, update_data.tool_name)
            if existing_tool:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Tool name already exists"
                )
        
        # Prepare update data
        update_dict = {}
        if update_data.tool_name is not None:
            update_dict["tool_name"] = update_data.tool_name
        if update_data.tool_logo is not None:
            update_dict["tool_logo"] = update_data.tool_logo
        if update_data.tool_url is not None:
            update_dict["tool_url"] = update_data.tool_url
        if update_data.tool_status is not None:
            update_dict["tool_status"] = update_data.tool_status
        
        # Update tool
        updated_tool = await Tool.update(db, update_data.id, **update_dict)
        
        return ToolResponse(
            id=updated_tool.id,
            tool_name=updated_tool.tool_name,
            tool_logo=updated_tool.tool_logo,
            tool_url=updated_tool.tool_url,
            tool_status=updated_tool.tool_status,
            created_at=updated_tool.created_at.isoformat() if updated_tool.created_at else None,
            updated_at=updated_tool.updated_at.isoformat() if updated_tool.updated_at else None
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update tool error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating tool"
        )


@router.post("/deleteTool")
async def delete_tool(
    delete_data: DeleteToolRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Delete tool"""
    try:
        tool = await Tool.get_by_id(db, delete_data.id)
        
        if not tool:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Tool not found"
            )
        
        # Check for related modules before deletion
        from app.models.sso import Module
        existing_modules = await Module.get_by_tool_id(db, delete_data.id)
        if existing_modules:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot delete tool. Tool has active modules. Please remove modules first."
            )
        
        await Tool.delete(db, delete_data.id)
        return {"message": "Tool deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete tool error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error deleting tool"
        )