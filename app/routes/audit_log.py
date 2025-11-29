"""
Audit Log management routes
"""

from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.ext.asyncio import AsyncSession
from app.config.database import get_sso_db
from app.middleware.auth import get_current_user
from app.models.sso.audit_log import AuditLog
from app.models.sso.user_details import UserDetails
from app.models.sso.group import Group

async def get_group_name(db: AsyncSession, group_id: int) -> str:
    """Helper function to get group name by ID"""
    if not group_id:
        return None
    try:
        group = await Group.get_by_id(db, group_id)
        return group.group_name if group else "Unknown"
    except:
        return "Unknown"
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


class CreateLogRequest(BaseModel):
    username: str
    user_email: Optional[str] = None
    role: Optional[str] = None
    action: str
    action_details: Optional[str] = None
    request: Optional[str] = None
    remarks: Optional[str] = None


class GetAuditLogsRequest(BaseModel):
    startDate: str
    endDate: str
    username: Optional[str] = None
    action: Optional[str] = None


class AuditLogResponse(BaseModel):
    id: int
    username: str
    user_email: Optional[str] = None
    system_ip: Optional[str] = None
    role: Optional[str] = None
    action: str
    action_details: Optional[str] = None
    request: Optional[str] = None
    remarks: Optional[str] = None
    created_at: Optional[str] = None


@router.post("/create")
async def create_log(
    log_data: CreateLogRequest,
    request: Request,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Create audit log entry"""
    try:
        # Get client IP
        client_ip = (
            request.headers.get("x-forwarded-for", "").split(",")[0].strip() or
            request.client.host if request.client else None
        )
        
        # Create audit log entry
        audit_log = await AuditLog.create(db,
            username=log_data.username,
            user_email=log_data.user_email,
            system_ip=client_ip,
            role=log_data.role,
            action=log_data.action,
            action_details=log_data.action_details,
            request=log_data.request,
            remarks=log_data.remarks
        )
        
        return {"success": True}
        
    except Exception as e:
        logger.error(f"Create audit log error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating audit log"
        )


@router.post("/list")
async def get_audit_logs(
    request_data: GetAuditLogsRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get audit logs with filters"""
    try:
        # Parse dates
        start_date = datetime.fromisoformat(request_data.startDate.replace('Z', '+00:00'))
        end_date = datetime.fromisoformat(request_data.endDate.replace('Z', '+00:00'))
        
        # Set end date to end of day
        end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999000)
        
        # Determine username filter
        username_filter = request_data.username
        if not username_filter and current_user.role_name == 2:
            username_filter = current_user.username
        
        # Get audit logs
        logs = await AuditLog.get_all_with_filters(
            db, start_date, end_date, username_filter, request_data.action
        )
        
        # Format response
        log_list = []
        for log in logs:
            log_list.append({
                "id": log.id,
                "username": log.username,
                "user_email": log.user_email,
                "system_ip": log.system_ip,
                "role": log.role,
                "action": log.action,
                "action_details": log.action_details,
                "request": log.request,
                "remarks": log.remarks,
                "created_at": log.created_at.isoformat() if log.created_at else None,
                "user_details": {
                    "id": current_user.id,
                    "username": current_user.username,
                    "name": current_user.name
                }
            })
        
        return {
            "message": "Activity logs fetched successfully",
            "status": 200,
            "Data": log_list
        }
        
    except Exception as e:
        logger.error(f"Get audit logs error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching activity logs"
        )


@router.get("/user/list")
async def get_all_organization_users(
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get all organization users"""
    try:
        # Get users by organization
        users = await UserDetails.get_all_by_organization(db, current_user.organization_id)
        
        # Format response
        user_list = []
        for user in users:
            user_list.append({
                "id": user.id,
                "username": user.username,
                "name": user.name,
                "email": user.email,
                "mobile": user.mobile,
                "active": user.active,
                "organization_id": user.organization_id,
                "group_id": user.group_id,
                "groups": {
                    "id": user.group_id,
                    "group_name": await get_group_name(db, user.group_id) if user.group_id else None
                } if user.group_id else None
            })
        
        return {
            "Message": "Users fetched successfully",
            "Status": 200,
            "Data": user_list
        }
        
    except Exception as e:
        logger.error(f"Get organization users error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching users"
        )