"""
User management routes
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


class CreateUserRequest(BaseModel):
    username: str
    password: str
    name: str
    email: str
    mobile: Optional[str] = None
    organization_id: int
    group_id: Optional[int] = None
    role_name: Optional[int] = 1
    user_label: Optional[str] = "user"
    level: Optional[str] = "1"


class UpdateUserRequest(BaseModel):
    id: int
    name: Optional[str] = None
    mobile: Optional[str] = None
    organization_id: Optional[int] = None
    group_id: Optional[int] = None
    active: Optional[bool] = None


class UpdatePasswordRequest(BaseModel):
    id: int
    password: str


class GetAllUsersRequest(BaseModel):
    organization_id: int


class GetUserModulesRequest(BaseModel):
    user_id: int


class UpdateUserModuleMappingRequest(BaseModel):
    user_id: int
    module_permission_mapping: Dict[str, List[int]]


class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    name: Optional[str] = None
    mobile: Optional[str] = None
    active: bool
    organization_id: Optional[int] = None
    group_id: Optional[int] = None
    role_name: Optional[int] = None
    user_label: Optional[str] = None
    level: Optional[str] = None
    created_date: Optional[str] = None
    updated_date: Optional[str] = None


@router.post("/createUser")
async def create_user(
    user_data: CreateUserRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Create new user"""
    try:
        # Validate required fields
        if not user_data.username or not user_data.password or not user_data.name or not user_data.email or not user_data.organization_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Required fields missing"
            )
        
        # Check if username already exists
        existing_user = await UserDetails.get_by_username(db, user_data.username)
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Username already exists",
                headers={"field": "username"}
            )
        
        # Check if email already exists
        existing_email = await UserDetails.get_by_email(db, user_data.email)
        if existing_email:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Email already exists",
                headers={"field": "email"}
            )
        
        # Hash password
        hashed_password = get_password_hash(user_data.password)
        
        # Create user
        user_dict = user_data.dict()
        user_dict["password"] = hashed_password
        user_dict["raw_password"] = user_data.password  # Note: Not recommended in production
        user_dict["active"] = True
        user_dict["created_by"] = current_user.username
        user_dict["updated_by"] = current_user.username
        
        user = await UserDetails.create(db, **user_dict)
        
        # Remove sensitive data from response
        user_response = UserResponse(
            id=user.id,
            username=user.username,
            email=user.email,
            name=user.name,
            mobile=user.mobile,
            active=user.active,
            organization_id=user.organization_id,
            group_id=user.group_id,
            role_name=user.role_name,
            user_label=user.user_label,
            level=user.level,
            created_date=user.created_date.isoformat() if user.created_date else None,
            updated_date=user.updated_date.isoformat() if user.updated_date else None
        )
        
        return {
            "message": "User created successfully",
            "user": user_response
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create user error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating user"
        )


@router.post("/updateUser")
async def update_user(
    update_data: UpdateUserRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Update user"""
    try:
        user = await UserDetails.get_by_id(db, update_data.id)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Prepare update data
        update_dict = {}
        if update_data.name is not None:
            update_dict["name"] = update_data.name
        if update_data.mobile is not None:
            update_dict["mobile"] = update_data.mobile
        if update_data.organization_id is not None:
            update_dict["organization_id"] = update_data.organization_id
        if update_data.group_id is not None:
            update_dict["group_id"] = update_data.group_id
        if update_data.active is not None:
            update_dict["active"] = update_data.active
        
        update_dict["updated_by"] = current_user.username
        
        # Update user
        updated_user = await UserDetails.update(db, update_data.id, **update_dict)
        
        # Remove sensitive data from response
        user_response = UserResponse(
            id=updated_user.id,
            username=updated_user.username,
            email=updated_user.email,
            name=updated_user.name,
            mobile=updated_user.mobile,
            active=updated_user.active,
            organization_id=updated_user.organization_id,
            group_id=updated_user.group_id,
            role_name=updated_user.role_name,
            user_label=updated_user.user_label,
            level=updated_user.level,
            created_date=updated_user.created_date.isoformat() if updated_user.created_date else None,
            updated_date=updated_user.updated_date.isoformat() if updated_user.updated_date else None
        )
        
        return {
            "message": "User updated successfully",
            "user": user_response
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update user error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating user"
        )


@router.post("/deleteUser")
async def delete_user(
    user_id: int,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Delete user"""
    try:
        user = await UserDetails.get_by_id(db, user_id)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Check for dependencies before deletion
        from app.models.sso import UserModuleMapping, AuditLog
        
        # Check if user has module mappings
        user_mappings = await UserModuleMapping.get_by_user(db, user_id)
        if user_mappings:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot delete user. User has active module mappings. Please remove mappings first."
            )
        
        # Check if user has audit logs (optional - you might want to keep audit logs)
        # audit_logs = await AuditLog.get_by_username(db, user.username)
        # if audit_logs:
        #     logger.warning(f"User {user.username} has {len(audit_logs)} audit log entries")
        
        await UserDetails.delete(db, user_id)
        return {"message": "User deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete user error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error deleting user"
        )


@router.post("/updatePassword")
async def update_password(
    password_data: UpdatePasswordRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Update password"""
    try:
        if not password_data.id or not password_data.password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User ID and password are required"
            )
        
        user = await UserDetails.get_by_id(db, password_data.id)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Hash new password
        hashed_password = get_password_hash(password_data.password)
        
        # Update password
        await UserDetails.update(db, password_data.id,
            password=hashed_password,
            raw_password=password_data.password,  # Note: Not recommended in production
            updated_by=current_user.username
        )
        
        return {"message": "Password changed successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Change password error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error changing password"
        )


@router.post("/getAllUsers")
async def get_all_users(
    request_data: GetAllUsersRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get all users by organization"""
    try:
        # Get users by organization_id
        users = await UserDetails.get_all_by_organization(db, request_data.organization_id)
        
        # Format response
        user_list = []
        for user in users:
            user_list.append({
                "id": user.id,
                "username": user.username,
                "email": user.email,
                "name": user.name,
                "mobile": user.mobile,
                "active": user.active,
                "organization_id": user.organization_id,
                "group_id": user.group_id
            })
        
        return {
            "Message": "Users fetched successfully",
            "Status": 200,
            "Data": user_list
        }
        
    except Exception as e:
        logger.error(f"Get users error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching users"
        )


@router.post("/getUserModules")
async def get_user_modules(
    request_data: GetUserModulesRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Get user modules"""
    try:
        # Get user module mappings
        from app.models.sso import UserModuleMapping, Module, Permission
        
        user_mappings = await UserModuleMapping.get_by_user(db, request_data.user_id)
        
        user_modules = []
        for mapping in user_mappings:
            module = await Module.get_by_id(db, mapping.module_id)
            permission = await Permission.get_by_id(db, mapping.permission_id)
            
            if module:
                user_modules.append({
                    "module_id": module.id,
                    "module_name": module.module_name,
                    "permission_id": permission.id if permission else None,
                    "permission_name": permission.permission_name if permission else None,
                    "permission_code": permission.permission_code if permission else None,
                    "is_active": mapping.is_active
                })
        
        return {
            "Message": "User mapping fetched successfully",
            "Status": 200,
            "Data": user_modules
        }
        
    except Exception as e:
        logger.error(f"Get user mapping error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching user mapping"
        )


@router.post("/updateUserModuleMapping")
async def update_user_module_mapping(
    mapping_data: UpdateUserModuleMappingRequest,
    db: AsyncSession = Depends(get_sso_db),
    current_user: UserDetails = Depends(get_current_user)
):
    """Update user module mapping"""
    try:
        if not mapping_data.user_id or not mapping_data.module_permission_mapping:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User ID and module permissions are required"
            )
        
        # Implement user module mapping update logic
        from app.models.sso import UserModuleMapping, Module, Permission
        
        # Get existing mappings
        existing_mappings = await UserModuleMapping.get_by_user(db, request_data.user_id)
        existing_mapping_ids = {mapping.id for mapping in existing_mappings}
        
        # Process new mappings
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
            existing_mapping = await UserModuleMapping.get_by_user_and_module(
                db, request_data.user_id, mapping_data.module_id
            )
            
            if existing_mapping:
                # Update existing mapping
                await UserModuleMapping.update(
                    db, existing_mapping.id,
                    permission_id=mapping_data.permission_id,
                    is_active=True
                )
            else:
                # Create new mapping
                await UserModuleMapping.create(
                    db,
                    user_id=request_data.user_id,
                    module_id=mapping_data.module_id,
                    permission_id=mapping_data.permission_id,
                    is_active=True
                )
        
        return {
            "message": "User module mapping updated successfully",
            "user_id": request_data.user_id,
            "updated_mappings": len(request_data.module_permissions)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update user module mapping error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating user module mapping"
        )
