"""
Authentication routes
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from app.config.database import get_sso_db
from app.config.security import verify_password, create_access_token, verify_token, get_password_hash
from app.models.sso.user_details import UserDetails
from pydantic import BaseModel
from typing import Optional
import random
import logging
from datetime import datetime, timedelta

router = APIRouter()
security = HTTPBearer()


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    user: dict


class RegisterRequest(BaseModel):
    username: str
    email: str
    password: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class ForgotPasswordRequest(BaseModel):
    username: str
    email: str
    resend: Optional[bool] = False


class VerifyOTPRequest(BaseModel):
    username: str
    email: str
    otp: str


class ResetPasswordRequest(BaseModel):
    username: str
    email: str
    new_password: str


class UpdateSubscriptionsRequest(BaseModel):
    organization_id: Optional[int] = None
    tool_id: Optional[int] = None


@router.post("/login", response_model=LoginResponse)
async def login(
    login_data: LoginRequest,
    db: AsyncSession = Depends(get_sso_db)
):
    """User login endpoint"""
    # Find user by username
    user = await UserDetails.get_by_username(db, login_data.username)
    
    if not user or not verify_password(login_data.password, user.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )
    
    # Create access token
    access_token = create_access_token(
        data={
            "id": user.id,
            "email": user.email,
            "role": user.role_name,
            "organization": user.organization_id,
            "name": user.name,
            "username": user.username  # Add username for jti field
        }
    )
    
    return LoginResponse(
        access_token=access_token,
        token_type="bearer",
        user={
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "name": user.name,
            "role_name": user.role_name,
            "organization_id": user.organization_id,
            "group_id": user.group_id
        }
    )


@router.post("/register")
async def register(
    register_data: RegisterRequest,
    db: AsyncSession = Depends(get_sso_db)
):
    """User registration endpoint"""
    # Check if user already exists
    existing_user = await UserDetails.get_by_username(db, register_data.username)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )
    
    # Create new user
    user_data = {
        "username": register_data.username,
        "email": register_data.email,
        "password": get_password_hash(register_data.password),
        "name": f"{register_data.first_name} {register_data.last_name}".strip(),
        "active": True,
        "level": "user",  # Default level
        "role_name": 1,    # Default role
        "created_by": "system",
        "updated_by": "system"
    }
    
    user = await UserDetails.create(db, **user_data)
    
    return {"message": "User registered successfully", "user_id": user.id}


@router.post("/auth/access/token")
async def login_alias(
    login_data: LoginRequest,
    db: AsyncSession = Depends(get_sso_db)
):
    """Alias for login endpoint"""
    return await login(login_data, db)


@router.post("/update_subscriptions")
async def update_subscriptions(
    subscription_data: UpdateSubscriptionsRequest,
    db: AsyncSession = Depends(get_sso_db)
):
    """Update subscriptions - placeholder implementation"""
    # This would typically call external subscription service
    return {
        "success": True,
        "message": "Subscription update initiated"
    }


@router.post("/end_user/forgot_password")
async def forgot_password(
    forgot_data: ForgotPasswordRequest,
    db: AsyncSession = Depends(get_sso_db)
):
    """Forgot password endpoint"""
    try:
        # Find user by username and email
        user = await UserDetails.get_by_username(db, forgot_data.username)
        
        if not user or user.email != forgot_data.email:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="No user found"
            )
        
        # Check if user is locked out due to too many failed attempts
        if user.otp_attempts and user.otp_attempts >= 3:
            if user.reset_otp_expires and datetime.utcnow() < user.reset_otp_expires:
                minutes_left = int((user.reset_otp_expires - datetime.utcnow()).total_seconds() / 60)
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Too many failed attempts. Please try again in {minutes_left} minutes or contact admin."
                )
        
        # Check resend count if this is a resend request
        if forgot_data.resend:
            if user.otp_resend_count and user.otp_resend_count >= 3:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Maximum OTP resend limit (3) reached. Please contact admin."
                )
        
        # Generate 6-digit OTP
        otp = random.randint(100000, 999999)
        
        # Update user with OTP and expiration (15 minutes)
        await UserDetails.update(db, user.id, 
            reset_otp=str(otp),
            reset_otp_expires=datetime.utcnow() + timedelta(minutes=15),
            otp_resend_count=(user.otp_resend_count or 0) + 1 if forgot_data.resend else 0,
            otp_attempts=0
        )
        
        # Send email with OTP
        try:
            from app.utils.email import send_email
            await send_email(
                to_email=user.email,
                subject="Password Reset OTP",
                body=f"Your OTP for password reset is: {otp}. This OTP is valid for 15 minutes."
            )
        except Exception as email_error:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to send OTP email: {email_error}")
            # Continue with the response even if email fails
        
        return {
            "success": True,
            "message": "OTP has been sent to your email address",
            "remainingResends": 3 - ((user.otp_resend_count or 0) + 1 if forgot_data.resend else 0)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Forgot password error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during forgot password process"
        )


@router.post("/end_user/verify_otp")
async def verify_otp(
    otp_data: VerifyOTPRequest,
    db: AsyncSession = Depends(get_sso_db)
):
    """Verify OTP endpoint"""
    try:
        # Find user
        user = await UserDetails.get_by_username(db, otp_data.username)
        
        if not user or user.email != otp_data.email:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found"
            )
        
        # Check if user is locked out
        if user.otp_attempts and user.otp_attempts >= 3:
            if user.reset_otp_expires and datetime.utcnow() < user.reset_otp_expires:
                minutes_left = int((user.reset_otp_expires - datetime.utcnow()).total_seconds() / 60)
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Too many failed attempts. Please try again in {minutes_left} minutes or contact admin."
                )
        
        # Check if OTP is expired
        if not user.reset_otp_expires or datetime.utcnow() > user.reset_otp_expires:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="OTP has expired. Please request a new one."
            )
        
        # Verify OTP
        if user.reset_otp != otp_data.otp:
            # Increment attempts and update lockout if necessary
            new_attempts = (user.otp_attempts or 0) + 1
            updates = {"otp_attempts": new_attempts}
            
            if new_attempts >= 3:
                updates["reset_otp_expires"] = datetime.utcnow() + timedelta(minutes=30)
            
            await UserDetails.update(db, user.id, **updates)
            
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid OTP",
                headers={"remainingAttempts": str(3 - new_attempts)}
            )
        
        # OTP is valid - reset attempts and clear OTP
        await UserDetails.update(db, user.id,
            reset_otp=None,
            reset_otp_expires=None,
            otp_attempts=0,
            otp_resend_count=0
        )
        
        return {
            "success": True,
            "message": "OTP verified successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"OTP verification error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during OTP verification"
        )


@router.post("/end_user/reset_password")
async def reset_password(
    reset_data: ResetPasswordRequest,
    db: AsyncSession = Depends(get_sso_db)
):
    """Reset password endpoint"""
    try:
        # Find user
        user = await UserDetails.get_by_username(db, reset_data.username)
        
        if not user or user.email != reset_data.email:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found"
            )
        
        # Check if OTP was verified (reset_otp should be null after successful verification)
        if user.reset_otp is not None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Please verify OTP before resetting password"
            )
        
        # Hash the new password
        hashed_password = get_password_hash(reset_data.new_password)
        
        # Update password and reset all OTP related fields
        await UserDetails.update(db, user.id,
            password=hashed_password,
            reset_otp=None,
            reset_otp_expires=None,
            otp_attempts=0,
            otp_resend_count=0
        )
        
        return {
            "success": True,
            "message": "Password has been reset successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Reset password error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during password reset process"
        )


@router.post("/verify-token")
async def verify_user_token(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Verify JWT token"""
    token = credentials.credentials
    payload = verify_token(token)
    
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )
    
    return {"valid": True, "payload": payload}
