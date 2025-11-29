"""
Security configuration and utilities
"""

import bcrypt
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
from typing import Optional
from app.config.settings import settings

# Password hashing context
# Note: Using direct bcrypt for verification due to passlib/bcrypt 5.0.0 compatibility issues
# Passlib is still used for hashing new passwords
try:
    pwd_context = CryptContext(
        schemes=["bcrypt"],
        deprecated="auto",
        bcrypt__rounds=12,
        bcrypt__ident="2b"
    )
except Exception:
    # Fallback if passlib initialization fails
    pwd_context = None


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    import logging
    
    if not plain_password or not hashed_password:
        return False
    
    try:
        # Check if hash looks like a bcrypt hash (starts with $2a$, $2b$, or $2y$)
        if not hashed_password.startswith(('$2a$', '$2b$', '$2y$')):
            logging.warning(f"Invalid bcrypt hash format: {hashed_password[:20]}...")
            return False
        
        # Use direct bcrypt for verification (more reliable with bcrypt 5.0.0)
        # This avoids passlib/bcrypt compatibility issues
        password_bytes = plain_password.encode('utf-8')
        hash_bytes = hashed_password.encode('utf-8')
        
        # Bcrypt has a 72-byte limit, but our passwords are shorter
        if len(password_bytes) > 72:
            password_bytes = password_bytes[:72]
        
        # Direct bcrypt verification
        result = bcrypt.checkpw(password_bytes, hash_bytes)
        return result
        
    except ValueError as e:
        logging.error(f"Bcrypt ValueError: {e}")
        return False
    except Exception as e:
        # Log other errors for debugging
        logging.error(f"Password verification error: {type(e).__name__}: {e}")
        # Fallback to passlib if direct bcrypt fails
        if pwd_context:
            try:
                return pwd_context.verify(plain_password, hashed_password)
            except Exception:
                pass
        return False


def get_password_hash(password: str) -> str:
    """Hash a password"""
    # Bcrypt has a 72-byte limit, truncate if necessary
    password_bytes = password.encode('utf-8')
    if len(password_bytes) > 72:
        password = password_bytes[:72].decode('utf-8', errors='ignore')
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create JWT access token"""
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(hours=24)
    
    # JWT payload format
    to_encode.update({
        "exp": expire,
        "id": data.get("id"),
        "email": data.get("email"),
        "role": data.get("role"),
        "organization": data.get("organization"),
        "name": data.get("name"),
        "jti": data.get("username")     # Add jti field for username lookup
    })
    encoded_jwt = jwt.encode(to_encode, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return encoded_jwt


def verify_token(token: str) -> Optional[dict]:
    """Verify JWT token"""
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
        return payload
    except JWTError:
        return None


def create_refresh_token(data: dict) -> str:
    """Create JWT refresh token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=7)  # Refresh token valid for 7 days
    to_encode.update({"exp": expire, "type": "refresh"})
    encoded_jwt = jwt.encode(to_encode, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return encoded_jwt
