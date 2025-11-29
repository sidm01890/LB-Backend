"""
Encryption utilities
"""

from cryptography.fernet import Fernet
from app.config.settings import settings
import base64
import logging

logger = logging.getLogger(__name__)


def encrypt_data(data: str) -> str:
    """Encrypt data using Fernet encryption"""
    try:
        # Convert secret key to bytes and create Fernet instance
        key = base64.urlsafe_b64encode(settings.secret_key.encode()[:32].ljust(32, b'0'))
        fernet = Fernet(key)
        
        # Encrypt the data
        encrypted_data = fernet.encrypt(data.encode())
        return base64.urlsafe_b64encode(encrypted_data).decode()
        
    except Exception as e:
        logger.error(f"Error encrypting data: {e}")
        raise


def decrypt_data(encrypted_data: str) -> str:
    """Decrypt data using Fernet encryption"""
    try:
        # Convert secret key to bytes and create Fernet instance
        key = base64.urlsafe_b64encode(settings.secret_key.encode()[:32].ljust(32, b'0'))
        fernet = Fernet(key)
        
        # Decode and decrypt the data
        decoded_data = base64.urlsafe_b64decode(encrypted_data.encode())
        decrypted_data = fernet.decrypt(decoded_data)
        return decrypted_data.decode()
        
    except Exception as e:
        logger.error(f"Error decrypting data: {e}")
        raise


def generate_otp(length: int = 6) -> str:
    """Generate OTP for verification"""
    import random
    import string
    
    return ''.join(random.choices(string.digits, k=length))


def hash_sensitive_data(data: str) -> str:
    """Hash sensitive data for storage"""
    import hashlib
    
    return hashlib.sha256(data.encode()).hexdigest()
