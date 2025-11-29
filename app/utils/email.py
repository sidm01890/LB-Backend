"""
Email utilities
"""

from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from app.config.settings import settings
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

# Email configuration
conf = ConnectionConfig(
    MAIL_USERNAME=settings.smtp_username,
    MAIL_PASSWORD=settings.smtp_password,
    MAIL_FROM=settings.smtp_username,
    MAIL_PORT=settings.smtp_port,
    MAIL_SERVER=settings.smtp_server,
    MAIL_FROM_NAME="Reconcii Admin",
    MAIL_STARTTLS=True,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True,
    VALIDATE_CERTS=True
)

fastmail = FastMail(conf)


async def send_email(
    subject: str,
    recipients: List[str],
    body: str,
    html_body: Optional[str] = None
):
    """Send email"""
    try:
        message = MessageSchema(
            subject=subject,
            recipients=recipients,
            body=body,
            html=html_body,
            subtype="html" if html_body else "plain"
        )
        
        await fastmail.send_message(message)
        logger.info(f"Email sent successfully to {recipients}")
        return True
        
    except Exception as e:
        logger.error(f"Error sending email: {e}")
        return False


async def send_password_reset_email(email: str, reset_token: str):
    """Send password reset email"""
    subject = "Password Reset Request"
    body = f"""
    You have requested a password reset for your Reconcii Admin account.
    
    Click the link below to reset your password:
    {reset_token}
    
    If you did not request this, please ignore this email.
    """
    
    html_body = f"""
    <html>
    <body>
        <h2>Password Reset Request</h2>
        <p>You have requested a password reset for your Reconcii Admin account.</p>
        <p>Click the link below to reset your password:</p>
        <a href="{reset_token}">Reset Password</a>
        <p>If you did not request this, please ignore this email.</p>
    </body>
    </html>
    """
    
    return await send_email(subject, [email], body, html_body)
