# dependencies/auth.py
from fastapi import HTTPException, Depends, Header
from typing import Optional
from config import config

async def verify_token(authorization: Optional[str] = Header(None)):
    """Simple token verification"""
    if not authorization:
        raise HTTPException(
            status_code=403,
            detail="Authorization header missing"
        )
    
    try:
        token_type, token = authorization.split()
        if token_type.lower() != "bearer":
            raise HTTPException(
                status_code=403,
                detail="Invalid token type. Use Bearer"
            )

        # Simple token check from env
        if token != config.API_TOKEN:
            raise HTTPException(
                status_code=403,
                detail="Invalid token"
            )
            
        return token

    except ValueError:
        raise HTTPException(
            status_code=403,
            detail="Invalid authorization header format"
        )
    except Exception as e:
        raise HTTPException(
            status_code=403,
            detail="Authorization failed"
        )