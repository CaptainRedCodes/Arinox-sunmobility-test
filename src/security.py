import os
from fastapi import Request, HTTPException


async def verify_api_key(request: Request):
    api_key = request.headers.get("X-API-Key")
    expected = os.getenv("API_KEY")
    if not expected:
        raise HTTPException(status_code=500, detail="API_KEY not configured")
    if api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")
