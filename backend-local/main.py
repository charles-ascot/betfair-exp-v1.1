from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://betfair1.thync.online", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

HISTORIC_DATA_BASE = "https://historicdata.betfair.com/api"

# Global HTTP client with cookie jar (persists session)
http_client = None

@app.on_event("startup")
async def startup_event():
    global http_client
    http_client = httpx.AsyncClient(
        timeout=120.0,
        follow_redirects=True,
        cookies=httpx.Cookies()
    )
    logger.info("🎰 Betfair Historic Data Explorer Starting...")
    logger.info(f"Historic Data API Base: {HISTORIC_DATA_BASE}")

@app.on_event("shutdown")
async def shutdown_event():
    global http_client
    if http_client:
        await http_client.aclose()

@app.post("/api/GetMyData")
async def get_my_data(data: dict):
    """Authenticate with ssoid token and get user packages"""
    try:
        ssoid = data.get("ssoid")
        if not ssoid:
            raise HTTPException(status_code=400, detail="Missing ssoid")
        
        headers = {
            "ssoid": ssoid,
            "Content-Type": "application/json"
        }
        
        logger.info(f"GetMyData - Authenticating with ssoid")
        
        response = await http_client.get(
            f"{HISTORIC_DATA_BASE}/GetMyData",
            headers=headers
        )
        
        logger.info(f"GetMyData response: {response.status_code}")
        
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text[:200])
        
        return response.json()
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GetMyData error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/getCollectionOptions")
async def get_collection_options(filter_data: dict):
    """Get available collection options for filtering"""
    try:
        ssoid = filter_data.get("ssoid")
        if not ssoid:
            raise HTTPException(status_code=400, detail="Missing ssoid")
        
        headers = {
            "ssoid": ssoid,
            "Content-Type": "application/json"
        }
        
        request_body = {
            "sport": filter_data.get("sport", "Horse Racing"),
            "plan": filter_data.get("plan", "Basic Plan"),
            "fromDay": int(filter_data.get("fromDay", 1)),
            "fromMonth": int(filter_data.get("fromMonth", 1)),
            "fromYear": int(filter_data.get("fromYear", 2024)),
            "toDay": int(filter_data.get("toDay", 31)),
            "toMonth": int(filter_data.get("toMonth", 12)),
            "toYear": int(filter_data.get("toYear", 2024)),
            "eventId": None,
            "eventName": None,
            "marketTypesCollection": filter_data.get("marketTypesCollection", []),
            "countriesCollection": filter_data.get("countriesCollection", []),
            "fileTypeCollection": filter_data.get("fileTypeCollection", [])
        }
        
        logger.info(f"GetCollectionOptions request")
        
        response = await http_client.post(
            f"{HISTORIC_DATA_BASE}/GetCollectionOptions",
            headers=headers,
            json=request_body
        )
        
        logger.info(f"GetCollectionOptions response: {response.status_code}")
        logger.debug(f"Response body (first 500 chars): {response.text[:500]}")
        
        if response.status_code != 200:
            logger.error(f"GetCollectionOptions returned {response.status_code}")
            raise HTTPException(status_code=response.status_code, detail=response.text[:200])
        
        return response.json()
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GetCollectionOptions error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/getAdvBasketDataSize")
async def get_adv_basket_data_size(filter_data: dict):
    """Calculate file count and size"""
    try:
        ssoid = filter_data.get("ssoid")
        if not ssoid:
            raise HTTPException(status_code=400, detail="Missing ssoid")
        
        headers = {
            "ssoid": ssoid,
            "Content-Type": "application/json"
        }
        
        request_body = {
            "sport": filter_data.get("sport", "Horse Racing"),
            "plan": filter_data.get("plan", "Basic Plan"),
            "fromDay": int(filter_data.get("fromDay", 1)),
            "fromMonth": int(filter_data.get("fromMonth", 1)),
            "fromYear": int(filter_data.get("fromYear", 2024)),
            "toDay": int(filter_data.get("toDay", 31)),
            "toMonth": int(filter_data.get("toMonth", 12)),
            "toYear": int(filter_data.get("toYear", 2024)),
            "eventId": None,
            "eventName": None,
            "marketTypesCollection": filter_data.get("marketTypesCollection", []),
            "countriesCollection": filter_data.get("countriesCollection", []),
            "fileTypeCollection": filter_data.get("fileTypeCollection", [])
        }
        
        logger.info(f"GetAdvBasketDataSize request")
        
        response = await http_client.post(
            f"{HISTORIC_DATA_BASE}/GetAdvBasketDataSize",
            headers=headers,
            json=request_body
        )
        
        logger.info(f"GetAdvBasketDataSize response: {response.status_code}")
        
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text[:200])
        
        return response.json()
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GetAdvBasketDataSize error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/downloadListOfFiles")
async def download_list_of_files(filter_data: dict):
    """Get list of files matching filter"""
    try:
        ssoid = filter_data.get("ssoid")
        if not ssoid:
            raise HTTPException(status_code=400, detail="Missing ssoid")
        
        headers = {
            "ssoid": ssoid,
            "Content-Type": "application/json"
        }
        
        request_body = {
            "sport": filter_data.get("sport", "Horse Racing"),
            "plan": filter_data.get("plan", "Basic Plan"),
            "fromDay": int(filter_data.get("fromDay", 1)),
            "fromMonth": int(filter_data.get("fromMonth", 1)),
            "fromYear": int(filter_data.get("fromYear", 2024)),
            "toDay": int(filter_data.get("toDay", 31)),
            "toMonth": int(filter_data.get("toMonth", 12)),
            "toYear": int(filter_data.get("toYear", 2024)),
            "eventId": None,
            "eventName": None,
            "marketTypesCollection": filter_data.get("marketTypesCollection", []),
            "countriesCollection": filter_data.get("countriesCollection", []),
            "fileTypeCollection": filter_data.get("fileTypeCollection", [])
        }
        
        logger.info(f"DownloadListOfFiles request: {request_body}")

        response = await http_client.post(
            f"{HISTORIC_DATA_BASE}/DownloadListOfFiles",
            headers=headers,
            json=request_body
        )

        logger.info(f"DownloadListOfFiles response: {response.status_code}")
        logger.info(f"DownloadListOfFiles response body (first 1000 chars): {response.text[:1000]}")

        if response.status_code != 200:
            logger.error(f"DownloadListOfFiles returned {response.status_code}: {response.text[:500]}")
            raise HTTPException(status_code=response.status_code, detail=response.text[:200])

        # Handle case where response might be empty or not JSON
        if not response.text or response.text.strip() == "":
            return []

        # The response might be a list of file paths as plain text (one per line)
        # or CSV format with headers
        try:
            return response.json()
        except Exception as json_err:
            logger.warning(f"Response is not JSON, trying to parse as text lines: {json_err}")
            # Parse as newline-separated file list, skip potential header row
            lines = [line.strip() for line in response.text.strip().split('\n') if line.strip()]
            # If first line looks like a header (contains common header words), skip it
            if lines and any(h in lines[0].lower() for h in ['file', 'path', 'name', 'url']):
                lines = lines[1:]
            # Filter out any empty lines or lines that don't look like file paths
            file_list = [line for line in lines if line and '/' in line or '.' in line]
            return file_list if file_list else lines
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"DownloadListOfFiles error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))