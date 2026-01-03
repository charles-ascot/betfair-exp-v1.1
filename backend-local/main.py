from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import httpx
import logging
import os
import urllib.parse
import asyncio
import zipfile
import io

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

# Global HTTP client - no shared cookies to avoid session conflicts
http_client = None

@app.on_event("startup")
async def startup_event():
    global http_client
    http_client = httpx.AsyncClient(
        timeout=180.0,  # Increased timeout
        follow_redirects=True,
        limits=httpx.Limits(max_keepalive_connections=5, max_connections=50)
    )
    logger.info("🎰 Betfair Historic Data Explorer Starting...")
    logger.info(f"Historic Data API Base: {HISTORIC_DATA_BASE}")

async def make_request_with_retry(method, url, headers, json=None, max_retries=2):
    """Make HTTP request with retry logic"""
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            if method == "GET":
                response = await http_client.get(url, headers=headers)
            else:
                response = await http_client.post(url, headers=headers, json=json)

            # If we get a valid response (even error), return it
            if response.status_code != 502 and response.status_code != 503:
                return response

            logger.warning(f"Attempt {attempt + 1} failed with {response.status_code}, retrying...")
            await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
        except Exception as e:
            last_error = e
            logger.warning(f"Attempt {attempt + 1} failed with error: {e}, retrying...")
            await asyncio.sleep(1 * (attempt + 1))

    if last_error:
        raise last_error
    return response

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

        response = await make_request_with_retry(
            "GET",
            f"{HISTORIC_DATA_BASE}/GetMyData",
            headers=headers
        )

        logger.info(f"GetMyData response: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"GetMyData returned {response.status_code}: {response.text[:500]}")
            raise HTTPException(status_code=response.status_code, detail=response.text[:200])

        # Check if response is HTML (session expired or invalid)
        if '<html' in response.text.lower() or '<!doctype' in response.text.lower():
            logger.error("GetMyData returned HTML - session likely expired or invalid")
            raise HTTPException(status_code=401, detail="Invalid or expired ssoid. Please get a new session token.")

        # Try to parse JSON with error handling
        try:
            return response.json()
        except Exception as json_err:
            logger.error(f"GetMyData JSON parse error: {json_err}, response: {response.text[:500]}")
            raise HTTPException(status_code=502, detail="Invalid response from Betfair. Session may have expired.")

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

        response = await make_request_with_retry(
            "POST",
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

        response = await make_request_with_retry(
            "POST",
            f"{HISTORIC_DATA_BASE}/GetAdvBasketDataSize",
            headers=headers,
            json=request_body
        )

        logger.info(f"GetAdvBasketDataSize response: {response.status_code}")
        logger.debug(f"GetAdvBasketDataSize response body: {response.text[:500]}")

        if response.status_code != 200:
            logger.error(f"GetAdvBasketDataSize returned {response.status_code}: {response.text[:500]}")
            raise HTTPException(status_code=response.status_code, detail=response.text[:200])

        # Check if response is HTML (session expired)
        if '<html' in response.text.lower() or '<!doctype' in response.text.lower():
            logger.error("GetAdvBasketDataSize returned HTML - session likely expired")
            raise HTTPException(status_code=401, detail="Session expired. Please log in again with a new ssoid.")

        # Try to parse JSON with error handling
        try:
            return response.json()
        except Exception as json_err:
            logger.error(f"GetAdvBasketDataSize JSON parse error: {json_err}, response: {response.text[:500]}")
            raise HTTPException(status_code=502, detail="Invalid response from Betfair. Session may have expired.")

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
            "marketTypesCollection": filter_data.get("marketTypesCollection", []),
            "countriesCollection": filter_data.get("countriesCollection", []),
            "fileTypeCollection": filter_data.get("fileTypeCollection", [])
        }

        logger.info(f"DownloadListOfFiles request: {request_body}")
        logger.info(f"DownloadListOfFiles headers: ssoid={ssoid[:10]}...")

        response = await make_request_with_retry(
            "POST",
            f"{HISTORIC_DATA_BASE}/DownloadListOfFiles",
            headers=headers,
            json=request_body
        )

        logger.info(f"DownloadListOfFiles response: {response.status_code}")
        logger.info(f"DownloadListOfFiles response body (first 1000 chars): {response.text[:1000]}")

        # Check if response is an HTML error page
        if '<html' in response.text.lower() or '<!doctype' in response.text.lower():
            logger.error(f"DownloadListOfFiles returned HTML error page")
            raise HTTPException(status_code=502, detail="Betfair API returned an error page. Session may have expired.")

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

@app.get("/api/downloadFile")
async def download_file(filePath: str, ssoid: str):
    """Download a single file from Betfair Historic Data"""
    try:
        if not ssoid:
            raise HTTPException(status_code=400, detail="Missing ssoid")
        if not filePath:
            raise HTTPException(status_code=400, detail="Missing filePath")

        headers = {
            "ssoid": ssoid,
            "Content-Type": "application/json"
        }

        # URL encode the file path for the query parameter
        encoded_path = urllib.parse.quote(filePath, safe='')
        download_url = f"{HISTORIC_DATA_BASE}/DownloadFile?filePath={encoded_path}"

        logger.info(f"DownloadFile request: {filePath}")

        response = await http_client.get(download_url, headers=headers)

        logger.info(f"DownloadFile response: {response.status_code}, size: {len(response.content)} bytes")

        if response.status_code != 200:
            logger.error(f"DownloadFile failed: {response.status_code}")
            raise HTTPException(status_code=response.status_code, detail="Failed to download file from Betfair")

        # Extract filename from path
        filename = filePath.split('/')[-1]

        return StreamingResponse(
            io.BytesIO(response.content),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(response.content))
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"DownloadFile error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/downloadFiles")
async def download_files(data: dict):
    """Download multiple files and return as a ZIP archive"""
    try:
        ssoid = data.get("ssoid")
        file_paths = data.get("filePaths", [])
        # Limit files to prevent timeout (max 500 files per request)
        max_files = data.get("maxFiles", 500)

        if not ssoid:
            raise HTTPException(status_code=400, detail="Missing ssoid")
        if not file_paths:
            raise HTTPException(status_code=400, detail="No files to download")

        # Limit the number of files to prevent timeout
        if len(file_paths) > max_files:
            logger.warning(f"Limiting download from {len(file_paths)} to {max_files} files")
            file_paths = file_paths[:max_files]

        headers = {
            "ssoid": ssoid,
            "Content-Type": "application/json"
        }

        logger.info(f"DownloadFiles request: {len(file_paths)} files")

        # Create an in-memory ZIP file
        zip_buffer = io.BytesIO()

        async def download_single_file(path, semaphore):
            async with semaphore:
                encoded_path = urllib.parse.quote(path, safe='')
                download_url = f"{HISTORIC_DATA_BASE}/DownloadFile?filePath={encoded_path}"
                try:
                    response = await http_client.get(download_url, headers=headers)
                    if response.status_code == 200:
                        content = response.content
                        # Check if response is HTML error page instead of actual file
                        content_str = content[:500].decode('utf-8', errors='ignore').lower()
                        if '<html' in content_str or '<!doctype' in content_str:
                            logger.warning(f"File {path} returned HTML instead of data (session expired?)")
                            return None
                        # Check minimum file size (real bz2 files should be > 100 bytes)
                        if len(content) < 100:
                            logger.warning(f"File {path} too small ({len(content)} bytes), skipping")
                            return None
                        filename = path.split('/')[-1]
                        return (filename, content)
                    else:
                        logger.warning(f"Failed to download {path}: {response.status_code}")
                        return None
                except Exception as e:
                    logger.warning(f"Error downloading {path}: {e}")
                    return None

        # Download files concurrently with semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(20)  # Increased concurrency
        tasks = [download_single_file(path, semaphore) for path in file_paths]
        results = await asyncio.gather(*tasks)
        downloaded_files = [r for r in results if r is not None]

        logger.info(f"Downloaded {len(downloaded_files)} of {len(file_paths)} files")

        if not downloaded_files:
            raise HTTPException(status_code=500, detail="Failed to download any files")

        # Create ZIP archive
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for filename, content in downloaded_files:
                zip_file.writestr(filename, content)

        zip_buffer.seek(0)

        logger.info(f"DownloadFiles complete: {len(downloaded_files)} files in ZIP")

        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={
                "Content-Disposition": "attachment; filename=betfair_historic_data.zip",
                "Content-Length": str(zip_buffer.getbuffer().nbytes)
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"DownloadFiles error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))