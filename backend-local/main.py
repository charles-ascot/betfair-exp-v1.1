from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
import httpx
import logging
import os
import urllib.parse
import asyncio
import zipfile
import io
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor
import functools

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://betfair1.thync.online", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Files-Requested", "X-Files-Downloaded", "X-Files-Failed"],
)

HISTORIC_DATA_BASE = "https://historicdata.betfair.com/api"

# GCS Configuration
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "betfair-basic-historic")
GCS_PROJECT_ID = os.getenv("GCS_PROJECT_ID", "betfair-data-explorer")

# Global HTTP client - no shared cookies to avoid session conflicts
http_client = None
gcs_client = None
thread_executor = None

@app.on_event("startup")
async def startup_event():
    global http_client, gcs_client, thread_executor
    http_client = httpx.AsyncClient(
        timeout=180.0,  # Increased timeout
        follow_redirects=True,
        limits=httpx.Limits(max_keepalive_connections=5, max_connections=50)
    )
    # Initialize GCS client
    try:
        gcs_client = storage.Client(project=GCS_PROJECT_ID)
        logger.info(f"GCS client initialized for project: {GCS_PROJECT_ID}")
        logger.info(f"GCS bucket: {GCS_BUCKET_NAME}")
    except Exception as e:
        logger.warning(f"GCS client initialization failed: {e}. GCS uploads will not work.")
        gcs_client = None

    # Thread executor for blocking GCS operations
    thread_executor = ThreadPoolExecutor(max_workers=20)

    logger.info("Betfair Historic Data Explorer Starting...")
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
    global http_client, thread_executor
    if http_client:
        await http_client.aclose()
    if thread_executor:
        thread_executor.shutdown(wait=False)

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
                "Content-Length": str(zip_buffer.getbuffer().nbytes),
                "X-Files-Requested": str(len(file_paths)),
                "X-Files-Downloaded": str(len(downloaded_files)),
                "X-Files-Failed": str(len(file_paths) - len(downloaded_files))
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"DownloadFiles error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def upload_to_gcs_sync(bucket, blob_path, content):
    """Synchronous GCS upload function to run in thread pool"""
    blob = bucket.blob(blob_path)
    blob.upload_from_string(content)
    return blob_path


@app.post("/api/downloadFilesToGCS")
async def download_files_to_gcs(data: dict):
    """Download files from Betfair and upload them to Google Cloud Storage"""
    try:
        if gcs_client is None:
            raise HTTPException(status_code=503, detail="GCS client not initialized")

        ssoid = data.get("ssoid")
        file_paths = data.get("filePaths", [])
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

        logger.info(f"DownloadFilesToGCS request: {len(file_paths)} files")

        # Get the GCS bucket
        bucket = gcs_client.bucket(GCS_BUCKET_NAME)

        uploaded_files = []
        failed_files = []

        async def download_and_upload_file(path, semaphore):
            async with semaphore:
                encoded_path = urllib.parse.quote(path, safe='')
                download_url = f"{HISTORIC_DATA_BASE}/DownloadFile?filePath={encoded_path}"

                # Retry logic for rate limiting (429)
                max_retries = 3
                response = None
                last_error = None

                for attempt in range(max_retries):
                    try:
                        response = await http_client.get(download_url, headers=headers)

                        # Handle rate limiting with exponential backoff
                        if response.status_code == 429:
                            wait_time = (attempt + 1) * 2  # 2, 4, 6 seconds
                            logger.warning(f"Rate limited on {path}, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})")
                            await asyncio.sleep(wait_time)
                            continue

                        break  # Success or non-retryable error
                    except Exception as e:
                        last_error = e
                        if attempt < max_retries - 1:
                            await asyncio.sleep((attempt + 1) * 2)
                            continue
                        # All retries exhausted
                        logger.error(f"All retries failed for {path}: {type(e).__name__}: {e}")
                        return {"path": path, "success": False, "error": f"Connection failed: {str(e)}"}

                # Check if we have a valid response
                if response is None:
                    logger.error(f"No response received for {path}")
                    return {"path": path, "success": False, "error": "No response received"}

                try:
                    if response.status_code == 200:
                        content = response.content
                        # Check if response is HTML error page instead of actual file
                        content_str = content[:500].decode('utf-8', errors='ignore').lower()
                        if '<html' in content_str or '<!doctype' in content_str:
                            logger.warning(f"File {path} returned HTML instead of data (session expired?)")
                            return {"path": path, "success": False, "error": "Session expired"}
                        # Check minimum file size (real bz2 files should be > 100 bytes)
                        if len(content) < 100:
                            logger.warning(f"File {path} too small ({len(content)} bytes), skipping")
                            return {"path": path, "success": False, "error": "File too small"}

                        # Extract filename and create GCS path
                        # Original path format: /xds_nfs/hdfs_supreme/BASIC/2016/Jan/...
                        # We'll store with a cleaner structure in GCS
                        filename = path.split('/')[-1]
                        # Create folder structure from the original path
                        # Remove leading slash and simplify path
                        gcs_path = path.lstrip('/')
                        # Remove xds_nfs/hdfs_supreme prefix if present
                        if gcs_path.startswith('xds_nfs/hdfs_supreme/'):
                            gcs_path = gcs_path[21:]  # Remove 'xds_nfs/hdfs_supreme/' prefix

                        # Upload to GCS in thread pool (since GCS client is blocking)
                        try:
                            loop = asyncio.get_event_loop()
                            await loop.run_in_executor(
                                thread_executor,
                                functools.partial(upload_to_gcs_sync, bucket, gcs_path, content)
                            )
                            logger.info(f"Uploaded {gcs_path} ({len(content)} bytes)")
                        except Exception as gcs_err:
                            logger.error(f"GCS upload failed for {path}: {gcs_err}")
                            return {"path": path, "success": False, "error": f"GCS upload failed: {str(gcs_err)}"}

                        return {"path": path, "success": True, "gcs_path": f"gs://{GCS_BUCKET_NAME}/{gcs_path}"}
                    else:
                        logger.warning(f"Failed to download {path}: {response.status_code}")
                        return {"path": path, "success": False, "error": f"HTTP {response.status_code}"}
                except Exception as e:
                    logger.error(f"Error downloading/uploading {path}: {type(e).__name__}: {e}")
                    return {"path": path, "success": False, "error": str(e)}

        # Download and upload files concurrently with semaphore to limit concurrent requests
        # Set to 10 for reasonable speed while avoiding rate limiting
        semaphore = asyncio.Semaphore(10)
        tasks = [download_and_upload_file(path, semaphore) for path in file_paths]
        results = await asyncio.gather(*tasks)

        for result in results:
            if result["success"]:
                uploaded_files.append(result)
            else:
                failed_files.append(result)

        logger.info(f"DownloadFilesToGCS complete: {len(uploaded_files)} uploaded, {len(failed_files)} failed")

        return JSONResponse(
            content={
                "success": True,
                "bucket": GCS_BUCKET_NAME,
                "filesRequested": len(file_paths),
                "filesUploaded": len(uploaded_files),
                "filesFailed": len(failed_files),
                "uploadedFiles": uploaded_files[:10],  # Return first 10 as sample
                "failedFiles": failed_files[:10]  # Return first 10 failed as sample
            },
            headers={
                "X-Files-Requested": str(len(file_paths)),
                "X-Files-Downloaded": str(len(uploaded_files)),
                "X-Files-Failed": str(len(failed_files))
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"DownloadFilesToGCS error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/gcsStatus")
async def gcs_status():
    """Check GCS connection status and bucket info"""
    try:
        if gcs_client is None:
            return JSONResponse(
                content={
                    "connected": False,
                    "error": "GCS client not initialized"
                },
                status_code=503
            )

        bucket = gcs_client.bucket(GCS_BUCKET_NAME)
        exists = bucket.exists()

        if exists:
            # Count objects in bucket (limit to first 1000 for speed)
            blobs = list(bucket.list_blobs(max_results=1000))
            return JSONResponse(
                content={
                    "connected": True,
                    "bucket": GCS_BUCKET_NAME,
                    "project": GCS_PROJECT_ID,
                    "bucketExists": True,
                    "objectCount": len(blobs),
                    "objectCountLimited": len(blobs) >= 1000
                }
            )
        else:
            return JSONResponse(
                content={
                    "connected": True,
                    "bucket": GCS_BUCKET_NAME,
                    "project": GCS_PROJECT_ID,
                    "bucketExists": False,
                    "error": f"Bucket {GCS_BUCKET_NAME} does not exist"
                },
                status_code=404
            )

    except Exception as e:
        logger.error(f"GCS status check error: {str(e)}", exc_info=True)
        return JSONResponse(
            content={
                "connected": False,
                "error": str(e)
            },
            status_code=500
        )