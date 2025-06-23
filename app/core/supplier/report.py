import json
from typing import Dict
from fastapi import  HTTPException
import urllib
from app.core.utils.db_utils import *
import io
from app.core.config import get_settings  # Import settings
from azure.storage.blob import BlobServiceClient
import zipfile
from app.schemas.logger import logger
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

async def report_download(session_id, ens_id, type_of_file, session)->Dict:
    try:
        session_supplier_data = await get_dynamic_ens_data(
            table_name="upload_supplier_master_data", 
            required_columns=['session_id'], 
            ens_id="", 
            session_id=session_id, 
            session=session
        )

        if not session_supplier_data:
            raise HTTPException(status_code=400, detail=f"No records found for session_id: {session_id}")
        
        initial_state = await get_session_screening_status_static(session_id, session)

        if not initial_state:
            raise HTTPException(status_code=404, detail=f"No status found for session_id: {session_id}")
        if len(initial_state):
            status = initial_state[0]['screening_analysis_status']
            status_message_map = {
                STATUS.FAILED: "failed",
                STATUS.IN_PROGRESS: "is in progress",
                STATUS.QUEUED: "is not started"
            }
            print("status in status_message_map", status in status_message_map)
            if status in status_message_map:
                raise HTTPException(
                    status_code=400,
                    detail=f"Screening Analysis Status {status_message_map[status]} for session_id: {session_id}. Please review and correct the uploaded session id."
                )
            
        logger.info("Start Report Download")
        # Retrieve storage settings
        storage_url = get_settings().storage.storage_account_url
        container_name = session_id  # Session ID is the container name
        sas_token = str(get_settings().storage.sas_token)
        logger.info("Initialize BlobServiceClient with SAS token")
        # Initialize BlobServiceClient with SAS token
        blob_service_client = BlobServiceClient(account_url=storage_url, credential=sas_token)
        logger.info("Got Blob_service_client")
        try:
            container_client = blob_service_client.get_container_client(container_name)
        except:
            logger.error("Failed to get container client")
            raise HTTPException(status_code=404, detail="Container not found or access denied")

        # Define folder path based on ens_id (no leading slash)
        folder_path = f"{ens_id}/"
        logger.info("Requesting ens report ")
        try:
            # List blobs inside the specific folder
            blob_list = container_client.list_blobs(name_starts_with=folder_path)
        except:
            logger.error("Failed to list blobs in container")
            raise HTTPException(status_code=500, detail="Failed to list blobs in container")


        # Filter blobs that match session_id, ens_id, and type_of_file
        matching_blobs = [
            blob for blob in blob_list 
            if ens_id in blob.name and blob.name.endswith(f".{type_of_file}")
        ]

        # Ensure at least one matching file exists
        if not matching_blobs:
            raise HTTPException(status_code=404, detail=f"No matching {type_of_file} file found for session_id {session_id} and ens_id {ens_id}")

        # Sort blobs by last modified date to get the latest file
        latest_blob = max(matching_blobs, key=lambda b: b.last_modified)

        # URL decode the latest filename
        try : 
            decoded_filename = urllib.parse.unquote(latest_blob.name)
        except: 
            logger.error("No Report Found")
            raise HTTPException(
                status_code=404,
                detail="No Report Found."
            )
        # Get the latest file's blob client
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=decoded_filename)

        # Download the blob data
        stream = blob_client.download_blob()
        file_data = stream.readall()

        return file_data, decoded_filename
    
    except HTTPException as http_err:
        raise http_err  # Propagate HTTP exceptions

    except Exception as error:
        logger.error(f"Unexpected error: {error}")
        raise HTTPException(
            status_code=404,
                detail="No Report Found."
        )
    

async def report_bulk_download(session_id, session) -> Dict:
    try:
        session_supplier_data = await get_dynamic_ens_data(
            table_name="upload_supplier_master_data", 
            required_columns=['session_id'], 
            ens_id="", 
            session_id=session_id, 
            session=session
        )

        if not session_supplier_data:
            raise HTTPException(status_code=404, detail=f"No records found for session_id: {session_id}")

        initial_state = await get_session_screening_status_static(session_id, session)

        if not initial_state:
            raise HTTPException(status_code=404, detail=f"No status found for session_id: {session_id}")
        if len(initial_state):
            status = initial_state[0]['screening_analysis_status']
            status_message_map = {
                STATUS.FAILED: "failed",
                STATUS.IN_PROGRESS: "is in progress",
                STATUS.QUEUED: "is not started"
            }
            print("status in status_message_map", status in status_message_map)
            if status in status_message_map:
                raise HTTPException(
                    status_code=400,
                    detail=f"Screening Analysis Status {status_message_map[status]} for session_id: {session_id}. Please review and correct the uploaded session id."
                )
            
        # Retrieve storage settings
        storage_url = get_settings().storage.storage_account_url
        container_name = session_id
        sas_token = str(get_settings().storage.sas_token)

        # Initialize BlobServiceClient with SAS token
        blob_service_client = BlobServiceClient(account_url=storage_url, credential=sas_token)
        try: 
            container_client = blob_service_client.get_container_client(container_name)
        except Exception as e:
            raise HTTPException(
                status_code=404,
                detail="No Report Found."
            )

        
        # List all blobs inside the container
        blob_list = list(container_client.list_blobs())  # Convert generator to list

        # Ensure there are files to download
        if not blob_list:
            raise HTTPException(status_code=404, detail=f"No files found for session_id {session_id}")

        # Create an in-memory ZIP file
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for blob in blob_list:
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob.name)
                file_data = blob_client.download_blob().readall()
                zip_file.writestr(blob.name, file_data)  # Add file to ZIP with original path

        # Seek to the beginning of the ZIP file
        zip_buffer.seek(0)
        
        return zip_buffer.getvalue(), f"{session_id}.zip"

    except HTTPException as http_err:
        raise http_err  # Propagate HTTP exceptions

    except Exception as error:
        logger.error(f"Unexpected error: {error}")
        raise HTTPException(
            status_code=404,
                detail="No Report Found."
        )

async def reviw_json_report_(
    session,
    session_id,
    ens_id,
    type_of_file
) -> Dict:
    try:
        # Step 1: Validate session data exists in DB
        session_supplier_data = await get_dynamic_ens_data(
            table_name="upload_supplier_master_data",
            required_columns=['session_id'],
            ens_id="",
            session_id=session_id,
            session=session
        )

        if not session_supplier_data:
            raise HTTPException(
                status_code=404,
                detail=f"No records found for session_id: {session_id}"
            )

        initial_state = await get_session_screening_status_static(session_id, session)

        if not initial_state:
            raise HTTPException(status_code=404, detail=f"No status found for session_id: {session_id}")
        if len(initial_state):
            status = initial_state[0]['screening_analysis_status']
            status_message_map = {
                STATUS.FAILED: "failed",
                STATUS.IN_PROGRESS: "is in progress",
                STATUS.QUEUED: "is not started"
            }
            print("status in status_message_map", status in status_message_map)
            if status in status_message_map:
                raise HTTPException(
                    status_code=400,
                    detail=f"Screening Analysis Status {status_message_map[status]} for session_id: {session_id}. Please review and correct the uploaded session id."
                )
            
        # Step 2: Initialize storage connection
        storage_url = get_settings().storage.storage_account_url
        sas_token = get_settings().storage.sas_token
        container_name = session_id
        folder_path = f"{ens_id}/"

        blob_service_client = BlobServiceClient(account_url=storage_url, credential=sas_token)
        container_client = blob_service_client.get_container_client(container_name)

        # Step 3: Try listing blobs — if container doesn't exist, catch it
        try:
            blob_list = list(container_client.list_blobs(name_starts_with=folder_path))
        except ResourceNotFoundError:
            raise HTTPException(
                status_code=404,
                detail=f"No report container found for session_id: {session_id}"
            )
        except HttpResponseError as e:
            logger.error(f"Storage access error: {e}")
            raise HTTPException(
                status_code=500,
                detail="Unable to access blob storage"
            )

        # Step 4: Filter matching blobs
        matching_blobs = [
            blob for blob in blob_list
            if ens_id in blob.name and blob.name.endswith(f".{type_of_file}")
        ]

        if not matching_blobs:
            raise HTTPException(
                status_code=404,
                detail=f"No .{type_of_file} report found for session_id {session_id} and ens_id {ens_id}"
            )

        # Step 5: Fetch the latest blob
        latest_blob = max(matching_blobs, key=lambda b: b.last_modified)
        decoded_filename = urllib.parse.unquote(latest_blob.name)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=decoded_filename)

        try:
            file_data = blob_client.download_blob().readall()
            json_data = json.loads(file_data.decode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to read or parse blob: {e}")
            raise HTTPException(
                status_code=500,
                detail="Unable to read or parse report JSON"
            )

        return {"data": json_data}

    except HTTPException as http_err:
        raise http_err  # Propagate HTTP exceptions

    except Exception as error:
        logger.error(f"Unexpected error: {error}")
        raise HTTPException(
            status_code=500, detail="Unexpected error occurred while processing the report"
        )
    