from typing import Dict

from fastapi import  HTTPException, status
import redis
from app.core.config import get_settings
from app.core.utils.db_utils import *
from app.models import *
from app.schemas.logger import logger
from app.task import process_session, validate_name

#  Redis config
REDIS_URL = "redis://redis:6379/0"
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

SESSION_SET_KEY = "queued_session_ids"
NAME_VALIDATION_SET_KEY = "queued_name_validation_ids"

def submit_session(session_id: str):
    if rdb.sismember(SESSION_SET_KEY, session_id):
        return {"already_exists": True}

    rdb.sadd(SESSION_SET_KEY, session_id)
    task = process_session.delay(session_id)
    return {"already_exists": False, "task_id": task.id}

def submit_name_validation(session_id: str):
    if rdb.sismember(NAME_VALIDATION_SET_KEY, session_id):
        return {"already_exists": True}

    rdb.sadd(NAME_VALIDATION_SET_KEY, session_id)
    task = validate_name.delay(session_id)
    return {"already_exists": False, "task_id": task.id}


# Your async queue trigger
async def queue_trigger_analysis_(session_id, session) -> Dict:
    try:
        session_supplier_data = await get_dynamic_ens_data(
            table_name="upload_supplier_master_data", 
            required_columns=['session_id'], 
            ens_id="", 
            session_id=session_id, 
            session=session
        )

        if not session_supplier_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No records found for session_id: {session_id}")

        # Step 1: Submit to Celery only if not already queued
        submit_result = submit_session(session_id)
        # Step 2: Prepare status update data
        data = [{
            "overall_status": STATUS.IN_PROGRESS,
            "list_upload_status": STATUS.COMPLETED,
            "supplier_name_validation_status": STATUS.COMPLETED,
            "screening_analysis_status": STATUS.QUEUED
        }]


        if submit_result.get("already_exists"):
            response = await upsert_session_screening_status(data, session_id, session)
            logger.info("Upsert completed")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Session ID '{session_id}' is already in queue"
            )

        # Step 3: Get the table
        table_class = Base.metadata.tables.get("session_screening_status")
        if table_class is None:
            raise ValueError("Table 'session_screening_status' does not exist in the database schema.")

        res = {}

        # Step 4: Upsert status into DB
        response = await upsert_session_screening_status(data, session_id, session)
        if response.get("message") == "Upsert completed":
            res["session_screening_status"] = "Updated"

        # Step 5: Return success
        return {
            "message": "Upsert completed successfully",
            "data": res,
            "celery_task_id": submit_result["task_id"]
        }

    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid input: {str(ve)}"
        )

    except SQLAlchemyError as sa_err:
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}

    except HTTPException:
        raise

    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unhandled error: {str(error)}"
        )
    
# Your async queue trigger
async def queue_trigger_entity_validation_(session_id, session) -> Dict:
    try:
        session_supplier_data = await get_dynamic_ens_data(
            table_name="upload_supplier_master_data", 
            required_columns=['session_id'], 
            ens_id="", 
            session_id=session_id, 
            session=session
        )

        if not session_supplier_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No records found for session_id: {session_id}")

        # Submit to name_validation_queue
        submit_result = submit_name_validation(session_id)

        # Prepare status update
        data = [{
            "overall_status": STATUS.IN_PROGRESS,
            "list_upload_status": STATUS.COMPLETED,
            "supplier_name_validation_status": STATUS.QUEUED,
            "screening_analysis_status": STATUS.NOT_STARTED
        }]

        if submit_result.get("already_exists"):
            response = await upsert_session_screening_status(data, session_id, session)
            logger.info("Upsert completed")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Session ID '{session_id}' is already in name validation queue"
            )

        # Get the table
        table_class = Base.metadata.tables.get("session_screening_status")
        if table_class is None:
            raise ValueError("Table 'session_screening_status' does not exist in the database schema.")

        res = {}

        # Update DB
        response = await upsert_session_screening_status(data, session_id, session)
        if response.get("message") == "Upsert completed":
            res["session_screening_status"] = "Updated"

        return {
            "message": "Upsert completed successfully",
            "data": res,
            "celery_task_id": submit_result["task_id"]
        }

    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid input: {str(ve)}"
        )

    except SQLAlchemyError as sa_err:
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}

    except HTTPException:
        raise

    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unhandled error: {str(error)}"
        )

async def get_session_queue(session_id: str, session) -> str:
    session_supplier_data = await get_dynamic_ens_data(
            table_name="upload_supplier_master_data", 
            required_columns=['session_id'], 
            ens_id="", 
            session_id=session_id, 
            session=session
        )
    if not session_supplier_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No records found for session_id: {session_id}")

    if rdb.sismember(SESSION_SET_KEY, session_id):
        return "screening_queue"
    elif rdb.sismember(NAME_VALIDATION_SET_KEY, session_id):
        return "name_validation_queue"
    else:
        return "not_in_any_queue"
