from typing import List, Literal, Optional
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.requests import BulkPayload, ClientConfigurationRequest, SessionRequest, SinglePayloadItem
from app.schemas.responses import *
from app.core.queue.queue import *
from app.api import deps
import pandas as pd
import io
from app.schemas.logger import logger

router = APIRouter()

@router.post("/queue-trigger-analysis/")
async def queue_trigger_analysis(session_id: str, session: AsyncSession = Depends(deps.get_session), current_user_id: User = Depends(deps.get_current_user)):
    # 1. Save to DB
    try:
        client_config_response = await queue_trigger_analysis_(session_id, session)
        response = ResponseMessage(
            status="success",
            data=client_config_response,  
            message="Client config processed successfully"
        )
        return response

    except HTTPException as http_err:
        # Return structured error responses for HTTP exceptions
        raise http_err

    except Exception as error:
        # Handle unexpected errors
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to processing client config: {str(error)}"
        ) 


@router.post("/queue-trigger-entity-validation/")
async def queue_trigger_entity_validation(session_id: str, session: AsyncSession = Depends(deps.get_session), current_user_id: User = Depends(deps.get_current_user)):
    # 1. Save to DB
    try:
        client_config_response = await queue_trigger_entity_validation_(session_id, session)
        response = ResponseMessage(
            status="success",
            data=client_config_response,  
            message="Client config processed successfully"
        )
        return response

    except HTTPException as http_err:
        # Return structured error responses for HTTP exceptions
        raise http_err

    except Exception as error:
        # Handle unexpected errors
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to processing client config: {str(error)}"
        ) 

@router.get("/check-queue")
async def check_session_queue(session_id: str, session: AsyncSession = Depends(deps.get_session), current_user_id: User = Depends(deps.get_current_user)):
    queue = await get_session_queue(session_id, session)
    return {"session_id": session_id, "queue": queue}

@router.get(
    "/poll-session-status",
    response_model=ResponseMessage,
    description="Poll the current screening status for a given session_id"
)
async def get_sessionid_status_poll(
    session_id: str = Query(..., description="Session ID"),
    db_session: AsyncSession = Depends(deps.get_session),
    current_user: User = Depends(deps.get_current_user)
):
    """
    Retrieve the current session-level screening status using the session_id.

    Returns the latest status snapshot for UI or automation polling.
    """
    try:
        logger.debug(f"Polling status for session_id: {session_id}")

        initial_state = await get_session_screening_status_static(session_id, db_session)

        if not initial_state:
            raise HTTPException(status_code=404, detail=f"No status found for session_id: {session_id}")

        return {"status": "success", "data": initial_state[0], "message": "Status retrieved successfully"}

    except Exception as e:
        logger.error(f"get_sessionid_status_poll → {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving session status: {str(e)}")