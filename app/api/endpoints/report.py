from typing import List
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.requests import BulkPayload, SinglePayloadItem
from app.schemas.responses import *
from app.core.supplier.report import *
from app.api import deps
import pandas as pd
import io
from app.core.config import get_settings

router = APIRouter()
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, Response
from typing import Optional
from app.models import User

router = APIRouter()

@router.get("/download-report/")
async def download_report(
    session_id: str = Query(..., description="Session ID"),
    ens_id: str = Query(..., description="ENS ID"),
    type_of_file: str = Query(..., description="Type of file (e.g., docx, pdf, csv)"),
    session: AsyncSession = Depends(deps.get_session),
    current_user: User = Depends(deps.get_current_user)
):
    try:
        file_data, result = await report_download(session_id, ens_id, type_of_file, session)

        if file_data is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No Report Found."
            )

        # Determine media type based on file type
        media_types = {
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "pdf": "application/pdf",
            "csv": "text/csv"
        }
        media_type = media_types.get(type_of_file.lower(), "application/octet-stream")

        return Response(
            content=file_data,
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={result}"}
        )

    except Exception as e:
        return {"error": str(e)}


@router.get("/bulk-download-report/")
async def bulk_download_report(session_id: str = Query(..., description="Session ID"), 
    session: AsyncSession = Depends(deps.get_session),
    current_user: User = Depends(deps.get_current_user)):
    try:
        file_data, result = await report_bulk_download(session_id, session)

        if file_data is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No Report Found."
            )

        return Response(
            content=file_data,
            media_type="application/zip",
            headers={
                "Content-Disposition": f"attachment; filename={result}",
                "Content-Type": "application/zip"
            },
            status_code=200
        )

    except Exception as e:
        return {"error": str(e)}


@router.get("/reviw-report/", response_model=Dict)
async def reviw_report(
    session_id: str = Query(..., description="Session ID"),
    ens_id: str = Query(..., description="ENS ID"),
    current_user: User = Depends(deps.get_current_user),
    dbsession: AsyncSession = Depends(deps.get_session)
):
    try:
        # Pass 'json' as the type_of_file
        json_data = await reviw_json_report_(dbsession, session_id, ens_id, type_of_file='json')
        print("json_data",json_data)
        if not json_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No Report Found."
            )

        return JSONResponse(content=json_data)

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
