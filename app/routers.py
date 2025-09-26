# routers.py
from typing import List
from fastapi import APIRouter, File, UploadFile, HTTPException, Body
from .chunkr_client import get_client
from .extract import extract_airline_rate_fields
import os
import tempfile
import httpx

router = APIRouter()

@router.post("/process/url")
async def process_url(url: str = Body(..., embed=True)) -> dict:
    client = get_client()
    try:
        # Fetch the file bytes first
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as http:
            r = await http.get(url)
            r.raise_for_status()
            content = r.content

        # Create a legacy structured-extraction task with our schema+instructions
        task_id = await client.create_structured_task_legacy(
            file_bytes=content,
            filename=url.split("/")[-1] or "document",
        )
        final = await client.poll_task_until_complete_legacy(task_id)

        # Convert Chunkr response -> your target JSON shape
        return extract_airline_rate_fields(final)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/process/file")
async def process_file(file: UploadFile = File(...)) -> dict:
    client = get_client()
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            data = await file.read()
            tmp.write(data)
            tmp_path = tmp.name

        task_id = await client.create_structured_task_legacy(
            file_bytes=data,
            filename=file.filename or "upload",
        )
        final = await client.poll_task_until_complete_legacy(task_id)
        return extract_airline_rate_fields(final)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except Exception:
            pass
