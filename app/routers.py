from fastapi import APIRouter, File, UploadFile, HTTPException, Query
from fastapi.responses import Response
import tempfile
import os
import json
import logging
from typing import Optional, Any, Dict
import httpx
import asyncio

from app.schemas.airline_rate_extract import AirlineRateExtract  # ✅ NEW SCHEMA CLASS

router = APIRouter()
log = logging.getLogger("chunkr_service")

# ====== CONFIG ======
CHUNKR_API_KEY = "ch_8U2hBpgPC85_4v7RuDVtzq4aUg8zerbSC24DDWWaZEcTx"  # TEMP for local testing
CHUNKR_BASE = "https://api.chunkr.ai"


def _build_schema() -> Dict[str, Any]:
    """Build the JSON schema for the Extract API."""
    return AirlineRateExtract.model_json_schema()  # ✅ FIXED class name


def _json(obj: Any) -> str:
    """Pretty-print JSON for responses."""
    return json.dumps(obj, ensure_ascii=False, indent=2)


@router.post("/process/file")
async def process_file(
    file: UploadFile = File(...),
    raw: Optional[str] = Query(
        default=None,
        description="Debug: 'results' to return only results; 'all' to return full output."
    ),
) -> Response:
    tmp_path = None
    try:
        # 1️⃣ Save upload locally
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
            log.info("📄 Saved upload to %s (%d bytes)", tmp_path, len(content))

        # 2️⃣ Upload to Chunkr /files → get file_url
        headers = {"Authorization": CHUNKR_API_KEY}
        timeout = httpx.Timeout(60.0, connect=10.0)

        async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
            files = {"file": (os.path.basename(tmp_path), open(tmp_path, "rb"), "application/pdf")}
            up = await client.post(f"{CHUNKR_BASE}/files", files=files)

            if up.status_code == 401:
                raise HTTPException(status_code=401, detail="Invalid or expired Chunkr API key")

            up.raise_for_status()
            file_url = up.json().get("url")

            if not file_url:
                raise HTTPException(status_code=502, detail="Chunkr /files did not return a file URL")

        log.info("✅ File uploaded successfully: %s", file_url)

        # 3️⃣ Create Extract task with schema
        schema = _build_schema()
        payload = {"file": file_url, "schema": schema}

        async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
            create = await client.post(f"{CHUNKR_BASE}/tasks/extract", json=payload)

            if create.status_code == 401:
                raise HTTPException(status_code=401, detail="Invalid or expired Chunkr API key")

            create.raise_for_status()
            task_id = create.json().get("task_id")

            if not task_id:
                raise HTTPException(status_code=502, detail="Chunkr did not return task_id")

        log.info("🚀 Created Extract task: %s", task_id)

        # 4️⃣ Poll /tasks/{task_id} until done
        max_wait_seconds = 360
        poll_interval = 10
        elapsed = 0
        result = None

        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0), headers=headers) as client:
            while elapsed < max_wait_seconds:
                r = await client.get(f"{CHUNKR_BASE}/tasks/{task_id}")

                if r.status_code == 401:
                    raise HTTPException(status_code=401, detail="Invalid or expired Chunkr API key")

                r.raise_for_status()
                j = r.json()
                status = j.get("status") or j.get("state")

                log.info("⏳ Task %s status: %s", task_id, status)

                if status in ("Succeeded", "completed", "done"):
                    result = j
                    break
                if status in ("Failed", "Error", "Cancelled"):
                    raise HTTPException(status_code=502, detail=f"Chunkr task failed: {j}")

                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

        if not result:
            raise HTTPException(status_code=504, detail=f"Chunkr task {task_id} timed out")

        output = result.get("output") or {}
        results = output.get("results")

        if results is None:
            if raw == "all":
                return Response(content=_json(result), media_type="application/json")
            raise HTTPException(status_code=502, detail="Chunkr returned no results")

        # 5️⃣ Debug variants
        if raw == "all":
            return Response(content=_json(result), media_type="application/json")
        if raw == "results":
            return Response(content=_json(results), media_type="application/json")

        # 6️⃣ Normal output
        return Response(content=_json(results), media_type="application/json")

    except HTTPException:
        raise
    except Exception as e:
        log.exception("❌ Error in process_file")
        raise HTTPException(status_code=502, detail=f"Upstream/parse error: {type(e).__name__}: {repr(e)}")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
                log.debug("🧹 Cleaned up temp file: %s", tmp_path)
            except Exception:
                pass
