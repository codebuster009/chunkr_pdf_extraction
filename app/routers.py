from fastapi import APIRouter, File, UploadFile, HTTPException, Query
from fastapi.responses import Response
from chunkr_ai import Chunkr
import tempfile
import os
import json
import logging
from typing import Optional, Any, Dict
import httpx
import asyncio

from .extract_v2 import extract_airline_from_chunks

router = APIRouter()
log = logging.getLogger("chunkr_service")


# -------------------------------------------------------------------
# 1️⃣  Initialize Chunkr client
# -------------------------------------------------------------------
def get_sdk_client() -> Chunkr:
    """Initialize Chunkr SDK client with API key and extended timeouts."""
    API_KEY = "ch_8U2hBpgPC85_4v7RuDVtzq4aUg8zerbSC24DDWWaZEcTx"  # TEMP local test key

    kwargs = {
        "api_key": API_KEY,
        "raise_on_failure": True,
    }

    client = Chunkr(**kwargs)

    if hasattr(client, "_client"):
        timeout = httpx.Timeout(300.0, connect=30.0, read=300.0, write=300.0, pool=30.0)
        client._client = httpx.AsyncClient(
            timeout=timeout,
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
        )
    return client


# -------------------------------------------------------------------
# 2️⃣  Utility: Safely parse Chunkr SDK output
# -------------------------------------------------------------------
def _output_to_dict(output: Any) -> Dict[str, Any]:
    """Coerce Chunkr SDK `output` into a python dict."""
    if hasattr(output, "json") and callable(output.json):
        data = output.json()
        if isinstance(data, (dict, list)):
            return data if isinstance(data, dict) else {"chunks": data}
        if isinstance(data, str):
            return json.loads(data)
    if isinstance(output, dict):
        return output
    if isinstance(output, str):
        return json.loads(output)
    raise TypeError(f"Unsupported output type: {type(output).__name__}")


# -------------------------------------------------------------------
# 3️⃣  Main upload + polling endpoint
# -------------------------------------------------------------------
@router.post("/process/file")
async def process_file(
    file: UploadFile = File(...),
    raw: Optional[str] = Query(
        default=None,
        description="Debug: 'all' to return full Chunkr output; 'chunks' to return only chunks."
    ),
) -> Response:
    sdk = get_sdk_client()
    tmp_path = None
    API_KEY = "ch_8U2hBpgPC85_4v7RuDVtzq4aUg8zerbSC24DDWWaZEcTx"

    try:
        log.info("📄 Received file: %s (content-type: %s)", file.filename, file.content_type)

        # Save upload to temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
            log.info("💾 Saved to temp file: %s (%d bytes)", tmp_path, len(content))

        # Create task using SDK
        log.info("🚀 Creating Chunkr task...")
        task = await sdk.create_task(tmp_path)

        # Extract and log task ID
        task_id = getattr(task, "id", None) or getattr(task, "task_id", None)
        if not task_id:
            raise RuntimeError("Could not determine Chunkr task ID after creation")

        log.info("✅ Task created successfully → Task ID: %s", task_id)
        print(f"🧩 [DEBUG] Created Chunkr Task ID: {task_id}")

        # -------------------------------------------------------------------
        # Poll manually via REST API (correct URL + header confirmed)
        # -------------------------------------------------------------------
        max_wait_seconds = 360  # 6 minutes max
        poll_interval = 10      # poll every 10 seconds
        elapsed = 0
        result = None

        log.info("⏳ Polling Chunkr task %s for completion (max %ds)...", task_id, max_wait_seconds)

        while elapsed < max_wait_seconds:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0),
                headers={"Authorization": API_KEY, "accept": "application/json"}
            ) as client:
                resp = await client.get(f"https://api.chunkr.ai/tasks/{task_id}")
                log.info("🌐 Polled task %s → HTTP %d", task_id, resp.status_code)

                if resp.status_code == 401:
                    raise HTTPException(status_code=401, detail="Invalid or expired Chunkr API key")

                resp.raise_for_status()
                data = resp.json()
                status = data.get("status") or data.get("state")
                log.info("🌀 Task %s current status: %s", task_id, status)

                if status in ("Succeeded", "completed", "done"):
                    log.info("✅ Task %s succeeded after %d seconds", task_id, elapsed)
                    result = data
                    break
                elif status in ("Failed", "Error", "Cancelled"):
                    raise HTTPException(status_code=502, detail=f"Chunkr task failed: {data}")

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        if not result:
            raise HTTPException(
                status_code=504,
                detail=f"Chunkr task {task_id} did not complete within {max_wait_seconds} seconds."
            )

        # -------------------------------------------------------------------
        # Process final result
        # -------------------------------------------------------------------
        status = result.get("status") or result.get("state") or "Succeeded"
        output = result.get("output")
        if not output:
            raise HTTPException(status_code=502, detail="No output from Chunkr task response")

        out_dict = _output_to_dict(output)

        # Summary logging
        chunks = out_dict.get("chunks") or []
        n_chunks = len(chunks) if isinstance(chunks, list) else 0
        n_segments = sum(len(c.get("segments", [])) for c in chunks) if isinstance(chunks, list) else 0
        log.info("📊 Chunkr summary: task=%s chunks=%d segments=%d", task_id, n_chunks, n_segments)

        # Optional dump to temp JSON
        try:
            fd, dump_path = tempfile.mkstemp(prefix="chunkr_raw_", suffix=".json")
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(out_dict, fh, ensure_ascii=False, indent=2)
            log.info("💾 Wrote full raw Chunkr output to %s", dump_path)
        except Exception as dbg:
            log.warning("⚠️ Couldn't write raw dump: %s", dbg)

        # Debug passthrough options
        if raw == "all":
            return Response(
                content=json.dumps(out_dict, ensure_ascii=False, indent=2),
                media_type="application/json",
            )
        if raw == "chunks":
            only_chunks = out_dict.get("chunks", [])
            return Response(
                content=json.dumps(only_chunks, ensure_ascii=False, indent=2),
                media_type="application/json",
            )

        # Extract airline information
        log.info("🔍 Extracting airline information from Chunkr output...")
        final = extract_airline_from_chunks(out_dict)

        return Response(
            content=json.dumps(final, ensure_ascii=False, indent=2),
            media_type="application/json",
        )

    # -------------------------------------------------------------------
    # Error handling + cleanup
    # -------------------------------------------------------------------
    except HTTPException:
        raise
    except Exception as e:
        log.exception("❌ Error in process_file")
        raise HTTPException(
            status_code=502,
            detail=f"Upstream/parse error: {type(e).__name__}: {repr(e)}",
        )
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
                log.debug("🗑️ Cleaned up temp file: %s", tmp_path)
            except Exception as cleanup_err:
                log.warning("⚠️ Failed to clean up temp file: %s", cleanup_err)
