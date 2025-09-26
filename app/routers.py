from fastapi import APIRouter, File, UploadFile, HTTPException
from chunkr_ai import Chunkr
import tempfile, os, logging

router = APIRouter()
log = logging.getLogger("chunkr_service")

def get_sdk_client() -> Chunkr:
    # Prefer passing the key explicitly so we know which value is used.
    # If you set CHUNKR_URL for self-host, pass it too.
    from .config import settings
    if settings.URL:
        return Chunkr(api_key=settings.API_KEY, chunkr_url=settings.URL, raise_on_failure=True)
    return Chunkr(api_key=settings.API_KEY, raise_on_failure=True)

@router.post("/process/file")
async def process_file(file: UploadFile = File(...)) -> dict:
    sdk = get_sdk_client()
    tmp_path = None
    try:
        # persist the upload to disk (SDK accepts path/File/URL/base64)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name

        # Use the async flow explicitly for clearer control and errors:
        # 1) create the task immediately
        task = await sdk.create_task(tmp_path)       # <- returns a TaskResponse (not yet completed)
        # 2) wait until it leaves Starting/Processing
        result = await task.poll()                   # <- completed/failed/cancelled

        # If your client was built with raise_on_failure=True, a failed task raises.
        # Still, we'll check explicitly and provide a good error to the caller.
        status = getattr(result, "status", None)
        output = getattr(result, "output", None)
        if status not in ("Succeeded", "completed", "done"):
            # Try to fetch any useful info from result
            # Many SDKs expose an error/ message field; if not, repr the object.
            err = getattr(result, "error", None) or getattr(result, "message", None)
            raise HTTPException(status_code=502, detail=f"Chunkr task status={status} error={err or 'unknown'}")

        # Return something useful. Adapt this to how you want to consume chunks/output.
        # If result.output has a .json() helper per docs:
        payload = output.json() if hasattr(output, "json") else output
        return {
            "task_id": getattr(result, "task_id", None),
            "status": status,
            "output": payload,
        }

    except HTTPException:
        raise
    except Exception as e:
        # Give maximum debuggability: type + repr
        log.exception("SDK flow failed")
        raise HTTPException(status_code=502, detail=f"SDK upload failed: {type(e).__name__}: {repr(e)}")
    finally:
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except Exception:
            pass
