import os
import json
import logging
import requests
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from .config import settings

# Databricks SDK import
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config
except ImportError:
    WorkspaceClient = None
    Config = None

router = APIRouter(prefix="/api/rules", tags=["rules"])
logger = logging.getLogger(__name__)

# Initialize WorkspaceClient if not local
w = None
if not settings.IS_LOCAL:
    if WorkspaceClient:
        try:
            host = os.getenv("DATABRICKS_HOST")
            client_id = os.getenv("DATABRICKS_CLIENT_ID")
            client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

            if host and client_id and client_secret:
                logger.info(f"Initializing WorkspaceClient with host={host}, client_id={client_id}")
                config = Config(
                    host=host,
                    client_id=client_id,
                    client_secret=client_secret
                )
                w = WorkspaceClient(config=config)
            else:
                w = WorkspaceClient()

            logger.info("Databricks WorkspaceClient initialized.")
        except Exception as e:
            logger.error(f"Failed to initialize WorkspaceClient: {e}")


def get_token_from_sdk() -> Optional[str]:
    """Try to extract token from SDK's internal state."""
    if w is None:
        return None

    # Method 1: Direct token attribute
    if hasattr(w.config, 'token') and w.config.token:
        logger.info("Got token from w.config.token")
        return w.config.token

    # Method 2: From _token_source
    if hasattr(w.config, '_token_source'):
        try:
            token_response = w.config._token_source.token()
            if hasattr(token_response, 'access_token'):
                logger.info("Got token from _token_source")
                return token_response.access_token
            elif isinstance(token_response, str):
                return token_response
        except Exception as e:
            logger.debug(f"_token_source failed: {e}")

    # Method 3: From header_factory
    if hasattr(w.config, '_header_factory'):
        try:
            headers = w.config._header_factory()
            if 'Authorization' in headers:
                logger.info("Got token from _header_factory")
                return headers['Authorization'].replace('Bearer ', '')
        except Exception as e:
            logger.debug(f"_header_factory failed: {e}")

    # Method 4: Try authenticate method with different signatures
    try:
        result = w.config.authenticate()
        if isinstance(result, dict) and 'Authorization' in result:
            logger.info("Got token from authenticate()")
            return result['Authorization'].replace('Bearer ', '')
    except Exception as e:
        logger.debug(f"authenticate() failed: {e}")

    # Method 5: Environment variable
    token = os.getenv("DATABRICKS_TOKEN")
    if token:
        logger.info("Got token from DATABRICKS_TOKEN env var")
        return token

    return None


# Local storage setup
if settings.IS_LOCAL:
    if not os.path.exists(settings.VOLUME_PATH):
        logger.warning(f"Local volume path {settings.VOLUME_PATH} does not exist. Using 'rules_mock'.")
        os.makedirs("rules_mock", exist_ok=True)
        STORAGE_PATH = "rules_mock"
    else:
        STORAGE_PATH = settings.VOLUME_PATH
else:
    STORAGE_PATH = settings.VOLUME_PATH


class RuleContent(BaseModel):
    content: Dict[str, Any]


@router.get("", response_model=List[str])
def list_rules():
    """List all rule files in the volume."""
    try:
        if settings.IS_LOCAL or w is None:
            files = [f for f in os.listdir(STORAGE_PATH) if f.endswith(".json")]
            return sorted(files)
        else:
            # Use Databricks SDK (this works fine)
            entries = w.files.list_directory_contents(STORAGE_PATH)
            files = [e.name for e in entries if e.name.endswith(".json")]
            return sorted(files)
    except Exception as e:
        logger.error(f"Error listing rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{filename}")
def get_rule(filename: str):
    """Get a specific rule file content."""
    try:
        if settings.IS_LOCAL:
            file_path = os.path.join(STORAGE_PATH, filename)
            if not os.path.exists(file_path):
                raise HTTPException(status_code=404, detail="Rule not found")

            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            # Get token from SDK and make direct API call
            token = get_token_from_sdk()
            host = os.getenv("DATABRICKS_HOST", "")

            # Ensure host has https:// scheme
            if host and not host.startswith("http"):
                host = f"https://{host}"

            if not token or not host:
                logger.error(f"Token extraction failed. host={host}, token={'present' if token else 'missing'}")
                raise HTTPException(status_code=500, detail="Failed to get authentication token")

            url = f"{host}/api/2.0/fs/files{STORAGE_PATH}/{filename}"
            headers = {"Authorization": f"Bearer {token}"}

            resp = requests.get(url, headers=headers, timeout=30)

            if resp.status_code == 200:
                # Files API returns raw bytes, parse as JSON
                content = resp.content.decode('utf-8')
                return json.loads(content)
            elif resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Rule not found")
            else:
                logger.error(f"Databricks API error: {resp.status_code} - {resp.text}")
                raise HTTPException(status_code=resp.status_code, detail=resp.text)

    except HTTPException:
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from file {filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Invalid JSON in file: {str(e)}")
    except Exception as e:
        logger.error(f"Error reading rule {filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{filename}")
def save_rule(filename: str, rule: RuleContent):
    """Save a rule file to the volume."""
    if not filename.endswith(".json"):
        filename += ".json"

    try:
        json_str = json.dumps(rule.content, indent=2, ensure_ascii=False)

        if settings.IS_LOCAL:
            file_path = os.path.join(STORAGE_PATH, filename)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(json_str)

            size = os.path.getsize(file_path)
            return {
                "message": "Rule saved successfully",
                "filename": filename,
                "path": file_path,
                "size_bytes": size
            }
        else:
            # Get token from SDK and make direct API call
            token = get_token_from_sdk()
            host = os.getenv("DATABRICKS_HOST", "")

            # Ensure host has https:// scheme
            if host and not host.startswith("http"):
                host = f"https://{host}"

            if not token or not host:
                raise HTTPException(status_code=500, detail="Failed to get authentication token")

            url = f"{host}/api/2.0/fs/files{STORAGE_PATH}/{filename}?overwrite=true"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/octet-stream"
            }

            resp = requests.put(url, headers=headers, data=json_str.encode('utf-8'), timeout=30)

            if resp.status_code in [200, 201, 204]:
                return {
                    "message": "Rule saved successfully",
                    "filename": filename,
                    "path": f"{STORAGE_PATH}/{filename}",
                    "size_bytes": len(json_str.encode('utf-8'))
                }
            else:
                logger.error(f"Databricks API error: {resp.status_code} - {resp.text}")
                raise HTTPException(status_code=resp.status_code, detail=resp.text)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error saving rule {filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
