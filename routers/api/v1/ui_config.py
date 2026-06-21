from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import Session, select
from database.session import get_session
from database.models import UIConfig, User, AICredential, decrypt_api_key
from web.dependencies import get_current_user_jwt, require_roles
from services.gemini_service import GeminiService
from pydantic import BaseModel
from datetime import datetime
import json
import os
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

class UIConfigRequest(BaseModel):
    layout: dict
    theme: dict

class UIConfigResponse(BaseModel):
    page_name: str
    layout: dict
    theme: dict
    updated_at: datetime

class UIGenerateRequest(BaseModel):
    page_name: str
    prompt: str

# Default configuration to prevent broken UI
DEFAULT_LAYOUT = {
    "modules": ["header", "catalog", "cart", "footer"],
    "grid_cols": 12
}

DEFAULT_THEME = {
    "primary_color": "#4F46E5",
    "secondary_color": "#10B981",
    "mode": "dark"
}

def get_gemini_api_key(session: Session, tenant_id: int) -> str:
    # 1. Search in AICredential for this tenant
    cred = session.exec(
        select(AICredential).where(
            AICredential.tenant_id == tenant_id,
            AICredential.provider == "gemini"
        )
    ).first()

    if cred:
        try:
            return decrypt_api_key(cred.api_key_enc)
        except Exception as e:
            logger.error(f"Failed to decrypt Gemini API key for tenant {tenant_id}: {e}")

    # 2. Fall back to environment variable
    env_key = os.getenv("GEMINI_API_KEY")
    if env_key:
        return env_key

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="API Key de Gemini no configurada para este Tenant ni en las variables de entorno."
    )

@router.get("/ui-config/{page_name}", response_model=UIConfigResponse)
def get_ui_config(
    page_name: str,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user_jwt)
):
    # Fetch from database, isolated by user's tenant_id
    config = session.exec(
        select(UIConfig).where(
            UIConfig.tenant_id == user.tenant_id,
            UIConfig.page_name == page_name
        )
    ).first()

    if not config:
        # Return standard default embedded in code
        return UIConfigResponse(
            page_name=page_name,
            layout=DEFAULT_LAYOUT,
            theme=DEFAULT_THEME,
            updated_at=datetime.utcnow()
        )

    try:
        parsed_layout = json.loads(config.layout_json)
    except Exception:
        parsed_layout = DEFAULT_LAYOUT

    try:
        parsed_theme = json.loads(config.theme_json)
    except Exception:
        parsed_theme = DEFAULT_THEME

    return UIConfigResponse(
        page_name=config.page_name,
        layout=parsed_layout,
        theme=parsed_theme,
        updated_at=config.updated_at
    )

@router.put("/ui-config/{page_name}", response_model=UIConfigResponse)
def update_ui_config(
    page_name: str,
    data: UIConfigRequest,
    session: Session = Depends(get_session),
    user: User = Depends(require_roles(["admin", "superadmin"]))
):
    # Fetch existing config for this page and tenant
    config = session.exec(
        select(UIConfig).where(
            UIConfig.tenant_id == user.tenant_id,
            UIConfig.page_name == page_name
        )
    ).first()

    layout_str = json.dumps(data.layout)
    theme_str = json.dumps(data.theme)

    if not config:
        config = UIConfig(
            tenant_id=user.tenant_id,
            page_name=page_name,
            layout_json=layout_str,
            theme_json=theme_str,
            updated_at=datetime.utcnow()
        )
    else:
        config.layout_json = layout_str
        config.theme_json = theme_str
        config.updated_at = datetime.utcnow()

    session.add(config)
    session.commit()
    session.refresh(config)

    return UIConfigResponse(
        page_name=config.page_name,
        layout=data.layout,
        theme=data.theme,
        updated_at=config.updated_at
    )

@router.post("/ui/generate", response_model=UIConfigResponse)
async def generate_ui(
    data: UIGenerateRequest,
    session: Session = Depends(get_session),
    user: User = Depends(require_roles(["admin", "superadmin"]))
):
    # 1. Fetch current UIConfig or defaults
    config = session.exec(
        select(UIConfig).where(
            UIConfig.tenant_id == user.tenant_id,
            UIConfig.page_name == data.page_name
        )
    ).first()

    if config:
        try:
            current_layout = json.loads(config.layout_json)
        except Exception:
            current_layout = DEFAULT_LAYOUT
        try:
            current_theme = json.loads(config.theme_json)
        except Exception:
            current_theme = DEFAULT_THEME
    else:
        current_layout = DEFAULT_LAYOUT
        current_theme = DEFAULT_THEME

    # 2. Get API key
    api_key = get_gemini_api_key(session, user.tenant_id)

    # 3. Call Gemini to generate design
    try:
        new_design = await GeminiService.generate_ui_design(
            prompt=data.prompt,
            current_layout=current_layout,
            current_theme=current_theme,
            api_key=api_key
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error al generar diseño con Gemini: {str(e)}"
        )

    # 4. Save and return (Only layout and theme are updated, isolating other transactional tables)
    layout_str = json.dumps(new_design.get("layout", current_layout))
    theme_str = json.dumps(new_design.get("theme", current_theme))

    if not config:
        config = UIConfig(
            tenant_id=user.tenant_id,
            page_name=data.page_name,
            layout_json=layout_str,
            theme_json=theme_str,
            updated_at=datetime.utcnow()
        )
    else:
        config.layout_json = layout_str
        config.theme_json = theme_str
        config.updated_at = datetime.utcnow()

    session.add(config)
    session.commit()
    session.refresh(config)

    return UIConfigResponse(
        page_name=config.page_name,
        layout=new_design.get("layout", current_layout),
        theme=new_design.get("theme", current_theme),
        updated_at=config.updated_at
    )

@router.post("/ui/cron/daily-theme")
async def force_daily_theme(
    page_name: str = "pos",
    session: Session = Depends(get_session),
    user: User = Depends(require_roles(["admin", "superadmin"]))
):
    """
    Triggers daily theme generation manually for the current tenant's UI configuration.
    """
    # 1. Fetch current UIConfig or defaults
    config = session.exec(
        select(UIConfig).where(
            UIConfig.tenant_id == user.tenant_id,
            UIConfig.page_name == page_name
        )
    ).first()

    # 2. Get API key
    api_key = get_gemini_api_key(session, user.tenant_id)

    # 3. Current date representation
    today_str = datetime.now().strftime("%d de %B de %Y")

    # 4. Call Gemini to generate daily theme
    try:
        daily_theme_res = await GeminiService.generate_daily_theme(
            date_str=today_str,
            api_key=api_key
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error al generar tema diario con Gemini: {str(e)}"
        )

    new_theme = daily_theme_res.get("theme", DEFAULT_THEME)
    theme_str = json.dumps(new_theme)

    if not config:
        config = UIConfig(
            tenant_id=user.tenant_id,
            page_name=page_name,
            layout_json=json.dumps(DEFAULT_LAYOUT),
            theme_json=theme_str,
            updated_at=datetime.utcnow()
        )
    else:
        config.theme_json = theme_str
        config.updated_at = datetime.utcnow()

    session.add(config)
    session.commit()
    session.refresh(config)

    return {
        "message": f"Tema diario generado con éxito para la fecha {today_str}",
        "theme": new_theme
    }
