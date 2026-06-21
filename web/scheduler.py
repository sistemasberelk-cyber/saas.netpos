import asyncio
import logging
from datetime import datetime
from sqlmodel import Session, select
from database.session import engine
from database.models import Tenant, UIConfig, AICredential, decrypt_api_key
from services.gemini_service import GeminiService
import os

logger = logging.getLogger(__name__)

async def run_daily_theme_generation():
    """
    Loops through all tenants in the DB, requests a daily theme from Gemini,
    and updates their UIConfig records for 'pos' page.
    """
    logger.info("Scheduler: Starting daily theme generation run...")
    
    # Use timezone-free utcnow or standard local date
    today_str = datetime.now().strftime("%d de %B de %Y")
    
    with Session(engine) as session:
        try:
            tenants = session.exec(select(Tenant)).all()
        except Exception as e:
            logger.error(f"Scheduler: Failed to query tenants: {e}")
            return
            
        for tenant in tenants:
            logger.info(f"Scheduler: Processing tenant '{tenant.name}' (ID: {tenant.id})")
            
            # 1. Resolve API Key for this tenant
            api_key = None
            cred = session.exec(
                select(AICredential).where(
                    AICredential.tenant_id == tenant.id,
                    AICredential.provider == "gemini"
                )
            ).first()
            
            if cred:
                try:
                    api_key = decrypt_api_key(cred.api_key_enc)
                except Exception as e:
                    logger.error(f"Scheduler: Failed to decrypt API key for tenant {tenant.id}: {e}")
            
            if not api_key:
                api_key = os.getenv("GEMINI_API_KEY")
                
            if not api_key:
                logger.warning(f"Scheduler: Skipping tenant {tenant.id} - No Gemini API Key found.")
                continue
                
            # 2. Call Gemini to generate theme
            try:
                result = await GeminiService.generate_daily_theme(
                    date_str=today_str,
                    api_key=api_key
                )
                theme_data = result.get("theme")
                if not theme_data:
                    logger.warning(f"Scheduler: No 'theme' field in Gemini response for tenant {tenant.id}")
                    continue
            except Exception as e:
                logger.error(f"Scheduler: Gemini API failed for tenant {tenant.id}: {e}")
                continue
                
            # 3. Update or create UIConfig for "pos" page
            try:
                import json
                config = session.exec(
                    select(UIConfig).where(
                        UIConfig.tenant_id == tenant.id,
                        UIConfig.page_name == "pos"
                    )
                ).first()
                
                theme_str = json.dumps(theme_data)
                
                if not config:
                    # Provide default layout when creating a new record
                    default_layout = {
                        "modules": ["header", "catalog", "cart", "footer"],
                        "grid_cols": 12
                    }
                    config = UIConfig(
                        tenant_id=tenant.id,
                        page_name="pos",
                        layout_json=json.dumps(default_layout),
                        theme_json=theme_str,
                        updated_at=datetime.utcnow()
                    )
                else:
                    config.theme_json = theme_str
                    config.updated_at = datetime.utcnow()
                    
                session.add(config)
                session.commit()
                logger.info(f"Scheduler: Updated theme for tenant {tenant.id} with theme: {theme_data}")
            except Exception as e:
                session.rollback()
                logger.error(f"Scheduler: Database save failed for tenant {tenant.id}: {e}")

async def theme_scheduler_loop():
    """
    Continuous background loop running daily.
    """
    logger.info("Scheduler: Initializing theme scheduler task...")
    # Initial startup sleep to let Uvicorn and DB initialize
    await asyncio.sleep(5)
    
    while True:
        try:
            await run_daily_theme_generation()
        except Exception as e:
            logger.error(f"Scheduler: Exception in scheduler loop: {e}")
        
        # Sleep for 24 hours
        await asyncio.sleep(86400)

def start_theme_scheduler():
    """
    Starts the background thread/task.
    """
    loop = asyncio.get_event_loop()
    loop.create_task(theme_scheduler_loop())
