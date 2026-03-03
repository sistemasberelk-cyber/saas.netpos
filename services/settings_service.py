from __future__ import annotations

import os
import shutil
from typing import Iterable, Optional

from fastapi import HTTPException, UploadFile
from sqlmodel import Session, select

from database.models import Settings, User


class SettingsService:
    SUPPORTED_FIELDS = {
        "company_name",
        "printer_name",
        "label_width_mm",
        "label_height_mm",
        "logo_file",
    }

    @staticmethod
    def ensure_admin(user: User) -> None:
        if user.role != "admin":
            raise HTTPException(status_code=403, detail="Admin role required")

    @staticmethod
    def get_or_create_settings(session: Session, tenant_id: Optional[int] = None) -> Settings:
        if tenant_id:
            settings = session.exec(select(Settings).where(Settings.tenant_id == tenant_id)).first()
        else:
            settings = session.exec(select(Settings)).first()
        if settings:
            return settings

        settings = Settings(company_name="Berel K", tenant_id=tenant_id)
        session.add(settings)
        session.commit()
        session.refresh(settings)
        return settings

    @staticmethod
    def validate_supported_fields(received_fields: Iterable[str]) -> None:
        unknown_fields = sorted(set(received_fields) - SettingsService.SUPPORTED_FIELDS)
        if unknown_fields:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported settings fields: {', '.join(unknown_fields)}",
            )

    @staticmethod
    def apply_updates(
        session: Session,
        settings: Settings,
        company_name: Optional[str] = None,
        printer_name: Optional[str] = None,
        label_width_mm: Optional[int] = None,
        label_height_mm: Optional[int] = None,
        logo_file: Optional[UploadFile] = None,
    ) -> Settings:
        if company_name is not None:
            normalized_company_name = company_name.strip()
            if not normalized_company_name:
                raise HTTPException(status_code=400, detail="company_name cannot be empty")
            settings.company_name = normalized_company_name

        if printer_name is not None:
            settings.printer_name = printer_name

        if label_width_mm is not None:
            if label_width_mm <= 0:
                raise HTTPException(status_code=400, detail="label_width_mm must be greater than 0")
            settings.label_width_mm = label_width_mm

        if label_height_mm is not None:
            if label_height_mm <= 0:
                raise HTTPException(status_code=400, detail="label_height_mm must be greater than 0")
            settings.label_height_mm = label_height_mm

        if logo_file and logo_file.filename:
            os.makedirs(os.path.join("static", "images"), exist_ok=True)
            file_name = os.path.basename(logo_file.filename)
            file_location = os.path.join("static", "images", file_name)
            with open(file_location, "wb") as buffer:
                shutil.copyfileobj(logo_file.file, buffer)
            settings.logo_url = f"/{file_location}"

        session.add(settings)
        session.commit()
        session.refresh(settings)
        return settings
