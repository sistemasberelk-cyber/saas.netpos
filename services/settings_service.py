from __future__ import annotations

import os
import shutil
import uuid
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
    SUPPORTED_LOGO_CONTENT_TYPES = {"image/png", "image/jpeg", "image/webp", "image/gif", "image/svg+xml"}

    @staticmethod
    def ensure_admin(user: User) -> None:
        if user.role != "admin":
            raise HTTPException(status_code=403, detail="Admin role required")

    @staticmethod
    def get_or_create_settings(session: Session) -> Settings:
        settings = session.exec(select(Settings)).first()
        if settings:
            return settings

        settings = Settings(company_name="Berel K")
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
            normalized_printer_name = printer_name.strip()
            settings.printer_name = normalized_printer_name or None

        if label_width_mm is not None:
            if label_width_mm <= 0:
                raise HTTPException(status_code=400, detail="label_width_mm must be greater than 0")
            settings.label_width_mm = label_width_mm

        if label_height_mm is not None:
            if label_height_mm <= 0:
                raise HTTPException(status_code=400, detail="label_height_mm must be greater than 0")
            settings.label_height_mm = label_height_mm

        if logo_file and logo_file.filename:
            if logo_file.content_type not in SettingsService.SUPPORTED_LOGO_CONTENT_TYPES:
                raise HTTPException(status_code=400, detail="logo_file must be a valid image")

            _, ext = os.path.splitext(logo_file.filename)
            ext = ext.lower() or ".png"
            file_name = f"logo-{uuid.uuid4().hex}{ext}"
            os.makedirs(os.path.join("static", "images"), exist_ok=True)
            file_location = os.path.join("static", "images", file_name)
            with open(file_location, "wb") as buffer:
                shutil.copyfileobj(logo_file.file, buffer)
            settings.logo_url = f"/{file_location}"

        session.add(settings)
        session.commit()
        session.refresh(settings)
        return settings
