from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from services.settings_service import SettingsService


class DummySession:
    def __init__(self):
        self.added = []
        self.committed = False
        self.refreshed = False

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.committed = True

    def refresh(self, _obj):
        self.refreshed = True


class DummyUploadFile:
    def __init__(self, filename="", content_type="image/png", file=None):
        self.filename = filename
        self.content_type = content_type
        self.file = file


def test_validate_supported_fields_rejects_unknown_fields():
    with pytest.raises(HTTPException) as exc:
        SettingsService.validate_supported_fields(["company_name", "unexpected"])

    assert exc.value.status_code == 400
    assert "Unsupported settings fields" in str(exc.value.detail)


def test_apply_updates_rejects_blank_company_name():
    session = DummySession()
    settings = SimpleNamespace(
        company_name="Acme",
        printer_name=None,
        label_width_mm=60,
        label_height_mm=40,
        logo_url="/static/images/logo.png",
    )

    with pytest.raises(HTTPException) as exc:
        SettingsService.apply_updates(
            session=session,
            settings=settings,
            company_name="   ",
            logo_file=DummyUploadFile(),
        )

    assert exc.value.status_code == 400
    assert "company_name cannot be empty" == exc.value.detail


def test_apply_updates_updates_supported_fields():
    session = DummySession()
    settings = SimpleNamespace(
        company_name="Old",
        printer_name=None,
        label_width_mm=60,
        label_height_mm=40,
        logo_url="/static/images/logo.png",
    )

    updated = SettingsService.apply_updates(
        session=session,
        settings=settings,
        company_name="  Nuevo Nombre  ",
        printer_name="Zebra",
        label_width_mm=80,
        label_height_mm=50,
        logo_file=DummyUploadFile(),
    )

    assert updated.company_name == "Nuevo Nombre"
    assert updated.printer_name == "Zebra"
    assert updated.label_width_mm == 80
    assert updated.label_height_mm == 50
    assert session.committed is True
    assert session.refreshed is True


def test_apply_updates_rejects_invalid_logo_content_type():
    session = DummySession()
    settings = SimpleNamespace(
        company_name="Old",
        printer_name=None,
        label_width_mm=60,
        label_height_mm=40,
        logo_url="/static/images/logo.png",
    )

    with pytest.raises(HTTPException) as exc:
        SettingsService.apply_updates(
            session=session,
            settings=settings,
            logo_file=DummyUploadFile(filename="script.exe", content_type="application/octet-stream"),
        )

    assert exc.value.status_code == 400
    assert exc.value.detail == "logo_file must be a valid image"
