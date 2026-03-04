from io import BytesIO
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from services.settings_service import SettingsService, MAX_LOGO_SIZE_BYTES


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
        self.file = file or BytesIO(b"")


def _make_settings():
    return SimpleNamespace(
        company_name="Old",
        printer_name=None,
        label_width_mm=60,
        label_height_mm=40,
        logo_url="/static/images/logo.png",
    )


# --- Field Validation ---

def test_validate_supported_fields_rejects_unknown_fields():
    with pytest.raises(HTTPException) as exc:
        SettingsService.validate_supported_fields(["company_name", "unexpected"])

    assert exc.value.status_code == 400
    assert "Unsupported settings fields" in str(exc.value.detail)


def test_validate_supported_fields_accepts_valid_fields():
    # Should NOT raise
    SettingsService.validate_supported_fields(["company_name", "printer_name", "logo_file"])


# --- company_name ---

def test_apply_updates_rejects_blank_company_name():
    session = DummySession()
    settings = _make_settings()

    with pytest.raises(HTTPException) as exc:
        SettingsService.apply_updates(
            session=session,
            settings=settings,
            company_name="   ",
            logo_file=DummyUploadFile(),
        )

    assert exc.value.status_code == 400
    assert "company_name cannot be empty" == exc.value.detail


# --- Successful Update ---

def test_apply_updates_updates_supported_fields():
    session = DummySession()
    settings = _make_settings()

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


# --- printer_name normalization ---

def test_apply_updates_normalizes_empty_printer_name_to_none():
    session = DummySession()
    settings = _make_settings()
    settings.printer_name = "OldPrinter"

    updated = SettingsService.apply_updates(
        session=session,
        settings=settings,
        printer_name="   ",
        logo_file=DummyUploadFile(),
    )

    assert updated.printer_name is None


# --- Logo MIME type ---

def test_apply_updates_rejects_invalid_logo_content_type():
    session = DummySession()
    settings = _make_settings()

    with pytest.raises(HTTPException) as exc:
        SettingsService.apply_updates(
            session=session,
            settings=settings,
            logo_file=DummyUploadFile(filename="script.exe", content_type="application/octet-stream"),
        )

    assert exc.value.status_code == 400
    assert exc.value.detail == "logo_file must be a valid image (png, jpg, webp, gif, svg)"


# --- Logo size limit ---

def test_apply_updates_rejects_oversized_logo():
    session = DummySession()
    settings = _make_settings()

    # Create a file that exceeds the limit
    oversized_content = b"\x89PNG\r\n\x1a\n" + (b"\x00" * (MAX_LOGO_SIZE_BYTES + 1))

    with pytest.raises(HTTPException) as exc:
        SettingsService.apply_updates(
            session=session,
            settings=settings,
            logo_file=DummyUploadFile(
                filename="huge.png",
                content_type="image/png",
                file=BytesIO(oversized_content),
            ),
        )

    assert exc.value.status_code == 400
    assert "exceeds maximum size" in exc.value.detail


# --- Logo magic bytes ---

def test_apply_updates_rejects_fake_image_wrong_magic_bytes():
    session = DummySession()
    settings = _make_settings()

    # File claims to be PNG (content_type) but has EXE magic bytes
    fake_content = b"MZ\x90\x00" + (b"\x00" * 100)

    with pytest.raises(HTTPException) as exc:
        SettingsService.apply_updates(
            session=session,
            settings=settings,
            logo_file=DummyUploadFile(
                filename="fake.png",
                content_type="image/png",
                file=BytesIO(fake_content),
            ),
        )

    assert exc.value.status_code == 400
    assert "does not match a valid image format" in exc.value.detail
