"""Exercise real parser work without importing an unused OCR engine."""

import os
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import pytest

_SRC_DIR = Path(__file__).resolve().parents[3]


@pytest.mark.parametrize(
    ("mode", "sample", "expected_text"),
    [
        ("off", "simple-digital.pdf", "This is a test document."),
        ("off", "multi-page-images.pdf", ""),
        ("off", "simple.png", ""),
        ("auto", "multi-page-digital.pdf", "page 1"),
    ],
)
def test_no_archive_parser_keeps_unused_ocr_unloaded(
    mode: str,
    sample: str,
    expected_text: str,
    tmp_path: Path,
) -> None:
    """Parse real files and produce PDF thumbnails/metadata without OCRmyPDF."""
    configuration = tmp_path / "paperless.conf"
    configuration.touch()
    env = os.environ.copy()
    env.update(
        DJANGO_SETTINGS_MODULE="paperless.settings",
        PAPERLESS_CONFIGURATION_PATH=str(configuration),
        PAPERLESS_DATA_DIR=str(tmp_path),
        PAPERLESS_DBENGINE="sqlite",
        PAPERLESS_DBHOST="",
        PAPERLESS_SECRET_KEY="ocr-import-test-secret",
        PAPERLESS_OCR_MODE=mode,
        PAPERLESS_ARCHIVE_FILE_GENERATION="never",
        PAPERLESS_DISABLE_DBHANDLER="true",
    )
    for name in ("MEDIA_ROOT", "CONSUMPTION_DIR", "SCRATCH_DIR"):
        directory = tmp_path / name.lower()
        directory.mkdir()
        env[f"PAPERLESS_{name}"] = str(directory)
    code = dedent(
        f"""
        import sys
        from pathlib import Path

        import django
        django.setup()
        from django.core.management import call_command
        call_command('migrate', verbosity=0, interactive=False, skip_checks=True)
        from paperless.parsers.tesseract import RasterisedDocumentParser

        def assert_no_ocr(stage):
            leaked = sorted(name for name in sys.modules
                if name == 'ocrmypdf' or name.startswith('ocrmypdf.'))
            assert not leaked, f'Unexpected OCR import after {{stage}}: {{leaked}}'

        from paperless_testing.samples import SHARED_SAMPLES_DIR
        sample = (
            Path('paperless/tests/samples/tesseract')
            if {sample!r} == 'simple.png' else SHARED_SAMPLES_DIR
        ) / {sample!r}
        mime_type = 'application/pdf' if sample.suffix == '.pdf' else 'image/png'
        with RasterisedDocumentParser() as parser:
            assert parser.settings.mode == {mode!r}
            assert parser.settings.archive_file_generation == 'never'
            assert_no_ocr('initialization')
            parser.parse(sample, mime_type, produce_archive=False)
            expected_text = {expected_text!r}
            if expected_text:
                assert expected_text.lower() in parser.get_text().lower()
            else:
                assert parser.get_text() == ''
            assert parser.get_archive_path() is None
            assert_no_ocr('parse')
            metadata = parser.extract_metadata(sample, mime_type)
            assert isinstance(metadata, list)
            if mime_type == 'application/pdf':
                assert parser.get_page_count(sample, mime_type) > 0
                thumbnail = parser.get_thumbnail(sample, mime_type)
                assert thumbnail.name == 'convert.webp'
                from PIL import Image
                with Image.open(thumbnail) as image:
                    assert image.format == 'WEBP'
                    assert image.width > 0 and image.height > 0
            else:
                assert metadata == []
            assert_no_ocr('metadata and thumbnail')
        """,
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=_SRC_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
