#!/usr/bin/env python3
"""Helper functions to extract text from various attachment types."""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

LIBREOFFICE = os.getenv('LIBREOFFICE_PATH', '/Applications/LibreOffice.app/Contents/MacOS/soffice')

def run_cmd(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command {' '.join(cmd)} failed: {result.stderr}")
    return result.stdout


def extract_text_from_file(path: Path, mime_type: str | None) -> str:
    suffix = path.suffix.lower()
    if suffix in {".txt", ".md", ".csv", ".log"}:
        return path.read_text(errors="ignore")
    if suffix in {".pdf"}:
        return run_cmd(["pdftotext", str(path), "-"])
    if suffix in {".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx"}:
        with tempfile.TemporaryDirectory() as tmp:
            if not Path(LIBREOFFICE).exists():
                raise RuntimeError('LibreOffice binary not found; set LIBREOFFICE_PATH')
            subprocess.run([
                LIBREOFFICE,
                '--headless',
                '--convert-to',
                'pdf',
                str(path),
                '--outdir',
                tmp,
            ], check=True)
            pdf_files = sorted(Path(tmp).glob('*.pdf'))
            if not pdf_files:
                raise RuntimeError('LibreOffice conversion did not produce a PDF')
            tmp_pdf = pdf_files[0]
            return run_cmd(['pdftotext', str(tmp_pdf), '-'])
    if suffix in {".jpg", ".jpeg", ".png", ".tiff", ".heic"}:
        return run_cmd(["tesseract", str(path), "stdout"])
    # fallback: treat as binary
    return ""
