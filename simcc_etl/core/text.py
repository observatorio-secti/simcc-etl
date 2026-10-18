import re
import unicodedata
from typing import Any

LATTES_ID_LENGTH = 16
CEP_LENGTH = 8


def strip_accents(text: str | None) -> str:
    """Remove marcas diacríticas/acentos de uma string."""
    if not text:
        return ''
    normalized = unicodedata.normalize('NFD', str(text))
    return ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')


def normalize_text(text: Any) -> str:
    """Normaliza texto: sem acentos, sem quebras de linha e em minúsculas."""
    if text is None:
        return ''
    s = strip_accents(str(text)).replace('\n', ' ').strip()
    return re.sub(r'\s+', ' ', s).lower()


def clean_code(code: Any) -> str:
    """Mantém apenas caracteres alfanuméricos em maiúsculas (ex: códigos)."""
    if not code:
        return ''
    return re.sub(r'[^A-Za-z0-9]', '', str(code)).upper()


def normalize_name_match(name: Any) -> str:
    """Normaliza nomes para conferência (sem acentos, maiúsculas, A-Z0-9)."""
    if not name:
        return ''
    stripped = strip_accents(str(name)).upper()
    return re.sub(r'[^A-Z0-9]', '', stripped)


def clean_lattes_id(lattes_id: Any) -> str | None:
    """Extrai e valida Lattes ID com 16 dígitos."""
    if not lattes_id:
        return None
    digits = re.sub(r'\D', '', str(lattes_id))
    if len(digits) == LATTES_ID_LENGTH:
        return digits
    if 0 < len(digits) < LATTES_ID_LENGTH:
        return digits.zfill(LATTES_ID_LENGTH)
    return None


def clean_cep(cep: Any) -> str | None:
    """Extrai CEP com 8 dígitos."""
    if not cep:
        return None
    digits = re.sub(r'\D', '', str(cep))
    if len(digits) == CEP_LENGTH:
        return digits
    if 0 < len(digits) < CEP_LENGTH:
        return digits.zfill(CEP_LENGTH)
    return None
