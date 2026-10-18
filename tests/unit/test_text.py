from simcc_etl.core.text import (
    clean_cep,
    clean_code,
    clean_lattes_id,
    normalize_name_match,
    normalize_text,
    strip_accents,
)


def test_strip_accents():
    assert strip_accents('Ciência da Computação') == 'Ciencia da Computacao'
    assert strip_accents('ÁÉÍÓÚ àèìòù ãõ ç') == 'AEIOU aeiou ao c'
    assert strip_accents(None) == ''
    assert strip_accents('') == ''


def test_normalize_text():
    assert normalize_text('  Atenção!\nNova Linha   ') == 'atencao! nova linha'
    assert normalize_text(None) == ''
    assert normalize_text(12345) == '12345'


def test_clean_code():
    assert clean_code(' 3100-1017-001P-0 ') == '31001017001P0'
    assert clean_code('abc.123') == 'ABC123'
    assert clean_code(None) == ''


def test_normalize_name_match():
    assert normalize_name_match('Hugo Saba Pereira Cardoso') == (
        'HUGOSABAPEREIRACARDOSO'
    )
    assert normalize_name_match('Eduardo M. de Freitas Jorge!') == (
        'EDUARDOMDEFREITASJORGE'
    )
    assert normalize_name_match(None) == ''


def test_clean_lattes_id():
    assert clean_lattes_id('1966167015825708') == '1966167015825708'
    assert clean_lattes_id(' 1966.1670.1582.5708 ') == '1966167015825708'
    assert clean_lattes_id('123') == '0000000000000123'
    assert clean_lattes_id(None) is None
    assert clean_lattes_id('') is None


def test_clean_cep():
    assert clean_cep('41150-000') == '41150000'
    assert clean_cep('1234') == '00001234'
    assert clean_cep(None) is None
