from collections import Counter
from unittest.mock import MagicMock, patch

from simcc_etl.routines.build_search_dictionary import (
    extract_terms,
    merge_variants,
)
from simcc_etl.routines.classify_researchers import (
    calculate_rank,
)
from simcc_etl.routines.classify_researchers import (
    run as run_classify,
)
from simcc_etl.routines.import_program_research_lines import (
    run as run_import_lines,
)
from simcc_etl.routines.resolve_cnpq_lattes_id import (
    fetch_lattes_10_id,
)


def test_classify_researchers_rank():
    assert calculate_rank({'first_doc': 0}) == 'E'

    senior_top = {
        'first_doc': 2000,
        'A1': 3,
        'A2': 2,
        'A3': 0,
        'A4': 0,
        'B1': 0,
        'B2': 0,
        'B3': 0,
        'B4': 0,
        'C': 0,
        'SQ': 0,
        'book': 2,
        'book_chapter': 2,
        'd_completed': 5,
        'd_in_progress': 0,
        'm_completed': 0,
        'm_in_progress': 0,
        'patent_granted': 1,
        'patent_not_granted': 0,
        'software': 0,
    }
    assert calculate_rank(senior_top) == 'A+'


def test_classify_researchers_empty():
    session = MagicMock()
    mock_res = MagicMock()
    mock_res.mappings.return_value.all.return_value = []
    session.execute.return_value = mock_res
    stats = run_classify(session)
    assert stats.found == 0


def test_build_search_dictionary_extract_terms():
    stopwords = frozenset({'de', 'da', 'do', 'em'})
    terms = extract_terms('Redes de Computadores', stopwords, max_n=3)
    assert 'redes' in terms
    assert 'computadores' in terms
    assert 'redes de computadores' in terms
    assert 'de' not in terms


def test_build_search_dictionary_merge_variants():
    counts = Counter({'educação': 5, 'educacao': 2})
    totals, display = merge_variants(counts)
    assert totals['educacao'] == 7
    assert display['educacao'] == 'educação'


def test_resolve_cnpq_lattes_id_redirect():
    with patch('httpx.Client.get') as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 302
        target_loc = (
            'http://buscatextual.cnpq.br/buscatextual/visualizacv.do?'
            'id=K470000001'
        )
        mock_resp.headers = {'Location': target_loc}
        mock_get.return_value = mock_resp

        result = fetch_lattes_10_id('1234567890123456')
        assert result == 'K470000001'


def test_import_program_research_lines_empty():
    session = MagicMock()
    mock_res = MagicMock()
    mock_res.mappings.return_value.all.return_value = []
    session.execute.return_value = mock_res
    stats = run_import_lines(session)
    assert stats.failed >= 0
