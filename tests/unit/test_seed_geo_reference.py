from unittest.mock import MagicMock

from simcc_etl.routines import seed_geo_reference as sgr


def test_reference_files_have_27_territories_and_all_ba_municipalities():
    names, by_ibge = sgr.load_territories()
    municipalities = sgr.load_municipalities()

    assert sorted(names) == list(range(1, 28))
    assert names[27] == 'COSTA DO DESCOBRIMENTO'
    ba = {m['ibge_code'] for m in municipalities if m['uf'] == 'BA'}
    assert len(ba) == 417
    assert set(by_ibge) == ba
    # Porto Seguro e Eunápolis saíram do Extremo Sul
    assert by_ibge['2925303'] == 27
    assert by_ibge['2910727'] == 27
    assert by_ibge['2931350'] == 7  # Teixeira de Freitas


def test_match_city_codes_normalizes_names_and_prefers_ba():
    municipalities = [
        {'ibge_code': '2913606', 'name': 'Ilhéus', 'uf': 'BA'},
        {'ibge_code': '2910800', 'name': 'Feira de Santana', 'uf': 'BA'},
        {'ibge_code': '2924900', 'name': 'Planalto', 'uf': 'BA'},
        {'ibge_code': '4119806', 'name': 'Planalto', 'uf': 'PR'},
        {'ibge_code': '1100015', 'name': 'Bom Jesus', 'uf': 'PI'},
        {'ibge_code': '4302600', 'name': 'Bom Jesus', 'uf': 'RS'},
    ]
    cities = [
        ('a', 'Ilheus'),
        ('b', 'Feira  De Santana'),
        ('c', 'Planalto'),
        ('d', 'Bom Jesus'),
        ('e', 'Viitoria Da Conquista'),
    ]

    matched, ambiguous = sgr.match_city_codes(cities, municipalities)

    assert matched == {'a': '2913606', 'b': '2910800', 'c': '2924900'}
    assert ambiguous == 1


def test_run_upserts_reference_and_fills_only_missing_codes():
    session = MagicMock()
    row = MagicMock(id='c1')
    row.name = 'Salvador'
    session.execute.return_value = [row]

    stats = sgr.run(session)

    statements = [str(c.args[0]) for c in session.execute.call_args_list]
    assert 'identity_territory' in statements[0]
    assert 'municipality' in statements[1]
    assert 'ibge_code IS NULL' in statements[2]
    assert 'ibge_code IS NULL' in statements[3]
    session.commit.assert_called_once()
    assert (stats.found, stats.succeeded, stats.failed) == (1, 1, 0)
