import csv
import os
from pathlib import Path

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger

logger = get_logger('simcc_etl.routines.classify_article_themes')

DEFAULT_OUTPUT_PATH = Path('storage/production_themes_ai.csv')
DEFAULT_MODEL = 'gpt-4o-mini'

DOCUMENT_THEMES = [
    'Energias Renováveis, Biomassa e Combustíveis Verdes',
    'Energias Renováveis nos Transportes',
    'Mercado de Carbono',
    'Exploração Mineral Sustentável',
    'Eficiência Energética',
]

DOCUMENT_THEME_SUBTHEMES = {
    DOCUMENT_THEMES[0]: [
        'energia solar',
        'energia eólica',
        'energia hidráulica',
        'energia geotérmica',
        'biomassa',
        'biogás',
        'biometano',
        'bioeletricidade',
        'biocombustíveis e combustíveis verdes',
        'hidrogênio renovável',
        'biorrefino e biorrefinarias',
        'aproveitamento energético de resíduos',
        'microrredes e geração renovável',
    ],
    DOCUMENT_THEMES[1]: [
        'mobilidade sustentável',
        'veículos elétricos e de baixa emissão',
        'infraestrutura de abastecimento e recarga',
        'combustíveis renováveis aplicados ao transporte',
        'baterias',
        'reciclagem de baterias de íon-lítio',
        'componentes e tecnologias para veículos de baixa emissão',
    ],
    DOCUMENT_THEMES[2]: [
        'mercado e créditos de carbono',
        'precificação de carbono',
        'certificação',
        'rastreabilidade',
        'mensuração, relato e verificação - MRV',
        'inventários e redução de emissões',
        'metodologias de carbono',
        'monitoramento de emissões',
        'soluções tecnológicas relacionadas ao mercado de carbono',
    ],
    DOCUMENT_THEMES[3]: [
        'minerais críticos',
        'metais prioritários para a transição energética',
        'terras raras',
        'prospecção e caracterização mineral',
        'extração mineral sustentável',
        'beneficiamento',
        'processamento mineral',
        'hidrometalurgia',
        'separação e refino',
        'redução e aproveitamento de resíduos e rejeitos',
        'reciclagem e recuperação de minerais',
        'descarbonização da atividade mineral',
    ],
    DOCUMENT_THEMES[4]: [
        'eficiência energética industrial',
        'geração distribuída',
        'autoprodução de energia',
        'cogeração',
        'aproveitamento energético de resíduos industriais',
        'redução do consumo energético em processos produtivos',
        'tecnologias e equipamentos de alta eficiência',
        'eficiência energética nos transportes',
    ],
}


class ProductionThemes(BaseModel):
    main_theme: str = Field(description='Tema principal associado ao artigo.')
    subthemes: list[str] = Field(description='Subtemas associados ao artigo.')


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Classifica temas de produções científicas via LLM."""
    logger.info('Starting classify_article_themes routine')
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.warning(
            'OPENAI_API_KEY not configured. Skipping classify_article_themes.'
        )
        return Stats(found=0, succeeded=0, failed=0)

    try:
        from langchain_openai import ChatOpenAI
    except ImportError:
        logger.warning('langchain_openai not installed. Skipping.')
        return Stats(found=0, succeeded=0, failed=0)

    sql = text("""
        SELECT DISTINCT ON (
            COALESCE(NULLIF(LOWER(TRIM(bp.doi)), ''), LOWER(TRIM(bp.title)))
        )
            bp.id,
            bp.title,
            oa.abstract,
            bp.doi,
            bp.year_
        FROM bibliographic_production bp
        INNER JOIN openalex_article oa ON oa.article_id = bp.id
        WHERE bp.type = 'ARTICLE'
          AND bp.year_ >= 2019
          AND bp.title IS NOT NULL
          AND oa.abstract IS NOT NULL
        ORDER BY
            COALESCE(NULLIF(LOWER(TRIM(bp.doi)), ''), LOWER(TRIM(bp.title))),
            bp.year_ DESC
        LIMIT 100;
    """)

    rows = session.execute(sql).mappings().all()
    if not rows:
        return Stats(found=0, succeeded=0, failed=0)

    llm = ChatOpenAI(
        model=DEFAULT_MODEL, temperature=0
    ).with_structured_output(ProductionThemes)

    out_path = DEFAULT_OUTPUT_PATH
    out_path.parent.mkdir(parents=True, exist_ok=True)

    succeeded = 0
    failed = 0
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f, fieldnames=['id', 'title', 'doi', 'main_theme', 'subthemes']
        )
        writer.writeheader()

        for row in rows:
            try:
                prompt = (
                    f'Título: {row["title"]}\nResumo: {row["abstract"]}\n'
                    f'Classifique nos temas: {DOCUMENT_THEMES}'
                )
                res: ProductionThemes = llm.invoke(prompt)
                writer.writerow({
                    'id': str(row['id']),
                    'title': row['title'],
                    'doi': row.get('doi'),
                    'main_theme': res.main_theme,
                    'subthemes': '; '.join(res.subthemes),
                })
                succeeded += 1
            except Exception as e:
                logger.warning(f'Error classifying article {row["id"]}: {e}')
                failed += 1

    return Stats(found=len(rows), succeeded=succeeded, failed=failed)
