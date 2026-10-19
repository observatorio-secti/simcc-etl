import html
import re
from collections import Counter
from collections.abc import Iterator

import nltk
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.text import strip_accents

logger = get_logger('simcc_etl.routines.build_search_dictionary')

MAX_NGRAM = 4
MIN_FREQUENCY = 5
MIN_UNIGRAM_LENGTH = 4
MAX_TERM_LENGTH = 255
NESTED_RATIO = 0.9
FETCH_BATCH = 5000
INSERT_BATCH = 5000

SEGMENT_BREAK = re.compile(r"[^\w\s'’-]+|\s-+\s")
TOKEN = re.compile(r"[^\W_]+(?:['’][^\W_]+)*")
HAS_DIGIT = re.compile(r'\d')

EXTRA_STOPWORDS = (
    'sobre',
    'após',
    'através',
    'partir',
    'desde',
    'durante',
    'el',
    'la',
    'los',
    'las',
    'en',
    'del',
    'y',
    'un',
    'una',
    'con',
    'su',
    'sus',
    'al',
    'lo',
)

SOURCES = [
    (
        'ARTICLE',
        'SELECT DISTINCT title FROM bibliographic_production '
        "WHERE type = 'ARTICLE' AND title IS NOT NULL;",
    ),
    (
        'BOOK_CHAPTER',
        'SELECT DISTINCT title FROM bibliographic_production '
        "WHERE type = 'BOOK_CHAPTER' AND title IS NOT NULL;",
    ),
    (
        'PATENT',
        'SELECT DISTINCT title FROM patent WHERE title IS NOT NULL;',
    ),
    (
        'SPEAKER',
        'SELECT DISTINCT title FROM participation_events '
        'WHERE title IS NOT NULL;',
    ),
    (
        'ABSTRACT',
        'SELECT DISTINCT abstract FROM researcher WHERE abstract IS NOT NULL;',
    ),
    (
        'BOOK',
        'SELECT DISTINCT title FROM bibliographic_production '
        "WHERE type = 'BOOK' AND title IS NOT NULL;",
    ),
]


def get_stopwords() -> frozenset[str]:
    try:
        nltk.data.find('corpora/stopwords')
    except LookupError:
        nltk.download('stopwords', quiet=True)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords.extend(nltk.corpus.stopwords.words('portuguese'))
    stopwords.extend(EXTRA_STOPWORDS)
    return frozenset(strip_accents(word.lower()) for word in stopwords)


def iter_runs(content: str) -> Iterator[list[str]]:
    content = html.unescape(content).lower()
    for segment in SEGMENT_BREAK.split(content):
        run = []
        for token in TOKEN.findall(segment):
            if HAS_DIGIT.search(token):
                if run:
                    yield run
                run = []
            else:
                run.append(token)
        if run:
            yield run


def extract_terms(
    content: str, stopwords: frozenset[str], max_n: int = MAX_NGRAM
) -> set[str]:
    terms = set()
    for tokens in iter_runs(content):
        is_stopword = [strip_accents(t) in stopwords for t in tokens]
        for start in range(len(tokens)):
            if is_stopword[start]:
                continue
            stop = min(start + max_n, len(tokens))
            for end in range(start, stop):
                if not is_stopword[end]:
                    terms.add(' '.join(tokens[start : end + 1]))
    return terms


def merge_variants(counts: Counter) -> tuple[Counter, dict[str, str]]:
    totals = Counter()
    display = {}
    for term, freq in counts.items():
        key = strip_accents(term)
        totals[key] += freq
        curr = display.get(key)
        if curr is None or (freq, term) > (counts[curr], curr):
            display[key] = term
    return totals, display


def find_nested(
    frequent: dict[str, int], stopwords: frozenset[str]
) -> set[str]:
    largest_container: dict[str, int] = {}
    for key, freq in frequent.items():
        tokens = key.split(' ')
        for size in range(2, len(tokens)):
            for start in range(len(tokens) - size + 1):
                part = tokens[start : start + size]
                if part[0] in stopwords or part[-1] in stopwords:
                    continue
                part_str = ' '.join(part)
                if freq > largest_container.get(part_str, 0):
                    largest_container[part_str] = freq

    return {
        part
        for part, freq in largest_container.items()
        if freq >= NESTED_RATIO * frequent.get(part, freq)
    }


def build_dictionary(
    contents, stopwords: frozenset[str]
) -> list[tuple[str, int]]:
    counts = Counter()
    for content in contents:
        counts.update(extract_terms(content, stopwords))

    totals, display = merge_variants(counts)
    frequent = {k: v for k, v in totals.items() if v >= MIN_FREQUENCY}
    nested = find_nested(frequent, stopwords)

    result = []
    for k, freq in frequent.items():
        if len(k) >= MAX_TERM_LENGTH or k in nested:
            continue
        if ' ' not in k and len(k) < MIN_UNIGRAM_LENGTH:
            continue
        result.append((display[k], freq))

    result.sort(key=lambda item: (-item[1], item[0]))
    return result


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Extrai e constrói o dicionário de busca para autocompletes."""
    logger.info('Starting build_search_dictionary routine')
    stopwords = get_stopwords()
    succeeded = 0
    total_sources = len(SOURCES)

    for doc_type, query in SOURCES:
        logger.debug(f'Processing dictionary for {doc_type}')
        session.execute(
            text('DELETE FROM research_dictionary WHERE type_ = :type_'),
            {'type_': doc_type},
        )
        cursor = session.execute(
            text(query).execution_options(yield_per=FETCH_BATCH)
        ).scalars()
        dictionary = build_dictionary(cursor, stopwords)

        insert_sql = text("""
            INSERT INTO research_dictionary (term, frequency, type_)
            VALUES (:term, :frequency, :type_);
        """)
        for offset in range(0, len(dictionary), INSERT_BATCH):
            batch = [
                {'term': term, 'frequency': freq, 'type_': doc_type}
                for term, freq in dictionary[offset : offset + INSERT_BATCH]
            ]
            session.execute(insert_sql, batch)

        session.commit()
        succeeded += 1

    logger.info(
        f'Finished build_search_dictionary: '
        f'{succeeded}/{total_sources} completed'
    )
    return Stats(found=total_sources, succeeded=succeeded, failed=0)
