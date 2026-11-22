"""Publica o write_db no read_db através de um schema de staging.

A cópia nunca toca as tabelas que a API está lendo:

1. Catálogo: lê a estrutura de ``public`` nos dois bancos e aborta se as
   tabelas ou colunas divergirem.
2. Staging: numa transação no read_db, recria ``STAGING_SCHEMA`` com a mesma
   estrutura de ``public``, copia os dados via COPY binário, cria
   constraints, índices e FKs (validadas), recria as visões materializadas a
   partir das definições atuais do read_db e roda ANALYZE. Se a rede cair
   aqui, a transação é desfeita e ``public`` continua intacto.
3. Troca: transação curta, com ``lock_timeout``, que remove as tabelas e MVs
   antigas de ``public`` e move as de staging para ``public``.

Requisitos do usuário do read_db: permissão de CREATE no banco e ser dono
das tabelas e MVs de ``public``. Superusuário não é necessário.
"""

import re
import time
from dataclasses import dataclass, field
from graphlib import TopologicalSorter

import psycopg
import redis
from psycopg import sql
from psycopg.errors import LockNotAvailable
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings

logger = get_logger('simcc_etl.routines.publish_to_read_db')

STAGING_SCHEMA = 'simcc_publish_staging'

EXCLUDED_TABLES = {
    'alembic_version',
    'alembic_version_etl',
    'etl_runs',
    'consistency_reports',
    'mv_refresh_metadata',
}

REDIS_GENERATION_KEY = 'simcc:v2:search:generation'

# Parâmetros de conexão do cliente: falha rápido ao conectar e detecta
# conexões mortas em vez de esperar o timeout de TCP do sistema (horas).
CONNECT_OPTIONS = {
    'connect_timeout': 10,
    'keepalives': 1,
    'keepalives_idle': 30,
    'keepalives_interval': 10,
    'keepalives_count': 3,
    'application_name': 'simcc-etl-publish',
}

# Parâmetros de sessão no servidor: o próprio Postgres encerra a sessão se o
# cliente sumir, liberando qualquer lock que ela segure.
SERVER_SESSION_OPTIONS = (
    '-c tcp_keepalives_idle=30 '
    '-c tcp_keepalives_interval=10 '
    '-c tcp_keepalives_count=3 '
    '-c idle_in_transaction_session_timeout=15min'
)

BUILD_SESSION_SETTINGS = (
    'max_parallel_workers_per_gather',
    'max_parallel_maintenance_workers',
)

SWAP_LOCK_TIMEOUT = '30s'
SWAP_STATEMENT_TIMEOUT = '5min'
SWAP_ATTEMPTS = 3
SWAP_RETRY_DELAY_S = 10.0


class PublishError(RuntimeError):
    """Falha na publicação; o read_db continua com os dados anteriores."""


class SchemaMismatchError(PublishError):
    """As tabelas de public diferem entre write_db e read_db."""


@dataclass
class ReadCatalog:
    """Estrutura de ``public`` no read_db, usada para montar o staging."""

    tables: dict[str, list[tuple[str, str]]]
    # (tabela, nome, tipo p/u/x/f, definição com nomes qualificados)
    constraints: list[tuple[str, str, str, str]] = field(default_factory=list)
    # (relação, definição do índice com nomes qualificados)
    indexes: list[tuple[str, str]] = field(default_factory=list)
    # nome -> SELECT da MV, com nomes resolvidos via search_path=public
    mviews: dict[str, str] = field(default_factory=dict)
    mview_deps: dict[str, set[str]] = field(default_factory=dict)
    owners: dict[str, str] = field(default_factory=dict)
    # (relação, papel ou None para PUBLIC, privilégio, com grant option)
    grants: list[tuple[str, str | None, str, bool]] = field(
        default_factory=list
    )
    # (tipo, relação, nome) de objetos que a troca descartaria em silêncio
    unsupported: list[tuple[str, str, str]] = field(default_factory=list)


@dataclass(slots=True)
class PublishResult:
    tables: int
    mviews: int


_COLUMNS_SQL = """
    SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod)
    FROM pg_class c
    JOIN pg_attribute a ON a.attrelid = c.oid
    WHERE c.relnamespace = 'public'::regnamespace
      AND c.relkind = 'r'
      AND NOT c.relispartition
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attgenerated = ''
    ORDER BY c.relname, a.attnum
"""

_CONSTRAINTS_SQL = """
    SELECT c.relname, con.conname, con.contype, pg_get_constraintdef(con.oid)
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    WHERE c.relnamespace = 'public'::regnamespace
      AND c.relkind = 'r'
      AND con.contype IN ('p', 'u', 'x', 'f')
    ORDER BY c.relname, con.conname
"""

_INDEXES_SQL = """
    SELECT c.relname, pg_get_indexdef(i.indexrelid)
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indrelid
    WHERE c.relnamespace = 'public'::regnamespace
      AND c.relkind IN ('r', 'm')
      AND NOT EXISTS (
          SELECT 1 FROM pg_constraint con
          WHERE con.conindid = i.indexrelid
            AND con.conrelid = i.indrelid
            AND con.contype IN ('p', 'u', 'x')
      )
    ORDER BY c.relname, 2
"""

_MVIEWS_SQL = """
    SELECT c.relname, pg_get_viewdef(c.oid)
    FROM pg_class c
    WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'm'
    ORDER BY c.relname
"""

# Dependências de cada relação (MV) em ``schema`` sobre relações de public.
_VIEW_DEPS_SQL = """
    SELECT DISTINCT v.relname, ref.relname
    FROM pg_rewrite r
    JOIN pg_class v ON v.oid = r.ev_class
    JOIN pg_depend d
      ON d.classid = 'pg_rewrite'::regclass AND d.objid = r.oid
    JOIN pg_class ref ON ref.oid = d.refobjid
    WHERE v.relkind = 'm'
      AND v.relnamespace = to_regnamespace(%s)
      AND ref.relnamespace = 'public'::regnamespace
      AND ref.relkind IN ('r', 'm')
      AND ref.oid <> v.oid
"""

_OWNERS_SQL = """
    SELECT c.relname, pg_get_userbyid(c.relowner)
    FROM pg_class c
    WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'm')
"""

_GRANTS_SQL = """
    SELECT c.relname,
           CASE WHEN a.grantee = 0 THEN NULL
                ELSE pg_get_userbyid(a.grantee) END,
           a.privilege_type,
           a.is_grantable
    FROM pg_class c
    CROSS JOIN LATERAL aclexplode(c.relacl) a
    WHERE c.relnamespace = 'public'::regnamespace
      AND c.relkind IN ('r', 'm')
      AND a.grantee <> c.relowner
    ORDER BY 1, 2, 3
"""

_UNSUPPORTED_SQL = """
    SELECT 'trigger', c.relname, t.tgname
    FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
    WHERE c.relnamespace = 'public'::regnamespace AND NOT t.tgisinternal
    UNION ALL
    SELECT 'policy', c.relname, p.polname
    FROM pg_policy p JOIN pg_class c ON c.oid = p.polrelid
    WHERE c.relnamespace = 'public'::regnamespace
    UNION ALL
    SELECT 'extended statistics', c.relname, s.stxname
    FROM pg_statistic_ext s JOIN pg_class c ON c.oid = s.stxrelid
    WHERE c.relnamespace = 'public'::regnamespace
"""

_INDEX_TARGET_RE = re.compile(
    r'^(CREATE (?:UNIQUE )?INDEX \S+ ON )(?:ONLY )?public\.'
)
_FK_TARGET_RE = re.compile(r'REFERENCES public\.("[^"]+"|[^\s(]+)\(')


def _clean_dsn(url: str) -> str:
    """Converte URL SQLAlchemy em string DSN compatível com psycopg."""
    if url.startswith('postgresql+psycopg://'):
        return url.replace('postgresql+psycopg://', 'postgresql://', 1)
    if url.startswith('postgresql+asyncpg://'):
        return url.replace('postgresql+asyncpg://', 'postgresql://', 1)
    return url


def _connect(dsn: str, **kwargs) -> psycopg.Connection:
    return psycopg.connect(
        _clean_dsn(dsn),
        **CONNECT_OPTIONS,
        options=SERVER_SESSION_OPTIONS,
        **kwargs,
    )


def _unquote(ident: str) -> str:
    if ident.startswith('"') and ident.endswith('"'):
        return ident[1:-1].replace('""', '"')
    return ident


def retarget_index_def(indexdef: str, schema: str) -> str:
    """Troca ``ON public.x`` por ``ON <schema>.x`` numa definição de índice."""
    new_def, count = _INDEX_TARGET_RE.subn(
        lambda m: m.group(1) + sql.Identifier(schema).as_string() + '.',
        indexdef,
        count=1,
    )
    if count != 1:
        raise PublishError(f'Definição de índice inesperada: {indexdef}')
    return new_def


def retarget_fk_def(fkdef: str, schema: str, tables: set[str]) -> str:
    """Aponta a FK para ``schema`` quando a tabela referenciada é publicada.

    FKs para tabelas que ficam em public (ex.: excluídas da cópia) mantêm o
    alvo original.
    """

    def repl(m: re.Match) -> str:
        if _unquote(m.group(1)) not in tables:
            return m.group(0)
        return f'REFERENCES {sql.Identifier(schema).as_string()}.{m.group(1)}('

    return _FK_TARGET_RE.sub(repl, fkdef)


def mview_creation_order(
    mviews: dict[str, str], deps: dict[str, set[str]]
) -> list[str]:
    """Ordena as MVs para que cada uma seja criada depois das que usa."""
    graph = {
        name: {d for d in deps.get(name, set()) if d in mviews}
        for name in mviews
    }
    return list(TopologicalSorter(graph).static_order())


def compare_schemas(
    write_tables: dict[str, list[tuple[str, str]]],
    read_tables: dict[str, list[tuple[str, str]]],
) -> list[str]:
    """Lista as divergências de tabelas e colunas entre os dois bancos."""
    problems = []
    write_names = set(write_tables) - EXCLUDED_TABLES
    read_names = set(read_tables) - EXCLUDED_TABLES
    for t in sorted(write_names - read_names):
        problems.append(f'tabela {t} existe só no write_db')
    for t in sorted(read_names - write_names):
        problems.append(f'tabela {t} existe só no read_db')
    for t in sorted(write_names & read_names):
        w_cols = dict(write_tables[t])
        r_cols = dict(read_tables[t])
        for col in sorted(set(w_cols) - set(r_cols)):
            problems.append(f'{t}.{col} existe só no write_db')
        for col in sorted(set(r_cols) - set(w_cols)):
            problems.append(f'{t}.{col} existe só no read_db')
        for col in sorted(set(w_cols) & set(r_cols)):
            if w_cols[col] != r_cols[col]:
                problems.append(
                    f'{t}.{col}: {w_cols[col]} no write_db, '
                    f'{r_cols[col]} no read_db'
                )
    return problems


def _fetch_columns(cur) -> dict[str, list[tuple[str, str]]]:
    cur.execute(_COLUMNS_SQL)
    tables: dict[str, list[tuple[str, str]]] = {}
    for table, col, typ in cur.fetchall():
        tables.setdefault(table, []).append((col, typ))
    return tables


def _set_search_path(cur, value: str) -> None:
    cur.execute("SELECT set_config('search_path', %s, true)", (value,))


def _read_write_columns(w_conn) -> dict[str, list[tuple[str, str]]]:
    with w_conn.cursor() as cur:
        # Nomes totalmente qualificados, para comparar com o read_db.
        _set_search_path(cur, '')
        return _fetch_columns(cur)


def _read_catalog(r_conn) -> ReadCatalog:
    with r_conn.transaction(), r_conn.cursor() as cur:
        _set_search_path(cur, '')
        tables = {
            t: cols
            for t, cols in _fetch_columns(cur).items()
            if t not in EXCLUDED_TABLES
        }
        cat = ReadCatalog(tables=tables)
        relations = set(tables)

        cur.execute(_CONSTRAINTS_SQL)
        cat.constraints = [r for r in cur.fetchall() if r[0] in relations]

        # Definições das MVs com nomes resolvidos via public, para que, ao
        # recriar com search_path=staging,public, as tabelas apontem para o
        # staging e funções/extensões continuem vindo de public.
        _set_search_path(cur, 'public')
        cur.execute(_MVIEWS_SQL)
        cat.mviews = {
            name: definition.strip().rstrip(';')
            for name, definition in cur.fetchall()
        }
        _set_search_path(cur, '')
        relations |= set(cat.mviews)

        cur.execute(_VIEW_DEPS_SQL, ('public',))
        for mv, ref in cur.fetchall():
            cat.mview_deps.setdefault(mv, set()).add(ref)

        cur.execute(_INDEXES_SQL)
        cat.indexes = [r for r in cur.fetchall() if r[0] in relations]

        cur.execute(_OWNERS_SQL)
        cat.owners = {
            rel: owner for rel, owner in cur.fetchall() if rel in relations
        }

        cur.execute(_GRANTS_SQL)
        cat.grants = [r for r in cur.fetchall() if r[0] in relations]

        cur.execute(_UNSUPPORTED_SQL)
        cat.unsupported = [r for r in cur.fetchall() if r[1] in relations]
    return cat


def _copy_table(w_conn, r_conn, table: str, columns: list[str]) -> None:
    cols = sql.SQL(', ').join(sql.Identifier(c) for c in columns)
    copy_out = sql.SQL('COPY public.{} ({}) TO STDOUT (FORMAT BINARY)').format(
        sql.Identifier(table), cols
    )
    copy_in = sql.SQL('COPY {}.{} ({}) FROM STDIN (FORMAT BINARY)').format(
        sql.Identifier(STAGING_SCHEMA), sql.Identifier(table), cols
    )
    with (
        w_conn.cursor() as w_cur,
        r_conn.cursor() as r_cur,
        w_cur.copy(copy_out) as out,
        r_cur.copy(copy_in) as inp,
    ):
        for data in out:
            inp.write(data)


def _build_staging(w_conn, r_conn, cat: ReadCatalog) -> dict[str, int]:
    staging = sql.Identifier(STAGING_SCHEMA)
    table_names = set(cat.tables)
    mv_durations_ms: dict[str, int] = {}

    def rel(name: str) -> sql.Composed:
        return sql.SQL('{}.{}').format(staging, sql.Identifier(name))

    with r_conn.transaction(), r_conn.cursor() as cur:
        # Sem workers paralelos na montagem: eles trocam dados por memória
        # compartilhada (/dev/shm), que em containers Docker tem 64 MB por
        # padrão e estoura na criação das MVs. Vale só para esta transação.
        for setting in BUILD_SESSION_SETTINGS:
            cur.execute("SELECT set_config(%s, '0', true)", (setting,))

        cur.execute(
            sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(staging)
        )
        cur.execute(sql.SQL('CREATE SCHEMA {}').format(staging))

        logger.info(f'Copiando {len(cat.tables)} tabelas para o staging')
        for table, columns in cat.tables.items():
            cur.execute(
                sql.SQL(
                    'CREATE TABLE {} (LIKE public.{} '
                    'INCLUDING ALL EXCLUDING INDEXES EXCLUDING STATISTICS)'
                ).format(rel(table), sql.Identifier(table))
            )
            _copy_table(w_conn, r_conn, table, [c for c, _ in columns])
            logger.debug(f'Tabela {table} copiada')
        # A leitura do write_db termina aqui; não segura o snapshot enquanto
        # o read_db cria índices e MVs.
        w_conn.rollback()

        logger.info('Criando constraints, índices e FKs no staging')
        for table, name, contype, definition in cat.constraints:
            if contype == 'f':
                continue
            cur.execute(
                sql.SQL('ALTER TABLE {} ADD CONSTRAINT {} {}').format(
                    rel(table), sql.Identifier(name), sql.SQL(definition)
                )
            )
        for relation, indexdef in cat.indexes:
            if relation in table_names:
                cur.execute(retarget_index_def(indexdef, STAGING_SCHEMA))
        for table, name, contype, definition in cat.constraints:
            if contype != 'f':
                continue
            cur.execute(
                sql.SQL('ALTER TABLE {} ADD CONSTRAINT {} {}').format(
                    rel(table),
                    sql.Identifier(name),
                    sql.SQL(
                        retarget_fk_def(
                            definition, STAGING_SCHEMA, table_names
                        )
                    ),
                )
            )

        logger.info(f'Criando {len(cat.mviews)} visões materializadas')
        cur.execute(
            sql.SQL('SET LOCAL search_path TO {}, public').format(staging)
        )
        for mv in mview_creation_order(cat.mviews, cat.mview_deps):
            logger.debug(f'Criando MV {mv}')
            t0 = time.perf_counter()
            cur.execute(
                sql.SQL('CREATE MATERIALIZED VIEW {} AS {} WITH DATA').format(
                    rel(mv), sql.SQL(cat.mviews[mv])
                )
            )
            mv_durations_ms[mv] = int((time.perf_counter() - t0) * 1000)
        for relation, indexdef in cat.indexes:
            if relation in cat.mviews:
                cur.execute(retarget_index_def(indexdef, STAGING_SCHEMA))
        cur.execute('SET LOCAL search_path TO DEFAULT')

        # Nenhuma MV nova pode depender de algo que será removido na troca.
        cur.execute(_VIEW_DEPS_SQL, (STAGING_SCHEMA,))
        leaks = [
            (mv, ref)
            for mv, ref in cur.fetchall()
            if ref in table_names or ref in cat.mviews
        ]
        if leaks:
            raise PublishError(
                'MVs do staging apontam para public: '
                + ', '.join(f'{mv} -> {ref}' for mv, ref in leaks)
            )

        cur.execute('SELECT current_user')
        current_user = cur.fetchone()[0]
        for relation, owner in cat.owners.items():
            if owner == current_user:
                continue
            kind = 'MATERIALIZED VIEW' if relation in cat.mviews else 'TABLE'
            cur.execute(
                sql.SQL('ALTER {} {} OWNER TO {}').format(
                    sql.SQL(kind), rel(relation), sql.Identifier(owner)
                )
            )
        for relation, grantee, privilege, grantable in cat.grants:
            cur.execute(
                sql.SQL('GRANT {} ON TABLE {} TO {}{}').format(
                    sql.SQL(privilege),
                    rel(relation),
                    sql.Identifier(grantee) if grantee else sql.SQL('PUBLIC'),
                    sql.SQL(' WITH GRANT OPTION' if grantable else ''),
                )
            )

        logger.info('Atualizando estatísticas do staging (ANALYZE)')
        for relation in [*cat.tables, *cat.mviews]:
            cur.execute(sql.SQL('ANALYZE {}').format(rel(relation)))

    return mv_durations_ms


def _swap(r_conn, cat: ReadCatalog) -> None:
    staging = sql.Identifier(STAGING_SCHEMA)

    def public_list(names) -> sql.Composed:
        return sql.SQL(', ').join(
            sql.SQL('public.{}').format(sql.Identifier(n)) for n in names
        )

    for attempt in range(1, SWAP_ATTEMPTS + 1):
        try:
            with r_conn.transaction(), r_conn.cursor() as cur:
                cur.execute(
                    "SELECT set_config('lock_timeout', %s, true), "
                    "set_config('statement_timeout', %s, true)",
                    (SWAP_LOCK_TIMEOUT, SWAP_STATEMENT_TIMEOUT),
                )
                if cat.mviews:
                    cur.execute(
                        sql.SQL('DROP MATERIALIZED VIEW {}').format(
                            public_list(cat.mviews)
                        )
                    )
                # Sem CASCADE: se algo fora da publicação depender dessas
                # tabelas, a troca falha e public fica como estava.
                cur.execute(
                    sql.SQL('DROP TABLE {}').format(public_list(cat.tables))
                )
                for table in cat.tables:
                    cur.execute(
                        sql.SQL('ALTER TABLE {}.{} SET SCHEMA public').format(
                            staging, sql.Identifier(table)
                        )
                    )
                for mv in cat.mviews:
                    cur.execute(
                        sql.SQL(
                            'ALTER MATERIALIZED VIEW {}.{} SET SCHEMA public'
                        ).format(staging, sql.Identifier(mv))
                    )
                cur.execute(sql.SQL('DROP SCHEMA {}').format(staging))
            return
        except LockNotAvailable as e:
            if attempt == SWAP_ATTEMPTS:
                raise PublishError(
                    f'Não foi possível obter os locks da troca após '
                    f'{SWAP_ATTEMPTS} tentativas; read_db não foi alterado.'
                ) from e
            logger.warning(
                f'Troca bloqueada por consultas em andamento '
                f'(tentativa {attempt}/{SWAP_ATTEMPTS}); '
                f'nova tentativa em {SWAP_RETRY_DELAY_S:.0f}s'
            )
            time.sleep(SWAP_RETRY_DELAY_S)


def _drop_staging(r_conn) -> None:
    try:
        if r_conn.closed or r_conn.broken:
            return
        r_conn.rollback()
        with r_conn.transaction(), r_conn.cursor() as cur:
            cur.execute(
                sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(
                    sql.Identifier(STAGING_SCHEMA)
                )
            )
    except Exception as e:  # limpeza best-effort; a próxima execução refaz
        logger.warning(f'Não foi possível remover o staging: {e}')


_UPSERT_MV_REFRESH_METADATA_SQL = """
    INSERT INTO public.mv_refresh_metadata (view_name, refreshed_at, duration_ms)
    VALUES (%s, now(), %s)
    ON CONFLICT (view_name) DO UPDATE
    SET refreshed_at = EXCLUDED.refreshed_at,
        duration_ms = EXCLUDED.duration_ms
"""


def _record_mv_refresh_metadata(cur, durations_ms: dict[str, int]) -> None:
    """Registra o momento e a duração do refresh em mv_refresh_metadata."""
    if not durations_ms:
        return
    cur.execute(
        "SELECT to_regclass('public.mv_refresh_metadata') IS NOT NULL"
    )
    row = cur.fetchone()
    if not row or not row[0]:
        logger.debug(
            'Tabela public.mv_refresh_metadata ausente; '
            'pulando registro de metadados'
        )
        return
    for view_name, duration_ms in durations_ms.items():
        cur.execute(_UPSERT_MV_REFRESH_METADATA_SQL, (view_name, duration_ms))


def publish_via_staging(write_dsn: str, read_dsn: str) -> PublishResult:
    """Copia o write_db para o staging do read_db e troca com public."""
    with (
        _connect(write_dsn) as w_conn,
        _connect(read_dsn) as r_conn,
    ):
        # Um único snapshot para todas as tabelas copiadas.
        w_conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        w_conn.read_only = True

        cat = _read_catalog(r_conn)
        problems = compare_schemas(_read_write_columns(w_conn), cat.tables)
        if problems:
            raise SchemaMismatchError(
                'write_db e read_db têm schemas diferentes; aplique as '
                'migrations nos dois bancos antes de publicar: '
                + '; '.join(problems)
            )
        if cat.unsupported:
            raise PublishError(
                'Objetos que a troca de tabelas descartaria: '
                + ', '.join(f'{k} {n} em {r}' for k, r, n in cat.unsupported)
            )

        mv_durations_ms = _build_staging(w_conn, r_conn, cat)
        logger.info('Staging pronto; trocando com public')
        try:
            _swap(r_conn, cat)
        except Exception:
            _drop_staging(r_conn)
            raise
        with r_conn.transaction(), r_conn.cursor() as cur:
            _record_mv_refresh_metadata(cur, mv_durations_ms)
        return PublishResult(tables=len(cat.tables), mviews=len(cat.mviews))


def refresh_materialized_views(read_dsn: str) -> int:
    """Atualiza todas as MVs de public, em ordem de dependência."""
    with _connect(read_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(_MVIEWS_SQL)
        mviews = {name: definition for name, definition in cur.fetchall()}
        cur.execute(_VIEW_DEPS_SQL, ('public',))
        deps: dict[str, set[str]] = {}
        for mv, ref in cur.fetchall():
            deps.setdefault(mv, set()).add(ref)

        for mv in mview_creation_order(mviews, deps):
            ident = sql.SQL('public.{}').format(sql.Identifier(mv))
            t0 = time.perf_counter()
            try:
                refresh = sql.SQL('REFRESH MATERIALIZED VIEW CONCURRENTLY {}')
                cur.execute(refresh.format(ident))
            except psycopg.Error as e:
                logger.warning(
                    f'Refresh concorrente de {mv} falhou ({e}); '
                    'usando refresh bloqueante'
                )
                cur.execute(
                    sql.SQL('REFRESH MATERIALIZED VIEW {}').format(ident)
                )
            duration_ms = int((time.perf_counter() - t0) * 1000)
            _record_mv_refresh_metadata(cur, {mv: duration_ms})
        return len(mviews)


def bump_redis_cache(redis_url: str) -> bool:
    """Incrementa a geração de cache de busca no Redis."""
    try:
        r = redis.Redis.from_url(redis_url, socket_timeout=2.0)
        with r:
            gen = r.incr(REDIS_GENERATION_KEY)
            logger.info(f'Redis search cache generation bumped to {gen}')
            return True
    except Exception as e:
        logger.warning(f'Could not bump Redis search cache: {e}')
        return False


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Publica dados no banco de leitura e invalida o cache de busca."""
    logger.info('Starting publish_to_read_db routine')
    settings = Settings()

    write_dsn = settings.WRITE_DATABASE_URL
    read_dsn = settings.READ_DATABASE_URL

    if _clean_dsn(write_dsn) == _clean_dsn(read_dsn):
        logger.info('WRITE_DB e READ_DB são o mesmo banco; só atualizando MVs')
        tables = 0
        mviews = refresh_materialized_views(read_dsn)
    else:
        result = publish_via_staging(write_dsn, read_dsn)
        tables, mviews = result.tables, result.mviews

    if settings.REDIS_URL:
        bump_redis_cache(settings.REDIS_URL)

    logger.info(
        f'Finished publish_to_read_db: {tables} tabelas e '
        f'{mviews} MVs publicadas'
    )
    return Stats(found=tables + mviews, succeeded=tables + mviews, failed=0)
