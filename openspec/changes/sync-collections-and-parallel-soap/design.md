# Design: Sincronização Direta de Coleções e Vínculos Institucionais + Download Paralelo com TTL

## Decisões

1. **Download Paralelo com ThreadPoolExecutor**:
   - `max_workers` configurável via `SOAP_MAX_WORKERS` (default 16).
   - Uso de um pool HTTP (`httpx.Client(transport=HTTPTransport(retries=3))`) compartilhado entre threads.
2. **TTL do Cache baseado em `refresh_interval_days`**:
   - Para cada `lattes_id`, se `(now - mtime) < timedelta(days=refresh_interval_days)`, aproveita o arquivo existente em `storage/xml/current/` sem requisitar ao CNPq/Proxy.
3. **Sincronização Direta do `admin_db` no `sanitize_lattes_data`**:
   - Conecta a `settings.ADMIN_DATABASE_URL` e `settings.WRITE_DATABASE_URL`.
   - Sincroniza `academic.institutions` $\rightarrow$ `public.institution` (por `acronym`).
   - Sincroniza `academic.researcher_institutions` cruzando com `academic.researchers (lattes_id)` e `public.researcher (lattes_id)` $\rightarrow$ `public.researcher_institution`.
   - Sincroniza `academic.collections` $\rightarrow$ `public.collection`.
   - Sincroniza `academic.researcher_collections` $\rightarrow$ `public.researcher_collection`.
4. **Hop Pipeline**:
   - `ImportResearcher.hpl` remove lookup e update de `institution_id`, `status` e `extra_field`.
