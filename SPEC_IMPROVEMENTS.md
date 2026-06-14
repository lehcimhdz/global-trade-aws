# Spec — Mejoras al proyecto `global-trade-aws`

Documento vivo. Cada cambio listado abajo se aplica en este mismo PR / commit.

---

## P0 · Bugs críticos de Terraform (rompen `terraform validate`)

### P0-1 · `local.tags` indefinido

**Síntoma**: 12 referencias a `local.tags` en `athena.tf`, `cloudwatch.tf`, `glue.tf`, `macie.tf`, `api.tf`, `quicksight.tf`. Solo existe `local.common_tags` en `main.tf`. `terraform validate` falla con `Reference to undeclared local value "tags"`.

**Fix**: Añadir alias `tags = local.common_tags` al bloque `locals` de `main.tf`. Mantiene compatibilidad con todas las llamadas existentes.

**Verificación**: `grep -c "local.tags" terraform/*.tf` debe seguir devolviendo los 12 hits y `terraform validate` no debe lanzar `undeclared local value`.

### P0-2 · `data "aws_caller_identity" "current"` duplicado

**Síntoma**: Declarado en `macie.tf:141` y en `quicksight.tf:18`. `terraform validate` falla con `Duplicate data "aws_caller_identity" configuration`.

**Fix**: Mover la declaración a `main.tf` (junto al provider) y eliminar de `macie.tf` y `quicksight.tf`. Las 5 referencias `data.aws_caller_identity.current.account_id` continúan funcionando.

**Verificación**: `grep -c 'data "aws_caller_identity"' terraform/*.tf` debe devolver 1 sola línea (en `main.tf`).

---

## P1 · Endurecimiento de seguridad

### P1-1 · API trade SQL — parametrización vía Athena `ExecutionParameters`

**Síntoma**: `api/main.py` valida inputs por regex y los interpola en f-strings (`f"period = '{iso}'"`). La validación previene inyección hoy, pero el patrón es frágil: cualquier nuevo filtro que omita el regex abre SQLi.

**Fix**:
- `api/athena.py::run_query`: añadir parámetro opcional `parameters: list[str] | None`; cuando se pasa, se reenvía como `ExecutionParameters` a `start_query_execution`.
- `api/main.py`: convertir cada `f"col = '{val}'"` a `"col = ?"` y empujar `val` al array `params`. `LIMIT` se mantiene como literal (Athena no acepta `LIMIT ?`); FastAPI ya valida `Query(ge=…, le=…)` así que el int es seguro.

**Verificación**: `tests/unit/test_api.py` se actualiza: en vez de `assert "period = '2022'" in sql` se valida `assert "period = ?" in sql` + `assert "2022" in params`.

### P1-2 · Lambda packaging — eliminar mismatch Make ↔ Terraform

**Síntoma**: `api.tf` declara `data "archive_file" "api"` que zipea solo `api/*.py` (sin dependencias FastAPI/Mangum). El `make api-build` produce `build/api.zip` con dependencias en `build/lambda_pkg/`. Cada `terraform plan` regenera el zip mal-formado y sobrescribe el del Makefile.

**Fix**: Eliminar `data "archive_file"`. La función Lambda apunta directamente a `${path.module}/../build/api.zip` y usa `filebase64sha256(...)` para el hash. Documentar en el header del recurso que `make api-build` es prerequisito (igual que dicta el README).

**Verificación**: `tests/unit/test_api_terraform.py` se actualiza — se eliminan `TestArchiveFile.*` y `test_uses_data_archive_file`. Se añade un test que confirme la referencia al zip pre-construido.

---

## P2 · Pulido y consistencia

### P2-1 · `Makefile` — encabezado duplicado "Trade API"

**Síntoma**: Líneas 194-198 y 206-209 ambas tienen el banner `── Trade API ──`. La primera está vacía.

**Fix**: Eliminar el encabezado huérfano de líneas 194-198.

### P2-2 · README — claim incorrecto sobre `retries`

**Síntoma**: README dice _"All DAGs share: `retries=1`"_. El código tiene `retries=2` en los 8 DAGs de ingestión (`comtrade_preview*`, `comtrade_world_share`, `comtrade_metadata`, `comtrade_mbs`, `comtrade_da*`, `comtrade_releases`) y `retries=1` solo en `comtrade_dbt` y `comtrade_backfill`.

**Fix**: Actualizar README para describir el valor real:
> All DAGs share: `catchup=False`, `on_failure_callback`, `sla_miss_callback`. Ingestion DAGs use `retries=2`, `retry_delay=5 min`; `comtrade_dbt` and `comtrade_backfill` use `retries=1`, `retry_delay=10 min`.

---

## Cambios fuera de scope (anotados, no aplicados)

- **Iceberg vacuum scheduled** — requiere DAG nuevo + tabla de retención; demasiado alcance.
- **Auth en la Function URL** — requiere flag nuevo + cambio en CORS; deferimos.
- **QuickSight refresh schedule** — `aws_quicksight_refresh_schedule` resource pequeño pero opinable; no aplicado.
- **dbt incremental** — refactor de modelos silver; out of scope para un PR de fixes.
- **API async Athena polling** — cambio arquitectónico; no aplicado.

---

## Orden de ejecución y verificación

1. Aplicar P0-1 y P0-2 (Terraform).
2. Aplicar P2-1 y P2-2 (cosmético).
3. Aplicar P1-1 (API param queries + tests).
4. Aplicar P1-2 (Lambda packaging + tests).
5. Verificar:
   - `grep` confirma la ausencia de duplicados y la presencia de `local.tags`.
   - `pytest tests/unit/test_api.py tests/unit/test_api_terraform.py` pasa.
   - `pytest tests/unit/test_cloudwatch_terraform.py` (que asserta `local.tags`) sigue verde.
