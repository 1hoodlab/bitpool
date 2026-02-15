# Migration Rollout Plan

## Strategy

- Use only versioned migrations in `migration/src`.
- Never use auto-sync schema in production.
- Apply forward-only migration in CI/CD deployment.

## Rollout steps

1. Deploy baseline migration `m20260215_000001_baseline_schema`.
2. Verify schema, constraints, and indexes using smoke SQL checks.
3. Seed default room and AI node data only after schema validation.
4. Introduce retention/partition migration in a separate release.
5. Add stricter constraints in a later migration after validating production-like data.

## Safety checks

- Backup database before migration.
- Run migration in staging with realistic load first.
- Ensure application version compatibility for both N and N-1 during rollout window.

## Operational notes

- Keep migration idempotent where possible (`IF NOT EXISTS` for create/index).
- Rollback uses `down` migration in reverse dependency order.
