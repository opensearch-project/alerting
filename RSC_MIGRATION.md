# Resource-Sharing Framework Migration (design)

**Status:** design draft — not yet implemented.

## Problem

When alerting onboards to the security plugin's Resource-Sharing Framework (RSC),
clusters that already contain monitors and workflows written under the legacy
`user.backend_roles` auth model need to be migrated so the framework can gate
access.

Two things must happen for existing docs:

1. **Discriminator backfill.** The framework's `ResourceProvider.typeField`
   points at `resource_type`. Existing docs in `.opendistro-alerting-config`
   don't have that field, so `postIndex` / access-check paths skip them.
2. **Sharing entry seeding.** The framework's `.opendistro-alerting-config-sharing`
   index must contain a share record per monitor/workflow with the original
   author as owner and (optionally) the legacy `backend_roles` mapped to a
   default access level.

Without these, every existing monitor/workflow becomes inaccessible to every
non-admin user the moment RSC is enabled — the "unexpected 403" scenario the
reporting PR flagged.

## Prior art

- **ml-commons #3715** — no in-plugin migrator. Users must call the security
  plugin's `POST /_plugins/_security/api/resources/migrate` endpoint.
- **flow-framework #1251** — same. Two resource types share config-index;
  users hit `_migrate` after enabling the feature flag.
- **reporting #1141** — same. Documented as an admin post-enablement step.

## Recommended flow (two steps, both admin-only)

### Step 1 — alerting-side backfill

`POST /_plugins/_alerting/_migrate_to_rsc`

Runs an update-by-query on `.opendistro-alerting-config`:

```
POST .opendistro-alerting-config/_update_by_query?refresh=true
{
  "script": {
    "source": """
      if (ctx._source.containsKey('monitor')) {
        ctx._source.resource_type = 'monitor';
      } else if (ctx._source.containsKey('workflow')) {
        ctx._source.resource_type = 'workflow';
      } else {
        ctx.op = 'noop';   // metadata / other docs
      }
    """,
    "lang": "painless"
  },
  "query": { "bool": { "must_not": { "exists": { "field": "resource_type" } } } }
}
```

Response: `{ updated: <n>, skipped_metadata: <m>, noops: <k> }`.

This endpoint must be gated on `all_access` (via `plugins.security.restapi.roles_enabled`).
Failure modes are the usual UBQ ones (conflicts on concurrent writes, version
conflicts). Retry-safe because the script is idempotent: if `resource_type`
already exists, the `must_not exists` clause excludes the doc.

### Step 2 — security-side sharing seed

Admin calls the security plugin's built-in endpoint:

```
POST /_plugins/_security/api/resources/migrate
{
  "source_index": ".opendistro-alerting-config",
  "type_field": "resource_type",
  "username_path": "/monitor/user/name",     // or /workflow/user/name — see note
  "backend_roles_path": "/monitor/user/backend_roles",
  "default_access_level": {
    "monitor": "alerting_full_access",
    "workflow": "alerting_full_access"
  }
}
```

**Note on `username_path`:** because monitor and workflow docs wrap their user
under different keys (`monitor.user.name` vs `workflow.user.name`), a single
JSON pointer can't address both. Two options:

- **A.** Call the endpoint twice — once with the monitor-scoped filter and
  paths, once for workflow. Requires the security migrate API to support a
  `filter` clause narrowing which docs it operates on.
- **B.** Have alerting's step-1 backfill *also* copy `user.name` and
  `user.backend_roles` to top-level fields, e.g. `_migration_user_name` and
  `_migration_backend_roles`, then the security migrate call can use a single
  path. Adds two throwaway fields to every doc — small cost.

Recommended: **B**. Keeps the security-side call to a single invocation and
avoids depending on any hypothetical `filter` feature.

## What our step-1 endpoint script should actually look like (approach B)

```painless
if (ctx._source.containsKey('monitor')) {
    ctx._source.resource_type = 'monitor';
    ctx._source._migration_user_name = ctx._source.monitor?.user?.name;
    ctx._source._migration_backend_roles = ctx._source.monitor?.user?.backend_roles;
} else if (ctx._source.containsKey('workflow')) {
    ctx._source.resource_type = 'workflow';
    ctx._source._migration_user_name = ctx._source.workflow?.user?.name;
    ctx._source._migration_backend_roles = ctx._source.workflow?.user?.backend_roles;
} else {
    ctx.op = 'noop';
}
```

Then the security migrate call uses:
- `username_path = "/_migration_user_name"`
- `backend_roles_path = "/_migration_backend_roles"`

After the security migrate call succeeds, admins can (optionally) run a second
update-by-query to strip the two `_migration_*` fields.

## Contract / edge cases

- **Metadata docs** (`<monitorId>-metadata` in the same index) — script `noop`s
  them. They're not shareable resources.
- **Docs authored by system/legacy jobs** with no `user` field — `username_path`
  resolution will return null; security's migrate reports them under
  `skippedNoOwner`. Admin gets a list and must decide whether to assign a
  synthetic owner or accept that those docs remain inaccessible.
- **Rerunning the endpoint** is safe. Step 1's `must_not exists` clause skips
  already-migrated docs. Step 2 is not idempotent in the security plugin (it
  creates duplicate sharing entries) — document that admins should only run it
  once.
- **Post-migration writes** — from PR onwards, every new monitor/workflow write
  emits `resource_type` (via `with_resource_type=true` in alerting's write
  path) and triggers `postIndex` to record the sharing entry automatically.
  No further admin action needed.

## Implementation checklist

- [ ] `TransportMigrateToRscAction` — HandledTransportAction that submits the
      UBQ request via `client.execute(UpdateByQueryAction.INSTANCE, ...)`.
- [ ] `RestMigrateToRscAction` — REST handler at
      `POST /_plugins/_alerting/_migrate_to_rsc`, admin-only.
- [ ] Action type constant `AlertingActions.MIGRATE_TO_RSC_ACTION_NAME` in
      common-utils.
- [ ] Wire into `AlertingPlugin.getRestHandlers` and
      `AlertingPlugin.getActions`.
- [ ] Add the cluster action to the `alerting_full_access` role or a new
      dedicated `alerting_migrate` role. Or gate via SecurityRestApi (admin).
- [ ] IT: create legacy-shape docs, hit the endpoint, verify docs get
      `resource_type` and `_migration_user_name`/`_migration_backend_roles`,
      then hit security's migrate endpoint and verify sharing entries land.
- [ ] `docs/rsc-migration.md` — user-facing runbook: step 1, step 2, verify,
      cleanup.

## Non-goals

- Automatic migration on plugin startup. Too risky (unattended, long-running
  UBQ on production data). Admin-triggered only.
- Migrating alerts, findings, comments, destinations. Only monitors and
  workflows are shareable resources in this PR.
- Reverse migration (RSC → legacy). Once `resource_type` is on docs and
  sharing entries exist, alerting always uses the RSC path.
