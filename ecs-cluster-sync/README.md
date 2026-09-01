# ecs-cluster-sync

Whole-cluster replication for ECS: given nothing but the management IP and admin credentials, it reproduces a
cluster's tenancy on the target and copies all object data.

It runs in three ordered phases:

1. **Discover** the source cluster: namespaces, object users and buckets.
2. **Create** on the target, in this order: namespaces → object users → buckets (each bucket owned by the replicated
   user).
3. **Copy** every bucket's objects, using one `EcsSync` job per bucket.

Because a namespace whose name is already taken is recreated under a new name, the source and target may be **the
same cluster** — which is how you migrate data between storage pools.

## Usage

```bash
java -cp ecs-sync-<version>.jar com.emc.ecs.sync.cluster.EcsClusterSyncCli \
    --source 10.0.0.1 --source-user root --source-password 'pw1' \
    --target 10.1.0.1 --target-user root --target-password 'pw2' \
    --trust-self-signed --verify
```

Or with a config file (keeps credentials off the process list):

```bash
java -cp ecs-sync-<version>.jar com.emc.ecs.sync.cluster.EcsClusterSyncCli \
    --xml-config sample/ecs-cluster-replication.xml
```

Run with `--help` for the full option list. Exit codes: `0` success, `1` fatal error, `2` finished with per-bucket
failures.

Always start with `--dry-run`, which reports the plan — including every namespace and user rename — and changes
nothing.

## Storage pool migration (within one cluster)

Point both sides at the same cluster and name the replication group backed by the destination pool:

```bash
java -cp ecs-sync-<version>.jar com.emc.ecs.sync.cluster.EcsClusterSyncCli \
    --source 10.0.0.1 --source-user root --source-password 'pw' \
    --target 10.0.0.1 --target-user root --target-password 'pw' \
    --target-replication-group 'rg-on-new-pool' \
    --trust-self-signed --dry-run
```

`--target-replication-group` is **mandatory** here — without it the copy would land in the same pool, which is
pointless, so the run is rejected.

To find the replication group names and the pools behind them:

```bash
TOKEN=$(curl -sk -u root:'pw' -D- -o /dev/null https://10.0.0.1:4443/login \
  | awk '/X-SDS-AUTH-TOKEN/{print $2}' | tr -d '\r')
curl -sk -H "X-SDS-AUTH-TOKEN: $TOKEN" https://10.0.0.1:4443/vdc/data-service/vpools
```

Each entry's `zone_mappings[].value` holds the storage pool IDs that group writes to.

## Namespace and user renaming

Every source namespace is checked against the target:

| Situation | Target namespace | Object users |
|---|---|---|
| Name is free | `ns1` | unchanged |
| `ns1` exists | `ns1_target` | `alice` → `alice_target` |
| `ns1` and `ns1_target` exist | `ns1_target2` | `alice` → `alice_target2` |

Change the suffix with `--target-namespace-suffix`.

Users are renamed alongside the namespace because ECS can be configured with **GLOBAL user scope**, where an object
user ID must be unique across the whole cluster — so `alice` cannot be recreated in `ns1_target` on the same cluster.
When nothing is renamed (the normal two-cluster case) user IDs are preserved exactly.

### Resuming a run

The counter means a re-run does **not** resume by default: `ns1` and `ns1_target` both exist, so the second run
allocates `ns1_target2`. Pin the mapping to resume instead:

```bash
--namespace-map ns1=ns1_target,ns2=ns2_target
```

Combine with `--db-connect-string` / `--db-file` so individual objects are skipped as well.

## Defaults

| | Default |
|---|---|
| Management API | `https` on port `4443` |
| S3 data path | `http` on port `9020`, using the management IP |
| Threads per bucket | 16 |
| Buckets in parallel | 4 |

Use `--source-data-hosts` / `--target-data-hosts` to spread data traffic across several nodes.

## Things worth understanding before you run it

**It writes to the source cluster.** The management API never discloses an existing user's secret key, so to read a
namespace's data the tool calls `POST /object/user-secret-keys/{uid}` for any user that has no key. ECS supports two
keys per user, so this does not disturb existing keys, but it *is* a modification. Pass `--no-source-key-generation`
to fail instead.

**Replication groups are matched by name.** Replication group IDs are cluster-specific URNs. The source bucket's group
name is looked up on the target; if the target has exactly one group, that is used. If the names differ and the target
has several groups, the run fails and asks you to set `--target-replication-group`.

**Bucket ownership.** Buckets are created through the management API with `owner` set to the replicated object user,
so ownership matches the source. The bucket owner is also the identity used to copy that bucket's data. If a bucket's
owner has no object-user record (for example, a bucket owned by a management user), any user in the namespace is used
instead and a warning is logged.

**Namespaces with no object users are skipped** during the copy phase — there is no identity to read or write their
data with. The namespace and its buckets are still created.

**Re-running is safe but not automatically resumable.** Existing namespaces, users and buckets are detected and left
alone, but a namespace created by a previous run counts as a name conflict, so a plain re-run creates a *new*
namespace. Use `--namespace-map` to resume into the existing one.

**The S3 namespace is pinned explicitly.** Both sides set `EcsS3Config.namespace`, so a same-cluster migration cannot
accidentally read and write the same namespace even though the bucket names are identical.

## Not replicated

- IAM users, groups and roles (object users only)
- Bucket policies, lifecycle rules, tags, quotas and retention/object-lock settings
- Namespace quotas and namespace-level settings other than the replication group
- ACL-only differences on the namespace or bucket
- CAS pools, NFS exports and other non-S3 protocol configuration

## Change to the ECS S3 plugin

This module added an optional `namespace` field to `EcsS3Config`, applied in `EcsS3Storage.configure()` via
`S3Config.setNamespace()`. It defaults to null, which preserves the previous behaviour exactly (ECS infers the
namespace from the object user). It is now also available to ordinary `syncConfig` jobs as
`--source-namespace` / `--target-namespace`.

## Web UI

The same migration is available in the ecs-sync web UI as an **E2E (ECS to ECS)** migration, under *Migration →
Start E2E Migration*. It reuses this project's `ecs-cluster-mgmt` module, so the naming rules and management API
behaviour are identical to the CLI.

Differences from the CLI:

- Source and target are chosen from the ECS entries configured under *Storage*, rather than passed as arguments.
- The replication group and namespaces are picked from live drop-downs, and **Test** reports what will be created or
  renamed before you start.
- Each bucket copy is submitted as a normal ecs-sync job, so it appears in the UI's job list with progress,
  pause/resume and an archived report.
- The UI runs one bucket at a time (the task list is sequential) rather than the CLI's `--bucket-parallelism`.

## Module layout

| Module | Contents | Depends on |
|---|---|---|
| `ecs-cluster-mgmt` | management API client, models, namespace/user naming rules, replication group resolution | `ecs-sync-model` only |
| `ecs-cluster-sync` | `ClusterReplicator`, the S3 data path and the CLI | `ecs-cluster-mgmt`, `ecs-sync-core` |

The split exists so the Grails UI can reuse the management logic without pulling the sync engine (and its JDBC/Jersey
stack) onto its classpath.

## Design note

This is deliberately *not* an ecs-sync storage plugin. A `SyncStorage` implementation models a single stream of
objects between one source and one target; a cluster replication is an orchestration that first mutates the target's
tenancy and then runs many independent sync jobs. Modelling it as a plugin would require `allObjects()` to enumerate
across buckets that do not exist yet, and would leave no place to create namespaces and users. It therefore has its
own entry point and its own config document (`clusterSyncConfig`) and reuses the ECS S3 plugin for the actual data
movement.

## Tests

```bash
./gradlew :ecs-cluster-sync:test
```

The unit tests cover management API response parsing, namespace filtering, replication group resolution, bucket-user
selection, config validation and DB table naming. They do not require a live cluster; the phases that talk to ECS need
a real pair of clusters to exercise.
