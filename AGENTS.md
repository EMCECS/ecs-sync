# ecs-sync Project Context

## Purpose
`ecs-sync` is a bulk copy and data migration utility for unstructured data. It moves data in parallel between various source and target systems, including Dell EMC ECS, Atmos, S3, Azure, CAS, NFS, and filesystems.

**Key Features:**
- Parallel multi-threaded data transfer
- MD5 checksum verification
- Database-backed job status tracking (MySQL/SQLite)
- Pluggable architecture for sources, targets, and filters
- XML or programmatic configuration
- REST API and web UI for monitoring
- Retry handling and error reporting

## Tech Stack
- Java (build targets Java 8/11 compatibility; Java 11 requires explicit JAXB/JAF dependencies)
- Gradle 7.x with `com.github.johnrengelman.shadow` 7.1.2
- Grails (ecs-sync-ui web UI)
- Spring, JAX-RS (Jersey 1.x), JAXB, Log4j2
- MySQL/SQLite for job status tracking
- HikariCP for connection pooling
- Apache Commons (CLI, CSV, Compress, Lang3)

## Architecture Overview

### Core Sync Engine (`ecs-sync-core`)
The main sync orchestrator is `com.emc.ecs.sync.EcsSync`, which:
- Manages thread pools for listing, syncing, querying, and retrying
- Coordinates source → filters → target pipeline
- Tracks statistics and performance metrics
- Handles throttling (TPS and bandwidth)
- Supports verification via `SyncVerifier`

### Plugin System
All storage and filter plugins implement the `SyncPlugin<C>` interface:
- **`SyncStorage<C>`** — source/target storage adapters (extends `SyncPlugin`)
- **`SyncFilter<C>`** — transformation/processing filters (extends `SyncPlugin`)
- Plugins are discovered via `PluginUtil` and configured via XML or programmatic config

### Data Model
- **`SyncObject`** — represents an object being synced (metadata, ACLs, data stream)
- **`ObjectMetadata`** — metadata (content type, size, modification time, user metadata)
- **`ObjectAcl`** — access control lists
- **`SyncRecord`** — database record tracking sync status per object
- **`SyncStats`** — runtime statistics (objects processed, bytes transferred, errors)

## Project Structure
Multi-module Gradle project defined in `settings.gradle`:

- `platform/` — centralized dependency version constraints (BOM); all versions are declared here
- `buildSrc/` — shared build logic (`java-module.gradle`, `storage-plugin.gradle`, `filter-plugin.gradle`, `library-publication.gradle`)
- `ecs-sync-model/` — data models and configuration POJOs
- `ecs-sync-core/` — core sync engine, plugin interfaces, utilities
- `ecs-sync-cli/` — CLI executable jar; main class `com.emc.ecs.sync.EcsSyncCli`
- `ecs-sync-ctl/` — control/client executable jar; main class `com.emc.ecs.sync.ctl.EcsSyncCtl`
- `ecs-sync-jaxrs/` — REST service resources
- `ecs-sync-ui/` — Grails web UI for job management
- `storage-plugins/` — source/target adapters (each has `-model` and implementation modules)
- `filter-plugins/` — transformation plugins (each has `-model` and implementation modules)
- `sample/` — sample XML sync configurations
- `script/` — deployment scripts for OVA, Windows, MySQL
- `docker/` — Docker images and configs
- `test-resources/` — test configuration templates

### Storage Plugins
Each plugin has a `-model` module (config POJO) and an implementation module:
- **`atmos-storage`** — Dell EMC Atmos object storage
- **`azure-storage`** — Microsoft Azure Blob Storage
- **`cas-storage`** — EMC Centera (CAS)
- **`filesystem-storage`** — Local/mounted filesystems and archive files (zip, tar)
- **`nfs-storage`** — NFS shares (excluded by default due to CVE in Netty 3.10.6)
- **`s3-storage`** — AWS S3 and ECS S3 (separate configs: `AwsS3Config`, `EcsS3Config`)

### Filter Plugins
- **`basic-filters`** — common transformations (path mapping, metadata manipulation)
- **`cas-extractors`** — CAS clip extraction
- **`cifs-ecs-ingester`** — CIFS to ECS ingestion with metadata preservation
- **`encryption-filters`** — encryption/decryption filters
- **`fs-filters`** — filesystem-specific filters

## Build & Test Commands

### Building
- **Main distribution**: `./gradlew :distZip` (default task)
  - Produces a zip archive with CLI/CTL jars, scripts, samples, and license reports
- **UI jar**: `cd ecs-sync-ui && ../gradlew -Dgrails.env=prod assemble`
- **License report**: `./gradlew generateLicenseReport`
- **Dependency update check**: `./gradlew dependencyUpdates`

### Excluding Plugins
You can exclude plugins from the build to reduce dependencies or avoid CVEs:
- **Storage plugins**: `-Pstorage.excludes="cas,nfs"`
- **Filter plugins**: `-Pfilter.excludes="cas-extractors,cifs-ecs-ingester"`

**Note**: The `nfs` storage plugin is excluded by default because of a CVE in a transitive Netty 3.10.6 dependency from `com.emc.ecs:nfs-client`.

### Testing
1. Copy `test-resources/test.properties.template` to `$HOME/test.properties`
2. Remove the `.template` extension
3. Populate with credentials and system access details (S3, Atmos, CAS, NFS, MySQL, etc.)
4. Run tests: `./gradlew testReport` (generates aggregated JUnit report in `build/reports/allTests`)

## Configuration

### XML Configuration
Sync jobs are typically configured via XML files (see `sample/` directory). Example structure:
```xml
<syncConfig xmlns="http://www.emc.com/ecs/sync/model">
    <options>
        <threadCount>32</threadCount>
        <verify>true</verify>
        <dbTable>my_sync_job</dbTable>
    </options>
    <source>
        <filesystemConfig>
            <path>/path/to/source</path>
        </filesystemConfig>
    </source>
    <target>
        <ecsS3Config>
            <protocol>https</protocol>
            <vdcs>datacenter(host1,host2)</vdcs>
            <accessKey>...</accessKey>
            <secretKey>...</secretKey>
            <bucketName>my-bucket</bucketName>
        </ecsS3Config>
    </target>
</syncConfig>
```

### Database Configuration
- **MySQL**: Use `dbConnectString` (JDBC URL) and `dbEncPassword` (encrypted password)
- **SQLite**: Use `dbFile` (path to SQLite database file)
- **No DB**: Omit both; sync will run without status tracking

### Sample Configurations
The `sample/` directory contains real-world examples:
- `directory-to-ecs-s3.xml` — filesystem to ECS S3
- `atmos-to-ecs-atmos-oid.xml` — Atmos to ECS Atmos migration
- `aws-ecs-bucket-migration.xml` — AWS S3 to ECS S3
- `azure-to-ecs-s3.xml` — Azure Blob to ECS S3
- `cas-to-s3-single-blob.xml` — CAS to S3 (single blob mode)
- `ecs-bucket-copy-with-virus-scan.xml` — ECS bucket copy with virus scanning filter
- `atmos-encrypt.xml` / `atmos-decrypt.xml` — encryption/decryption examples

## Dependency Conventions
- All dependency versions are declared in `platform/build.gradle` inside a `constraints` block.
- Subprojects import the platform with `internal platform(project(":platform"))` and declare dependencies **without versions**.
- Transitive excludes and version overrides are managed in `buildSrc/src/main/groovy/java-module.gradle`.
- Dependencies are alphabetically ordered in `platform/build.gradle`.

## Distribution
`./gradlew :distZip` produces an archive (`build/distributions/ecs-sync-<version>.zip`) containing:
- Shadow jars: `ecs-sync-<version>.jar` (CLI) and `ecs-sync-ctl-<version>.jar` (control client)
- `readme.txt`, `LICENSE`, `NOTICE`
- `3rd-party-licenses/` — HTML and CSV license reports
- `script/` — deployment scripts for OVA, Windows, MySQL
- `samples/` — sample XML configurations
- `custom-plugin-package.txt` — lists included/excluded plugins (if any)

## Deployment Options
- **Standalone CLI**: Run `java -jar ecs-sync-<version>.jar --help`
- **REST Service**: Deploy `ecs-sync-<version>.jar` as a service (see `script/ova/` or `script/windows/`)
- **Web UI**: Deploy `ecs-sync-ui` Grails app (connects to REST service)
- **Docker**: See `docker/README` for containerized deployment
- **OVA Appliance**: Pre-configured Linux VM with MySQL, REST service, and web UI (see `script/ova/`)

## Key Classes and Interfaces

### Core Engine
- **`com.emc.ecs.sync.EcsSync`** — main sync orchestrator (implements `Runnable`, `RetryHandler`, `AutoCloseable`)
- **`com.emc.ecs.sync.SyncTask`** — task for syncing a single object
- **`com.emc.ecs.sync.SyncStats`** — runtime statistics tracker
- **`com.emc.ecs.sync.config.SyncConfig`** — top-level configuration object
- **`com.emc.ecs.sync.config.SyncOptions`** — sync options (thread count, verify, DB settings, etc.)

### Plugin Interfaces
- **`com.emc.ecs.sync.SyncPlugin<C>`** — base plugin interface (all plugins extend this)
- **`com.emc.ecs.sync.storage.SyncStorage<C>`** — storage plugin interface (source/target)
- **`com.emc.ecs.sync.filter.SyncFilter<C>`** — filter plugin interface (transformations)
- **`com.emc.ecs.sync.AbstractPlugin<C>`** — abstract base class for plugins
- **`com.emc.ecs.sync.storage.AbstractStorage<C>`** — abstract base class for storage plugins
- **`com.emc.ecs.sync.filter.AbstractFilter<C>`** — abstract base class for filters

### Data Model
- **`com.emc.ecs.sync.model.SyncObject`** — object being synced (metadata + data stream)
- **`com.emc.ecs.sync.model.ObjectMetadata`** — object metadata (content type, size, mtime, user metadata)
- **`com.emc.ecs.sync.model.ObjectAcl`** — access control lists
- **`com.emc.ecs.sync.model.ObjectContext`** — context for object processing
- **`com.emc.ecs.sync.service.SyncRecord`** — database record for sync status

### Utilities
- **`com.emc.ecs.sync.util.PluginUtil`** — plugin discovery and instantiation
- **`com.emc.ecs.sync.util.EnhancedThreadPoolExecutor`** — custom thread pool with monitoring
- **`com.emc.ecs.sync.util.SyncUtil`** — general sync utilities
- **`com.emc.ecs.sync.util.TimingUtil`** — performance timing utilities
- **`engineering.clientside.throttle.Throttle`** — TPS/bandwidth throttling (integrated from `throttle` library)

## Useful Metadata
- **Version**: Set in `gradle.properties` (currently `3.5.5`)
- **License**: Apache License 2.0 (see `LICENSE` and `NOTICE`)
- **Copyright**: Dell Inc. or its subsidiaries
- **Repository**: https://github.com/EMCECS/ecs-sync
- **Wiki**: https://github.com/EMCECS/ecs-sync/wiki

## ECS Cluster Replication (`ecs-cluster-sync`)

Replicates an ECS cluster's tenancy and data given only the management IP(s) and admin credentials. Source and target
may be **the same cluster**, which is how a storage pool migration is performed.

Split across two modules so the Grails UI can reuse the logic without the sync engine on its classpath:

| Module | Contents | Depends on |
|---|---|---|
| `ecs-cluster-mgmt` | `EcsManagementClient`, models, `NamespaceMapper`, `NamespaceFilter`, `ReplicationGroupResolver`, `ClusterEndpoint`, `ClusterReplicationPlan` | `ecs-sync-model` only |
| `ecs-cluster-sync` | `ClusterReplicator`, `S3Support`, `ClusterSyncConfig/Options/Report`, `EcsClusterSyncCli` | `ecs-cluster-mgmt`, `ecs-sync-core` |

Consumers: `ecs-sync-cli` depends on `ecs-cluster-sync`; `ecs-sync-ui` depends only on `ecs-cluster-mgmt`.

### Entry point
`com.emc.ecs.sync.cluster.EcsClusterSyncCli`, shipped inside the CLI shadow jar:
```bash
java -cp ecs-sync-<version>.jar com.emc.ecs.sync.cluster.EcsClusterSyncCli \
    --source 10.0.0.1 --source-user root --source-password pw1 \
    --target 10.1.0.1 --target-user root --target-password pw2 \
    --trust-self-signed --dry-run
```
It also accepts `--xml-config` with a `clusterSyncConfig` document (see `sample/ecs-cluster-replication.xml`). That is
a separate document type from `syncConfig` and is not loaded by `EcsSyncCli`.

### Phases
1. `ClusterDiscovery` (in `ClusterReplicator.discover()`) lists namespaces, users and buckets from the source.
2. `ClusterReplicator.replicateStructure()` creates namespaces, then users, then buckets owned by the replicated user.
3. `ClusterReplicator.copyAllBuckets()` runs one `EcsSync` job per bucket, `bucketParallelism` at a time.

### Key classes
- `EcsManagementClient` — ECS Management REST API client (login, namespaces, users, secret keys, replication groups,
  buckets)
- `ManagementResponseParser` — pure JSON parsing, unit-tested against captured payloads
- `ReplicationGroupResolver` — maps a source replication group to a target one **by name** (IDs are cluster-specific)
- `NamespaceMapper` — decides the target namespace name and the matching object-user rename
- `ClusterReplicationPlan` — discovered structure; `resolveBucketUser()` picks the identity for each bucket
- `ClusterReplicator` — orchestration; `ClusterSyncReport` — results
- `S3Support` — builds `EcsS3Config` / reconciles bucket versioning over S3

### Important behaviours
- **Secret keys are per-cluster and per-namespace.** The management API does not return an existing user's key, so the
  tool fetches or generates one on *each* side separately (`POST /object/user-secret-keys/{uid}`). This writes to the
  source cluster; `--no-source-key-generation` disables it.
- **Replication groups** are matched by name, never by ID.
- **Namespace conflicts** → suffix `_target`, then a counter (`ns1_target2`). Object users in a renamed namespace get
  the *same* suffix, because ECS GLOBAL user scope requires cluster-unique user IDs. Nothing is renamed when the name
  is free, so the normal two-cluster case preserves all names.
- **Same-cluster runs** require `--target-replication-group` and cannot use `--skip-namespace-creation`.
- **Re-runs are safe but not auto-resuming**: a namespace created by a previous run reads as a conflict, so a new name
  is allocated. Use `--namespace-map ns1=ns1_target` to resume deterministically.
- Namespaces with no object users get their structure created but their buckets are not copied.

### Web UI (E2E migration)
`ecs-sync-ui` exposes the same feature as an **E2E** migration (Migration → Start E2E Migration), built on the
existing `AbstractMigration` task framework alongside the older `a2e` (Atmos→ECS) migration:
- `sync.ui.migration.e2e.E2EMigrationConfig` / `E2EMigration` — config + task list, discovery via `EcsManagementClient`
- Tasks: `CreateNamespaceTask`, `CreateUserTask`, `CreateBucketTask`, `BucketSyncTask`
- `ClusterEndpoints` — adapts the UI's `EcsStorage` entry to `ClusterEndpoint`
- View: `grails-app/views/migration/_e2e.gsp`; entry point added to `migration/index.gsp`
- Registered in three dispatch points: `MigrationEntry.newMigrationConfig`, `MigrationService.createMigration`, and
  `AbstractMigrationConfig`'s `@XmlSeeAlso` (required for the config to persist as XML)

`BucketSyncTask` submits each bucket copy as a normal sync job via `syncJobService`, so it appears in the UI job list.
Note the UI's `EcsService`/`MgmtApi` cannot list buckets or object users — that is why the E2E migration uses
`EcsManagementClient` from `ecs-cluster-mgmt` instead.

**Jersey dependency fix**: `com.emc.ecs:mgmt-client` (used by `EcsService`/`StorageController` for the storage-entry
"Test" button — this is unrelated to the E2E migration code) pulls in `org.glassfish.jersey.core:jersey-client`
(JAX-RS 2.x), whose `ClientBuilder.newClient()` looks up an `InjectionManagerFactory` via `ServiceLoader` at runtime.
The actual implementation lives in `org.glassfish.jersey.inject:jersey-hk2`, which is not a transitive dependency of
`jersey-client` and was missing from the UI's runtime classpath, causing "InjectionManagerFactory not found." on any
storage connection test. Fixed by adding `runtimeOnly "org.glassfish.jersey.inject:jersey-hk2:2.35"` to
`ecs-sync-ui/build.gradle` — the version must match whatever `org.glassfish.jersey.core:jersey-common` resolves to
(check with `./gradlew :ecs-sync-ui:dependencies --configuration runtimeClasspath`).

### Change made to the s3-storage plugin
`EcsS3Config` gained an optional `namespace` field (orderIndex 45), applied in `EcsS3Storage.configure()` via
`S3Config.setNamespace()`. Defaults to null = previous behaviour. This is required for same-cluster migrations, where
source and target bucket names are identical and only the namespace distinguishes them.

### Not replicated
IAM identities, bucket policies/lifecycle/tags/quotas/retention, namespace quotas, non-S3 protocol config.

### Build / test
```bash
./gradlew :ecs-cluster-sync:test
```
NOTE: `java-module.gradle` sets `test { ignoreFailures = true }`, so `BUILD SUCCESSFUL` does **not** mean tests
passed — check `<module>/build/test-results/test/*.xml`.

## Build environment gotchas
- `~/.gradle/gradle.properties` may pin `org.gradle.java.home` to a JDK newer than Gradle 7.6.4 supports (Java 25
  fails with `Unsupported class file major version 69` while compiling init scripts). Override per invocation:
  `./gradlew -Dorg.gradle.java.home=/usr/lib/jvm/java-11-openjdk-amd64 ...`

## Common Development Tasks

### Adding a New Storage Plugin
1. Create `-model` module in `storage-plugins/` with config POJO (annotate with `@Label`)
2. Create implementation module with storage class extending `AbstractStorage<YourConfig>`
3. Add both modules to `settings.gradle` (auto-included via `file('storage-plugins').eachDir`)
4. Implement required methods: `configure()`, `allObjects()`, `loadObject()`, `updateObject()`, etc.
5. Add dependencies to `platform/build.gradle` (with versions)

### Adding a New Filter Plugin
1. Create `-model` module in `filter-plugins/` with config POJO
2. Create implementation module with filter class extending `AbstractFilter<YourConfig>`
3. Add both modules to `settings.gradle` (auto-included via `file('filter-plugins').eachDir`)
4. Implement `filter()` method to transform `SyncObject`
5. Add dependencies to `platform/build.gradle` (with versions)

### Running a Sync Job
```bash
# From XML config
java -jar ecs-sync-<version>.jar --xml-config sample/directory-to-ecs-s3.xml

# With CLI options
java -jar ecs-sync-<version>.jar \
  --source filesystem --source-path /data \
  --target ecs-s3 --target-bucket my-bucket \
  --target-access-key <key> --target-secret-key <secret> \
  --threads 32 --verify
```

### Monitoring a Running Job
- **CLI**: Use `ecs-sync-ctl-<version>.jar` to query job status
- **REST API**: Query `http://localhost:9200/job/<jobId>` (if REST service is running)
- **Web UI**: Navigate to `http://localhost:9200/ui` (Grails app)

## Troubleshooting

### Common Issues
- **OutOfMemoryError**: Increase heap size with `-Xmx` (e.g., `-Xmx4g`)
- **Connection timeouts**: Adjust network settings or reduce thread count
- **Database connection errors**: Verify JDBC URL and credentials in config
- **Plugin not found**: Ensure plugin is not excluded via `-Pstorage.excludes` or `-Pfilter.excludes`
- **CVE warnings**: Check `platform/build.gradle` for dependency version overrides

### Logging
- **CLI**: Configured via `ecs-sync-cli/src/main/resources/log4j2.xml`
- **UI**: Configured via `ecs-sync-ui/grails-app/conf/logback.xml`
- Adjust log levels for `com.emc.ecs.sync` package to `DEBUG` for verbose output

### Performance Tuning
- **Thread count**: Start with 2x CPU cores; adjust based on I/O vs CPU bottleneck
- **Throttling**: Use `--tps` or `--bandwidth` to limit throughput
- **Verification**: Disable `--verify` for faster transfers (at cost of data integrity checks)
- **Database**: Use MySQL for large jobs (better performance than SQLite)
