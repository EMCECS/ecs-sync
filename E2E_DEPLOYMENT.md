# E2E Cluster Replication - Deployment Guide

## Issue Resolved
**Problem**: "InjectionManagerFactory not found" error when testing cluster connections in the UI.

**Root Cause**: The UI's Grails/Spring Boot environment excluded JAX-RS dependencies, and Jersey 1.x client (used by `ecs-cluster-mgmt`) requires `jersey-core` and `jersey-client` runtime libraries for its HK2 injection manager.

**Fix**: Added Jersey 1.x runtime dependencies to `ecs-sync-ui/build.gradle`:
```gradle
runtimeOnly "com.sun.jersey:jersey-core:1.19.4"
runtimeOnly "com.sun.jersey:jersey-client:1.19.4"
```

## Artifacts on 10.118.244.155

Located in `/tmp/`:

1. **ecs-sync-3.5.5.zip** (62 MB)
   - MD5: `73d8a508089163ba843984d5d0c9ece2`
   - Contains CLI, control client, samples, scripts

2. **ecs-sync-ui-3.5.5-fixed.jar** (90 MB)
   - MD5: `27cc08fa64e0dd6885a310c71ba5e714`
   - Grails web UI with E2E migration feature and Jersey dependencies

## Deployment Steps

### 1. Extract the Distribution
```bash
cd /tmp
unzip ecs-sync-3.5.5.zip
cd ecs-sync-3.5.5
```

### 2. Start the REST Service
The REST service handles sync job execution and must be running for the UI to work.

```bash
# Requires Java 11
java -jar ecs-sync-3.5.5.jar --rest-only &
```

The service will listen on port 9200 by default.

### 3. Start the Web UI
In a separate terminal or screen session:

```bash
cd /opt
nohup java -jar ecs-sync-ui-3.5.5.jar --server.address=0.0.0.0 --server.port=8080 > ui.log 2>&1 &
```

**IMPORTANT**: Use `--server.address=0.0.0.0` to make the UI accessible from outside the machine. By default, Spring Boot binds to localhost (127.0.0.1) only.

The UI will listen on port 8080.

### 4. Access the UI
Navigate to: **http://10.118.244.155:8080/**

**Current Status on 10.118.244.155**: The UI is already running and accessible at the above URL.

### 5. Configure E2E Migration
1. Go to **Storage** → **Add Storage**
2. Add source and target ECS clusters with management credentials
3. Go to **Migration** → **Start E2E Migration**
4. Select source and target storage entries
5. Configure options (namespace mapping, replication groups, etc.)
6. Click **Test** to verify connectivity before starting
7. Click **Start** to begin the migration

## E2E Migration Features

### What Gets Replicated
- Namespaces
- Object users (with secret keys)
- Buckets (with ownership and replication group assignment)
- All objects in each bucket (via S3 protocol)

### What Does NOT Get Replicated
- IAM identities
- Bucket policies, lifecycle rules, tags, quotas, retention settings
- Namespace quotas
- Non-S3 protocol configurations

### Same-Cluster Migration (Storage Pool Migration)
The tool supports same-cluster replication for storage pool migrations:
- Requires `--target-replication-group` to be specified
- Namespaces are renamed with `_target` suffix to avoid conflicts
- Object users are also renamed to match namespace suffix
- Use `--namespace-map` for deterministic naming on re-runs

### CLI Alternative
For automation or scripting, use the CLI instead of the UI:

```bash
java -cp ecs-sync-3.5.5.jar com.emc.ecs.sync.cluster.EcsClusterSyncCli \
    --source 10.0.0.1 --source-user root --source-password 'pw1' \
    --target 10.1.0.1 --target-user root --target-password 'pw2' \
    --trust-self-signed --dry-run
```

See `samples/ecs-cluster-replication.xml` for XML configuration examples.

## Verification

### Check Jersey Dependencies
Verify the fixed UI jar contains Jersey:
```bash
unzip -l ecs-sync-ui-3.5.5-fixed.jar | grep jersey
```

Should show `jersey-core` and `jersey-client` jars.

### Test Connection
In the UI:
1. Add an ECS storage entry with management credentials
2. Click the **Test** button
3. Should see "Connection successful" (or specific error messages)
4. Should NOT see "InjectionManagerFactory not found"

## Troubleshooting

### Port Already in Use
If port 9200 is taken, specify a different port:
```bash
java -jar ecs-sync-3.5.5.jar --rest-only --rest-endpoint http://0.0.0.0:9201
java -jar ecs-sync-ui-3.5.5-fixed.jar --server.port=9202
```

### Java Version Issues
Both jars require Java 11:
```bash
java -version  # Should show Java 11
```

### Memory Issues
For large migrations, increase heap size:
```bash
java -Xmx4g -jar ecs-sync-3.5.5.jar --rest-only
java -Xmx2g -jar ecs-sync-ui-3.5.5-fixed.jar
```

### Connection Timeouts
- Verify network connectivity to ECS management endpoints
- Check firewall rules (port 4443 for ECS management API)
- Use `--trust-self-signed` if using self-signed certificates

## Important Notes

1. **Always use Test first**: The Test button validates credentials, connectivity, and replication group names before starting a migration.

2. **Start small**: Test with a single small namespace before migrating production data.

3. **Secret key generation**: The tool will generate secret keys on both clusters if they don't exist. Use `--no-source-key-generation` to prevent writes to the source cluster.

4. **Re-runs**: The tool is safe to re-run but not auto-resuming. Use `--namespace-map` to control naming on subsequent runs.

5. **Monitoring**: Each bucket copy appears as a separate job in the UI's job list, allowing individual monitoring and control.

## Support

For issues or questions:
- Check logs in the terminal where the services are running
- Review `AGENTS.md` for detailed architecture and behavior
- See `README.md` for general ecs-sync documentation
- Check `CLUSTER_REPLICATION_IMPLEMENTATION.md` for implementation details
