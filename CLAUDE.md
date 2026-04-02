<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a fork of [Apache Accumulo](https://accumulo.apache.org) (4.0.0-SNAPSHOT) — a sorted, distributed key/value store built on Hadoop, ZooKeeper, and Thrift. This fork adds Kubernetes deployment infrastructure using Helm charts with Alluxio as a cloud-native storage layer replacing HDFS.

The repo has two distinct concerns:
1. **Accumulo itself** — a multi-module Java 17 Maven project (upstream Apache code)
2. **Kubernetes deployment** — Helm charts, Docker images, and deployment scripts (added in this fork)

## Build Commands

### Java (Accumulo)
```bash
mvn clean package                          # Build + unit tests
mvn clean package -DskipTests              # Build only (faster)
mvn clean verify                           # Build + unit + integration tests
mvn clean verify -PskipQA -DskipTests=false  # Unit tests only (skip QA checks)
mvn clean verify -Psec-bugs -DskipTests    # SpotBugs security analysis
mvn verify javadoc:jar -Psec-bugs -DskipTests -Dspotbugs.timeout=3600000  # Full QA checks

# Run a single unit test
mvn test -pl core -Dtest=MyTestClass
mvn test -pl core -Dtest=MyTestClass#testMethodName

# Run a single integration test
mvn clean verify -Dit.test=WriteAheadLogIT -Dtest=foo -Dspotbugs.skip

# Run sunny day (minimal) integration tests
mvn clean verify -Psunny

# Format verification
mvn -B validate -DverifyFormat
```

Java 17 required. Integration tests need 3-4GB free memory and 10GB free disk.

### CI Validation Scripts
These run fast and work without a full build:
```bash
src/build/ci/find-unapproved-chars.sh              # Check for disallowed characters
src/build/ci/find-unapproved-junit.sh               # Check JUnit API usage
src/build/ci/check-module-package-conventions.sh     # Package naming conventions
src/build/ci/find-startMini-without-stopMini.sh      # Ensure MAC cleanup
src/build/ci/find-unapproved-abstract-ITs.sh         # Abstract IT class check
```

### Docker
```bash
# Build Accumulo distribution first, then Docker image
mvn clean package -DskipTests -pl assemble -am
./scripts/build-docker.sh -r <registry> -t <tag>
./scripts/build-docker.sh -r <registry> -t <tag> -p   # Build and push
```

### Helm / Kubernetes
```bash
make help                    # Show all available Make targets
make build                   # Build Accumulo distribution
make docker-build            # Build Docker image
make deploy-dev              # Deploy dev environment (uses values-dev.yaml)
make deploy                  # Deploy with generated secrets
make test                    # Run smoke tests
make validate-init           # Validate Accumulo initialization with Alluxio
make status                  # Show deployment status
make shell                   # Access Accumulo shell in cluster
make validate                # Lint Helm chart
make template                # Render Helm templates to YAML
make full-dev-setup          # kind-create → deploy-dev → test
make full-cleanup            # uninstall → kind-delete → clean-docker
```

Makefile variables: `REGISTRY`, `TAG`, `RELEASE_NAME`, `NAMESPACE`, `VALUES_FILE`.

## Architecture

### Maven Modules
- **core** — Core Accumulo APIs and data model
- **start** — Startup utilities (minimal dependencies, builds first)
- **server/base** — Shared server code
- **server/manager** — Manager (cluster coordination, metadata)
- **server/tserver** — Tablet server (read/write, hosts tablets)
- **server/gc** — Garbage collector
- **server/compactor** — Compaction service
- **server/monitor** — Web UI for monitoring
- **server/native** — Native C++ libraries for in-memory map
- **shell** — CLI shell
- **minicluster** — MiniAccumuloCluster for testing
- **hadoop-mapreduce** — MapReduce integration
- **iterator-test-harness** — Test utilities for iterators
- **test** — Integration tests
- **assemble** — Distribution assembly (produces the tar.gz)

### Kubernetes Deployment Architecture

```
Helm Release
├── Accumulo (Deployments):
│   ├── Manager (1 replica, init containers wait for ZK + Alluxio)
│   ├── TabletServers (3 replicas, pod anti-affinity)
│   ├── Monitor (1 replica, port 9995)
│   ├── GC (1 replica)
│   └── Compactor (2 replicas, pod anti-affinity)
├── Alluxio (cache layer between Accumulo and object storage):
│   ├── Master (Deployment, ports 19998/19999)
│   └── Workers (DaemonSet, ramdisk cache)
├── ZooKeeper (embedded subchart, 3 replicas)
└── MinIO (embedded subchart, S3-compatible — dev only)
```

Alluxio replaces HDFS, providing a unified cache over cloud object stores (S3, GCS, Azure Blob). The Docker image includes Alluxio 2.9.4 client patched for Java 17.

Storage provider is configured in Helm values (`storage.provider`: s3, gcs, azure, minio). Init containers ensure ZooKeeper and Alluxio are ready before Accumulo starts.

### Key Directories
- `charts/accumulo/` — Helm chart (templates, values files, embedded ZK/MinIO subcharts)
- `docker/accumulo/` — Dockerfile and pre-built Accumulo distribution
- `scripts/` — Build, deploy, secrets generation, and validation scripts
- `src/build/ci/` — CI validation shell scripts
- `.github/workflows/` — GitHub Actions (QA on push/PR, full ITs on demand)

### Helm Values Files
- `charts/accumulo/values.yaml` — Defaults
- `charts/accumulo/values-dev.yaml` — Development (MinIO, reduced resources)
- `values-generated.yaml` — Auto-generated by `scripts/generate-secrets.sh`

## CI Workflows

- **QA** (`maven.yaml`) — Runs on push/PR. Fast build (CI checks + compile), then 4-way matrix: unit tests, QA checks (SpotBugs + javadoc), compatibility (Hadoop 3.0.3/ZK 3.6.4), ErrorProne.
- **Full ITs** (`maven-full-its.yaml`) — Manual trigger. Partitions integration tests across parallel jobs (15 ITs per job via `src/build/ci/it-matrix.sh`).
- **On-demand** (`maven-on-demand.yaml`) — Manual trigger with configurable Maven goals/options.
- **ScriptQA** (`scripts.yaml`) — Runs shfmt and ShellCheck on shell scripts, validates Thrift code generation.

## Contributing

Per `.github/CONTRIBUTING.md`: search existing issues first, email dev list for feature ideas, commit messages should start with issue number. Build verification: `mvn clean verify -DskipITs`.
