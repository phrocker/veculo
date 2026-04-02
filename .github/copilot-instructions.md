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
# Apache Accumulo (Veculo Repository)
Apache Accumulo is a sorted, distributed key/value store based on Google's BigTable design. It is built on top of Apache Hadoop, Zookeeper, and Thrift. This repository contains a multi-module Java Maven project requiring Java 17.

**ALWAYS reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.**

## Working Effectively

### Environment Requirements
- **Java Version**: Java 17 (OpenJDK 17.0.16+ required)
- **Build Tool**: Apache Maven 3.9.11+
- **Memory**: 3-4GB free memory recommended for integration tests
- **Disk Space**: 10GB free disk space recommended for integration tests
- **Network**: **CRITICAL LIMITATION** - Apache snapshots repository (`repository.apache.org`) is not accessible due to DNS restrictions

### Build Status: **DOES NOT BUILD** 
**DO NOT attempt to build this repository** - it will fail due to network restrictions preventing access to essential dependencies.

#### Critical Build Limitation
```bash
# This command WILL FAIL - do not attempt:
mvn clean package
# Error: Could not transfer artifact org.apache.accumulo:accumulo-access:pom:1.0.0-SNAPSHOT 
# from/to apache.snapshots (https://repository.apache.org/snapshots): repository.apache.org
```

**Root Cause**: The project depends on `org.apache.accumulo:accumulo-access:1.0.0-SNAPSHOT` which is only available from Apache snapshots repository. This dependency is essential - it provides core classes like `AccessEvaluator`, `AccessExpression` used throughout the codebase and cannot be removed.

### Working Commands
Despite build limitations, these commands work correctly:

#### Static Analysis and Validation (All work perfectly)
```bash
# Check for unapproved characters - takes 2 seconds
src/build/ci/find-unapproved-chars.sh

# Check for unapproved JUnit usage - takes 1 second  
src/build/ci/find-unapproved-junit.sh

# Check package naming conventions - takes 1 second
src/build/ci/check-module-package-conventions.sh

# Check for startMini without stopMini - takes 1 second
src/build/ci/find-startMini-without-stopMini.sh

# Check for abstract IT classes - takes 1 second
src/build/ci/find-unapproved-abstract-ITs.sh
```

#### Maven Analysis Commands (Work for first 2 modules only)
```bash
# Show active profiles - works, takes 1 second
mvn help:active-profiles

# Validate first 2 modules (accumulo-project, accumulo-start) - takes 3 seconds, FAILS at accumulo-core
mvn -B validate -DverifyFormat

# Show effective POM - works, takes 1 second
mvn help:effective-pom -q
```

#### What Works in Validation
- **accumulo-project module**: Full validation including format checks (SUCCESS)
- **accumulo-start module**: Full validation including format checks (SUCCESS)
- **accumulo-core module and beyond**: FAIL due to dependency resolution (FAILS)

### Repository Structure
```
/home/runner/work/veculo/veculo/
|-- assemble/           # Assembly configuration and distribution
|   |-- conf/          # Configuration files (accumulo-env.sh, etc.)
|   +-- bin/           # Binary scripts
|-- core/              # Core Accumulo libraries (FAILS to build)
|-- server/            # Server components
|   |-- base/          # Base server classes
|   |-- compactor/     # Compaction service
|   |-- gc/            # Garbage collector
|   |-- manager/       # Manager server
|   |-- monitor/       # Monitor server  
|   |-- native/        # Native libraries
|   +-- tserver/       # Tablet server
|-- shell/             # Accumulo shell CLI
|-- start/             # Startup utilities (builds successfully)
|-- test/              # Test harness and utilities
|-- minicluster/       # Mini cluster for testing
+-- src/build/ci/      # CI scripts (all work)
```

## Validation Workflows

### When Making Changes
1. **ALWAYS** run static analysis first (works in any environment):
   ```bash
   src/build/ci/find-unapproved-chars.sh
   src/build/ci/find-unapproved-junit.sh  
   src/build/ci/check-module-package-conventions.sh
   ```

2. **Test format validation on working modules** (takes 3 seconds, NEVER CANCEL):
   ```bash
   # This will validate accumulo-project and accumulo-start, then fail at accumulo-core
   mvn -B validate -DverifyFormat
   ```

3. **DO NOT attempt compilation** - it will fail due to missing accumulo-access dependency

### Module Analysis
- **start/**: Simple startup utilities, minimal dependencies, validates successfully
- **core/**: Contains core Accumulo APIs, depends on accumulo-access (fails)
- **shell/**: Interactive command-line interface for Accumulo
- **server/***: Various server components (manager, tablet server, etc.)

## Network Requirements
**CRITICAL**: This repository requires access to Apache snapshots repository which is not available in this environment.

Required but unavailable repositories:
- `https://repository.apache.org/snapshots` - **BLOCKED** (DNS resolution fails)

Available repositories:
- `https://repo.maven.apache.org/maven2` - Maven Central (ACCESSIBLE)
- `https://repo1.maven.org` - Maven Central Mirror (ACCESSIBLE)

## Testing Capabilities

### What CAN Be Tested
- Code format validation (Java source formatting)
- Static code analysis (character validation, JUnit usage, package conventions)
- Maven project structure analysis
- Repository exploration and documentation

### What CANNOT Be Tested
- **Compilation**: Fails at accumulo-core due to missing dependencies
- **Unit Tests**: Cannot run due to compilation failure
- **Integration Tests**: Cannot run due to compilation failure
- **Application Startup**: Cannot test without successful build
- **End-to-End Scenarios**: Not possible without working build

## CI/CD Context
Based on `.github/workflows/maven.yaml`:
- **Normal CI Build Time**: 60 minutes (with 60-minute timeout)  
- **Unit Tests**: Would normally take significant time with `-Xmx1G` heap
- **Integration Tests**: Require MiniCluster setup with substantial memory/disk
- **QA Checks**: Include SpotBugs, format verification, security scans

**In this environment**: Only static analysis and format validation work.

## Common Tasks Reference

### Repository Root Structure
```bash
ls -la /home/runner/work/veculo/veculo/
# Returns:
# .asf.yaml               - Apache Software Foundation config
# .github/                - GitHub workflows and templates  
# .mvn/                   - Maven wrapper configuration
# DEPENDENCIES            - Dependency notices
# LICENSE, NOTICE         - Apache license files
# README.md               - Project documentation
# TESTING.md              - Testing instructions
# pom.xml                 - Root Maven POM
# assemble/               - Distribution assembly
# core/                   - Core libraries (fails to build)
# server/                 - Server components  
# shell/                  - CLI interface
# start/                  - Startup utilities
# test/                   - Test utilities
```

### Key Configuration Files
- `pom.xml` - Root Maven configuration with 16 modules
- `assemble/conf/accumulo-env.sh` - Environment setup script
- `assemble/conf/accumulo.properties` - Main configuration
- `.github/workflows/maven.yaml` - Main CI workflow (60min timeout)

## Error Messages to Expect

### Build Failure
```
[ERROR] Could not transfer artifact org.apache.accumulo:accumulo-access:pom:1.0.0-SNAPSHOT 
from/to apache.snapshots (https://repository.apache.org/snapshots): repository.apache.org: 
No address associated with hostname
```

### DNS Resolution Failure  
```
** server can't find repository.apache.org: REFUSED
```

### Dependency Resolution
```
[ERROR] Failed to read artifact descriptor for org.apache.accumulo:accumulo-access:jar:1.0.0-SNAPSHOT
```

## Troubleshooting

### "Build hangs or times out"
- **Expected**: Network timeouts when trying to reach Apache snapshots repository
- **Action**: Use static analysis tools instead of build commands

### "Cannot find accumulo-access dependency"
- **Expected**: This dependency is only in Apache snapshots repository  
- **Action**: Document the limitation; cannot be worked around

### "Single module builds fail"
- **Expected**: Maven enforcer rules require full reactor for module convergence
- **Action**: Use `mvn validate` for partial validation only

Remember: The goal is to document and understand this repository's structure and limitations, not to achieve a working build in this restricted environment.