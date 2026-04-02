/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.graph.tenant;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.TablePermission;

/**
 * Orchestrates tenant lifecycle against a live {@link AccumuloClient}. Manages namespace creation,
 * user provisioning, authorization setup, and table creation for multi-tenant graph isolation.
 *
 * <p>
 * Uses the shared-cluster model: tenants share one Accumulo cluster, isolated by namespaces and
 * cell-level visibility labels.
 */
public final class TenantManager {

  private static final String NAMESPACE_PREFIX = "t_";

  private TenantManager() {}

  /**
   * Provisions a new tenant by creating its namespace, user, authorizations, and graph table.
   *
   * <ol>
   * <li>Create namespace {@code t_<id>}</li>
   * <li>Create user {@code tenant_<id>} with the given password</li>
   * <li>Set user authorizations (at minimum {@code tenant_<id>})</li>
   * <li>Grant namespace READ + WRITE permissions</li>
   * <li>Create the graph table with tenant-specific configuration</li>
   * <li>Grant table READ + WRITE permissions</li>
   * </ol>
   *
   * @param client AccumuloClient with admin privileges
   * @param tenantId the tenant identifier
   * @param password the password for the tenant user
   * @return the TenantContext for the newly provisioned tenant
   */
  public static TenantContext provisionTenant(AccumuloClient client, String tenantId,
      PasswordToken password) throws AccumuloException, AccumuloSecurityException,
      NamespaceExistsException, TableExistsException {
    TenantContext ctx = TenantContext.of(tenantId);

    // 1. Create namespace
    client.namespaceOperations().create(ctx.getNamespace());

    // 2. Create user
    client.securityOperations().createLocalUser(ctx.getUserName(), password);

    // 3. Set user authorizations
    client.securityOperations().changeUserAuthorizations(ctx.getUserName(),
        ctx.getAuthorizations());

    // 4. Grant namespace permissions
    client.securityOperations().grantNamespacePermission(ctx.getUserName(), ctx.getNamespace(),
        NamespacePermission.READ);
    client.securityOperations().grantNamespacePermission(ctx.getUserName(), ctx.getNamespace(),
        NamespacePermission.WRITE);

    // 5. Create graph table with tenant configuration
    NewTableConfiguration tableConfig = TenantTableConfiguration.build(ctx);
    client.tableOperations().create(ctx.getGraphTableName(), tableConfig);

    // 6. Grant table permissions
    client.securityOperations().grantTablePermission(ctx.getUserName(), ctx.getGraphTableName(),
        TablePermission.READ);
    client.securityOperations().grantTablePermission(ctx.getUserName(), ctx.getGraphTableName(),
        TablePermission.WRITE);

    return ctx;
  }

  /**
   * Deprovisions a tenant by deleting its table, namespace, and user.
   *
   * @param client AccumuloClient with admin privileges
   * @param tenantId the tenant identifier
   */
  public static void deprovisionTenant(AccumuloClient client, String tenantId)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      NamespaceNotFoundException, NamespaceNotEmptyException {
    TenantContext ctx = TenantContext.of(tenantId);

    // Delete table first (namespace must be empty before deletion)
    if (client.tableOperations().exists(ctx.getGraphTableName())) {
      client.tableOperations().delete(ctx.getGraphTableName());
    }

    // Delete namespace
    if (client.namespaceOperations().exists(ctx.getNamespace())) {
      client.namespaceOperations().delete(ctx.getNamespace());
    }

    // Delete user
    client.securityOperations().dropLocalUser(ctx.getUserName());
  }

  /**
   * Checks whether a tenant exists by checking for its namespace.
   *
   * @param client AccumuloClient
   * @param tenantId the tenant identifier
   * @return true if the tenant's namespace exists
   */
  public static boolean tenantExists(AccumuloClient client, String tenantId)
      throws AccumuloException, AccumuloSecurityException {
    TenantContext ctx = TenantContext.of(tenantId);
    return client.namespaceOperations().exists(ctx.getNamespace());
  }

  /**
   * Lists all tenant IDs by filtering namespaces with the {@code t_} prefix.
   *
   * @param client AccumuloClient
   * @return list of tenant IDs (without the namespace prefix)
   */
  public static List<String> listTenants(AccumuloClient client)
      throws AccumuloException, AccumuloSecurityException {
    List<String> tenants = new ArrayList<>();
    for (String ns : client.namespaceOperations().list()) {
      if (ns.startsWith(NAMESPACE_PREFIX)) {
        tenants.add(ns.substring(NAMESPACE_PREFIX.length()));
      }
    }
    return tenants;
  }

  /**
   * Adds additional authorization labels to a tenant's user. Performs a read-merge-write since
   * Accumulo's {@code changeUserAuthorizations} is a full replacement.
   *
   * @param client AccumuloClient with admin privileges
   * @param tenantId the tenant identifier
   * @param newAuths additional authorization labels to add
   */
  public static void addTenantAuthorization(AccumuloClient client, String tenantId,
      String... newAuths) throws AccumuloException, AccumuloSecurityException {
    TenantContext ctx = TenantContext.of(tenantId);

    // Read existing authorizations
    Authorizations existing = client.securityOperations().getUserAuthorizations(ctx.getUserName());

    // Merge with new ones
    Set<String> merged = new HashSet<>();
    for (byte[] auth : existing) {
      merged.add(new String(auth));
    }
    for (String auth : newAuths) {
      merged.add(auth);
    }

    // Write back the full set
    client.securityOperations().changeUserAuthorizations(ctx.getUserName(),
        new Authorizations(merged.toArray(new String[0])));
  }
}
