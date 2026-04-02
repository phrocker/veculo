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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Immutable value object encapsulating all tenant-derived state. Provides the namespace, visibility
 * label, table name, and user name conventions for a given tenant.
 */
public final class TenantContext {

  private static final Pattern VALID_TENANT_ID = Pattern.compile("[A-Za-z0-9_-]+");
  private static final String NAMESPACE_PREFIX = "t_";
  private static final String VISIBILITY_PREFIX = "tenant_";

  private final String tenantId;
  private final String namespace;
  private final String visibilityLabel;
  private final ColumnVisibility visibility;
  private final Authorizations authorizations;
  private final String graphTableName;
  private final String userName;

  private TenantContext(String tenantId, Authorizations authorizations) {
    this.tenantId = tenantId;
    this.namespace = NAMESPACE_PREFIX + tenantId;
    this.visibilityLabel = VISIBILITY_PREFIX + tenantId;
    this.visibility = new ColumnVisibility(this.visibilityLabel);
    this.authorizations = authorizations;
    this.graphTableName = this.namespace + ".graph";
    this.userName = VISIBILITY_PREFIX + tenantId;
  }

  /**
   * Creates a TenantContext with minimal authorizations (only the tenant's own label).
   *
   * @param tenantId the tenant identifier (must be non-null, non-empty, alphanumeric + underscore)
   * @return a new TenantContext
   */
  public static TenantContext of(String tenantId) {
    validateTenantId(tenantId);
    String label = VISIBILITY_PREFIX + tenantId;
    return new TenantContext(tenantId, new Authorizations(label));
  }

  /**
   * Creates a TenantContext with the tenant label plus additional authorization labels.
   *
   * @param tenantId the tenant identifier
   * @param additionalAuths additional authorization labels (e.g., "admin", "analyst")
   * @return a new TenantContext
   */
  public static TenantContext of(String tenantId, String... additionalAuths) {
    validateTenantId(tenantId);
    String label = VISIBILITY_PREFIX + tenantId;
    List<String> allAuths = new ArrayList<>();
    allAuths.add(label);
    if (additionalAuths != null) {
      for (String auth : additionalAuths) {
        requireNonNull(auth, "Additional authorization must not be null");
        allAuths.add(auth);
      }
    }
    return new TenantContext(tenantId, new Authorizations(allAuths.toArray(new String[0])));
  }

  /**
   * Returns a compound visibility expression combining the tenant label with additional labels
   * using AND (&amp;). For example, for tenant "acme" with additional label "admin", returns
   * "tenant_acme&amp;admin".
   *
   * @param additionalLabels additional visibility labels to AND with the tenant label
   * @return a ColumnVisibility with the compound expression
   */
  public ColumnVisibility compoundVisibility(String... additionalLabels) {
    if (additionalLabels == null || additionalLabels.length == 0) {
      return visibility;
    }
    StringBuilder sb = new StringBuilder(visibilityLabel);
    for (String label : additionalLabels) {
      requireNonNull(label, "Additional label must not be null");
      sb.append("&").append(label);
    }
    return new ColumnVisibility(sb.toString());
  }

  private static void validateTenantId(String tenantId) {
    requireNonNull(tenantId, "Tenant ID must not be null");
    if (tenantId.isEmpty()) {
      throw new IllegalArgumentException("Tenant ID must not be empty");
    }
    if (!VALID_TENANT_ID.matcher(tenantId).matches()) {
      throw new IllegalArgumentException(
          "Tenant ID must contain only alphanumeric characters, underscores, and hyphens: "
              + tenantId);
    }
  }

  public String getTenantId() {
    return tenantId;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getVisibilityLabel() {
    return visibilityLabel;
  }

  public ColumnVisibility getVisibility() {
    return visibility;
  }

  public Authorizations getAuthorizations() {
    return authorizations;
  }

  public String getGraphTableName() {
    return graphTableName;
  }

  public String getUserName() {
    return userName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TenantContext)) {
      return false;
    }
    TenantContext that = (TenantContext) o;
    return tenantId.equals(that.tenantId) && authorizations.equals(that.authorizations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, authorizations);
  }

  @Override
  public String toString() {
    return "TenantContext{tenantId='" + tenantId + "', namespace='" + namespace + "', graphTable='"
        + graphTableName + "'}";
  }
}
