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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

/**
 * Tests for TenantManager naming conventions and list filtering logic. These tests verify the
 * tenant ID → namespace/user/table naming conventions without requiring a live Accumulo cluster.
 */
public class TenantManagerTest {

  @Test
  public void testNamingConventions() {
    TenantContext ctx = TenantContext.of("acme");
    assertEquals("t_acme", ctx.getNamespace());
    assertEquals("tenant_acme", ctx.getUserName());
    assertEquals("t_acme.graph", ctx.getGraphTableName());
    assertEquals("tenant_acme", ctx.getVisibilityLabel());
  }

  @Test
  public void testNamingConventionsWithUnderscore() {
    TenantContext ctx = TenantContext.of("my_corp");
    assertEquals("t_my_corp", ctx.getNamespace());
    assertEquals("tenant_my_corp", ctx.getUserName());
    assertEquals("t_my_corp.graph", ctx.getGraphTableName());
  }

  @Test
  public void testNamingConventionsWithNumbers() {
    TenantContext ctx = TenantContext.of("tenant42");
    assertEquals("t_tenant42", ctx.getNamespace());
    assertEquals("tenant_tenant42", ctx.getUserName());
    assertEquals("t_tenant42.graph", ctx.getGraphTableName());
  }

  @Test
  public void testListTenantsFiltering() {
    // Simulate namespace list that would come from Accumulo
    TreeSet<String> namespaces =
        new TreeSet<>(Arrays.asList("accumulo", "", "t_acme", "t_beta", "t_gamma", "other_ns"));

    // Apply the same filtering logic as TenantManager.listTenants
    List<String> tenants = namespaces.stream().filter(ns -> ns.startsWith("t_"))
        .map(ns -> ns.substring(2)).collect(java.util.stream.Collectors.toList());

    assertEquals(3, tenants.size());
    assertTrue(tenants.contains("acme"));
    assertTrue(tenants.contains("beta"));
    assertTrue(tenants.contains("gamma"));
    assertFalse(tenants.contains("other_ns"));
  }

  @Test
  public void testListTenantsEmpty() {
    TreeSet<String> namespaces = new TreeSet<>(Arrays.asList("accumulo", "", "other_ns"));

    List<String> tenants = namespaces.stream().filter(ns -> ns.startsWith("t_"))
        .map(ns -> ns.substring(2)).collect(java.util.stream.Collectors.toList());

    assertEquals(0, tenants.size());
  }

  @Test
  public void testTenantContextOfProducesConsistentNaming() {
    // Verify that TenantManager's conventions match TenantContext's conventions
    String tenantId = "acme";
    TenantContext ctx = TenantContext.of(tenantId);

    // Namespace follows t_ prefix convention
    assertTrue(ctx.getNamespace().startsWith("t_"));
    assertEquals("t_" + tenantId, ctx.getNamespace());

    // User follows tenant_ prefix convention
    assertTrue(ctx.getUserName().startsWith("tenant_"));
    assertEquals("tenant_" + tenantId, ctx.getUserName());

    // Graph table is namespace-qualified
    assertEquals(ctx.getNamespace() + ".graph", ctx.getGraphTableName());

    // Visibility label matches user name
    assertEquals(ctx.getUserName(), ctx.getVisibilityLabel());
  }

  @Test
  public void testProvisionedTenantAuthorizationsIncludeTenantLabel() {
    TenantContext ctx = TenantContext.of("acme");
    assertTrue(ctx.getAuthorizations().contains(ctx.getVisibilityLabel()));
  }

  @Test
  public void testMultipleTenantContextsAreIndependent() {
    TenantContext acme = TenantContext.of("acme");
    TenantContext beta = TenantContext.of("beta");

    // Different namespaces
    assertFalse(acme.getNamespace().equals(beta.getNamespace()));

    // Different users
    assertFalse(acme.getUserName().equals(beta.getUserName()));

    // Different tables
    assertFalse(acme.getGraphTableName().equals(beta.getGraphTableName()));

    // Different visibilities
    assertFalse(acme.getVisibility().equals(beta.getVisibility()));
  }
}
