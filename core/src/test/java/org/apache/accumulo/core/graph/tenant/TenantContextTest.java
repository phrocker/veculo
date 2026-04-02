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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

public class TenantContextTest {

  @Test
  public void testDerivedNames() {
    TenantContext ctx = TenantContext.of("acme");
    assertEquals("acme", ctx.getTenantId());
    assertEquals("t_acme", ctx.getNamespace());
    assertEquals("tenant_acme", ctx.getVisibilityLabel());
    assertEquals("t_acme.graph", ctx.getGraphTableName());
    assertEquals("tenant_acme", ctx.getUserName());
  }

  @Test
  public void testVisibility() {
    TenantContext ctx = TenantContext.of("acme");
    assertEquals(new ColumnVisibility("tenant_acme"), ctx.getVisibility());
  }

  @Test
  public void testMinimalAuthorizations() {
    TenantContext ctx = TenantContext.of("acme");
    Authorizations auths = ctx.getAuthorizations();
    assertTrue(auths.contains("tenant_acme"));
    assertEquals(1, auths.getAuthorizations().size());
  }

  @Test
  public void testAdditionalAuthorizations() {
    TenantContext ctx = TenantContext.of("acme", "admin", "analyst");
    Authorizations auths = ctx.getAuthorizations();
    assertTrue(auths.contains("tenant_acme"));
    assertTrue(auths.contains("admin"));
    assertTrue(auths.contains("analyst"));
    assertEquals(3, auths.getAuthorizations().size());
  }

  @Test
  public void testCompoundVisibility() {
    TenantContext ctx = TenantContext.of("acme");
    ColumnVisibility compound = ctx.compoundVisibility("admin");
    assertEquals(new ColumnVisibility("tenant_acme&admin"), compound);
  }

  @Test
  public void testCompoundVisibilityMultipleLabels() {
    TenantContext ctx = TenantContext.of("acme");
    ColumnVisibility compound = ctx.compoundVisibility("admin", "sensitive");
    assertEquals(new ColumnVisibility("tenant_acme&admin&sensitive"), compound);
  }

  @Test
  public void testCompoundVisibilityNoLabels() {
    TenantContext ctx = TenantContext.of("acme");
    ColumnVisibility compound = ctx.compoundVisibility();
    assertEquals(ctx.getVisibility(), compound);
  }

  @Test
  public void testValidateNullTenantId() {
    assertThrows(NullPointerException.class, () -> TenantContext.of(null));
  }

  @Test
  public void testValidateEmptyTenantId() {
    assertThrows(IllegalArgumentException.class, () -> TenantContext.of(""));
  }

  @Test
  public void testValidateInvalidCharacters() {
    assertThrows(IllegalArgumentException.class, () -> TenantContext.of("acme corp"));
    assertThrows(IllegalArgumentException.class, () -> TenantContext.of("acme.corp"));
    assertThrows(IllegalArgumentException.class, () -> TenantContext.of("acme@corp"));
  }

  @Test
  public void testValidTenantIds() {
    // Should not throw
    TenantContext.of("acme");
    TenantContext.of("acme_corp");
    TenantContext.of("acme-corp");
    TenantContext.of("cl-07l2hk");
    TenantContext.of("Acme123");
    TenantContext.of("tenant_42");
    TenantContext.of("A");
  }

  @Test
  public void testEquality() {
    TenantContext ctx1 = TenantContext.of("acme");
    TenantContext ctx2 = TenantContext.of("acme");
    TenantContext ctx3 = TenantContext.of("other");
    assertEquals(ctx1, ctx2);
    assertEquals(ctx1.hashCode(), ctx2.hashCode());
    assertNotEquals(ctx1, ctx3);
  }

  @Test
  public void testEqualityWithDifferentAuths() {
    TenantContext ctx1 = TenantContext.of("acme");
    TenantContext ctx2 = TenantContext.of("acme", "admin");
    assertNotEquals(ctx1, ctx2);
  }

  @Test
  public void testToString() {
    TenantContext ctx = TenantContext.of("acme");
    String str = ctx.toString();
    assertTrue(str.contains("acme"));
    assertTrue(str.contains("t_acme"));
    assertTrue(str.contains("t_acme.graph"));
  }

  @Test
  public void testUnderscoreInTenantId() {
    TenantContext ctx = TenantContext.of("acme_corp_42");
    assertEquals("t_acme_corp_42", ctx.getNamespace());
    assertEquals("tenant_acme_corp_42", ctx.getVisibilityLabel());
    assertEquals("t_acme_corp_42.graph", ctx.getGraphTableName());
  }
}
