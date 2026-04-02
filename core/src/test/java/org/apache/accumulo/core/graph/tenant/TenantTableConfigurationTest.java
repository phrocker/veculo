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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.graph.GraphSchema;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class TenantTableConfigurationTest {

  @Test
  public void testLocalityGroups() {
    Map<String,Set<Text>> groups = TenantTableConfiguration.getLocalityGroups();
    assertNotNull(groups);
    assertEquals(1, groups.size());
    assertTrue(groups.containsKey("vertex"));

    Set<Text> vertexFamilies = groups.get("vertex");
    assertEquals(1, vertexFamilies.size());
    assertTrue(vertexFamilies.contains(GraphSchema.VERTEX_COLFAM_TEXT));
  }

  @Test
  public void testBuildReturnsNonNull() {
    TenantContext ctx = TenantContext.of("acme");
    NewTableConfiguration config = TenantTableConfiguration.build(ctx);
    assertNotNull(config);
  }

  @Test
  public void testBuildSetsHostedAvailability() {
    TenantContext ctx = TenantContext.of("acme");
    NewTableConfiguration config = TenantTableConfiguration.build(ctx);
    assertEquals(TabletAvailability.HOSTED, config.getInitialTabletAvailability());
  }

  @Test
  public void testBuildIncludesEdgeFilter() {
    TenantContext ctx = TenantContext.of("acme");
    NewTableConfiguration config = TenantTableConfiguration.build(ctx);
    Map<String,String> props = config.getProperties();

    boolean hasEdgeFilter =
        props.keySet().stream().anyMatch(k -> k.contains("table.iterator.scan.edgeFilter"));
    assertTrue(hasEdgeFilter, "Expected edgeFilter in scan scope configuration");
  }

  @Test
  public void testNoTenantVisibilityFilterOnIsolatedCluster() {
    TenantContext ctx = TenantContext.of("acme");
    NewTableConfiguration config = TenantTableConfiguration.build(ctx);
    Map<String,String> props = config.getProperties();

    // Per-tenant clusters don't need the visibility filter — isolation is at the cluster level
    boolean hasVisFilter = props.keySet().stream().anyMatch(k -> k.contains("tenantVisFilter"));
    assertFalse(hasVisFilter,
        "TenantVisibilityFilter should NOT be attached on per-tenant clusters");
  }

  @Test
  public void testBuildIncludesCompactionIterators() {
    TenantContext ctx = TenantContext.of("acme");
    NewTableConfiguration config = TenantTableConfiguration.build(ctx);
    Map<String,String> props = config.getProperties();

    boolean hasMincIterator =
        props.keySet().stream().anyMatch(k -> k.contains("table.iterator.minc.autoEmbed"));
    assertTrue(hasMincIterator, "Expected autoEmbed at minc scope");

    boolean hasMajcIterator =
        props.keySet().stream().anyMatch(k -> k.contains("table.iterator.majc.tessellation"));
    assertTrue(hasMajcIterator, "Expected tessellation at majc scope");
  }
}
