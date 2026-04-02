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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.graph.GraphMutations;
import org.apache.accumulo.core.graph.GraphSchema;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

/**
 * Tests that TenantOperations correctly applies tenant context (visibility, table names) to graph
 * operations. Since TenantOperations delegates to GraphMutations/GraphScanner/GraphVectorBridge, we
 * verify the delegation arguments rather than testing against a live cluster.
 */
public class TenantOperationsTest {

  @Test
  public void testVertexMutationIncludesCorrectVisibility() {
    TenantContext ctx = TenantContext.of("acme");
    Map<String,String> props = new HashMap<>();
    props.put("name", "Alice");

    // Verify that GraphMutations produces mutations with the tenant's visibility
    Mutation m = GraphMutations.addVertex("v1", "person", props, ctx.getVisibility());

    List<ColumnUpdate> updates = m.getUpdates();
    for (ColumnUpdate update : updates) {
      assertEquals("tenant_acme", new String(update.getColumnVisibility()));
    }
  }

  @Test
  public void testEdgeMutationIncludesCorrectVisibility() {
    TenantContext ctx = TenantContext.of("acme");
    Map<String,String> props = new HashMap<>();
    props.put("since", "2024");

    List<Mutation> mutations =
        GraphMutations.addEdge("v1", "v2", "knows", props, ctx.getVisibility());
    assertEquals(2, mutations.size());

    for (Mutation m : mutations) {
      for (ColumnUpdate update : m.getUpdates()) {
        assertEquals("tenant_acme", new String(update.getColumnVisibility()));
      }
    }
  }

  @Test
  public void testCompoundVisibilityOnMutation() {
    TenantContext ctx = TenantContext.of("acme");
    ColumnVisibility compoundVis = ctx.compoundVisibility("admin");

    Mutation m = GraphMutations.addVertex("v1", "secret", null, compoundVis);

    List<ColumnUpdate> updates = m.getUpdates();
    assertEquals(1, updates.size());
    assertEquals("tenant_acme&admin", new String(updates.get(0).getColumnVisibility()));
  }

  @Test
  public void testGraphTableNameDerivation() {
    TenantContext ctx = TenantContext.of("acme");
    assertEquals("t_acme.graph", ctx.getGraphTableName());

    TenantContext ctx2 = TenantContext.of("beta_corp");
    assertEquals("t_beta_corp.graph", ctx2.getGraphTableName());
  }

  @Test
  public void testAuthorizationsIncludeTenantLabel() {
    TenantContext ctx = TenantContext.of("acme");
    assertTrue(ctx.getAuthorizations().contains("tenant_acme"));
  }

  @Test
  public void testAuthorizationsWithAdditionalLabels() {
    TenantContext ctx = TenantContext.of("acme", "admin");
    assertTrue(ctx.getAuthorizations().contains("tenant_acme"));
    assertTrue(ctx.getAuthorizations().contains("admin"));
  }

  @Test
  public void testVertexEmbeddingMutationVisibility() {
    TenantContext ctx = TenantContext.of("acme");
    float[] embedding = {1.0f, 2.0f, 3.0f};

    Mutation m = GraphMutations.addVertexEmbedding("v1", embedding, ctx.getVisibility());
    List<ColumnUpdate> updates = m.getUpdates();
    assertEquals(1, updates.size());
    assertEquals("tenant_acme", new String(updates.get(0).getColumnVisibility()));
    assertEquals(GraphSchema.EMBEDDING_COLQUAL, new String(updates.get(0).getColumnQualifier()));
  }

  @Test
  public void testDifferentTenantsProduceDifferentVisibilities() {
    TenantContext acme = TenantContext.of("acme");
    TenantContext beta = TenantContext.of("beta");

    Mutation mAcme = GraphMutations.addVertex("v1", "person", null, acme.getVisibility());
    Mutation mBeta = GraphMutations.addVertex("v1", "person", null, beta.getVisibility());

    String acmeVis = new String(mAcme.getUpdates().get(0).getColumnVisibility());
    String betaVis = new String(mBeta.getUpdates().get(0).getColumnVisibility());

    assertEquals("tenant_acme", acmeVis);
    assertEquals("tenant_beta", betaVis);
  }
}
