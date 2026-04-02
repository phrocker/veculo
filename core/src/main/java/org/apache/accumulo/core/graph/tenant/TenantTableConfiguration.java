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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.graph.EdgeFilterIterator;
import org.apache.accumulo.core.graph.GraphSchema;
import org.apache.accumulo.core.graph.LatentEdgeDiscoveryIterator;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.io.Text;

/**
 * Factory for {@link NewTableConfiguration} objects for tenant graph tables. Configures locality
 * groups, iterators at appropriate scopes, and tablet availability.
 *
 * <p>
 * Iterator attachment plan:
 * <ul>
 * <li><b>scan</b>: TenantVisibilityFilter(5), EdgeFilter(15)</li>
 * <li><b>minc</b>: AutoEmbed(10), EntityExtract(15)</li>
 * <li><b>majc</b>: SemanticEdge(10), SphericalTessellation(15), GraphRank(20), Summary(25),
 * EmbeddingRefresh(30)</li>
 * </ul>
 *
 * <p>
 * AnomalyDetect is intentionally NOT auto-attached (expensive, on-demand only via scan iterator
 * setting).
 */
public final class TenantTableConfiguration {

  private static final int TENANT_FILTER_PRIORITY = 5;
  private static final int EDGE_FILTER_PRIORITY = 15;

  // Minc priorities must avoid priority 20 (used by default VersioningIterator)
  private static final int AUTO_EMBED_PRIORITY = 25;
  private static final int ENTITY_EXTRACT_PRIORITY = 30;
  private static final int MULTIMODAL_EXTRACT_PRIORITY = 35;

  // Vidx majc priorities
  private static final int LATENT_EDGE_DISCOVERY_PRIORITY = 10;

  // Majc priorities must avoid priority 20 (used by default VersioningIterator)
  private static final int SEMANTIC_EDGE_PRIORITY = 30;
  private static final int TESSELLATION_PRIORITY = 35;
  private static final int GRAPH_RANK_PRIORITY = 40;
  private static final int SUMMARY_PRIORITY = 45;
  private static final int EMBEDDING_REFRESH_PRIORITY = 50;

  private TenantTableConfiguration() {}

  /**
   * Builds a NewTableConfiguration for a tenant's graph table with all iterators attached.
   *
   * <p>
   * Configures:
   * <ul>
   * <li>Locality group "vertex" containing column family "V"</li>
   * <li>Scan-scope: TenantVisibilityFilter(5), EdgeFilter(15)</li>
   * <li>Minc-scope: AutoEmbed(10), EntityExtract(15)</li>
   * <li>Majc-scope: SemanticEdge(10), SphericalTessellation(15), GraphRank(20), Summary(25),
   * EmbeddingRefresh(30)</li>
   * <li>{@link TabletAvailability#HOSTED} for low-latency graph queries</li>
   * </ul>
   *
   * @param context the tenant context providing the visibility label
   * @return a configured NewTableConfiguration
   */
  public static NewTableConfiguration build(TenantContext context) {
    // Scan-scope iterators (no TenantVisibilityFilter — per-tenant clusters provide isolation)
    IteratorSetting edgeFilterSetting =
        new IteratorSetting(EDGE_FILTER_PRIORITY, "edgeFilter", EdgeFilterIterator.class);

    // No minc-scope iterators — marker-based extraction breaks locality groups.
    // The worker service scans for unprocessed file vertices directly.

    // WAL durability: sync ensures every write is persisted to PVC-backed WAL before ACK.
    // With tservers on StatefulSet+PVC, WAL sync is ~1ms (local SSD) vs ~200ms on GCS.
    // This provides crash durability without the latency penalty of cloud storage WALs.
    Map<String,String> tableProps = new HashMap<>();
    tableProps.put("table.durability", "sync");

    return new NewTableConfiguration().setLocalityGroups(getLocalityGroups())
        .setProperties(tableProps)
        // scan
        .attachIterator(edgeFilterSetting, EnumSet.of(IteratorScope.scan))
        .withInitialTabletAvailability(TabletAvailability.HOSTED);
  }

  /**
   * Builds a NewTableConfiguration for a tenant's graph table in shared cluster mode. Identical to
   * {@link #build(TenantContext)} but additionally attaches a {@link TenantVisibilityFilter} at
   * scan scope for multi-tenant visibility isolation.
   *
   * <p>
   * Shared clusters host multiple tenants in the same Accumulo instance, relying on the visibility
   * filter to enforce data isolation instead of separate per-tenant clusters.
   *
   * @param context the tenant context providing the visibility label
   * @return a configured NewTableConfiguration with visibility filtering
   */
  public static NewTableConfiguration buildShared(TenantContext context) {
    // Scan-scope iterators (includes TenantVisibilityFilter for shared cluster isolation)
    IteratorSetting tenantFilterSetting =
        TenantVisibilityFilter.buildSetting(TENANT_FILTER_PRIORITY, context.getVisibilityLabel());
    IteratorSetting edgeFilterSetting =
        new IteratorSetting(EDGE_FILTER_PRIORITY, "edgeFilter", EdgeFilterIterator.class);

    // No minc-scope iterators — marker-based extraction breaks locality groups.
    // The worker service scans for unprocessed file vertices directly.

    // WAL durability: sync for PVC-backed WALs (~1ms local SSD vs ~200ms GCS).
    Map<String,String> tableProps = new HashMap<>();
    tableProps.put("table.durability", "sync");

    return new NewTableConfiguration().setLocalityGroups(getLocalityGroups())
        .setProperties(tableProps)
        // scan (with visibility filter for shared cluster)
        .attachIterator(tenantFilterSetting, EnumSet.of(IteratorScope.scan))
        .attachIterator(edgeFilterSetting, EnumSet.of(IteratorScope.scan))
        .withInitialTabletAvailability(TabletAvailability.HOSTED);
  }

  /**
   * Builds a NewTableConfiguration for the vidx (vector index) table. Minimal config: just the
   * TenantVisibilityFilter at scan scope for ABAC enforcement.
   *
   * @param context the tenant context providing the visibility label
   * @return a configured NewTableConfiguration for the vidx table
   */
  public static NewTableConfiguration buildVidx(TenantContext context) {
    IteratorSetting latentEdgeSetting = new IteratorSetting(LATENT_EDGE_DISCOVERY_PRIORITY,
        "latentEdgeDiscovery", LatentEdgeDiscoveryIterator.class);
    latentEdgeSetting.addOption(LatentEdgeDiscoveryIterator.SIMILARITY_THRESHOLD, "0.85");
    latentEdgeSetting.addOption(LatentEdgeDiscoveryIterator.MAX_PAIRS_PER_CELL, "500");
    latentEdgeSetting.addOption(LatentEdgeDiscoveryIterator.MAX_CELL_BUFFER, "200");

    return new NewTableConfiguration()
        .attachIterator(latentEdgeSetting, EnumSet.of(IteratorScope.majc))
        .withInitialTabletAvailability(TabletAvailability.HOSTED);
  }

  /**
   * Returns the standard locality group map for tenant graph tables.
   *
   * <p>
   * Only the "vertex" column family ("V") is placed in a named locality group. Edge column families
   * are dynamic prefixes (e.g., "E_knows", "E_references") and cannot be enumerated at table
   * creation time, so they remain in the default group.
   *
   * @return locality group map with a single "vertex" group
   */
  public static Map<String,Set<Text>> getLocalityGroups() {
    Map<String,Set<Text>> groups = new HashMap<>();
    Set<Text> vertexFamily = new HashSet<>();
    vertexFamily.add(GraphSchema.VERTEX_COLFAM_TEXT);
    groups.put("vertex", vertexFamily);
    return groups;
  }
}
