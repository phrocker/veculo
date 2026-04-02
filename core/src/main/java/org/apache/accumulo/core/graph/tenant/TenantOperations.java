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

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.graph.GraphMutations;
import org.apache.accumulo.core.graph.GraphScanner;
import org.apache.accumulo.core.graph.GraphVectorBridge;

/**
 * Tenant-aware wrappers around existing graph operations. All methods take a {@link TenantContext}
 * to ensure correct visibility labels, authorizations, and table names.
 *
 * <p>
 * These methods delegate to the existing {@link GraphMutations}, {@link GraphScanner}, and
 * {@link GraphVectorBridge} classes without modifying them.
 */
public final class TenantOperations {

  private TenantOperations() {}

  /**
   * Adds a vertex with the tenant's visibility label.
   *
   * @param client AccumuloClient for writing
   * @param ctx the tenant context
   * @param id vertex identifier
   * @param label vertex label
   * @param properties vertex properties
   */
  public static void addVertex(AccumuloClient client, TenantContext ctx, String id, String label,
      Map<String,String> properties) throws TableNotFoundException, MutationsRejectedException {
    Mutation m = GraphMutations.addVertex(id, label, properties, ctx.getVisibility());
    try (BatchWriter writer = client.createBatchWriter(ctx.getGraphTableName())) {
      writer.addMutation(m);
    }
  }

  /**
   * Adds a directed edge with the tenant's visibility label.
   *
   * @param client AccumuloClient for writing
   * @param ctx the tenant context
   * @param src source vertex ID
   * @param dst destination vertex ID
   * @param edgeType edge type label
   * @param properties edge properties
   */
  public static void addEdge(AccumuloClient client, TenantContext ctx, String src, String dst,
      String edgeType, Map<String,String> properties)
      throws TableNotFoundException, MutationsRejectedException {
    List<Mutation> mutations =
        GraphMutations.addEdge(src, dst, edgeType, properties, ctx.getVisibility());
    try (BatchWriter writer = client.createBatchWriter(ctx.getGraphTableName())) {
      for (Mutation m : mutations) {
        writer.addMutation(m);
      }
    }
  }

  /**
   * Adds a vertex with both properties and an embedding, using the tenant's visibility label.
   *
   * @param client AccumuloClient for writing
   * @param ctx the tenant context
   * @param id vertex identifier
   * @param label vertex label
   * @param embedding vector embedding
   * @param properties vertex properties
   */
  public static void addVertexWithEmbedding(AccumuloClient client, TenantContext ctx, String id,
      String label, float[] embedding, Map<String,String> properties) throws Exception {
    try (BatchWriter writer = client.createBatchWriter(ctx.getGraphTableName())) {
      GraphVectorBridge.addVertexWithEmbedding(writer, id, label, embedding, properties,
          ctx.getVisibility());
    }
  }

  /**
   * Reads all vertex properties for a given vertex ID using the tenant's authorizations.
   *
   * @param client AccumuloClient for reading
   * @param ctx the tenant context
   * @param vertexId the vertex to read
   * @return map of property name to value
   */
  public static Map<String,String> getVertexProperties(AccumuloClient client, TenantContext ctx,
      String vertexId) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    try (Scanner scanner = client.createScanner(ctx.getGraphTableName(), ctx.getAuthorizations())) {
      return GraphScanner.getVertexProperties(scanner, vertexId);
    }
  }

  /**
   * Gets the neighbor vertex IDs for outgoing edges of the specified type using the tenant's
   * authorizations.
   *
   * @param client AccumuloClient for reading
   * @param ctx the tenant context
   * @param vertexId the source vertex
   * @param edgeType the edge type to follow
   * @return list of neighbor vertex IDs
   */
  public static List<String> getNeighbors(AccumuloClient client, TenantContext ctx, String vertexId,
      String edgeType) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    try (Scanner scanner = client.createScanner(ctx.getGraphTableName(), ctx.getAuthorizations())) {
      return GraphScanner.getNeighbors(scanner, vertexId, edgeType);
    }
  }

  /**
   * Full RAG pipeline: vector similarity search followed by graph neighborhood expansion, scoped to
   * the tenant's table and authorizations.
   *
   * @param client AccumuloClient for reading
   * @param ctx the tenant context
   * @param queryVector the query embedding
   * @param topK number of similar vertices to find
   * @param threshold minimum similarity score
   * @param edgeType edge type to traverse (null for all)
   * @param depth graph traversal depth
   * @return map from similar vertex IDs to their graph neighborhoods
   */
  public static Map<String,List<String>> vectorGraphQuery(AccumuloClient client, TenantContext ctx,
      float[] queryVector, int topK, float threshold, String edgeType, int depth)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    return GraphVectorBridge.vectorGraphQuery(client, ctx.getGraphTableName(),
        ctx.getAuthorizations(), queryVector, topK, threshold, edgeType, depth);
  }

  /**
   * Creates a BatchWriter for the tenant's graph table, for custom batch operations.
   *
   * @param client AccumuloClient
   * @param ctx the tenant context
   * @return a BatchWriter for the tenant's graph table
   */
  public static BatchWriter createBatchWriter(AccumuloClient client, TenantContext ctx)
      throws TableNotFoundException {
    return client.createBatchWriter(ctx.getGraphTableName());
  }

  /**
   * Creates a Scanner for the tenant's graph table with the tenant's authorizations.
   *
   * @param client AccumuloClient
   * @param ctx the tenant context
   * @return a Scanner configured with the tenant's authorizations
   */
  public static Scanner createScanner(AccumuloClient client, TenantContext ctx)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    return client.createScanner(ctx.getGraphTableName(), ctx.getAuthorizations());
  }
}
