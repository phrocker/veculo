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

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Server-side defense-in-depth iterator that rejects cells whose visibility expression does not
 * contain the required tenant term. This is a safety net — Accumulo's own visibility evaluation is
 * the primary access control mechanism.
 *
 * <p>
 * This filter should be attached at scan scope only (NOT minc/majc) to avoid deleting valid cells
 * during compaction.
 *
 * <p>
 * Empty visibility expressions are always rejected, since every cell in a tenant namespace must be
 * labeled.
 */
public class TenantVisibilityFilter extends Filter {

  public static final String REQUIRED_VISIBILITY_TERM = "requiredVisibilityTerm";

  private String requiredTerm;
  private Pattern termPattern;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    requiredTerm = options.get(REQUIRED_VISIBILITY_TERM);
    if (requiredTerm != null) {
      // Match the term as a complete token bounded by &|() or string boundaries
      termPattern = Pattern.compile("(^|[&|\\(])" + Pattern.quote(requiredTerm) + "($|[&|\\)])");
    }
  }

  @Override
  public boolean accept(Key k, Value v) {
    String visibility = k.getColumnVisibility().toString();

    // Empty visibility → reject (every cell in a tenant namespace must be labeled)
    if (visibility.isEmpty()) {
      return false;
    }

    // If no required term is configured, accept everything with non-empty visibility
    if (requiredTerm == null) {
      return true;
    }

    // Check that the visibility contains the required tenant term as a complete token
    return termPattern.matcher(visibility).find();
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("tenantVisibilityFilter");
    io.setDescription(
        "Defense-in-depth filter that rejects cells without the required tenant visibility term");
    io.addNamedOption(REQUIRED_VISIBILITY_TERM,
        "The visibility term that must be present (e.g., 'tenant_acme')");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    super.validateOptions(options);
    String term = options.get(REQUIRED_VISIBILITY_TERM);
    if (term != null && term.isEmpty()) {
      throw new IllegalArgumentException("Required visibility term must not be empty");
    }
    return true;
  }

  /**
   * Convenience builder for creating an IteratorSetting for this filter.
   *
   * @param priority the iterator priority
   * @param tenantLabel the required visibility term (e.g., "tenant_acme")
   * @return a configured IteratorSetting
   */
  public static IteratorSetting buildSetting(int priority, String tenantLabel) {
    IteratorSetting is =
        new IteratorSetting(priority, "tenantVisFilter", TenantVisibilityFilter.class);
    is.addOption(REQUIRED_VISIBILITY_TERM, tenantLabel);
    return is;
  }
}
