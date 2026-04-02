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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.jupiter.api.Test;

public class TenantVisibilityFilterTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();

  private TreeMap<Key,Value> buildMixedTenantData() {
    TreeMap<Key,Value> data = new TreeMap<>();

    // Tenant acme data
    data.put(new Key("v1", "V", "_label", "tenant_acme"), new Value("person"));
    data.put(new Key("v1", "V", "name", "tenant_acme"), new Value("Alice"));
    data.put(new Key("v1", "E_knows", "v2", "tenant_acme"), new Value("{}"));

    // Tenant beta data
    data.put(new Key("v2", "V", "_label", "tenant_beta"), new Value("person"));
    data.put(new Key("v2", "V", "name", "tenant_beta"), new Value("Bob"));

    // Compound visibility: tenant_acme&admin
    data.put(new Key("v3", "V", "_label", "tenant_acme&admin"), new Value("secret_person"));

    // Empty visibility (should always be rejected)
    data.put(new Key("v4", "V", "_label", ""), new Value("unlabeled"));

    // Data with OR expression: tenant_acme|tenant_beta
    data.put(new Key("v5", "V", "_label", "tenant_acme|tenant_beta"), new Value("shared"));

    return data;
  }

  private List<Key> collectKeys(TenantVisibilityFilter iter) throws IOException {
    List<Key> keys = new ArrayList<>();
    while (iter.hasTop()) {
      keys.add(new Key(iter.getTopKey()));
      iter.next();
    }
    return keys;
  }

  @Test
  public void testFilterForTenantAcme() throws IOException {
    TreeMap<Key,Value> data = buildMixedTenantData();
    TenantVisibilityFilter iter = new TenantVisibilityFilter();

    Map<String,String> options = new HashMap<>();
    options.put(TenantVisibilityFilter.REQUIRED_VISIBILITY_TERM, "tenant_acme");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);

    // Should accept: v1 (3 cells with tenant_acme), v3 (tenant_acme&admin),
    // v5 (tenant_acme|tenant_beta)
    assertEquals(5, keys.size());

    // Should NOT include: v2 (tenant_beta), v4 (empty visibility)
    assertFalse(keys.stream().anyMatch(k -> k.getRow().toString().equals("v2")));
    assertFalse(keys.stream().anyMatch(k -> k.getRow().toString().equals("v4")));
  }

  @Test
  public void testFilterForTenantBeta() throws IOException {
    TreeMap<Key,Value> data = buildMixedTenantData();
    TenantVisibilityFilter iter = new TenantVisibilityFilter();

    Map<String,String> options = new HashMap<>();
    options.put(TenantVisibilityFilter.REQUIRED_VISIBILITY_TERM, "tenant_beta");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);

    // Should accept: v2 (2 cells with tenant_beta), v5 (tenant_acme|tenant_beta)
    assertEquals(3, keys.size());

    // Should NOT include: v1 (tenant_acme), v3 (tenant_acme&admin), v4 (empty)
    assertFalse(keys.stream().anyMatch(k -> k.getRow().toString().equals("v1")));
    assertFalse(keys.stream().anyMatch(k -> k.getRow().toString().equals("v3")));
    assertFalse(keys.stream().anyMatch(k -> k.getRow().toString().equals("v4")));
  }

  @Test
  public void testRejectsEmptyVisibility() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", ""), new Value("unlabeled"));

    TenantVisibilityFilter iter = new TenantVisibilityFilter();
    Map<String,String> options = new HashMap<>();
    options.put(TenantVisibilityFilter.REQUIRED_VISIBILITY_TERM, "tenant_acme");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    assertEquals(0, keys.size());
  }

  @Test
  public void testRejectsEmptyVisibilityEvenWithoutRequiredTerm() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", ""), new Value("unlabeled"));
    data.put(new Key("v2", "V", "_label", "some_label"), new Value("labeled"));

    TenantVisibilityFilter iter = new TenantVisibilityFilter();
    iter.init(new SortedMapIterator(data), new HashMap<>(), null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    assertEquals(1, keys.size());
    assertEquals("v2", keys.get(0).getRow().toString());
  }

  @Test
  public void testAcceptsCompoundVisibility() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", "tenant_acme&admin"), new Value("admin_data"));

    TenantVisibilityFilter iter = new TenantVisibilityFilter();
    Map<String,String> options = new HashMap<>();
    options.put(TenantVisibilityFilter.REQUIRED_VISIBILITY_TERM, "tenant_acme");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    assertEquals(1, keys.size());
  }

  @Test
  public void testDoesNotMatchPartialTerm() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    // "tenant_acme_extra" should NOT match if required term is "tenant_acme"
    // because it's not bounded by delimiters
    data.put(new Key("v1", "V", "_label", "tenant_acme_extra"), new Value("not_acme"));
    // But "tenant_acme" by itself should match
    data.put(new Key("v2", "V", "_label", "tenant_acme"), new Value("is_acme"));

    TenantVisibilityFilter iter = new TenantVisibilityFilter();
    Map<String,String> options = new HashMap<>();
    options.put(TenantVisibilityFilter.REQUIRED_VISIBILITY_TERM, "tenant_acme");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    assertEquals(1, keys.size());
    assertEquals("v2", keys.get(0).getRow().toString());
  }

  @Test
  public void testAcceptsOrExpression() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", "tenant_acme|tenant_beta"), new Value("shared"));

    TenantVisibilityFilter iter = new TenantVisibilityFilter();
    Map<String,String> options = new HashMap<>();
    options.put(TenantVisibilityFilter.REQUIRED_VISIBILITY_TERM, "tenant_acme");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    assertEquals(1, keys.size());
  }

  @Test
  public void testAcceptsParenthesizedExpression() throws IOException {
    TreeMap<Key,Value> data = new TreeMap<>();
    data.put(new Key("v1", "V", "_label", "(tenant_acme&admin)|public"), new Value("complex_vis"));

    TenantVisibilityFilter iter = new TenantVisibilityFilter();
    Map<String,String> options = new HashMap<>();
    options.put(TenantVisibilityFilter.REQUIRED_VISIBILITY_TERM, "tenant_acme");
    iter.init(new SortedMapIterator(data), options, null);
    iter.seek(new Range(), EMPTY_COL_FAMS, false);

    List<Key> keys = collectKeys(iter);
    assertEquals(1, keys.size());
  }

  @Test
  public void testBuildSetting() {
    IteratorSetting is = TenantVisibilityFilter.buildSetting(5, "tenant_acme");
    assertEquals(5, is.getPriority());
    assertEquals("tenant_acme",
        is.getOptions().get(TenantVisibilityFilter.REQUIRED_VISIBILITY_TERM));
    assertEquals(TenantVisibilityFilter.class.getName(), is.getIteratorClass());
  }

  @Test
  public void testDescribeOptions() {
    TenantVisibilityFilter iter = new TenantVisibilityFilter();
    var options = iter.describeOptions();
    assertEquals("tenantVisibilityFilter", options.getName());
    assertTrue(
        options.getNamedOptions().containsKey(TenantVisibilityFilter.REQUIRED_VISIBILITY_TERM));
  }

  @Test
  public void testValidateOptionsRejectsEmptyTerm() {
    TenantVisibilityFilter iter = new TenantVisibilityFilter();
    Map<String,String> options = new HashMap<>();
    options.put(TenantVisibilityFilter.REQUIRED_VISIBILITY_TERM, "");
    assertThrows(IllegalArgumentException.class, () -> iter.validateOptions(options));
  }

  @Test
  public void testValidateOptionsAcceptsValidTerm() {
    TenantVisibilityFilter iter = new TenantVisibilityFilter();
    Map<String,String> options = new HashMap<>();
    options.put(TenantVisibilityFilter.REQUIRED_VISIBILITY_TERM, "tenant_acme");
    assertTrue(iter.validateOptions(options));
  }
}
