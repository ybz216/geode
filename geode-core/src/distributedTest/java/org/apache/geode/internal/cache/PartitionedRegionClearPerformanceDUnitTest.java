/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class PartitionedRegionClearPerformanceDUnitTest implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  private MemberVM locator, server1;

  private String regionName = "testRegion";

  private int numEntries = 100_000;

  @Before
  public void setup() throws Exception {
    locator = clusterStartupRule.startLocatorVM(0, 0);
    server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    clusterStartupRule.startServerVM(2, locator.getPort());
    clusterStartupRule.startServerVM(3, locator.getPort());
  }

  @Test
  public void testNonPersistentNonRedundant() {
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);

      Map<String, String> entries = new HashMap<>();
      IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, "value-" + i));
      populateRegion(regionName, entries);

      Region region = ClusterStartupRule.getCache().getRegion(regionName);

      assertThat(region.size()).isEqualTo(numEntries);
      assertThat(region.getAttributes().getDataPolicy().withPersistence()).isFalse();
      assertThat(region.getAttributes().getPartitionAttributes().getRedundantCopies()).isEqualTo(0);

      long startTime = System.currentTimeMillis();
      region.removeAll(entries.keySet()); // should be region.clear();
      long endTime = System.currentTimeMillis();
      System.out.println(
          "Partitioned region with " + numEntries + " entries takes " + (endTime - startTime)
              + " milliseconds to clear.");
      assertThat(region.size()).isEqualTo(0);
    });
  }

  @Test
  public void testRedundancyOneNonPersistent() {
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT).setPartitionAttributes(
          new PartitionAttributesFactory().setRedundantCopies(1).create()).create(regionName);

      Map<String, String> entries = new HashMap<>();
      IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, "value-" + i));
      populateRegion(regionName, entries);

      Region region = ClusterStartupRule.getCache().getRegion(regionName);

      assertThat(region.size()).isEqualTo(numEntries);
      assertThat(region.getAttributes().getDataPolicy().withPersistence()).isFalse();
      assertThat(region.getAttributes().getPartitionAttributes().getRedundantCopies()).isEqualTo(1);

      long startTime = System.currentTimeMillis();
      region.removeAll(entries.keySet()); // should be region.clear();
      long endTime = System.currentTimeMillis();
      System.out.println(
          "Partitioned region with " + numEntries + " entries takes " + (endTime - startTime)
              + " milliseconds to clear.");
      assertThat(region.size()).isEqualTo(0);
    });
  }

  @Test
  public void testRedundancyTwoNonPersistent() {
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT).setPartitionAttributes(
          new PartitionAttributesFactory().setRedundantCopies(2).create()).create(regionName);

      Map<String, String> entries = new HashMap<>();
      IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, "value-" + i));
      populateRegion(regionName, entries);

      Region region = ClusterStartupRule.getCache().getRegion(regionName);

      assertThat(region.size()).isEqualTo(numEntries);
      assertThat(region.getAttributes().getDataPolicy().withPersistence()).isFalse();
      assertThat(region.getAttributes().getPartitionAttributes().getRedundantCopies()).isEqualTo(2);

      long startTime = System.currentTimeMillis();
      region.removeAll(entries.keySet()); // should be region.clear();
      long endTime = System.currentTimeMillis();
      System.out.println(
          "Partitioned region with " + numEntries + " entries takes " + (endTime - startTime)
              + " milliseconds to clear.");
      assertThat(region.size()).isEqualTo(0);
    });
  }

  @Test
  public void testPersistentNonRedundant() {
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION_PERSISTENT).create(regionName);

      Map<String, String> entries = new HashMap<>();
      IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, "value-" + i));
      populateRegion(regionName, entries);

      Region region = ClusterStartupRule.getCache().getRegion(regionName);

      assertThat(region.size()).isEqualTo(numEntries);
      assertThat(region.getAttributes().getDataPolicy().withPersistence()).isTrue();
      assertThat(region.getAttributes().getPartitionAttributes().getRedundantCopies()).isEqualTo(0);

      long startTime = System.currentTimeMillis();
      region.removeAll(entries.keySet()); // should be region.clear();
      long endTime = System.currentTimeMillis();
      System.out.println(
          "Partitioned region with " + numEntries + " entries takes " + (endTime - startTime)
              + " milliseconds to clear.");
      assertThat(region.size()).isEqualTo(0);
    });
  }

  @Test
  public void testPersistentRedundancyOne() {
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT)
          .setPartitionAttributes(
              new PartitionAttributesFactory().setRedundantCopies(1).create())
          .create(regionName);

      Map<String, String> entries = new HashMap<>();
      IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, "value-" + i));
      populateRegion(regionName, entries);

      Region region = ClusterStartupRule.getCache().getRegion(regionName);

      assertThat(region.size()).isEqualTo(numEntries);
      assertThat(region.getAttributes().getDataPolicy().withPersistence()).isTrue();
      assertThat(region.getAttributes().getPartitionAttributes().getRedundantCopies()).isEqualTo(1);

      long startTime = System.currentTimeMillis();
      region.removeAll(entries.keySet()); // should be region.clear();
      long endTime = System.currentTimeMillis();
      System.out.println(
          "Partitioned region with " + numEntries + " entries takes " + (endTime - startTime)
              + " milliseconds to clear.");
      assertThat(region.size()).isEqualTo(0);
    });
  }

  @Test
  public void testPersistentRedundancyTwo() {
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT)
          .setPartitionAttributes(
              new PartitionAttributesFactory().setRedundantCopies(2).create())
          .create(regionName);

      Map<String, String> entries = new HashMap<>();
      IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, "value-" + i));
      populateRegion(regionName, entries);

      Region region = ClusterStartupRule.getCache().getRegion(regionName);

      assertThat(region.size()).isEqualTo(numEntries);
      assertThat(region.getAttributes().getDataPolicy().withPersistence()).isTrue();
      assertThat(region.getAttributes().getPartitionAttributes().getRedundantCopies()).isEqualTo(2);

      long startTime = System.currentTimeMillis();
      region.removeAll(entries.keySet()); // should be region.clear();
      long endTime = System.currentTimeMillis();
      System.out.println(
          "Partitioned region with " + numEntries + " entries takes " + (endTime - startTime)
              + " milliseconds to clear.");
      assertThat(region.size()).isEqualTo(0);
    });
  }

  @Test
  public void testOneBucketPersistentRedundancyTwo() {
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT)
          .setPartitionAttributes(
              new PartitionAttributesFactory().setTotalNumBuckets(1).setRedundantCopies(2).create())
          .create(regionName);

      Map<String, String> entries = new HashMap<>();
      IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, "value-" + i));
      populateRegion(regionName, entries);

      Region region = ClusterStartupRule.getCache().getRegion(regionName);

      assertThat(region.size()).isEqualTo(numEntries);
      assertThat(region.getAttributes().getPartitionAttributes().getTotalNumBuckets()).isEqualTo(1);
      assertThat(region.getAttributes().getDataPolicy().withPersistence()).isTrue();
      assertThat(region.getAttributes().getPartitionAttributes().getRedundantCopies()).isEqualTo(2);

      long startTime = System.currentTimeMillis();
      region.removeAll(entries.keySet()); // should be region.clear();
      long endTime = System.currentTimeMillis();
      System.out.println(
          "Partitioned region with " + numEntries + " entries takes " + (endTime - startTime)
              + " milliseconds to clear.");
      assertThat(region.size()).isEqualTo(0);
    });
  }

  private void populateRegion(String regionName, Map<String, String> entries) {
    Region r = ClusterStartupRule.getCache().getRegion("/" + regionName);
    entries.entrySet().forEach(e -> {
      r.put(e.getKey(), e.getValue());
    });
  }
}
