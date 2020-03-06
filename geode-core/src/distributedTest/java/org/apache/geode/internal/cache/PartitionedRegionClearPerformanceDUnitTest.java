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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@RunWith(JUnitParamsRunner.class)
public class PartitionedRegionClearPerformanceDUnitTest implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(5);

  private MemberVM locator, server1, server2, server3;

  private ClientVM client;

  private String regionName = "testRegion";

  private int numEntries = 100_000;

  @Before
  public void setup() throws Exception {
    locator = clusterStartupRule.startLocatorVM(0, 0);
    server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    server2 = clusterStartupRule.startServerVM(2, locator.getPort());
    server3 = clusterStartupRule.startServerVM(3, locator.getPort());
    client = clusterStartupRule.startClientVM(4, c -> c.withLocatorConnection(locator.getPort()));
  }

  private void createRegionOnServer(MemberVM server, RegionShortcut type, int numBuckets,
      int redundancy) {
    server.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(type)
          .setPartitionAttributes(
              new PartitionAttributesFactory().setTotalNumBuckets(numBuckets)
                  .setRedundantCopies(redundancy).create())
          .create(regionName);
    });
  }

  private void createRegionOnClient(ClientRegionShortcut type) {
    client.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      clientCache.createClientRegionFactory(type).create(regionName);
    });
  }

  private void createRegionInCluster(RegionShortcut type, int numBuckets, int redundancy) {
    createRegionOnServer(server1, type, numBuckets, redundancy);
    createRegionOnServer(server2, type, numBuckets, redundancy);
    createRegionOnServer(server3, type, numBuckets, redundancy);
    createRegionOnClient(ClientRegionShortcut.CACHING_PROXY);
  }

  private void populateRegion() {
    server1.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion("/" + regionName);
      Map<String, String> entries = new HashMap<>();
      IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, "value-" + i));
      entries.entrySet().forEach(e -> {
        region.put(e.getKey(), e.getValue());
      });
    });
  }

  @Test
  @Parameters({"true", "false"})
  public void testNonPersistentNonRedundant(boolean isClient) {
    createRegionInCluster(RegionShortcut.PARTITION, 113, 0);
    populateRegion();
    server1.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion(regionName);
      assertThat(region.size()).isEqualTo(numEntries);
      assertThat(region.getAttributes().getDataPolicy().withPersistence()).isFalse();
      assertThat(region.getAttributes().getPartitionAttributes().getRedundantCopies()).isEqualTo(0);
    });
    if (isClient) {
      client.invoke(() -> {
        Region clientRegion = ClusterStartupRule.getClientCache().getRegion(regionName);
        long startTime = System.currentTimeMillis();
        Set keys = new HashSet<>();
        IntStream.range(0, numEntries).forEach(i -> keys.add("key-" + i));
        clientRegion.removeAll(keys); // should be clientRegion.clear();
        long endTime = System.currentTimeMillis();
        System.out.println(
            "Partitioned region with " + numEntries + " entries takes " + (endTime - startTime)
                + " milliseconds to clear. " + " isClient=" + isClient);
        assertThat(clientRegion.size()).isEqualTo(0);
      });
    } else {
      server1.invoke(() -> {
        Region region = ClusterStartupRule.getCache().getRegion(regionName);
        long startTime = System.currentTimeMillis();
        region.removeAll(region.keySet()); // should be region.clear();
        long endTime = System.currentTimeMillis();
        System.out.println(
            "Partitioned region with " + numEntries + " entries takes " + (endTime - startTime)
                + " milliseconds to clear. " + " isClient=" + isClient);
      });
    }
    server1.invoke(()-> {
      Region region = ClusterStartupRule.getCache().getRegion(regionName);
      assertThat(region.size()).isEqualTo(0);
    });
  }
/*
  @Test
  public void testRedundancyOneNonPersistent() {
    createRegionInCluster(RegionShortcut.PARTITION_REDUNDANT, 113, 1);
    server1.invoke(() -> {
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
    createRegionInCluster(RegionShortcut.PARTITION_REDUNDANT, 113, 2);
    server1.invoke(() -> {
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
    createRegionInCluster(RegionShortcut.PARTITION_PERSISTENT, 113, 0);
    server1.invoke(() -> {
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
    createRegionInCluster(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT, 113, 1);
    server1.invoke(() -> {
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
    createRegionInCluster(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT, 113, 2);
    server1.invoke(() -> {
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
    createRegionInCluster(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT, 1, 2);

    server1.invoke(() -> {
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
*/
}
