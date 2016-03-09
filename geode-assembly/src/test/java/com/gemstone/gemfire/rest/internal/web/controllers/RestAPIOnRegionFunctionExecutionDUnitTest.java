/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.rest.internal.web.controllers;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.PartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionTestHelper;
import com.gemstone.gemfire.rest.internal.web.RestFunctionTemplate;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.Serializable;
import java.util.*;

/**
 * Dunit Test to validate OnRegion function execution with REST APIs
 *
 * @since 8.0
 */

public class RestAPIOnRegionFunctionExecutionDUnitTest extends RestAPITestBase {

  private final String REPLICATE_REGION_NAME = "sampleRRegion";

  private final String PR_REGION_NAME = "samplePRRegion";

  public RestAPIOnRegionFunctionExecutionDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  private class SampleFunction extends RestFunctionTemplate {
    public static final String Id = "SampleFunction";

    @Override
    public void execute(FunctionContext context) {
      invocationCount++;
      if (context instanceof RegionFunctionContext) {
        RegionFunctionContext rfContext = (RegionFunctionContext) context;
        rfContext.getDataSet().getCache().getLogger()
            .info("Executing function :  SampleFunction.execute(hasResult=true) with filter: " + rfContext.getFilter() + "  " + rfContext);
        if (rfContext.getArguments() instanceof Boolean) {
          /* return rfContext.getArguments(); */
          if (hasResult()) {
            rfContext.getResultSender().lastResult(
                (Serializable) rfContext.getArguments());
          } else {
            rfContext
                .getDataSet()
                .getCache()
                .getLogger()
                .info(
                    "Executing function :  SampleFunction.execute(hasResult=false) " + rfContext);
            while (true && !rfContext.getDataSet().isDestroyed()) {
              rfContext.getDataSet().getCache().getLogger()
                  .info("For Bug43513 ");
              try {
                Thread.sleep(100);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
              }
            }
          }
        } else if (rfContext.getArguments() instanceof String) {
          String key = (String) rfContext.getArguments();
          if (key.equals("TestingTimeOut")) { // for test
            // PRFunctionExecutionDUnitTest#testRemoteMultiKeyExecution_timeout
            try {
              Thread.sleep(2000);
            } catch (InterruptedException e) {
              rfContext.getDataSet().getCache().getLogger()
                  .warning("Got Exception : Thread Interrupted" + e);
            }
          }
          if (PartitionRegionHelper.isPartitionedRegion(rfContext.getDataSet())) {
            /*
             * return
             * (Serializable)PartitionRegionHelper.getLocalDataForContext(
             * rfContext).get(key);
             */
            rfContext.getResultSender().lastResult(
                (Serializable) PartitionRegionHelper.getLocalDataForContext(
                    rfContext).get(key));
          } else {
            rfContext.getResultSender().lastResult(
                (Serializable) rfContext.getDataSet().get(key));
          }
          /* return (Serializable)rfContext.getDataSet().get(key); */
        } else if (rfContext.getArguments() instanceof Set) {
          Set origKeys = (Set) rfContext.getArguments();
          ArrayList vals = new ArrayList();
          for (Object key : origKeys) {
            Object val = PartitionRegionHelper
                .getLocalDataForContext(rfContext).get(key);
            if (val != null) {
              vals.add(val);
            }
          }
          rfContext.getResultSender().lastResult(vals);
          /* return vals; */
        } else if (rfContext.getArguments() instanceof HashMap) {
          HashMap putData = (HashMap) rfContext.getArguments();
          for (Iterator i = putData.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry me = (Map.Entry) i.next();
            rfContext.getDataSet().put(me.getKey(), me.getValue());
          }
          rfContext.getResultSender().lastResult(Boolean.TRUE);
        } else {
          rfContext.getResultSender().lastResult(Boolean.FALSE);
        }
      } else {
        if (hasResult()) {
          context.getResultSender().lastResult(Boolean.FALSE);
        } else {
          DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
          LogWriter logger = ds.getLogWriter();
          logger.info("Executing in SampleFunction on Server : "
              + ds.getDistributedMember() + "with Context : " + context);
          while (ds.isConnected()) {
            logger
                .fine("Just executing function in infinite loop for Bug43513");
            try {
              Thread.sleep(250);
            } catch (InterruptedException e) {
              return;
            }
          }
        }
      }
    }

    @Override
    public String getId() {
      return Id;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  private void verifyAndResetInvocationCount(final int count) {
    SampleFunction f = (SampleFunction) FunctionService
        .getFunction(SampleFunction.Id);
    assertEquals(count, f.invocationCount);
  }

  private void createPeer(DataPolicy policy) {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(policy);
    Region region = CacheFactory.getAnyInstance().createRegion(REPLICATE_REGION_NAME, factory.create());
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Region Created :" + region);
    assertNotNull(region);
  }

  private boolean createPeerWithPR() {
    RegionAttributes ra = PartitionedRegionTestHelper.createRegionAttrsForPR(0,
        10);
    AttributesFactory raf = new AttributesFactory(ra);
    PartitionAttributesImpl pa = new PartitionAttributesImpl();
    pa.setAll(ra.getPartitionAttributes());
    pa.setTotalNumBuckets(17);
    raf.setPartitionAttributes(pa);

    Region region = CacheFactory.getAnyInstance().createRegion(PR_REGION_NAME, raf.create());
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Region Created :" + region);
    assertNotNull(region);
    return Boolean.TRUE;
  }

  private void populatePRRegion() {
    PartitionedRegion pr = (PartitionedRegion) CacheFactory.getAnyInstance().getRegion(PR_REGION_NAME);
    DistributedSystem.setThreadsSocketPolicy(false);

    for (int i = (pr.getTotalNumberOfBuckets() * 3); i > 0; i--) {
      Integer val = new Integer(i + 1);
      pr.put("execKey-" + i, val);
    }
    // Assert there is data in each bucket
    for (int bid = 0; bid < pr.getTotalNumberOfBuckets(); bid++) {
      assertTrue(pr.getBucketKeys(bid).size() > 0);
    }
  }

  private void populateRRRegion() {
    Region region = CacheFactory.getAnyInstance().getRegion(REPLICATE_REGION_NAME);
    assertNotNull(region);

    final HashSet testKeys = new HashSet();
    for (int i = 17 * 3; i > 0; i--) {
      testKeys.add("execKey-" + i);
    }
    int j = 0;
    for (Iterator i = testKeys.iterator(); i.hasNext(); ) {
      Integer val = new Integer(j++);
      region.put(i.next(), val);
    }

  }

  @Override
  protected String getFunctionID() {
    return SampleFunction.Id;
  }

  private void createCacheAndRegisterFunction() {
    restURLs.add(vm0.invoke(() -> createCacheWithGroups(vm0, null)));
    restURLs.add(vm1.invoke(() -> createCacheWithGroups(vm1, null)));
    restURLs.add(vm2.invoke(() -> createCacheWithGroups(vm2, null)));
    restURLs.add(vm3.invoke(() -> createCacheWithGroups(vm3, null)));

    vm0.invoke(() -> FunctionService.registerFunction(new SampleFunction()));
    vm1.invoke(() -> FunctionService.registerFunction(new SampleFunction()));
    vm2.invoke(() -> FunctionService.registerFunction(new SampleFunction()));
    vm3.invoke(() -> FunctionService.registerFunction(new SampleFunction()));
  }

  public void testOnRegionExecutionWithReplicateRegion() {
    createCacheAndRegisterFunction();

    vm3.invoke(() -> createPeer(DataPolicy.EMPTY));
    vm0.invoke(() -> createPeer(DataPolicy.REPLICATE));
    vm1.invoke(() -> createPeer(DataPolicy.REPLICATE));
    vm2.invoke(() -> createPeer(DataPolicy.REPLICATE));

    vm3.invoke(() -> populateRRRegion());

    CloseableHttpResponse response = executeFunctionThroughRestCall("SampleFunction", REPLICATE_REGION_NAME, null, null, null, null);
    assertEquals(200, response.getStatusLine().getStatusCode());
    assertNotNull(response.getEntity());

    int c0 = vm0.invoke(() -> getInvocationCount());
    int c1 = vm1.invoke(() -> getInvocationCount());
    int c2 = vm2.invoke(() -> getInvocationCount());
    int c3 = vm3.invoke(() -> getInvocationCount());

    assertEquals(1, c0 + c1 + c2 + c3);

    // remove the expected exception
    restURLs.clear();
  }

  public void testOnRegionExecutionWithPartitionRegion() throws Exception {
    createCacheAndRegisterFunction();

    vm0.invoke(() -> createPeerWithPR());
    vm1.invoke(() -> createPeerWithPR());
    vm2.invoke(() -> createPeerWithPR());
    vm3.invoke(() -> createPeerWithPR());

    vm3.invoke(() -> populatePRRegion());

    CloseableHttpResponse response = executeFunctionThroughRestCall("SampleFunction", PR_REGION_NAME, null, null, null, null);
    assertEquals(200, response.getStatusLine().getStatusCode());
    assertNotNull(response.getEntity());

    vm0.invoke(() -> verifyAndResetInvocationCount(1));
    vm1.invoke(() -> verifyAndResetInvocationCount(1));
    vm2.invoke(() -> verifyAndResetInvocationCount(1));
    vm3.invoke(() -> verifyAndResetInvocationCount(1));

    restURLs.clear();
  }

  public void testOnRegionWithFilterExecutionWithPartitionRegion() throws Exception {
    createCacheAndRegisterFunction();

    vm0.invoke(() -> createPeerWithPR());
    vm1.invoke(() -> createPeerWithPR());
    vm2.invoke(() -> createPeerWithPR());
    vm3.invoke(() -> createPeerWithPR());

    vm3.invoke(() -> populatePRRegion());

    CloseableHttpResponse response = executeFunctionThroughRestCall("SampleFunction", PR_REGION_NAME, "key2", null, null, null);
    assertEquals(200, response.getStatusLine().getStatusCode());
    assertNotNull(response.getEntity());

    int c0 = vm0.invoke(() -> getInvocationCount());
    int c1 = vm1.invoke(() -> getInvocationCount());
    int c2 = vm2.invoke(() -> getInvocationCount());
    int c3 = vm3.invoke(() -> getInvocationCount());

    assertEquals(1, (c0 + c1 + c2 + c3));

    restURLs.clear();
  }

//  public void testOnRegionWithFilterExecutionWithPartitionRegionJsonArgs() throws Exception {
//    createCacheAndRegisterFunction();
//
//    vm0.invoke(() -> createPeerWithPR());
//    vm1.invoke(() -> createPeerWithPR());
//    vm2.invoke(() -> createPeerWithPR());
//    vm3.invoke(() -> createPeerWithPR());
//
//    vm3.invoke(() -> populatePRRegion());
//
//    String jsonBody = "["
//        + "{\"@type\": \"double\",\"@value\": 210}"
//        + ",{\"@type\":\"com.gemstone.gemfire.web.rest.domain.Item\","
//        + "\"itemNo\":\"599\",\"description\":\"Part X Free on Bumper Offer\","
//        + "\"quantity\":\"2\","
//        + "\"unitprice\":\"5\","
//        + "\"totalprice\":\"10.00\"}"
//        + "]";
//
//    CloseableHttpResponse response = executeFunctionThroughRestCall("SampleFunction", PR_REGION_NAME, null, jsonBody, null, null);
//    assertEquals(200, response.getStatusLine().getStatusCode());
//    assertNotNull(response.getEntity());
//
//    // Assert that only 1 node has executed the function.
//    int c0 = vm0.invoke(() -> getInvocationCount());
//    int c1 = vm1.invoke(() -> getInvocationCount());
//    int c2 = vm2.invoke(() -> getInvocationCount());
//    int c3 = vm3.invoke(() -> getInvocationCount());
//
//    assertEquals(1, (c0 + c1 + c2 + c3));
//
//    restURLs.clear();
//  }

}
