/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeCollectorManager;
import org.apache.iotdb.db.pipe.core.collector.realtime.listener.PipeChangeDataCaptureListener;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.EventType;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PipeRealtimeCollectTest {
  private PipeRealtimeCollectorManager collectorManager;
  private final String dataRegion1 = "dataRegion-1";
  private final String dataRegion2 = "dataRegion-2";
  private final String pattern1 = "root.sg.d";
  private final String pattern2 = "root.sg.d.a";
  private final String[] device = new String[] {"root", "sg", "d"};

  private ExecutorService executorService;

  @Before
  public void setUp() {
    collectorManager = new PipeRealtimeCollectorManager();
    executorService = Executors.newFixedThreadPool(4);
  }

  @After
  public void tearDown() {
    executorService.shutdownNow();
  }

  @Test
  public void testRealtimeCollectProcess() throws ExecutionException, InterruptedException {
    // set up realtime collector
    PipeRealtimeCollector[] collectors =
        new PipeRealtimeCollector[] {
          collectorManager.createPipeRealtimeCollector(pattern1, dataRegion1),
          collectorManager.createPipeRealtimeCollector(pattern2, dataRegion1),
          collectorManager.createPipeRealtimeCollector(pattern1, dataRegion2),
          collectorManager.createPipeRealtimeCollector(pattern2, dataRegion2)
        };

    // start collector 0, 1
    collectors[0].start();
    collectors[1].start();

    // test result of collector 0, 1
    int writeNum = 10;
    Future<?> future1 = write2DataRegion(writeNum, dataRegion1);
    Future<?> future2 = write2DataRegion(writeNum, dataRegion2);
    future1.get();
    future2.get();

    int eventNum = 0;
    for (int i = 0; i < 10000; i++) {
      Event event = collectors[0].supply();
      if (event != null) {
        System.out.println("collector 0 event = " + event);
        eventNum += (event.getType().equals(EventType.TABLET_INSERTION) ? 1 : 2);
      }
    }
    Assert.assertEquals(writeNum << 1, eventNum);

    eventNum = 0;
    for (int i = 0; i < 10000; i++) {
      Event event = collectors[1].supply();
      if (event != null) {
        System.out.println("collector 1 event = " + event);
        eventNum += 1;
      }
    }
    Assert.assertEquals(writeNum, eventNum);

    // start collector 2, 3
    collectors[2].start();
    collectors[3].start();

    // test result of collector 0 - 3
    future1 = write2DataRegion(writeNum, dataRegion1);
    future2 = write2DataRegion(writeNum, dataRegion2);
    future1.get();
    future2.get();

    eventNum = 0;
    for (int i = 0; i < 10000; i++) {
      Event event = collectors[0].supply();
      if (event != null) {
        System.out.println("collector 0 event = " + event);
        eventNum += (event.getType().equals(EventType.TABLET_INSERTION) ? 1 : 2);
      }
    }
    Assert.assertEquals(writeNum << 1, eventNum);

    eventNum = 0;
    for (int i = 0; i < 10000; i++) {
      Event event = collectors[1].supply();
      if (event != null) {
        System.out.println("collector 1 event = " + event);
        eventNum += 1;
      }
    }
    Assert.assertEquals(writeNum, eventNum);

    eventNum = 0;
    for (int i = 0; i < 10000; i++) {
      Event event = collectors[2].supply();
      if (event != null) {
        System.out.println("collector 2 event = " + event);
        eventNum += (event.getType().equals(EventType.TABLET_INSERTION) ? 1 : 2);
      }
    }
    Assert.assertEquals(writeNum << 1, eventNum);

    eventNum = 0;
    for (int i = 0; i < 10000; i++) {
      Event event = collectors[3].supply();
      if (event != null) {
        System.out.println("collector 3 event = " + event);
        eventNum += 1;
      }
    }
    Assert.assertEquals(writeNum, eventNum);
  }

  private Future<?> write2DataRegion(int writeNum, String dataRegionId) {
    return executorService.submit(
        () -> {
          for (int i = 0; i < writeNum; ++i) {
            TsFileResource resource =
                new TsFileResource(new File(dataRegionId, String.format("%s-%s-0-0.tsfile", i, i)));
            resource.updateStartTime(String.join(TsFileConstant.PATH_SEPARATOR, device), 0);

            PipeChangeDataCaptureListener.getInstance()
                .collectPlanNode(
                    dataRegionId,
                    new InsertRowNode(
                        new PlanNodeId(String.valueOf(i)),
                        new PartialPath(device),
                        false,
                        new String[] {"a"},
                        null,
                        0,
                        null,
                        false),
                    resource);
            PipeChangeDataCaptureListener.getInstance()
                .collectPlanNode(
                    dataRegionId,
                    new InsertRowNode(
                        new PlanNodeId(String.valueOf(i)),
                        new PartialPath(device),
                        false,
                        new String[] {"b"},
                        null,
                        0,
                        null,
                        false),
                    resource);
            PipeChangeDataCaptureListener.getInstance().collectTsFile(dataRegionId, resource);
          }
        });
  }
}
