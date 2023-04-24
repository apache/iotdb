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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class PipeRealtimeCollectTest {
  private static final Logger logger = LoggerFactory.getLogger(PipeRealtimeCollectTest.class);
  private PipeRealtimeCollectorManager collectorManager;
  private final String dataRegion1 = "dataRegion-1";
  private final String dataRegion2 = "dataRegion-2";
  private final String pattern1 = "root.sg.d";
  private final String pattern2 = "root.sg.d.a";
  private final String[] device = new String[] {"root", "sg", "d"};
  private static volatile boolean alive;

  private ExecutorService writeService;
  private ExecutorService listenerService;

  @Before
  public void setUp() {
    collectorManager = new PipeRealtimeCollectorManager();
    writeService = Executors.newFixedThreadPool(2);
    listenerService = Executors.newFixedThreadPool(4);
  }

  @After
  public void tearDown() {
    writeService.shutdownNow();
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
    List<Future<?>> writeFutures =
        Arrays.asList(
            write2DataRegion(writeNum, dataRegion1), write2DataRegion(writeNum, dataRegion2));

    Function<EventType, Integer> weight1 = type -> type.equals(EventType.TABLET_INSERTION) ? 1 : 2;
    Function<EventType, Integer> weight2 = typ2 -> 1;
    alive = true;
    List<Future<?>> listenFutures =
        Arrays.asList(
            listen(collectors[0], weight1, writeNum << 1),
            listen(collectors[1], weight2, writeNum));

    try {
      listenFutures.get(0).get(10, TimeUnit.MINUTES);
      listenFutures.get(1).get(1, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      logger.warn("Time out when listening collector", e);
      alive = false;
      Assert.fail();
    }
    writeFutures.forEach(
        future -> {
          try {
            future.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });

    // start collector 2, 3
    collectors[2].start();
    collectors[3].start();

    // test result of collector 0 - 3
    writeFutures =
        Arrays.asList(
            write2DataRegion(writeNum, dataRegion1), write2DataRegion(writeNum, dataRegion2));

    alive = true;
    listenFutures =
        Arrays.asList(
            listen(collectors[0], weight1, writeNum << 1),
            listen(collectors[1], weight2, writeNum),
            listen(collectors[2], weight1, writeNum << 1),
            listen(collectors[3], weight2, writeNum));
    try {
      listenFutures.get(0).get(10, TimeUnit.MINUTES);
      listenFutures.get(1).get(1, TimeUnit.MILLISECONDS);
      listenFutures.get(2).get(1, TimeUnit.MILLISECONDS);
      listenFutures.get(3).get(1, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      logger.warn("Time out when listening collector", e);
      alive = false;
      Assert.fail();
    }
    writeFutures.forEach(
        future -> {
          try {
            future.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private Future<?> write2DataRegion(int writeNum, String dataRegionId) {
    return writeService.submit(
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

  private Future<?> listen(
      PipeRealtimeCollector collector, Function<EventType, Integer> weight, int expectNum) {
    return listenerService.submit(
        () -> {
          int eventNum = 0;
          try {
            while (alive && eventNum < expectNum) {
              Event event = collector.supply();
              if (event != null) {
                eventNum += weight.apply(event.getType());
              }
            }
          } finally {
            Assert.assertEquals(expectNum, eventNum);
          }
        });
  }
}
