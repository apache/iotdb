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

package org.apache.iotdb.db.pipe.core.collector;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.pipe.config.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeDataRegionCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeDataRegionHybridCollector;
import org.apache.iotdb.db.pipe.core.collector.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.task.queue.ListenableUnblockingPendingQueue;
import org.apache.iotdb.db.wal.utils.WALEntryHandler;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.mockito.Mockito.mock;

public class PipeRealtimeCollectTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeRealtimeCollectTest.class);

  private final String dataRegion1 = "1";
  private final String dataRegion2 = "2";
  private final String pattern1 = "root.sg.d";
  private final String pattern2 = "root.sg.d.a";
  private final String[] device = new String[] {"root", "sg", "d"};
  private final AtomicBoolean alive = new AtomicBoolean();
  private File tmpDir;
  private File tsFileDir;

  private ExecutorService writeService;
  private ExecutorService listenerService;

  @Before
  public void setUp() throws IOException {
    writeService = Executors.newFixedThreadPool(2);
    listenerService = Executors.newFixedThreadPool(4);
    tmpDir = new File(Files.createTempDirectory("pipeRealtimeCollect").toString());
    tsFileDir =
        new File(
            tmpDir.getPath()
                + File.separator
                + IoTDBConstant.SEQUENCE_FLODER_NAME
                + File.separator
                + "root.sg");
  }

  @After
  public void tearDown() {
    writeService.shutdownNow();
    listenerService.shutdownNow();
    FileUtils.deleteDirectory(tmpDir);
  }

  @Test
  public void testRealtimeCollectProcess() throws ExecutionException, InterruptedException {
    // set up realtime collector

    try (PipeRealtimeDataRegionHybridCollector collector1 =
            new PipeRealtimeDataRegionHybridCollector(
                null, new ListenableUnblockingPendingQueue<>());
        PipeRealtimeDataRegionHybridCollector collector2 =
            new PipeRealtimeDataRegionHybridCollector(
                null, new ListenableUnblockingPendingQueue<>());
        PipeRealtimeDataRegionHybridCollector collector3 =
            new PipeRealtimeDataRegionHybridCollector(
                null, new ListenableUnblockingPendingQueue<>());
        PipeRealtimeDataRegionHybridCollector collector4 =
            new PipeRealtimeDataRegionHybridCollector(
                null, new ListenableUnblockingPendingQueue<>())) {

      collector1.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeCollectorConstant.COLLECTOR_PATTERN_KEY, pattern1);
                  put(PipeCollectorConstant.DATA_REGION_KEY, dataRegion1);
                }
              }),
          null);
      collector2.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeCollectorConstant.COLLECTOR_PATTERN_KEY, pattern2);
                  put(PipeCollectorConstant.DATA_REGION_KEY, dataRegion1);
                }
              }),
          null);
      collector3.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeCollectorConstant.COLLECTOR_PATTERN_KEY, pattern1);
                  put(PipeCollectorConstant.DATA_REGION_KEY, dataRegion2);
                }
              }),
          null);
      collector4.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeCollectorConstant.COLLECTOR_PATTERN_KEY, pattern2);
                  put(PipeCollectorConstant.DATA_REGION_KEY, dataRegion2);
                }
              }),
          null);

      PipeRealtimeDataRegionCollector[] collectors =
          new PipeRealtimeDataRegionCollector[] {collector1, collector2, collector3, collector4};

      // start collector 0, 1
      collectors[0].start();
      collectors[1].start();

      // test result of collector 0, 1
      int writeNum = 10;
      List<Future<?>> writeFutures =
          Arrays.asList(
              write2DataRegion(writeNum, dataRegion1, 0),
              write2DataRegion(writeNum, dataRegion2, 0));

      alive.set(true);
      List<Future<?>> listenFutures =
          Arrays.asList(
              listen(
                  collectors[0],
                  event -> event instanceof TabletInsertionEvent ? 1 : 2,
                  writeNum << 1),
              listen(collectors[1], event -> 1, writeNum));

      try {
        listenFutures.get(0).get(10, TimeUnit.MINUTES);
        listenFutures.get(1).get(10, TimeUnit.MINUTES);
      } catch (TimeoutException e) {
        LOGGER.warn("Time out when listening collector", e);
        alive.set(false);
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
              write2DataRegion(writeNum, dataRegion1, writeNum),
              write2DataRegion(writeNum, dataRegion2, writeNum));

      alive.set(true);
      listenFutures =
          Arrays.asList(
              listen(
                  collectors[0],
                  event -> event instanceof TabletInsertionEvent ? 1 : 2,
                  writeNum << 1),
              listen(collectors[1], event -> 1, writeNum),
              listen(
                  collectors[2],
                  event -> event instanceof TabletInsertionEvent ? 1 : 2,
                  writeNum << 1),
              listen(collectors[3], event -> 1, writeNum));
      try {
        listenFutures.get(0).get(10, TimeUnit.MINUTES);
        listenFutures.get(1).get(10, TimeUnit.MINUTES);
        listenFutures.get(2).get(10, TimeUnit.MINUTES);
        listenFutures.get(3).get(10, TimeUnit.MINUTES);
      } catch (TimeoutException e) {
        LOGGER.warn("Time out when listening collector", e);
        alive.set(false);
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
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Future<?> write2DataRegion(int writeNum, String dataRegionId, int startNum) {

    File dataRegionDir =
        new File(tsFileDir.getPath() + File.separator + dataRegionId + File.separator + "0");
    boolean ignored = dataRegionDir.mkdirs();
    return writeService.submit(
        () -> {
          for (int i = startNum; i < startNum + writeNum; ++i) {
            File tsFile = new File(dataRegionDir, String.format("%s-%s-0-0.tsfile", i, i));
            try {
              boolean ignored1 = tsFile.createNewFile();
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }

            TsFileResource resource = new TsFileResource(tsFile);
            resource.updateStartTime(String.join(TsFileConstant.PATH_SEPARATOR, device), 0);

            PipeInsertionDataNodeListener.getInstance()
                .listenToInsertNode(
                    dataRegionId,
                    mock(WALEntryHandler.class),
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
            PipeInsertionDataNodeListener.getInstance()
                .listenToInsertNode(
                    dataRegionId,
                    mock(WALEntryHandler.class),
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
            PipeInsertionDataNodeListener.getInstance().listenToTsFile(dataRegionId, resource);
          }
        });
  }

  private Future<?> listen(
      PipeRealtimeDataRegionCollector collector, Function<Event, Integer> weight, int expectNum) {
    return listenerService.submit(
        () -> {
          int eventNum = 0;
          try {
            while (alive.get() && eventNum < expectNum) {
              Event event;
              try {
                event = collector.supply();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              if (event != null) {
                eventNum += weight.apply(event);
              }
            }
          } finally {
            Assert.assertEquals(expectNum, eventNum);
          }
        });
  }
}
