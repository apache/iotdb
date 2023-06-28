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

package org.apache.iotdb.db.pipe.extractor;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.db.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionHybridExtractor;
import org.apache.iotdb.db.pipe.extractor.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
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

public class PipeRealtimeExtractTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeRealtimeExtractTest.class);

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
    tmpDir = new File(Files.createTempDirectory("pipeRealtimeExtractor").toString());
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
  public void testRealtimeExtractProcess() {
    // set up realtime extractor

    try (PipeRealtimeDataRegionHybridExtractor extractor1 =
            new PipeRealtimeDataRegionHybridExtractor();
        PipeRealtimeDataRegionHybridExtractor extractor2 =
            new PipeRealtimeDataRegionHybridExtractor();
        PipeRealtimeDataRegionHybridExtractor extractor3 =
            new PipeRealtimeDataRegionHybridExtractor();
        PipeRealtimeDataRegionHybridExtractor extractor4 =
            new PipeRealtimeDataRegionHybridExtractor()) {

      extractor1.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeExtractorConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                }
              }),
          new PipeTaskRuntimeConfiguration(
              new PipeTaskExtractorRuntimeEnvironment(
                  "1", 1, Integer.parseInt(dataRegion1), null)));
      extractor2.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeExtractorConstant.EXTRACTOR_PATTERN_KEY, pattern2);
                }
              }),
          new PipeTaskRuntimeConfiguration(
              new PipeTaskExtractorRuntimeEnvironment(
                  "1", 1, Integer.parseInt(dataRegion1), null)));
      extractor3.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeExtractorConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                }
              }),
          new PipeTaskRuntimeConfiguration(
              new PipeTaskExtractorRuntimeEnvironment(
                  "1", 1, Integer.parseInt(dataRegion2), null)));
      extractor4.customize(
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeExtractorConstant.EXTRACTOR_PATTERN_KEY, pattern2);
                }
              }),
          new PipeTaskRuntimeConfiguration(
              new PipeTaskExtractorRuntimeEnvironment(
                  "1", 1, Integer.parseInt(dataRegion2), null)));

      PipeRealtimeDataRegionExtractor[] extractors =
          new PipeRealtimeDataRegionExtractor[] {extractor1, extractor2, extractor3, extractor4};

      // start extractor 0, 1
      extractors[0].start();
      extractors[1].start();

      // test result of extractor 0, 1
      int writeNum = 10;
      List<Future<?>> writeFutures =
          Arrays.asList(
              write2DataRegion(writeNum, dataRegion1, 0),
              write2DataRegion(writeNum, dataRegion2, 0));

      alive.set(true);
      List<Future<?>> listenFutures =
          Arrays.asList(
              listen(
                  extractors[0],
                  event -> event instanceof TabletInsertionEvent ? 1 : 2,
                  writeNum << 1),
              listen(extractors[1], event -> 1, writeNum));

      try {
        listenFutures.get(0).get(10, TimeUnit.MINUTES);
        listenFutures.get(1).get(10, TimeUnit.MINUTES);
      } catch (TimeoutException e) {
        LOGGER.warn("Time out when listening extractor", e);
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

      // start extractor 2, 3
      extractors[2].start();
      extractors[3].start();

      // test result of extractor 0 - 3
      writeFutures =
          Arrays.asList(
              write2DataRegion(writeNum, dataRegion1, writeNum),
              write2DataRegion(writeNum, dataRegion2, writeNum));

      alive.set(true);
      listenFutures =
          Arrays.asList(
              listen(
                  extractors[0],
                  event -> event instanceof TabletInsertionEvent ? 1 : 2,
                  writeNum << 1),
              listen(extractors[1], event -> 1, writeNum),
              listen(
                  extractors[2],
                  event -> event instanceof TabletInsertionEvent ? 1 : 2,
                  writeNum << 1),
              listen(extractors[3], event -> 1, writeNum));
      try {
        listenFutures.get(0).get(10, TimeUnit.MINUTES);
        listenFutures.get(1).get(10, TimeUnit.MINUTES);
        listenFutures.get(2).get(10, TimeUnit.MINUTES);
        listenFutures.get(3).get(10, TimeUnit.MINUTES);
      } catch (TimeoutException e) {
        LOGGER.warn("Time out when listening extractor", e);
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
      PipeRealtimeDataRegionExtractor extractor, Function<Event, Integer> weight, int expectNum) {
    return listenerService.submit(
        () -> {
          int eventNum = 0;
          try {
            while (alive.get() && eventNum < expectNum) {
              Event event;
              try {
                event = extractor.supply();
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
