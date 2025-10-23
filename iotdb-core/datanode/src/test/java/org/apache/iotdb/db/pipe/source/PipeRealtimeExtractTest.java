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

package org.apache.iotdb.db.pipe.source;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSourceRuntimeEnvironment;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionHybridSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionLogSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionTsFileSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
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
  private int dataNodeId;

  @Before
  public void setUp() throws IOException {
    dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    writeService = Executors.newFixedThreadPool(2);
    listenerService = Executors.newFixedThreadPool(4);
    tmpDir = new File(Files.createTempDirectory("pipeRealtimeExtractor").toString());
    tsFileDir =
        new File(
            tmpDir.getPath()
                + File.separator
                + IoTDBConstant.SEQUENCE_FOLDER_NAME
                + File.separator
                + "root.sg");
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(dataNodeId);
    writeService.shutdownNow();
    listenerService.shutdownNow();
    FileUtils.deleteFileOrDirectory(tmpDir);
  }

  @Test
  public void testRealtimeExtractProcess() {
    // set up realtime extractor

    try (final PipeRealtimeDataRegionLogSource extractor0 = new PipeRealtimeDataRegionLogSource();
        final PipeRealtimeDataRegionHybridSource extractor1 =
            new PipeRealtimeDataRegionHybridSource();
        final PipeRealtimeDataRegionTsFileSource extractor2 =
            new PipeRealtimeDataRegionTsFileSource();
        final PipeRealtimeDataRegionHybridSource extractor3 =
            new PipeRealtimeDataRegionHybridSource()) {

      final PipeParameters parameters0 =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                }
              });
      final PipeParameters parameters1 =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern2);
                }
              });
      final PipeParameters parameters2 =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern1);
                }
              });
      final PipeParameters parameters3 =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern2);
                }
              });

      final PipeTaskRuntimeConfiguration configuration0 =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  "1",
                  1,
                  Integer.parseInt(dataRegion1),
                  new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1)));
      final PipeTaskRuntimeConfiguration configuration1 =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  "1",
                  1,
                  Integer.parseInt(dataRegion1),
                  new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1)));
      final PipeTaskRuntimeConfiguration configuration2 =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  "1",
                  1,
                  Integer.parseInt(dataRegion2),
                  new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1)));
      final PipeTaskRuntimeConfiguration configuration3 =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskSourceRuntimeEnvironment(
                  "1",
                  1,
                  Integer.parseInt(dataRegion2),
                  new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1)));

      // Some parameters of extractor are validated and initialized during the validation process.
      extractor0.validate(new PipeParameterValidator(parameters0));
      extractor0.customize(parameters0, configuration0);
      extractor1.validate(new PipeParameterValidator(parameters1));
      extractor1.customize(parameters1, configuration1);
      extractor2.validate(new PipeParameterValidator(parameters2));
      extractor2.customize(parameters2, configuration2);
      extractor3.validate(new PipeParameterValidator(parameters3));
      extractor3.customize(parameters3, configuration3);

      final PipeRealtimeDataRegionSource[] extractors =
          new PipeRealtimeDataRegionSource[] {extractor0, extractor1, extractor2, extractor3};

      // start extractor 0, 1
      extractors[0].start();
      extractors[1].start();

      // test result of extractor 0, 1
      final int writeNum = 10;
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
      } catch (final TimeoutException e) {
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
      } catch (final TimeoutException e) {
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
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Future<?> write2DataRegion(
      final int writeNum, final String dataRegionId, final int startNum) {
    final File dataRegionDir =
        new File(tsFileDir.getPath() + File.separator + dataRegionId + File.separator + "0");
    final boolean ignored = dataRegionDir.mkdirs();
    return writeService.submit(
        () -> {
          for (int i = startNum; i < startNum + writeNum; ++i) {
            final File tsFile = new File(dataRegionDir, String.format("%s-%s-0-0.tsfile", i, i));
            try {
              final boolean ignored1 = tsFile.createNewFile();
            } catch (final IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }

            final TsFileResource resource = new TsFileResource(tsFile);
            resource.updateStartTime(
                IDeviceID.Factory.DEFAULT_FACTORY.create(
                    String.join(TsFileConstant.PATH_SEPARATOR, device)),
                0);

            try {
              resource.close();
            } catch (final IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }

            PipeInsertionDataNodeListener.getInstance()
                .listenToInsertNode(
                    dataRegionId,
                    dataRegionId,
                    new InsertRowNode(
                        new PlanNodeId(String.valueOf(i)),
                        new PartialPath(device),
                        false,
                        new String[] {"a"},
                        new TSDataType[] {TSDataType.INT32},
                        0,
                        new Integer[] {1},
                        false),
                    resource);
            PipeInsertionDataNodeListener.getInstance()
                .listenToInsertNode(
                    dataRegionId,
                    dataRegionId,
                    new InsertRowNode(
                        new PlanNodeId(String.valueOf(i)),
                        new PartialPath(device),
                        false,
                        new String[] {"b"},
                        new TSDataType[] {TSDataType.INT32},
                        0,
                        new Integer[] {1},
                        false),
                    resource);
            PipeInsertionDataNodeListener.getInstance()
                .listenToTsFile(dataRegionId, dataRegionId, resource, false);
          }
        });
  }

  private Future<?> listen(
      final PipeRealtimeDataRegionSource extractor,
      final Function<Event, Integer> weight,
      final int expectNum) {
    return listenerService.submit(
        () -> {
          int eventNum = 0;
          try {
            while (alive.get() && eventNum < expectNum) {
              Event event;
              try {
                event = extractor.supply();
              } catch (final Exception e) {
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
