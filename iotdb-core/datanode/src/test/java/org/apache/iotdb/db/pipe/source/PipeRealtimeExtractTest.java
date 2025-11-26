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
  private final String pattern1 = "root.db.d";
  private final String pattern2 = "root.db.d.a";
  private final String[] device = new String[] {"root", "db", "d"};
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
    tmpDir = new File(Files.createTempDirectory("pipeRealtimeSource").toString());
    tsFileDir =
        new File(
            tmpDir.getPath()
                + File.separator
                + IoTDBConstant.SEQUENCE_FOLDER_NAME
                + File.separator
                + "root.db");
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
    // set up realtime source

    try (final PipeRealtimeDataRegionLogSource source0 = new PipeRealtimeDataRegionLogSource();
        final PipeRealtimeDataRegionHybridSource source1 =
            new PipeRealtimeDataRegionHybridSource();
        final PipeRealtimeDataRegionTsFileSource source2 =
            new PipeRealtimeDataRegionTsFileSource();
        final PipeRealtimeDataRegionHybridSource source3 =
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

      // Some parameters of source are validated and initialized during the validation process.
      source0.validate(new PipeParameterValidator(parameters0));
      source0.customize(parameters0, configuration0);
      source1.validate(new PipeParameterValidator(parameters1));
      source1.customize(parameters1, configuration1);
      source2.validate(new PipeParameterValidator(parameters2));
      source2.customize(parameters2, configuration2);
      source3.validate(new PipeParameterValidator(parameters3));
      source3.customize(parameters3, configuration3);

      final PipeRealtimeDataRegionSource[] sources =
          new PipeRealtimeDataRegionSource[] {source0, source1, source2, source3};

      // start source 0, 1
      sources[0].start();
      sources[1].start();

      // test result of source 0, 1
      final int writeNum = 10;
      List<Future<?>> writeFutures =
          Arrays.asList(
              write2DataRegion(writeNum, dataRegion1, 0),
              write2DataRegion(writeNum, dataRegion2, 0));

      alive.set(true);
      List<Future<?>> listenFutures =
          Arrays.asList(
              listen(
                  sources[0],
                  event -> event instanceof TabletInsertionEvent ? 1 : 2,
                  writeNum << 1),
              listen(sources[1], event -> 1, writeNum));

      try {
        listenFutures.get(0).get(10, TimeUnit.MINUTES);
        listenFutures.get(1).get(10, TimeUnit.MINUTES);
      } catch (final TimeoutException e) {
        LOGGER.warn("Time out when listening source", e);
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

      // start source 2, 3
      sources[2].start();
      sources[3].start();

      // test result of source 0 - 3
      writeFutures =
          Arrays.asList(
              write2DataRegion(writeNum, dataRegion1, writeNum),
              write2DataRegion(writeNum, dataRegion2, writeNum));

      alive.set(true);
      listenFutures =
          Arrays.asList(
              listen(
                  sources[0],
                  event -> event instanceof TabletInsertionEvent ? 1 : 2,
                  writeNum << 1),
              listen(sources[1], event -> 1, writeNum),
              listen(
                  sources[2],
                  event -> event instanceof TabletInsertionEvent ? 1 : 2,
                  writeNum << 1),
              listen(sources[3], event -> 1, writeNum));
      try {
        listenFutures.get(0).get(10, TimeUnit.MINUTES);
        listenFutures.get(1).get(10, TimeUnit.MINUTES);
        listenFutures.get(2).get(10, TimeUnit.MINUTES);
        listenFutures.get(3).get(10, TimeUnit.MINUTES);
      } catch (final TimeoutException e) {
        LOGGER.warn("Time out when listening source", e);
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
      final PipeRealtimeDataRegionSource source,
      final Function<Event, Integer> weight,
      final int expectNum) {
    return listenerService.submit(
        () -> {
          int eventNum = 0;
          try {
            while (alive.get() && eventNum < expectNum) {
              Event event;
              try {
                event = source.supply();
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
