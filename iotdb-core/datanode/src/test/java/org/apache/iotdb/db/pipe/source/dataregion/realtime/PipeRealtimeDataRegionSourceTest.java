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

package org.apache.iotdb.db.pipe.source.dataregion.realtime;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSourceRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEventFactory;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;

public class PipeRealtimeDataRegionSourceTest {

  private static final String TEST_REFERENCE_HOLDER =
      PipeRealtimeDataRegionSourceTest.class.getName();

  @Test
  public void customizeUsesConfiguredTimePartitionOriginForBounds() throws Exception {
    final long previousOrigin = CommonDescriptor.getInstance().getConfig().getTimePartitionOrigin();
    final long previousInterval =
        CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();

    try {
      final long origin = 1_000L;
      final long interval = 3_600_000L;
      CommonDescriptor.getInstance().getConfig().setTimePartitionOrigin(origin);
      CommonDescriptor.getInstance().getConfig().setTimePartitionInterval(interval);
      TimePartitionUtils.setTimePartitionOrigin(origin);
      TimePartitionUtils.setTimePartitionInterval(interval);

      final PipeParameters parameters =
          new PipeParameters(
              new HashMap<String, String>() {
                {
                  put(PipeSourceConstant.EXTRACTOR_START_TIME_KEY, "0");
                  put(PipeSourceConstant.EXTRACTOR_END_TIME_KEY, "3600000");
                }
              });
      try (final ProgressReportTestSource source = new ProgressReportTestSource()) {
        source.validate(new PipeParameterValidator(parameters));
        source.customize(
            parameters,
            new PipeTaskRuntimeConfiguration(
                new PipeTaskSourceRuntimeEnvironment(
                    "pipe", 1L, -1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1))));

        Assert.assertEquals(0, getPrivateLong(source, "startTimePartitionIdLowerBound"));
        Assert.assertEquals(-1, getPrivateLong(source, "endTimePartitionIdUpperBound"));
      }
    } finally {
      CommonDescriptor.getInstance().getConfig().setTimePartitionOrigin(previousOrigin);
      CommonDescriptor.getInstance().getConfig().setTimePartitionInterval(previousInterval);
      TimePartitionUtils.setTimePartitionOrigin(previousOrigin);
      TimePartitionUtils.setTimePartitionInterval(previousInterval);
    }
  }

  @Test
  public void progressReportEventReleasesDroppedHeartbeatEvent() throws Exception {
    try (final ProgressReportTestSource source = new ProgressReportTestSource()) {
      final PipeRealtimeEvent heartbeatEvent = createHeartbeatEvent();
      source.extract(heartbeatEvent);
      Assert.assertEquals(1, source.getEventCount());
      Assert.assertEquals(1, source.getPipeHeartbeatEventCount());

      final PipeRealtimeEvent progressReportEvent = createProgressReportEvent();
      source.extract(progressReportEvent);

      Assert.assertTrue(heartbeatEvent.getEvent().isReleased());
      Assert.assertEquals(0, heartbeatEvent.getEvent().getReferenceCount());
      Assert.assertEquals(1, source.getEventCount());
      Assert.assertEquals(0, source.getPipeHeartbeatEventCount());
      Assert.assertFalse(progressReportEvent.getEvent().isReleased());
    }
  }

  @Test
  public void mergedProgressReportEventReleasesNewEvent() throws Exception {
    try (final ProgressReportTestSource source = new ProgressReportTestSource()) {
      final PipeRealtimeEvent firstProgressReportEvent = createProgressReportEvent();
      final PipeRealtimeEvent secondProgressReportEvent = createProgressReportEvent();

      source.extract(firstProgressReportEvent);
      source.extract(secondProgressReportEvent);

      Assert.assertFalse(firstProgressReportEvent.getEvent().isReleased());
      Assert.assertTrue(secondProgressReportEvent.getEvent().isReleased());
      Assert.assertEquals(1, source.getEventCount());
    }
  }

  private static PipeRealtimeEvent createHeartbeatEvent() {
    final PipeRealtimeEvent event = PipeRealtimeEventFactory.createRealtimeEvent(1, false);
    Assert.assertTrue(event.increaseReferenceCount(TEST_REFERENCE_HOLDER));
    return event;
  }

  private static PipeRealtimeEvent createProgressReportEvent() {
    final ProgressReportEvent progressReportEvent = new ProgressReportEvent("pipe", 1L, null);
    progressReportEvent.bindProgressIndex(MinimumProgressIndex.INSTANCE);

    final PipeRealtimeEvent event =
        PipeRealtimeEventFactory.createRealtimeEvent(progressReportEvent);
    Assert.assertTrue(event.increaseReferenceCount(TEST_REFERENCE_HOLDER));
    return event;
  }

  private static long getPrivateLong(final Object target, final String fieldName) throws Exception {
    final Field field = PipeRealtimeDataRegionSource.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.getLong(target);
  }

  private static class ProgressReportTestSource extends PipeRealtimeDataRegionSource {

    @Override
    protected void doExtract(final PipeRealtimeEvent event) {
      if (event.getEvent() instanceof PipeHeartbeatEvent) {
        extractHeartbeat(event);
        return;
      }
      pendingQueue.offer(event);
    }

    @Override
    public Event supply() {
      return pendingQueue.directPoll();
    }

    @Override
    public boolean isNeedListenToTsFile() {
      return false;
    }

    @Override
    public boolean isNeedListenToInsertNode() {
      return false;
    }
  }
}
