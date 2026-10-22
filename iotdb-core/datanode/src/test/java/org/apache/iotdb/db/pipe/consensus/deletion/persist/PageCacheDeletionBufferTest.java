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

package org.apache.iotdb.db.pipe.consensus.deletion.persist;

import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.consensus.deletion.recover.DeletionReader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

public class PageCacheDeletionBufferTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private int previousDalBufferSize;

  @Before
  public void setUp() {
    previousDalBufferSize = PageCacheDeletionBuffer.DAL_BUFFER_SIZE;
  }

  @After
  public void tearDown() {
    PageCacheDeletionBuffer.setDalBufferSize(previousDalBufferSize);
  }

  @Test
  public void testDequeuedDeletionIsNotReportedAsFlushed() throws Exception {
    final CountDownLatch serializationStarted = new CountDownLatch(1);
    final CountDownLatch continueSerialization = new CountDownLatch(1);
    final int dataRegionId = 1;
    final DeleteDataNode deleteDataNode =
        new DeleteDataNode(
            new PlanNodeId("1"),
            Collections.singletonList(new MeasurementPath("root.vehicle.d2.s0")),
            50,
            150);
    deleteDataNode.setProgressIndex(
        new RecoverProgressIndex(
            IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
            new SimpleProgressIndex(0, 1)));
    final DeletionResource deletionResource =
        new DeletionResource(deleteDataNode, ignored -> {}, dataRegionId) {
          @Override
          public ByteBuffer serialize() {
            serializationStarted.countDown();
            try {
              continueSerialization.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return super.serialize();
          }
        };
    final PageCacheDeletionBuffer deletionBuffer =
        new PageCacheDeletionBuffer(
            dataRegionId, temporaryFolder.newFolder("deletions").getAbsolutePath());

    deletionBuffer.start();
    try {
      deletionBuffer.registerDeletionResource(deletionResource);
      Assert.assertTrue(serializationStarted.await(10, TimeUnit.SECONDS));

      Assert.assertFalse(deletionBuffer.isAllDeletionFlushed());
    } finally {
      continueSerialization.countDown();
      deletionBuffer.close();
    }
    Assert.assertSame(DeletionResource.Status.SUCCESS, deletionResource.waitForResult());
  }

  @Test(timeout = 30000)
  public void testOversizedFirstDeletionFailsWithoutAffectingNextDeletion() throws Exception {
    final DeletionResource oversizedDeletion = spy(createDeletion(1, 32));
    final DeletionResource nextDeletion = createDeletion(2, 1);
    PageCacheDeletionBuffer.setDalBufferSize(1024);
    Assert.assertTrue(
        oversizedDeletion.serialize().position() > PageCacheDeletionBuffer.DAL_BUFFER_SIZE);
    Assert.assertTrue(
        nextDeletion.serialize().position() < PageCacheDeletionBuffer.DAL_BUFFER_SIZE);
    final File directory = temporaryFolder.newFolder("oversized");
    final PageCacheDeletionBuffer deletionBuffer =
        new PageCacheDeletionBuffer(1, directory.getAbsolutePath());
    deletionBuffer.start();
    try {
      deletionBuffer.registerDeletionResource(oversizedDeletion);
      Assert.assertSame(DeletionResource.Status.FAILURE, oversizedDeletion.waitForResult());
      Assert.assertTrue(oversizedDeletion.getCause() instanceof IOException);
      verify(oversizedDeletion).onPersistFailed(any(IOException.class));
      Awaitility.await().atMost(10, TimeUnit.SECONDS).until(deletionBuffer::isAllDeletionFlushed);

      deletionBuffer.registerDeletionResource(nextDeletion);
      Assert.assertSame(DeletionResource.Status.SUCCESS, nextDeletion.waitForResult());
      Awaitility.await().atMost(10, TimeUnit.SECONDS).until(deletionBuffer::isAllDeletionFlushed);
      Assert.assertSame(DeletionResource.Status.FAILURE, oversizedDeletion.waitForResult());
      verify(oversizedDeletion, never()).onPersistSucceed();
    } finally {
      deletionBuffer.close();
    }
    try (DeletionReader reader =
        new DeletionReader(
            new File(directory, "_0-0" + DeletionResourceManager.DELETION_FILE_SUFFIX),
            1,
            ignored -> {})) {
      final List<DeletionResource> recovered = reader.readAllDeletions();
      Assert.assertEquals(1, recovered.size());
      Assert.assertEquals(nextDeletion.getDeleteDataNode(), recovered.get(0).getDeleteDataNode());
    }
  }

  @Test(timeout = 30000)
  public void testThresholdRotationForcesBeforeSuccess() throws Exception {
    assertRotationForcesBeforeSuccess(true);
  }

  @Test(timeout = 30000)
  public void testFullBatchRotationForcesBeforeSuccess() throws Exception {
    assertRotationForcesBeforeSuccess(false);
  }

  private void assertRotationForcesBeforeSuccess(boolean reachesThreshold) throws Exception {
    final DeletionResource firstDeletion = spy(createDeletion(1, 1));
    final DeletionResource nextDeletion = createDeletion(2, 2);
    final int firstDeletionSize = firstDeletion.serialize().position();
    PageCacheDeletionBuffer.setDalBufferSize(
        reachesThreshold ? firstDeletionSize : firstDeletionSize * 2);
    final FileChannel channel = createLoggingChannel();
    final PageCacheDeletionBuffer deletionBuffer =
        new PageCacheDeletionBuffer(1, temporaryFolder.newFolder("rotation").getAbsolutePath());
    deletionBuffer.start();
    replaceLoggingChannel(deletionBuffer, channel);
    final AtomicBoolean closedBeforeSuccess = new AtomicBoolean();
    final CountDownLatch deletionsRegistered = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              Assert.assertTrue(deletionsRegistered.await(10, TimeUnit.SECONDS));
              return invocation.callRealMethod();
            })
        .when(firstDeletion)
        .serialize();
    doAnswer(
            invocation -> {
              closedBeforeSuccess.set(!channel.isOpen());
              invocation.callRealMethod();
              return null;
            })
        .when(firstDeletion)
        .onPersistSucceed();
    try {
      deletionBuffer.registerDeletionResource(firstDeletion);
      if (!reachesThreshold) {
        deletionBuffer.registerDeletionResource(nextDeletion);
      }
      deletionsRegistered.countDown();
      Assert.assertSame(DeletionResource.Status.SUCCESS, firstDeletion.waitForResult());
      Assert.assertTrue(closedBeforeSuccess.get());
      if (!reachesThreshold) {
        Assert.assertSame(DeletionResource.Status.SUCCESS, nextDeletion.waitForResult());
      }
      final InOrder order = inOrder(channel, firstDeletion);
      order.verify(channel).write(any(ByteBuffer.class));
      order.verify(channel).force(false);
      order.verify(firstDeletion).onPersistSucceed();
    } finally {
      deletionsRegistered.countDown();
      deletionBuffer.close();
    }
  }

  @Test(timeout = 30000)
  public void testRotationForceFailureDoesNotReportSuccess() throws Exception {
    final DeletionResource firstDeletion = spy(createDeletion(1, 1));
    final DeletionResource nextDeletion = createDeletion(2, 1);
    PageCacheDeletionBuffer.setDalBufferSize(firstDeletion.serialize().position());
    final FileChannel channel = createLoggingChannel();
    final IOException failure = new IOException("injected force failure");
    doThrow(failure).when(channel).force(false);
    final PageCacheDeletionBuffer deletionBuffer =
        new PageCacheDeletionBuffer(
            1, temporaryFolder.newFolder("force-failure").getAbsolutePath());
    deletionBuffer.start();
    replaceLoggingChannel(deletionBuffer, channel);
    try {
      deletionBuffer.registerDeletionResource(firstDeletion);
      Assert.assertSame(DeletionResource.Status.FAILURE, firstDeletion.waitForResult());
      Assert.assertSame(failure, firstDeletion.getCause());
      Awaitility.await().atMost(10, TimeUnit.SECONDS).until(deletionBuffer::isAllDeletionFlushed);

      doNothing().when(channel).force(false);
      deletionBuffer.registerDeletionResource(nextDeletion);
      Assert.assertSame(DeletionResource.Status.SUCCESS, nextDeletion.waitForResult());
      Awaitility.await().atMost(10, TimeUnit.SECONDS).until(deletionBuffer::isAllDeletionFlushed);
      Assert.assertSame(DeletionResource.Status.FAILURE, firstDeletion.waitForResult());
      verify(firstDeletion, never()).onPersistSucceed();
    } finally {
      deletionBuffer.close();
    }
  }

  private DeletionResource createDeletion(long progressIndex, int pathCount) throws Exception {
    final DeleteDataNode node =
        new DeleteDataNode(
            new PlanNodeId("1"),
            Collections.nCopies(pathCount, new MeasurementPath("root.vehicle.d2.s0")),
            50,
            150);
    node.setProgressIndex(
        new RecoverProgressIndex(
            IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
            new SimpleProgressIndex(0, progressIndex)));
    return new DeletionResource(node, ignored -> {}, 1);
  }

  private FileChannel createLoggingChannel() throws IOException {
    final FileChannel channel = mock(FileChannel.class, withSettings().useConstructor());
    doAnswer(
            invocation -> {
              final ByteBuffer buffer = invocation.getArgument(0);
              final int size = buffer.remaining();
              buffer.position(buffer.limit());
              return size;
            })
        .when(channel)
        .write(any(ByteBuffer.class));
    return channel;
  }

  private void replaceLoggingChannel(PageCacheDeletionBuffer deletionBuffer, FileChannel channel)
      throws Exception {
    final Field field = PageCacheDeletionBuffer.class.getDeclaredField("logChannel");
    field.setAccessible(true);
    ((FileChannel) field.get(deletionBuffer)).close();
    field.set(deletionBuffer, channel);
  }
}
