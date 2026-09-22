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

package com.timecho.iotdb.os.io.aws;

import com.sun.net.httpserver.HttpServer;
import com.timecho.iotdb.os.exception.ObjectStorageException;
import com.timecho.iotdb.os.fileSystem.OSURI;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3ObjectStorageReadTimeoutTest {

  private static final OSURI OBJECT = new OSURI("test-bucket", "test.tsfile");
  private static final byte[] CONTENT = new byte[] {1, 2};

  @Test
  public void testQueryReadsCarryTimeoutConfiguration() throws Exception {
    S3Client client = mock(S3Client.class);
    when(client.getObjectAsBytes(any(GetObjectRequest.class)))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), CONTENT));
    when(client.headObject(any(HeadObjectRequest.class)))
        .thenReturn(
            HeadObjectResponse.builder().contentLength(2L).lastModified(Instant.EPOCH).build());
    S3ObjectStorageConnector connector =
        new S3ObjectStorageConnector(client, S3ObjectStorageConnector.READ_REQUEST_CONFIGURATION);

    assertArrayEquals(CONTENT, connector.getRemoteObject(OBJECT, 10, 2));
    assertEquals(2L, connector.getMetaData(OBJECT).length());
    assertTrue(connector.doesObjectExist(OBJECT));

    ArgumentCaptor<GetObjectRequest> get = ArgumentCaptor.forClass(GetObjectRequest.class);
    verify(client).getObjectAsBytes(get.capture());
    assertEquals("bytes=10-11", get.getValue().range());
    assertReadTimeouts(get.getValue().overrideConfiguration().get());
    ArgumentCaptor<HeadObjectRequest> head = ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(client, times(2)).headObject(head.capture());
    for (HeadObjectRequest request : head.getAllValues()) {
      assertReadTimeouts(request.overrideConfiguration().get());
    }
  }

  @Test
  public void testMetadataReadPreservesTimeoutCause() {
    S3Client client = mock(S3Client.class);
    ApiCallTimeoutException timeout = ApiCallTimeoutException.create(60000);
    when(client.headObject(any(HeadObjectRequest.class))).thenThrow(timeout);
    S3ObjectStorageConnector connector =
        new S3ObjectStorageConnector(client, S3ObjectStorageConnector.READ_REQUEST_CONFIGURATION);

    assertSame(
        timeout,
        assertThrows(ObjectStorageException.class, () -> connector.getMetaData(OBJECT)).getCause());
    assertSame(
        timeout,
        assertThrows(ObjectStorageException.class, () -> connector.doesObjectExist(OBJECT))
            .getCause());
  }

  @Test(timeout = 20000)
  public void testStalledResponseBodyTimesOutAndReleasesLock() throws Exception {
    assertSlowReadTimesOut(false);
  }

  @Test(timeout = 20000)
  public void testContinuouslyArrivingResponseBodyStillHasTotalTimeout() throws Exception {
    assertSlowReadTimesOut(true);
  }

  private void assertReadTimeouts(AwsRequestOverrideConfiguration configuration) {
    assertEquals(Duration.ofMinutes(1), configuration.apiCallTimeout().get());
    assertEquals(Duration.ofSeconds(30), configuration.apiCallAttemptTimeout().get());
  }

  private void assertSlowReadTimesOut(boolean trickle) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    ExecutorService handlers = Executors.newCachedThreadPool();
    ExecutorService readers = Executors.newFixedThreadPool(2);
    AtomicBoolean slowResponse = new AtomicBoolean();
    AtomicInteger requests = new AtomicInteger();
    CountDownLatch releaseResponse = new CountDownLatch(1);
    server.setExecutor(handlers);
    server.createContext(
        "/",
        exchange -> {
          requests.incrementAndGet();
          try {
            if (!slowResponse.get()) {
              exchange.sendResponseHeaders(200, CONTENT.length);
              exchange.getResponseBody().write(CONTENT);
              return;
            }
            exchange.sendResponseHeaders(200, 1000);
            exchange.getResponseBody().write(1);
            exchange.getResponseBody().flush();
            if (trickle) {
              // Data keeps arriving, so an idle socket timeout cannot bound this read.
              while (!releaseResponse.await(20, TimeUnit.MILLISECONDS)) {
                exchange.getResponseBody().write(1);
                exchange.getResponseBody().flush();
              }
            } else {
              releaseResponse.await(10, TimeUnit.SECONDS);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (IOException e) {
            // The SDK aborts timed-out HTTP attempts.
          } finally {
            exchange.close();
          }
        });
    server.start();
    try (S3Client client =
        S3Client.builder()
            .region(Region.US_EAST_1)
            .endpointOverride(URI.create("http://127.0.0.1:" + server.getAddress().getPort()))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
            .serviceConfiguration(builder -> builder.pathStyleAccessEnabled(true))
            .overrideConfiguration(
                builder ->
                    builder.retryPolicy(
                        RetryPolicy.builder()
                            .numRetries(10)
                            .backoffStrategy(FixedDelayBackoffStrategy.create(Duration.ZERO))
                            .build()))
            .build()) {
      // Warm up the client before testing the shortened request deadline.
      assertArrayEquals(
          CONTENT,
          new S3ObjectStorageConnector(client, S3ObjectStorageConnector.READ_REQUEST_CONFIGURATION)
              .getRemoteObject(OBJECT, 0, CONTENT.length));
      AwsRequestOverrideConfiguration timeouts =
          S3ObjectStorageConnector.READ_REQUEST_CONFIGURATION.toBuilder()
              .apiCallTimeout(Duration.ofMillis(1500))
              .apiCallAttemptTimeout(Duration.ofMillis(300))
              .build();
      S3ObjectStorageConnector connector = new S3ObjectStorageConnector(client, timeouts);
      slowResponse.set(true);
      requests.set(0);
      Object readerLock = new Object();
      Future<byte[]> blockedRead =
          readers.submit(
              () -> {
                synchronized (readerLock) {
                  return connector.getRemoteObject(OBJECT, 0, 1000);
                }
              });
      ExecutionException failure =
          assertThrows(ExecutionException.class, () -> blockedRead.get(5, TimeUnit.SECONDS));
      assertTrue(failure.getCause() instanceof ObjectStorageException);
      assertTrue(failure.getCause().getCause() instanceof ApiCallTimeoutException);
      assertTrue("The total deadline must also bound retries", requests.get() > 1);

      slowResponse.set(false);
      releaseResponse.countDown();
      Future<byte[]> nextRead =
          readers.submit(
              () -> {
                synchronized (readerLock) {
                  return connector.getRemoteObject(OBJECT, 0, CONTENT.length);
                }
              });
      assertArrayEquals(CONTENT, nextRead.get(5, TimeUnit.SECONDS));
    } finally {
      releaseResponse.countDown();
      server.stop(0);
      handlers.shutdownNow();
      readers.shutdownNow();
      assertTrue(handlers.awaitTermination(5, TimeUnit.SECONDS));
      assertTrue(readers.awaitTermination(5, TimeUnit.SECONDS));
    }
  }
}
