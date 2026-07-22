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

package org.apache.iotdb.rest.protocol.filter;

import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.rest.i18n.RestMessages;
import org.apache.iotdb.rest.protocol.model.ExecutionStatus;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

@Provider
@PreMatching
public class RequestSizeLimitFilter implements ContainerRequestFilter {

  private static final int PAYLOAD_TOO_LARGE_STATUS_CODE = 413;
  private static final int SERVICE_UNAVAILABLE_STATUS_CODE = 503;

  @Override
  public void filter(ContainerRequestContext requestContext) {
    long maxBodySize =
        IoTDBRestServiceDescriptor.getInstance().getConfig().getRestMaxRequestBodySizeInBytes();
    long memoryLimit =
        IoTDBRestServiceDescriptor.getInstance()
            .getConfig()
            .getRestMaxTotalConcurrentRequestBodySizeInBytes();
    if (maxBodySize <= 0 && memoryLimit <= 0) {
      return;
    }

    int contentLength = requestContext.getLength();
    if (maxBodySize > 0 && contentLength > maxBodySize) {
      requestContext.abortWith(buildPayloadTooLargeResponse(maxBodySize));
      return;
    }

    RestRequestBodyMemoryManager.Reservation memoryReservation =
        RestRequestBodyMemoryManager.newReservation(memoryLimit);
    long memoryReservedByContentLength = 0;
    if (contentLength > 0 && memoryReservation.isEnabled()) {
      if (!memoryReservation.reserve(contentLength)) {
        memoryReservation.close();
        requestContext.abortWith(buildMemoryQuotaExceededResponse(memoryLimit));
        return;
      }
      memoryReservedByContentLength = contentLength;
      RestRequestBodyMemoryManager.registerReservation(requestContext, memoryReservation);
    }

    requestContext.setEntityStream(
        new LimitedInputStream(
            requestContext.getEntityStream(),
            maxBodySize,
            memoryLimit,
            memoryReservation,
            memoryReservedByContentLength));
    if (memoryReservation.isEnabled() && memoryReservedByContentLength == 0) {
      RestRequestBodyMemoryManager.registerReservation(requestContext, memoryReservation);
    }
  }

  private static Response buildPayloadTooLargeResponse(long maxBodySize) {
    return Response.status(PAYLOAD_TOO_LARGE_STATUS_CODE)
        .type(MediaType.APPLICATION_JSON_TYPE)
        .entity(
            new ExecutionStatus()
                .code(PAYLOAD_TOO_LARGE_STATUS_CODE)
                .message(
                    String.format(
                        RestMessages
                            .MESSAGE_REST_REQUEST_BODY_EXCEEDS_LIMIT_ARG_BYTES_USE_SET_CONFIGURATION_REST_MAX_REQUEST_BODY_SIZE_IN_BYTES_BYTES_TO_INCREASE_IT_424392C6,
                        maxBodySize)))
        .build();
  }

  private static Response buildMemoryQuotaExceededResponse(long memoryLimit) {
    return Response.status(SERVICE_UNAVAILABLE_STATUS_CODE)
        .type(MediaType.APPLICATION_JSON_TYPE)
        .entity(
            new ExecutionStatus()
                .code(SERVICE_UNAVAILABLE_STATUS_CODE)
                .message(
                    String.format(
                        RestMessages
                            .MESSAGE_REST_REQUEST_BODY_MEMORY_QUOTA_EXCEEDS_LIMIT_ARG_BYTES_USE_SET_CONFIGURATION_REST_MAX_TOTAL_CONCURRENT_REQUEST_BODY_SIZE_IN_BYTES_BYTES_TO_INCREASE_IT_F07B9DDD,
                        memoryLimit)))
        .build();
  }

  private static class LimitedInputStream extends FilterInputStream {

    private final long maxBodySize;
    private final long memoryLimit;
    private final RestRequestBodyMemoryManager.Reservation memoryReservation;
    private long memoryCoveredBytes;
    private long bytesRead;

    private LimitedInputStream(
        InputStream in,
        long maxBodySize,
        long memoryLimit,
        RestRequestBodyMemoryManager.Reservation memoryReservation,
        long memoryCoveredBytes) {
      super(in);
      this.maxBodySize = maxBodySize;
      this.memoryLimit = memoryLimit;
      this.memoryReservation = memoryReservation;
      this.memoryCoveredBytes = memoryCoveredBytes;
    }

    @Override
    public int read() throws IOException {
      int result = super.read();
      if (result != -1) {
        incrementBytesRead(1);
      }
      return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int result = super.read(b, off, len);
      if (result > 0) {
        incrementBytesRead(result);
      }
      return result;
    }

    private void incrementBytesRead(int increment) {
      bytesRead += increment;
      if (maxBodySize > 0 && bytesRead > maxBodySize) {
        memoryReservation.close();
        throw new WebApplicationException(buildPayloadTooLargeResponse(maxBodySize));
      }
      reserveMemoryIfNecessary();
    }

    private void reserveMemoryIfNecessary() {
      if (bytesRead <= memoryCoveredBytes) {
        return;
      }
      long sizeToReserve = bytesRead - memoryCoveredBytes;
      if (!memoryReservation.reserve(sizeToReserve)) {
        memoryReservation.close();
        throw new WebApplicationException(buildMemoryQuotaExceededResponse(memoryLimit));
      }
      memoryCoveredBytes = bytesRead;
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        memoryReservation.close();
      }
    }
  }
}
