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

import jakarta.ws.rs.container.ContainerRequestContext;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

final class RestRequestBodyMemoryManager {

  static final String REQUEST_BODY_MEMORY_RESERVATION_PROPERTY =
      RestRequestBodyMemoryManager.class.getName() + ".requestBodyMemoryReservation";
  private static final AtomicLong RESERVED_MEMORY_IN_BYTES = new AtomicLong();

  private RestRequestBodyMemoryManager() {}

  static Reservation newReservation(long memoryLimitInBytes) {
    return new Reservation(memoryLimitInBytes);
  }

  static long getReservedMemoryInBytes() {
    return RESERVED_MEMORY_IN_BYTES.get();
  }

  static void resetForTest() {
    RESERVED_MEMORY_IN_BYTES.set(0);
  }

  static void registerReservation(
      ContainerRequestContext requestContext, Reservation memoryReservation) {
    if (memoryReservation.isEnabled()) {
      requestContext.setProperty(REQUEST_BODY_MEMORY_RESERVATION_PROPERTY, memoryReservation);
    }
  }

  static void releaseReservation(ContainerRequestContext requestContext) {
    Object reservation = requestContext.getProperty(REQUEST_BODY_MEMORY_RESERVATION_PROPERTY);
    if (reservation instanceof Reservation) {
      ((Reservation) reservation).close();
      requestContext.removeProperty(REQUEST_BODY_MEMORY_RESERVATION_PROPERTY);
    }
  }

  private static boolean tryReserve(long sizeInBytes, long memoryLimitInBytes) {
    if (sizeInBytes <= 0 || memoryLimitInBytes <= 0) {
      return true;
    }

    while (true) {
      long currentReservedBytes = RESERVED_MEMORY_IN_BYTES.get();
      if (currentReservedBytes > memoryLimitInBytes - sizeInBytes) {
        return false;
      }
      if (RESERVED_MEMORY_IN_BYTES.compareAndSet(
          currentReservedBytes, currentReservedBytes + sizeInBytes)) {
        return true;
      }
    }
  }

  static final class Reservation implements AutoCloseable {

    private final long memoryLimitInBytes;
    private final AtomicBoolean released = new AtomicBoolean();
    private long reservedMemoryInBytes;

    private Reservation(long memoryLimitInBytes) {
      this.memoryLimitInBytes = memoryLimitInBytes;
    }

    boolean isEnabled() {
      return memoryLimitInBytes > 0;
    }

    synchronized boolean reserve(long sizeInBytes) {
      if (sizeInBytes <= 0 || !isEnabled()) {
        return true;
      }
      if (released.get()) {
        return false;
      }
      if (!tryReserve(sizeInBytes, memoryLimitInBytes)) {
        return false;
      }
      reservedMemoryInBytes += sizeInBytes;
      return true;
    }

    @Override
    public synchronized void close() {
      if (released.compareAndSet(false, true)) {
        long reservedBytes = reservedMemoryInBytes;
        reservedMemoryInBytes = 0;
        if (reservedBytes > 0) {
          RESERVED_MEMORY_IN_BYTES.addAndGet(-reservedBytes);
        }
      }
    }
  }
}
