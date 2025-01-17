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

package org.apache.iotdb.db.subscription.event.response;

import org.apache.iotdb.db.subscription.event.SubscriptionEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public interface SubscriptionEventResponse<E> {

  /////////////////////////////// response ///////////////////////////////

  E getCurrentResponse();

  @Deprecated // TBD.
  void prefetchRemainingResponses();

  void fetchNextResponse(final long offset) throws Exception;

  /////////////////////////////// byte buffer ///////////////////////////////

  void trySerializeCurrentResponse();

  void trySerializeRemainingResponses();

  ByteBuffer getCurrentResponseByteBuffer() throws IOException;

  void invalidateCurrentResponseByteBuffer();

  /////////////////////////////// lifecycle ///////////////////////////////

  default void ack(final Consumer<SubscriptionEvent> onCommittedHook) {
    // do nothing
  }

  void nack();

  void cleanUp();

  boolean isCommittable();
}
