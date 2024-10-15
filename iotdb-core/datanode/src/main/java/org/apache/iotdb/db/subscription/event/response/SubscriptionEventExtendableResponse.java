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

import java.util.LinkedList;
import java.util.stream.Stream;

public abstract class SubscriptionEventExtendableResponse<E>
    implements SubscriptionEventResponse<E> {

  private final LinkedList<E> responses;

  protected SubscriptionEventExtendableResponse() {
    this.responses = new LinkedList<>();
  }

  protected void offer(final E response) {
    responses.addLast(response);
  }

  protected E poll() {
    return responses.isEmpty() ? null : responses.removeFirst();
  }

  protected E peekFirst() {
    return responses.isEmpty() ? null : responses.getFirst();
  }

  protected E peekLast() {
    return responses.isEmpty() ? null : responses.getLast();
  }

  protected Stream<E> stream() {
    return responses.stream();
  }

  protected int size() {
    return responses.size();
  }

  protected boolean isEmpty() {
    return responses.isEmpty();
  }
}
