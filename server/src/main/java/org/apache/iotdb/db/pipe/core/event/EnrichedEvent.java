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

package org.apache.iotdb.db.pipe.core.event;

/**
 * EnrichedEvent is an event that can be enriched with additional runtime information. The
 * additional information mainly includes the reference count of the event.
 */
public interface EnrichedEvent {

  /**
   * Increase the reference count of this event.
   *
   * @param holderMessage the message of the invoker
   * @return true if the reference count is increased successfully, false if the event is not
   *     controlled by the invoker, which means the data stored in the event is not safe to use
   */
  boolean increaseReferenceCount(String holderMessage);

  /**
   * Decrease the reference count of this event. If the reference count is decreased to 0, the event
   * can be recycled and the data stored in the event is not safe to use.
   *
   * @param holderMessage the message of the invoker
   * @return true if the reference count is decreased successfully, false otherwise
   */
  boolean decreaseReferenceCount(String holderMessage);

  /**
   * Get the reference count of this event.
   *
   * @return the reference count
   */
  int getReferenceCount();

  /** set the pattern of this event. */
  void setPattern(String pattern);

  /**
   * Get the pattern of this event.
   *
   * @return the pattern
   */
  String getPattern();

  // TODO: ConsensusIndex getConsensusIndex();
}
