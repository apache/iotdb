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

package org.apache.iotdb.commons.path.fa;

/** This interface defines the behaviour of a FA(Finite Automation)'s transition. */
public interface IFATransition {

  /** @return the value of this transition, which is used to match the events */
  String getAcceptEvent();

  /**
   * @param event event happened on one of the source state of this transition and is trying to find
   *     the next state
   * @return whether this transition can match the event
   */
  boolean isMatch(String event);

  int getIndex();
}
