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

/** This interface defines the behaviour of a FA(Finite Automation)'s states. */
public interface IFAState {

  /** @return whether this state is the initial state of the belonged FA */
  boolean isInitial();

  /** @return whether this state is one of the final state of the belonged FA */
  boolean isFinal();

  /** @return the index of this state, used for uniquely identifying states in FA */
  int getIndex();
}
