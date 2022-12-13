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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This interface defines the behaviour of a FA(Finite Automation), generated from a path pattern or
 * a pattern tree.
 */
public interface IPatternFA {

  /**
   * @param state the source state of the returned transitions
   * @return transitions, that the given state has and only match one specified event rather than
   *     batch events
   */
  Map<String, IFATransition> getPreciseMatchTransition(IFAState state);

  /**
   * @param state the source state of the returned transitions
   * @return transitions, that the given state has and only match one specified event rather than
   *     batch events
   */
  Iterator<IFATransition> getPreciseMatchTransitionIterator(IFAState state);

  /**
   * @param state state the source state of the returned transitions
   * @return transitions, that the given state has and can match batch events
   */
  List<IFATransition> getFuzzyMatchTransition(IFAState state);

  /**
   * @param sourceState source state
   * @param transition transition that the source state has
   * @return next state
   */
  IFAState getNextState(IFAState sourceState, IFATransition transition);

  /** @return the initial state of this FA */
  IFAState getInitialState();

  /** @return the size of states this FA has */
  int getStateSize();

  /**
   * @param index the index of the target state, used for uniquely identifying states in FA
   * @return the state identified by given index
   */
  IFAState getState(int index);
}
