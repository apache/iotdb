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

package org.apache.iotdb.confignode.procedure.scheduler;

import org.apache.iotdb.confignode.procedure.Procedure;

import java.util.concurrent.TimeUnit;

/** Keep track of the runnable procedures */
public interface ProcedureScheduler {
  /** Start the scheduler */
  void start();

  /** Stop the scheduler */
  void stop();

  /**
   * In case the class is blocking on poll() waiting for items to be added, this method should awake
   * poll() and poll() should return.
   */
  void signalAll();

  /**
   * Inserts the specified element at the front of this queue.
   *
   * @param proc the Procedure to add
   */
  void addFront(Procedure proc);

  /**
   * Inserts the specified element at the front of this queue.
   *
   * @param proc the Procedure to add
   * @param notify whether need to notify worker
   */
  void addFront(Procedure proc, boolean notify);

  /**
   * Inserts the specified element at the end of this queue.
   *
   * @param proc the Procedure to add
   */
  void addBack(Procedure proc);

  /**
   * Inserts the specified element at the end of this queue.
   *
   * @param proc the Procedure to add
   * @param notify whether need to notify worker
   */
  void addBack(Procedure proc, boolean notify);

  /**
   * The procedure can't run at the moment. add it back to the queue, giving priority to someone
   * else.
   *
   * @param proc the Procedure to add back to the list
   */
  void yield(Procedure proc);

  /** @return true if there are procedures available to process, otherwise false. */
  boolean hasRunnables();

  /**
   * Fetch one Procedure from the queue
   *
   * @return the Procedure to execute, or null if nothing present.
   */
  Procedure poll();

  /**
   * Fetch one Procedure from the queue
   *
   * @param timeout how long to wait before giving up, in units of unit
   * @param unit a TimeUnit determining how to interpret the timeout parameter
   * @return the Procedure to execute, or null if nothing present.
   */
  Procedure poll(long timeout, TimeUnit unit);

  /**
   * Returns the number of elements in this queue.
   *
   * @return the number of elements in this queue.
   */
  int size();

  /**
   * Clear current state of scheduler such that it is equivalent to newly created scheduler. Used
   * for testing failure and recovery.
   */
  void clear();
}
