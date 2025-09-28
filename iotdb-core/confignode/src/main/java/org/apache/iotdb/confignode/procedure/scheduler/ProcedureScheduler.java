1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure.scheduler;
1
1import org.apache.iotdb.confignode.procedure.Procedure;
1
1import java.util.concurrent.TimeUnit;
1
1/** Keep track of the runnable procedures */
1public interface ProcedureScheduler {
1  /** Start the scheduler */
1  void start();
1
1  /** Stop the scheduler */
1  void stop();
1
1  /**
1   * In case the class is blocking on poll() waiting for items to be added, this method should awake
1   * poll() and poll() should return.
1   */
1  void signalAll();
1
1  /**
1   * Inserts the specified element at the front of this queue.
1   *
1   * @param proc the Procedure to add
1   */
1  void addFront(Procedure proc);
1
1  /**
1   * Inserts the specified element at the front of this queue.
1   *
1   * @param proc the Procedure to add
1   * @param notify whether need to notify worker
1   */
1  void addFront(Procedure proc, boolean notify);
1
1  /**
1   * Inserts the specified element at the end of this queue.
1   *
1   * @param proc the Procedure to add
1   */
1  void addBack(Procedure proc);
1
1  /**
1   * Inserts the specified element at the end of this queue.
1   *
1   * @param proc the Procedure to add
1   * @param notify whether need to notify worker
1   */
1  void addBack(Procedure proc, boolean notify);
1
1  /**
1   * The procedure can't run at the moment. add it back to the queue, giving priority to someone
1   * else.
1   *
1   * @param proc the Procedure to add back to the list
1   */
1  void yield(Procedure proc);
1
1  /**
1   * @return true if there are procedures available to process, otherwise false.
1   */
1  boolean hasRunnables();
1
1  /**
1   * Fetch one Procedure from the queue
1   *
1   * @return the Procedure to execute, or null if nothing present.
1   */
1  Procedure poll();
1
1  /**
1   * Fetch one Procedure from the queue
1   *
1   * @param timeout how long to wait before giving up, in units of unit
1   * @param unit a TimeUnit determining how to interpret the timeout parameter
1   * @return the Procedure to execute, or null if nothing present.
1   */
1  Procedure poll(long timeout, TimeUnit unit);
1
1  /**
1   * Returns the number of elements in this queue.
1   *
1   * @return the number of elements in this queue.
1   */
1  int size();
1
1  /**
1   * Clear current state of scheduler such that it is equivalent to newly created scheduler. Used
1   * for testing failure and recovery.
1   */
1  void clear();
1}
1