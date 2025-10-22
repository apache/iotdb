/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.collect;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Static utility methods pertaining to {@link Queue} and {@link Deque} instances. Also see this
 * class's counterparts {@link Lists}, {@link Sets}, and {@link Maps}.
 *
 * @author Kurt Alfred Kluever
 * @since 11.0
 */
public final class Queues {
  private Queues() {}

  // ArrayDeque

  /**
   * Creates an empty {@code ArrayDeque}.
   *
   * @since 12.0
   */
  public static <E> ArrayDeque<E> newArrayDeque() {
    return new ArrayDeque<E>();
  }

  // ConcurrentLinkedQueue

  /** Creates an empty {@code ConcurrentLinkedQueue}. */
  public static <E> ConcurrentLinkedQueue<E> newConcurrentLinkedQueue() {
    return new ConcurrentLinkedQueue<E>();
  }

  // LinkedBlockingQueue

  /** Creates an empty {@code LinkedBlockingQueue} with a capacity of {@link Integer#MAX_VALUE}. */
  public static <E> LinkedBlockingQueue<E> newLinkedBlockingQueue() {
    return new LinkedBlockingQueue<E>();
  }
}
