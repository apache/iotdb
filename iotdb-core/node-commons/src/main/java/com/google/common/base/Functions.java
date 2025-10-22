/*
 * Copyright (C) 2007 The Guava Authors
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

package com.google.common.base;

import javax.annotation.CheckForNull;

/**
 * Static utility methods pertaining to {@code com.google.common.base.Function} instances; see that
 * class for information about migrating to {@code java.util.function}.
 *
 * <p>All methods return serializable functions as long as they're given serializable parameters.
 *
 * <p>See the Guava User Guide article on <a
 * href="https://github.com/google/guava/wiki/FunctionalExplained">the use of {@code Function}</a>.
 *
 * @author Mike Bostock
 * @author Jared Levy
 * @since 2.0
 */
public final class Functions {
  private Functions() {}

  /**
   * Returns the identity function.
   *
   * <p><b>Discouraged:</b> Prefer using a lambda like {@code v -> v}, which is shorter and often
   * more readable.
   */
  // implementation is "fully variant"; E has become a "pass-through" type
  @SuppressWarnings("unchecked")
  public static <E extends Object> Function<E, E> identity() {
    return (Function<E, E>) IdentityFunction.INSTANCE;
  }

  // enum singleton pattern
  private enum IdentityFunction implements Function<Object, Object> {
    INSTANCE;

    @Override
    @CheckForNull
    public Object apply(@CheckForNull Object o) {
      return o;
    }

    @Override
    public String toString() {
      return "Functions.identity()";
    }
  }
}
