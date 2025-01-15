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

package org.apache.iotdb.commons.pipe.datastructure.result;

public class Result<T, E> {

  private final T ok;
  private final E err;

  private Result(final T ok, final E err) {
    this.ok = ok;
    this.err = err;
  }

  public static <T, E> Result<T, E> ok(final T value) {
    return new Result<>(value, null);
  }

  public static <T, E> Result<T, E> err(final E error) {
    return new Result<>(null, error);
  }

  public boolean isOk() {
    return ok != null;
  }

  public boolean isErr() {
    return err != null;
  }

  public T getOk() {
    return ok;
  }

  public E getErr() {
    return err;
  }
}
