/**
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
#ifndef IOTDB_OPTIONAL_H
#define IOTDB_OPTIONAL_H

#include <utility>

template <typename T> class Optional {
public:
  Optional() : hasValue_(false) {}

  Optional(const T &value) : hasValue_(true), value_(value) {}

  Optional(T &&value) : hasValue_(true), value_(std::move(value)) {}

  static Optional<T> of(const T &value) { return Optional<T>(value); }

  static Optional<T> of(T &&value) { return Optional<T>(std::move(value)); }

  static Optional<T> none() { return Optional<T>(); }

  bool is_initialized() const { return hasValue_; }

  bool has_value() const { return hasValue_; }

  const T &value() const { return value_; }

  T &value() { return value_; }

  const T &get() const { return value_; }

  T &get() { return value_; }

  explicit operator bool() const { return hasValue_; }

private:
  bool hasValue_;
  T value_{};
};

#endif
