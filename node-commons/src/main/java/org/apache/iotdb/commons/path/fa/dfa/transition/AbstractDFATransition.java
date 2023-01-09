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
package org.apache.iotdb.commons.path.fa.dfa.transition;

import org.apache.iotdb.commons.path.fa.IFATransition;

import java.util.Objects;

abstract class AbstractDFATransition implements IFATransition {
  /**
   * A transition does not change after it has been created externally, the id is used as a unique
   * identifier. It is necessary to ensure that the other properties of a transition with a
   * different id are different.
   */
  private final int id; // only used for hash

  protected AbstractDFATransition(int index) {
    this.id = index;
  }

  @Override
  public int getIndex() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AbstractDFATransition that = (AbstractDFATransition) o;
    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
