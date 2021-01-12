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

package org.apache.iotdb.db.query.udf.core.input;

import java.util.ArrayList;
import java.util.List;

public class SafetyLine {

  public static final int INITIAL_PILE_POSITION = -1;

  private final List<Integer> safetyPiles;

  public SafetyLine() {
    safetyPiles = new ArrayList<>();
  }

  /**
   * @return the index of the first element that cannot be evicted. in other words, elements whose
   * index are <b>less than</b> the return value can be evicted.
   */
  public int getSafetyLine() {
    final int size = safetyPiles.size();
    int min = safetyPiles.get(0);
    for (int i = 1; i < size; ++i) {
      min = Math.min(safetyPiles.get(i), min);
    }
    return min;
  }

  public SafetyPile addSafetyPile() {
    safetyPiles.add(INITIAL_PILE_POSITION);
    return new SafetyPile(safetyPiles.size() - 1);
  }

  public class SafetyPile {

    private final int safetyPileIndex;

    public SafetyPile(int safetyPileIndex) {
      this.safetyPileIndex = safetyPileIndex;
    }

    /**
     * @param safetyPilePosition the index of the first element that cannot be evicted. in other
     *                           words, elements whose index are <b>less than</b> the
     *                           safetyPilePosition can be evicted.
     */
    public void moveForwardTo(int safetyPilePosition) {
      safetyPiles.set(safetyPileIndex, safetyPilePosition);
    }
  }
}
