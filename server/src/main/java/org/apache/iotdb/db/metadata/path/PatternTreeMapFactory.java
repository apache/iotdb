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
package org.apache.iotdb.db.metadata.path;

import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.engine.modification.Modification;

import java.util.HashSet;

public class PatternTreeMapFactory {
  public static PatternTreeMap<String> getTriggerPatternTreeMap() {
    return new PatternTreeMap<>(
        HashSet::new,
        (triggerName, set) -> set.add(triggerName),
        (triggerName, set) -> set.remove(triggerName));
  }

  public static PatternTreeMap<Modification> getModsPatternTreeMap() {
    return new PatternTreeMap<>(
        HashSet::new,
        (mod, set) -> set.add(mod),
        (mod, set) -> set.remove(mod));
  }
}
