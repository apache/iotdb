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

import org.apache.iotdb.commons.path.PathPatternNode.StringSerializer;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.tsfile.read.common.TimeRange;

import java.util.HashSet;
import java.util.TreeSet;

public class PatternTreeMapFactory {
  public static PatternTreeMap<String, StringSerializer> getTriggerPatternTreeMap() {
    return new PatternTreeMap<>(
        HashSet::new,
        (triggerName, set) -> set.add(triggerName),
        (triggerName, set) -> set.remove(triggerName),
        StringSerializer.getInstance());
  }

    public static PatternTreeMap<Modification> getModsPatternTreeMap() {
        return new PatternTreeMap<>(
                HashSet::new, (mod, set) -> set.add(mod), (mod, set) -> set.remove(mod));
    }

    public static PatternTreeMap<TimeRange> getModsPatternTreeMap2() {
        return new PatternTreeMap<>(
                TreeSet::new,
                (range, set) -> {
                    TreeSet<TimeRange> treeSet = (TreeSet) set;
                    TimeRange tr = treeSet.floor(range);
                    while (tr != null && tr.intersects(range)) {
                        range.merge(tr);
                        treeSet.remove(tr);
                        tr = treeSet.floor(range);
                    }
                    tr = treeSet.ceiling(range);
                    while (tr != null && tr.intersects(range)) {
                        range.merge(tr);
                        treeSet.remove(tr);
                        tr = treeSet.ceiling(range);
                    }
                    set.add(range);
                },
                null);
    }

    public static PatternTreeMap<TimeRange> getModsPatternTreeMap3() {
        return new PatternTreeMap<>(
                TreeSet::new,
                (range, set) -> {
                    //          TreeSet<TimeRange> treeSet = (TreeSet) set;
                    //          TimeRange tr = treeSet.floor(range);
                    //          while (tr != null && tr.intersects(range)) {
                    //            range.merge(tr);
                    //            treeSet.remove(tr);
                    //            tr = treeSet.floor(range);
                    //          }
                    //          tr = treeSet.ceiling(range);
                    //          while (tr != null && tr.intersects(range)) {
                    //            range.merge(tr);
                    //            treeSet.remove(tr);
                    //            tr = treeSet.ceiling(range);
                    //          }
                    set.add(range);
                },
                null);
    }
}
