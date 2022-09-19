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

import org.apache.iotdb.commons.path.PathPatternNode;
import org.apache.iotdb.commons.path.PathPatternNode.StringSerializer;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
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

  public static PatternTreeMap<Modification, ModsSerializer> getModsPatternTreeMap() {
    return new PatternTreeMap<>(
        HashSet::new,
        (mod, set) -> set.add(mod),
        (mod, set) -> set.remove(mod),
        ModsSerializer.getInstance());
  }

  public static PatternTreeMap<Modification, ModsSerializer> getModsPatternTreeMap1() {
    return new PatternTreeMap<>(
        () ->
            new TreeSet<>(
                new Comparator<Modification>() {
                  @Override
                  public int compare(Modification o1, Modification o2) {
                    if (!o1.getType().equals(o2.getType())) {
                      return o1.getType().compareTo(o2.getType());
                    } else if (!o1.getPath().equals(o2.getPath())) {
                      return o1.getPath().compareTo(o2.getPath());
                    } else if (o1.getFileOffset() != o2.getFileOffset()) {
                      return (int) (o1.getFileOffset() - o2.getFileOffset());
                    } else {
                      switch (o1.getType()) {
                        case DELETION:
                          Deletion del1 = (Deletion) o1;
                          Deletion del2 = (Deletion) o2;
                          return del1.getTimeRange().compareTo(del2.getTimeRange());
                        default:
                          throw new IllegalArgumentException();
                      }
                    }
                  }
                }),
        (mod, set) -> {
          TreeSet<Modification> treeSet = (TreeSet) set;
          Deletion modToAdd = (Deletion) mod;
          Deletion modInSet = (Deletion) treeSet.floor(mod);
          while (modInSet != null && modInSet.intersects(modToAdd)) {
            treeSet.remove(modInSet);
            modToAdd.merge(modInSet);
            modInSet = (Deletion) treeSet.floor(mod);
          }
          modInSet = (Deletion) treeSet.ceiling(mod);
          while (modInSet != null && modInSet.intersects(modToAdd)) {
            treeSet.remove(modInSet);
            modToAdd.merge(modInSet);
            modInSet = (Deletion) treeSet.ceiling(mod);
          }
          set.add(modToAdd);
        },
        null,
        ModsSerializer.getInstance());
  }

  public static PatternTreeMap<TimeRange, TimeRangeSerializer> getModsPatternTreeMap2() {
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
        null,
        TimeRangeSerializer.getInstance());
  }

  //    public static PatternTreeMap<TimeRange> getModsPatternTreeMap3() {
  //        return new PatternTreeMap<>(
  //                TreeSet::new,
  //                (range, set) -> {
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
  //                    set.add(range);
  //                },
  //                null);
  //    }

  public static class ModsSerializer implements PathPatternNode.Serializer<Modification> {

    @Override
    public void write(Modification modification, ByteBuffer buffer) {}

    @Override
    public void write(Modification modification, PublicBAOS buffer) throws IOException {}

    @Override
    public void write(Modification modification, DataOutputStream buffer) throws IOException {}

    @Override
    public Modification read(ByteBuffer buffer) {
      return null;
    }

    private static class ModsSerializerHolder {
      private static final ModsSerializer INSTANCE = new ModsSerializer();

      private ModsSerializerHolder() {}
    }

    public static ModsSerializer getInstance() {
      return ModsSerializer.ModsSerializerHolder.INSTANCE;
    }
  }

  public static class TimeRangeSerializer implements PathPatternNode.Serializer<TimeRange> {

    @Override
    public void write(TimeRange timeRange, ByteBuffer buffer) {}

    @Override
    public void write(TimeRange timeRange, PublicBAOS buffer) throws IOException {}

    @Override
    public void write(TimeRange timeRange, DataOutputStream buffer) throws IOException {}

    @Override
    public TimeRange read(ByteBuffer buffer) {
      return null;
    }

    private static class TimeRangeSerializerHolder {
      private static final TimeRangeSerializer INSTANCE = new TimeRangeSerializer();

      private TimeRangeSerializerHolder() {}
    }

    public static TimeRangeSerializer getInstance() {
      return TimeRangeSerializerHolder.INSTANCE;
    }
  }
}
