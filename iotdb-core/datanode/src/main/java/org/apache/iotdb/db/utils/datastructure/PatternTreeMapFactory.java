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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PathPatternNode;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;

public class PatternTreeMapFactory {

  private PatternTreeMapFactory() {
    // not allowed construction
  }

  public static PatternTreeMap<String, StringSerializer> getTriggerPatternTreeMap() {
    return new PatternTreeMap<>(
        HashSet::new,
        (triggerName, set) -> set.add(triggerName),
        (triggerName, set) -> set.remove(triggerName),
        StringSerializer.getInstance());
  }

  /**
   * This PatternTreeMap is used to manage Modification. The append will merge of Modification that
   * intersect.
   */
  public static PatternTreeMap<Modification, ModsSerializer> getModsPatternTreeMap() {
    return new PatternTreeMap<>(
        HashSet::new, (mod, set) -> set.add(mod), null, ModsSerializer.getInstance());
  }

  public static class ModsSerializer implements PathPatternNode.Serializer<Modification> {

    @Override
    public void write(Modification modification, ByteBuffer buffer) {
      ReadWriteIOUtils.write(modification.getType().ordinal(), buffer);
      modification.getPath().serialize(buffer);
      ReadWriteIOUtils.write(modification.getFileOffset(), buffer);
      switch (modification.getType()) {
        case DELETION:
          ReadWriteIOUtils.write(((Deletion) modification).getStartTime(), buffer);
          ReadWriteIOUtils.write(((Deletion) modification).getEndTime(), buffer);
          break;
        default:
          throw new IllegalArgumentException();
      }
    }

    @Override
    public void write(Modification modification, PublicBAOS stream) throws IOException {
      ReadWriteIOUtils.write(modification.getType().ordinal(), stream);
      modification.getPath().serialize(stream);
      ReadWriteIOUtils.write(modification.getFileOffset(), stream);
      switch (modification.getType()) {
        case DELETION:
          ReadWriteIOUtils.write(((Deletion) modification).getStartTime(), stream);
          ReadWriteIOUtils.write(((Deletion) modification).getEndTime(), stream);
          break;
        default:
          throw new IllegalArgumentException();
      }
    }

    @Override
    public void write(Modification modification, DataOutputStream stream) throws IOException {
      ReadWriteIOUtils.write(modification.getType().ordinal(), stream);
      modification.getPath().serialize(stream);
      ReadWriteIOUtils.write(modification.getFileOffset(), stream);
      switch (modification.getType()) {
        case DELETION:
          ReadWriteIOUtils.write(((Deletion) modification).getStartTime(), stream);
          ReadWriteIOUtils.write(((Deletion) modification).getEndTime(), stream);
          break;
        default:
          throw new IllegalArgumentException();
      }
    }

    @Override
    public Modification read(ByteBuffer buffer) {
      int type = ReadWriteIOUtils.read(buffer);
      MeasurementPath partialPath = MeasurementPath.deserialize(buffer);
      long fileOffset = ReadWriteIOUtils.readLong(buffer);
      switch (Modification.Type.values()[type]) {
        case DELETION:
          long startTime = ReadWriteIOUtils.readLong(buffer);
          long endTime = ReadWriteIOUtils.readLong(buffer);
          return new Deletion(partialPath, fileOffset, startTime, endTime);
        default:
          throw new IllegalArgumentException();
      }
    }

    private static class ModsSerializerHolder {
      private static final ModsSerializer INSTANCE = new ModsSerializer();

      private ModsSerializerHolder() {}
    }

    public static ModsSerializer getInstance() {
      return ModsSerializer.ModsSerializerHolder.INSTANCE;
    }
  }

  public static class StringSerializer implements PathPatternNode.Serializer<String> {

    private static class StringSerializerHolder {
      private static final StringSerializer INSTANCE = new StringSerializer();

      private StringSerializerHolder() {}
    }

    public static StringSerializer getInstance() {
      return StringSerializerHolder.INSTANCE;
    }

    private StringSerializer() {}

    @Override
    public void write(String s, ByteBuffer buffer) {
      ReadWriteIOUtils.write(s, buffer);
    }

    @Override
    public void write(String s, PublicBAOS stream) throws IOException {
      ReadWriteIOUtils.write(s, stream);
    }

    @Override
    public void write(String s, DataOutputStream stream) throws IOException {
      ReadWriteIOUtils.write(s, stream);
    }

    @Override
    public String read(ByteBuffer buffer) {
      return ReadWriteIOUtils.readString(buffer);
    }
  }
}
