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

package org.apache.iotdb.db.schemaengine.schemaregion.write.resp;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;

import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Unit tests for {@link ConstructSchemaBlackListResult} binary serialization. */
public class ConstructSchemaBlackListResultTest {

  @Test
  public void testSerializeDeserialize_emptyLists() {
    ConstructSchemaBlackListResult original =
        new ConstructSchemaBlackListResult(7L, true, false, true, null, null);
    ByteBuffer buf = original.serialize();
    ConstructSchemaBlackListResult restored = ConstructSchemaBlackListResult.deserialize(buf);
    Assert.assertEquals(original, restored);
  }

  @Test
  public void testSerializeDeserialize_pairConstructor() {
    ConstructSchemaBlackListResult original =
        new ConstructSchemaBlackListResult(new Pair<>(42L, false));
    ByteBuffer buf = original.serialize();
    ConstructSchemaBlackListResult restored = ConstructSchemaBlackListResult.deserialize(buf);
    Assert.assertEquals(original, restored);
  }

  @Test
  public void testSerializeDeserialize_withPaths() throws IllegalPathException {
    PartialPath p1 = new PartialPath("root.sg.d1.s1");
    PartialPath p2 = new PartialPath("root.sg.d1.s2_physical");
    PartialPath pre1 = new PartialPath("root.sg.d1.s3");

    List<ConstructSchemaBlackListResult.ReferencedInvalidPathInfo> refs = new ArrayList<>();
    refs.add(new ConstructSchemaBlackListResult.ReferencedInvalidPathInfo(p1, null, false));
    refs.add(new ConstructSchemaBlackListResult.ReferencedInvalidPathInfo(p2, p1, true));

    List<PartialPath> preDeleted = new ArrayList<>();
    preDeleted.add(pre1);

    ConstructSchemaBlackListResult original =
        new ConstructSchemaBlackListResult(100L, false, true, true, refs, preDeleted);

    ByteBuffer buf = original.serialize();
    ConstructSchemaBlackListResult restored = ConstructSchemaBlackListResult.deserialize(buf);
    Assert.assertEquals(original, restored);
  }

  @Test
  public void testSerializeIntoPreallocatedBuffer() throws IllegalPathException {
    ConstructSchemaBlackListResult original =
        new ConstructSchemaBlackListResult(
            1L,
            true,
            true,
            false,
            Collections.singletonList(
                new ConstructSchemaBlackListResult.ReferencedInvalidPathInfo(
                    new PartialPath("root.a.b"), new PartialPath("root.a.c"), false)),
            Collections.singletonList(new PartialPath("root.x.y")));

    int capacity = 16 * 1024;
    ByteBuffer buf = ByteBuffer.allocate(capacity);
    original.serialize(buf);
    buf.flip();
    ConstructSchemaBlackListResult restored = ConstructSchemaBlackListResult.deserialize(buf);
    Assert.assertEquals(original, restored);
  }
}
