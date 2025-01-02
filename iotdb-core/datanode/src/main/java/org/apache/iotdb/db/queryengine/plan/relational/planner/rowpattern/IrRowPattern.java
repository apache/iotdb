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

package org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class IrRowPattern {
  protected <R, C> R accept(IrRowPatternVisitor<R, C> visitor, C context) {
    return visitor.visitIrRowPattern(this, context);
  }

  public static void serialize(IrRowPattern pattern, ByteBuffer byteBuffer) {
    if (pattern instanceof IrAlternation) {
      ReadWriteIOUtils.write(0, byteBuffer); // Type marker for IrAlternation
      IrAlternation.serialize((IrAlternation) pattern, byteBuffer);
    } else if (pattern instanceof IrAnchor) {
      ReadWriteIOUtils.write(1, byteBuffer); // Type marker for IrAnchor
      IrAnchor.serialize((IrAnchor) pattern, byteBuffer);
    } else if (pattern instanceof IrConcatenation) {
      ReadWriteIOUtils.write(2, byteBuffer); // Type marker for IrConcatenation
      IrConcatenation.serialize((IrConcatenation) pattern, byteBuffer);
    } else if (pattern instanceof IrEmpty) {
      ReadWriteIOUtils.write(3, byteBuffer); // Type marker for IrEmpty
      IrEmpty.serialize((IrEmpty) pattern, byteBuffer);
    } else if (pattern instanceof IrExclusion) {
      ReadWriteIOUtils.write(4, byteBuffer); // Type marker for IrExclusion
      IrExclusion.serialize((IrExclusion) pattern, byteBuffer);
    } else if (pattern instanceof IrLabel) {
      ReadWriteIOUtils.write(5, byteBuffer); // Type marker for IrLabel
      IrLabel.serialize((IrLabel) pattern, byteBuffer);
    } else if (pattern instanceof IrPermutation) {
      ReadWriteIOUtils.write(6, byteBuffer); // Type marker for IrPermutation
      IrPermutation.serialize((IrPermutation) pattern, byteBuffer);
    } else if (pattern instanceof IrQuantified) {
      ReadWriteIOUtils.write(7, byteBuffer); // Type marker for IrQuantified
      IrQuantified.serialize((IrQuantified) pattern, byteBuffer);
    } else {
      throw new IllegalArgumentException("Unknown IrRowPattern type");
    }
  }

  public static void serialize(IrRowPattern pattern, DataOutputStream stream) throws IOException {
    if (pattern instanceof IrAlternation) {
      ReadWriteIOUtils.write(0, stream); // Type marker for IrAlternation
      IrAlternation.serialize((IrAlternation) pattern, stream);
    } else if (pattern instanceof IrAnchor) {
      ReadWriteIOUtils.write(1, stream); // Type marker for IrAnchor
      IrAnchor.serialize((IrAnchor) pattern, stream);
    } else if (pattern instanceof IrConcatenation) {
      ReadWriteIOUtils.write(2, stream); // Type marker for IrConcatenation
      IrConcatenation.serialize((IrConcatenation) pattern, stream);
    } else if (pattern instanceof IrEmpty) {
      ReadWriteIOUtils.write(3, stream); // Type marker for IrEmpty
      IrEmpty.serialize((IrEmpty) pattern, stream);
    } else if (pattern instanceof IrExclusion) {
      ReadWriteIOUtils.write(4, stream); // Type marker for IrExclusion
      IrExclusion.serialize((IrExclusion) pattern, stream);
    } else if (pattern instanceof IrLabel) {
      ReadWriteIOUtils.write(5, stream); // Type marker for IrLabel
      IrLabel.serialize((IrLabel) pattern, stream);
    } else if (pattern instanceof IrPermutation) {
      ReadWriteIOUtils.write(6, stream); // Type marker for IrPermutation
      IrPermutation.serialize((IrPermutation) pattern, stream);
    } else if (pattern instanceof IrQuantified) {
      ReadWriteIOUtils.write(7, stream); // Type marker for IrQuantified
      IrQuantified.serialize((IrQuantified) pattern, stream);
    } else {
      throw new IllegalArgumentException("Unknown IrRowPattern type");
    }
  }

  public static IrRowPattern deserialize(ByteBuffer byteBuffer) {
    int type = ReadWriteIOUtils.readInt(byteBuffer);

    switch (type) {
      case 0:
        return IrAlternation.deserialize(byteBuffer);
      case 1:
        return IrAnchor.deserialize(byteBuffer);
      case 2:
        return IrConcatenation.deserialize(byteBuffer);
      case 3:
        return IrEmpty.deserialize(byteBuffer);
      case 4:
        return IrExclusion.deserialize(byteBuffer);
      case 5:
        return IrLabel.deserialize(byteBuffer);
      case 6:
        return IrPermutation.deserialize(byteBuffer);
      case 7:
        return IrQuantified.deserialize(byteBuffer);
      default:
        throw new IllegalArgumentException("Unknown IrRowPattern type");
    }
  }
}
