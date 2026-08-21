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

package org.apache.iotdb.google.common.io;

import org.apache.iotdb.google.common.annotations.GwtIncompatible;
import org.apache.iotdb.google.common.annotations.J2ktIncompatible;

import java.io.DataInput;
import java.io.IOException;

/**
 * An extension of {@code DataInput} for reading from in-memory byte arrays; its methods offer
 * identical functionality but do not throw {@link IOException}.
 *
 * <p><b>Warning:</b> The caller is responsible for not attempting to read past the end of the
 * array. If any method encounters the end of the array prematurely, it throws {@link
 * IllegalStateException} to signify <i>programmer error</i>. This behavior is a technical violation
 * of the supertype's contract, which specifies a checked exception.
 *
 * @author Kevin Bourrillion
 * @since 1.0
 */
@J2ktIncompatible
@GwtIncompatible
@ElementTypesAreNonnullByDefault
public interface ByteArrayDataInput extends DataInput {
  @Override
  void readFully(byte b[]);

  @Override
  void readFully(byte b[], int off, int len);

  // not guaranteed to skip n bytes so result should NOT be ignored
  // use ByteStreams.skipFully or one of the read methods instead
  @Override
  int skipBytes(int n);

  // to skip a byte
  @Override
  boolean readBoolean();

  // to skip a byte
  @Override
  byte readByte();

  // to skip a byte
  @Override
  int readUnsignedByte();

  // to skip some bytes
  @Override
  short readShort();

  // to skip some bytes
  @Override
  int readUnsignedShort();

  // to skip some bytes
  @Override
  char readChar();

  // to skip some bytes
  @Override
  int readInt();

  // to skip some bytes
  @Override
  long readLong();

  // to skip some bytes
  @Override
  float readFloat();

  // to skip some bytes
  @Override
  double readDouble();

  // to skip a line
  @Override
  String readLine();

  // to skip a field
  @Override
  String readUTF();
}
