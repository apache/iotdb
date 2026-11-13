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

package org.apache.iotdb.cli;

import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.cli.utils.CliContext;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CliTest {
  private final CliContext context = mock(CliContext.class);
  private final LineReader reader = mock(LineReader.class);

  @Before
  public void setUp() {
    when(context.getLineReader()).thenReturn(reader);
  }

  @Test
  public void testInterruptedEofRequiresExitConfirmation() {
    when(reader.readLine(anyString(), isNull()))
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              throw new EndOfFileException();
            });
    when(reader.readLine(anyString(), eq('\0')))
        .thenAnswer(
            invocation -> {
              assertFalse(Thread.currentThread().isInterrupted());
              return "";
            });

    try {
      assertFalse(Cli.readerReadLine(context, null));
      verify(reader)
          .readLine(
              CliMessages.EXCEPTION_PRESS_CTRL_C_AGAIN_TO_EXIT_OR_PRESS_ENTER_TO_CONTINUE_8CA49A92,
              '\0');
      verify(context, never()).exit(anyInt());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void testEofExitsWithoutConfirmation() {
    when(reader.readLine(anyString(), isNull())).thenThrow(new EndOfFileException());

    Cli.readerReadLine(context, null);

    verify(context).exit(AbstractCli.CODE_OK);
    verify(reader, never()).readLine(anyString(), eq('\0'));
  }
}
