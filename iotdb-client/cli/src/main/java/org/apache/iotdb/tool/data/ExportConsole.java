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

package org.apache.iotdb.tool.data;

import org.jline.utils.WCWidth;

import java.io.Console;
import java.io.PrintStream;

/** What a person reads while exporting; the export log keeps the machine-readable events. */
final class ExportConsole {
  // Querying the terminal for its size would replace the signal handlers that report the exit
  // status, so the width comes from COLUMNS, which the shell launcher sets, or is taken as 80.
  private static final int DEFAULT_WIDTH = 80;
  private static final String ELLIPSIS = "...";

  private final PrintStream out;
  private final boolean terminal;
  private final int width;
  private int drawn;

  ExportConsole(PrintStream out, boolean terminal) {
    this(out, terminal, columns());
  }

  ExportConsole(PrintStream out, boolean terminal, int width) {
    this.out = out;
    this.terminal = terminal;
    this.width = width;
  }

  static boolean isTerminal() {
    Console console = System.console();
    if (console == null) {
      return false;
    }
    try {
      // From JDK 22 redirected streams also get a console; isTerminal tells them apart.
      return (Boolean) Console.class.getMethod("isTerminal").invoke(console);
    } catch (ReflectiveOperationException e) {
      return true;
    }
  }

  private static int columns() {
    try {
      int columns = Integer.parseInt(System.getenv("COLUMNS"));
      return columns >= 20 ? columns : DEFAULT_WIDTH;
    } catch (NumberFormatException e) {
      return DEFAULT_WIDTH;
    }
  }

  /** Prints a lasting line, first erasing a progress line that would otherwise share its row. */
  void line(String text) {
    erase();
    out.println(text);
    out.flush();
  }

  /**
   * A terminal redraws one line in place, shortening {@code tail} from the left to fit. Redirected
   * output gets {@code plain} as a line of its own, so files and container logs hold no redraws; so
   * does a terminal too narrow for {@code text}, since a carriage return reaches only the last row
   * of a wrapped line and would leave the rest of every redraw behind.
   */
  void progress(String text, String tail, String plain) {
    if (!terminal || width(text) > width - 1) {
      erase();
      out.println(plain);
      out.flush();
      return;
    }
    // The last column stays free: filling it makes some terminals wrap before the next redraw.
    int room = width - 1 - width(text) - 2;
    String shown =
        tail.isEmpty() || room <= ELLIPSIS.length() ? text : text + "  " + left(tail, room);
    int shownWidth = width(shown);
    out.print('\r' + shown + " ".repeat(Math.max(0, drawn - shownWidth)));
    out.flush();
    drawn = shownWidth;
  }

  private void erase() {
    if (drawn > 0) {
      out.print('\r' + " ".repeat(drawn) + '\r');
      drawn = 0;
    }
  }

  /** The end of {@code text} that fits in {@code columns}, marked as shortened when cut. */
  static String left(String text, int columns) {
    if (width(text) <= columns) {
      return text;
    }
    int budget = columns - ELLIPSIS.length();
    int start = text.length();
    int used = 0;
    while (start > 0) {
      int codePoint = text.codePointBefore(start);
      int codePointWidth = Math.max(0, WCWidth.wcwidth(codePoint));
      if (used + codePointWidth > budget) {
        break;
      }
      used += codePointWidth;
      start -= Character.charCount(codePoint);
    }
    // Cut between path nodes, so the shown part is whole nodes such as line19.s0.
    int node = text.indexOf('.', start);
    if (start > 0 && text.charAt(start - 1) != '.' && node >= 0 && node + 1 < text.length()) {
      start = node + 1;
    }
    return ELLIPSIS + text.substring(start);
  }

  /** Display columns: a CJK character takes two. */
  static int width(String text) {
    return text.codePoints().map(c -> Math.max(0, WCWidth.wcwidth(c))).sum();
  }
}
