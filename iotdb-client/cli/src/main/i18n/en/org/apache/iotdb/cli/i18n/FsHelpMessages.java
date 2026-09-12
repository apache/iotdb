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

package org.apache.iotdb.cli.i18n;

public final class FsHelpMessages {
  private FsHelpMessages() {}

  public static final String GENERAL =
      "Use help <command>, <command> -h or <command> --help for details.\n"
          + "Quote paths and patterns containing spaces. Use -- before operands beginning with -.\n"
          + "Virtual paths address remote database objects; local write/export/sketch paths use the"
          + " process directory.\n"
          + "Pipelines use |; < reads local input; > and >> redirect to local files.\n"
          + "Writes require --fs_write_mode enabled. Output goes to stdout; errors go to stderr.\n"
          + "Status: 0 success, 1 usage error, 2 input error, 3 runtime error; grep uses 0 match, 1"
          + " no match, 2 error.";
  public static final String READ_OPTIONS =
      "-m selects FIELD columns while retaining time and TAG columns. -d selects a tree device; -t"
          + " selects a table.\n"
          + "-n limits data rows; --offset skips matched rows. --start/--end are inclusive"
          + " timestamps.\n"
          + "TAG operators: eq, neq, regexp, is-null, not-null. Multiple filters require"
          + " --tag-match all or any.\n"
          + "CSV null is unquoted \\N; literal \\N is quoted. NDJSON preserves schema types and"
          + " quotes INT64/TIMESTAMP values.";
  private static final String CURRENT =
      "Current virtual directory; success is silent unless output is described.";
  private static final String INPUT = "All input is processed; an omitted path or - reads stdin.";
  private static final String WRITE = "Paths are required; requires --fs_write_mode enabled.";
  public static final String[] PWD = {
    "Print the absolute virtual working directory.", "No options."
  };
  public static final String[] LS = {
    "List virtual entries; -a includes . and .., -R recurses, -l prints available attributes."
        + " Unavailable Unix attributes use -.",
    CURRENT
  };
  public static final String[] LL = {"Long listing, equivalent to ls -l.", CURRENT};
  public static final String[] CD = {
    "Change virtual directory; cd - returns to the previous directory.",
    "No path or ~ selects virtual home /."
  };
  public static final String[] STAT = {
    "Print the virtual object type, readable byte count and available database metadata; no inode"
        + " or permission claims.",
    CURRENT
  };
  public static final String[] FILE = {
    "Print the virtual object type and available content description.", CURRENT
  };
  public static final String[] SCHEMA = {
    "Print model/object/column/category/data_type/encoding/compression. Unavailable physical"
        + " properties are NULL.",
    "Current scope; all columns. Formats: table, ndjson, csv."
  };
  public static final String[] META = {
    "Print available database object metadata; this describes the remote object, not a local"
        + " TsFile.",
    CURRENT
  };
  public static final String[] STATS = {
    "Print typed FIELD statistics per device, with TAG values, real null counts and non-null time"
        + " ranges; stats_source is scan.",
    "Current scope; all FIELD columns. BOOLEAN sum counts true; INT64/TIMESTAMP/DATE sum is NULL."
  };
  public static final String[] COUNT = {
    "Print row/entity/non-null/null counts and time ranges for TAG and FIELD columns. TIME and"
        + " ATTRIBUTE are excluded.",
    "Current scope; all TAG/FIELD columns. Tree entity_count is NULL."
  };
  public static final String[] CAT = {
    "Print selected typed data rows, or unchanged .meta text.",
    "Current scope; all matching rows. Default format is table; CSV includes a schema header."
  };
  public static final String[] HEAD = {
    "Print the first selected data rows, or the first input text lines.",
    "Current scope; 10 data rows. The data header is additional to the row limit."
  };
  public static final String[] TAIL = {
    "Print the last lines or bytes; +N starts at position N, -f follows appended content. Data"
        + " format uses --format.",
    "stdin when no path is given; 10 lines. Virtual data is serialized as CSV text."
  };
  public static final String[] WC = {
    "Count UTF-8 bytes in readable content; multiple inputs also print a total. Only -c is"
        + " supported.",
    INPUT
  };
  public static final String[] GREP = {
    "Match basic regular expressions; -E uses extended expressions, -F literal text. Supports case"
        + " folding, inversion and line numbers.",
    "All input; stdin if no path. Status is 0 for matches, 1 for no matches, 2 for errors."
  };
  public static final String[] FIND = {
    "Recursively print paths matching shell-style -name patterns, optional object type and depth"
        + " limits.",
    "Current directory, all names and unlimited depth."
  };
  public static final String[] PAGING = {
    "Use interactive paging and search on a terminal; print all content in batch mode.", CURRENT
  };
  public static final String[] MKDIR = {
    "Create table-model databases; -p accepts existing parents. Unix permission mode -m is"
        + " unsupported for virtual objects.",
    WRITE
  };
  public static final String[] RMDIR = {
    "Remove empty databases; non-empty databases are rejected.", WRITE
  };
  public static final String[] RM = {
    "Remove tables; -r removes databases and their contents. -f ignores missing targets; -i"
        + " confirms removal.",
    WRITE
  };
  public static final String[] MV = {
    "Move tables, including across databases; multiple sources require a destination database. -n"
        + " skips existing targets and -i confirms replacement.",
    WRITE
  };
  public static final String[] CP = {
    "Copy table schema and data, including across databases; multiple sources require a destination"
        + " database. -n skips existing targets and -i confirms replacement.",
    WRITE
  };
  public static final String[] CUT = {
    "Select fields, bytes or characters; preserve empty fields. Lists accept N, N-M, N- and -M; -s"
        + " suppresses lines without the field delimiter.",
    INPUT + " Field delimiter defaults to TAB."
  };
  public static final String[] PASTE = {
    "Merge corresponding lines; -s merges each input serially, -d supplies cyclic delimiters.",
    INPUT + " The delimiter defaults to TAB."
  };
  public static final String[] JOIN = {
    "Join sorted inputs by key; -a includes unmatched lines, -v prints only unmatched lines, -e"
        + " replaces empty fields and -o selects output fields.",
    "Two inputs are required; - reads stdin. The key defaults to field 1 and whitespace separates"
        + " fields."
  };
  public static final String[] TEE = {
    "Read stdin to EOF, echo it to stdout and write CSV data to target tables. -a appends;"
        + " otherwise existing table data is replaced.",
    "Without targets, only echo stdin. Writing targets requires --fs_write_mode enabled."
  };
  public static final String[] EXPORT = {
    "Export selected remote devices or tables to local files using typed data formatting.",
    "--type and object selection are required. Use -o for one object or a new --output-dir with a"
        + " completion manifest; --force only applies to -o."
  };
  public static final String[] SKETCH = {
    "Inspect a local TsFile and print its path and model between sketch markers.",
    "Local input is required. Default output is stdout; -o writes a new file and --force permits"
        + " replacement except the input itself."
  };
  public static final String[] TREE = {
    "Print the virtual directory tree.",
    "Current directory; unlimited depth. -L 0 prints no descendants."
  };
  public static final String[] SQL = {
    "Execute a SQL statement and print any result rows.",
    "The statement keeps its original quoting. Statements beyond recognized read-only queries"
        + " require --fs_write_mode enabled."
  };
  public static final String[] HELP = {
    "Print general or per-command help; -h and --help are equivalent.",
    "General help when no command is given."
  };
  public static final String[] EXIT = {
    "Leave filesystem mode with the requested exit status; quit is an alias.",
    "Status is 0; supplied status is reduced to its low 8 bits."
  };
}
