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

package org.apache.iotdb.postgresql.sql;

import org.apache.iotdb.postgresql.catalog.StaticCatalog;
import org.apache.iotdb.postgresql.protocol.PgException;
import org.apache.iotdb.postgresql.protocol.SqlState;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Translates a PostgreSQL query into table-model SQL: names, casts, parameters, catalog functions
 * and the constructs with identical meaning. Anything else is refused with 0A000 naming the
 * construct; the translator never evaluates data itself.
 */
public final class PgToTableSql {

  private static final Logger LOGGER = LoggerFactory.getLogger(PgToTableSql.class);

  /** The translated statement. */
  public static final class Translation {
    private final String sql;
    private final List<Integer> parameterOrder;
    private final boolean[] dateParameters;

    Translation(String sql, List<Integer> parameterOrder) {
      this(sql, parameterOrder, null);
    }

    private Translation(String sql, List<Integer> parameterOrder, boolean[] dateParameters) {
      this.sql = sql;
      this.parameterOrder = Collections.unmodifiableList(parameterOrder);
      this.dateParameters = dateParameters;
    }

    /** The table-model SQL. */
    public String sql() {
      return sql;
    }

    /**
     * For each {@code ?} of {@link #sql()} in order, the 1-based number of the PostgreSQL parameter
     * ({@code $n}) it stands for; a parameter may appear several times.
     */
    public List<Integer> parameterOrder() {
      return parameterOrder;
    }

    /**
     * The statement with the parameters the client declared as {@code date} marked: the engine has
     * no date literal and cannot bind a parameter inside a CAST, so their values are written into
     * the SQL as {@code CAST('yyyy-mm-dd' AS DATE)} when bound ({@link #sqlFor}).
     *
     * @param types the parameter type OIDs from Parse (index = PostgreSQL parameter number - 1)
     */
    public Translation withTypedParameters(int[] types) {
      boolean[] dates = new boolean[parameterOrder.size()];
      boolean any = false;
      for (int k = 0; k < dates.length; k++) {
        int number = parameterOrder.get(k);
        dates[k] = number - 1 < types.length && types[number - 1] == 1082;
        any |= dates[k];
      }
      return any ? new Translation(sql, parameterOrder, dates) : this;
    }

    /** The SQL to run with {@code values} (one per {@code ?}, in order). */
    public String sqlFor(List<? extends Object> values) {
      if (dateParameters == null) {
        return sql;
      }
      StringBuilder out = new StringBuilder(sql.length());
      int k = 0;
      char quote = 0;
      for (int i = 0; i < sql.length(); i++) {
        char c = sql.charAt(i);
        if (quote != 0) {
          out.append(c);
          if (c == quote) {
            quote = 0;
          }
          continue;
        }
        if (c == '\'' || c == '"') {
          quote = c;
          out.append(c);
          continue;
        }
        if (c == '?' && k < dateParameters.length) {
          if (dateParameters[k]) {
            Object value = values.get(k);
            String text =
                value
                        instanceof
                        org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral
                    ? ((org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral)
                            value)
                        .getValue()
                    : null;
            // the value was parsed as a date by the parameter decoder: digits and dashes only
            out.append(
                text == null
                    ? "CAST(NULL AS DATE)"
                    : "CAST("
                        + Identifiers.literal(java.time.LocalDate.parse(text).toString())
                        + " AS DATE)");
          } else {
            out.append('?');
          }
          k++;
          continue;
        }
        out.append(c);
      }
      return out.toString();
    }

    /** The values still bound as parameters (those not written into {@link #sqlFor}). */
    public <T> List<T> remaining(List<T> values) {
      if (dateParameters == null) {
        return values;
      }
      List<T> kept = new ArrayList<>();
      for (int k = 0; k < values.size(); k++) {
        if (!dateParameters[k]) {
          kept.add(values.get(k));
        }
      }
      return kept;
    }

    /** The highest PostgreSQL parameter number used, 0 if none. */
    public int parameterCount() {
      int max = 0;
      for (int n : parameterOrder) {
        max = Math.max(max, n);
      }
      return max;
    }
  }

  /** Functions with the same name, arguments and meaning in both dialects. */
  private static final Set<String> SAME =
      new HashSet<>(
          Arrays.asList(
              "count",
              "sum",
              "avg",
              "min",
              "max",
              "stddev",
              "stddev_pop",
              "stddev_samp",
              "variance",
              "var_pop",
              "var_samp",
              "corr",
              "covar_pop",
              "covar_samp",
              "regr_slope",
              "regr_intercept",
              "lower",
              "upper",
              "length",
              "trim",
              "ltrim",
              "rtrim",
              "replace",
              "substring",
              "strpos",
              "starts_with",
              "abs",
              "sign",
              "ceil",
              "floor",
              "round",
              "sqrt",
              "exp",
              "ln",
              "log10",
              "sin",
              "cos",
              "tan",
              "asin",
              "acos",
              "atan",
              "degrees",
              "radians",
              "pi",
              "greatest",
              "least",
              "coalesce"));

  /** Functions with another name in the table model. */
  private static final Map<String, String> RENAMED = new HashMap<>();

  static {
    RENAMED.put("char_length", "length");
    RENAMED.put("character_length", "length");
    RENAMED.put("ceiling", "ceil");
    RENAMED.put("substr", "substring");
  }

  /** Functions that are always true because the catalog only shows what the user may read. */
  private static final Set<String> ALWAYS_TRUE =
      new HashSet<>(
          Arrays.asList(
              "has_table_privilege",
              "has_schema_privilege",
              "has_database_privilege",
              "has_column_privilege",
              "pg_table_is_visible",
              "pg_type_is_visible",
              "pg_function_is_visible"));

  /** Cast targets that print a value as text. */
  private static final Set<String> TEXT_TYPES =
      new HashSet<>(Arrays.asList("text", "varchar", "character varying", "name", "bpchar"));

  /** PostgreSQL's encoding names by number ({@code pg_enc}). */
  private static final String[] ENCODINGS = {
    "SQL_ASCII",
    "EUC_JP",
    "EUC_CN",
    "EUC_KR",
    "EUC_TW",
    "EUC_JIS_2004",
    "UTF8",
    "MULE_INTERNAL",
    "LATIN1",
    "LATIN2",
    "LATIN3",
    "LATIN4",
    "LATIN5",
    "LATIN6",
    "LATIN7",
    "LATIN8",
    "LATIN9",
    "LATIN10",
    "WIN1256",
    "WIN1258",
    "WIN866",
    "WIN874",
    "KOI8R",
    "WIN1251",
    "WIN1252",
    "ISO_8859_5",
    "ISO_8859_6",
    "ISO_8859_7",
    "ISO_8859_8",
    "WIN1250",
    "WIN1253",
    "WIN1254",
    "WIN1255",
    "WIN1257",
    "KOI8U",
    "SJIS",
    "BIG5",
    "GBK",
    "UHC",
    "GB18030",
    "JOHAB",
    "SHIFT_JIS_2004"
  };

  private PgToTableSql() {}

  /**
   * Translates one statement (already split, without the trailing semicolon).
   *
   * @throws PgException 42601 for syntax errors, 0A000 for constructs without exact translation,
   *     25006 for statements that write, 42P01 for tables not found on the search path
   */
  public static Translation translate(String sql, TranslationContext context) throws PgException {
    return translate(sql, context, true);
  }

  /**
   * As {@link #translate(String, TranslationContext)}; {@code outputNames} false leaves output
   * columns without alias unnamed (rule tests that look at expressions only).
   */
  static Translation translate(String sql, TranslationContext context, boolean outputNames)
      throws PgException {
    Statement statement;
    try {
      statement = CCJSqlParserUtil.parse(PreParse.rewrite(sql));
    } catch (JSQLParserException e) {
      throw new PgException(SqlState.SYNTAX_ERROR, "syntax error: " + firstLine(e));
    }
    if (!(statement instanceof Select)) {
      throw notAQuery(statement);
    }
    try {
      EmptyRelations.simplify(statement);
      ConstructCheck.Result checked = ConstructCheck.check(statement);
      Writer writer =
          new Writer(context, checked.cteNames, checked.otherTables ? null : checked.catalogTables);
      writer.qualifiedCatalogTables = checked.catalogTables;
      writer.outputNames = outputNames;
      writer.write((Select) statement);
      return new Translation(writer.builder.toString(), writer.parameters);
    } catch (UnsupportedConstructException e) {
      LOGGER.debug("Refused PostgreSQL statement ({}): {}", e.construct(), sql);
      throw new PgException(SqlState.FEATURE_NOT_SUPPORTED, e.getMessage());
    } catch (Refusal e) {
      throw new PgException(e.sqlState, e.getMessage());
    } catch (TableNotFound e) {
      throw new PgException(
          SqlState.UNDEFINED_TABLE,
          "relation \"" + e.getMessage() + "\" does not exist",
          "Qualify the table with its database, for example mydb." + e.getMessage(),
          false);
    }
  }

  private static PgException notAQuery(Statement statement) {
    String kind = statement.getClass().getSimpleName().toLowerCase(Locale.ROOT);
    if (kind.matches(
        "insert|update|delete|upsert|merge|truncate|create.*|alter.*|drop|grant|replace")) {
      return new PgException(
          SqlState.READ_ONLY_SQL_TRANSACTION,
          "cannot execute " + kind.toUpperCase(Locale.ROOT) + " in a read-only transaction");
    }
    return new PgException(
        SqlState.FEATURE_NOT_SUPPORTED,
        "statements of type " + statement.getClass().getSimpleName() + " are not supported");
  }

  private static String firstLine(Exception e) {
    String message = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
    if (message == null) {
      return "cannot parse the statement";
    }
    int newline = message.indexOf('\n');
    String line = newline < 0 ? message : message.substring(0, newline);
    // the parser's class name and lexer state mean nothing to a client
    return line.replaceFirst("^[\\w.]+Exception: ", "")
        .replaceFirst(",? in lexical state \\w+\\.?$", "");
  }

  /** A table not found on the search path (carries the table name). */
  private static final class TableNotFound extends RuntimeException {
    private static final long serialVersionUID = 1L;

    TableNotFound(String table) {
      super(table, null, false, false);
    }
  }

  /** A statement PostgreSQL itself would refuse with this SQLSTATE (unknown type, ...). */
  private static final class Refusal extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final String sqlState;

    Refusal(String sqlState, String message) {
      super(message, null, false, false);
      this.sqlState = sqlState;
    }
  }

  /** The deparsers with the translation rules. */
  private static final class Writer {
    final StringBuilder builder = new StringBuilder();
    final List<Integer> parameters = new ArrayList<>();
    final TranslationContext context;
    final Set<String> cteNames;

    /** Catalog tables by qualifier, {@code null} if the statement also reads other tables. */
    final Map<String, String> catalogTables;

    /** Catalog tables by qualifier, for qualified column references. */
    Map<String, String> qualifiedCatalogTables;

    final Expressions expressions;
    final Selects selects;

    Writer(TranslationContext context, Set<String> cteNames, Map<String, String> catalogTables) {
      this.context = context;
      this.cteNames = cteNames;
      this.catalogTables = catalogTables;
      this.expressions = new Expressions(this);
      this.selects = new Selects(this);
      expressions.setSelectVisitor(selects);
      expressions.setBuilder(builder);
      selects.setExpressionVisitor(expressions);
      selects.setBuilder(builder);
    }

    /** The select list whose names the client sees (PostgreSQL: the first branch's). */
    List<?> outputItems;

    boolean outputNames = true;

    void write(Select select) {
      Select first = select;
      while (first instanceof net.sf.jsqlparser.statement.select.SetOperationList) {
        first = ((net.sf.jsqlparser.statement.select.SetOperationList) first).getSelects().get(0);
      }
      while (first instanceof net.sf.jsqlparser.statement.select.ParenthesedSelect) {
        first = ((net.sf.jsqlparser.statement.select.ParenthesedSelect) first).getSelect();
      }
      if (first instanceof PlainSelect && outputNames) {
        outputItems = ((PlainSelect) first).getSelectItems();
      }
      select.accept(
          (net.sf.jsqlparser.statement.select.SelectVisitor<StringBuilder>) selects, null);
    }

    /** The table-model name of a table reference. */
    String table(Table table) {
      String schema = Identifiers.fold(table.getSchemaName());
      String name = Identifiers.fold(table.getName());
      if (schema == null) {
        if (cteNames.contains(name)) {
          return Identifiers.quote(name);
        }
        if (name.startsWith("pg_")) {
          return "information_schema." + Identifiers.quote(name);
        }
        for (String candidate : context.searchPath()) {
          if (candidate.equals("pg_catalog") || candidate.equals("information_schema")) {
            continue;
          }
          if (context.tableExists(candidate, name)) {
            return Identifiers.quote(candidate) + "." + Identifiers.quote(name);
          }
        }
        throw new TableNotFound(name);
      }
      switch (schema) {
        case "pg_catalog":
          return "information_schema." + Identifiers.quote(name);
        case "information_schema":
          // IoTDB's own information_schema stays hidden from PostgreSQL clients
          return "information_schema." + Identifiers.quote("pg_is_" + name);
        default:
          return Identifiers.quote(schema) + "." + Identifiers.quote(name);
      }
    }
  }

  /** Statement-level rules: table names, FETCH FIRST. */
  private static final class Selects extends SelectDeParser {
    private final Writer writer;

    Selects(Writer writer) {
      this.writer = writer;
    }

    @Override
    public <S> StringBuilder visit(Table table, S context) {
      builder.append(writer.table(table));
      Alias alias = table.getAlias();
      if (alias != null) {
        if (alias.getAliasColumns() != null && !alias.getAliasColumns().isEmpty()) {
          throw new UnsupportedConstructException("column aliases of a table");
        }
        builder.append(" AS ").append(Identifiers.quote(Identifiers.fold(alias.getName())));
      }
      return builder;
    }

    @Override
    protected void deparseSelectItemsClause(
        List<net.sf.jsqlparser.statement.select.SelectItem<?>> items) {
      for (int i = 0; i < items.size(); i++) {
        if (i > 0) {
          builder.append(", ");
        }
        net.sf.jsqlparser.statement.select.SelectItem<?> item = items.get(i);
        if (item.getExpression() instanceof net.sf.jsqlparser.expression.NullValue) {
          // PostgreSQL returns an untyped NULL output column as text
          builder.append("CAST(NULL AS STRING)");
        } else {
          item.getExpression().accept(writer.expressions, null);
        }
        Alias alias = item.getAlias();
        if (alias != null) {
          // output names follow the identifier rules: unquoted aliases fold to lower case
          builder.append(" AS ").append(Identifiers.quote(Identifiers.fold(alias.getName())));
        } else if (items == writer.outputItems) {
          // the name PostgreSQL gives an output column without alias (the engine's are _colN)
          String name = OutputNames.of(item.getExpression());
          if (name != null) {
            builder.append(" AS ").append(Identifiers.quote(name));
          }
        }
      }
    }

    @Override
    public <S> StringBuilder visit(PlainSelect select, S context) {
      if (select.getFetch() != null) {
        // FETCH FIRST n ROWS ONLY = LIMIT n (WITH TIES has no table-model form)
        List<String> options = select.getFetch().getFetchParameters();
        if (options != null
            && options.stream().anyMatch(p -> p.toUpperCase(Locale.ROOT).contains("TIES"))) {
          throw new UnsupportedConstructException("FETCH ... WITH TIES");
        }
        select.setLimit(
            new Limit()
                .withRowCount(
                    select.getFetch().getExpression() == null
                        ? new net.sf.jsqlparser.expression.LongValue(1)
                        : select.getFetch().getExpression()));
        select.setFetch(null);
      }
      return super.visit(select, context);
    }
  }

  /** Expression-level rules. */
  private static final class Expressions extends ExpressionDeParser {
    private final Writer writer;

    Expressions(Writer writer) {
      this.writer = writer;
    }

    @Override
    public <S> StringBuilder visit(Column column, S context) {
      Table table = column.getTable();
      String name = Identifiers.fold(column.getColumnName());
      if ((table == null || table.getName() == null) && !isQuoted(column.getColumnName())) {
        // keywords PostgreSQL parses as values, not columns
        switch (name) {
          case "current_user":
          case "session_user":
          case "user":
          case "current_role":
            builder.append(Identifiers.literal(writer.context.user()));
            return builder;
          case "current_catalog":
            builder.append(Identifiers.literal(writer.context.currentDatabase()));
            return builder;
          case "current_schema":
            appendCurrentSchema();
            return builder;
          case "true":
          case "false":
            builder.append(name.toUpperCase(Locale.ROOT));
            return builder;
          default:
            break;
        }
      }
      if (table != null && table.getName() != null) {
        if (table.getSchemaName() != null) {
          builder.append(writer.table(table));
        } else {
          builder.append(Identifiers.quote(Identifiers.fold(table.getName())));
        }
        builder.append('.');
      }
      builder.append(Identifiers.quote(name));
      return builder;
    }

    @Override
    public <S> StringBuilder visit(Function function, S context) {
      List<String> parts = function.getMultipartName();
      if (parts.size() > 2
          || (parts.size() == 2 && !"pg_catalog".equals(Identifiers.fold(parts.get(0))))) {
        throw new UnsupportedConstructException("function " + function.getName() + "()");
      }
      String name = Identifiers.fold(parts.get(parts.size() - 1));
      int arguments =
          function.getParameters() == null ? 0 : function.getParameters().getExpressions().size();
      switch (name) {
        case "version":
          builder.append(Identifiers.literal(writer.context.version()));
          return builder;
        case "current_database":
          builder.append(Identifiers.literal(writer.context.currentDatabase()));
          return builder;
        case "current_schema":
          appendCurrentSchema();
          return builder;
        case "pg_backend_pid":
          builder.append(writer.context.backendPid());
          return builder;
        case "format_type":
          if (arguments == 2) {
            appendTypeName(
                (Expression) function.getParameters().getExpressions().get(0), context, false);
            return builder;
          }
          break;
        case "pg_get_expr":
          // only pg_attrdef and pg_index hold expressions, and both are empty
          if (arguments == 2 || arguments == 3) {
            builder.append("CAST(NULL AS STRING)");
            return builder;
          }
          break;
        case "obj_description":
          if (arguments == 1 || arguments == 2) {
            List<?> args = function.getParameters().getExpressions();
            appendDescription(
                (Expression) args.get(0),
                null,
                arguments == 2 ? (Expression) args.get(1) : null,
                context);
            return builder;
          }
          break;
        case "col_description":
          if (arguments == 2) {
            List<?> args = function.getParameters().getExpressions();
            appendDescription((Expression) args.get(0), (Expression) args.get(1), null, context);
            return builder;
          }
          break;
        case "pg_encoding_to_char":
          if (arguments == 1) {
            appendEncodingName(
                (Expression) function.getParameters().getExpressions().get(0), context);
            return builder;
          }
          break;
        case "array_to_string":
          if (arguments == 2 || arguments == 3) {
            Object array = function.getParameters().getExpressions().get(0);
            org.apache.iotdb.postgresql.catalog.PgCatalogTables.Column column =
                array instanceof Column ? catalogColumn((Column) array) : null;
            if (column != null && column.array()) {
              // the catalog's array columns are always NULL, and so is the result
              builder.append("CAST(NULL AS STRING)");
              return builder;
            }
          }
          break;
        case "date_trunc":
          if (arguments == 2) {
            List<?> args = function.getParameters().getExpressions();
            appendTemplate(
                DateTimeRules.truncate(
                    literalText(args.get(0), "date_trunc"),
                    writer.context.zone(),
                    writer.context.precision()),
                (Expression) args.get(1),
                context);
            return builder;
          }
          break;
        case "date_part":
          if (arguments == 2) {
            List<?> args = function.getParameters().getExpressions();
            // date_part returns double precision
            builder.append("CAST(");
            appendTemplate(
                DateTimeRules.extract(
                    literalText(args.get(0), "date_part"),
                    writer.context.zone(),
                    writer.context.precision()),
                (Expression) args.get(1),
                context);
            builder.append(" AS DOUBLE)");
            return builder;
          }
          break;
        case "nullif":
          if (arguments == 2) {
            // the engine cannot evaluate NULLIF; this is its definition, without an untyped NULL
            List<?> args = function.getParameters().getExpressions();
            Expression a = (Expression) args.get(0);
            Expression b = (Expression) args.get(1);
            builder.append("CASE WHEN ");
            a.accept(this, context);
            builder.append(" <> ");
            b.accept(this, context);
            builder.append(" OR ");
            a.accept(this, context);
            builder.append(" IS NULL OR ");
            b.accept(this, context);
            builder.append(" IS NULL THEN ");
            a.accept(this, context);
            builder.append(" END");
            return builder;
          }
          break;
        case "concat":
          // PostgreSQL's concat skips NULLs and returns '' when all are NULL
          builder.append("COALESCE(");
          function.setName("concat");
          super.visit(function, context);
          builder.append(", '')");
          return builder;
        case "pg_get_userbyid":
          if (arguments == 1) {
            // every catalog object is shown as owned by the querying user (contract)
            builder.append(Identifiers.literal(writer.context.user()));
            return builder;
          }
          break;
        case "now":
          if (arguments == 0) {
            builder.append("now()");
            return builder;
          }
          break;
        default:
          break;
      }
      if (ALWAYS_TRUE.contains(name)) {
        builder.append("TRUE");
        return builder;
      }
      String mapped = SAME.contains(name) ? name : RENAMED.get(name);
      if (mapped == null) {
        throw new UnsupportedConstructException("function " + name + "()");
      }
      function.setName(mapped);
      return super.visit(function, context);
    }

    @Override
    public <S> StringBuilder visit(BooleanValue value, S context) {
      builder.append(value.getValue() ? "TRUE" : "FALSE");
      return builder;
    }

    @Override
    public <S> StringBuilder visit(CastExpression cast, S context) {
      String target = PgTypeNames.baseName(cast.getColDataType());
      if (TEXT_TYPES.contains(target) && cast.getLeftExpression() instanceof CastExpression) {
        CastExpression inner = (CastExpression) cast.getLeftExpression();
        if (PgTypeNames.baseName(inner.getColDataType()).equals("regtype")
            && !(inner.getLeftExpression() instanceof StringValue)) {
          // regtype prints the type name, or the number for an unknown OID
          appendTypeName(inner.getLeftExpression(), context, true);
          return builder;
        }
      }
      if (target.startsWith("reg")) {
        appendRegCast(target, cast.getLeftExpression());
        return builder;
      }
      if (cast.getLeftExpression() instanceof StringValue
          && ((StringValue) cast.getLeftExpression()).getPrefix() == null
          && !target.equals("interval")) {
        // literals are read as PostgreSQL reads them (time zone, formats)
        String text = ((StringValue) cast.getLeftExpression()).getValue().replace("''", "'");
        switch (target) {
          case "date":
            appendDate(text);
            return builder;
          case "timestamp":
          case "timestamp without time zone":
            appendTimestamp(text, false);
            return builder;
          case "timestamptz":
          case "timestamp with time zone":
            appendTimestamp(text, true);
            return builder;
          default:
            break;
        }
      }
      String type = PgTypeNames.tableType(cast.getColDataType());
      builder.append("CAST(");
      cast.getLeftExpression().accept(this, context);
      builder.append(" AS ").append(type).append(')');
      return builder;
    }

    @Override
    public <S> StringBuilder visit(JdbcParameter parameter, S context) {
      Integer index = parameter.getIndex();
      writer.parameters.add(index == null ? writer.parameters.size() + 1 : index);
      builder.append('?');
      return builder;
    }

    @Override
    public <S> StringBuilder visit(StringValue value, S context) {
      String prefix = value.getPrefix();
      if (prefix == null) {
        if (value.getQuoteStr() == null || "'".equals(value.getQuoteStr())) {
          // standard_conforming_strings: the body is already a table-model literal body
          builder.append('\'').append(value.getValue()).append('\'');
        } else {
          builder.append(Identifiers.literal(value.getValue()));
        }
        return builder;
      }
      if (prefix.equalsIgnoreCase("E")) {
        builder.append(Identifiers.literal(StringLiterals.unescape(value.getValue())));
        return builder;
      }
      throw new UnsupportedConstructException(prefix + "'...' literals");
    }

    @Override
    public <S> StringBuilder visit(TimeKeyExpression key, S context) {
      String value = key.getStringValue().toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
      if (value.startsWith("CURRENT_TIMESTAMP") || value.startsWith("LOCALTIMESTAMP")) {
        if (value.contains("(")) {
          throw new UnsupportedConstructException(value + " with a precision");
        }
        builder.append("now()");
      } else if (value.equals("CURRENT_DATE")) {
        builder.append("CAST(now() AS DATE)");
      } else {
        throw new UnsupportedConstructException(value);
      }
      return builder;
    }

    @Override
    public <S> StringBuilder visit(LikeExpression like, S context) {
      switch (like.getLikeKeyWord()) {
        case LIKE:
          return super.visit(like, context);
        case ILIKE:
          builder.append("lower(");
          like.getLeftExpression().accept(this, context);
          builder.append(like.isNot() ? ") NOT LIKE lower(" : ") LIKE lower(");
          like.getRightExpression().accept(this, context);
          builder.append(')');
          if (like.getEscape() != null) {
            builder.append(" ESCAPE ");
            like.getEscape().accept(this, context);
          }
          return builder;
        default:
          throw new UnsupportedConstructException(like.getLikeKeyWord().name());
      }
    }

    /**
     * {@code 'name'::regclass} and the other object-identifier casts of a literal: the OID as a
     * constant for catalog objects, as a lookup in the catalog tables for user tables. Casts of
     * other expressions are refused: PostgreSQL prints such values as names, the engine would print
     * numbers.
     */
    private void appendRegCast(String type, Expression operand) {
      if (!(operand instanceof StringValue)) {
        throw new UnsupportedConstructException("casts of expressions to " + type);
      }
      StringValue literal = (StringValue) operand;
      String text =
          "E".equalsIgnoreCase(literal.getPrefix())
              ? StringLiterals.unescape(literal.getValue())
              : literal.getValue().replace("''", "'");
      switch (type) {
        case "regclass":
          appendRegclass(text);
          return;
        case "regtype":
          builder.append(
              StaticCatalog.typeOid(text.toLowerCase(Locale.ROOT))
                  .orElseThrow(() -> new Refusal("42704", "type \"" + text + "\" does not exist")));
          return;
        case "regproc":
        case "regprocedure":
          String proc = text.contains("(") ? text.substring(0, text.indexOf('(')) : text;
          builder.append(
              StaticCatalog.procOid(Identifiers.fold(proc.trim()))
                  .orElseThrow(
                      () -> new Refusal("42883", "function \"" + text + "\" does not exist")));
          return;
        case "regnamespace":
          String schema = Identifiers.fold(text.trim());
          if (schema.equals("pg_catalog")) {
            builder.append(StaticCatalog.PG_CATALOG);
          } else if (schema.equals("information_schema")) {
            builder.append(StaticCatalog.INFORMATION_SCHEMA);
          } else {
            builder
                .append("(SELECT max(pg_rn.\"oid\") FROM information_schema.\"pg_namespace\" pg_rn")
                .append(" WHERE pg_rn.\"nspname\" = ")
                .append(Identifiers.literal(schema))
                .append(')');
          }
          return;
        default:
          throw new UnsupportedConstructException("casts to " + type);
      }
    }

    private void appendRegclass(String text) {
      List<String> parts = qualifiedName(text);
      if (parts.size() > 2) {
        throw new UnsupportedConstructException("cross-database references in regclass");
      }
      String schema = parts.size() == 2 ? parts.get(0) : null;
      String name = parts.get(parts.size() - 1);
      if (schema == null || schema.equals("pg_catalog") || schema.equals("information_schema")) {
        java.util.Optional<Long> system = StaticCatalog.systemRelationOid(schema, name);
        if (system.isPresent()) {
          builder.append(system.get());
          return;
        }
        if (schema != null) {
          throw new Refusal("42P01", "relation \"" + text + "\" does not exist");
        }
        schema = searchPathDatabase(name);
      }
      // a table without read permission gives NULL, as an absent one: no existence leak
      builder
          .append("(SELECT max(pg_rc.\"oid\") FROM information_schema.\"pg_class\" pg_rc")
          .append(" JOIN information_schema.\"pg_namespace\" pg_rn")
          .append(" ON pg_rc.\"relnamespace\" = pg_rn.\"oid\"")
          .append(" WHERE pg_rn.\"nspname\" = ")
          .append(Identifiers.literal(schema))
          .append(" AND pg_rc.\"relname\" = ")
          .append(Identifiers.literal(name))
          .append(')');
    }

    private String searchPathDatabase(String table) {
      for (String candidate : writer.context.searchPath()) {
        if (!candidate.equals("pg_catalog")
            && !candidate.equals("information_schema")
            && writer.context.tableExists(candidate, table)) {
          return candidate;
        }
      }
      throw new Refusal("42P01", "relation \"" + table + "\" does not exist");
    }

    /** Splits {@code a.b} or {@code "A"."b"} into folded parts. */
    private static List<String> qualifiedName(String text) {
      List<String> parts = new ArrayList<>();
      StringBuilder part = new StringBuilder();
      boolean quoted = false;
      boolean wasQuoted = false;
      for (int i = 0; i < text.length(); i++) {
        char c = text.charAt(i);
        if (c == '"') {
          if (quoted && i + 1 < text.length() && text.charAt(i + 1) == '"') {
            part.append('"');
            i++;
          } else {
            quoted = !quoted;
            wasQuoted = true;
          }
        } else if (c == '.' && !quoted) {
          parts.add(wasQuoted ? part.toString() : part.toString().trim().toLowerCase(Locale.ROOT));
          part.setLength(0);
          wasQuoted = false;
        } else {
          part.append(c);
        }
      }
      parts.add(wasQuoted ? part.toString() : part.toString().trim().toLowerCase(Locale.ROOT));
      return parts;
    }

    /** {@code format_type(oid, typmod)}: the type names of the catalog (typmods are always -1). */
    private <S> void appendTypeName(Expression oid, S context, boolean numberForUnknown) {
      // CASE needs equal types; every catalog OID is below 2^31, so INT32 holds it
      builder.append("CASE CAST(");
      oid.accept(this, context);
      builder.append(" AS INT32)");
      for (Map.Entry<Long, String> type : StaticCatalog.formatTypeNames().entrySet()) {
        builder
            .append(" WHEN ")
            .append(type.getKey())
            .append(" THEN ")
            .append(Identifiers.literal(type.getValue()));
      }
      if (numberForUnknown) {
        builder.append(" ELSE CAST(");
        oid.accept(this, context);
        builder.append(" AS STRING) END");
        return;
      }
      builder.append(" ELSE CASE WHEN ");
      oid.accept(this, context);
      builder.append(" IS NULL THEN CAST(NULL AS STRING) ELSE '???' END END");
    }

    /**
     * {@code obj_description(oid [, catalog])} and {@code col_description(table, column)}: a lookup
     * in {@code pg_description}.
     */
    private <S> void appendDescription(
        Expression object, Expression column, Expression catalog, S context) {
      builder.append(
          "(SELECT max(pg_d.\"description\") FROM information_schema.\"pg_description\" pg_d"
              + " WHERE pg_d.\"objoid\" = ");
      object.accept(this, context);
      builder.append(" AND pg_d.\"objsubid\" = ");
      if (column == null) {
        builder.append('0');
      } else {
        column.accept(this, context);
      }
      if (column != null) {
        builder.append(" AND pg_d.\"classoid\" = ").append(StaticCatalog.PG_CLASS);
      } else if (catalog != null) {
        if (!(catalog instanceof StringValue)) {
          throw new UnsupportedConstructException("obj_description with a computed catalog name");
        }
        String name = Identifiers.fold(((StringValue) catalog).getValue());
        builder
            .append(" AND pg_d.\"classoid\" = ")
            .append(
                StaticCatalog.systemRelationOid(null, name)
                    .orElseThrow(
                        () -> new Refusal("42P01", "relation \"" + name + "\" does not exist")));
      }
      builder.append(')');
    }

    /**
     * Comparisons: a quoted literal compared with a typed catalog column takes the column's type,
     * as an untyped literal does in PostgreSQL ({@code a.attrelid = '16386'}).
     */
    @Override
    public <S> StringBuilder deparse(
        net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression expression,
        String operator,
        S context) {
      Expression left = expression.getLeftExpression();
      Expression right = expression.getRightExpression();
      String leftLiteral = typedLiteral(right, left);
      String rightLiteral = typedLiteral(left, right);
      if (leftLiteral == null && rightLiteral == null) {
        return super.deparse(expression, operator, context);
      }
      if (leftLiteral != null) {
        builder.append(leftLiteral);
      } else {
        left.accept(this, context);
      }
      builder.append(operator.startsWith(" ") ? operator : " " + operator + " ");
      if (rightLiteral != null) {
        builder.append(rightLiteral);
      } else {
        right.accept(this, context);
      }
      return builder;
    }

    /**
     * The literal {@code value} in the type of the catalog column {@code other}, or {@code null} if
     * nothing is to change.
     */
    private String typedLiteral(Expression other, Expression value) {
      if (!(other instanceof Column) || !(value instanceof StringValue)) {
        return null;
      }
      StringValue literal = (StringValue) value;
      if (literal.getPrefix() != null) {
        return null;
      }
      org.apache.tsfile.enums.TSDataType type = catalogColumnType((Column) other);
      if (type == null) {
        return null;
      }
      String text = literal.getValue().replace("''", "'").trim();
      try {
        switch (type) {
          case INT64:
          case INT32:
            return String.valueOf(Long.parseLong(text));
          case FLOAT:
          case DOUBLE:
            return String.valueOf(Double.parseDouble(text));
          case BOOLEAN:
            String b = text.toLowerCase(Locale.ROOT);
            if (Arrays.asList("t", "true", "y", "yes", "on", "1").contains(b)) {
              return "TRUE";
            }
            if (Arrays.asList("f", "false", "n", "no", "off", "0").contains(b)) {
              return "FALSE";
            }
            throw new NumberFormatException(text);
          default:
            return null;
        }
      } catch (NumberFormatException e) {
        throw new Refusal(
            "22P02",
            "invalid input syntax for type "
                + type.name().toLowerCase(Locale.ROOT)
                + ": \""
                + text
                + "\"");
      }
    }

    /** The type of a column of a catalog table, or {@code null} if it is not one. */
    private org.apache.tsfile.enums.TSDataType catalogColumnType(Column column) {
      org.apache.iotdb.postgresql.catalog.PgCatalogTables.Column found = catalogColumn(column);
      return found == null ? null : found.type();
    }

    /** The catalog column a column reference names, or {@code null} if unknown or ambiguous. */
    private org.apache.iotdb.postgresql.catalog.PgCatalogTables.Column catalogColumn(
        Column column) {
      String name = Identifiers.fold(column.getColumnName());
      Table table = column.getTable();
      List<String> candidates = new ArrayList<>();
      if (table != null && table.getName() != null) {
        String catalog = writer.qualifiedCatalogTables.get(Identifiers.fold(table.getName()));
        if (catalog == null) {
          return null;
        }
        candidates.add(catalog);
      } else {
        if (writer.catalogTables == null) {
          // the column may belong to a user table
          return null;
        }
        for (String catalog : writer.catalogTables.values()) {
          if (catalog != null) {
            candidates.add(catalog);
          }
        }
      }
      org.apache.iotdb.postgresql.catalog.PgCatalogTables.Column found = null;
      for (String catalog : candidates) {
        for (org.apache.iotdb.postgresql.catalog.PgCatalogTables.Column c :
            org.apache.iotdb.postgresql.catalog.PgCatalogTables.tables().get(catalog)) {
          if (c.name().equals(name)) {
            if (found != null && (found.type() != c.type() || found.array() != c.array())) {
              return null;
            }
            found = c;
          }
        }
      }
      return found;
    }

    /**
     * PostgreSQL gives a bare {@code NULL} result of a CASE the type of the other results; the
     * engine wants equal types, so the NULL is written as a cast when another result's type is
     * known.
     */
    @Override
    public <S> StringBuilder visit(
        net.sf.jsqlparser.expression.CaseExpression expression, S context) {
      List<net.sf.jsqlparser.expression.WhenClause> whens = expression.getWhenClauses();
      String type = null;
      for (net.sf.jsqlparser.expression.WhenClause when : whens) {
        type = type != null ? type : pgTypeOf(when.getThenExpression());
      }
      type = type != null ? type : pgTypeOf(expression.getElseExpression());
      if (type != null) {
        for (net.sf.jsqlparser.expression.WhenClause when : whens) {
          if (when.getThenExpression() instanceof net.sf.jsqlparser.expression.NullValue) {
            when.setThenExpression(typedNull(type));
          }
        }
        if (expression.getElseExpression() instanceof net.sf.jsqlparser.expression.NullValue) {
          expression.setElseExpression(typedNull(type));
        }
      }
      return super.visit(expression, context);
    }

    private static Expression typedNull(String pgType) {
      net.sf.jsqlparser.expression.CastExpression cast =
          new net.sf.jsqlparser.expression.CastExpression();
      cast.setLeftExpression(new net.sf.jsqlparser.expression.NullValue());
      cast.setColDataType(new net.sf.jsqlparser.statement.create.table.ColDataType(pgType));
      return cast;
    }

    /** The PostgreSQL type name of an expression whose type is known here, else {@code null}. */
    private String pgTypeOf(Expression expression) {
      if (expression instanceof StringValue) {
        return "text";
      }
      if (expression instanceof net.sf.jsqlparser.expression.LongValue) {
        long value = ((net.sf.jsqlparser.expression.LongValue) expression).getValue();
        return value == (int) value ? "int4" : "int8";
      }
      if (expression instanceof net.sf.jsqlparser.expression.DoubleValue) {
        return "float8";
      }
      if (expression instanceof BooleanValue) {
        return "bool";
      }
      if (expression instanceof CastExpression) {
        return ((CastExpression) expression).getColDataType().getDataType();
      }
      if (expression instanceof net.sf.jsqlparser.expression.CaseExpression) {
        net.sf.jsqlparser.expression.CaseExpression nested =
            (net.sf.jsqlparser.expression.CaseExpression) expression;
        for (net.sf.jsqlparser.expression.WhenClause when : nested.getWhenClauses()) {
          String type = pgTypeOf(when.getThenExpression());
          if (type != null) {
            return type;
          }
        }
        return pgTypeOf(nested.getElseExpression());
      }
      if (expression instanceof Column) {
        org.apache.tsfile.enums.TSDataType type = catalogColumnType((Column) expression);
        if (type == null) {
          return null;
        }
        switch (type) {
          case INT32:
            return "int4";
          case INT64:
            return "int8";
          case FLOAT:
            return "float4";
          case DOUBLE:
            return "float8";
          case BOOLEAN:
            return "bool";
          default:
            return "text";
        }
      }
      return null;
    }

    /**
     * {@code a ~ 'pattern'} and its variants, for patterns with an exact LIKE form (psql's {@code
     * ^(name)$}, {@code ^pg_}); others are refused.
     */
    @Override
    public <S> StringBuilder visit(
        net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator match, S context) {
      Expression pattern = match.getRightExpression();
      if (!(pattern instanceof StringValue) || ((StringValue) pattern).getPrefix() != null) {
        throw new UnsupportedConstructException("regular expressions that are not literals");
      }
      String regex = ((StringValue) pattern).getValue().replace("''", "'");
      RegexPatterns.LikeForm like = RegexPatterns.toLike(regex);
      if (like == null) {
        throw new UnsupportedConstructException("the regular expression '" + regex + "'");
      }
      String type = match.getOperatorType().name();
      boolean negated = type.startsWith("NOT_");
      boolean insensitive = type.endsWith("CASEINSENSITIVE");
      builder.append(negated ? "NOT (" : "(");
      if (like.exact()) {
        appendOperand(match.getLeftExpression(), insensitive, context);
        if (like.literals.size() == 1) {
          builder.append(" = ");
          appendPattern(like.literals.get(0), insensitive);
        } else {
          builder.append(" IN (");
          for (int i = 0; i < like.literals.size(); i++) {
            builder.append(i > 0 ? ", " : "");
            appendPattern(like.literals.get(i), insensitive);
          }
          builder.append(')');
        }
      } else {
        for (int i = 0; i < like.patterns.size(); i++) {
          builder.append(i > 0 ? " OR " : "");
          appendOperand(match.getLeftExpression(), insensitive, context);
          builder.append(" LIKE ");
          appendPattern(like.patterns.get(i), insensitive);
          if (like.escaped) {
            builder.append(" ESCAPE '\\'");
          }
        }
      }
      builder.append(')');
      return builder;
    }

    private <S> void appendOperand(Expression operand, boolean lower, S context) {
      if (lower) {
        builder.append("lower(");
      }
      operand.accept(this, context);
      if (lower) {
        builder.append(')');
      }
    }

    private void appendPattern(String text, boolean lower) {
      builder.append(Identifiers.literal(lower ? text.toLowerCase(Locale.ROOT) : text));
    }

    /** {@code pg_encoding_to_char(n)}: PostgreSQL's encoding names, '' for unknown numbers. */
    private <S> void appendEncodingName(Expression encoding, S context) {
      builder.append("CASE CAST(");
      encoding.accept(this, context);
      builder.append(" AS INT32)");
      for (int i = 0; i < ENCODINGS.length; i++) {
        builder.append(" WHEN ").append(i).append(" THEN '").append(ENCODINGS[i]).append('\'');
      }
      builder.append(" ELSE CASE WHEN ");
      encoding.accept(this, context);
      builder.append(" IS NULL THEN CAST(NULL AS STRING) ELSE '' END END");
    }

    /** True while writing the sub-query of IN or EXISTS (not a scalar sub-query). */
    private boolean nonScalar;

    @Override
    public <S> StringBuilder visit(
        net.sf.jsqlparser.expression.operators.relational.ExistsExpression exists, S context) {
      boolean saved = nonScalar;
      nonScalar = true;
      try {
        return super.visit(exists, context);
      } finally {
        nonScalar = saved;
      }
    }

    @Override
    public <S> StringBuilder visit(
        net.sf.jsqlparser.expression.operators.relational.InExpression in, S context) {
      boolean saved = nonScalar;
      nonScalar = true;
      try {
        return super.visit(in, context);
      } finally {
        nonScalar = saved;
      }
    }

    /**
     * A scalar sub-query whose WHERE has conditions on the outer row only ({@code AND a.atthasdef}
     * in psql's {@code \d}): the engine cannot plan those as join criteria. {@code (SELECT x ...
     * WHERE c AND p)} equals {@code CASE WHEN p THEN (SELECT x ... WHERE c) END} when the sub-query
     * has no aggregates (no rows means NULL either way).
     */
    @Override
    public <S> StringBuilder visit(net.sf.jsqlparser.statement.select.Select any, S context) {
      if (!(any instanceof net.sf.jsqlparser.statement.select.ParenthesedSelect)) {
        return super.visit(any, context);
      }
      net.sf.jsqlparser.statement.select.ParenthesedSelect select =
          (net.sf.jsqlparser.statement.select.ParenthesedSelect) any;
      boolean scalar = !nonScalar;
      nonScalar = false;
      try {
        PlainSelect inner =
            select.getSelect() instanceof PlainSelect ? (PlainSelect) select.getSelect() : null;
        if (scalar
            && inner != null
            && inner.getWhere() instanceof BooleanValue
            && !((BooleanValue) inner.getWhere()).getValue()
            && inner.getSelectItems().size() == 1
            && inner.getGroupBy() == null
            && !EmptyRelations.hasAggregate(inner.getSelectItems())) {
          // a scalar sub-query without rows is NULL (EmptyRelations wrote a typed NULL)
          inner.getSelectItems().get(0).getExpression().accept(this, context);
          return builder;
        }
        List<Expression> outer =
            scalar && inner != null ? OuterConditions.split(inner) : Collections.emptyList();
        if (outer.isEmpty()) {
          return super.visit(any, context);
        }
        Expression where = inner.getWhere();
        inner.setWhere(OuterConditions.remaining(where, outer));
        try {
          builder.append("CASE WHEN ");
          OuterConditions.and(outer).accept(this, context);
          builder.append(" THEN ");
          super.visit(any, context);
          builder.append(" END");
        } finally {
          inner.setWhere(where);
        }
        return builder;
      } finally {
        nonScalar = !scalar;
      }
    }

    /** The text of a string literal argument (unit and field names). */
    private String literalText(Object argument, String function) {
      if (!(argument instanceof StringValue)) {
        throw new UnsupportedConstructException(function + "() with a computed unit");
      }
      return ((StringValue) argument).getValue();
    }

    /**
     * Writes {@code template} with its {@code %s} replaced by {@code argument}: a column as it is
     * (so the engine still sees {@code date_bin(..., time)}), anything else cast to TIMESTAMP.
     */
    private <S> void appendTemplate(String template, Expression argument, S context) {
      String[] parts = template.split("%s", -1);
      for (int i = 0; i < parts.length; i++) {
        builder.append(parts[i]);
        if (i + 1 < parts.length) {
          if (argument instanceof Column || isNow(argument)) {
            argument.accept(this, context);
          } else {
            builder.append("CAST(");
            argument.accept(this, context);
            builder.append(" AS TIMESTAMP)");
          }
        }
      }
    }

    private static boolean isNow(Expression expression) {
      if (expression instanceof TimeKeyExpression) {
        String key = ((TimeKeyExpression) expression).getStringValue().toUpperCase(Locale.ROOT);
        return key.startsWith("CURRENT_TIMESTAMP") || key.startsWith("LOCALTIMESTAMP");
      }
      return expression instanceof Function
          && Identifiers.fold(((Function) expression).getName()).equals("now")
          && (((Function) expression).getParameters() == null
              || ((Function) expression).getParameters().getExpressions().isEmpty());
    }

    @Override
    public <S> StringBuilder visit(
        net.sf.jsqlparser.expression.ExtractExpression extract, S context) {
      appendTemplate(
          DateTimeRules.extract(
              extract.getName(), writer.context.zone(), writer.context.precision()),
          extract.getExpression(),
          context);
      return builder;
    }

    /** An interval literal ({@code interval '2 hours'}, {@code '2 hours'::interval}), or null. */
    private static String intervalText(Expression expression) {
      if (expression instanceof net.sf.jsqlparser.expression.IntervalExpression) {
        net.sf.jsqlparser.expression.IntervalExpression interval =
            (net.sf.jsqlparser.expression.IntervalExpression) expression;
        String parameter = interval.getParameter();
        if (parameter == null) {
          throw new UnsupportedConstructException("INTERVAL with a computed value");
        }
        String text = parameter.trim();
        if (text.length() >= 2 && text.startsWith("'") && text.endsWith("'")) {
          text = text.substring(1, text.length() - 1).replace("''", "'");
        }
        if (interval.getIntervalQualifier() != null) {
          String qualifier = interval.getIntervalQualifier().toString().trim();
          if (qualifier.toUpperCase(Locale.ROOT).contains(" TO ")) {
            throw new UnsupportedConstructException("INTERVAL ... TO ...");
          }
          return text + " " + qualifier;
        }
        return interval.getIntervalType() == null ? text : text + " " + interval.getIntervalType();
      }
      if (expression instanceof CastExpression
          && PgTypeNames.baseName(((CastExpression) expression).getColDataType()).equals("interval")
          && ((CastExpression) expression).getLeftExpression() instanceof StringValue) {
        return ((StringValue) ((CastExpression) expression).getLeftExpression())
            .getValue()
            .replace("''", "'");
      }
      return null;
    }

    @Override
    public <S> StringBuilder visit(
        net.sf.jsqlparser.expression.operators.arithmetic.Addition addition, S context) {
      String right = intervalText(addition.getRightExpression());
      String left = intervalText(addition.getLeftExpression());
      if (right != null) {
        appendShifted(addition.getLeftExpression(), "+", right, context);
        return builder;
      }
      if (left != null) {
        appendShifted(addition.getRightExpression(), "+", left, context);
        return builder;
      }
      return super.visit(addition, context);
    }

    @Override
    public <S> StringBuilder visit(
        net.sf.jsqlparser.expression.operators.arithmetic.Subtraction subtraction, S context) {
      String right = intervalText(subtraction.getRightExpression());
      if (right != null) {
        appendShifted(subtraction.getLeftExpression(), "-", right, context);
        return builder;
      }
      if (intervalText(subtraction.getLeftExpression()) != null) {
        throw new UnsupportedConstructException("an interval minus a timestamp");
      }
      return super.visit(subtraction, context);
    }

    /** {@code timestamp ± interval}: the engine adds whole timestamp units to a TIMESTAMP. */
    private <S> void appendShifted(Expression time, String operator, String interval, S context) {
      long units =
          DateTimeRules.intervalUnits(interval, writer.context.zone(), writer.context.precision());
      builder.append('(');
      if (isNow(time)) {
        time.accept(this, context);
      } else {
        // a DATE operand becomes a timestamp first, as in PostgreSQL
        builder.append("CAST(");
        time.accept(this, context);
        builder.append(" AS TIMESTAMP)");
      }
      builder.append(' ').append(operator).append(' ').append(units).append(')');
    }

    @Override
    public <S> StringBuilder visit(
        net.sf.jsqlparser.expression.IntervalExpression interval, S context) {
      throw new UnsupportedConstructException(
          "INTERVAL values outside timestamp + interval and timestamp - interval");
    }

    @Override
    public <S> StringBuilder visit(
        net.sf.jsqlparser.expression.DateTimeLiteralExpression literal, S context) {
      String value = literal.getValue().trim();
      if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
        value = value.substring(1, value.length() - 1);
      }
      switch (literal.getType()) {
        case DATE:
          appendDate(value);
          break;
        case TIMESTAMP:
        case DATETIME:
          appendTimestamp(value, false);
          break;
        case TIMESTAMPTZ:
          appendTimestamp(value, true);
          break;
        default:
          throw new UnsupportedConstructException(literal.getType() + " literals");
      }
      return builder;
    }

    private void appendDate(String text) {
      builder
          .append("CAST(")
          .append(Identifiers.literal(DateTimeRules.dateLiteral(text)))
          .append(" AS DATE)");
    }

    private void appendTimestamp(String text, boolean withTimeZone) {
      builder
          .append("CAST(")
          .append(
              DateTimeRules.timestampLiteral(
                  text, withTimeZone, writer.context.zone(), writer.context.precision()))
          .append(" AS TIMESTAMP)");
    }

    /**
     * {@code (current_schemas(b))[n]} (pgJDBC's {@code getSchemas}): the n-th schema of the
     * effective search path, with {@code pg_catalog} first if implicit schemas are included. Other
     * array subscripts are refused.
     */
    @Override
    public <S> StringBuilder visit(net.sf.jsqlparser.expression.ArrayExpression array, S context) {
      Expression object = array.getObjExpression();
      while (object
              instanceof net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList
          && ((net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList<?>)
                      object)
                  .size()
              == 1) {
        object =
            (Expression)
                ((net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList<?>)
                        object)
                    .get(0);
      }
      if (object instanceof Function
          && array.getStartIndexExpression() == null
          && array.getIndexExpression() instanceof net.sf.jsqlparser.expression.LongValue) {
        Function function = (Function) object;
        List<String> parts = function.getMultipartName();
        String name = Identifiers.fold(parts.get(parts.size() - 1));
        if (name.equals("current_schemas")
            && function.getParameters() != null
            && function.getParameters().size() == 1
            && function.getParameters().get(0) instanceof BooleanValue) {
          List<String> schemas = new ArrayList<>();
          if (((BooleanValue) function.getParameters().get(0)).getValue()) {
            schemas.add("pg_catalog");
          }
          for (String schema : writer.context.searchPath()) {
            if (!schemas.contains(schema)) {
              schemas.add(schema);
            }
          }
          long index =
              ((net.sf.jsqlparser.expression.LongValue) array.getIndexExpression()).getValue();
          builder.append(
              index >= 1 && index <= schemas.size()
                  ? Identifiers.literal(schemas.get((int) index - 1))
                  : "CAST(NULL AS STRING)");
          return builder;
        }
      }
      throw new UnsupportedConstructException("array subscripts");
    }

    /**
     * JSqlParser reads {@code day}, {@code month}, ... as date units where PostgreSQL sees column
     * names; outside EXTRACT and INTERVAL (handled before) they are columns.
     */
    @Override
    public <S> StringBuilder visit(
        net.sf.jsqlparser.expression.DateUnitExpression unit, S context) {
      builder.append(Identifiers.quote(unit.toString().toLowerCase(Locale.ROOT)));
      return builder;
    }

    /** {@code a || b}: NULL if either is NULL (the engine's concat would skip it). */
    @Override
    public <S> StringBuilder visit(
        net.sf.jsqlparser.expression.operators.arithmetic.Concat concat, S context) {
      builder.append("CASE WHEN ");
      concat.getLeftExpression().accept(this, context);
      builder.append(" IS NULL OR ");
      concat.getRightExpression().accept(this, context);
      builder.append(" IS NULL THEN CAST(NULL AS STRING) ELSE concat(");
      concat.getLeftExpression().accept(this, context);
      builder.append(", ");
      concat.getRightExpression().accept(this, context);
      builder.append(") END");
      return builder;
    }

    private void appendCurrentSchema() {
      for (String schema : writer.context.searchPath()) {
        builder.append(Identifiers.literal(schema));
        return;
      }
      builder.append("NULL");
    }

    private static boolean isQuoted(String identifier) {
      return identifier != null && identifier.startsWith("\"");
    }
  }
}
