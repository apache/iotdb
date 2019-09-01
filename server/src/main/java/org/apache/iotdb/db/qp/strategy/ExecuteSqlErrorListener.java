package org.apache.iotdb.db.qp.strategy;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.apache.iotdb.db.sql.parse.SqlParseException;

public class ExecuteSqlErrorListener extends BaseErrorListener {

  @Override
  public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
    String out = "line " + line + ": " + charPositionInLine  + " at " + offendingSymbol + ":" +msg + "\n";
    out += "Please refer to SQL document and check if there is any keyword conflict.";
    throw new SqlParseException(out);
  }
}
