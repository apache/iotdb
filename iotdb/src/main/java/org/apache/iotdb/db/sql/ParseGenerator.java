
package org.apache.iotdb.db.sql;


import org.apache.iotdb.db.sql.parse.ASTNode;
import org.apache.iotdb.db.sql.parse.ParseDriver;
import org.apache.iotdb.db.sql.parse.ParseException;
import org.apache.iotdb.db.sql.parse.ASTNode;
import org.apache.iotdb.db.sql.parse.ParseDriver;
import org.apache.iotdb.db.sql.parse.ParseException;

/**
 * ParseContextGenerator is a class that offers methods to generate ASTNode Tree
 *
 */
public final class ParseGenerator {

  /**
   * Parse the input {@link String} command and generate an ASTNode Tree.
   */
  public static ASTNode generateAST(String command) throws ParseException {
    ParseDriver pd = new ParseDriver();
    return pd.parse(command);
   }

}
