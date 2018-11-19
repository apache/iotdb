
package cn.edu.tsinghua.iotdb.sql;


import cn.edu.tsinghua.iotdb.sql.parse.ASTNode;
import cn.edu.tsinghua.iotdb.sql.parse.ParseDriver;
import cn.edu.tsinghua.iotdb.sql.parse.ParseException;

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
