
package cn.edu.thu.tsfiledb.sql;


import cn.edu.thu.tsfiledb.sql.parse.ASTNode;
import cn.edu.thu.tsfiledb.sql.parse.ParseDriver;
import cn.edu.thu.tsfiledb.sql.parse.ParseException;
import org.antlr.runtime.RecognitionException;


/**
 * ParseContextGenerator is a class that offers methods to generate ASTNode Tree
 *
 */
public final class ParseGenerator {

  /**
   * Parse the input {@link String} command and generate an ASTNode Tree.
   * @param command
   */
  public static ASTNode generateAST(String command) throws ParseException, RecognitionException {
    ParseDriver pd = new ParseDriver();
    ASTNode tree = pd.parse(command);
    
    return tree;
   }

}
