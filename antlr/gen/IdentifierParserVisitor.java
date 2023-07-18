// Generated from /Users/lly/Documents/SE/Projects/github-community/iotdb/iotdb/antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IdentifierParser.g4 by ANTLR 4.10.1
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link IdentifierParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface IdentifierParserVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link IdentifierParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(IdentifierParser.IdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link IdentifierParser#keyWords}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitKeyWords(IdentifierParser.KeyWordsContext ctx);
}