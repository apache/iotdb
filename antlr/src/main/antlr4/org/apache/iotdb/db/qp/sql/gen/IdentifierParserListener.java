// Generated from /Users/lly/Documents/SE/Projects/github-community/iotdb/iotdb/antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IdentifierParser.g4 by ANTLR 4.10.1
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link IdentifierParser}.
 */
public interface IdentifierParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link IdentifierParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(IdentifierParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link IdentifierParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(IdentifierParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link IdentifierParser#keyWords}.
	 * @param ctx the parse tree
	 */
	void enterKeyWords(IdentifierParser.KeyWordsContext ctx);
	/**
	 * Exit a parse tree produced by {@link IdentifierParser#keyWords}.
	 * @param ctx the parse tree
	 */
	void exitKeyWords(IdentifierParser.KeyWordsContext ctx);
}