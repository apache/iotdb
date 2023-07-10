// Generated from /Users/lly/Documents/SE/Projects/github-community/iotdb/iotdb/iotdb-core/antlr/src/main/antlr4/org/apache/iotdb/db/qp/sql/IdentifierParser.g4 by ANTLR 4.10.1
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;

/**
 * This class provides an empty implementation of {@link IdentifierParserVisitor},
 * which can be extended to create a visitor which only needs to handle a subset
 * of the available methods.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public class IdentifierParserBaseVisitor<T> extends AbstractParseTreeVisitor<T> implements IdentifierParserVisitor<T> {
	/**
	 * {@inheritDoc}
	 *
	 * <p>The default implementation returns the result of calling
	 * {@link #visitChildren} on {@code ctx}.</p>
	 */
	@Override public T visitIdentifier(IdentifierParser.IdentifierContext ctx) { return visitChildren(ctx); }
	/**
	 * {@inheritDoc}
	 *
	 * <p>The default implementation returns the result of calling
	 * {@link #visitChildren} on {@code ctx}.</p>
	 */
	@Override public T visitKeyWords(IdentifierParser.KeyWordsContext ctx) { return visitChildren(ctx); }
}