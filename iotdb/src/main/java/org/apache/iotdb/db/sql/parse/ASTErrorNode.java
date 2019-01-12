
package org.apache.iotdb.db.sql.parse;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonErrorNode;

public class ASTErrorNode extends ASTNode {

	private static final long serialVersionUID = 1L;
	CommonErrorNode delegate;

	public ASTErrorNode(TokenStream input, Token start, Token stop, RecognitionException e) {
		delegate = new CommonErrorNode(input, start, stop, e);
	}

	@Override
	public boolean isNil() {
		return delegate.isNil();
	}

	@Override
	public int getType() {
		return delegate.getType();
	}

	@Override
	public String getText() {
		return delegate.getText();
	}

	@Override
	public String toString() {
		return delegate.toString();
	}
}
