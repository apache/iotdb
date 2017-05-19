package com.corp.delta.sql.src.ql.parse;

/**
 * ASTNodeOrigin contains contextual information about the object from whose
 * definition a particular ASTNode originated. For example, suppose a view v is
 * defined as <code>select x+1 as y from t</code>, and we're processing a query
 * <code>select v1.y from v as v1</code>, and there's a type-checking problem
 * with the expression <code>x+1</code> due to an ALTER TABLE on t subsequent to
 * the creation of v. Then, when reporting the error, we want to provide the
 * parser location with respect to the definition of v (rather than with respect
 * to the top-level query, since that represents a completely different
 * "parser coordinate system").
 * 
 *<p>
 * 
 * So, when expanding the definition of v while analyzing the top-level query,
 * we tag each ASTNode with a reference to an ASTNodeOrigin describing v and its
 * usage within the query.
 */
public class ASTNodeOrigin {
  private final String objectType;
  private final String objectName;
  private final String objectDefinition;
  private final String usageAlias;
  private final ASTNode usageNode;

  public ASTNodeOrigin(String objectType, String objectName,
      String objectDefinition, String usageAlias, ASTNode usageNode) {
    this.objectType = objectType;
    this.objectName = objectName;
    this.objectDefinition = objectDefinition;
    this.usageAlias = usageAlias;
    this.usageNode = usageNode;
  }

  /**
   * @return the type of the object from which an ASTNode originated, e.g.
   *         "view".
   */
  public String getObjectType() {
    return objectType;
  }

  /**
   * @return the name of the object from which an ASTNode originated, e.g. "v".
   */
  public String getObjectName() {
    return objectName;
  }

  /**
   * @return the definition of the object from which an ASTNode originated, e.g.
   *         <code>select x+1 as y from t</code>.
   */
  public String getObjectDefinition() {
    return objectDefinition;
  }

  /**
   * @return the alias of the object from which an ASTNode originated, e.g. "v1"
   *         (this can help with debugging context-dependent expansions)
   */
  public String getUsageAlias() {
    return usageAlias;
  }

  /**
   * @return the expression node triggering usage of an object from which an
   *         ASTNode originated, e.g. <code>v as v1</code> (this can help with
   *         debugging context-dependent expansions)
   */
  public ASTNode getUsageNode() {
    return usageNode;
  }
}

