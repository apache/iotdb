package org.apache.iotdb.db.metadata.view.viewExpression;

public enum ViewExpressionType {
  CONSTANT((short) -4, (short) 1400),
  TIMESTAMP((short) -3, (short) 1300),
  TIMESERIES((short) -2, (short) 1200),
  FUNCTION((short) -1, (short) 1100),

  NEGATION((short) 0, (short) 1000),
  LOGIC_NOT((short) 1, (short) 1000),

  MULTIPLICATION((short) 2, (short) 900),
  DIVISION((short) 3, (short) 900),
  MODULO((short) 4, (short) 900),

  ADDITION((short) 5, (short) 800),
  SUBTRACTION((short) 6, (short) 800),

  EQUAL_TO((short) 7, (short) 600),
  NON_EQUAL((short) 8, (short) 600),
  GREATER_EQUAL((short) 9, (short) 600),
  GREATER_THAN((short) 10, (short) 600),
  LESS_EQUAL((short) 11, (short) 600),
  LESS_THAN((short) 12, (short) 600),

  LIKE((short) 13, (short) 500),
  REGEXP((short) 14, (short) 500),

  IS_NULL((short) 15, (short) 475),

  BETWEEN((short) 16, (short) 450),

  IN((short) 17, (short) 400),

  LOGIC_AND((short) 18, (short) 300),

  LOGIC_OR((short) 19, (short) 200),

  NULL((short) 20, (short) 1400),
  ;

  private final short expressionType;
  private final short priority;

  ViewExpressionType(short expressionType, short priority) {
    this.expressionType = expressionType;
    this.priority = priority;
  }

  public short getExpressionTypeInShortEnum() {
    return expressionType;
  }

  public short getPriority() {
    return priority;
  }
}
