package cn.edu.tsinghua.tsfile.read.expression.impl;

import cn.edu.tsinghua.tsfile.read.expression.IBinaryExpression;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.ExpressionType;


public abstract class BinaryExpression implements IBinaryExpression {

    protected static class AndExpression extends BinaryExpression {
        public IExpression left;
        public IExpression right;

        public AndExpression(IExpression left, IExpression right){
            this.left = left;
            this.right = right;
        }

        @Override
        public IExpression getLeft() {
            return left;
        }

        @Override
        public IExpression getRight() {
            return right;
        }

        @Override
        public ExpressionType getType() {
            return ExpressionType.AND;
        }

        @Override
        public String toString() {
            return "[" + left + " && " + right + "]";
        }
    }

    protected static class OrExpression extends BinaryExpression {
        public IExpression left;
        public IExpression right;
        public OrExpression(IExpression left, IExpression right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public IExpression getLeft() {
            return left;
        }
        @Override
        public IExpression getRight() {
            return right;
        }

        @Override
        public ExpressionType getType() {
            return ExpressionType.OR;
        }

        @Override
        public String toString() {
            return "[" + left + " || " + right + "]";
        }
    }

    public static AndExpression and(IExpression left, IExpression right){
        return new AndExpression(left, right);
    }

    public static OrExpression or(IExpression left, IExpression right) {
        return new OrExpression(left, right);
    }
}
