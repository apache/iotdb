/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.read.expression.util;

import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;

public class ExpressionPrinter {

    private static final int MAX_DEPTH = 100;
    private static final char PREFIX_CHAR = '\t';
    private static final String[] PREFIX = new String[MAX_DEPTH];

    static {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < MAX_DEPTH; i++) {
            PREFIX[i] = stringBuilder.toString();
            stringBuilder.append(PREFIX_CHAR);
        }
    }

    public static void print(IExpression IExpression) {
        print(IExpression, 0);
    }

    private static void print(IExpression IExpression, int level) {
        if (IExpression instanceof IUnaryExpression) {
            System.out.println(getPrefix(level) + IExpression);
        } else {
            System.out.println(getPrefix(level) + IExpression.getType() + ":");
            print(((IBinaryExpression) IExpression).getLeft(), level + 1);
            print(((IBinaryExpression) IExpression).getRight(), level + 1);
        }
    }

    private static String getPrefix(int count) {
        if (count < MAX_DEPTH) {
            return PREFIX[count];
        } else {
            return PREFIX[MAX_DEPTH - 1];
        }
    }
}
