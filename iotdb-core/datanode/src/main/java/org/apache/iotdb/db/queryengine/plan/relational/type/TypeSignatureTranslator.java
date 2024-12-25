/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.type;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataTypeParameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericDataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NumericParameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TypeParameter;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.common.type.Type;

import java.util.Collections;
import java.util.Locale;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureParameter.numericParameter;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureParameter.typeParameter;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureParameter.typeVariable;

public class TypeSignatureTranslator {

  private TypeSignatureTranslator() {}

  public static DataType toSqlType(Type type) {
    return new GenericDataType(
        new Identifier(type.getTypeEnum().name(), false), Collections.emptyList());
  }

  public static TypeSignature toTypeSignature(DataType type) {
    return toTypeSignature(type, Collections.emptySet());
  }

  private static TypeSignature toTypeSignature(DataType type, Set<String> typeVariables) {
    if (type instanceof GenericDataType) {
      return toTypeSignature((GenericDataType) type, typeVariables);
    }

    throw new UnsupportedOperationException("Unsupported DataType: " + type.getClass().getName());
  }

  private static TypeSignature toTypeSignature(GenericDataType type, Set<String> typeVariables) {
    ImmutableList.Builder<TypeSignatureParameter> parameters = ImmutableList.builder();

    //    if (type.getName().getValue().equalsIgnoreCase(VARCHAR) && type.getArguments().isEmpty())
    // {
    //      // We treat VARCHAR specially because currently, the unbounded VARCHAR type is modeled
    // in the system as a VARCHAR(n) with a "magic" length
    //      // TODO: Eventually, we should split the types into VARCHAR and VARCHAR(n)
    //      return VarcharType.VARCHAR.getTypeSignature();
    //    }

    checkArgument(
        !typeVariables.contains(type.getName().getValue()),
        "Base type name cannot be a type variable");

    for (DataTypeParameter parameter : type.getArguments()) {
      if (parameter instanceof NumericParameter) {
        String value = ((NumericParameter) parameter).getValue();
        try {
          parameters.add(numericParameter(Long.parseLong(value)));
        } catch (NumberFormatException e) {
          throw new SemanticException(String.format("Invalid type parameter: %s", value));
        }
      } else if (parameter instanceof TypeParameter) {
        DataType value = ((TypeParameter) parameter).getValue();
        if (value instanceof GenericDataType
            && ((GenericDataType) value).getArguments().isEmpty()
            && typeVariables.contains(((GenericDataType) value).getName().getValue())) {
          parameters.add(typeVariable(((GenericDataType) value).getName().getValue()));
        } else {
          parameters.add(typeParameter(toTypeSignature(value, typeVariables)));
        }
      } else {
        throw new UnsupportedOperationException(
            "Unsupported type parameter kind: " + parameter.getClass().getName());
      }
    }

    return new TypeSignature(canonicalize(type.getName()), parameters.build());
  }

  private static String canonicalize(Identifier identifier) {
    if (identifier.isDelimited()) {
      return identifier.getValue();
    }

    return identifier
        .getValue()
        .toLowerCase(Locale.ENGLISH); // TODO: make this toUpperCase to match standard SQL semantics
  }
}
