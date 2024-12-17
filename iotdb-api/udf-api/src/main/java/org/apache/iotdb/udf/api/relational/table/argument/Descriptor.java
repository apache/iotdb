package org.apache.iotdb.udf.api.relational.table.argument;

import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class Descriptor {
  private final List<Field> fields;

  public Descriptor(List<Field> fields) {
    requireNonNull(fields, "fields is null");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("descriptor has no fields");
    }
    if (!fields.stream().allMatch(field -> field.getName().isPresent())) {
      throw new IllegalArgumentException("All fields of a descriptor argument must have names");
    }
    this.fields = fields;
  }

  public static Descriptor descriptor(String... names) {
    List<Field> fields =
        Arrays.stream(names)
            .map(name -> new Field(name, Optional.empty()))
            .collect(Collectors.toList());
    return new Descriptor(fields);
  }

  public static Descriptor descriptor(List<String> names, List<Type> types) {
    requireNonNull(names, "names is null");
    requireNonNull(types, "types is null");
    if (names.size() != types.size()) {
      throw new IllegalArgumentException("names and types lists do not match");
    }
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < names.size(); i++) {
      fields.add(new Field(names.get(i), Optional.of(types.get(i))));
    }
    return new Descriptor(fields);
  }

  public List<Field> getFields() {
    return fields;
  }

  public boolean isTyped() {
    return fields.stream().allMatch(field -> field.type.isPresent());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Descriptor that = (Descriptor) o;
    return fields.equals(that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields);
  }

  public static class Field {
    private final Optional<String> name;
    private final Optional<Type> type;

    public Field(String name, Optional<Type> type) {
      this(Optional.of(name), type);
    }

    public Field(Optional<String> name, Optional<Type> type) {
      this.name = requireNonNull(name, "name is null");
      this.type = requireNonNull(type, "type is null");
    }

    public Optional<String> getName() {
      return name;
    }

    public Optional<Type> getType() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Field field = (Field) o;
      return name.equals(field.name) && type.equals(field.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type);
    }
  }
}
