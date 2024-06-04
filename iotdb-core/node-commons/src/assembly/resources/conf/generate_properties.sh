#!/bin/sh

source_file="src/assembly/resources/conf/iotdb-system.properties"
target_template_file="target/conf/iotdb-system.properties.template"
target_properties_file="target/conf/iotdb-system.properties"

if [ -f "$target_template_file" ]; then
    rm "$target_template_file"
fi

if [ -f "$target_properties_file" ]; then
    rm "$target_properties_file"
fi

mkdir -p "$(dirname "$target_template_file")"

cp "$source_file" "$target_template_file"

grep -v '^\s*#' "$target_template_file" | grep -v '^\s*$' > "$target_properties_file"
