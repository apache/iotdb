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

import fmpp.setting.Settings

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

// Each Maven module owns its output and uses a unique temporary directory.
def codegen = Path.of(properties['fmpp.codegen.directory'].toString()).toAbsolutePath()
def output = Path.of(properties['fmpp.output.directory'].toString()).toAbsolutePath()
Files.createDirectories(output)
def temporary = Files.createTempDirectory(output.parent, 'fmpp-')
try {
    def settings = new Settings(codegen.toFile())
    settings.set(Settings.NAME_SOURCE_ROOT, codegen.resolve('templates').toString())
    settings.set(Settings.NAME_OUTPUT_ROOT, temporary.toString())
    settings.load(codegen.resolve('config.fmpp').toFile())
    settings.execute()

    // Publish only after every template succeeds; unchanged files retain their timestamps.
    Files.walk(temporary).withCloseable { paths ->
        paths.filter { Files.isRegularFile(it) }.forEach { generated ->
            def destination = output.resolve(temporary.relativize(generated))
            if (!Files.exists(destination) || Files.mismatch(generated, destination) != -1) {
                Files.createDirectories(destination.parent)
                Files.move(generated, destination, StandardCopyOption.REPLACE_EXISTING)
            }
        }
    }
} finally {
    Files.walk(temporary).withCloseable { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
    }
}
