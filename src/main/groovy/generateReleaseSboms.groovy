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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.time.Instant
import java.util.zip.ZipFile

import static java.nio.charset.StandardCharsets.UTF_8
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING

static String sha256(File archive) {
    def digest = MessageDigest.getInstance('SHA-256')
    archive.eachByte(1024 * 1024) { bytes, length -> digest.update(bytes, 0, length) }
    return HexFormat.of().formatHex(digest.digest())
}

static Map sourceSbom(File zip, Map archive) {
    def aggregate = new File(zip.parentFile, 'apache-iotdb-parent-' + archive.version + '-sbom.json')
    def bom = new JsonSlurper().parse(aggregate, 'UTF-8')
    def maven = bom.metadata?.component
    if (bom.bomFormat != 'CycloneDX' || maven?.group != 'org.apache.iotdb'
            || maven.name != 'iotdb-parent' || maven.version != archive.version || !maven['bom-ref']) {
        throw new IllegalStateException(aggregate.path + ' is not the Maven aggregate SBOM for this release')
    }
    archive.description = 'IoTDB source archive and its Maven build dependencies.'
    // Preserve the existing Maven project's identity and dependency graph under the ZIP subject.
    bom.components = bom.components ?: []
    if (!bom.components.any { it['bom-ref'] == maven['bom-ref'] }) {
        bom.components.add(0, maven)
    }
    bom.metadata.component = archive
    bom.dependencies = bom.dependencies ?: []
    bom.dependencies.add(0, [ref: archive['bom-ref'], dependsOn: [maven['bom-ref']]])
    return bom
}

static Map readAssignments(String contents, boolean cmakeCache) {
    def values = [:]
    contents.eachLine { line ->
        int separator = line.indexOf('=')
        if (separator >= 0 && !line.startsWith('#') && !line.startsWith('//')) {
            def key = line.substring(0, separator)
            if (cmakeCache) {
                key = key.takeBefore(':')
            }
            values[key] = line.substring(separator + 1).trim()
        }
    }
    return values
}

static boolean cmakeEnabled(String value) {
    return value?.toUpperCase(Locale.ROOT) in ['1', 'ON', 'YES', 'TRUE', 'Y']
}

static String boostVersion(String includeDirectory) {
    if (!includeDirectory) {
        throw new IllegalStateException('CMakeCache.txt is missing the resolved BOOST_INCLUDE_DIR')
    }
    def header = Path.of(includeDirectory, 'boost', 'version.hpp')
    def macro = Files.readString(header, UTF_8) =~ /(?m)^\s*#\s*define\s+BOOST_VERSION\s+(\d+)\b/
    if (!macro.find()) {
        throw new IllegalStateException('Could not read BOOST_VERSION from ' + header)
    }
    int encoded = macro.group(1).toInteger()
    return [encoded.intdiv(100000), encoded.intdiv(100) % 1000, encoded % 100].join('.')
}

static Map nativeComponent(String repository, String name, String version, String license, String supplier) {
    def reference = 'pkg:github/' + repository + '@' + URLEncoder.encode(version, UTF_8).replace('+', '%20')
    return [
            type: 'library',
            'bom-ref': reference,
            supplier: [name: supplier],
            name: name,
            version: version,
            purl: reference,
            licenses: [[license: [id: license]]],
    ]
}

static Map cppSbom(ZipFile zip, Map archive, Path cache) {
    def entries = Collections.list(zip.entries()).findAll { !it.directory }
    def manifests = entries.findAll { it.name ==~ /(?:[^\/]+\/)?BUILD-INFO\.txt/ }
    if (manifests.size() != 1) {
        throw new IllegalStateException('Expected one C++ BUILD-INFO.txt at the package root')
    }
    def manifest = manifests[0]
    byte[] info = zip.getInputStream(manifest).withCloseable { it.bytes }
    def build = readAssignments(new String(info, UTF_8), false)
    if (build.version != archive.version ||
            !Arrays.equals(info, Files.readAllBytes(cache.parent.resolve('package-metadata/BUILD-INFO.txt')))) {
        throw new IllegalStateException('C++ BUILD-INFO.txt does not match the selected CMake build')
    }

    def settings = readAssignments(Files.readString(cache, UTF_8), true)
    if (!settings.THRIFT_VERSION) {
        throw new IllegalStateException('CMakeCache.txt is missing THRIFT_VERSION')
    }
    def components = [
            nativeComponent('apache/thrift', 'Apache Thrift', settings.THRIFT_VERSION,
                    'Apache-2.0', 'Apache Software Foundation'),
            nativeComponent('boostorg/boost', 'Boost', boostVersion(settings.BOOST_INCLUDE_DIR),
                    'BSL-1.0', 'Boost'),
    ]
    def libraryPrefix = manifest.name.takeBefore('BUILD-INFO.txt') + 'lib/'
    def bundledSsl = entries.any { entry ->
        entry.name.startsWith(libraryPrefix) && entry.name.substring(libraryPrefix.length()) ==~
                /(?i)lib(?:ssl|crypto)(?:[.-][\w.-]+)?\.(?:a|dll|dylib|so(?:\.\d+)*)/
    }
    def staticSsl = cmakeEnabled(build['with.ssl']) &&
            (['OPENSSL_SSL_LIBRARY', 'OPENSSL_CRYPTO_LIBRARY'].any {
                settings[it]?.toLowerCase(Locale.ROOT)?.endsWith('.a')
            } || (System.getProperty('os.name').startsWith('Windows') &&
                    cmakeEnabled(settings.OPENSSL_USE_STATIC_LIBS)))
    if (bundledSsl || staticSsl) {
        def version = build['openssl.version'] ?: ''
        def major = version =~ /^(\d+)\.\d+\.\d+/
        if (!major.find()) {
            throw new IllegalStateException('C++ BUILD-INFO.txt is missing the packaged OpenSSL version')
        }
        def license = major.group(1).toInteger() >= 3 ? 'Apache-2.0' : 'OpenSSL'
        components.add(nativeComponent('openssl/openssl', 'OpenSSL', version, license, 'OpenSSL Project'))
    } else if (build['openssl.provider'] == 'pinned-source') {
        throw new IllegalStateException('C++ ZIP is missing the OpenSSL runtime libraries from the build')
    }
    return [
            '$schema': 'http://cyclonedx.org/schema/bom-1.6.schema.json',
            bomFormat: 'CycloneDX',
            specVersion: '1.6',
            version: 1,
            metadata: [
                    timestamp: Instant.now().toString(),
                    tools: [[vendor: 'Apache Software Foundation', name: 'IoTDB release SBOM generator']],
                    component: archive,
            ],
            components: components,
            dependencies: [[ref: archive['bom-ref'], dependsOn: components.collect { it['bom-ref'] }]],
    ]
}

static Map binarySbom(ZipFile zip, Map archive) {
    def directory = Files.createTempDirectory('iotdb-release-zip-')
    try {
        zip.entries().each { entry ->
            def destination = directory.resolve(entry.name).normalize()
            if (!destination.startsWith(directory)) {
                throw new IllegalStateException('ZIP entry is outside the archive root: ' + entry.name)
            }
            if (entry.directory) {
                Files.createDirectories(destination)
            } else {
                Files.createDirectories(destination.parent)
                zip.getInputStream(entry).withCloseable { Files.copy(it, destination) }
            }
        }
        def scanner = new ProcessBuilder(
                'syft', 'scan', '--from', 'dir', '--override-default-catalogers',
                'java-archive-cataloger,python-installed-package-cataloger',
                '--source-name', archive.name, '--source-version', archive.version,
                '--output', 'cyclonedx-json@1.6', directory.toString())
        scanner.environment().put('SYFT_CHECK_FOR_APP_UPDATE', 'false')
        scanner.environment().put('SYFT_JAVA_USE_NETWORK', 'false')
        def process = scanner.redirectError(ProcessBuilder.Redirect.INHERIT).start()
        def raw
        try {
            raw = process.inputStream.getText('UTF-8')
            if (process.waitFor() != 0) {
                throw new IllegalStateException('Syft could not inventory ' + archive.name)
            }
        } finally {
            process.destroy()
        }
        def bom = new JsonSlurper().parseText(raw)
        if (!bom.components) {
            throw new IllegalStateException('Syft did not identify any components in ' + archive.name)
        }
        archive['bom-ref'] = bom.metadata.component['bom-ref'] ?: archive['bom-ref']
        bom.metadata.component = archive
        return bom
    } finally {
        directory.toFile().deleteDir()
    }
}

static boolean isCppZip(String name) {
    return name.startsWith('iotdb-session-cpp-') || name.contains('-client-cpp-')
}

static void generateSbom(File zip, Path cppCache) {
    def name = zip.name =~ /(?:apache-iotdb|iotdb-session-cpp)-(\d+\.\d+\.\d+(?:-SNAPSHOT)?)-.+\.zip/
    if (!name.matches()) {
        throw new IllegalStateException('Expected an IoTDB release ZIP: ' + zip)
    }
    def path = zip.toPath()
    def before = Files.readAttributes(path, BasicFileAttributes)
    def checksum = sha256(zip)
    def archive = [
            type: 'file',
            'bom-ref': 'urn:sha256:' + checksum,
            name: zip.name,
            version: name.group(1),
            supplier: [name: 'Apache Software Foundation'],
            licenses: [[license: [id: 'Apache-2.0']]],
            hashes: [[alg: 'SHA-256', content: checksum]],
    ]
    def bom = new ZipFile(zip).withCloseable { contents ->
        if (zip.name.endsWith('-source-release.zip')) {
            return sourceSbom(zip, archive)
        }
        return isCppZip(zip.name) ? cppSbom(contents, archive, cppCache) : binarySbom(contents, archive)
    }
    def after = Files.readAttributes(path, BasicFileAttributes)
    if (before.size() != after.size() || before.lastModifiedTime() != after.lastModifiedTime()
            || before.fileKey() != after.fileKey()) {
        throw new IllegalStateException('ZIP changed while its SBOM was being generated: ' + zip)
    }
    // Source Maven metadata covers one ecosystem, and binaries can contain code without metadata.
    bom.compositions = bom.compositions ?: []
    bom.compositions.add([aggregate: 'incomplete', assemblies: [archive['bom-ref']]])

    def output = path.resolveSibling(zip.name + '.cdx.json')
    def temporary = Files.createTempFile(output.parent, '.iotdb-release-sbom-', '.json')
    try {
        Files.writeString(temporary, JsonOutput.prettyPrint(JsonOutput.toJson(bom)) + '\n', UTF_8)
        try {
            Files.move(temporary, output, ATOMIC_MOVE, REPLACE_EXISTING)
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(temporary, output, REPLACE_EXISTING)
        }
    } finally {
        Files.deleteIfExists(temporary)
    }
    println output
}

// Only scan this invocation's attachments. AINode needs its own PyInstaller build inventory.
def archives = project.attachedArtifacts.findAll {
    it.type == 'zip' && !it.file.name.endsWith('-ainode-bin.zip')
}.collect { it.file }
Path cppCache = null
if (archives.any { isCppZip(it.name) }) {
    def siblingBuild = new File(project.basedir, '../iotdb-client/client-cpp/target/build')
    def cmakeBuild = properties.getProperty('cmake.project.dir', siblingBuild.absolutePath)
    cppCache = Path.of(cmakeBuild, 'CMakeCache.txt')
}
archives.each { generateSbom(it, cppCache) }
