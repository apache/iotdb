/*
 * Copyright 2007-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Properties;

public class MavenWrapperDownloader {

  /**
   * Default URL to download the maven-wrapper.jar from, if no 'downloadUrl' is provided.
   */
  private static final String DEFAULT_DOWNLOAD_URL =
      "https://repo1.maven.org/maven2/io/takari/maven-wrapper/0.5.3/maven-wrapper-0.5.3.jar";

  /**
   * Path to the maven-wrapper.properties file, which might contain a downloadUrl property to
   * use instead of the default one.
   */
  private static final String MAVEN_WRAPPER_PROPERTIES_PATH =
      ".mvn/wrapper/maven-wrapper.properties";

  /**
   * Path where the maven-wrapper.jar will be saved to.
   */
  private static final String MAVEN_WRAPPER_JAR_PATH =
      ".mvn/wrapper/maven-wrapper.jar";

  /**
   * Name of the property which should be used to override the default download url for the wrapper.
   */
  private static final String PROPERTY_NAME_WRAPPER_URL = "wrapperUrl";

  public static void main(String args[]) {
    File baseDirectory = new File(args[0]);

    // If the maven-wrapper.properties exists, read it and check if it contains a custom
    // wrapperUrl parameter.
    File mavenWrapperPropertyFile = new File(baseDirectory, MAVEN_WRAPPER_PROPERTIES_PATH);
    String url = DEFAULT_DOWNLOAD_URL;
    if (mavenWrapperPropertyFile.exists()) {
      FileInputStream mavenWrapperPropertyFileInputStream = null;
      try {
        mavenWrapperPropertyFileInputStream = new FileInputStream(mavenWrapperPropertyFile);
        Properties mavenWrapperProperties = new Properties();
        mavenWrapperProperties.load(mavenWrapperPropertyFileInputStream);
        url = mavenWrapperProperties.getProperty(PROPERTY_NAME_WRAPPER_URL, url);
      } catch (IOException e) {
      } finally {
        try {
          if (mavenWrapperPropertyFileInputStream != null) {
            mavenWrapperPropertyFileInputStream.close();
          }
        } catch (IOException e) {
          // Ignore ...
        }
      }
    }

    File outputFile = new File(baseDirectory.getAbsolutePath(), MAVEN_WRAPPER_JAR_PATH);
    if (!outputFile.getParentFile().exists()) {
      if (!outputFile.getParentFile().mkdirs()) {
        System.out.println(
            "- ERROR creating output direcrory '" + outputFile.getParentFile().getAbsolutePath()
                + "'");
      }
    }
    System.out.println("- Downloading to: " + outputFile.getAbsolutePath());
    try {
      downloadFileFromURL(url, outputFile);
      System.out.println("Done");
      System.exit(0);
    } catch (Throwable e) {
      System.out.println("- Error downloading");
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void downloadFileFromURL(String urlString, File destination) throws Exception {
    URL website = new URL(urlString);
    ReadableByteChannel rbc;
    rbc = Channels.newChannel(website.openStream());
    FileOutputStream fos = new FileOutputStream(destination);
    fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
    fos.close();
    rbc.close();
  }

}
