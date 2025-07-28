<?xml version="1.0" encoding="UTF-8"?>
<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
<xsl:stylesheet version="2.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:sbom="http://cyclonedx.org/schema/bom/1.4">
    
    <xsl:output
        method="text"
        indent="no"
        encoding="utf-8"
    />
    
    <xsl:template match="/">{
    "dependencies": [<xsl:for-each select="sbom:bom/sbom:components/sbom:component[sbom:group!='org.apache.iotdb']">
    <xsl:sort select="sbom:group"/>
    <xsl:sort select="sbom:name"/>
        "<xsl:value-of select="sbom:group"/>:<xsl:value-of select="sbom:name"/>"<xsl:if test="position() != last()">,</xsl:if>
</xsl:for-each>
    ]
}
    </xsl:template>
</xsl:stylesheet>