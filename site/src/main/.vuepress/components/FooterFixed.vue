/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

<template>
<div style="margin-top:120px; 
    height: 10px;
    text-align: center;
    line-height: 50px;
    width: 100%;
    ">
   
       <p>Copyright © 2020 The Apache Software Foundation.<br>
       Apache and the Apache feather logo are trademarks of The Apache Software Foundation</p>
        <p style="text-align:justify!important;">
        Disclaimer: Apache IoTDB (incubating) (Database for Internet of Things) is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.
      </p>

</div>
</template>

<script>
  export default {
    name: 'FooterFixed',
    year: "",
    created() {
    }
  }
</script>

<style>
.footer-p{
  text-align: center;
  line-height: 5px;
}
</style>
