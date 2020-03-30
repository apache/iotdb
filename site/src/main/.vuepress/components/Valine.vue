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
  <div> 
    <div id="vcomments"></div>
  </div>

</template>

<script>
export default {
  name: 'Valine',
  mounted: function(){
    // require window 
    const Valine = require('valine');
    if (typeof window !== 'undefined') {
      this.window = window
      window.AV = require('leancloud-storage')
      
    }
     
    new Valine({
      el: '#vcomments' ,
      appId: 'R1Qyeuv2BEooSWSVB2HmJyB7-gzGzoHsz',// your appId
      appKey: 'GGneEbK7wGIhef7CVnUScHH7', // your appKey
      notify:false, 
      verify:false, 
      avatar:'mm', 
      placeholder: 'Please enter comments！' ,
      path:window.location.pathname,
    });
  },
}
</script>
<style>
#vcomments{
  margin-top:100px;
}
</style>