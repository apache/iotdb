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
#include <stdio.h>
#include <curl/curl.h>
#include <string.h>
#include "base64.h"

char* author;
char authorization1[100] = "Authorization:Basic ";
char authorization2[100] = "Authorization:Basic ";
char authorization3[100] = "Authorization:Basic ";

// In addition to the check live interface '/ping'，
// RESTful service use the basic authorization，every URL request needs to carry 'Authorization': 'Basic ' + base64.encode(username + ':' + password) in the header.
void ping() {
    // curl http://127.0.0.1:18080/ping
    CURL *curl_handle = curl_easy_init();
    if (curl_handle == NULL) {
        fprintf(stderr, "curl_handle == NULL\n");
    }

    curl_easy_setopt(curl_handle, CURLOPT_URL, "http://127.0.0.1:18080/ping");
    CURLcode res = curl_easy_perform(curl_handle);
    if(res != CURLE_OK) {
        fprintf(stderr, "curl_easy_perform() execute fail, the reason is %s\n", curl_easy_strerror(res));
    }
    curl_easy_cleanup(curl_handle);
}

void query(char * sql_str) {
    //curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"show functions"}' http://127.0.0.1:18080/rest/v1/query
    CURL *curl_handle = curl_easy_init();
    if (curl_handle == NULL) {
        fprintf(stderr, "curl_handle == NULL\n");
    }

    // set the header
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type:application/json");
    headers = curl_slist_append(headers, authorization1);
    curl_easy_setopt(curl_handle, CURLOPT_URL, "http://127.0.0.1:18080/rest/v1/query");
    curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, sql_str);
    curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, headers);
    CURLcode res = curl_easy_perform(curl_handle);
    if(res != CURLE_OK) {
        fprintf(stderr, "curl_easy_perform() execute fail, the reason is %s\n", curl_easy_strerror(res));
    }
    curl_easy_cleanup(curl_handle);
}

void nonQuery(char* sql_str) {
    //curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"sql":"CREATE DATABASE root.ln"}' http://127.0.0.1:18080/rest/v1/nonQuery
    CURL *curl_handle = curl_easy_init();
    if (curl_handle == NULL) {
        fprintf(stderr, "curl_handle == NULL\n");
    }

    // set the header
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type:application/json");
    headers = curl_slist_append(headers, authorization2);
    curl_easy_setopt(curl_handle, CURLOPT_URL, "http://127.0.0.1:18080/rest/v1/nonQuery");
    curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, sql_str);
    curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, headers);
    CURLcode res = curl_easy_perform(curl_handle);
    if(res != CURLE_OK) {
        fprintf(stderr, "curl_easy_perform() execute fail, the reason is %s\n", curl_easy_strerror(res));
    }
    curl_easy_cleanup(curl_handle);
}

void insertTablet(char * sql_str) {
    // curl -H "Content-Type:application/json" -H "Authorization:Basic cm9vdDpyb290" -X POST --data '{"timestamps":[1635232143960,1635232153960],"measurements":["s3","s4"],"dataTypes":["INT32","BOOLEAN"],"values":[[11,null],[false,true]],"isAligned":false,"deviceId":"root.sg27"}' http://127.0.0.1:18080/rest/v1/insertTablet
    CURL *curl_handle = curl_easy_init();
    if (curl_handle == NULL) {
        fprintf(stderr, "curl_handle == NULL\n");
    }

    // set the header
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type:application/json");
    headers = curl_slist_append(headers, authorization3);
    curl_easy_setopt(curl_handle, CURLOPT_URL, "http://127.0.0.1:18080/rest/v1/insertTablet");
    curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, sql_str);
    curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, headers);
    CURLcode res = curl_easy_perform(curl_handle);
    if(res != CURLE_OK) {
        fprintf(stderr, "curl_easy_perform() execute fail, the reason is %s\n", curl_easy_strerror(res));
    }
    curl_easy_cleanup(curl_handle);
}

int main() {
    author = base64_encode("root:root");    // username:password
    printf("%s\n", author);
    strcat(authorization1, author);
    strcat(authorization2, author);
    strcat(authorization3, author);

    curl_global_init(CURL_GLOBAL_ALL);
    ping();
    printf("\n");
    query("{\"sql\":\"show functions\"}");
    printf("\n");
    nonQuery("{\"sql\":\"CREATE DATABASE root.lns\"}");
    printf("\n");
    insertTablet("{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"s3\",\"s4\"],\"dataTypes\":[\"INT32\",\"BOOLEAN\"],\"values\":[[11,null],[false,true]],\"isAligned\":false,\"deviceId\":\"root.lns.d1\"}");
    printf("\n");
    query("{\"sql\":\"select s3, s4 from root.lns.d1\"}");
    printf("\n");
    return 0;
}
