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

#include "base64.h"

unsigned char *base64_encode(unsigned char *str) {
    long len;
    long str_len;
    unsigned char *res;
    int i,j;
    // define the base64 table
    unsigned char *base64_table = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    // calculate the length of the base64 encoded str
    str_len = strlen(str);
    if (str_len % 3 == 0)
        len = str_len/3*4;
    else
        len = (str_len/3+1)*4;

    res = malloc(sizeof(unsigned char)*len+1);
    res[len] = '\0';

    // encode in groups of three 8-bit characters
    for(i=0,j=0;i<len-2;j+=3,i+=4) {
        res[i] = base64_table[str[j]>>2]; // take out the first 6 digits of the first character and find the corresponding character
        res[i+1] = base64_table[(str[j]&0x3)<<4 | (str[j+1]>>4)]; // combine the last four digits of the first character with the first four digits of the second character and find the corresponding character
        res[i+2] = base64_table[(str[j+1]&0xf)<<2 | (str[j+2]>>6)]; // combine the last 4 digits of the second character with the first 2 digits of the third character and find the corresponding character
        res[i+3] = base64_table[str[j+2]&0x3f]; // take out the last 6 digits of the third character and find the corresponding character
    }

    switch(str_len % 3){
        case 1:
            res[i-2] = '=';
            res[i-1] = '=';
            break;
        case 2:
            res[i-1] = '=';
            break;
    }

    return res;
}

unsigned char *base64_decode(unsigned char *code) {
    // according to the base64 table, find the corresponding decimal data in characters
    int table[] = {0,0,0,0,0,0,0,0,0,0,0,0,
                   0,0,0,0,0,0,0,0,0,0,0,0,
                   0,0,0,0,0,0,0,0,0,0,0,0,
                   0,0,0,0,0,0,0,62,0,0,0,
                   63,52,53,54,55,56,57,58,
                   59,60,61,0,0,0,0,0,0,0,0,
                   1,2,3,4,5,6,7,8,9,10,11,12,
                   13,14,15,16,17,18,19,20,21,
                   22,23,24,25,0,0,0,0,0,0,26,
                   27,28,29,30,31,32,33,34,35,
                   36,37,38,39,40,41,42,43,44,
                   45,46,47,48,49,50,51
    };
    long len;
    long str_len;
    unsigned char *res;
    int i,j;

    // calculates the length of the decoded str
    len = strlen(code);
    // judge whether the encoded str contains '='
    if(strstr(code,"=="))
        str_len = len/4*3-2;
    else if(strstr(code,"="))
        str_len = len/4*3-1;
    else
        str_len = len/4*3;

    res = malloc(sizeof(unsigned char)*str_len+1);
    res[str_len] = '\0';

    for(i=0,j=0;i < len-2;j+=3,i+=4) {
        res[j] = ((unsigned char)table[code[i]])<<2 | (((unsigned char)table[code[i+1]])>>4);
        res[j+1] = (((unsigned char)table[code[i+1]])<<4) | (((unsigned char)table[code[i+2]])>>2);
        res[j+2] = (((unsigned char)table[code[i+2]])<<6) | ((unsigned char)table[code[i+3]]);
    }
    return res;
}