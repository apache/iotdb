//
// Created by JiaXinZhang on 2022/3/30.
//

#ifndef C_REST_IOTDB_BASE64_H
#define C_REST_IOTDB_BASE64_H

#include <stdlib.h>
#include <string.h>

unsigned char *base64_encode(unsigned char *str);

unsigned char *bae64_decode(unsigned char *code);

#endif//C_REST_IOTDB_BASE64_H
