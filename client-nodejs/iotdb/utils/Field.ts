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

import {TSDataType} from "./IoTDBConstants";

export class Field{

    private data_type;
    private bool_value;
    private int_value;
    private long_value;
    private float_value;
    private double_value;
    private binary_value;

    constructor(data_type){
        /*
        :param data_type: TSDataType
        */
        this.data_type = data_type;
        this.bool_value = null;
        this.int_value = null;
        this.long_value = null;
        this.float_value = null;
        this.double_value = null;
        this.binary_value = null;
    }

    public static copy(field){
        let output = new Field(field.get_data_type());
        if(output.get_data_type() !== null){
            if(output.get_data_type() == TSDataType.BOOLEAN){
                output.set_bool_value(field.get_bool_value());
            }else if(output.get_data_type() == TSDataType.INT32){
                output.set_int_value(field.get_int_value());
            }else if(output.get_data_type() == TSDataType.INT64){
                output.set_long_value(field.get_long_value());
            }else if(output.get_data_type() == TSDataType.FLOAT){
                output.set_float_value(field.get_float_value());
            }else if(output.get_data_type() == TSDataType.DOUBLE){
                output.set_double_value(field.get_double_value());
            }else if(output.get_data_type() == TSDataType.TEXT){
                output.set_binary_value(field.get_binary_value());
            }else{
                throw new Error("unsupported data type");
            }
        }
        return output;
    }

    public get_data_type(){
        return this.data_type;
    }

    public is_null(){
        return this.data_type === null;
    }

    public set_bool_value(value){
        this.bool_value = value;
    }

    public get_bool_value(){
        if(this.data_type === null){
            throw new Error("Null Field Exception!");
        }
        return this.bool_value;
    }

    public set_int_value(value){
        this.int_value = value;
    }

    public get_int_value(){
        if(this.data_type === null){
            throw new Error("Null Field Exception!");
        }
        return this.int_value;
    }

    public set_long_value(value){
        this.long_value = value;
    }

    public get_long_value(){
        if(this.data_type === null){
            throw new Error("Null Field Exception!");
        }
        return this.long_value;
    }

    public set_float_value(value){
        this.float_value = value;
    }

    public get_float_value(){
        if(this.data_type === null){
            throw new Error("Null Field Exception!");
        }
        return this.float_value;
    }

    public set_double_value(value){
        this.double_value = value;
    }

    public get_double_value(){
        if(this.data_type === null){
            throw new Error("Null Field Exception!");
        }
        return this.double_value;
    }

    public set_binary_value(value){
        this.binary_value = value;
    }

    public get_binary_value(){
        if(this.data_type === null){
            throw new Error("Null Field Exception!");
        }
        return this.binary_value;
    }

    public get_string_value(): string{
        if(this.data_type === null){
            return "null";
        }else if(this.data_type == TSDataType.BOOLEAN){
            return this.bool_value.toString();
        }else if(this.data_type == TSDataType.INT64){
            return this.long_value.toString();
        }else if(this.data_type == TSDataType.INT32){
            return this.int_value.toString();
        }else if(this.data_type == TSDataType.FLOAT){
            return this.float_value.toString();
        }else if(this.data_type == TSDataType.DOUBLE){
            return this.double_value.toString();
        }else if (this.data_type == TSDataType.TEXT){
            return this.binary_value.toString();
        }else{
            throw new Error("unsupported data type");
        }
    }

    public str(){
        return this.get_string_value();
    }

    public get_object_value(data_type){
        /*
        :param data_type: TSDataType
        */
        if(this.data_type === null){
            return null;
        }else if(data_type == TSDataType.BOOLEAN) {
            return this.get_bool_value();
        }else if(data_type == TSDataType.INT32) {
            return this.get_int_value();
        }else if(data_type == TSDataType.INT64) {
            return this.get_long_value();
        }else if(data_type == TSDataType.FLOAT) {
            return this.get_float_value();
        }else if(data_type == TSDataType.DOUBLE) {
            return this.get_double_value();
        }else if(data_type == TSDataType.TEXT) {
            return this.get_binary_value();
        }else{
            throw new Error("unsupported data type");
        }
    }

    public static get_field(value, data_type){
        /*
        :param value: field value corresponding to the data type
        :param data_type: TSDataType
        */
        if(value === null){
            return null;
        }
        let field = new Field(data_type);
        if(data_type == TSDataType.BOOLEAN) {
            field.set_bool_value(value);
        }else if(data_type == TSDataType.INT32) {
            field.set_int_value(value);
        }else if(data_type == TSDataType.INT64) {
            field.set_long_value(value);
        }else if(data_type == TSDataType.FLOAT) {
            field.set_float_value(value);
        }else if(data_type == TSDataType.DOUBLE) {
            field.set_double_value(value);
        }else if(data_type == TSDataType.TEXT) {
            field.set_binary_value(value);
        }else{
            throw new Error("unsupported data type");
        }
        return field;
    }

}
