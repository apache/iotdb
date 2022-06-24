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

import {Field} from "./Field";

export class RowRecord{

    private timestamp;
    private field_list;

    constructor(timestamp, field_list=null){
        this.timestamp = timestamp;
        this.field_list = field_list;
    }

    public add_field(value, data_type){
        this.field_list.push(Field.get_field(value, data_type));
    }

    public str(){
        let str_list = new Array(this.timestamp.toString());
        for(let field in this.field_list){
            str_list.push( field.toString() );
        }
        return str_list.join("     ");
    }

    public get_timestamp(){
        return this.timestamp;
    }

    public set_timestamp(timestamp){
        this.timestamp = timestamp;
    }

    public get_fields(){
        return this.field_list;
    }

    public set_fields(field_list){
        this.field_list = field_list;
    }

    public set_field(index, field){
        this.field_list[index] = field;
    }

}
