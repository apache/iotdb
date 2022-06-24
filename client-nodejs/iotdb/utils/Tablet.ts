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
import {BitMap} from "./BitMap";

export class Tablet {

    private values;
    private timestamps;
    private device_id;
    private measurements;
    private data_types;
    private row_number;
    private column_number;   

    constructor(device_id, measurements, data_types, values, timestamps){
        /*
        creating a tablet for insertion
          for example, considering device: root.sg1.d1
            timestamps,     m1,    m2,     m3
                     1,  125.3,  True,  text1
                     2,  111.6, False,  text2
                     3,  688.6,  True,  text3
        Notice: From 0.13.0, the tablet can contain empty cell
                The tablet will be sorted at the initialization by timestamps
        :param device_id: String, IoTDB time series path to device layer (without sensor).
        :param measurements: List, sensors.
        :param data_types: TSDataType List, specify value types for sensors.
        :param values: 2-D List, the values of each row should be the outer list element.
        :param timestamps: List.
        */
        if( timestamps.length != values.length ){
            throw new Error("Input error! len(timestamps) does not equal to len(values)!");
        }

        if(!Tablet.check_sorted(timestamps)){
            let tuple_unsorted = [];
            for(let j in timestamps){
                tuple_unsorted.push([ timestamps[j], values[j] ]);
            }
            //let tuple_sorted = tuple_unsorted.sort();
            let tuple_sorted = tuple_unsorted.sort(function(x,y) {
                if (x[0] > y[0]) {
                    return 1;
                }
            
                if (x[0] < y[0]) {
                    return -1;
                }
                return 0;
            });
            for(let p in tuple_sorted){
                timestamps[p] = tuple_sorted[p][0];
                values[p] = tuple_sorted[p][1];
            }
        }
        this.values = values;
        this.timestamps = timestamps;

        this.device_id = device_id;
        this.measurements = measurements;
        this.data_types = data_types;
        this.row_number = timestamps.length;
        this.column_number = measurements.length;
    }

    public static check_sorted(timestamps){
        for(let i=1; i<timestamps.length; i++){
            if(timestamps[i] < timestamps[i - 1]){
                return false;
            }
        }
        return true;
    }

    public get_measurements(){
        return this.measurements;
    }

    public get_data_types(){
        return this.data_types;
    }

    public get_row_number(){
        return this.row_number;
    }

    public get_device_id(){
        return this.device_id;
    }

    public get_binary_timestamps(){
        let values_tobe_packed = [];
        for(let i in this.timestamps){
            let bigint64 = new BigInt64Array( [ this.timestamps[i] ] );
            let uint8 = new Uint8Array(bigint64.buffer).reverse();
            for(let j=0; j<8; j++){
                values_tobe_packed.push(uint8[j]);
            }
        }
        return Buffer.from(values_tobe_packed);                
    }

    public get_binary_values(){
        let values_tobe_packed = [];
        let bitmaps = [];
        let has_none = false;
        for(let i=0; i<this.column_number; i++){
            let bitmap = null;
            bitmaps.push(bitmap);
            if (this.data_types[i] == TSDataType.BOOLEAN){
                for(let j=0; j<this.row_number; j++){
                    if (this.values[j][i] != null) {
                        values_tobe_packed.push( this.values[j][i] );
                    }else{
                        values_tobe_packed.push(false);
                        this.mark_none_value(bitmaps, i, j);
                        has_none = true;
                    }
                }
            }else if (this.data_types[i] == TSDataType.INT32){
                for(let j=0; j<this.row_number; j++){
                    if (this.values[j][i] != null) {
                        let int32 = new Int32Array( [ this.values[j][i] ] );
                        let uint8 = new Uint8Array(int32.buffer).reverse();
                        for(let k=0; k<4; k++){
                            values_tobe_packed.push(uint8[k]);
                        }
                    }else{
                        for(let k=0; k<4; k++){
                            values_tobe_packed.push(0);
                        }
                        this.mark_none_value(bitmaps, i, j);
                        has_none = true;
                    }       
                }
            }else if (this.data_types[i] == TSDataType.INT64){
                for(let j=0; j<this.row_number; j++){
                    if (this.values[j][i] != null) {
                        let bigint64 = new BigInt64Array( [ this.values[j][i] ] );
                        let uint8 = new Uint8Array(bigint64.buffer).reverse();
                        for(let k=0; k<8; k++){
                            values_tobe_packed.push(uint8[k]);
                        }
                    }else{
                        for(let k=0; k<8; k++){
                            values_tobe_packed.push(0);
                        }
                        this.mark_none_value(bitmaps, i, j);
                        has_none = true;
                    }
                }
            }else if (this.data_types[i] == TSDataType.FLOAT){
                for(let j=0; j<this.row_number; j++){
                    if (this.values[j][i] != null) {
                        let float32 = new Float32Array( [ this.values[j][i] ] );
                        let uint8 = new Uint8Array(float32.buffer).reverse();
                        for(let k=0; k<4; k++){
                            values_tobe_packed.push(uint8[k]);
                        }
                    }else{
                        for(let k=0; k<4; k++){
                            values_tobe_packed.push(0);
                        }
                        this.mark_none_value(bitmaps, i, j);
                        has_none = true;
                    }
                }
            }else if (this.data_types[i] == TSDataType.DOUBLE){
                for(let j=0; j<this.row_number; j++){
                    if (this.values[j][i] != null) {
                        let float64 = new Float64Array( [ this.values[j][i] ] );
                        let uint8 = new Uint8Array(float64.buffer).reverse();
                        for(let k=0; k<8; k++){
                            values_tobe_packed.push(uint8[k]);
                        }
                    }else{
                        for(let k=0; k<8; k++){
                            values_tobe_packed.push(0);
                        }
                        this.mark_none_value(bitmaps, i, j);
                        has_none = true;
                    }
                }
            }else if (this.data_types[i] == TSDataType.TEXT){
                for(let j=0; j<this.row_number; j++){
                    if (this.values[j][i] != null) {
                        let utf8arr = Buffer.from(this.values[j][i]);
                        let int32 = new Uint32Array([utf8arr.length]);
                        let uint8 = new Uint8Array(int32.buffer).reverse();
                        for(let k=0; k<4; k++){
                            values_tobe_packed.push(uint8[k]);
                        }
                        // @ts-ignore
                        for(let item of utf8arr){
                            values_tobe_packed.push(item);
                        }
                    }else{
                        let utf8arr = Buffer.from("");
                        let int32 = new Uint32Array([utf8arr.length]);
                        let uint8 = new Uint8Array(int32.buffer).reverse();
                        for(let k=0; k<4; k++){
                            values_tobe_packed.push(uint8[k]);
                        }
                        // @ts-ignore
                        for(let item of utf8arr){
                            values_tobe_packed.push(item);
                        }
                        this.mark_none_value(bitmaps, i, j);
                        has_none = true;
                    }
                }
            }else{
                throw new Error("Unsupported data type");
            }
        }
        if (has_none){
            for(let i=0; i<this.column_number; i++){
                if (bitmaps[i] == null){
                    values_tobe_packed.push(false);
                }else{
                    values_tobe_packed.push(true);
                    for(let j=0; j<Math.ceil(this.row_number/8); j++){
                        values_tobe_packed.push(Buffer.from([bitmaps[i].bits[j]]));
                    }
                }
            }
        }
        return Buffer.from(values_tobe_packed);    
    }

    private mark_none_value(bitmaps, column, row){
        if (bitmaps[column] == null){
            bitmaps[column] = new BitMap(this.row_number);
        }
        bitmaps[column].mark(row);
    }

}

