/**
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

package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
	"tsfile/common/log"
)

const (
	UNICODE_Start int = 0x4E00
	UNICODE_End   int = 0x9FA5
	UNICODE_Count     = UNICODE_End - UNICODE_Start + 1
)

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}

func checkFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

func genInt32File(filename string, iMaxRow int, ts time.Time, durPerRS time.Duration, bAppendIfExist bool) bool {
	//var filename = "output1.txt"
	var file *os.File
	var err1 error
	var fileExist bool = checkFileIsExist(filename)
	if bAppendIfExist {
		file, err1 = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	} else {
		file, err1 = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
		fileExist = false
	}
	if err1 != nil {
		return false
	}
	writer := bufio.NewWriter(file) //创建新的 Writer 对象
	if !fileExist {
		buf := []byte{0xEF, 0xBB, 0xBF}
		for _, b := range buf {
			writer.WriteByte(b)
		}
	}

	var iValue int64
	var strTsValue string
	var strValue string
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < iMaxRow; i++ {
		iValue = int64(r.Int31())
		strTsValue = ts.Format("2006-01-02 15:04:05")
		ts = ts.Add(durPerRS)
		strValue = strconv.FormatInt(iValue, 10)
		_, err1 = writer.WriteString(strTsValue + ";" + strValue + "\n")
		if err1 != nil {
			log.Info("write file failed")
		}
	}
	writer.Flush()
	file.Close()
	return true
}

func genInt64File(filename string, iMaxRow int, ts time.Time, durPerRS time.Duration, bAppendIfExist bool) bool {
	//var filename = "output1.txt"
	var file *os.File
	var err1 error
	var fileExist bool = checkFileIsExist(filename)
	if bAppendIfExist {
		file, err1 = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	} else {
		file, err1 = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
		fileExist = false
	}
	if err1 != nil {
		return false
	}
	writer := bufio.NewWriter(file) //创建新的 Writer 对象
	if !fileExist {
		buf := []byte{0xEF, 0xBB, 0xBF}
		for _, b := range buf {
			writer.WriteByte(b)
		}
	}

	var iValue int64
	var strValue string
	var strTsValue string
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < iMaxRow; i++ {
		iValue = r.Int63()
		strTsValue = ts.Format("2006-01-02 15:04:05")
		ts = ts.Add(durPerRS)
		strValue = strconv.FormatInt(iValue, 10)
		_, err1 = writer.WriteString(strTsValue + ";" + strValue + "\n")
		if err1 != nil {
			log.Info("write file failed")
		}
	}
	writer.Flush()
	file.Close()
	return true
}

func genFloat32File(filename string, iMaxRow int, ts time.Time, durPerRS time.Duration, bAppendIfExist bool) bool {
	var file *os.File
	var err1 error
	var fileExist bool = checkFileIsExist(filename)
	if bAppendIfExist {
		file, err1 = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	} else {
		file, err1 = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
		fileExist = false
	}
	if err1 != nil {
		return false
	}
	writer := bufio.NewWriter(file) //创建新的 Writer 对象
	if !fileExist {
		buf := []byte{0xEF, 0xBB, 0xBF}
		for _, b := range buf {
			writer.WriteByte(b)
		}
	}

	var fValue float32
	var strValue string
	var strTsValue string
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < iMaxRow; i++ {
		fValue = r.Float32()
		strTsValue = ts.Format("2006-01-02 15:04:05")
		ts = ts.Add(durPerRS)
		strValue = strconv.FormatFloat(float64(fValue), 'f', 6, 64)
		//strValue = strconv.FormatFloat(float64(fValue), 'E', -1, 32)
		_, err1 = writer.WriteString(strTsValue + ";" + strValue + "\n")
		if err1 != nil {
			log.Info("write file failed")
		}
	}
	writer.Flush()
	file.Close()
	return true
}

func genFloat64File(filename string, iMaxRow int, ts time.Time, durPerRS time.Duration, bAppendIfExist bool) bool {
	var file *os.File
	var err1 error
	var fileExist bool = checkFileIsExist(filename)
	if bAppendIfExist {
		file, err1 = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	} else {
		file, err1 = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
		fileExist = false
	}
	if err1 != nil {
		return false
	}
	writer := bufio.NewWriter(file) //创建新的 Writer 对象
	if !fileExist {
		buf := []byte{0xEF, 0xBB, 0xBF}
		for _, b := range buf {
			writer.WriteByte(b)
		}
	}

	var fValue float64
	var strValue string
	var strTsValue string
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < iMaxRow; i++ {
		fValue = r.Float64()
		strTsValue = ts.Format("2006-01-02 15:04:05")
		ts = ts.Add(durPerRS)
		strValue = strconv.FormatFloat(fValue, 'E', -1, 64)
		_, err1 = writer.WriteString(strTsValue + ";" + strValue + "\n")
		if err1 != nil {
			log.Info("write file failed")
		}
	}
	writer.Flush()
	file.Close()
	return true
}

func getStringEx(iValue int, iFormat int) string {
	if iFormat == 1 {
		iValue = iValue % 52
		if iValue >= 26 {
			iValue = 'A' + iValue - 26
		} else {
			iValue = 'a' + iValue
		}
		return fmt.Sprintf("%c", iValue)
	} else if iFormat == 2 {
		iValue = iValue % UNICODE_Count
		return fmt.Sprintf("%c", iValue+UNICODE_Start)
	} else {
		if iValue < UNICODE_Count {
			iValue = iValue % 52
			if iValue >= 26 {
				iValue = 'A' + iValue - 26
			} else {
				iValue = 'a' + iValue
			}
			return fmt.Sprintf("%c", iValue)
		} else {
			iValue = iValue % UNICODE_Count
			return fmt.Sprintf("%c", iValue+UNICODE_Start)
		}
	}
}

func genTextFile(filename string, iMaxRow int, iMaxColumn int, iFormat int, bRandColum bool, ts time.Time, durPerRS time.Duration, bAppendIfExist bool) bool {
	var file *os.File
	var err1 error
	var fileExist bool = checkFileIsExist(filename)
	if bAppendIfExist {
		file, err1 = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	} else {
		file, err1 = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
		fileExist = false
	}
	if err1 != nil {
		return false
	}
	writer := bufio.NewWriter(file) //创建新的 Writer 对象
	if !fileExist {
		buf := []byte{0xEF, 0xBB, 0xBF}
		for _, b := range buf {
			writer.WriteByte(b)
		}
	}

	//4E00..9FA5
	var iCount = UNICODE_End * 2
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var strLine string = ""
	var iColumn int
	var strTsValue string
	for i := 0; i < iMaxRow; i++ {
		strLine = ""
		if bRandColum {
			iColumn = r.Intn(iMaxColumn)
			if iColumn <= 0 {
				iColumn = iMaxColumn
			}
		} else {
			iColumn = iMaxColumn
		}

		for j := 0; j < iColumn; j++ {
			strLine += getStringEx(r.Intn(iCount), iFormat)
		}
		strTsValue = ts.Format("2006-01-02 15:04:05")
		ts = ts.Add(durPerRS)
		_, err1 = writer.WriteString(strTsValue + ";" + strLine + "\n")
		if err1 != nil {
			log.Info("write file failed")
		}
	}
	writer.Flush()
	file.Close()
	return true
}

func TestGenFilePerf(iMaxRowE int) {
	var iMaxRow = iMaxRowE //1000000
	perRSDur, _ := time.ParseDuration("1s")
	addYesday, _ := time.ParseDuration("-96h")
	now := time.Now()
	var ts = now.Add(addYesday)

	genInt32File("datain/output1.txt", iMaxRow, ts, perRSDur, false)
	genInt64File("datain/output2.txt", iMaxRow, ts, perRSDur, true)
	genFloat32File("datain/output3.txt", iMaxRow, ts, perRSDur, false)
	genFloat64File("datain/output4.txt", iMaxRow, ts, perRSDur, false)
	genTextFile("datain/output5.txt", //输出文件
		iMaxRow, //行
		80,      //每列最大字符数（中文算1个）
		0,       // 0 中英文， 1 英文， 2 中文
		true,    //列中字符数随机，最大80个，80是第三个参数
		ts, perRSDur,
		false) // true 文件追加方式

}

//func main() {
//	TestGenFilePerf()
//}
