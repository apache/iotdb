// influxdb2test.cpp: 定义应用程序的入口点。
//
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <string>
#include <memory>
#include "iotdbtest.h"
#include "cpr/cpr.h"
#include "nlohmann/json.hpp"
#include "iotdbclient.h"
#include "ThreadPool.h"
#include <fmt/format.h>
#include <mutex>
#include <chrono>
#include <fmt/chrono.h>

// for convenience
//using json = nlohmann::json;

using namespace std;
using namespace chrono;

shared_ptr<ThreadPool> gthreadpools;
shared_ptr<ThreadPool> gthreadpoolm;

string g_address = "localhost:8086";

int g_start_no_mul = 1; //多点写，起始点号
int g_num_onetime_mul = 5000; //多点写，每包数据的点数
int g_write_times_mul = 3600 * 24 * 365; //多点写，写次数
int g_time_offset_mul = 0; //多点写，往前推的时间偏移(S)，即从当前时间往前推多少时间的时标开始写
int g_sleep_time_mul = 1000; //单点写，每次写的sleep时间

int g_start_no_single = 1; //单点写，起始点号
int g_num_onetime_single = 1; //单点写，每包数据的点(tag)数
int g_write_time_single = 3600; //单点写，每次每个点(tag)写的时间数(s)
int g_time_offset_single = 31536000; //单点写，往前推的时间偏移(S)，即从当前时间往前推多少时间的时标开始写
int g_sleep_time_single = 1000; //单点写，每次写的sleep时间

int g_create_tag_start_no = 1; //起始点号
int g_create_tag_onetime_num = 10000; // 每次创建多少点
int g_create_tag_num = 50000; //要创建的总点数

std::atomic<int> g_min_write_time(50000); //最小写时间
std::atomic<int> g_max_write_time(0); //最大写时间


time_t test_start_time = 0; //测试开始时间

//CRITICAL_SECTION g_criticalSection;
mutex g_mutex;
void workerthreads()
{
	//写数据
	//string strUrlw = "http://localhost:8086/write";
	IotdbClient client;

	string strUrlw = "http://";
	strUrlw += g_address;
	strUrlw += "/api/v2/write";

	int iRet = -1;
	string strResponse;

	int iStartNo = 0;
	int iEndNo = 0;
	int write_times = 0;

	std::unique_lock<std::mutex> lk(g_mutex);
	iStartNo = g_start_no_single;
	iEndNo = iStartNo + g_num_onetime_single;
	g_start_no_single = iEndNo;
	write_times = (g_time_offset_single - 1) / g_write_time_single + 1;

	lk.unlock();
	//int iBufLen = 1024*500;
	time_t tt_now = time(NULL);

	fmt::memory_buffer data_buffer;
	data_buffer.resize(1024*5);

	int64_t time1 = 0;
	int write_count = 0;
	double dValue = 0;
	int i_q = 1;
	srand(time(nullptr));

	for (int j = 0; j < write_times; j++)
	{
		auto time_1 = steady_clock::now();
		data_buffer.clear();
		for (int i = iStartNo; i < iEndNo; i++)
		{
			for (int k = 0; k < g_write_time_single; k++)
			{
				i_q = k % 4;
				fmt::format_to(data_buffer, "v,TagName=Tag_{}", i);
				dValue = (double)rand() / 101;
				fmt::format_to(data_buffer, " value={},q={} ", dValue, i_q);

				time1 = (tt_now - g_time_offset_single + g_write_time_single* j + k) * 1000000000; //时间转换为ns
				fmt::format_to(data_buffer, "{}\n", time1);
			}
		}
		auto time_2 = steady_clock::now();
		string data = fmt::to_string(data_buffer);
		bool bRet = client.WriteData(strUrlw, "org_test", "bucket_test", data);
		write_count++;
		auto time_3 = steady_clock::now();

		auto duration_write = duration_cast<milliseconds>(time_3 - time_2);
		auto duration_make_pkt = duration_cast<milliseconds>(time_2 - time_1);

		cout << "iStartNo=" << iStartNo << ",count=" << write_count << ",write time=" << duration_write.count() << ",make pkg time=" << duration_make_pkt.count() << ",iRet=" << iRet << ",ilen=" << data.size() << ",Response=" << strResponse << endl;

		if (g_sleep_time_single > 0)//sleep时间大于0
		{
			auto use_time = duration_cast<milliseconds>(time_3 - time_1);
			auto sleep_time = std::chrono::milliseconds(g_sleep_time_single) - use_time;
			if (sleep_time.count() > 0)
			{
				this_thread::sleep_for(sleep_time);
			}
		}
	}
}

void workerthreadaggregation()
{
	//写数据
	//string strUrlw = "http://localhost:8086/query?";
	string strUrlw = "http://172.21.31.51:8086/query?";
	IotdbClient client;
	int iRet = -1;
	string strResponse;
	int iTagNo = 0;
	int iTagNum = 0;
	std::unique_lock<std::mutex> lk(g_mutex);
	iTagNum = g_num_onetime_single;
	iTagNo = g_start_no_single;
	g_start_no_single += g_num_onetime_single;

	iTagNum = g_num_onetime_single;
	lk.unlock();
	//int iBufLen = 1024*500;
	time_t tt = time(NULL);

	fmt::memory_buffer data_buffer;
	data_buffer.resize(1024 * 5);

	int iLen = 0;
	int iCount = 0;
	long lValue = 0;
	for (int k = 0; k < iTagNum; k++)
	{
		data_buffer.clear();
		/*
		wPkt << "SELECT max(value),min(value),mean(value) INTO mydb.longyears.TagValue20m FROM TagValue where TagName='Tag_";
		wPkt << iTagNo;
		wPkt << "'";
		wPkt << " GROUP BY time(20m),TagName";

		wPkt.GetUsedBuf(&szData, iLen);
		cout << "aggregation begain" << endl;

		pClient->aggregation(strUrlw, "mydb", szData, strResponse, 300000);
		cout << "aggregation end " << strResponse << endl;

		iTagNo++;
		*/
	}

}

/**
 * @brief 多点单时刻写
*/
void workerthreadm()
{
	//写数据
	//string strUrlw = "http://172.21.31.51:8086/write";
	//string strUrlw = "http://localhost:8086/write";

	string strUrlw = "http://";
	strUrlw += g_address;
	strUrlw += "/api/v2/write";

	//string strUrlw = "http://localhost:8086/write";

	//string strUrlw = "http://localhost:8428/write";

	IotdbClient client;
	client.SessionOpen("127.0.0.1", 6667, "root", "root");
	int iRet = -1;
	string strResponse;
	int iStartNo = 0;
	int iEndNo = 0;
	std::unique_lock<std::mutex> lk(g_mutex);
	iStartNo = g_start_no_mul;
	iEndNo = iStartNo + g_num_onetime_mul;
	g_start_no_mul = iEndNo;
	lk.unlock();

	time_t tt = time(NULL);

	fmt::memory_buffer data_buffer;
	data_buffer.resize(1024 * 5);
	int iLen = 0;

	long interval = 31536 * 1000;
	long interval1 = interval;
	int64_t time1 = 0;
	int iCount = 0;
	long lValue = 0;
	double dValue = 1.1;
	double tag1_value = 0;
	int i_q = 1;

	string deviceId;
	vector<string> measurements;
	measurements.push_back("v");
	measurements.push_back("q");

	time_t start_tt = time(NULL);
	//有前推时间，造较早时间的数据
	if (g_time_offset_mul > 0)
	{
		tt = time(NULL);
		start_tt = tt;
		tt -= g_time_offset_mul;
	}
	srand(time(nullptr));

	for (int j = 0; j < g_write_times_mul; j++)
	{
		vector<string> deviceIds;
		vector<vector<string>> measurementsList;
		vector<std::vector<TSDataType::TSDataType>> typesList;
		vector<vector<string>> valuesList;
		vector<std::vector<char*>> sz_valuesList;
		vector<int64_t> timestamps;
		vector<string>  values_temp;
		string value;

		data_buffer.clear();
		i_q = j % 4;
		time_t now_tt = time(NULL);
		if (0 == g_time_offset_mul)
		{
			tt = time(NULL);
		}
		else
		{
			tt++;
		}

		auto time_1 = steady_clock::now();

		//fmt::format_to(data_buffer, "INSERT INTO v VALUES");

		for (int i = iStartNo; i < iEndNo; i++)
		{
			dValue = (double)rand() / 101;

			string deviceId = fmt::format("root.test2.tag_{}", i);
			deviceIds.push_back(deviceId);

			measurementsList.push_back(measurements);

			//vector<string> values;
			vector<char*> sz_values;
			value = fmt::format("{}", dValue);
			values_temp.push_back(value);
			//sz_values.push_back((char*)value.c_str());
			sz_values.push_back((char*)&dValue);

			value = fmt::format("{}", i_q);
			values_temp.push_back(value);
			//sz_values.push_back((char*)value.c_str());
			sz_values.push_back((char*)&i_q);

			sz_valuesList.push_back(sz_values);



			vector<TSDataType::TSDataType> dataTypes;
			dataTypes.push_back(TSDataType::DOUBLE);
			dataTypes.push_back(TSDataType::INT32);
			typesList.push_back(dataTypes);

			//time1 = tt * 1000000000;
			time1 = tt *   1000;

			timestamps.push_back(time1);

			if (i == iStartNo)
			{
				tag1_value = dValue;
			}

		}
		iCount++;
		if (iCount > 1000)
		{
			iCount = 0;
		}
		auto time_2 = steady_clock::now();
		interval1 = interval - j;
		/*
		for (int i = 0; i < 5000; i++)
		{
			ssDatas << "Tag_0000009";
			ssDatas << ",";
			ssDatas<<"TagName=Tag_0000009";
			ssDatas<<" value=";
			ssDatas<<GetTickCount();
			ssDatas<<" ";
			ssDatas<<(tt-interval)*1000000000;
			ssDatas<<endl;
			interval--;
		}
		*/
		//strDatasw = ssDatas.str();
		//ssDatas.str("");
		//ssDatas.clear();
		auto time_3 = steady_clock::now();
		string data = fmt::to_string(data_buffer);
		//bool bRet = client.WriteDataTcp(data);
		client.insertAlignedRecords(deviceIds, timestamps, measurementsList, typesList, sz_valuesList);
		//bool bRet = client.Exec("http://localhost:9000/exec",data);
		auto time_4 = steady_clock::now();

		auto duration_write = duration_cast<milliseconds>(time_4 - time_3);
		auto duration_make_pkt = duration_cast<milliseconds>(time_2 - time_1);

		/*
		cout << "iStartNo=" << iStartNo << ",count=" << j << ",write time=" << duration_write.count()
			<< ",make pkg time=" << duration_make_pkt.count() << ",iRet=" << iRet << ",ilen=" << iLen << ",Response=" << strResponse << endl;
		*/

		if (duration_write.count() < g_min_write_time)
		{
			g_min_write_time = duration_write.count();
		}

		if (duration_write.count() > g_max_write_time)
		{
			g_max_write_time = duration_write.count();
		}

		string str_time = fmt::format("{:%Y-%m-%d %H:%M:%S}", fmt::localtime(tt));
		//string str_start_time = fmt::format("{:%Y-%m-%d %H:%M:%S}", fmt::localtime(start_tt));
		string str_now_time = fmt::format("{:%Y-%m-%d %H:%M:%S}", fmt::localtime(now_tt));
		//fmt::print("The date is {:%Y-%m-%d}.", fmt::localtime(t));

		cout << "iStartNo=" << iStartNo << ",time=" << str_time << ",starttime=" << start_tt << ",now=" << str_now_time
			<< ",tag1=" << tag1_value << ",count=" << j << ",write time=" << duration_write.count() << ",make pkg time="
			<< duration_make_pkt.count() << ",iRet=" << iRet << ",ilen=" << iLen << ",Response=" << strResponse
			<<",min write time="<< g_min_write_time<<",max write time=" << g_max_write_time<<endl;

		if (g_sleep_time_mul > 0)//sleep时间大于0
		{
			auto use_time = duration_cast<milliseconds>(time_4 - time_1);
			auto sleep_time = std::chrono::milliseconds(g_sleep_time_mul) - use_time;
			if (sleep_time.count() > 0)
			{
				this_thread::sleep_for(sleep_time);
			}
		}

		//this_thread::sleep_for(std::chrono::milliseconds(1000)- duration_write);
		//Sleep(abs((int)(1000 - (dwTick2 - dwTick3))));
	}

	time_t test_end_time = time(NULL);
	string str_start_time = fmt::format("{:%Y-%m-%d %H:%M:%S}", fmt::localtime(test_start_time));
	string str_end_time = fmt::format("{:%Y-%m-%d %H:%M:%S}", fmt::localtime(test_end_time));
	cout << "test end,start time:" << str_start_time << ",end time:" << str_end_time<< endl;
}

void createMultiTimeseries(IotdbClient& client)
{
	/*
	vector<string> paths;
	vector<TSDataType::TSDataType> tsDataTypes;
	vector<TSEncoding::TSEncoding> tsEncodings;
	vector<CompressionType::CompressionType> compressionTypes;
	*/

	/*
	int g_create_tag_start_no = 1; //起始点号
	int g_create_tag_onetime_num = 10000; // 每次创建多少点
	int g_create_tag_num = 50000; //要创建的总点数
	*/

	int j_count = g_create_tag_num / g_create_tag_onetime_num;
	if (g_create_tag_num % g_create_tag_onetime_num == 0)
	{
		j_count = g_create_tag_num / g_create_tag_onetime_num;
	}
	else
	{
		j_count = g_create_tag_num / g_create_tag_onetime_num + 1;
	}
	int tag_count = 0; //要创建的总点数计数
	int tag_no = g_create_tag_start_no; //点号

	for (int j = 0; j < j_count; j++)
	{
		/*
		paths.clear();
		tsDataTypes.clear();
		tsEncodings.clear();
		compressionTypes.clear();
		*/
		vector<string> paths;
		vector<TSDataType::TSDataType> tsDataTypes;
		vector<TSEncoding::TSEncoding> tsEncodings;
		vector<CompressionType::CompressionType> compressionTypes;
		string path;
		for (int i = 1; i <= g_create_tag_onetime_num && tag_count < g_create_tag_num; i++)
		{
			path = fmt::format("root.test2.tag_{}.v", tag_no);
			paths.push_back(path);
			tsDataTypes.push_back(TSDataType::DOUBLE);
			tsEncodings.push_back(TSEncoding::GORILLA);
			compressionTypes.push_back(CompressionType::LZ4);

			path = fmt::format("root.test2.tag_{}.q", tag_no);
			paths.push_back(path);
			tsDataTypes.push_back(TSDataType::INT64);
			tsEncodings.push_back(TSEncoding::RLE);
			compressionTypes.push_back(CompressionType::LZ4);

			tag_no++;
			tag_count++;
		}

		client.createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes);
	}

	std::cout << "createMultiTimeseries end"<<endl;
}

void insertRecords(IotdbClient& client)
{
	string deviceId;
	vector<string> measurements;
	measurements.push_back("v");
	measurements.push_back("q");

	for (int k = 0; k < 1000; k++)
	{
		time_t now_tt = time(NULL);
		for (int j = 0; j < 5; j++)
		{
			vector<string> deviceIds;
			vector<vector<string>> measurementsList;
			vector<vector<string>> valuesList;
			vector<int64_t> timestamps;

			for (int i = 1; i <= 10000; i++)
			{
				string deviceId = fmt::format("root.test2.tag_{}", j * 10000 + i);
				deviceIds.push_back(deviceId);

				measurementsList.push_back(measurements);

				vector<string> values;
				values.push_back("100");
				values.push_back("2");
				valuesList.push_back(values);

				int64_t time1 = now_tt * 1000000000;

				timestamps.push_back(time1);
			}

			client.insertRecords(deviceIds, timestamps, measurementsList, valuesList);
		}
		cout << "insertRecords count:" << k<<endl;
		sleep(1000);
	}

	cout << "insertRecords end" << endl;
}

void QueryTest(std::string& sql)
{
	if (sql.empty())
	{
		return;
	}

	IotdbClient client;
	auto time_1 = steady_clock::now();
	client.SessionOpen("127.0.0.1", 6667, "root", "root");
	//client.SetStorageGroup("root.test2");
	bool query = client.Query(sql);
	client.SessionClose();
	auto time_2 = steady_clock::now();

	auto query_write = duration_cast<milliseconds>(time_2 - time_1).count();

	cout << "QueryTest:sql=" << sql <<",res:"<< query<<",use time(ms):"<< query_write<< endl;
}


int main(int argc, char* argv[])
{
	IotdbClient client;
	string url = "http://localhost:9000/exec";
	/*
	string query;
	query = "CREATE TABLE IF NOT EXISTS tagvalue1(name STRING, value double,ts timestamp) TIMESTAMP(ts) PARTITION BY MONTH";

	client.Exec(url, query);
	*/

/*
	//cout << "Hello CMake." << endl;
	url = "http://";
	url += g_address;
	url += "/api/v2/setup";

	UserInfor user_infor;
	user_infor.user_name = "admin";
	user_infor.password = "12345678";
	user_infor.org = "org_test";
	user_infor.bucket = "bucket_test";
	user_infor.token = "token_string";
	//client.InitUser_Org_Bucket(url,user_infor);

	user_infor.user_name = "admin";
	user_infor.password = "12345678";
	user_infor.org = "org_test2";
	user_infor.bucket = "bucket_test2";
	user_infor.token = "token_string2";
	url = "http://";
	url += g_address;
	url += "/api/v2/setup/user";

	//client.AddUser_Org_Bucket(url,user_infor);

	//std::unique_lock<std::mutex> lk(g_mutex);
*/
	int ithreadnum0 = 1;
	int imode = 1;
	int ithreadnum1 = 5;
	if (argc > 1)
	{
		g_address = argv[1];
		if (argc > 2)
		{
			imode = atoi(argv[2]);
		}

		if (0 == imode)//写单标签模式
		{
			if (argc > 3)
			{
				ithreadnum0 = atoi(argv[3]);
			}

			if (argc > 4)
			{
				g_start_no_single = atoi(argv[4]);
			}

			if (argc > 5)
			{
				g_num_onetime_single = atoi(argv[5]);
			}

			if (argc > 6)
			{
				g_write_time_single = atoi(argv[6]);
			}

			if (argc > 7)
			{
				g_time_offset_single = atoi(argv[7]);
			}

			if (argc > 8)
			{
				g_sleep_time_single = atoi(argv[8]);
			}
		}
		else if (1 == imode)//批量标签模式
		{
			if (argc > 3)
			{
				ithreadnum1 = atoi(argv[3]);
			}
			if (argc > 4)
			{
				g_write_times_mul = atoi(argv[4]);
			}

			if (argc > 5)
			{
				g_start_no_mul = atoi(argv[5]);
			}

			if (argc > 6)
			{
				g_num_onetime_mul = atoi(argv[6]);
			}

			if (argc > 7)
			{
				g_time_offset_mul = atoi(argv[7]);
			}

			if (argc > 8)
			{
				g_sleep_time_mul = atoi(argv[8]);
			}
		}
		else if (2 == imode)//写单标签模式和批量标签模式
		{
			if (argc > 10)
			{
				ithreadnum0 = atoi(argv[3]);
				g_start_no_single = atoi(argv[4]);
				g_num_onetime_single = atoi(argv[5]);
				g_write_time_single = atoi(argv[6]);
				g_time_offset_single = atoi(argv[7]);
				g_sleep_time_single = atoi(argv[8]);

				ithreadnum1 = atoi(argv[9]);
				g_write_times_mul = atoi(argv[10]);
				g_start_no_mul = atoi(argv[11]);
				g_num_onetime_mul = atoi(argv[12]);
				g_time_offset_mul = atoi(argv[13]);
				g_sleep_time_mul = atoi(argv[14]);
			}
		}
		else if (3 == imode) //聚合
		{
			if (argc > 3)
			{
				ithreadnum0 = atoi(argv[3]);
			}

			if (argc > 4)
			{
				g_start_no_single = atoi(argv[5]);
			}

			if (argc > 6)
			{
				g_num_onetime_single = atoi(argv[7]);
			}
		}
		else if (3 == imode) //聚合
		{
			if (argc > 2)
			{
				ithreadnum0 = atoi(argv[2]);
			}

			if (argc > 3)
			{
				g_start_no_single = atoi(argv[3]);
			}

			if (argc > 4)
			{
				g_num_onetime_single = atoi(argv[4]);
			}
		}
		else if (4 == imode)
		{
			if (argc > 3)
			{
				g_create_tag_num = atoi(argv[3]);
			}

			if (argc > 4)
			{
				g_create_tag_start_no = atoi(argv[4]);
			}

			if (argc > 5)
			{
				g_create_tag_onetime_num = atoi(argv[5 ]);
			}
		}


	}
	cout <<"param num:           " << argc<<endl;
	//<< "    " << *argv << endl;
	cout<< "mode=                " << imode << endl
		<< "g_address=           " << g_address << endl
		<< endl
		<< "threadnum0=          " << ithreadnum0 << endl
		<< "g_start_no_single=   " << g_start_no_single << endl
		<< "g_num_onetime_single=" << g_num_onetime_single << endl
		<< "g_write_time_single= " << g_write_time_single << endl
		<< "g_time_offset_single=" << g_time_offset_single << endl
		<< "g_sleep_time_single= " << g_sleep_time_single << endl
		<< endl
		<< "threadnum1=          " << ithreadnum1 << endl
		<< "g_write_times_mul=   " << g_write_times_mul << endl
		<< "g_start_no_mul=      " << g_start_no_mul << endl
		<< "g_num_onetime_mul=   "<< g_num_onetime_mul << endl
		<< "g_time_offset_mul=   " << g_time_offset_mul << endl
		<< "g_sleep_time_mul=    "<< g_sleep_time_mul<< endl
	    << "g_create_tag_num=    " << g_create_tag_num << endl
		<< "g_create_tag_start_no=    " << g_create_tag_start_no << endl
		<< "g_create_tag_onetime_num=    " << g_create_tag_onetime_num << endl;
	char c1;
	cin >> c1;
	//return 0;

	//写单标签模式
	if (0 == imode)
	{
		gthreadpools = make_shared<ThreadPool>(ithreadnum0);
		int par = 1;
		for (int i = 0; i < ithreadnum0; i++)
		{
			gthreadpools->enqueue(std::bind(&workerthreads));
		}
	}
	else if (1 == imode) //批量标签模式
	{
		test_start_time = time(NULL);

		gthreadpoolm = make_shared<ThreadPool>(ithreadnum1);
		int par = 1;
		for (int i = 0; i < ithreadnum1; i++)
		{
			gthreadpoolm->enqueue(std::bind(&workerthreadm));
		}
	}
	else if (2 == imode)//写单标签模式和批量标签模式
	{
		gthreadpools = make_shared<ThreadPool>(ithreadnum0);
		int par = 1;
		for (int i = 0; i < ithreadnum0; i++)
		{
			gthreadpools->enqueue(std::bind(&workerthreads));
		}

		gthreadpoolm = make_shared<ThreadPool>(ithreadnum1);
		for (int i = 0; i < ithreadnum1; i++)
		{
			gthreadpoolm->enqueue(std::bind(&workerthreadm));
		}


	}
	else if (3 == imode)//聚合
	{
		gthreadpools = make_shared<ThreadPool>(ithreadnum0);
		int par = 1;
		for (int i = 0; i < ithreadnum0; i++)
		{
			gthreadpools->enqueue(std::bind(&workerthreadaggregation));
		}
	}
	else if (4 == imode) //创建时间序列
	{
		client.SessionOpen("127.0.0.1", 6667, "root", "root");
		client.SetStorageGroup("root.test2");
		createMultiTimeseries(client);
		//insertRecords(client);
		client.SessionClose();
	}
	else if (5 == imode) //查询
	{
		while (true)
		{
			std::string s_input;
			std::getline(std::cin, s_input);

			if (s_input == "q" || s_input == "Q")
			{
				break;
			}

			QueryTest(s_input);
		}
	}
	//lk.unlock();
	char c;
	cin >> c;

	stringstream ss;
	url = "http://localhost:8086/write";
	ss << "mem,host=host1 used_percent=23.43234543 1556896326" << endl;
	ss << "mem,host=host2 used_percent=26.81522361 1556896326" << endl;
	ss << "mem,host=host1 used_percent=22.52984738 1556896336" << endl;
	ss << "mem,host=host2 used_percent=27.18294630 1556896336" << endl;
	string sData = ss.str();
	//client.WriteData(url,"org_test","bucket_test", sData);


	return 0;
}
