#pragma once
#include <string>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include "Session.h"
//#include "asio.hpp"

//using asio::ip::tcp;

using namespace std;

typedef struct UserInfor 
{
	string user_name;
	string password;
	string org;
	string bucket;
	int retentionPeriodSeconds;
	int retentionPeriodHrs;
	string token;
	UserInfor()
	{
		retentionPeriodSeconds = 0;
		retentionPeriodHrs = 0;

	}
}UserInfor;

class IotdbClient
{
public:
	bool InitUser_Org_Bucket(const string& url,const UserInfor& user_infor);
	bool AddUser_Org_Bucket(const string& url, const UserInfor& user_infor);
	bool WriteData(const string& url, const string org, const string bucket,const string& data);
	bool Exec(const string& url, const string& query);
	bool Connect(const string& address,const string& port);
	bool WriteDataTcp(const string& data);
	bool SessionOpen(const string& address, const int& port,const string& user_name,const string& user_pwd);
	bool SessionClose();
	bool SetStorageGroup(const string& group_name);
	bool createMultiTimeseries(
		std::vector <std::string> paths,
		std::vector <TSDataType::TSDataType> dataTypes,
		std::vector <TSEncoding::TSEncoding> encodings,
		std::vector <CompressionType::CompressionType> compressors);

	bool createAlignedTimeseries(const std::string& deviceId,
		const std::vector<std::string>& measurements,
		const std::vector<TSDataType::TSDataType>& dataTypes,
		const std::vector<TSEncoding::TSEncoding>& encodings,
		const std::vector<CompressionType::CompressionType>& compressors);

	bool insertRecords(
		std::vector <std::string>& deviceIds,
		std::vector <int64_t>& times,
		std::vector <std::vector<std::string>>& measurementsList,
		std::vector <std::vector<std::string>>& valuesList
	);

	bool insertAlignedRecords(
		std::vector <std::string>& deviceIds,
		std::vector <int64_t>& times,
		std::vector <std::vector<std::string>>& measurementsList,
		std::vector <std::vector<std::string>>& valuesList
	);

	bool insertAlignedRecords(const std::vector<std::string>& deviceIds,
		const std::vector<int64_t>& times,
		const std::vector<std::vector<std::string>>& measurementsList,
		const std::vector<std::vector<TSDataType::TSDataType>>& typesList,
		const std::vector<std::vector<char*>>& valuesList);

	bool Query(std::string& sql);
private:
	//shared_ptr<asio::io_context> m_io_context;
	//shared_ptr<tcp::socket> m_socket;

	shared_ptr <Session> m_session;
};
