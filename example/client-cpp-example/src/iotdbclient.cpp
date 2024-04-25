#include <iostream>
#include "iotdbclient.h"
//#include "cpr/cpr.h"
#include "nlohmann/json.hpp"

bool IotdbClient::InitUser_Org_Bucket(const string& url, const UserInfor& user_infor)
{
	/*
	nlohmann::json user_infor_json;
	user_infor_json["username"] = user_infor.user_name;
	user_infor_json["password"] = user_infor.password;
	user_infor_json["org"] = user_infor.org;
	user_infor_json["bucket"] = user_infor.bucket;
	user_infor_json["retentionPeriodSeconds"] = 0;
	user_infor_json["retentionPeriodHrs"] = 0;
	user_infor_json["token"] = user_infor.token;

	auto sInit = user_infor_json.dump();

	cpr::Response res = cpr::Post(cpr::Url{ "http://localhost:8086/api/v2/setup" },
	cpr::Body{ sInit },
	cpr::Header{ {"Content-Type", "text/plain"} });
	std::cout << res.text << std::endl;
	*/
	return true;
}

bool IotdbClient::AddUser_Org_Bucket(const string& url, const UserInfor& user_infor)
{
	/*
	nlohmann::json user_infor_json;
	user_infor_json["username"] = user_infor.user_name;
	user_infor_json["password"] = user_infor.password;
	user_infor_json["org"] = user_infor.org;
	user_infor_json["bucket"] = user_infor.bucket;
	user_infor_json["retentionPeriodSeconds"] = 0;
	user_infor_json["retentionPeriodHrs"] = 0;
	user_infor_json["token"] = user_infor.token;

	auto user_infor_s = user_infor_json.dump();

	cpr::Response res = cpr::Post(cpr::Url{ "http://localhost:8086/api/v2/setup/user" },
		cpr::Body{ user_infor_s },
		cpr::Header{ {"Content-Type", "text/plain"}, {"Authorization","Token token_string"} });
	std::cout << res.text << std::endl;
	*/
	return true;
}

bool IotdbClient::WriteData(const string& url, const string org,const string bucket, const string& data)
{
	/*
	cpr::Response res = cpr::Post(cpr::Url{ url },
		cpr::Parameters{ {"org",org},{"bucket",bucket}, {"precision","ns"} },
		cpr::Body{ data },
		cpr::Header{ {"Content-Type", "text/plain"}, {"Authorization","Token token_string"} });
	std::cout << res.url <<" "<<res.text<<std::endl;
	*/
	return true;
}

bool IotdbClient::Exec(const string& url, const string& query)
{
	/*
	cpr::Response res = cpr::Post(cpr::Url{ url },
		cpr::Parameters{ {"query",query} });
	//std::cout << res.url << " " << res.text << std::endl;
	std::cout <<"res:"<< res.text << std::endl;
	*/

	return true;
}

bool IotdbClient::Connect(const string& address,const string& port)
{
	bool ret = false;
	/*
	try
	{
		m_io_context = make_shared<asio::io_context>();
		m_socket = make_shared<tcp::socket>(*m_io_context);

		shared_ptr<tcp::resolver> resolver = make_shared<tcp::resolver>(*m_io_context);
		auto con = asio::connect(*m_socket, resolver->resolve(address, port));
		ret = true;
	}
	catch (std::exception& e)
	{
		std::cerr << e.what() << std::endl;
	}
	*/
	return ret;
}
enum { max_length = 1024 };
bool IotdbClient::WriteDataTcp(const string& data)
{
	bool ret = false;
	/*
	try
	{
		size_t size_w = asio::write(*m_socket, asio::buffer(data));

		if (size_w == data.size())
		{
			ret = true;
		}
		else
		{

		}

		std::cout << "write bytes:" << size_w << ",data size:"<< data.size()<<endl;

		char reply[max_length];
		//string reply;
		//reply.resize(1024);
		//size_t reply_length = asio::read(*m_socket,asio::buffer(reply, max_length));

		//std::cout << "reply bytes:" << reply_length << ",reply:" << reply << endl;
	}
	catch (std::exception& e)
	{
		std::cerr << e.what() << std::endl;
	}
	*/
	return ret;
}

bool IotdbClient::SessionOpen(const string& address, const int& port, const string& user_name, const string& user_pwd)
{
	bool ret = false;
	try {
		m_session = make_shared<Session>(address, port, user_name, user_pwd);

		m_session->open(false);

		ret = true;
	}
	catch (IoTDBConnectionException e) {
		string errorMessage(e.what());
		std::cerr << errorMessage << std::endl;
	}
	return ret;
}

bool IotdbClient::SessionClose()
{
	bool ret = false;
	try {
	
		m_session->close();
		m_session = nullptr;
		ret = true;
	}
	catch (IoTDBConnectionException e) {
		string errorMessage(e.what());
		std::cerr << errorMessage << std::endl;
	}
	return ret;
}

bool IotdbClient::SetStorageGroup(const string& group_name)
{
	bool ret = false;

	if (m_session)
	{
		try {
			m_session->setStorageGroup(group_name);
			ret = true;
		}
		catch (IoTDBConnectionException e) {
			string errorMessage(e.what());
			if (errorMessage.find("StorageGroupAlreadySetException") == string::npos) {
				std::cerr << errorMessage << std::endl;
			}
		}
	}
	
	return ret;
}

bool IotdbClient::createMultiTimeseries(
	std::vector <std::string> paths,
	std::vector <TSDataType::TSDataType> dataTypes,
	std::vector <TSEncoding::TSEncoding> encodings,
	std::vector <CompressionType::CompressionType> compressors)
{
	bool ret = false;

	if (m_session)
	{
		try {
			m_session->createMultiTimeseries(paths, dataTypes, encodings, compressors, nullptr, nullptr, nullptr, nullptr);
			ret = true;
		}
		catch (BatchExecutionException e) {
			string errorMessage(e.what());
			std::cerr << errorMessage << std::endl;
		}
	}

	return ret;
}


bool IotdbClient::createAlignedTimeseries(const std::string& deviceId,
	const std::vector<std::string>& measurements,
	const std::vector<TSDataType::TSDataType>& dataTypes,
	const std::vector<TSEncoding::TSEncoding>& encodings,
	const std::vector<CompressionType::CompressionType>& compressors)
{
	bool ret = false;

	if (m_session)
	{
		try {
			m_session->createAlignedTimeseries(deviceId, measurements, dataTypes, encodings, compressors);
			ret = true;
		}
		catch (BatchExecutionException e) {
			string errorMessage(e.what());
			std::cerr << errorMessage << std::endl;
		}
	}

	return ret;
}

bool IotdbClient::insertRecords(
	std::vector <std::string>& deviceIds,
	std::vector <int64_t>& times,
	std::vector <std::vector<std::string>>& measurementsList,
	std::vector <std::vector<std::string>>& valuesList
)
{
	bool ret = false;

	if (m_session)
	{
		try {
			m_session->insertRecords(deviceIds, times, measurementsList, valuesList);
			ret = true;
		}
		catch (IoTDBConnectionException e) {
			string errorMessage(e.what());
			std::cerr << errorMessage << std::endl;
		}
	}

	return ret;

}

bool IotdbClient::Query(std::string& sql)
{
	bool ret = false;

	if (m_session)
	{
		try {
			std::unique_ptr <SessionDataSet> data_sets =  m_session->executeQueryStatement(sql);
			ret = true;
		}
		catch (IoTDBConnectionException e) {
			string errorMessage(e.what());
			std::cerr << errorMessage << std::endl;
		}
	}

	return ret;
}

bool IotdbClient::insertAlignedRecords(
	std::vector <std::string>& deviceIds,
	std::vector <int64_t>& times,
	std::vector <std::vector<std::string>>& measurementsList,
	std::vector <std::vector<std::string>>& valuesList
)
{
	bool ret = false;

	if (m_session)
	{
		try {
			m_session->insertAlignedRecords(deviceIds, times, measurementsList, valuesList);
			ret = true;
		}
		catch (IoTDBConnectionException e) {
			string errorMessage(e.what());
			std::cerr << errorMessage << std::endl;
		}
	}

	return ret;
}

bool IotdbClient::insertAlignedRecords(const std::vector<std::string>& deviceIds,
	const std::vector<int64_t>& times,
	const std::vector<std::vector<std::string>>& measurementsList,
	const std::vector<std::vector<TSDataType::TSDataType>>& typesList,
	const std::vector<std::vector<char*>>& valuesList)
{
	bool ret = false;

	if (m_session)
	{
		try {
			m_session->insertAlignedRecords(deviceIds, times, measurementsList, typesList,valuesList);
			ret = true;
		}
		catch (IoTDBConnectionException e) {
			string errorMessage(e.what());
			std::cerr << errorMessage << std::endl;
		}
	}

	return ret;
}