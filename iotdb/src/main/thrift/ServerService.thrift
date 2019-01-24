namespace java org.apache.iotdb.db.postback.receiver

typedef i32 int 
typedef i16 short
typedef i64 long
service ServerService{
	bool getUUID(1:string uuid, 2:string address)
	string startReceiving(1:string md5, 2:list<string> filename, 3:binary buff, 4:int status)
	void getFileNodeInfo()
	void mergeOldData(1:string path)
	void mergeData()
	void getSchema(1:binary buff, 2:int status)
	bool merge()
	void afterReceiving()
	void init(1:string storageGroup)
}