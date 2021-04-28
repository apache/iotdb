using System.Collections.Generic;
namespace iotdb_client_csharp.client.utils
{
    public class Utils
    {
        public bool check_sorted(List<long> timestamp_lst){
            for(int i = 1; i < timestamp_lst.Count; i++){
                if(timestamp_lst[i] < timestamp_lst[i-1]){
                    return false;
                }
            }
            return true;
        }
        public int verify_success(TSStatus status, int SUCCESS_CODE){
            if(status.__isset.subStatus){
                foreach(var sub_status in status.SubStatus){
                    if(verify_success(sub_status, SUCCESS_CODE) != 0){
                        return -1;
                    }
                }
                return 0;
            }
            if (status.Code == SUCCESS_CODE){
                return 0;
            }
            return -1;
        }
    }
}