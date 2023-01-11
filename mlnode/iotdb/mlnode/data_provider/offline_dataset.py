import pandas as pd
from torch.utils.data import Dataset
from .utils.timefeatures import time_features



# Offline
class TimeSeriesDataset_OFL(Dataset):
    def __init__(self, session, sql, freq='h'):  
        self.session = session
        self.sql = sql
        self.freq = freq

        self.__read_data__()
        self.n_vars = self.data.shape[-1]


    def get_variable_num(self):
        return self.n_vars


    def __read_data__(self):
        result = self.session.execute_query_statement(self.sql)
        assert result # return None if fail to fetch
        df_raw = result.todf()

        cols_data = df_raw.columns[1:]
        self.data = df_raw[cols_data].values
        
        df_stamp = pd.to_datetime(df_raw.Time.values, unit='ms', utc=True).tz_convert('Asia/Shanghai')
        self.data_stamp = time_features(df_stamp, freq=self.freq)
        self.data_stamp = self.data_stamp.transpose(1, 0)


    def __getitem__(self, index):
        seq = self.data[index]
        seq_mark = self.data_stamp[index]

        return seq, seq_mark


    def __len__(self):
        return len(self.data)


# Offline
class WindowDataset_OFL(TimeSeriesDataset_OFL):
    def __init__(self, session, sql, seq_len, pred_len, label_len=0, freq='h'):
        self.session = session
        self.sql = sql
        self.freq = freq
        self.seq_len = seq_len
        self.pred_len = pred_len
        self.label_len = label_len

        self.__read_data__()
        self.n_vars = self.data.shape[-1]


    def __getitem__(self, index):
        s_begin = index
        s_end = s_begin + self.seq_len
        r_begin = s_end - self.label_len
        r_end = s_end + self.pred_len

        seq_x = self.data[s_begin:s_end]
        seq_y = self.data[r_begin:r_end]
        seq_x_mark = self.data_stamp[s_begin:s_end]
        seq_y_mark = self.data_stamp[r_begin:r_end]

        return seq_x, seq_y, seq_x_mark, seq_y_mark


    def __len__(self):
        return len(self.data) - self.seq_len - self.pred_len + 1


