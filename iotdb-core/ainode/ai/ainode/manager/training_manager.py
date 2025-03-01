import argparse

import numpy as np
import torch
import time
from iotdb.table_session import TableSession, TableSessionConfig
from torch.utils.data import Dataset, DataLoader

from ai.thrift.ainode.ttypes import ITableSchema
from constant import TSStatusCode
from log import Logger
from model.TimerXL.exp.exp_forecast import ExpForecast
from util.status import get_status

config = TableSessionConfig(
    node_urls=["localhost:6667"],
    username="root",
    password="root",
    time_zone="UTC+8",
)

SELECT_SERIES_FORMAT_SQL = "select distinct item_id from %s.%s"
COUNT_SERIES_LENGTH_SQL = "select count(value) from %s.%s where item_id = '%s'"
FETCH_SERIES_SQL = "select value from %s.%s where item_id = '%s' offset %s limit %s"
SERIES_NAME = "%s.%s.%s"

logger = Logger()

class TrainingParameters:
    def __init__(self):
        self.seed = 2025

        # forecasting task
        self.seq_len = 672
        self.input_token_len = 96
        self.output_token_len = 96

        # model define
        self.dropout = 0.1
        self.e_layers = 1
        self.d_model = 512
        self.n_heads = 8
        self.d_ff = 2048
        self.activation = 'relu'
        self.covariate = False
        self.node_num = 100
        self.node_list = '23,37,40'
        self.use_norm = False
        self.nonautoregressive = False
        self.output_attention = False
        self.flash_attention = False

        # adaptation
        self.adaptation = False
        self.pretrain_model_path = 'pretrain_model.pth'
        self.subset_rand_ratio = 1

        # optimization
        self.num_workers = 10
        self.itr = 1
        self.train_epochs = 10
        self.batch_size = 32
        self.patience = 3
        self.learning_rate = 0.0001
        self.des = 'test'
        self.loss = 'MSE'
        self.lradj = 'type1'
        self.cosine = False
        self.tmax = 10
        self.weight_decay = 0
        self.valid_last = False
        self.last_token = False

        # GPU
        self.device = 'cpu'
        self.ddp = False
        self.dp = False
        self.devices = '0,1,2,3'

        # LLM-based model
        self.gpt_layers = 6
        self.patch_size = 16
        self.kernel_size = 25
        self.stride = 8

        self.context_length = self.seq_len + self.output_token_len

class IoTDBDataset(Dataset):

    def __init__(self, sorted_series:list, start_index:int, end_index:int, output_tokens:int, input_tokens:int, predict_length:int):
        self.sorted_series = sorted_series
        self.start_index = start_index
        self.end_index = end_index
        self.output_tokens = output_tokens
        self.input_tokens = input_tokens
        self.context_length = predict_length + output_tokens
        self.predict_length = predict_length
        self.iotdb_session = TableSession(config)

    def __getitem__(self, index):
        window_index = index + self.start_index

        series_index = 0

        while self.sorted_series[series_index][2] < window_index:
            series_index += 1

        if series_index != 0:
            window_index -= self.sorted_series[series_index - 1][2]

        # while self.sorted_series[series_index][1] - real_index < self.context_len:
        #     series_index += 1
        if window_index != 0:
            window_index -= 1
        series = self.sorted_series[series_index - 1][0]
        schema = series.split(".")

        start_time = time.time()
        result = []
        try:
            with self.iotdb_session.execute_query_statement(FETCH_SERIES_SQL % (schema[0], schema[1], schema[2], window_index, self.context_length)) as query_result:
                while query_result.has_next():
                    result.append(query_result.next().get_fields()[0].get_double_value())
        except Exception:
            print("error")
        result = torch.tensor(result)
        x_mark = torch.zeros((result.shape[0], 1))
        y_mark = torch.zeros((result.shape[0], 1))
        logger.info("fetch data from iotdb cost %s", time.time() - start_time)
        return result[0:self.predict_length].unsqueeze(-1), result[-self.output_tokens:].unsqueeze(-1), x_mark, y_mark

    def __len__(self):
        return self.end_index - self.start_index - 1


def _fetch_schema(table_schema_list: list[ITableSchema], args:TrainingParameters) -> (int, list):
    iotdb_session = TableSession(config)
    series_to_length = {}
    for table_schema in table_schema_list:
        series_list = []
        with iotdb_session.execute_query_statement(SELECT_SERIES_FORMAT_SQL % (table_schema.database, table_schema.tableName)) as show_devices_result:
            while show_devices_result.has_next():
                series_list.append(str(show_devices_result.next().get_fields()[0]))

        for series in series_list:
            with iotdb_session.execute_query_statement(COUNT_SERIES_LENGTH_SQL % (table_schema.database, table_schema.tableName, series)) as count_series_result:
                length = int(str(count_series_result.next().get_fields()[0]))
                series_to_length[SERIES_NAME % (table_schema.database, table_schema.tableName, series)] = length

    iotdb_session.close()

    sorted_series = sorted(series_to_length.items(), key=lambda x: x[1])
    sorted_series_with_prefix_sum = []
    current_sum = 0
    window_count = 0
    for seq_name, seq_length in sorted_series:
        window_count = seq_length - args.context_length + 1
        current_sum += window_count
        sorted_series_with_prefix_sum.append((seq_name, window_count , current_sum))

    return window_count, sorted_series_with_prefix_sum


class TrainingManager:
    def __init__(self):
        pass

    def create_training_task(self, model_id:str, model_type, table_list: list[ITableSchema], parameters: dict[str, str]):
        args = get_args()
        args.model = "timer_xl"

        total_count, sorted_series = _fetch_schema(table_list, args)

        training_dataset = IoTDBDataset(sorted_series, 0, total_count, args.input_token_len, args.output_token_len, args.seq_len)
        training_dataloader = DataLoader(training_dataset, batch_size=args.batch_size, shuffle=True, num_workers=1)
        exp = ExpForecast(args)

        exp.train(training_dataloader)

        return get_status(TSStatusCode.SUCCESS_STATUS)
        # validation_dataset = IoTDBDataset()


def get_args():
    args = TrainingParameters()
    fix_seed = args.seed
    torch.manual_seed(fix_seed)
    np.random.seed(fix_seed)

    return args
