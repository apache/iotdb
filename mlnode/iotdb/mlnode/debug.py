# from datats.data_factory import create_forecasting_dataset
# from iotdb.mlnode.datats.offline.data_source import DataSource
# from algorithm.model_factory import create_forecast_model
# from iotdb.Session import Session


def debug_config():
    config = {
        'source_type': 'file',
        'filename': 'dataset/exchange_rate/exchange_rate.csv',
        'ip': '127.0.0.1',
        'port': '6667',
        'username': 'root',
        'password': 'root',
        'sql': {
            'train': 'SELECT * FROM root.eg.etth1 WHERE Time<=2017-08-01',
            'val': 'SELECT * FROM root.eg.etth1 WHERE Time>2017-08-01 and Time<2018-01-01',
            # overlap lead to sample loss
            'test': 'SELECT * FROM root.eg.etth1 WHERE Time>=2018-01-01',
        },
        'dataset_type': 'window',
        'time_embed': 'h',
        'input_len': 96,
        'pred_len': 96,
        'model_name': 'dlinear',
        'input_vars': 7,
        'output_vars': 7,
        'task_type': 'm',
        'kernel_size': 25,
        'block_type': 'g',
        'd_model': 128,
        'inner_layers': 4,
        'outer_layers': 4,
        'model_id': 0,
        'learning_rate': 1e-4,
        'batch_size': 32,
        'num_workers': 0,
        'epochs': 10,
        'metric_name': ['MSE', 'MAE'],
    }
    return config


def debug_data_config():
    data_config = {
        # ---configs for DataSource---
        'source_type': 'file',  # or 'sql'
        'filename': 'dataset/ETT-small/ETTh2.csv',

        # should obtain a session, when source_type='sql' (unused in current config)
        # this session is pre-built by mlnode, not from training request

        # 'ip': '127.0.0.1',
        # 'port': '6667',
        # 'username': 'root',
        # 'password': 'root',
        # 'sql': {
        #     'train': 'SELECT * FROM root.eg.etth1 WHERE Time<=2017-08-01',
        #     'val': 'SELECT * FROM root.eg.etth1 WHERE Time>2017-08-01 and Time<2018-01-01',
        #     # overlap lead to sample loss
        #     'test': 'SELECT * FROM root.eg.etth1 WHERE Time>=2018-01-01',
        # },

        'query_expressions': ['SELECT * FROM root.eg.etth1 WHERE Time<=2017-08-01', 'SELECT * FROM root.eg.etth1 WHERE Time>2017-08-01 and Time<2018-01-01'],
        'query_filter': '',
        # ---configs for Dataset---
        'dataset_type': 'window',  # or 'timeseries'
        'time_embed': 'h',
        'input_len': 96,
        'pred_len': 96,
    }

    return data_config


def debug_model_config():
    model_cfg = {
        # ---configs for Model---
        'model_name': 'dlinear',  # or 'dlinear_individual', 'nbeats'
        'input_len': 96,
        'pred_len': 96,
        'input_vars': 7,  # can obtain from dataset_cfg
        'output_vars': 7,
        'task_type': 'm',
        'kernel_size': 25,
        'block_type': 'g',  # (ununsed in current config)
        'd_model': 128,
        'inner_layers': 4,
        'outer_layers': 4,
    }
    return model_cfg


def debug_trial_config():
    #TODO
    # model_id
    trial_config = {
        'task_type': 'forecast',
        'input_len': 96,
        'pred_len': 96,
        'input_vars': 7,
        'output_vars': 7,
        'learning_rate': 1e-4,
        'batch_size': 32,
        'num_workers': 1,
        'epochs': 10,
        'metric_name': 'MSE',
        'tuning': False,
        'model_id': 'debug'
    }
    return trial_config

