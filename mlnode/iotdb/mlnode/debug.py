from datats.data_factory import create_forecasting_dataset
from iotdb.mlnode.datats.offline.data_source import DataSource
from algorithm.model_factory import create_forecast_model
from iotdb.Session import Session


def debug_data_config():
    data_config = {
        # ---configs for DataSource---
        'source_type': 'file',  # or 'sql'
        'filename': 'dataset/exchange_rate/exchange_rate.csv',

        # should obtain a session, while source_type='sql' (ununsed in current config)
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
    # task_type
    # input_len
    # pred_len
    # input_vars
    # output_vars
    # learning_rate
    # batch_size=32
    # num_workers=0
    # epochs=10
    # lradj='type1'
    # metric_name
    pass


# def debug_dataset():
#     ip = '127.0.0.1'
#     port = '6667'
#     username = 'root'
#     password = 'root'
#     session = Session(ip, port, username, password, fetch_size=1024, zone_id='UTC+8')
#     session.open(False)
#     sql = {
#         'train': 'SELECT * FROM root.eg.etth1 WHERE Time<=2017-08-01',
#         'val': 'SELECT * FROM root.eg.etth1 WHERE Time>2017-08-01 and Time<2018-01-01',  # overlap lead to sample loss
#         'test': 'SELECT * FROM root.eg.etth1 WHERE Time>=2018-01-01',
#     }
#     input_len = 96
#     pred_len = 96
#     time_embed = 'h'
#
#     train_ds = DataSource(type='sql', session=session, sql=sql['train'])
#     train_dataset, train_cfg = create_forecasting_dataset('window', data_source=train_ds, input_len=input_len,
#                                                           pred_len=pred_len, time_embed=time_embed)
#     val_ds = DataSource(type='sql', session=session, sql=sql['val'])
#     val_dataset, val_cfg = create_forecasting_dataset('window', data_source=val_ds, input_len=input_len,
#                                                       pred_len=pred_len, time_embed=time_embed)
#     test_ds = DataSource(type='sql', session=session, sql=sql['test'])
#     test_dataset, test_cfg = create_forecasting_dataset('window', data_source=test_ds, input_len=input_len,
#                                                         pred_len=pred_len, time_embed=time_embed)
#
#     return train_dataset, train_cfg


# def debug_model():
#     model_name = 'dlinear'
#     input_len = 96
#     pred_len = 96
#     input_vars = 7  # dataset_cfg['input_vars']
#     output_vars = 7  # dataset_cfg['output_vars']
#     kernel_size = 25
#
#     model, model_cfg = create_forecast_model(
#         model_name=model_name,
#         input_len=input_len,
#         pred_len=pred_len,
#         input_vars=input_vars,
#         output_vars=output_vars,
#         kernel_size=kernel_size
#     )
#
#     return model, model_cfg


if __name__ == '__main__':
    model, model_cfg = debug_model()
    print(model_cfg)
    dataset, dataset_config = debug_dataset()
    print(dataset_config)

# import argparse
# import torch

# def default_configs():
# configs = argparse.Namespace(
#     freq='h',
#     seq_len=96,
#     pred_len=96,

#     output_attention=False,
#     d_model=512,

#     n_heads=8,
#     e_layers=2,
#     d_layers=1,
#     d_ff=2048,
#     dropout=0.05,
#     activation='gelu',
#     p_hidden_dims=[64, 64],
#     individual = 'True',
#     num_series=7,

#     learning_rate = 0.0001,
#     batch_size=32,
#     num_workers=0,
#     epochs=10,
#     lradj='type1',
# )
#     configs.device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
#     configs.device = 'cpu'
#     return configs

# def debug_dataset():
#     ip = '127.0.0.1'
#     port = '6667'
#     username = 'root'
#     password = 'root'
#     session = Session(ip, port, username, password, fetch_size=1024, zone_id='UTC+8')
#     session.open(False)

#     # configs = iotdb.ml.sql_config

#     configs = default_configs()


#     # see from Grafana
#     sql = {
#         'train': 'SELECT * FROM root.eg.etth1 WHERE Time<=2017-08-01',
#         'val': 'SELECT * FROM root.eg.etth1 WHERE Time>2017-08-01 and Time<2018-01-01', # overlap lead to sample loss
#         'test': 'SELECT * FROM root.eg.etth1 WHERE Time>=2018-01-01',
#     }


#     # dataset = iotdb.ml.get_dataset

#     # dataset = iotdb.ml.get_dataset
#     train_dataset = WindowDataset_OFL(session, sql['train'], seq_len=configs.seq_len, pred_len=configs.pred_len, label_len=configs.label_len)
#     val_dataset = WindowDataset_OFL(session, sql['val'], seq_len=configs.seq_len, pred_len=configs.pred_len, label_len=configs.label_len)
#     test_dataset = WindowDataset_OFL(session, sql['test'], seq_len=configs.seq_len, pred_len=configs.pred_len, label_len=configs.label_len)
#     configs.enc_in=train_dataset.get_variable_num()
#     configs.dec_in=train_dataset.get_variable_num()
#     configs.c_out=train_dataset.get_variable_num()
#     print('train samples: ', len(train_dataset))
#     print('val samples: ', len(val_dataset))
#     print('test samples: ', len(test_dataset))
#     train_loader = DataLoader(
#         train_dataset,
#         batch_size=configs.batch_size,
#         shuffle=True,
#         num_workers=configs.num_workers,
#         drop_last=True)
#     val_loader = DataLoader(
#         val_dataset,
#         batch_size=configs.batch_size,
#         shuffle=False,
#         num_workers=configs.num_workers,
#         drop_last=True)
#     test_loader = DataLoader(
#         test_dataset,
#         batch_size=configs.batch_size,
#         shuffle=False,
#         num_workers=configs.num_workers,
#         drop_last=True)

#     return train_dataset, train_loader, val_dataset, val_loader

# def debug_inference_data():
#     ip = '127.0.0.1'
#     port = '6667'
#     username = 'root'
#     password = 'root'
#     session = Session(ip, port, username, password, fetch_size=1024, zone_id='UTC+8')
#     session.open(False)

#     # configs = iotdb.ml.sql_config

#     configs = default_configs()


#     # see from Grafana
#     sql = {
#         'test': 'SELECT * FROM root.eg.etth1 WHERE Time>=2018-01-01'
#     }
#     result = session.execute_query_statement(sql["test"])
#     assert result # return None if fail to fetch
#     df_raw = result.todf()

#     return df_raw
