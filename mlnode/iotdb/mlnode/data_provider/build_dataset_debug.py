from .offline_dataset import WindowDataset_OFL

from iotdb.Session import Session
from torch.utils.data import DataLoader

import argparse
import torch

def debug_configs():
    configs = argparse.Namespace(
        seq_len=96,
        pred_len=96,
        label_len=48,
        output_attention=False,
        d_model=512,
        embed='timeF',
        freq='h',
        n_heads=8,
        e_layers=2,
        d_layers=1,
        d_ff=2048,
        dropout=0.05,
        activation='gelu',
        p_hidden_dims=[64, 64],
        individual = 'True',
        num_series=7,

        learning_rate = 0.0001,
        batch_size=32,
        num_workers=0,
        epochs=10,
        lradj='type1',
    )
    configs.device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
    # configs.device = 'cpu'
    return configs

def debug_dataset():
    ip = '127.0.0.1'
    port = '6667'
    username = 'root'
    password = 'root'
    session = Session(ip, port, username, password, fetch_size=1024, zone_id='UTC+8')
    session.open(False)

    # configs = iotdb.ml.sql_config
    
    configs = debug_configs()


    # see from Grafana
    sql = {
        'train': 'SELECT * FROM root.eg.etth1 WHERE Time<=2017-08-01',
        'val': 'SELECT * FROM root.eg.etth1 WHERE Time>2017-08-01 and Time<2018-01-01', # overlap lead to sample loss
        'test': 'SELECT * FROM root.eg.etth1 WHERE Time>=2018-01-01',
    }


    # dataset = iotdb.ml.get_dataset
    train_dataset = WindowDataset_OFL(session, sql['train'], seq_len=configs.seq_len, pred_len=configs.pred_len, label_len=configs.label_len)
    configs.enc_in=train_dataset.get_variable_num()
    configs.dec_in=train_dataset.get_variable_num()
    configs.c_out=train_dataset.get_variable_num()
    train_loader = DataLoader(
        train_dataset,
        batch_size=configs.batch_size,
        shuffle=True,
        num_workers=configs.num_workers,
        drop_last=True)

    return train_dataset, train_loader