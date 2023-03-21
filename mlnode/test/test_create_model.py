import torch
from iotdb.mlnode.algorithm.model_factory import create_forecast_model


def test_create_model():
    model, model_cfg = create_forecast_model(model_name='dlinear')
    assert model
    assert model_cfg['model_name'] == 'dlinear'

    model, model_cfg = create_forecast_model(
        model_name='nbeats',
        input_len=96,
        pred_len=96,
        input_vars=7,
        output_vars=7,
    )
    sample_input = torch.randn(1, model_cfg['input_len'], model_cfg['input_vars'])
    sample_output = model(sample_input)
    assert model
    assert model_cfg['model_name'] == 'nbeats'
    assert sample_output.shape[1] == model_cfg['pred_len']
    assert sample_output.shape[2] == model_cfg['output_vars']

