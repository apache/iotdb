from .offline.dataset import *

def _dataset_cfg(**kwargs):
    return {
        'dataset_type': 'WindowDataset',
        'input_len': 96,
        'pred_len': 96,
        'time_embed': 'h',
        **kwargs
    }

support_dataset_cfgs = {
    'timeseries': _dataset_cfg(
        dataset_type='TimeSeriesDataset',
        input_len=0,
        pred_len=0),

    'window': _dataset_cfg(
        dataset_type='WindowDataset',)
}


def create_forecasting_dataset(
    dataset_type,
    data_source=None,
    **kwargs,
):
    """ 
    Factory method for all support dataset
    currently implement WindowDataset, TimeSeriesDataset
    for specific dataset configs, see __dataset_cfg

    Args:
        dataset_type: available choice in ['window', 'timeseries']
        datasource: offline multi-variate time series for a while (all pre-fetched in memory)

    Returns:
        model: torch.nn.Module
        model_config: dict of model configurations
    """
    
    if dataset_type not in support_dataset_cfgs.keys():
        raise RuntimeError('Unknown dataset type (%s)' % dataset_type)

    dataset_cfg = support_dataset_cfgs[dataset_type]
    dataset_cls = eval(dataset_cfg['dataset_type'])
    dataset = dataset_cls(data_source, **kwargs)
    dataset_cfg.update(**kwargs)
    # for multivariate only
    dataset_cfg['input_vars'] = dataset.get_variable_num()
    dataset_cfg['output_vars'] = dataset.get_variable_num()
    
    return dataset, dataset_cfg
