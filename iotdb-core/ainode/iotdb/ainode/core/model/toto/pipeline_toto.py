import torch

from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline
from iotdb.ainode.core.model.toto.data.util.dataset import MaskedTimeseries


class TotoPipeline(ForecastPipeline):
    def __init__(self, model_info, **model_kwargs):
        super().__init__(model_info, **model_kwargs)

    def preprocess(self, inputs, **infer_kwargs):
        super().preprocess(inputs, **infer_kwargs)
        processed_inputs = []

        for item in inputs:
            targets = item["targets"]
            if targets.ndim == 1:
                targets = targets.unsqueeze(0)

            variate_count, series_len = targets.shape
            device = targets.device

            processed_inputs.append(
                MaskedTimeseries(
                    series=targets,
                    padding_mask=torch.ones(
                        (variate_count, series_len), dtype=torch.bool, device=device
                    ),
                    id_mask=torch.arange(
                        variate_count, dtype=torch.int64, device=device
                    ).unsqueeze(-1).expand(variate_count, series_len),
                    timestamp_seconds=torch.arange(
                        series_len, dtype=torch.int64, device=device
                    ).unsqueeze(0).expand(variate_count, series_len),
                    time_interval_seconds=torch.ones(
                        variate_count, dtype=torch.int64, device=device
                    ),
                    num_exogenous_variables=0,
                )
            )

        return processed_inputs

    def forecast(self, inputs, **infer_kwargs):
        output_length = infer_kwargs.get("output_length", 96)
        num_samples = infer_kwargs.get("num_samples", None)

        outputs = []
        for item in inputs:
            forecast = self.model.forecast(
                item,
                prediction_length=output_length,
                num_samples=num_samples,
            )
            mean = forecast.mean
            if mean.ndim == 3 and mean.shape[0] == 1:
                mean = mean.squeeze(0)
            outputs.append(mean)
        return outputs

    def postprocess(self, outputs, **infer_kwargs):
        return super().postprocess(outputs, **infer_kwargs)
