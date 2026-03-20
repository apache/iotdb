import torch

from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline

class PatchTSTFMPipeline(ForecastPipeline):
    def __init__(self, model_info, **model_kwargs):
        super().__init__(model_info, **model_kwargs)

    def preprocess(self, inputs, **infer_kwargs):
        inputs = super().preprocess(inputs, **infer_kwargs)
        for idx, item in enumerate(inputs):
            # Model expects float32
            target_tensor = item["targets"].to(torch.float32)
            
            # Expand 1D tensor [length] to [batch=1, length]
            if target_tensor.ndim == 1:
                target_tensor = target_tensor.unsqueeze(0)
                
            item["targets"] = target_tensor
        return inputs

    def forecast(self, inputs, **infer_kwargs) -> list[torch.Tensor]:
        """
        TODO: YOU WRITE THIS.
        1. Create an empty list called `forecasts`.
        2. Iterate through the [inputs](cci:1://file:///Users/karthik/Documents/projects/iotdb/iotdb-core/ainode/iotdb/ainode/core/model/chronos2/pipeline_chronos2.py:143:4-178:77) list.
        3. For each input dictionary, extract the "targets" tensor.
        4. Extract the prediction length using `infer_kwargs.get("output_length", 96)`.
        5. Move the tensor to the model's device: `tensor = tensor.to(self.device)`
        6. IBM's PatchTST expects "past_values" and "prediction_length" as arguments.
           Run the forward pass inside a `with torch.no_grad():` block natively using:
           output = self.model(past_values=tensor, prediction_length=pred_len)
        7. Extract the forecast using `output.prediction_outputs` and append it to your list.
        8. Return the list.
        """
        forecasts = []
        for input in inputs:
            targets = input['targets']
            pred_length = infer_kwargs.get("output_length", 96)
            tensor = targets.to(self.device)
            with torch.no_grad():
                output = self.model(past_values = tensor, prediction_length = pred_length)

            forecasts.append(output.prediction_outputs)
        return forecasts







        

    def postprocess(self, outputs: list[torch.Tensor], **infer_kwargs) -> list[torch.Tensor]:
        """
        The IBM Model returns quantiles [batch, variates, prediction_length, quantiles].
        We reduce this to [variates, prediction_length] by taking the median or mean.
        """
        final_outputs = []
        for output in outputs:
            # Remove batch dimension if it is just a single batch
            if output.ndim == 4:
                output = output.squeeze(0)  
            
            # Average out the quantiles to get a point forecast
            point_forecast = output.mean(dim=-1)
            final_outputs.append(point_forecast)
            
        return super().postprocess(final_outputs, **infer_kwargs)
