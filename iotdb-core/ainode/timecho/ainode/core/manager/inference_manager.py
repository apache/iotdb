import torch

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.exception import NumericalRangeException
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.inference_manager import InferenceManager
from iotdb.ainode.core.rpc.status import get_status
from iotdb.ainode.core.util.serde import convert_tsblock_to_tensor
from iotdb.thrift.ainode.ttypes import (
    TClassifyReq,
    TClassifyResp,
    TForecastReq,
    TForecastResp,
)
from timecho.ainode.core.ingress.data_fetcher import IoTDBDataFetcher

logger = Logger()


class TimechoInferenceManager(InferenceManager):
    def __init__(self):
        super().__init__()

    def _get_covariate_if_needed(
        self,
        inputs: torch.Tensor,  # [batch_size(1), target_count, input_length]
        history_covs_sql: str,
        future_covs_sql: str,
    ) -> list[dict[str, torch.Tensor | dict[str, torch.Tensor]]]:
        """
        Fetches the historical and future covariates based on provided SQL queries and combines them with the input data.

        Args:
            inputs (torch.Tensor): A tensor of shape [batch_size(1), target_count, input_length] representing the input data.
            history_covs_sql (str): SQL query to fetch historical covariates.
            future_covs_sql (str): SQL query to fetch future covariates.

        Returns:
            list: Each is a dict, which contains the following keys:
                - `targets`: The input tensor for the target variable(s), whose shape is [target_count, input_length].
                - `past_covariates` (optional): A dictionary of past covariates (if `history_covs_sql` is provided).
                - `future_covariates` (optional): A dictionary of future covariates (if `future_covs_sql` is provided).

        Raises:
            ValueError: If `future_covs_sql` is provided without `history_covs_sql`, or if no covariates are found.
        """
        DEFAULT_TAG_VALUE = ("__DEFAULT_TAG__",)
        model_inputs: list[dict[str, torch.Tensor | dict[str, torch.Tensor]]] = []

        history_covs: dict[str, torch.Tensor] = {}
        future_covs: dict[str, torch.Tensor] = {}

        # Ensure both history and future covariates are valid
        if future_covs_sql and not history_covs_sql:
            logger.error(
                "[Inference] Future_covs_sql is specified yet history_covs_sql is None."
            )
            raise ValueError(
                "Future_covs_sql is specified yet history_covs_sql is None."
            )

        # Fetch history covariates if provided
        if history_covs_sql:
            data_fetcher = IoTDBDataFetcher()
            history_covs_all, history_timestamps_all = data_fetcher.fetch_data(
                history_covs_sql
            )
            if DEFAULT_TAG_VALUE not in history_covs_all:
                logger.error(
                    "[Inference] Tag other than default tag not supported now."
                )
                raise ValueError("Tag other than default tag not supported now.")
            else:
                history_covs = history_covs_all[DEFAULT_TAG_VALUE]
                if not history_covs:
                    logger.error(
                        "[Inference] The history covariates are specified but no history data are selected."
                    )
                    raise ValueError(
                        "The history covariates are specified but no history data are selected."
                    )
                if len(history_covs_all) > 1:
                    logger.warning(
                        "[Inference] Tag other than default tag will be ignored now."
                    )

            # Fetch future covariates if provided
            if future_covs_sql:
                future_covs_all, future_timestamps_all = data_fetcher.fetch_data(
                    future_covs_sql
                )
                if DEFAULT_TAG_VALUE not in future_covs_all:
                    logger.error(
                        "[Inference] Tag other than default tag not supported now."
                    )
                    raise ValueError("Tag other than default tag not supported now.")
                else:
                    future_covs = future_covs_all[DEFAULT_TAG_VALUE]
                    if not future_covs:
                        logger.error(
                            "[Inference] The future covariates are specified but no future data are selected."
                        )
                        raise ValueError(
                            "The future covariates are specified but no future data are selected."
                        )
                    if len(future_covs_all) > 1:
                        logger.warning(
                            "[Inference] Tag other than default tag will be ignored now."
                        )

        model_inputs.append({"targets": inputs[0]})
        # Combine covariates if available
        if history_covs:
            model_inputs[0].update({"past_covariates": history_covs})
            if future_covs:
                model_inputs[0].update({"future_covariates": future_covs})

        # Note: Currently, only contain one dict in list
        return model_inputs

    def _run_classify(self, req, data_getter, extract_attrs, resp_cls, single_batch):
        model_id = req.modelId
        try:
            raw = data_getter(req)

            inputs = convert_tsblock_to_tensor(raw)
            inference_attrs = extract_attrs(req)

            resp_list = self._do_inference_and_construct_resp(
                model_id, inputs, inference_attrs
            )

            return resp_cls(
                get_status(TSStatusCode.SUCCESS_STATUS),
                [resp_list[0]] if single_batch else resp_list,
            )
        except Exception as e:
            logger.error(e)
            status = get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))
            empty = b"" if single_batch else []
            return resp_cls(status, empty)

    def _run_forecast(
        self,
        req,
        data_getter,
        extract_attrs,
        resp_cls,
        single_batch: bool,
    ):
        model_id = req.modelId
        try:
            raw = data_getter(req)

            # inputs: [batch_size(1), target_count, input_length]
            inputs = convert_tsblock_to_tensor(raw)

            inference_attrs = extract_attrs(req)

            output_length = int(inference_attrs.get("output_length", 96))
            if (
                output_length
                > AINodeDescriptor().get_config().get_ain_inference_max_output_length()
            ):
                raise NumericalRangeException(
                    "output_length",
                    output_length,
                    1,
                    AINodeDescriptor()
                    .get_config()
                    .get_ain_inference_max_output_length(),
                )

            history_covs_sql = str(inference_attrs.pop("history_covs", ""))
            future_covs_sql = str(inference_attrs.pop("future_covs", ""))
            model_inputs: list[dict[str, torch.Tensor | dict[str, torch.Tensor]]] = (
                self._get_covariate_if_needed(inputs, history_covs_sql, future_covs_sql)
            )

            resp_list = self._do_inference_and_construct_resp(
                model_id,
                model_inputs,
                inference_attrs,
            )

            return resp_cls(
                get_status(TSStatusCode.SUCCESS_STATUS),
                [resp_list[0]] if single_batch else resp_list,
            )

        except Exception as e:
            logger.error(e)
            status = get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))
            empty = b"" if single_batch else []
            return resp_cls(status, empty)

    def forecast(self, req: TForecastReq):
        return self._run_forecast(
            req,
            data_getter=lambda r: r.inputData,
            extract_attrs=lambda r: {
                "output_length": r.outputLength,
                "history_covs": r.historyCovs or "",
                "future_covs": r.futureCovs or "",
                "auto_adapt": r.autoAdapt,
                **(r.options or {}),
            },
            resp_cls=TForecastResp,
            single_batch=True,
        )

    def classify(self, req: TClassifyReq):
        return self._run_classify(
            req,
            data_getter=lambda r: r.inputData,
            extract_attrs=lambda r: {
                **(r.options or {}),
            },
            resp_cls=TClassifyResp,
            single_batch=True,
        )
