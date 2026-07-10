import numpy as np
import torch

from iotdb.tsfile.utils.tsblock_serde import deserialize


def _expand_null_values(
    data: np.ndarray, null_indicator, position_count: int
) -> np.ndarray:
    if null_indicator is None:
        return data

    expanded = np.full(position_count, np.nan, dtype=np.float32)
    if data.size > 0:
        expanded[np.logical_not(null_indicator)] = data.astype(np.float32)
    return expanded


# Full data deserialized from iotdb tsblock is composed of [timestampList, multiple valueList, None, length].
def convert_tsblock_to_tensor_and_timestamps(
    tsblock_data: bytes,
) -> tuple[torch.Tensor, list[int]]:
    full_data = deserialize(tsblock_data)
    # ensure the byteorder is correct.
    for i, data in enumerate(full_data[1]):
        if data.dtype.byteorder not in ("=", "|"):
            np_data = data.byteswap()
            full_data[1][i] = np_data.view(np_data.dtype.newbyteorder())
        full_data[1][i] = _expand_null_values(
            full_data[1][i], full_data[2][i], full_data[3]
        )
    # tensor_data: [batch_size, target_count, input_length]
    tensor_data = torch.from_numpy(np.stack(full_data[1], axis=0)).unsqueeze(0).float()
    # timestamps: [input_length,]
    timestamps: np.ndarray = full_data[0]
    # data should be on CPU before passing to the inference request
    return tensor_data.to("cpu"), timestamps.tolist()
