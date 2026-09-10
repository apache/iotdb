# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import logging

import pandas as pd
from pandas._libs import OutOfBoundsDatetime
from tzlocal import get_localzone_name

from iotdb.thrift.common.ttypes import TSStatus
from iotdb.utils.exception import RedirectException, StatementExecutionException

logger = logging.getLogger("IoTDB")

SUCCESS_STATUS = 200
MULTIPLE_ERROR = 302
REDIRECTION_RECOMMEND = 400


def verify_success(status: TSStatus):
    """
    verify success of operation
    :param status: execution result status
    """
    if status.code == MULTIPLE_ERROR:
        verify_success_by_list(status.subStatus)
        return 0
    if status.code == SUCCESS_STATUS or status.code == REDIRECTION_RECOMMEND:
        return 0

    raise StatementExecutionException(status)


def verify_success_by_list(status_list: list):
    """
    verify success of operation
    :param status_list: execution result status
    """
    error_messages = [
        status.message
        for status in status_list
        if status.code not in {SUCCESS_STATUS, REDIRECTION_RECOMMEND}
    ]
    if error_messages:
        message = f"{MULTIPLE_ERROR}: {'; '.join(error_messages)}"
        raise StatementExecutionException(message=message)


def verify_success_with_redirection(status: TSStatus):
    verify_success(status)
    if status.redirectNode is not None:
        raise RedirectException(status.redirectNode)
    return 0


def verify_success_with_redirection_for_multi_devices(status: TSStatus, devices: list):
    verify_success(status)
    if status.code == MULTIPLE_ERROR or status.code == REDIRECTION_RECOMMEND:
        device_to_endpoint = {}
        for i in range(len(status.subStatus)):
            if status.subStatus[i].redirectNode is not None:
                device_to_endpoint[devices[i]] = status.subStatus[i].redirectNode
        raise RedirectException(device_to_endpoint)


def convert_to_timestamp(time: int, precision: str, timezone: str):
    try:
        ts = pd.Timestamp(time, unit=precision, tz=timezone)
    except OutOfBoundsDatetime:
        try:
            ts = pd.Timestamp(time, unit=precision).tz_localize(timezone)
        except NotImplementedError:
            return pd.Timestamp(time, unit=precision)
    except ValueError:
        logger.warning(
            f"Timezone string '{timezone}' cannot be recognized by pandas. "
            f"Falling back to local timezone: '{get_localzone_name()}'."
        )
        ts = pd.Timestamp(time, unit=precision, tz=get_localzone_name())
    except NotImplementedError:
        # Timestamp falls outside Python's stdlib datetime range (year < 1 or
        # > 9999). pandas >= 3.0 cannot attach a timezone to such Timestamps
        # because tz conversion relies on toordinal(). Return naive instead.
        return pd.Timestamp(time, unit=precision)
    # Construction succeeded but utcoffset() may still fail downstream (e.g.
    # in pd.DataFrame's tz alignment) when the Timestamp's year is outside
    # 1..9999 and tz is not the canonical UTC. Probe early and fall back to
    # naive so callers like result_set_to_pandas() don't blow up. NaT does
    # not need this check (and does not support utcoffset()).
    if ts is not pd.NaT:
        try:
            ts.utcoffset()
        except NotImplementedError:
            return pd.Timestamp(time, unit=precision)
    return ts


unit_map = {
    "ms": "milliseconds",
    "us": "microseconds",
    "ns": "nanoseconds",
}


def isoformat(ts: pd.Timestamp, unit: str):
    if unit not in unit_map:
        raise ValueError(f"Unsupported unit: {unit}")
    try:
        return ts.isoformat(timespec=unit_map[unit])
    except ValueError:
        logger.warning(
            f"Timezone string '{unit_map[unit]}' cannot be recognized by old version pandas. "
            f"Falling back to use auto timespec'."
        )
        return ts.isoformat()
    except NotImplementedError:
        # Timestamp falls outside Python's stdlib datetime range. pandas >= 3.0
        # routes isoformat through datetime.isoformat, which calls toordinal()
        # and fails. Build the ISO 8601 string from individual components.
        return _isoformat_from_components(ts, unit)


def _isoformat_from_components(ts: pd.Timestamp, unit: str) -> str:
    base = (
        f"{ts.year:04d}-{ts.month:02d}-{ts.day:02d}"
        f"T{ts.hour:02d}:{ts.minute:02d}:{ts.second:02d}"
    )
    if unit == "ms":
        base += f".{ts.microsecond // 1000:03d}"
    elif unit == "us":
        base += f".{ts.microsecond:06d}"
    elif unit == "ns":
        base += f".{ts.microsecond:06d}{ts.nanosecond:03d}"
    if ts.tzinfo is not None:
        try:
            offset = ts.utcoffset()
        except NotImplementedError:
            # utcoffset() needs toordinal() for zones like Etc/UTC. Omit offset.
            offset = None
        if offset is not None:
            total_seconds = int(offset.total_seconds())
            sign = "+" if total_seconds >= 0 else "-"
            total_seconds = abs(total_seconds)
            hh, mm = divmod(total_seconds // 60, 60)
            base += f"{sign}{hh:02d}:{mm:02d}"
    return base
