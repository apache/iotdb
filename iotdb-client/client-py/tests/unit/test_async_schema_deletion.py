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

from unittest.mock import Mock, patch

import pytest

from tests.integration.utils import assert_timeseries_deleted


def test_waits_until_every_deleted_timeseries_disappears():
    session = Mock()
    session.check_time_series_exists.side_effect = [False, True, False, False]
    with patch("tests.integration.utils.time.sleep") as sleep:
        assert_timeseries_deleted(session, ["root.test.s1", "root.test.s2"])
    sleep.assert_called_once()
    assert session.check_time_series_exists.call_count == 4


def test_deletion_timeout_reports_remaining_paths():
    session = Mock()
    session.check_time_series_exists.side_effect = [False, True]
    with pytest.raises(AssertionError, match=r"root.test.s2"):
        assert_timeseries_deleted(session, ["root.test.s1", "root.test.s2"], timeout=0)


def test_completed_deletion_does_not_sleep():
    session = Mock()
    session.check_time_series_exists.return_value = False
    with patch("tests.integration.utils.time.sleep") as sleep:
        assert_timeseries_deleted(session, ["root.test.s1"], timeout=0)
    sleep.assert_not_called()


def test_deletion_check_does_not_hide_query_errors():
    session = Mock()
    session.check_time_series_exists.side_effect = RuntimeError("connection lost")
    with pytest.raises(RuntimeError, match="connection lost"):
        assert_timeseries_deleted(session, ["root.test.s1"])
