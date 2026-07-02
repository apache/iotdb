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

import json
import os

import pytest

from iotdb.ainode.core.model.model_constants import (
    MANIFEST_TEMP_SUFFIX,
    MANIFEST_VERSION,
    USER_DEFINED_MANIFEST,
)
from iotdb.ainode.core.model.model_manifest import ModelManifest


def _entry(model_type="timer", base_model_id=""):
    return {
        "model_type": model_type,
        "pipeline_cls": "pipeline_timer.TimerPipeline",
        "auto_map": {
            "AutoConfig": "configuration_timer.TimerConfig",
            "AutoModelForCausalLM": "modeling_timer.TimerForPrediction",
        },
        "hub_mixin_cls": "",
        "base_model_id": base_model_id,
    }


def _manifest_path(d):
    return os.path.join(d, USER_DEFINED_MANIFEST)


def test_load_missing_returns_empty_ok(tmp_path):
    manifest = ModelManifest(str(tmp_path))
    models, ok = manifest.load()
    assert models == {}
    assert ok is True
    assert manifest.file_exists() is False


def test_add_then_reload_roundtrip(tmp_path):
    manifest = ModelManifest(str(tmp_path))
    entry = _entry(base_model_id="timer_xl")
    manifest.add("m1", entry)

    # A fresh instance on the same dir must observe the same content.
    reloaded = ModelManifest(str(tmp_path))
    models, ok = reloaded.load()
    assert ok is True
    assert models["m1"] == entry
    assert reloaded.contains("m1") is True


def test_remove_idempotent(tmp_path):
    manifest = ModelManifest(str(tmp_path))
    manifest.add("m1", _entry())
    manifest.remove("m1")
    assert manifest.contains("m1") is False
    # Removing an absent id must not raise.
    manifest.remove("m1")
    manifest.remove("never-existed")

    reloaded = ModelManifest(str(tmp_path))
    models, ok = reloaded.load()
    assert ok is True
    assert "m1" not in models


def test_atomic_write_leaves_no_tmp(tmp_path):
    manifest = ModelManifest(str(tmp_path))
    manifest.add("m1", _entry())
    assert os.path.exists(_manifest_path(str(tmp_path)))
    assert not os.path.exists(_manifest_path(str(tmp_path)) + MANIFEST_TEMP_SUFFIX)


def test_corrupt_json_returns_not_ok(tmp_path):
    with open(_manifest_path(str(tmp_path)), "w", encoding="utf-8") as f:
        f.write("{ this is not valid json")
    manifest = ModelManifest(str(tmp_path))
    models, ok = manifest.load()  # must not raise
    assert models == {}
    assert ok is False
    # The file still exists (quarantining is the storage layer's job, not load()'s).
    assert manifest.file_exists() is True


def test_unknown_version_returns_not_ok(tmp_path):
    with open(_manifest_path(str(tmp_path)), "w", encoding="utf-8") as f:
        json.dump({"version": 999, "models": {}}, f)
    manifest = ModelManifest(str(tmp_path))
    models, ok = manifest.load()
    assert models == {}
    assert ok is False


def test_missing_models_key_returns_not_ok(tmp_path):
    with open(_manifest_path(str(tmp_path)), "w", encoding="utf-8") as f:
        json.dump({"version": MANIFEST_VERSION}, f)
    manifest = ModelManifest(str(tmp_path))
    models, ok = manifest.load()
    assert models == {}
    assert ok is False


def test_replace_all_bulk(tmp_path):
    manifest = ModelManifest(str(tmp_path))
    bulk = {"a": _entry("t1"), "b": _entry("t2")}
    manifest.replace_all(bulk)

    with open(_manifest_path(str(tmp_path)), "r", encoding="utf-8") as f:
        payload = json.load(f)
    assert payload["version"] == MANIFEST_VERSION
    assert payload["models"] == bulk

    reloaded = ModelManifest(str(tmp_path))
    models, ok = reloaded.load()
    assert ok is True
    assert models == bulk


def test_non_serializable_entry_raises(tmp_path):
    manifest = ModelManifest(str(tmp_path))
    bad_entry = _entry()
    bad_entry["auto_map"] = {"AutoConfig": object()}  # not JSON serializable
    # Must raise loudly rather than silently persist a "<object ...>" string.
    with pytest.raises(TypeError):
        manifest.add("bad", bad_entry)
    # And it must not have corrupted the (still absent) manifest.
    reloaded = ModelManifest(str(tmp_path))
    models, ok = reloaded.load()
    assert ok is True
    assert "bad" not in models


def test_stale_tmp_is_cleaned_on_load(tmp_path):
    # Simulate a crash mid-write that left a stale .tmp behind.
    tmp_file = _manifest_path(str(tmp_path)) + MANIFEST_TEMP_SUFFIX
    with open(tmp_file, "w", encoding="utf-8") as f:
        f.write("partial")
    manifest = ModelManifest(str(tmp_path))
    manifest.load()
    assert not os.path.exists(tmp_file)
