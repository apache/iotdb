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
"""Discovery / manifest tests for :class:`ModelStorage`.

These are the regression tests for the bug where an unrecognized directory
under ``user_defined/`` crashed AINode restart, plus the manifest that fixes it.
Built-in discovery (which downloads from HuggingFace) is stubbed out so the
tests are hermetic.
"""

import json
import os
import shutil
import threading

import pytest

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.exception import ModelExistedException
from iotdb.ainode.core.model.model_constants import (
    CONFIG_JSON,
    MODEL_SAFETENSORS,
    USER_DEFINED_MANIFEST,
    ModelCategory,
    ModelStates,
)
from iotdb.ainode.core.model.model_info import BUILTIN_HF_TRANSFORMERS_MODEL_MAP
from iotdb.ainode.core.model.model_manifest import ModelManifest
from iotdb.ainode.core.model.model_storage import ModelStorage

USER_DEFINED = ModelCategory.USER_DEFINED.value


@pytest.fixture
def models_dir(tmp_path, monkeypatch):
    """Point ModelStorage at a throwaway models dir and stub builtin discovery.

    ``ModelStorage`` computes ``os.path.join(os.getcwd(), get_ain_models_dir())``;
    an absolute ``get_ain_models_dir()`` makes the join independent of cwd.
    """
    root = tmp_path / "models"
    monkeypatch.setattr(
        AINodeDescriptor().get_config(), "_ain_models_dir", str(root), raising=False
    )
    # Builtin discovery downloads weights from HuggingFace; make it a no-op.
    monkeypatch.setattr(ModelStorage, "_discover_builtin_models", lambda self, p: None)
    return root


def _user_dir(models_root):
    return os.path.join(str(models_root), USER_DEFINED)


def _make_model_dir(models_root, model_id, with_config=True, with_weights=True):
    d = os.path.join(_user_dir(models_root), model_id)
    os.makedirs(d, exist_ok=True)
    if with_config:
        with open(os.path.join(d, CONFIG_JSON), "w", encoding="utf-8") as f:
            json.dump({"model_type": "custom", "auto_map": None}, f)
    if with_weights:
        with open(os.path.join(d, MODEL_SAFETENSORS), "wb") as f:
            f.write(b"\x00")
    return d


def _make_src(models_root, name):
    """Create a local source directory suitable for ``register_model``."""
    src = os.path.join(str(models_root), f"src_{name}")
    os.makedirs(src, exist_ok=True)
    with open(os.path.join(src, CONFIG_JSON), "w", encoding="utf-8") as f:
        json.dump({"model_type": "custom", "auto_map": None}, f)
    with open(os.path.join(src, MODEL_SAFETENSORS), "wb") as f:
        f.write(b"\x00")
    return src


def _manifest(models_root):
    return ModelManifest(_user_dir(models_root))


def _user_models(storage):
    return storage._models[USER_DEFINED]


# ==================== Startup robustness (the core bug) ====================


def test_stray_dir_does_not_crash_startup(models_dir):
    # A __pycache__ dir and a config-less/weight-less "broken" dir, no manifest.
    os.makedirs(os.path.join(_user_dir(models_dir), "__pycache__"), exist_ok=True)
    os.makedirs(os.path.join(_user_dir(models_dir), "broken"), exist_ok=True)

    storage = (
        ModelStorage()
    )  # must NOT raise (regression: NameError on config-less dir)

    assert "__pycache__" not in _user_models(storage)
    assert "broken" not in _user_models(storage)


# ==================== First-upgrade bootstrap ====================


def test_bootstrap_adopts_valid_and_skips_strays(models_dir):
    _make_model_dir(models_dir, "m1")
    os.makedirs(os.path.join(_user_dir(models_dir), "__pycache__"), exist_ok=True)
    assert not os.path.exists(
        os.path.join(_user_dir(models_dir), USER_DEFINED_MANIFEST)
    )

    storage = ModelStorage()

    assert "m1" in _user_models(storage)
    assert _user_models(storage)["m1"].state == ModelStates.ACTIVE
    assert "__pycache__" not in _user_models(storage)
    # Bootstrap wrote a manifest listing only m1.
    models, ok = _manifest(models_dir).load()
    assert ok is True
    assert set(models.keys()) == {"m1"}


def test_bootstrap_keeps_model_with_weights_but_corrupt_config(models_dir):
    d = _make_model_dir(models_dir, "m1", with_config=False, with_weights=True)
    with open(os.path.join(d, CONFIG_JSON), "w", encoding="utf-8") as f:
        f.write("{ truncated json")

    storage = ModelStorage()  # must not raise, must not drop m1

    assert "m1" in _user_models(storage)
    assert _user_models(storage)["m1"].state == ModelStates.ACTIVE


# ==================== Steady state trusts the manifest ====================


def test_steady_state_trusts_manifest_only(models_dir):
    _make_model_dir(models_dir, "m1")
    _make_model_dir(models_dir, "m2")  # valid on disk but NOT in the manifest
    _manifest(models_dir).replace_all(
        {"m1": {"model_type": "custom", "auto_map": None}}
    )

    storage = ModelStorage()

    assert "m1" in _user_models(storage)
    assert "m2" not in _user_models(storage)  # not scanned in — manifest is the truth


def test_manifest_entry_missing_dir_is_pruned(models_dir):
    _manifest(models_dir).replace_all(
        {"gone": {"model_type": "custom", "auto_map": None}}
    )

    storage = ModelStorage()  # must not raise

    assert "gone" not in _user_models(storage)
    # Reconciliation rewrote the manifest without the vanished entry.
    models, ok = _manifest(models_dir).load()
    assert ok is True
    assert "gone" not in models


def test_corrupt_manifest_quarantined_and_rebuilt(models_dir):
    _make_model_dir(models_dir, "m1")
    with open(
        os.path.join(_user_dir(models_dir), USER_DEFINED_MANIFEST),
        "w",
        encoding="utf-8",
    ) as f:
        f.write("{ corrupt")

    storage = ModelStorage()  # must not raise

    assert "m1" in _user_models(storage)
    # The corrupt file was quarantined and a fresh valid manifest written.
    entries = os.listdir(_user_dir(models_dir))
    assert any(name.startswith(USER_DEFINED_MANIFEST + ".corrupt.") for name in entries)
    models, ok = _manifest(models_dir).load()
    assert ok is True
    assert "m1" in models


# ==================== Register / delete update the manifest ====================


def test_register_adds_to_manifest_and_survives_restart(models_dir):
    # Prepare a local source dir to register from.
    src = os.path.join(str(models_dir), "src_model")
    os.makedirs(src, exist_ok=True)
    with open(os.path.join(src, CONFIG_JSON), "w", encoding="utf-8") as f:
        json.dump({"model_type": "custom", "auto_map": None}, f)
    with open(os.path.join(src, MODEL_SAFETENSORS), "wb") as f:
        f.write(b"\x00")

    storage = ModelStorage()
    storage.register_model("reg1", f"file://{src}", existing_model_id=None)

    assert "reg1" in _user_models(storage)
    assert _manifest(models_dir).contains("reg1") is True

    # A fresh storage (simulated restart) re-loads it ACTIVE.
    restarted = ModelStorage()
    assert "reg1" in _user_models(restarted)
    assert _user_models(restarted)["reg1"].state == ModelStates.ACTIVE


def test_register_duplicate_raises(models_dir):
    src = _make_src(models_dir, "dup")
    storage = ModelStorage()
    storage.register_model("dup", f"file://{src}", existing_model_id=None)
    # A second registration of the same id must be rejected.
    with pytest.raises(ModelExistedException):
        storage.register_model("dup", f"file://{src}", existing_model_id=None)


def test_plain_model_base_model_id_none_across_restart(models_dir):
    src = _make_src(models_dir, "plain")
    storage = ModelStorage()
    storage.register_model("plain", f"file://{src}", existing_model_id=None)

    # Fresh: a plain model has base_model_id None (not "") ...
    assert _user_models(storage)["plain"].base_model_id is None
    assert _manifest(models_dir).snapshot()["plain"]["base_model_id"] is None
    # ... and it stays None after restart (the pipeline loader keys off `is not None`).
    restarted = ModelStorage()
    assert _user_models(restarted)["plain"].base_model_id is None


def test_legacy_empty_base_model_id_normalized_to_none(models_dir):
    # A pre-existing manifest may carry base_model_id "" from the old discovery.
    _make_model_dir(models_dir, "legacy")
    _manifest(models_dir).replace_all(
        {"legacy": {"model_type": "custom", "auto_map": None, "base_model_id": ""}}
    )
    storage = ModelStorage()
    assert _user_models(storage)["legacy"].base_model_id is None


def test_register_base_model_persists_override(models_dir):
    src = os.path.join(str(models_dir), "src_alias")
    os.makedirs(src, exist_ok=True)
    with open(os.path.join(src, CONFIG_JSON), "w", encoding="utf-8") as f:
        json.dump({"model_type": "custom"}, f)
    with open(os.path.join(src, MODEL_SAFETENSORS), "wb") as f:
        f.write(b"\x00")

    storage = ModelStorage()
    # Builtin discovery is stubbed in this fixture, so seed the base model that
    # register_model reads auto_map from when existing_model_id is given.
    base_info = BUILTIN_HF_TRANSFORMERS_MODEL_MAP["timer_xl"]
    base_auto_map = base_info.auto_map
    storage._models[ModelCategory.BUILTIN.value]["timer_xl"] = base_info

    storage.register_model("alias1", f"file://{src}", existing_model_id="timer_xl")

    entry = _manifest(models_dir).snapshot()["alias1"]
    assert entry["base_model_id"] == "timer_xl"
    assert entry["auto_map"] == base_auto_map
    # config.json on disk was rewritten with the override before the manifest.
    reg_dir = os.path.join(_user_dir(models_dir), "alias1")
    with open(os.path.join(reg_dir, CONFIG_JSON), "r", encoding="utf-8") as f:
        cfg = json.load(f)
    assert cfg["base_model_id"] == "timer_xl"
    assert cfg["auto_map"] == base_auto_map


def test_delete_removes_from_manifest_and_disk(models_dir):
    _make_model_dir(models_dir, "m1")
    _manifest(models_dir).replace_all(
        {"m1": {"model_type": "custom", "auto_map": None}}
    )
    storage = ModelStorage()
    assert "m1" in _user_models(storage)

    storage.delete_model("m1")

    assert "m1" not in _user_models(storage)
    assert _manifest(models_dir).contains("m1") is False
    assert not os.path.exists(os.path.join(_user_dir(models_dir), "m1"))
    # A fresh storage does not resurrect it.
    restarted = ModelStorage()
    assert "m1" not in _user_models(restarted)


def test_delete_rmtree_failure_still_deregisters(models_dir, monkeypatch):
    _make_model_dir(models_dir, "m1")
    _manifest(models_dir).replace_all(
        {"m1": {"model_type": "custom", "auto_map": None}}
    )
    storage = ModelStorage()

    def boom(path):
        raise OSError("cannot remove")

    monkeypatch.setattr(shutil, "rmtree", boom)
    with pytest.raises(OSError):
        storage.delete_model("m1")

    # No zombie: gone from both memory and the manifest despite the rmtree error.
    assert "m1" not in _user_models(storage)
    assert _manifest(models_dir).contains("m1") is False
    # A fresh storage ignores the leftover directory (payload present but unlisted).
    monkeypatch.undo()
    restarted = ModelStorage()
    assert "m1" not in _user_models(restarted)


def test_crash_after_files_before_manifest_ignored(models_dir):
    # Valid model dir on disk that is NOT in the manifest -> crash before add().
    _make_model_dir(models_dir, "half")
    _manifest(models_dir).replace_all({})  # manifest exists, but empty

    storage = ModelStorage()  # must not resurrect the half-registered model

    assert "half" not in _user_models(storage)


# ==================== Concurrency ====================


def test_concurrent_register_delete_manifest_consistent(models_dir):
    storage = ModelStorage()

    def make_src(name):
        src = os.path.join(str(models_dir), f"src_{name}")
        os.makedirs(src, exist_ok=True)
        with open(os.path.join(src, CONFIG_JSON), "w", encoding="utf-8") as f:
            json.dump({"model_type": "custom", "auto_map": None}, f)
        with open(os.path.join(src, MODEL_SAFETENSORS), "wb") as f:
            f.write(b"\x00")
        return src

    ids = [f"c{i}" for i in range(12)]
    errors = []

    def worker(model_id):
        try:
            storage.register_model(
                model_id, f"file://{make_src(model_id)}", existing_model_id=None
            )
        except Exception as e:  # pragma: no cover - surfaced via assert below
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(mid,)) for mid in ids]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    # Manifest is valid JSON and its key set matches in-memory (no lost update / torn write).
    models, ok = _manifest(models_dir).load()
    assert ok is True
    assert set(models.keys()) == set(_user_models(storage).keys()) == set(ids)
