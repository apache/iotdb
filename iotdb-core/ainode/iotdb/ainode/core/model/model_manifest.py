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
import threading
from typing import Dict, Tuple

from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.model_constants import (
    MANIFEST_TEMP_SUFFIX,
    MANIFEST_VERSION,
    USER_DEFINED_MANIFEST,
)

logger = Logger()


class ModelManifest:
    """Durable registration list for user-defined models.

    The manifest is the single source of truth for which user-defined models
    were actually registered by the user. On restart AINode reads this manifest
    instead of blindly scanning every directory under ``user_defined/`` – so an
    unrecognized/stray directory can neither crash startup nor be mistaken for a
    registered model.

    Persistence follows the ``FinetuneTaskQueue`` manifest precedent
    (``timecho/ainode/core/finetune/task/task_queue.py``): a small JSON file
    written atomically via a ``.tmp`` staging file plus ``os.replace``. Disk is
    authoritative – every mutation re-reads the file, mutates, and rewrites it
    under an internal lock, so an in-memory cache can never launder stale state
    onto disk and concurrent register/delete never lose an update.

    On-disk schema::

        {
          "version": 1,
          "models": {
            "<model_id>": {
              "model_type": "...",
              "pipeline_cls": "...",
              "auto_map": {...} | null,
              "hub_mixin_cls": "...",
              "base_model_id": "..."
            },
            ...
          }
        }
    """

    def __init__(self, manifest_dir: str) -> None:
        self._dir = manifest_dir
        self._path = os.path.join(manifest_dir, USER_DEFINED_MANIFEST)
        self._tmp = self._path + MANIFEST_TEMP_SUFFIX
        self._lock = threading.Lock()
        # In-memory cache; disk stays authoritative (mutations re-read the file).
        self._models: Dict[str, dict] = {}

    # ==================== Read ====================

    def file_exists(self) -> bool:
        """Whether the manifest file itself exists on disk.

        Distinguishes "no manifest yet" (first upgrade -> bootstrap) from
        "present but unreadable" (corruption -> quarantine). Does not touch
        the in-memory cache.
        """
        return os.path.exists(self._path)

    def load(self) -> Tuple[Dict[str, dict], bool]:
        """Read the manifest from disk into the cache.

        Returns ``(models, ok)``:
            * file absent                     -> ``({}, True)``  (caller bootstraps)
            * present & valid                 -> ``(models, True)``
            * present & corrupt / bad version -> ``({}, False)`` (caller quarantines)

        Never raises – a broken manifest must not crash AINode startup (same
        contract as ``FinetuneTaskQueue._load_state``). Also best-effort removes
        a stale ``.tmp`` file left behind by a crashed write.
        """
        with self._lock:
            self._cleanup_tmp()
            if not os.path.exists(self._path):
                self._models = {}
                return {}, True
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                if (
                    not isinstance(payload, dict)
                    or payload.get("version") != MANIFEST_VERSION
                    or not isinstance(payload.get("models"), dict)
                ):
                    logger.error(
                        f"Model manifest {self._path} has an unexpected schema "
                        f"or version; treating it as corrupt."
                    )
                    self._models = {}
                    return {}, False
                self._models = dict(payload["models"])
                return dict(self._models), True
            except Exception as e:
                logger.error(f"Failed to load model manifest {self._path}: {e}")
                self._models = {}
                return {}, False

    def snapshot(self) -> Dict[str, dict]:
        """Thread-safe copy of the current models, read from disk (authoritative).

        Reads the file rather than the cache so a freshly-constructed manifest
        (or one mutated by another instance on the same path) reflects reality.
        """
        with self._lock:
            models = self._read_disk()
            self._models = models
            return dict(models)

    def contains(self, model_id: str) -> bool:
        """Whether ``model_id`` is registered, per the on-disk manifest."""
        with self._lock:
            return model_id in self._read_disk()

    # ==================== Write (atomic, crash-safe) ====================

    def add(self, model_id: str, entry: dict) -> None:
        """Register (or update) one model, re-reading the file first.

        Re-reading the file (rather than trusting the cache) means a concurrent
        add/remove that already touched disk is not clobbered.
        """
        with self._lock:
            models = self._read_disk()
            models[model_id] = entry
            self._atomic_write(models)

    def remove(self, model_id: str) -> None:
        """De-register one model. Idempotent – removing an absent id is a no-op."""
        with self._lock:
            models = self._read_disk()
            if model_id in models:
                models.pop(model_id, None)
                self._atomic_write(models)
            else:
                # Keep the cache in sync even when nothing was on disk.
                self._models = models

    def replace_all(self, models: Dict[str, dict]) -> None:
        """Overwrite the whole manifest in one atomic write.

        Used by the first-upgrade bootstrap and by the startup reconciliation
        pass (pruning entries whose directories vanished).
        """
        with self._lock:
            self._atomic_write(dict(models))

    # ==================== Internals ====================

    def _read_disk(self) -> Dict[str, dict]:
        """Return the models dict from disk, or ``{}`` if absent/corrupt.

        Callers already hold ``self._lock``.
        """
        if not os.path.exists(self._path):
            return {}
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if (
                isinstance(payload, dict)
                and payload.get("version") == MANIFEST_VERSION
                and isinstance(payload.get("models"), dict)
            ):
                return dict(payload["models"])
            logger.error(
                f"Model manifest {self._path} is corrupt during read-modify-write; "
                f"starting from an empty set for this operation."
            )
        except Exception as e:
            logger.error(f"Failed to re-read model manifest {self._path}: {e}")
        return {}

    def _atomic_write(self, models: Dict[str, dict]) -> None:
        """Persist ``models`` atomically and refresh the cache.

        Writes ``{"version": ..., "models": ...}`` to the ``.tmp`` file, fsyncs
        it, ``os.replace``s it over the real path, then fsyncs the containing
        directory so the rename itself is durable. ``os.replace`` overwrites an
        existing target atomically on both POSIX and Windows (plain ``os.rename``
        raises ``FileExistsError`` on Windows). No ``default=str`` is used: the
        schema is JSON-native, so a non-serializable value must raise here rather
        than silently persist a ``"<object ...>"`` string.

        Callers already hold ``self._lock``.
        """
        payload = {"version": MANIFEST_VERSION, "models": models}
        # Defensive: the manifest dir normally already exists (ModelStorage
        # creates user_defined/ before constructing the manifest), but ensure it
        # so a write never fails on a fresh/pruned tree.
        os.makedirs(self._dir, exist_ok=True)
        with open(self._tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(self._tmp, self._path)
        self._fsync_dir()
        self._models = dict(models)

    def _fsync_dir(self) -> None:
        """fsync the manifest's directory so a just-committed rename survives a crash.

        Without this, ``os.replace`` can be lost on power loss even though the
        file data is durable, silently reverting the manifest. Directory fsync is
        a POSIX facility; it is unsupported (and unnecessary) on Windows, where
        ``os.open`` on a directory fails – swallow that case.
        """
        try:
            dir_fd = os.open(self._dir, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        except OSError as e:
            logger.warning(f"Failed to fsync manifest directory {self._dir}: {e}")
        finally:
            os.close(dir_fd)

    def _cleanup_tmp(self) -> None:
        """Best-effort removal of a stale ``.tmp`` from a crashed write."""
        if os.path.exists(self._tmp):
            try:
                os.remove(self._tmp)
            except OSError as e:
                logger.warning(f"Failed to remove stale manifest temp {self._tmp}: {e}")
