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

import abc
from dataclasses import dataclass
from typing import Any


class Transformation(abc.ABC):
    @abc.abstractmethod
    def __call__(self, data_entry: dict[str, Any]) -> dict[str, Any]: ...

    def chain(self, other: "Transformation") -> "Chain":
        return Chain([self, other])

    def __add__(self, other: "Transformation") -> "Chain":
        return self.chain(other)

    def __radd__(self, other):
        if other == 0:
            return self
        return other + self


@dataclass
class Chain(Transformation):
    """
    Chain multiple transformations together.
    """

    transformations: list[Transformation]

    def __post_init__(self) -> None:
        transformations = []

        for transformation in self.transformations:
            if isinstance(transformation, Identity):
                continue
            elif isinstance(transformation, Chain):
                transformations.extend(transformation.transformations)
            else:
                assert isinstance(transformation, Transformation)
                transformations.append(transformation)

        self.transformations = transformations
        self.__init_passed_kwargs__ = {"transformations": transformations}

    def __call__(self, data_entry: dict[str, Any]) -> dict[str, Any]:
        for t in self.transformations:
            data_entry = t(data_entry)
        return data_entry


class Identity(Transformation):
    def __call__(self, data_entry: dict[str, Any]) -> dict[str, Any]:
        return data_entry
