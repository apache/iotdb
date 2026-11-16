#!/usr/bin/env python3
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
"""Exercise ConfigDSN against process-local registry hives, without installation."""

import ctypes
import sys
import uuid
import winreg
from pathlib import Path


def remove_tree(parent, name):
    with winreg.OpenKey(parent, name, 0, winreg.KEY_ALL_ACCESS) as key:
        while winreg.QueryInfoKey(key)[0]:
            remove_tree(key, winreg.EnumKey(key, 0))
    winreg.DeleteKey(parent, name)


def main():
    driver_path = str(Path(sys.argv[1]).resolve())
    scope = r"Software\IoTDB-ODBC-Setup-Test-" + uuid.uuid4().hex
    original_user = winreg.OpenKey(
        winreg.HKEY_CURRENT_USER, "", 0, winreg.KEY_ALL_ACCESS
    )
    machine = winreg.CreateKey(original_user, scope + r"\Machine")
    user = winreg.CreateKey(original_user, scope + r"\User")
    override = ctypes.WinDLL("advapi32").RegOverridePredefKey
    override.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    override.restype = ctypes.c_long

    # Predefined HKEY pseudo-handles are sign-extended on 64-bit Windows.
    def hkey(value):
        return ctypes.c_void_p(ctypes.c_int32(value).value)

    changed = []
    try:
        for root, replacement in (
            (winreg.HKEY_LOCAL_MACHINE, machine),
            (winreg.HKEY_CURRENT_USER, user),
        ):
            result = override(hkey(root), int(replacement))
            if result:
                raise ctypes.WinError(result)
            changed.append(root)
        driver_name = "IoTDB isolated setup fixture"
        registry_base = r"SOFTWARE\ODBC\ODBCINST.INI"
        with winreg.CreateKey(
            winreg.HKEY_LOCAL_MACHINE, registry_base + "\\" + driver_name
        ) as key:
            for name in ("Driver", "Setup"):
                winreg.SetValueEx(key, name, 0, winreg.REG_SZ, driver_path)
        with winreg.CreateKey(
            winreg.HKEY_LOCAL_MACHINE, registry_base + r"\ODBC Drivers"
        ) as key:
            winreg.SetValueEx(key, driver_name, 0, winreg.REG_SZ, "Installed")
        installer = ctypes.WinDLL("odbccp32")
        installer.SQLSetConfigMode.argtypes = [ctypes.c_ushort]
        assert installer.SQLSetConfigMode(1)
        driver = ctypes.WinDLL(driver_path)
        configure = driver.ConfigDSN
        configure.argtypes = [
            ctypes.c_void_p,
            ctypes.c_ushort,
            ctypes.c_char_p,
            ctypes.c_char_p,
        ]
        configure.restype = ctypes.c_int

        def config(request, *attributes):
            data = ("\0".join(attributes) + "\0\0").encode("ascii")
            return configure(None, request, driver_name.encode(), data)

        def read(name):
            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER, r"SOFTWARE\ODBC\ODBC.INI\fixture"
            ) as key:
                return winreg.QueryValueEx(key, name)[0]

        assert config(
            1, "dsn=fixture", "SERVER=127.0.0.1", "DATABASE=initial", "BATCHSIZE=2"
        )
        assert read("SERVER") == "127.0.0.1"
        assert read("DATABASE") == "initial"
        for credential in ("UID", "PWD"):
            try:
                read(credential)
            except FileNotFoundError:
                pass
            else:
                raise AssertionError("Silent setup persisted implicit credentials")
        assert config(
            2, "DSN=fixture", "database=changed", "SSL=1", "SSLCA=C:\\certs\\ca.pem"
        )
        assert read("DATABASE") == "changed" and read("BATCHSIZE") == "2"
        assert read("SSL") == "1" and read("SSLCA") == r"C:\certs\ca.pem"
        assert not config(2, "DSN=missing", "DATABASE=wrong")
        assert not config(1, "DSN=bad;name", "SERVER=localhost")
        assert not config(1, "SERVER=localhost")
        assert not config(
            2, "DSN=fixture", "DATABASE=should-not-be-written", "UNKNOWN=x"
        )
        assert read("DATABASE") == "changed"
        assert not config(2, "DSN=fixture", "malformed")
        assert config(3, "DSN=fixture")
        try:
            read("SERVER")
        except FileNotFoundError:
            pass
        else:
            raise AssertionError("DSN removal left its registry entry")
        print("Windows silent DSN add/update/remove and invalid-input checks passed")
    finally:
        for root in reversed(changed):
            result = override(hkey(root), None)
            if result:
                raise ctypes.WinError(result)
        machine.Close()
        user.Close()
        remove_tree(original_user, scope)
        original_user.Close()


if __name__ == "__main__":
    main()
