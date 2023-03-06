from collections import OrderedDict
from typing import Any, Callable, TypeVar, Generic

import hashlib
import json
from pathlib import Path


class _NONE_TYPE:
    pass


class Cache:
    _directory: Path
    _version: bytes
    _ram_capacity: int
    _disk_max_size: int
    _encode: Callable[[Any], bytes]
    _decode: Callable[[bytes], Any]
    _ram_cache: OrderedDict

    def __init__(
        self,
        directory: str | Path,
        version: str,
        ram_capacity: int = 128,
        encode=None,
        decode=None,
    ) -> None:
        self._directory = Path(directory)
        self._version = version.encode("utf-8")
        self._ram_capacity = ram_capacity
        self._encode = encode or self._encode_default
        self._decode = decode or self._decode_default
        self._ram_cache = OrderedDict()

        self._directory.mkdir(parents=True, exist_ok=True)

    def get(self, key: str, default: Any | _NONE_TYPE = _NONE_TYPE()) -> Any:
        value = self.get_from_ram(key)
        if value is not None:
            return value

        value = self.get_from_disk(key)
        if value is not None:
            # promote to ram cache
            self.insert_to_ram(key, value)
            return value

        if not isinstance(default, _NONE_TYPE):
            return default

        raise KeyError(key)

    def get_from_ram(self, key: str) -> Any | None:
        value = self._ram_cache.pop(key, None)
        if value is not None:
            self._ram_cache[key] = value

        return value

    def get_from_disk(self, key: str) -> Any | None:
        encoded = key.encode("utf-8")
        filename = hashlib.md5(encoded).hexdigest()
        path = self._directory / filename
        if not path.is_file():
            return None

        with open(path, "rb") as fp:
            version_len = int.from_bytes(fp.read(8), "little")
            if fp.read(version_len) != self._version:
                return None

            key_len = int.from_bytes(fp.read(8), "little")
            if fp.read(key_len) != encoded:
                return None

            value_len = int.from_bytes(fp.read(8), "little")
            value = self._decode(fp.read(value_len))
            return value

    def insert(self, key: str, value: Any) -> None:
        self.insert_to_ram(key, value)
        self.insert_to_disk(key, value)

    def insert_to_ram(self, key: str, value: Any) -> None:
        if (
            self._ram_cache.pop(key, None) is None
            and len(self._ram_cache) == self._ram_capacity
        ):
            self._ram_cache.popitem(last=False)

        self._ram_cache[key] = value

    def insert_to_disk(self, key: str, value: Any) -> None:
        encoded_key = key.encode("utf-8")
        filename = hashlib.md5(encoded_key).hexdigest()
        path = self._directory / filename
        encoded_val = self._encode(value)

        contents = len(self._version).to_bytes(8, "little") + self._version
        contents += len(encoded_key).to_bytes(8, "little") + encoded_key
        contents += len(encoded_val).to_bytes(8, "little") + encoded_val

        path.write_bytes(contents)

    def clear_ram(self) -> None:
        self._ram_cache = OrderedDict()

    def __getitem__(self, key: str) -> Any:
        return self.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        return self.insert(key, value)

    def _encode_default(self, obj: Any) -> bytes:
        tmp = json.dumps(obj, ensure_ascii=False, separators=(',', ':'))
        return tmp.encode("utf-8")

    def _decode_default(self, contents: bytes):
        return json.loads(contents.decode("utf-8"))
