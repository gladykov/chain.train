import re
import uuid
from datetime import datetime


# TODO: Make configurable per project / schema (?)
DATE_FORMAT = "%Y-%m-%d"
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
YEAR_FORMAT = "%Y"


class ExpectedFormatValidators:  # noqa PIE798
    @staticmethod
    def date_time(result):
        try:
            datetime.strptime(result, DATE_TIME_FORMAT)
            return True
        except:  # noqa E722
            return False

    @staticmethod
    def date(result):
        try:
            datetime.strptime(result, DATE_FORMAT)
            return True
        except:  # noqa E722
            return False

    @staticmethod
    def year(result):
        try:
            datetime.strptime(str(result), YEAR_FORMAT)
            return True
        except:  # noqa E722
            return False

    @staticmethod
    def timestamp(result):
        try:
            datetime.fromtimestamp(int(result) / 1000.0)
            return True
        except:  # noqa E722
            return False

    @staticmethod
    def guid(result):
        try:
            uuid.UUID(result)
            return True
        except:  # noqa E722
            return False

    @staticmethod
    def int(result):
        try:
            int(result)
            return True
        except:  # noqa E722
            return False

    @staticmethod
    def float(result):
        try:
            float(result)
            return True
        except:  # noqa E722
            return False

    @staticmethod
    def email(result):
        email_regex = r"^\S+@\S+\.\S+$"
        return bool(re.fullmatch(email_regex, result))

    @staticmethod
    def hash(result):
        # https://gist.github.com/NullArray/923f879c7b6f2ef0778ed43c6bdbd646#file-hashpect-py

        HASH_TYPE_REGEX = {
            re.compile(r"^[a-f0-9]{32}(:.+)?$", re.IGNORECASE): [
                "MD5",
                "MD4",
                "MD2",
                "Double MD5",
                "LM",
                "RIPEMD-128",
                "Haval-128",
                "Tiger-128",
                "Skein-256(128)",
                "Skein-512(128",
                "Lotus Notes/Domino 5",
                "Skype",
                "ZipMonster",
                "PrestaShop",
            ],
            re.compile(r"^[a-f0-9]{64}(:.+)?$", re.IGNORECASE): [
                "SHA-256",
                "RIPEMD-256",
                "SHA3-256",
                "Haval-256",
                "GOST R 34.11-94",
                "GOST CryptoPro S-Box",
                "Skein-256",
                "Skein-512(256)",
                "Ventrilo",
            ],
            re.compile(r"^[a-f0-9]{128}(:.+)?$", re.IGNORECASE): [
                "SHA-512",
                "Whirlpool",
                "Salsa10",
                "Salsa20",
                "SHA3-512",
                "Skein-512",
                "Skein-1024(512)",
            ],
            re.compile(r"^[a-f0-9]{56}$", re.IGNORECASE): [
                "SHA-224",
                "Haval-224",
                "SHA3-224",
                "Skein-256(224)",
                "Skein-512(224)",
            ],
            re.compile(r"^[a-f0-9]{40}(:.+)?$", re.IGNORECASE): [
                "SHA-1",
                "Double SHA-1",
                "RIPEMD-160",
                "Haval-160",
                "Tiger-160",
                "HAS-160",
                "LinkedIn",
                "Skein-256(160)",
                "Skein-512(160)",
                "MangoWeb Enhanced CMS",
            ],
            re.compile(r"^[a-f0-9]{96}$", re.IGNORECASE): [
                "SHA-384",
                "SHA3-384",
                "Skein-512(384)",
                "Skein-1024(384)",
            ],
            re.compile(r"^[a-f0-9]{16}$", re.IGNORECASE): [
                "MySQL323",
                "DES(Oracle)",
                "Half MD5",
                "Oracle 7-10g",
                "FNV-164",
                "CRC-64",
            ],
            re.compile(r"^\*[a-f0-9]{40}$", re.IGNORECASE): ["MySQL5.x", "MySQL4.1"],
            re.compile(r"^[a-f0-9]{48}$", re.IGNORECASE): [
                "Haval-192",
                "Tiger-192",
                "SHA-1(Oracle)",
                "XSHA (v10.4 - v10.6)",
            ],
        }

        return any(algorithm.match(result) for algorithm, _ in HASH_TYPE_REGEX.items())


def expected_formats():
    return [
        validator
        for validator in ExpectedFormatValidators.__dict__.keys()
        if not validator.startswith("__")
    ]
