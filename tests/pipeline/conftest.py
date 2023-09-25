"""
Shared fixtures for pipeline testing
"""

import sys

import pytest
import mockssh
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization


@pytest.fixture
def ssh_server(tmp_path):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_path = tmp_path / "ssh"
    with open(key_path, "wb") as key_file:
        key_file.write(private_key_bytes)

    users = {
        "user": str(key_path),
    }
    with mockssh.Server(users) as s:
        yield s
