from subprocess import CompletedProcess
from unittest.mock import patch

from limsync.ssh_pool import resolve_ssh_connection_options


def test_resolve_ssh_connection_options_uses_openssh_effective_config(
    tmp_path,
) -> None:
    identity = tmp_path / "id_test"
    identity.touch()
    output = "\n".join(
        [
            "user remote-user",
            "hostname lime.local",
            "port 2200",
            f"identityfile {identity}",
        ]
    )

    with patch(
        "limsync.ssh_pool.subprocess.run",
        return_value=CompletedProcess(["ssh", "-G", "lime"], 0, output, ""),
    ):
        options = resolve_ssh_connection_options("lime", None, None)

    assert options.hostname == "lime.local"
    assert options.username == "remote-user"
    assert options.port == 2200
    assert options.key_filenames == (str(identity),)
