from limsync.config import RemoteConfig
from limsync.scanner_remote import RemoteScanner


def test_remote_helper_source_injects_shared_ignore_rules() -> None:
    scanner = RemoteScanner(RemoteConfig(host="h", user="u", root="~/r"))
    source = scanner._remote_helper_source()
    assert "# [[IGNORE_RULES_SHARED]]" not in source
    assert "class IgnoreRules" in source
    compile(source, "<stdin>", "exec")
