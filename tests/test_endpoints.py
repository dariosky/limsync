from limsync.endpoints import endpoint_to_string, parse_endpoint


def test_parse_scp_remote_with_ssh_config_alias() -> None:
    endpoint = parse_endpoint("lime:~/Dropbox")

    assert endpoint.is_remote
    assert endpoint.host == "lime"
    assert endpoint.user is None
    assert endpoint.port is None
    assert endpoint.root == "~/Dropbox"


def test_remote_endpoint_round_trip_without_explicit_user() -> None:
    endpoint = parse_endpoint("lime:~/Dropbox")

    assert parse_endpoint(endpoint_to_string(endpoint)) == endpoint


def test_remote_endpoint_round_trip_with_user_and_port() -> None:
    endpoint = parse_endpoint("ssh://dario@lime:2200/~/Dropbox")

    assert endpoint.root == "~/Dropbox"
    assert parse_endpoint(endpoint_to_string(endpoint)) == endpoint
