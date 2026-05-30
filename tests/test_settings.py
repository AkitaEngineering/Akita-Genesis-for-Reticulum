from akita_genesis.config.settings import AppSettings


def test_valid_api_keys_accepts_comma_separated_env(monkeypatch):
    monkeypatch.setenv("AKITA_VALID_API_KEYS", "key1, key2 ,key3")

    settings = AppSettings(_env_file=None)
    parsed_keys = {key.get_secret_value() for key in settings.VALID_API_KEYS}

    assert parsed_keys == {"key1", "key2", "key3"}


def test_valid_api_keys_accepts_json_array_env(monkeypatch):
    monkeypatch.setenv("AKITA_VALID_API_KEYS", '["alpha", "beta"]')

    settings = AppSettings(_env_file=None)
    parsed_keys = {key.get_secret_value() for key in settings.VALID_API_KEYS}

    assert parsed_keys == {"alpha", "beta"}