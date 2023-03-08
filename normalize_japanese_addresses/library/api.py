import urllib.parse


def apiFetch(endpoint: str = '') -> str:
    if endpoint.startswith('http'):
        import requests
        return requests.get(f'{endpoint}').content.decode("utf-8")
    elif endpoint.startswith('file'):
        filepath = urllib.parse.unquote(endpoint.replace("file://", ""))
        with open(filepath, encoding="utf-8") as fp:
            return fp.read()
    else:
        raise ValueError("Invalid endpoint type")
