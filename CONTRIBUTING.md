## Development installation

Install packaging/distribution tools and linter:

```shell
pip install hatch twine black
```

From top-level repo directory, install the package in editable mode:

```shell
pip install -e .
```

Local edits to the package will immediately take effect.

Get the PyPI Test API Key from 1Password and add it to `~/.pypirc`:

```bash
[testpypi]
  username = __token__
  password = <PyPI Test API Key>
```
