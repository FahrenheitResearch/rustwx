# rustwx-python

`rustwx-python` is the optional thin Python layer for `rustwx`.

## Design goal

Keep Python convenient but thin. The hot path should stay in Rust.
Today that means the crate only depends on the core/model/io path plus optional `pyo3`.

## What is implemented

With the `python` feature enabled, the module currently exposes:

- model listing
- URL resolution
- latest-run probing
- forecast-hour availability
- source probing

The current Python API intentionally returns JSON strings so the Rust surface can keep moving without committing to a wide Python object model too early.

## Current limits

- no Python bindings yet for render/calc workflows or full fetch/download flows
- no typed Python objects yet

## Minimal example

```python
import rustwx_python

print(rustwx_python.list_models_json())
```
