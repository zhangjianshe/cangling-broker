# cangling-message (Python)

Producer and consumer for the cangling-message broker. Same contract as the Java client: `AcceptMessages` to publish, `Subscribe` to consume, optional `Register` metadata, `AUTH_TOKEN` on every RPC.

## Install

```bash
pip install cangling-message
```

From this tree:

```bash
pip install -e python
```

## Use

```python
from cangling_message import SatwayClient, SubscribeOptions

with SatwayClient.connect("127.0.0.1:7500", "change-me") as client:
    client.send("cangling-test", "hello")
    with client.subscribe(
        SubscribeOptions(topic="cangling-test", name="worker-1"),
        lambda message: print(message.id, message.payload),
    ):
        ...
```

`SatwayClient.connect(broker)` also reads `AUTH_TOKEN` from the environment.

```bash
# consume
python python/examples/consumer.py --broker 127.0.0.1:7500 --topic cangling-test --name py-s0 --token change-me

# produce
python python/examples/producer.py --broker 127.0.0.1:7500 --topic cangling-test --text hello --count 1 --token change-me
```

## Publish to PyPI

CI on tag `v*` builds the package and uploads with `pypa/gh-action-pypi-publish`. Set repository secret `PYPI_API_TOKEN`.

```bash
cd python
python generate_proto.py
python -m build
```
