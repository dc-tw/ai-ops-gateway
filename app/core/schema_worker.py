"""Disposable JSON Schema process; stdout is a tiny verdict, never input data."""

import json
import os
import signal
import sys


def main():
    import resource

    resource.setrlimit(resource.RLIMIT_CPU, (3, 3))
    if sys.platform.startswith("linux"):
        resource.setrlimit(resource.RLIMIT_AS, (256 * 1024 * 1024,) * 2)
    signal.signal(signal.SIGALRM, lambda *_: os._exit(124))
    signal.setitimer(signal.ITIMER_REAL, 2)
    try:
        from jsonschema import Draft202012Validator
        from referencing import Registry
        from referencing.exceptions import NoSuchResource

        def no_network(uri):
            raise NoSuchResource(ref=uri)

        raw = sys.stdin.buffer.read(131_073)
        if len(raw) > 131_072:
            raise ValueError
        payload = json.loads(raw)
        schema = payload["schema"]
        Draft202012Validator.check_schema(schema)
        if payload["operation"] == "instance":
            Draft202012Validator(schema, registry=Registry(retrieve=no_network)).validate(
                payload["instance"]
            )
        elif payload["operation"] != "schema":
            raise ValueError
        sys.stdout.write("valid")
    except Exception:
        sys.stdout.write("invalid")


if __name__ == "__main__":
    main()
