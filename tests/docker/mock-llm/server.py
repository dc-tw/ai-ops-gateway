import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        return None

    def do_GET(self) -> None:
        if self.path != "/healthz":
            self.send_error(404)
            return
        body = b'{"status":"ok"}'
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        if self.path != "/v1/chat/completions":
            self.send_error(404)
            return
        length = int(self.headers.get("content-length", "0"))
        request = json.loads(self.rfile.read(length) or b"{}")
        if "checkpoint-resume" in json.dumps(request):
            time.sleep(10)
        if request.get("stream"):
            self._stream_response()
        else:
            self._json_response()

    def _json_response(self) -> None:
        body = json.dumps(
            {
                "id": "chatcmpl-mock",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": "mock-model",
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": (
                                "Mock model generated a sufficiently detailed verified response."
                            ),
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 10, "completion_tokens": 8, "total_tokens": 18},
            }
        ).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _stream_response(self) -> None:
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("cache-control", "no-cache")
        self.send_header("connection", "close")
        self.end_headers()
        for token in ("Mock ", "streaming ", "final ", "answer"):
            chunk = {
                "id": "chatcmpl-mock",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": "mock-model",
                "choices": [{"index": 0, "delta": {"content": token}, "finish_reason": None}],
            }
            self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
            self.wfile.flush()
            time.sleep(0.03)
        finish = {
            "id": "chatcmpl-mock",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": "mock-model",
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        }
        usage = {
            "id": "chatcmpl-mock",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": "mock-model",
            "choices": [],
            "usage": {"prompt_tokens": 10, "completion_tokens": 8, "total_tokens": 18},
        }
        self.wfile.write(
            f"data: {json.dumps(finish)}\n\ndata: {json.dumps(usage)}\n\ndata: [DONE]\n\n".encode()
        )
        self.wfile.flush()


ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
