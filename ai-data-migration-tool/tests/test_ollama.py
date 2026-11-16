"""OllamaClient against a fake local Ollama server (no download needed)."""
import json
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from amt.llm import OllamaClient, extract_json, ollama_status

REQUESTS = []


class FakeOllama(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._send({"models": [{"name": "qwen2.5:7b"}]})

    def do_POST(self):
        REQUESTS.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
        self._send({"message": {"role": "assistant", "content": '{"columns": [], "rules": []}'},
                    "prompt_eval_count": 120, "eval_count": 30})


class OllamaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = HTTPServer(("127.0.0.1", 0), FakeOllama)
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()
        cls.url = f"http://127.0.0.1:{cls.srv.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def test_chat_request_and_parse(self):
        c = OllamaClient("qwen2.5:7b", base_url=self.url, cache_dir=None)
        out = extract_json(c.complete("system text", "user prompt"))
        self.assertEqual(out, {"columns": [], "rules": []})
        req = REQUESTS[-1]
        self.assertEqual(req["format"], "json")
        self.assertFalse(req["stream"])
        self.assertEqual(req["options"]["temperature"], 0.0)
        self.assertGreaterEqual(req["options"]["num_ctx"], 8192)
        self.assertEqual([m["role"] for m in req["messages"]], ["system", "user"])
        self.assertEqual((c.input_tokens, c.output_tokens), (120, 30))

    def test_status(self):
        self.assertTrue(ollama_status(self.url, "qwen2.5:7b")["has_model"])
        self.assertFalse(ollama_status(self.url, "llama3.1:8b")["has_model"])
        self.assertFalse(ollama_status("http://127.0.0.1:1", "x")["running"])


if __name__ == "__main__":
    unittest.main()
