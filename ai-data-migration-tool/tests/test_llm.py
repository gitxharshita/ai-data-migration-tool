"""AnthropicClient against a fake `anthropic` module (no network, no key)."""
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from amt.llm import AnthropicClient, LLMError


def fake_anthropic(response, record):
    mod = types.ModuleType("anthropic")
    for name in ("AuthenticationError", "BadRequestError", "RateLimitError", "APIStatusError", "APIConnectionError"):
        setattr(mod, name, type(name, (Exception,), {"message": name}))

    class Messages:
        def create(self, **kw):
            record.append(kw)
            return response

    class Client:
        def __init__(self, api_key=None):
            self.beta = types.SimpleNamespace(messages=Messages())

    mod.Anthropic = Client
    return mod


def resp(text, stop="end_turn"):
    return types.SimpleNamespace(
        stop_reason=stop,
        content=[types.SimpleNamespace(type="thinking", thinking=""), types.SimpleNamespace(type="text", text=text)],
        usage=types.SimpleNamespace(input_tokens=11, output_tokens=7))


class AnthropicClientTests(unittest.TestCase):
    def test_request_shape_text_extraction_and_cache(self):
        calls = []
        with mock.patch.dict(sys.modules, {"anthropic": fake_anthropic(resp('{"columns": []}'), calls)}):
            c = AnthropicClient(cache_dir=tempfile.mkdtemp())
            self.assertEqual(c.complete("sys", "prompt"), '{"columns": []}')
            self.assertEqual(c.complete("sys", "prompt"), '{"columns": []}')   # served from cache
        self.assertEqual(len(calls), 1)
        kw = calls[0]
        self.assertEqual(kw["model"], "claude-opus-5-5")
        self.assertNotIn("temperature", kw)
        self.assertEqual(kw["max_tokens"], 16000)
        self.assertEqual(kw["output_config"], {"effort": "medium"})
        self.assertEqual(kw["fallbacks"], "default")
        self.assertEqual((c.calls, c.input_tokens, c.output_tokens), (1, 11, 7))

    def test_refusal_and_truncation_raise(self):
        for stop in ("refusal", "max_tokens"):
            with mock.patch.dict(sys.modules, {"anthropic": fake_anthropic(resp("x", stop), [])}):
                c = AnthropicClient(cache_dir=None)
                with self.assertRaises(LLMError):
                    c.complete("s", "p")


if __name__ == "__main__":
    unittest.main()
