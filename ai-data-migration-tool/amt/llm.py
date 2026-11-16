"""LLM clients with an on-disk response cache.

* AnthropicClient   - Claude via the official anthropic SDK (needs ANTHROPIC_API_KEY)
* OpenAICompatClient - any /v1/chat/completions endpoint, e.g. vLLM or Ollama
  serving the LoRA-fine-tuned model used by strategy S2.

The cache makes evaluation runs reproducible and avoids paying twice for the
same prompt.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path


class LLMError(RuntimeError):
    pass


class LLMClient:
    model: str = ""

    def __init__(self, cache_dir: str | None = ".llm_cache", temperature: float = 0.0):
        self.cache = Path(cache_dir) if cache_dir else None
        self.temperature = temperature
        self.calls = 0
        self.input_tokens = 0
        self.output_tokens = 0

    def _complete(self, system: str, prompt: str, max_tokens: int) -> tuple[str, int, int]:
        raise NotImplementedError

    def complete(self, system: str, prompt: str, max_tokens: int = 16000) -> str:
        key = hashlib.sha256(f"{self.model}\n{self.temperature}\n{system}\n{prompt}".encode()).hexdigest()
        path = self.cache / f"{key}.json" if self.cache else None
        if path and path.exists():
            return json.loads(path.read_text(encoding="utf-8"))["text"]
        text, tin, tout = self._complete(system, prompt, max_tokens)
        self.calls += 1
        self.input_tokens += tin
        self.output_tokens += tout
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"model": self.model, "text": text}), encoding="utf-8")
        return text


def _post(url: str, headers: dict, body: dict, retries: int = 4, timeout: float = 180) -> dict:
    data = json.dumps(body).encode()
    for attempt in range(retries):
        req = urllib.request.Request(url, data=data, headers={"content-type": "application/json", **headers})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 529) and attempt < retries - 1:
                time.sleep(2 ** attempt * 2)
                continue
            raise LLMError(f"HTTP {e.code}: {e.read()[:500]!r}") from e
        except urllib.error.URLError as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise LLMError(str(e)) from e
    raise LLMError("unreachable")


class AnthropicClient(LLMClient):
    """Claude through the official `anthropic` SDK (pip install anthropic).

    Current models think adaptively and do not accept a custom temperature, so
    depth is set with `effort` and reproducibility comes from the response
    cache. Credentials are resolved by the SDK (ANTHROPIC_API_KEY, or an
    `ant auth login` profile). Server-side refusal fallback is enabled so a
    rare safety decline is re-run on a fallback model instead of failing.
    """

    def __init__(self, model: str | None = None, api_key: str | None = None,
                 effort: str | None = None, **kw):
        super().__init__(**kw)
        try:
            import anthropic
        except ImportError as e:
            raise LLMError("the anthropic package is missing: run  pip install anthropic") from e
        self._anthropic = anthropic
        self.model = model or os.environ.get("AMT_MODEL", "claude-opus-5-5")
        self.effort = effort or os.environ.get("AMT_EFFORT", "medium")
        self.temperature = f"effort={self.effort}"      # part of the cache key
        self.client = anthropic.Anthropic(api_key=api_key) if api_key else anthropic.Anthropic()

    def _complete(self, system, prompt, max_tokens):
        a = self._anthropic
        req = dict(model=self.model, max_tokens=max_tokens, system=system,
                   messages=[{"role": "user", "content": prompt}],
                   betas=["server-side-fallback-2026-07-01"])
        extra = {"output_config": {"effort": self.effort}, "fallbacks": "default"}
        try:
            try:
                r = self.client.beta.messages.create(**req, **extra)
            except TypeError:
                # older SDK releases without these keyword arguments: send them as raw body fields
                r = self.client.beta.messages.create(**req, extra_body=extra)
        except a.AuthenticationError as e:
            raise LLMError("Anthropic API key missing or invalid: set ANTHROPIC_API_KEY") from e
        except a.BadRequestError as e:
            raise LLMError(f"request rejected: {e.message}") from e
        except (a.RateLimitError, a.APIStatusError, a.APIConnectionError) as e:
            raise LLMError(f"Anthropic API error: {e}") from e
        if r.stop_reason == "refusal":
            raise LLMError("the model declined this request (refusal)")
        if r.stop_reason == "max_tokens":
            raise LLMError("response cut off at max_tokens; raise max_tokens")
        text = "".join(b.text for b in r.content if b.type == "text")
        return text, r.usage.input_tokens, r.usage.output_tokens


class OpenAICompatClient(LLMClient):
    def __init__(self, model: str, base_url: str | None = None, api_key: str | None = None, **kw):
        super().__init__(**kw)
        self.model = model
        self.base = (base_url or os.environ.get("AMT_OPENAI_BASE_URL", "http://localhost:8000/v1")).rstrip("/")
        self.api_key = api_key or os.environ.get("AMT_OPENAI_API_KEY", "none")

    def _complete(self, system, prompt, max_tokens):
        r = _post(f"{self.base}/chat/completions", {"authorization": f"Bearer {self.api_key}"},
                  {"model": self.model, "max_tokens": max_tokens, "temperature": self.temperature,
                   "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}]})
        u = r.get("usage", {})
        return r["choices"][0]["message"]["content"], u.get("prompt_tokens", 0), u.get("completion_tokens", 0)


class OllamaClient(LLMClient):
    """A free open-weight model running locally under Ollama (https://ollama.com).

    Uses Ollama's native /api/chat endpoint so the context window can be
    raised (the default is too small for the S3 prompt) and JSON output can be
    enforced. Temperature 0 makes runs deterministic.
    """

    def __init__(self, model: str | None = None, base_url: str | None = None,
                 num_ctx: int = 8192, timeout: float = 1800, **kw):
        super().__init__(**kw)
        self.model = model or os.environ.get("AMT_OLLAMA_MODEL", "qwen2.5:7b")
        self.base = (base_url or os.environ.get("OLLAMA_HOST", "http://localhost:11434")).rstrip("/")
        if not self.base.startswith("http"):
            self.base = "http://" + self.base
        self.num_ctx, self.timeout = num_ctx, timeout

    def _complete(self, system, prompt, max_tokens):
        # keep_alive keeps the model in memory between requests (loading it takes ~40 s on a
        # laptop); num_ctx must stay the same for every call, or Ollama reloads the model.
        r = _post(f"{self.base}/api/chat", {},
                  {"model": self.model, "stream": False, "format": "json", "keep_alive": KEEP_ALIVE,
                   "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}],
                   "options": {"temperature": self.temperature, "num_ctx": self.num_ctx,
                               "num_predict": min(max_tokens, 4096)}},
                  retries=2, timeout=self.timeout)
        if "error" in r:
            raise LLMError(f"Ollama: {r['error']}")
        return r["message"]["content"], r.get("prompt_eval_count", 0), r.get("eval_count", 0)

    def warm_up(self) -> None:
        """Load the model into memory now, so the first real request does not wait for it."""
        _post(f"{self.base}/api/chat", {}, {"model": self.model, "messages": [], "keep_alive": KEEP_ALIVE,
                                            "options": {"num_ctx": self.num_ctx}}, retries=1, timeout=300)


KEEP_ALIVE = os.environ.get("AMT_OLLAMA_KEEP_ALIVE", "60m")


def ollama_status(base_url: str | None = None, model: str | None = None) -> dict:
    """Is Ollama running, and is the configured model downloaded?"""
    base = (base_url or os.environ.get("OLLAMA_HOST", "http://localhost:11434")).rstrip("/")
    if not base.startswith("http"):
        base = "http://" + base
    model = model or os.environ.get("AMT_OLLAMA_MODEL", "qwen2.5:7b")
    try:
        with urllib.request.urlopen(f"{base}/api/tags", timeout=1.5) as resp:
            names = [m["name"] for m in json.loads(resp.read()).get("models", [])]
    except (urllib.error.URLError, OSError, ValueError):
        return {"running": False, "model": model, "has_model": False}
    has = any(n == model or n.split(":")[0] == model or n == model + ":latest" for n in names)
    return {"running": True, "model": model, "has_model": has, "models": names}


def make_client(provider: str | None = None, model: str | None = None, cache_dir: str | None = ".llm_cache") -> LLMClient:
    """provider: 'anthropic', 'ollama', or None/'auto' (Anthropic if a key is set, else local Ollama)."""
    provider = (provider or os.environ.get("AMT_LLM_PROVIDER") or "auto").lower()
    if provider == "auto":
        provider = "anthropic" if os.environ.get("ANTHROPIC_API_KEY") else "ollama"
    if provider == "anthropic":
        return AnthropicClient(model, cache_dir=cache_dir)
    if provider == "ollama":
        return OllamaClient(model, cache_dir=cache_dir)
    raise LLMError(f"unknown LLM provider {provider!r} (use anthropic or ollama)")


def extract_json(text: str) -> dict:
    """Pull the first top-level JSON object out of a model response."""
    dec = json.JSONDecoder()
    i = text.find("{")
    while i >= 0:
        try:
            obj, _ = dec.raw_decode(text, i)
            if isinstance(obj, dict):
                return obj
        except ValueError:
            pass
        i = text.find("{", i + 1)
    raise LLMError("no JSON object in response")
