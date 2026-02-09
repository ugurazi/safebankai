from __future__ import annotations

import requests
from typing import Any, Dict, Optional


class OllamaClient:
    def __init__(self, base_url: str = "http://localhost:11434", model: str = "llama3.1:latest"):
        self.base_url = base_url.rstrip("/")
        self.model = model

    def generate(
        self,
        prompt: str,
        system: Optional[str] = None,
        temperature: float = 0.0,
        force_json: bool = False,   # ✅ yeni
    ) -> str:
        url = f"{self.base_url}/api/generate"
        payload: Dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": temperature},
        }
        if system:
            payload["system"] = system

        # ✅ Ollama'da JSON output zorla
        if force_json:
            payload["format"] = "json"

        r = requests.post(url, json=payload, timeout=120)
        r.raise_for_status()
        return r.json().get("response", "")
