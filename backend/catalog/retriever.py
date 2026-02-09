from __future__ import annotations

from typing import Any, Dict, List, Tuple
import re


def _tokenize(text: str) -> List[str]:
    text = (text or "").lower()
    text = re.sub(r"[^a-z0-9çğıöşüİı\s]", " ", text)
    parts = [p.strip() for p in text.split() if p.strip()]
    # çok kısa kelimeleri at (ör: "de", "da" vb)
    return [p for p in parts if len(p) >= 2]


def retrieve_relevant_columns(catalog: Dict[str, Any], question: str, top_k: int = 12) -> List[Dict[str, Any]]:
    """
    Basit retrieval:
    - question tokenları
    - column_name / description / synonyms içinde geçiyorsa skor +1/+2
    """
    tokens = _tokenize(question)
    if not tokens:
        return []

    scored: List[Tuple[int, Dict[str, Any]]] = []

    for table in catalog["tables"].values():
        for col in table["columns"]:
            hay = " ".join(
                [
                    (col.get("table_name") or "").lower(),
                    (col.get("column_name") or "").lower(),
                    (col.get("description") or "").lower(),
                    " ".join([s.lower() for s in col.get("synonyms", [])]),
                    (col.get("semantic_role") or "").lower(),
                ]
            )

            score = 0
            for t in tokens:
                if t in (col.get("column_name") or "").lower():
                    score += 4
                if t in (col.get("table_name") or "").lower():
                    score += 2
                if t in hay:
                    score += 1

            if score > 0:
                scored.append((score, col))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for _, c in scored[:top_k]]
