# /src/rules_csv.py
from __future__ import annotations
import csv
from pathlib import Path
from typing import Dict, List
from .config import RULES_DIR

def _read_csv(path: Path) -> List[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def _split(cell: str) -> List[str]:
    return [s.strip() for s in str(cell or "").split("|") if s.strip()]

def _to_bool(cell: str) -> bool:
    return str(cell).strip().lower() in {"1", "true", "yes", "y", "t"}

def _to_int(cell: str, default: int) -> int:
    try:
        return int(str(cell).strip())
    except Exception:
        return default

# /src/rules_csv.py (additions)
def _load_investor_rules(rules_dir: Path):
    rows = _read_csv(rules_dir / "investor_tags.csv")
    tags = []
    for r in rows:
        tag = (r.get("tag") or "").strip()
        if not tag:
            continue
        tags.append({
            "tag": tag,
            "synonyms": _split(r.get("synonyms")),
            "priority": _to_int(r.get("priority", 100), 100),
            "years_required": _to_bool(r.get("years_required")),
        })
    # sort by priority asc
    tags.sort(key=lambda x: x["priority"])
    return tags

def _load_expense_rules(rules_dir: Path):
    rows = _read_csv(rules_dir / "expense_tags.csv")
    by_type = {}
    for r in rows:
        typ = (r.get("type") or "").strip()
        tag = (r.get("tag") or "").strip()
        if not typ or not tag:
            continue
        by_type.setdefault(typ, []).append({
            "tag": tag,
            "synonyms": _split(r.get("synonyms")),
            "priority": _to_int(r.get("priority", 100), 100),
            "years_required": _to_bool(r.get("years_required")),
        })
    # sort each type list by priority asc
    for t, lst in by_type.items():
        lst.sort(key=lambda x: x["priority"])
    return by_type

def load_rules_from_csv(rules_dir: Path = RULES_DIR) -> Dict:
    """
    Load ops-maintained CSV rules and build the in-memory rules dict.
    For non-investment rows: Tag = Type (no keywords/regex).
    SPV-level `years_required` is enforced when an SPV is matched.
    """
    rules_dir.mkdir(parents=True, exist_ok=True)

    originators_rows = _read_csv(rules_dir / "investment_originators.csv")
    spvs_rows        = _read_csv(rules_dir / "investment_spvs.csv")
    pri_phr_rows     = _read_csv(rules_dir / "priority_phrases.csv")
    investor_rules = _load_investor_rules(rules_dir)
    expense_rules = _load_expense_rules(rules_dir)

    # Priority phrases (global; not used to override investment logic, but used for non-investment)
    priority_phrases = [
        r.get("phrase", "").strip()
        for r in sorted(pri_phr_rows, key=lambda x: _to_int(x.get("priority", 100), 100))
        if (r.get("phrase") or "").strip()
    ]

    # Type grouping — fixed mapping (based on your description)
    type_groups = {
        "Investor payments": ["Capital Paydown", "Subscription", "Distribution"],
        "Investment payments": ["Prepayment", "Investment", "Rent"],
        "Murabaha": ["Murabaha"],
        "Expense": ["Mgmt Fees", "Fees and Expenses"],
        "Other": []
    }

    # Investment: originators & SPVs (SPV-level `years_required`)
    originators: Dict[str, Dict] = {}
    for r in originators_rows:
        org = (r.get("originator") or "").strip()
        if not org:
            continue
        originators.setdefault(org, {"synonyms": [], "spvs": {}})
        originators[org]["synonyms"].extend(_split(r.get("originator_synonyms")))

    spv_priority_pairs = []
    for r in spvs_rows:
        org = (r.get("originator") or "").strip()
        spv = (r.get("spv") or "").strip()
        if not org or not spv:
            continue
        originators.setdefault(org, {"synonyms": [], "spvs": {}})
        syns = _split(r.get("spv_synonyms"))
        years_required = _to_bool(r.get("years_required"))
        originators[org]["spvs"].setdefault(spv, {"synonyms": [], "years_required": False})
        originators[org]["spvs"][spv]["synonyms"].extend(syns)
        originators[org]["spvs"][spv]["years_required"] = years_required

        prio = _to_int(r.get("priority", 100), 100)
        # Add canonical SPV and each synonym to global priority with same precedence
        spv_priority_pairs.append((spv, prio))
        for s in syns:
            spv_priority_pairs.append((s, prio))

    spv_priority_sorted = [p for p, _ in sorted(spv_priority_pairs, key=lambda x: x[1])]
    seen = set()
    spv_priority = []
    for item in spv_priority_sorted:
        key = item.strip().upper()
        if key and key not in seen:
            spv_priority.append(item)
            seen.add(key)

    rules = {
        "version": 4,
        "priority_phrases": priority_phrases,
        "type_groups": type_groups,
        # No type_rules: Tag = Type for non-investment rows
        "type_rules": {},
        "investment": {
            "spv_priority": spv_priority,
            "originators": {
                org: {
                    "synonyms": sorted(set(defn.get("synonyms", []))),
                    "spvs": {
                        spv: {
                            "synonyms": sorted(set(spvdef.get("synonyms", []))),
                            "years_required": bool(spvdef.get("years_required", False))
                        }
                        for spv, spvdef in (defn.get("spvs") or {}).items()
                    }
                }
                for org, defn in originators.items()
            }
        },
        # NEW:
        "investor": {
            "tags": investor_rules  # list of dicts with tag, synonyms, priority, years_required
        },
        "expenses": {
            "by_type": expense_rules  # dict: type -> list of tag dicts
        }
    }
    return rules