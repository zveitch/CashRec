# /src/tagging.py
import re
from typing import Dict, List, Optional, Tuple
import pandas as pd
from .text_utils import normalize_text, extract_years, first_nonempty

def derive_type_group(txn_type: str, rules: Dict) -> str:
    type_groups = rules.get("type_groups", {})
    for group_name, types in type_groups.items():
        if txn_type in types:
            return group_name
    return "Other"

def match_priority_phrase(text_original: str, priority_phrases: List[str]) -> Optional[str]:
    if not isinstance(text_original, str):
        return None
    for phrase in priority_phrases:
        pattern = r"\b" + re.escape(phrase) + r"\b"
        if re.search(pattern, text_original, flags=re.IGNORECASE):
            return phrase.upper()
    return None

def tag_investment(text_original: str, text_norm: str, rules: Dict):
    inv_rules = rules.get("investment", {})
    spv_priority = [normalize_text(x) for x in inv_rules.get("spv_priority", [])]
    originators = inv_rules.get("originators", {})

    found_spv = None
    found_originator = None
    found_year = None
    meta = {"source": "investment", "candidates": {}}

    # Build SPV synonym map -> canonical SPV name
    spv_syn_to_name = {}
    for org_name, org_def in originators.items():
        for spv_name, spv_def in (org_def.get("spvs") or {}).items():
            for syn in spv_def.get("synonyms", []) + [spv_name]:
                spv_syn_to_name[normalize_text(syn)] = spv_name

    # Priority SPV scan (by normalized phrase)
    for spv_phrase_norm in spv_priority:
        if not spv_phrase_norm:
            continue
        if re.search(r"\b" + re.escape(spv_phrase_norm) + r"\b", text_norm):
            found_spv = spv_syn_to_name.get(spv_phrase_norm) or spv_phrase_norm
            break

    # Fallback: any SPV synonym
    if not found_spv:
        for syn_norm, spv_name in spv_syn_to_name.items():
            if re.search(r"\b" + re.escape(syn_norm) + r"\b", text_norm):
                found_spv = spv_name
                break

    # Originator
    if found_spv:
        for org_name, org_def in originators.items():
            if found_spv in (org_def.get("spvs") or {}):
                found_originator = org_name
                break
    else:
        for org_name, org_def in originators.items():
            for syn in (org_def.get("synonyms", []) + [org_name]):
                if re.search(r"\b" + re.escape(normalize_text(syn)) + r"\b", text_norm):
                    found_originator = org_name
                    break
            if found_originator:
                break

    # Year extraction
    years = extract_years(text_original or "")
    if years:
        found_year = years[0]

    # Enforce SPV-level year requirement ONLY if SPV matched
    year_required_missing = False
    if found_spv and found_originator:
        spv_def = (
            rules.get("investment", {})
                 .get("originators", {})
                 .get(found_originator, {})
                 .get("spvs", {})
                 .get(found_spv, {})
        )
        if spv_def.get("years_required", False) and not found_year:
            year_required_missing = True

    meta["candidates"] = {"spv": found_spv, "originator": found_originator, "year": found_year}
    meta["year_required_missing"] = year_required_missing
    return (found_originator, found_spv, found_year, meta)

def _find_year(text_original: str) -> Optional[str]:
    yrs = extract_years(text_original or "")
    return yrs[0] if yrs else None

def _match_by_synonyms(text_norm: str, items: list[dict]) -> Tuple[Optional[dict], Optional[str]]:
    for item in items:  # items pre-sorted by priority
        for syn in item.get("synonyms", []):
            syn_norm = normalize_text(syn)
            if syn_norm and re.search(r"\b" + re.escape(syn_norm) + r"\b", text_norm):
                year = _find_year(text_norm)  # or text_original if you prefer
                if item.get("years_required", False) and not year:
                    continue
                return item, year
    return None, None

def tag_investor(text_original: str, text_norm: str, rules: Dict):
    inv_rules = rules.get("investor", {}).get("tags", [])
    if not inv_rules:
        return None, None, {"source": "investor_rules", "matched": False}
    item, year = _match_by_synonyms(text_norm, inv_rules)
    if item:
        # item["tag"] is your canonical investor name
        return item.get("tag"), year, {"source": "investor_rules", "matched": True}
    return None, None, {"source": "investor_rules", "matched": False}

def tag_expense(text_original: str, text_norm: str, txn_type: str, rules: Dict):
    by_type = rules.get("expenses", {}).get("by_type", {})
    items = by_type.get(txn_type, [])
    if not items:
        return None, None, {"source": "expense_rules", "matched": False}
    item, year = _match_by_synonyms(text_norm, items)  # already defined for investor
    if item:
        return item.get("tag"), year, {"source": "expense_rules", "matched": True}
    return None, None, {"source": "expense_rules", "matched": False}

def tag_row(row: pd.Series, rules: Dict) -> Dict:
    d1b = row.get("Description1B", "")
    d2 = row.get("Description2", "")
    det = row.get("Detail", "")
    text_original = " | ".join([str(d1b or ""), str(d2 or ""), str(det or "")]).strip()
    text_norm = normalize_text(text_original)

    txn_type = str(row.get("Type", "")).strip()
    type_group = derive_type_group(txn_type, rules)

    # ✳️ Priority phrase override only for NON-investment rows.
    # Investment rows rely on SPV priority logic so we can set Originator/SPV and enforce year.
    if type_group != "Investment payments":
        priority = match_priority_phrase(text_original, rules.get("priority_phrases", []))
        if priority:
            return {
                "Type_Group": type_group,
                "Tag": priority,
                "Investor": None,
                "Originator": None,
                "SPV": None,
                "Tag_Year": first_nonempty(*extract_years(text_original)),
                "Tag_Source": "priority_phrase",
                "Tag_Year_Required_Missing": False,
            }

    if type_group == "Investment payments":
        org, spv, year, meta = tag_investment(text_original, text_norm, rules)
        if org or spv:
            comp = " / ".join([x for x in [org, spv, year] if x])
            return {
                "Type_Group": type_group,
                "Tag": comp if comp else None,
                "Investor": None,
                "Originator": org,
                "SPV": spv,
                "Tag_Year": year,
                "Tag_Source": meta.get("source", "investment"),
                "Tag_Year_Required_Missing": bool(meta.get("year_required_missing", False)),
            }
        # Nothing matched in investment context
        return {
            "Type_Group": type_group,
            "Tag": None,
            "Investor": None,
            "Originator": None,
            "SPV": None,
            "Tag_Year": first_nonempty(*extract_years(text_original)),
            "Tag_Source": "investment_no_match",
            "Tag_Year_Required_Missing": False,
        }

    if type_group == "Investor payments":
        inv_tag, inv_year, meta = tag_investor(text_original, text_norm, rules)
        if inv_tag:
            return {
                "Type_Group": type_group,
                "Tag": inv_tag,  # << Tag is the investor name
                "Investor": inv_tag,  # << explicit column for clarity
                "Originator": None,
                "SPV": None,
                "Tag_Year": inv_year,  # use if you want investor-year notion
                "Tag_Source": meta.get("source", "investor_rules"),
                "Tag_Year_Required_Missing": (
                        meta.get("matched") and inv_year is None and any(
                    t.get("tag") == inv_tag and t.get("years_required", False)
                    for t in rules.get("investor", {}).get("tags", [])
                )
                ),
            }
        # No match → show up in exceptions so ops can add synonyms
        return {
            "Type_Group": type_group,
            "Tag": None,
            "Investor": None,
            "Originator": None,
            "SPV": None,
            "Tag_Year": first_nonempty(*extract_years(text_original)),
            "Tag_Source": "investor_no_match",
            "Tag_Year_Required_Missing": False,
        }
    if type_group == "Expense":
        exp_tag, exp_year, meta = tag_expense(text_original, text_norm, txn_type, rules)
        if exp_tag:
            return {
                "Type_Group": type_group,
                "Tag": exp_tag,  # ⬅️ Tag is the expense tag from CSV
                "Investor": None,
                "Originator": None,
                "SPV": None,
                "Tag_Year": exp_year,
                "Tag_Source": meta.get("source", "expense_rules"),
                "Tag_Year_Required_Missing": False,
            }
        # Fallback only if no rule matched for the given Type
        return {
            "Type_Group": type_group,
            "Tag": txn_type if txn_type else None,
            "Investor": None,
            "Originator": None,
            "SPV": None,
            "Tag_Year": first_nonempty(*extract_years(text_original)),
            "Tag_Source": "expense_fallback_type",
            "Tag_Year_Required_Missing": False,
        }
    # ✨ For all other Types: Tag = Type (passthrough)
    return {
        "Type_Group": type_group,
        "Tag": txn_type if txn_type else None,
        "Investor": None,
        "Originator": None,
        "SPV": None,
        "Tag_Year": first_nonempty(*extract_years(text_original)),
        "Tag_Source": "type_passthrough",
        "Tag_Year_Required_Missing": False,
    }
