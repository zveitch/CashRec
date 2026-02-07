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
            "Originator": None,
            "SPV": None,
            "Tag_Year": first_nonempty(*extract_years(text_original)),
            "Tag_Source": "investment_no_match",
            "Tag_Year_Required_Missing": False,
        }

    # ✨ For all other Types: Tag = Type (passthrough)
    return {
        "Type_Group": type_group,
        "Tag": txn_type if txn_type else None,
        "Originator": None,
        "SPV": None,
        "Tag_Year": first_nonempty(*extract_years(text_original)),
        "Tag_Source": "type_passthrough",
        "Tag_Year_Required_Missing": False,
    }
