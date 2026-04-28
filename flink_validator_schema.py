
#  FLINK SCHEMA-BASED DATA VALIDATOR

SCHEMA = {
    "name": {
        "type": str,
        "required": True,
        "allow_empty": False,
        "min_length": 2,
        "max_length": 100,
    },
    "age": {
        "type": int,
        "required": True,
        "allow_empty": False,
        "min": 0,
        "max": 130,
    },
    "city": {
        "type": str,
        "required": True,
        "allow_empty": False,
    },
    "status": {
        "type": str,
        "required": True,
        "allow_empty": False,
        "allowed": ["active", "inactive", "pending"],
    },
    "score": {
        "type": float,
        "required": False,
        "allow_empty": True,
        "nullable": True,
        "min": 0.0,
        "max": 100.0,
    },
}



INPUT_MODE  = "json_string"   

FILE_PATH   = "data.json"     

JSON_STRING = """
[
  {"name": "Alice",  "age": 28,   "city": "Lagos",  "status": "active",   "score": 87.5},
  {"name": "",       "age": 30,   "city": "Abuja",  "status": "inactive", "score": null},
  {"name": "Chidi",  "age": -5,   "city": "PH",     "status": "unknown",  "score": 110.0},
  {"name": "T",      "age": null, "city": "",        "status": "pending",  "score": 60.0},
  {"name": "Emeka",  "age": 25,   "city": "Kano",   "status": "active"}
]
"""

STRICT_MODE  = False  
SAVE_REPORT  = False   
REPORT_PATH  = "flink_validation_report.json"


#  ENGINE 

import json, os
from collections import defaultdict
from typing import Any

try:
    import fastavro
    AVRO_AVAILABLE = True
except ImportError:
    AVRO_AVAILABLE = False


# ── Loaders ───────────────────────────────────────────────

def load_json_file(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [data] if isinstance(data, dict) else data

def load_json_str(raw):
    data = json.loads(raw)
    return [data] if isinstance(data, dict) else data

def load_avro_file(path):
    if not AVRO_AVAILABLE:
        raise ImportError("Run:  !pip install fastavro")
    records = []
    with open(path, "rb") as f:
        for rec in fastavro.reader(f):
            records.append(dict(rec))
    return records



def flatten(record: dict, parent: str = "", sep: str = ".") -> dict:
    out = {}
    for k, v in record.items():
        key = f"{parent}{sep}{k}" if parent else k
        if isinstance(v, dict):
            out.update(flatten(v, key, sep))
        else:
            out[key] = v
    return out


# ── Value checks ──────────────────────────────────────────

def is_missing(v):
    return v is None

def is_empty(v, strict=False):
    if isinstance(v, str):
        return (v.strip() == "") if strict else (v == "")
    if isinstance(v, (list, dict)):
        return len(v) == 0
    return False

def type_name(t):
    return t.__name__ if isinstance(t, type) else str(t)


# ── Core validator ────────────────────────────────────────

def validate_record(record: dict, idx: int, schema: dict, strict: bool = False) -> list:
    issues = []
    flat   = flatten(record)

    for field, rules in schema.items():
        expected_type = rules.get("type")
        required      = rules.get("required", True)
        allow_empty   = rules.get("allow_empty", False)
        nullable      = rules.get("nullable", False)
        allowed_vals  = rules.get("allowed")
        min_val       = rules.get("min")
        max_val       = rules.get("max")
        min_len       = rules.get("min_length")
        max_len       = rules.get("max_length")

        # ── Field presence ────────────────────────────────
        if field not in flat:
            if required:
                issues.append({
                    "record_index": idx,
                    "field": field,
                    "value": "<field not present>",
                    "issue_type": "MISSING FIELD",
                    "detail": f"Required field '{field}' is absent from the record.",
                })
            continue

        value = flat[field]

        # ── Null check ────────────────────────────────────
        if is_missing(value):
            if required and not nullable:
                issues.append({
                    "record_index": idx,
                    "field": field,
                    "value": "null",
                    "issue_type": "MISSING VALUE (null/None)",
                    "detail": f"Field '{field}' is required but is null.",
                })
            continue

        # ── Empty check ───────────────────────────────────
        if not allow_empty and is_empty(value, strict=strict):
            issues.append({
                "record_index": idx,
                "field": field,
                "value": repr(value),
                "issue_type": "EMPTY VALUE",
                "detail": f"Field '{field}' must not be empty.",
            })
            continue

        # ── Type check ────────────────────────────────────
        if expected_type and not isinstance(value, expected_type):
       
            if not (expected_type is float and isinstance(value, int)):
                issues.append({
                    "record_index": idx,
                    "field": field,
                    "value": repr(value),
                    "issue_type": "WRONG TYPE",
                    "detail": (
                        f"Expected {type_name(expected_type)}, "
                        f"got {type(value).__name__} → {repr(value)}"
                    ),
                })
                continue  

        # ── Numeric constraints ───────────────────────────
        if isinstance(value, (int, float)):
            if min_val is not None and value < min_val:
                issues.append({
                    "record_index": idx,
                    "field": field,
                    "value": repr(value),
                    "issue_type": "CONSTRAINT VIOLATION",
                    "detail": f"Value {value} is below minimum allowed ({min_val}).",
                })
            if max_val is not None and value > max_val:
                issues.append({
                    "record_index": idx,
                    "field": field,
                    "value": repr(value),
                    "issue_type": "CONSTRAINT VIOLATION",
                    "detail": f"Value {value} exceeds maximum allowed ({max_val}).",
                })

        # ── Length constraints (strings / lists) ──────────
        if isinstance(value, (str, list)):
            length = len(value)
            if min_len is not None and length < min_len:
                issues.append({
                    "record_index": idx,
                    "field": field,
                    "value": repr(value),
                    "issue_type": "CONSTRAINT VIOLATION",
                    "detail": f"Length {length} is below minimum length ({min_len}).",
                })
            if max_len is not None and length > max_len:
                issues.append({
                    "record_index": idx,
                    "field": field,
                    "value": repr(value),
                    "issue_type": "CONSTRAINT VIOLATION",
                    "detail": f"Length {length} exceeds maximum length ({max_len}).",
                })

        # ── Allowed values ────────────────────────────────
        if allowed_vals is not None and value not in allowed_vals:
            issues.append({
                "record_index": idx,
                "field": field,
                "value": repr(value),
                "issue_type": "INVALID VALUE",
                "detail": f"'{value}' is not in allowed values: {allowed_vals}",
            })

    return issues


# ── Report ────────────────────────────────────────────────

ISSUE_ICONS = {
    "MISSING FIELD":           "[MISSING FIELD]  ",
    "MISSING VALUE (null/None)":"[MISSING VALUE]  ",
    "EMPTY VALUE":             "[EMPTY]          ",
    "WRONG TYPE":              "[WRONG TYPE]     ",
    "CONSTRAINT VIOLATION":    "[CONSTRAINT]     ",
    "INVALID VALUE":           "[INVALID VALUE]  ",
}

def print_report(issues, total_records, fmt, source):
    DIV  = "─" * 64
    HDIV = "═" * 64

    print(HDIV)
    print("  FLINK SCHEMA VALIDATION REPORT")
    print(HDIV)
    print(f"  Source  : {source}")
    print(f"  Format  : {fmt.upper()}")
    print(f"  Records : {total_records}")
    print(DIV)

    if not issues:
        print("\n  ✔  All records passed schema validation.\n")
        print(HDIV)
        return

    by_record = defaultdict(list)
    for iss in issues:
        by_record[iss["record_index"]].append(iss)

    for rec_idx in sorted(by_record.keys()):
        rec_issues = by_record[rec_idx]
        print(f"\n  Record #{rec_idx + 1}  —  {len(rec_issues)} issue(s)")
        print(f"  {DIV}")
        for iss in rec_issues:
            icon = ISSUE_ICONS.get(iss["issue_type"], "[ISSUE]          ")
            print(f"    {icon} Field  : {iss['field']}")
            print(f"                       Issue  : {iss['issue_type']}")
            print(f"                       Detail : {iss['detail']}")
            print(f"                       Value  : {iss['value']}")
        print()

    # Tally by type
    tally = defaultdict(int)
    for iss in issues:
        tally[iss["issue_type"]] += 1

    affected = len(by_record)

    print(HDIV)
    print("  SUMMARY")
    print(DIV)
    print(f"  Total records checked          : {total_records}")
    print(f"  Records with issues            : {affected}")
    print(f"  {'─'*44}")
    for issue_type, count in sorted(tally.items()):
        label = f"  {issue_type}"
        print(f"  {issue_type:<40}: {count}")
    print(f"  {'─'*44}")
    print(f"  TOTAL FLAGGED ISSUES           : {len(issues)}")
    print(HDIV)


def save_json_report(issues, total_records, fmt, source, out_path):
    by_record = defaultdict(list)
    for iss in issues:
        by_record[str(iss["record_index"])].append({
            "field":      iss["field"],
            "value":      str(iss["value"]),
            "issue_type": iss["issue_type"],
            "detail":     iss["detail"],
        })
    tally = defaultdict(int)
    for iss in issues:
        tally[iss["issue_type"]] += 1

    report = {
        "source": source,
        "format": fmt,
        "total_records": total_records,
        "total_flagged_issues": len(issues),
        "tally_by_type": dict(tally),
        "records_with_issues": len(by_record),
        "details": by_record,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"\n  JSON report saved → {out_path}")


# ── RUN ───────────────────────────────────────────────────

if INPUT_MODE == "file":
    ext = os.path.splitext(FILE_PATH)[1].lower()
    if ext == ".json":
        records = load_json_file(FILE_PATH)
        fmt, source = "json", FILE_PATH
    elif ext == ".avro":
        records = load_avro_file(FILE_PATH)
        fmt, source = "avro", FILE_PATH
    else:
        raise ValueError(f"Unsupported extension '{ext}'. Use .json or .avro")

elif INPUT_MODE == "json_string":
    records = load_json_str(JSON_STRING)
    fmt, source = "json", "<inline JSON string>"

else:
    raise ValueError("INPUT_MODE must be 'file' or 'json_string'")

all_issues = []
for idx, record in enumerate(records):
    if not isinstance(record, dict):
        print(f"  Warning: Record #{idx+1} is not a dict — skipped.")
        continue
    all_issues.extend(validate_record(record, idx, SCHEMA, strict=STRICT_MODE))

print_report(all_issues, len(records), fmt, source)

if SAVE_REPORT:
    save_json_report(all_issues, len(records), fmt, source, REPORT_PATH)
