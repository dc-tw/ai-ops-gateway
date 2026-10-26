"""Pinned, inspectable ATR rules; never silently accept a failed condition."""

import json
from dataclasses import asdict, replace
from hashlib import sha256
from pathlib import Path

from pyatr import ATREngine
from pyatr.engine import _load_yaml_file, _parse_rule, _read_bundled_rules

# pyatr 0.3.1 bundled condition references group 1 without declaring it.
# Preserve the expression, adding only that capture: a local rule revision.
BROKEN_REPEAT_PATTERN = r"(?:(?:\w+\s+){40,})\1{3,}"
FIXED_REPEAT_PATTERN = r"((?:\w+\s+){40,})\1{3,}"
RULESET_REVISION = "gateway-atr-0.3.1-r1"


class GatewayATREngine(ATREngine):
    def load_bundled_rules(self):
        # Upstream loaders skip parse errors. Read the pinned bundle strictly so
        # one malformed necessary rule cannot disappear behind a nonzero count.
        raw = _read_bundled_rules()
        if not isinstance(raw, list) or not raw:
            raise RuntimeError("ATR bundled rules unavailable")
        for item in raw:
            self._load_required_rule(item)
        return len(raw)

    def load_rules_from_directory(self, directory):
        root = Path(directory)
        if not root.is_dir():
            raise RuntimeError("ATR rule directory unavailable")
        files = sorted(path for path in root.rglob("*") if path.suffix.lower() in {".yaml", ".yml"})
        if not files or len(files) > 4096:
            raise RuntimeError("ATR rule directory exceeds limits or is empty")
        for path in files:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 262_144:
                raise RuntimeError("ATR rule file is not supported")
            try:
                self._load_required_rule(_load_yaml_file(path))
            except Exception:
                raise RuntimeError("ATR rule file failed validation") from None
        return len(files)

    def _load_required_rule(self, data):
        if not isinstance(data, dict) or not isinstance(data.get("id"), str) or not data["id"]:
            raise RuntimeError("ATR rule identity is invalid")
        try:
            self._add_rule(_parse_rule(data))
        except Exception:
            raise RuntimeError("ATR rule failed validation") from None

    def _add_rule(self, rule):
        operators = {"regex", "contains", "exact", "starts_with", "gt", "lt", "gte", "lte", "eq"}
        if (
            rule.condition_logic not in {"any", "all"}
            or rule.severity not in {"informational", "low", "medium", "high", "critical"}
            or not 1 <= len(rule.conditions) <= 64
            or any(
                not c.field or c.operator not in operators or not c.value or len(c.value) > 8192
                for c in rule.conditions
            )
        ):
            raise RuntimeError("ATR rule conditions are invalid")
        if rule.id == "ATR-2026-00290" and len(rule.conditions) > 3:
            condition = rule.conditions[3]
            if condition.operator == "regex" and condition.value == BROKEN_REPEAT_PATTERN:
                conditions = list(rule.conditions)
                conditions[3] = replace(condition, value=FIXED_REPEAT_PATTERN)
                rule = replace(rule, conditions=tuple(conditions))
        super()._add_rule(rule)

    def verify(self) -> str:
        active = {rule.id for rule in self.rules if rule.status not in {"deprecated", "draft"}}
        if not active or any(rule_id in active for rule_id, _ in self._failed_regexes):
            raise RuntimeError("ATR rules failed integrity validation")
        if len({rule.id for rule in self.rules}) != len(self.rules):
            raise RuntimeError("ATR rules contain duplicate identifiers")
        document = {"revision": RULESET_REVISION, "rules": [asdict(rule) for rule in self.rules]}
        return sha256(json.dumps(document, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
