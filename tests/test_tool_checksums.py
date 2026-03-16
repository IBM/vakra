"""Unit tests for tool checksum utilities.

Tests verify:
  1. Checksum computation is stable and order-independent.
  2. Checksums differ across domains/tool-sets.
  3. verify_checksum passes when expected == actual.
  4. verify_checksum raises ValueError on mismatch (wrong domain guard).
  5. verify_checksum warns but does NOT raise when no checksum is registered.
  6. Pydantic-model inputSchema is handled correctly.

These tests are fully self-contained — no Docker containers required.

Usage:
    python -m pytest tests/test_tool_checksums.py -v
"""

import json
import logging
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from environment.tool_checksums import (
    compute_tool_checksum,
    load_checksums,
    verify_checksum,
    _verification_enabled,
)


# ---------------------------------------------------------------------------
# Helpers — lightweight stand-ins for mcp.types.Tool
# ---------------------------------------------------------------------------

def _make_tool(name: str, schema: Dict[str, Any]) -> SimpleNamespace:
    """Return a minimal object that mimics mcp.types.Tool."""
    t = SimpleNamespace()
    t.name = name
    t.inputSchema = schema
    return t


def _make_tools(names: List[str]) -> List[SimpleNamespace]:
    """Create a list of tools with simple schemas, one per name."""
    return [
        _make_tool(
            name,
            {
                "type": "object",
                "properties": {"q": {"type": "string", "description": "query"}},
                "required": ["q"],
            },
        )
        for name in names
    ]


def _write_checksums(data: Dict, path: Path) -> None:
    with open(path, "w") as f:
        json.dump(data, f)


# ---------------------------------------------------------------------------
# compute_tool_checksum
# ---------------------------------------------------------------------------

class TestComputeToolChecksum:
    def test_returns_64_char_hex(self):
        tools = _make_tools(["tool_a"])
        result = compute_tool_checksum(tools)
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_stable_across_calls(self):
        tools = _make_tools(["tool_a", "tool_b"])
        assert compute_tool_checksum(tools) == compute_tool_checksum(tools)

    def test_order_independent(self):
        tools_ab = _make_tools(["tool_a", "tool_b"])
        tools_ba = _make_tools(["tool_b", "tool_a"])
        assert compute_tool_checksum(tools_ab) == compute_tool_checksum(tools_ba)

    def test_different_names_differ(self):
        tools_a = _make_tools(["address_tool"])
        tools_b = _make_tools(["airline_tool"])
        assert compute_tool_checksum(tools_a) != compute_tool_checksum(tools_b)

    def test_different_schemas_differ(self):
        tool_v1 = _make_tool("t", {"type": "object", "properties": {}, "required": []})
        tool_v2 = _make_tool(
            "t",
            {
                "type": "object",
                "properties": {"extra": {"type": "integer"}},
                "required": ["extra"],
            },
        )
        assert compute_tool_checksum([tool_v1]) != compute_tool_checksum([tool_v2])

    def test_empty_list(self):
        result = compute_tool_checksum([])
        assert len(result) == 64

    def test_dict_input(self):
        """Accepts plain dicts as well as objects with attributes."""
        tool_obj = _make_tools(["t"])[0]
        tool_dict = {"name": "t", "inputSchema": tool_obj.inputSchema}
        assert compute_tool_checksum([tool_obj]) == compute_tool_checksum([tool_dict])

    def test_pydantic_v2_schema(self):
        """inputSchema that is a Pydantic v2 model is handled via model_dump()."""
        mock_schema = MagicMock()
        mock_schema.model_dump.return_value = {"type": "object", "properties": {}}
        # Ensure hasattr(..., "dict") returns False so model_dump branch is taken
        del mock_schema.dict
        tool = SimpleNamespace(name="pydantic_tool", inputSchema=mock_schema)
        result = compute_tool_checksum([tool])
        assert len(result) == 64
        mock_schema.model_dump.assert_called_once()


# ---------------------------------------------------------------------------
# load_checksums
# ---------------------------------------------------------------------------

class TestLoadChecksums:
    def test_returns_empty_dict_when_missing(self, tmp_path):
        result = load_checksums(tmp_path / "nonexistent.json")
        assert result == {}

    def test_loads_valid_file(self, tmp_path):
        data = {"2": {"address": "abc123"}, "4": {"address": "def456"}}
        p = tmp_path / "checksums.json"
        _write_checksums(data, p)
        assert load_checksums(p) == data

    def test_ignores_comment_key(self, tmp_path):
        """_comment key is present in the file but doesn't break loading."""
        data = {"_comment": "ignore me", "2": {"address": "abc"}}
        p = tmp_path / "checksums.json"
        _write_checksums(data, p)
        result = load_checksums(p)
        assert result["2"] == {"address": "abc"}


# ---------------------------------------------------------------------------
# verify_checksum — flag control
# ---------------------------------------------------------------------------

class TestVerificationFlag:
    """MCP_VERIFY_CHECKSUMS env var controls whether verification runs."""

    def test_disabled_by_default(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MCP_VERIFY_CHECKSUMS", None)
            assert _verification_enabled() is False

    def test_enabled_with_1(self):
        with patch.dict(os.environ, {"MCP_VERIFY_CHECKSUMS": "1"}):
            assert _verification_enabled() is True

    def test_enabled_with_true(self):
        with patch.dict(os.environ, {"MCP_VERIFY_CHECKSUMS": "true"}):
            assert _verification_enabled() is True

    def test_enabled_case_insensitive(self):
        with patch.dict(os.environ, {"MCP_VERIFY_CHECKSUMS": "TRUE"}):
            assert _verification_enabled() is True

    def test_disabled_with_0(self):
        with patch.dict(os.environ, {"MCP_VERIFY_CHECKSUMS": "0"}):
            assert _verification_enabled() is False

    def test_no_error_when_disabled_even_with_mismatch(self, tmp_path):
        """When flag is off, verify_checksum is a no-op regardless of stored checksums."""
        tools_correct = _make_tools(["tool_x"])
        tools_wrong = _make_tools(["tool_y"])
        checksum = compute_tool_checksum(tools_correct)
        p = tmp_path / "cs.json"
        _write_checksums({"2": {"address": checksum}}, p)

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MCP_VERIFY_CHECKSUMS", None)
            # Should NOT raise even though tools don't match
            verify_checksum(2, "address", tools_wrong, checksums_path=p)


# ---------------------------------------------------------------------------
# verify_checksum — matching and mismatch
# ---------------------------------------------------------------------------

class TestVerifyChecksum:
    @pytest.fixture(autouse=True)
    def _enable(self, monkeypatch):
        monkeypatch.setenv("MCP_VERIFY_CHECKSUMS", "1")

    def _write_with_real_checksum(
        self, tmp_path: Path, capability_id: int, domain: str, tools
    ) -> Path:
        """Write a checksums file with the real checksum for tools."""
        checksum = compute_tool_checksum(tools)
        data = {str(capability_id): {domain: checksum}}
        p = tmp_path / "tool_checksums.json"
        _write_checksums(data, p)
        return p

    # --- happy path ---

    def test_passes_when_tools_match(self, tmp_path):
        tools = _make_tools(["get_address_streets", "filter_address"])
        p = self._write_with_real_checksum(tmp_path, capability_id=2, domain="address", tools=tools)
        # Should not raise
        verify_checksum(2, "address", tools, checksums_path=p)

    # --- mismatch (wrong domain) ---

    def test_raises_on_wrong_domain_tools(self, tmp_path):
        address_tools = _make_tools(["get_address_streets", "filter_address"])
        airline_tools = _make_tools(["get_airline_routes", "filter_airline"])

        # Store checksum for "address"
        p = self._write_with_real_checksum(tmp_path, 2, "address", address_tools)

        # Client accidentally connected with MCP_DOMAIN=airline, so it gets
        # airline tools, but is asserting against "address" checksum.
        with pytest.raises(ValueError, match="checksum mismatch"):
            verify_checksum(2, "address", airline_tools, checksums_path=p)

    def test_error_message_contains_capability_and_domain(self, tmp_path):
        tools_correct = _make_tools(["tool_x"])
        tools_wrong = _make_tools(["tool_y"])
        p = self._write_with_real_checksum(tmp_path, 3, "bpo", tools_correct)

        with pytest.raises(ValueError) as exc_info:
            verify_checksum(3, "bpo", tools_wrong, checksums_path=p)

        msg = str(exc_info.value)
        assert "capability 3" in msg
        assert "'bpo'" in msg
        assert "Expected" in msg
        assert "Got" in msg

    # --- missing checksums (warn, don't error) ---

    def test_no_error_when_capability_not_registered(self, tmp_path, caplog):
        tools = _make_tools(["t"])
        p = tmp_path / "empty.json"
        _write_checksums({}, p)

        with caplog.at_level(logging.WARNING, logger="environment.tool_checksums"):
            verify_checksum(2, "address", tools, checksums_path=p)  # must not raise

        assert any("No checksums registered for capability" in r.message for r in caplog.records)

    def test_no_error_when_domain_not_registered(self, tmp_path, caplog):
        tools = _make_tools(["t"])
        p = tmp_path / "partial.json"
        _write_checksums({"2": {"hockey": "somehash"}}, p)

        with caplog.at_level(logging.WARNING, logger="environment.tool_checksums"):
            verify_checksum(2, "address", tools, checksums_path=p)  # must not raise

        assert any("No checksum registered" in r.message for r in caplog.records)

    def test_no_error_when_file_missing(self, tmp_path, caplog):
        tools = _make_tools(["t"])

        with caplog.at_level(logging.WARNING, logger="environment.tool_checksums"):
            verify_checksum(2, "address", tools, checksums_path=tmp_path / "missing.json")

        # Should warn (no registered capability) but not raise
        assert any("No checksums registered" in r.message for r in caplog.records)

    # --- checksums are independent per capability ---

    def test_same_tools_different_capability_independent(self, tmp_path):
        """Tasks/capabilities are looked up independently in the checksums dict."""
        tools = _make_tools(["shared_tool"])
        checksum = compute_tool_checksum(tools)
        data = {
            "2": {"address": checksum},
            "4": {"address": "different_checksum_here"},
        }
        p = tmp_path / "cs.json"
        _write_checksums(data, p)

        # Capability 2 address matches
        verify_checksum(2, "address", tools, checksums_path=p)

        # Capability 4 address does NOT match (different stored value)
        with pytest.raises(ValueError):
            verify_checksum(4, "address", tools, checksums_path=p)
