"""predict.fun DRY paper-instance deploy wiring (second scanner, own DB).

Regression guards for the operational invariants that make the paper instance
SAFE next to the real-money bot:

  * its own systemd unit, flag-gated default-OFF, own DB file, loopback :8801,
    forced-empty signing key, no live-failure pager;
  * deploy.sh stages the unit + generates the all-DRY V6 artifact + excludes
    the paper DB from the data/ prune (the b70a95f recorder-wipe lesson);
  * install_remote.sh writes the second env file and no longer wipes bnb.svg;
  * main.py venue-gates the Polymarket-only WS mirror / prefetch fetcher.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from cryptobot import config

# Tracked deploy sources anchor to the CODE tree this test lives in, never the
# env-redirectable config._REPO_ROOT: with CRYPTOBOT_ROOT pointed at a data
# checkout, config's root is a different tree that may predate this layout.
# parents[5]: tests -> cryptobot_predictfun -> src -> live -> predictfun -> repo.
# Was parents[4] while this file lived in core/src/cryptobot/tests/; Phase 2
# moved it one level deeper into the venue package.
_CODE_ROOT = Path(__file__).resolve().parents[5]  # legacy-path-ok: code-tree anchor for TRACKED files
# The flat deploy/ dir was split per venue on 2026-07-28 (plan §2). deploy.sh
# still stages every one of these into ONE $STAGING_DIR/deploy/ because the
# REMOTE layout is unchanged, so this map is the local-source index.
_DEPLOY_SOURCES = {
    "cryptobot.service": "polymarket/live/service",
    "cryptobot.env.example": "polymarket/infra",
    "deploy.sh": "polymarket/infra",
    "install_remote.sh": "polymarket/infra",
    "cryptobot-predictfun.service": "predictfun/live/service",
}


def _read(name: str) -> str:
    return (_CODE_ROOT / _DEPLOY_SOURCES[name] / name).read_text(encoding="utf-8")


def test_unit_file_safety_invariants():
    unit = _read("cryptobot-predictfun.service")
    # Env layering: shared file first, per-instance overrides file second
    # (EnvironmentFile beats Environment= regardless of order — the overrides
    # MUST come from a second file, listed later so it wins per-key).
    env_files = re.findall(r"^EnvironmentFile=(.+)$", unit, flags=re.MULTILINE)
    assert env_files == ["/etc/cryptobot.env", "/etc/cryptobot-predictfun.env"]
    # Paper crash must never page as a live-money event (no ACTIVE OnFailure=
    # directive; the explanatory comment is allowed).
    assert re.search(r"^OnFailure=", unit, flags=re.MULTILINE) is None
    # Restart=on-failure (NOT always): Restart=always would respawn a deliberate
    # flag-unset exit-0 every RestartSec forever. (The sibling assertion on
    # predictfun-book-recorder.service went with that unit, deleted 2026-08-04.)
    assert re.search(r"^Restart=on-failure$", unit, flags=re.MULTILINE)
    # Same entrypoint as the live scanner (venue seam is env-driven).
    assert "ExecStart=/home/ec2-user/cryptobot/.venv/bin/python -m cryptobot" in unit
    # Own log files — never scanner.log/scanner.err.
    assert "predictfun-scanner.log" in unit and "predictfun-scanner.err" in unit
    assert "append:/var/log/cryptobot/scanner.log" not in unit

    # OOM-killer preference: paper MemoryMax strictly below the live unit's.
    live = _read("cryptobot.service")
    live_max = int(re.search(r"^MemoryMax=(\d+)M", live, flags=re.MULTILINE).group(1))
    pf_max = int(re.search(r"^MemoryMax=(\d+)M", unit, flags=re.MULTILINE).group(1))
    assert pf_max < live_max

    # 2026-07-21 safety audit: the sandbox writes EXACTLY its own DB files, not
    # the whole data/ tree — the live cryptobot.db is read-only in this unit's
    # mount namespace, so a paper write to the live ledger is structurally
    # impossible (not just configured away).
    rw_paths = " ".join(
        re.findall(r"^ReadWritePaths=(.+)$", unit, flags=re.MULTILINE)
    ).split()
    stripped = [p.lstrip("-") for p in rw_paths]
    assert "/home/ec2-user/cryptobot/data" not in stripped
    # Directory-level carve-out for the paper DB ONLY: SQLite WAL needs
    # create/unlink freedom in the DB's directory (file-level bind mounts of
    # db/-wal/-shm broke PRAGMA journal_mode=WAL on first boot, 2026-07-22).
    assert (
        "-/home/ec2-user/cryptobot/data/predictfun_paper" in rw_paths
    ), "own-DB-directory ReadWritePaths carve-out missing"
    # Live-mode variant's DB dir (2026-07-23): also '-' prefixed so a
    # paper-only box (dir absent) still boots.
    assert (
        "-/home/ec2-user/cryptobot/data/predictfun_live" in rw_paths
    ), "live-DB-directory ReadWritePaths carve-out missing"
    assert not any("cryptobot_predictfun.db" in p for p in rw_paths)
    # It still needs the runtime LUT cache + its own logs — nothing else.
    assert "/home/ec2-user/cryptobot/runtime/data" in stripped
    assert "/var/log/cryptobot" in stripped


def _pf_env_blocks(script: str) -> tuple[dict, dict]:
    """Parse the LIVE and PAPER /etc/cryptobot-predictfun.env heredocs.

    install_remote.sh 4b branches on CRYPTOBOT_PREDICTFUN_LIVE (2026-07-23):
    the LIVE variant's heredoc comes first (inside `if`), the paper variant
    second (inside `else`). Returns (live_env, paper_env) dicts.
    """
    blocks = re.findall(
        r"sudo tee /etc/cryptobot-predictfun\.env > /dev/null <<EOF\n(.*?)\nEOF",
        script,
        flags=re.DOTALL,
    )
    assert len(blocks) == 2, "expected exactly the LIVE + paper env heredocs"

    def _parse(block: str) -> dict:
        return dict(
            line.split("=", 1)
            for line in block.splitlines()
            if "=" in line and not line.startswith("#")
        )

    live, paper = _parse(blocks[0]), _parse(blocks[1])
    # Orientation guard: the first block must be the live one.
    assert live.get("DRY_RUN") == "false" and paper.get("DRY_RUN") == "true"
    return live, paper


def test_install_remote_writes_override_env_and_arms_unit():
    script = _read("install_remote.sh")
    # The unit is installed via the sed/tee loop like every other unit.
    assert "cryptobot-predictfun.service" in script
    # Flag-gated arming, same pattern as the recorders, default OFF.
    assert re.search(
        r"arm_flag_gated_unit CRYPTOBOT_PREDICTFUN_DRY_ENABLED "
        r"cryptobot-predictfun\.service",
        script,
    )
    _live, env = _pf_env_blocks(script)
    assert env["CRYPTOBOT_VENUE"] == "predictfun"
    assert env["DRY_RUN"] == "true"
    assert env["CRYPTOBOT_INITIAL_BANKROLL"] == "100"
    # OWN DB — never the live data/cryptobot.db.
    assert env["CRYPTOBOT_DB_PATH"].endswith("/data/predictfun_paper/cryptobot_predictfun.db")
    # No 8800 collision — distinct loopback port.
    assert env["CRYPTOBOT_DASHBOARD_PORT"] == "8801"
    # Explicit coin pin: the shared env force-pins the Polymarket universe and
    # an explicit CRYPTOBOT_COINS wins over the venue default. BTC,ETH ONLY
    # (operator order 2026-07-21): BNB is out of the paper universe — it scored
    # zero paper bets (no BNB-trained model slice). The book RECORDER keeps
    # recording BNB (its coin set is untouched).
    assert env["CRYPTOBOT_COINS"] == "BTC,ETH"
    # The DRY-modes artifact copy (the live artifact seeds wallets LIVE).
    assert env["CRYPTOBOT_V6_CONFIG"].endswith("v2_step1last_predictfun.json")
    # THE sizing knob: 1% of equity per bet on BOTH intervals (operator order
    # 2026-08-02, superseding the 08-01 2%/2%), same pin as the live branch so
    # paper calibrates the stakes live would actually place.
    # sizing_v2.base_fraction reads these from the env at CALL time.
    assert env["CRYPTOBOT_V2_BASE_FRAC_5M"] == "0.010"
    assert env["CRYPTOBOT_V2_BASE_FRAC_15M"] == "0.010"
    # CRYPTOBOT_BET_SIZE_MULTIPLIER is REMOVED (2026-08-01) and must not come
    # back: it was INERT on the sub-minute path — pipeline's v2 sizing
    # overwrites portfolio_loader's target with sizing_v2's equity fraction —
    # so it never moved a real stake and only made the dashboard's sizing panel
    # misreport it. Its ONLY consumer is portfolio_loader.target_bet_size_for.
    assert "CRYPTOBOT_BET_SIZE_MULTIPLIER" not in env
    assert not re.search(
        r"(set_env_line|ensure_env_line) CRYPTOBOT_BET_SIZE_MULTIPLIER", script
    )
    # PF trades through the FINAL SECONDS (operator order 2026-08-01): the
    # late-window gate is pinned EMPTY here, so the poly-only 4-coin pin in the
    # shared env can never leak onto this venue (this file loads second).
    assert env["CRYPTOBOT_LATE_WINDOW_SKIP"] == ""
    # Loopback + SSH tunnel is the boundary; main.py refuses to boot with an
    # empty CRYPTOBOT_DASHBOARD_PASS unless auth is explicitly disabled.
    assert env["CRYPTOBOT_DASHBOARD_AUTH_DISABLED"] == "1"
    # PF WS book mirror now EXISTS (predictfun/ws_mirror.py) — enabled but
    # SHADOW-FIRST: REST stays authoritative while the comparator logs WS-vs-REST
    # divergence (order_client=None keeps the paper path read-only regardless).
    assert env["CRYPTOBOT_BOOK_WS_ENABLED"] == "1"
    assert env["CRYPTOBOT_BOOK_WS_SHADOW"] == "1"
    # Stop-loss fast cadence 1000ms — decouples the stop pass onto its own
    # scheduler job (pipeline skips the inline ~2s fill-poll stop pass when
    # >0). PF-ONLY: it must land in this override heredoc and NEVER in the
    # shared /etc/cryptobot.env block — a set_env_line/ensure_env_line there
    # would push the 1s cadence onto the live Polymarket unit (unordered).
    assert env["CRYPTOBOT_STOPLOSS_FAST_CADENCE_MS"] == "1000"
    assert not re.search(
        r"(set_env_line|ensure_env_line) CRYPTOBOT_STOPLOSS_FAST_CADENCE_MS",
        script,
    ), "fast-cadence knob leaked into the shared /etc/cryptobot.env block"
    # Venue-min stop-exit floor 0.90 (measured 2026-07-23:
    # create_order_min_order_value_not_met — the id6 silent-400 incident).
    # PF-ONLY, exactly like the fast-cadence pin: it must land in this
    # override heredoc and NEVER in the shared /etc/cryptobot.env block,
    # where it would gate the live Polymarket unit's stop sells (no measured
    # order minimum there).
    assert env["CRYPTOBOT_STOPLOSS_MIN_EXIT_VALUE_USD"] == "0.90"
    assert not re.search(
        r"(set_env_line|ensure_env_line) CRYPTOBOT_STOPLOSS_MIN_EXIT_VALUE_USD",
        script,
    ), "venue-min exit floor leaked into the shared /etc/cryptobot.env block"
    # The restart-gate stake lever is POLICY and must NOT be armed by deploy:
    # commented-out only (operator decides at restart), never an active key
    # in either heredoc and never set_env_line'd into the shared env.
    assert "CRYPTOBOT_MIN_STOPPABLE_STAKE_USD" not in env
    assert "# CRYPTOBOT_MIN_STOPPABLE_STAKE_USD=" in script
    assert not re.search(
        r"(set_env_line|ensure_env_line) CRYPTOBOT_MIN_STOPPABLE_STAKE_USD",
        script,
    ), "min-stoppable stake lever must stay operator-set, not deploy-pinned"
    # Structural LIVE-impossibility + no live-look-alike Telegram alerts.
    assert env["PREDICTFUN_PRIVATE_KEY"] == ""
    assert env["NOTIFY_TELEGRAM_TOKEN"] == ""

    # bnb.svg is no longer wiped on deploy (the port re-added it) — the rm
    # cleanup block must not name it.
    assert '"$PROJECT_DIR/dashboard/assets/coins/bnb.svg"' not in script

    # The tightened unit sandbox bind-mounts the instance DB DIRECTORIES, so
    # install_remote.sh must pre-create BOTH (systemd can only bind-mount
    # paths that exist at unit start).
    assert (
        'mkdir -p "$PROJECT_DIR/data/predictfun_paper" '
        '"$PROJECT_DIR/data/predictfun_live"' in script
    )


def test_install_remote_live_mode_branch():
    """The REAL-MONEY variant (CRYPTOBOT_PREDICTFUN_LIVE=1, 2026-07-23):
    inherited signing key + telegram, fresh live DB, LIVE artifact, preserved
    accounting cutover, failure paging drop-in — and a clean paper revert."""
    script = _read("install_remote.sh")
    # Branch reads the operator flag from the SHARED env (survives deploys).
    assert re.search(r"grep '\^CRYPTOBOT_PREDICTFUN_LIVE=' /etc/cryptobot\.env", script)
    live, _paper = _pf_env_blocks(script)
    assert live["CRYPTOBOT_VENUE"] == "predictfun"
    assert live["DRY_RUN"] == "false"
    # FRESH live DB — never the paper DB, never the Polymarket DB.
    assert live["CRYPTOBOT_DB_PATH"].endswith(
        "/data/predictfun_live/cryptobot_predictfun_live.db"
    )
    # The LIVE artifact (wallet_default_modes LIVE) — not the DRY copy.
    assert live["CRYPTOBOT_V6_CONFIG"].endswith("v2_step1last_live.json")
    assert live["CRYPTOBOT_COINS"] == "BTC,ETH"
    assert live["CRYPTOBOT_DASHBOARD_PORT"] == "8801"
    # SIZING (operator order 2026-08-02): 1% of equity per bet on BOTH
    # intervals (superseded the 08-01 2%/2%; ~$2 stakes sit below the stop-
    # salvage floor — accepted). This is the REAL knob — sizing_v2.base_fraction
    # reads CRYPTOBOT_V2_BASE_FRAC_{5M,15M} from the env at call time — and it
    # replaced the inert CRYPTOBOT_BET_SIZE_MULTIPLIER (asserted absent in the
    # paper test, which also guards the shared-env block).
    assert live["CRYPTOBOT_V2_BASE_FRAC_5M"] == "0.010"
    assert live["CRYPTOBOT_V2_BASE_FRAC_15M"] == "0.010"
    assert "CRYPTOBOT_BET_SIZE_MULTIPLIER" not in live
    # Both intervals trade — pinned explicitly so the predict.fun unit never
    # depends on the Polymarket interval decision in the shared env.
    assert live["CRYPTOBOT_INTERVAL_5M_ENABLED"] == "1"
    # PF trades through the FINAL SECONDS: no late-window gate, pinned empty so
    # the shared env's armed poly value cannot leak onto this venue.
    assert live["CRYPTOBOT_LATE_WINDOW_SKIP"] == ""
    # LIVE-READINESS UNBLOCK (both lines, or the box never places a live bet):
    # live_readiness evaluates POLYMARKET wallet topology on every venue and
    # fails closed with 'no_private_key_for_required_deposit_wallet' when
    # LIVE_REQUIRE_DEPOSIT_WALLET=true (the cryptobot.env.example default) and
    # no poly key exists — which is BY DESIGN on a predict.fun-only box. Never
    # READY => pipeline skips every LIVE cell.
    assert live["LIVE_REQUIRE_DEPOSIT_WALLET"] == "false"
    assert live["POLY_SIGNATURE_TYPE"] == "0"
    # Loopback + SSH tunnel is the boundary (main.py re-checks the bind).
    assert live["CRYPTOBOT_DASHBOARD_AUTH_DISABLED"] == "1"
    # PF WS book mirror now EXISTS (predictfun/ws_mirror.py) — enabled but
    # SHADOW-FIRST: REST stays authoritative on the live unit too while the
    # comparator logs WS-vs-REST divergence (operator flips SHADOW=0 to go
    # authoritative after a clean divergence run).
    assert live["CRYPTOBOT_BOOK_WS_ENABLED"] == "1"
    assert live["CRYPTOBOT_BOOK_WS_SHADOW"] == "1"
    # Stop-loss fast cadence 1000ms carries on the LIVE unit too (own scheduler
    # job, decoupled from the ~2s inline fill-polls) — same PF-scoped pin as the
    # paper heredoc; the shared-block exclusion is asserted in the paper test.
    assert live["CRYPTOBOT_STOPLOSS_FAST_CADENCE_MS"] == "1000"
    # Venue-min stop-exit floor 0.90 on the LIVE unit — the money unit the
    # 2026-07-23 id6 incident happened on (three sub-min FOK sells 400'd
    # invisibly, position rode to -100%); same PF-scoped pin as the paper
    # heredoc, shared-block exclusion asserted in the paper test. The
    # restart-gate stake lever stays commented-out (asserted there too).
    assert live["CRYPTOBOT_STOPLOSS_MIN_EXIT_VALUE_USD"] == "0.90"
    assert "CRYPTOBOT_MIN_STOPPABLE_STAKE_USD" not in live
    # Bankroll comes from the operator knob with a 100 default.
    assert live["CRYPTOBOT_INITIAL_BANKROLL"] == "$PF_BANKROLL"
    assert 'PF_BANKROLL="${PF_BANKROLL:-100}"' in script
    # Accounting cutover stamped at first arm and PRESERVED across redeploys —
    # in BOTH branches (2026-08-01). Before, only the live heredoc carried it,
    # so a revert-to-paper rewrote the file without the key and the next live
    # arm stamped a NEW anchor over the venue's era.
    assert live["CRYPTOBOT_WALLET_START_AT"] == "$PF_START"
    assert _paper["CRYPTOBOT_WALLET_START_AT"] == "$PF_START"
    assert script.count('PF_START="${PREV_PF_START:-$WALLET_START_NOW}"') == 2
    # THE POINT of live mode: key + predict account + telegram are inherited
    # from the shared env — the live heredoc must NOT override them.
    assert "PREDICTFUN_PRIVATE_KEY" not in live
    assert "PREDICTFUN_PREDICT_ACCOUNT" not in live
    assert "NOTIFY_TELEGRAM_TOKEN" not in live
    # Real-money unit pages on failure via the drop-in; paper revert removes it.
    assert "cryptobot-predictfun.service.d/live.conf" in script
    assert "OnFailure=cryptobot-failure-alert.service" in script
    assert re.search(
        r"rm -f /etc/systemd/system/cryptobot-predictfun\.service\.d/live\.conf",
        script,
    )


def test_deploy_sh_stages_unit_artifact_and_preserves_paper_db():
    script = _read("deploy.sh")
    # Unit staged into deploy/.
    assert "cryptobot-predictfun.service" in script
    # The staged jsx bundle matches index.html's module set exactly —
    # pages-calibration was removed with the Calibration tab (2026-07-21), so
    # staging it again would break the cp loop on the missing file.
    m = re.search(r"^for jsx in (.+); do$", script, flags=re.MULTILINE)
    assert m is not None
    staged = m.group(1).split()
    assert "pages-calibration" not in staged
    index = (_CODE_ROOT / "polymarket" / "dashboard" / "index.html").read_text(encoding="utf-8")
    loaded = re.findall(r'src="([a-z0-9-]+)\.jsx\?v=', index)
    assert sorted(staged) == sorted(set(loaded))
    # Prune-exclusion (b70a95f lesson): the paper ledger dir survives every
    # deploy — AND the REAL-MONEY live DB dir (2026-07-23: a missing
    # predictfun_live exclusion wiped the live DB + stopped the bot mid-session).
    assert '! -name "predictfun_paper"' in script
    assert '! -name "predictfun_live"' in script
    # The generated artifact is boot-critical for the paper unit.
    assert 'test -f "$tmp_extract/runtime/config/v2_step1last_predictfun.json"' in script


def test_deploy_sh_dry_artifact_generator_rewrites_all_modes():
    """Run the exact generator heredoc shipped in deploy.sh against the real
    live artifact: every wallet_default_modes value becomes DRY and NOTHING
    else changes (the parallel lane's amendments propagate automatically)."""
    script = _read("deploy.sh")
    heredocs = re.findall(r"<<'PY'\n(.*?)\nPY\n", script, flags=re.DOTALL)
    gen = [h for h in heredocs if "wallet_default_modes" in h]
    assert len(gen) == 1, "deploy.sh must carry exactly one DRY-artifact generator"

    src = Path(config.V6_CONFIG_PATH)
    if not src.exists():
        import pytest

        pytest.skip("live artifact not present in this checkout")
    with tempfile.TemporaryDirectory(prefix="cryptobot-dry-artifact-") as tmp:
        dst = Path(tmp) / "v2_step1last_predictfun.test.json"
        proc = subprocess.run(
            [sys.executable, "-", str(src), str(dst)],
            input=gen[0].encode("utf-8"),
            capture_output=True,
        )
        assert proc.returncode == 0, proc.stderr
        live_cfg = json.loads(src.read_text(encoding="utf-8"))
        pf_cfg = json.loads(dst.read_text(encoding="utf-8"))

    assert set(pf_cfg["wallet_default_modes"]) == set(live_cfg["wallet_default_modes"])
    assert all(v == "DRY" for v in pf_cfg["wallet_default_modes"].values())
    # Everything except the modes is byte-equal (drift-check trio inherited).
    live_cfg.pop("wallet_default_modes")
    pf_cfg.pop("wallet_default_modes")
    assert pf_cfg == live_cfg


def test_env_example_documents_flag_and_keeps_key_empty():
    example = _read("cryptobot.env.example")
    # Documented, default OFF (commented out).
    assert "# CRYPTOBOT_PREDICTFUN_DRY_ENABLED=1" in example
    assert re.search(
        r"^CRYPTOBOT_PREDICTFUN_DRY_ENABLED=", example, flags=re.MULTILINE
    ) is None
    # Live-mode flags documented, default OFF (commented out), with the
    # validation-CLI prerequisite named.
    assert "# CRYPTOBOT_PREDICTFUN_LIVE=1" in example
    assert re.search(
        r"^CRYPTOBOT_PREDICTFUN_LIVE=", example, flags=re.MULTILINE
    ) is None
    assert "# CRYPTOBOT_PREDICTFUN_LIVE_BANKROLL=" in example
    assert "cbot-predictfun-live-validation" in example
    assert "# PREDICTFUN_PREDICT_ACCOUNT=" in example
    # The API key stays EMPTY in every git-tracked file.
    assert re.search(r"^#? ?PREDICTFUN_API_KEY=\s*$", example, flags=re.MULTILINE)
    assert re.search(r"^#? ?PREDICTFUN_API_KEY=\S", example, flags=re.MULTILINE) is None


def test_main_venue_gates_book_ws_mirrors_per_venue():
    """Each venue builds its OWN mirror class; neither leaks into the other.

    Phase 2 moved the construction out of main.py's `if config.VENUE ==
    "predictfun"` branch and into each adapter's build_ws_mirror, so the
    invariant is now asserted where it lives — against the two venue packages
    plus main.py's single adapter call — instead of against a source branch
    that no longer exists.
    """
    main_src = (_CODE_ROOT / "polymarket" / "src" / "cryptobot" / "main.py").read_text(
        encoding="utf-8"
    )
    pf_src = (
        _CODE_ROOT / "predictfun" / "live" / "src" / "cryptobot_predictfun"
        / "__init__.py"
    ).read_text(encoding="utf-8")
    poly_src = (
        _CODE_ROOT / "polymarket" / "src" / "cryptobot_polymarket"
        / "__init__.py"
    ).read_text(encoding="utf-8")

    # main.py no longer knows either mirror class; it asks the adapter once.
    ws_block = main_src.split("if config.BOOK_WS_ENABLED:", 1)[1]
    ws_block = ws_block.split("if config.BOOK_PREFETCH_ENABLED", 1)[0]
    assert "build_ws_mirror(" in ws_block
    # It never IMPORTS either mirror class any more (the class names still
    # appear in main.py's explanatory comment, which is fine — what matters is
    # that no venue module is reached).
    assert "import PredictFunBookWsMirror" not in main_src
    assert "import ClobBookWsMirror" not in main_src
    assert "cryptobot_predictfun" not in ws_block
    assert "cryptobot_polymarket" not in ws_block

    # predictfun adapter builds the PF mirror (own module), NOT the Polymarket
    # one, and honors shadow vs authoritative.
    assert "PredictFunBookWsMirror" in pf_src
    assert "ClobBookWsMirror" not in pf_src
    assert "config.BOOK_WS_SHADOW" in pf_src
    # WS authoritative => it replaces the A8 REST prefetcher's off-tick role.
    assert "replaces_rest_prefetch=True" in pf_src

    # Polymarket adapter still builds the Polymarket mirror, and never claims
    # the prefetch-replacement (its mirror does not warm books off-tick).
    assert "ClobBookWsMirror" in poly_src
    assert "PredictFunBookWsMirror" not in poly_src
    assert "replaces_rest_prefetch=True" not in poly_src

    # The prefetch skip is now venue-declared rather than PF-named in main.
    assert (
        "if config.BOOK_PREFETCH_ENABLED and ws_replaces_rest_prefetch:" in main_src
    )
    # The A8 prefetcher uses the ACTIVE venue's batch fetch + its own kwargs.
    assert "_bp_adapter.fetch_books_batch" in main_src
    assert "_bp_adapter.prefetch_fetch_kwargs()" in main_src
    # ...and only predict.fun supplies a prefetch bucket.
    assert "prefetch_bucket" in pf_src
