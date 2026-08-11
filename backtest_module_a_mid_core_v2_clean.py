"""
ModuleA Mid Core V2 runner (clean final)
========================================

依赖：
- ``GateMoudleStrategy_V09.py``
- ``GateMoudleStrategy_ModuleA_MidCoreV2_clean.py``
- ``module_a_v2_score_calibration.json``（用于 D6/D8/VETO20 frozen overlay）

当前用途：
- 统一回放 2024 IF 主力月度分段。
- 统一跑 6 条已冻结 profile：broad NONE/D6/D8 + strong NONE/VETO20/D8。
- 输出 event/enriched/candidate/signal_trace/trade/monitor/summary/report。

工程踩坑提醒：
- ``STRATEGY_PATH`` 与真实策略文件名必须同步；否则会出现 12 段全失败。
- 输出目录建议使用独立 clean 目录，避免旧结果残留污染判断。
- 结果核查时，summary 与 monitor 必须同时看；只看 summary 容易误判链路是否真的对齐。
- profile 对照必须包含 strong NONE，否则 strong veto20 / D8 的层次是“不完整对照”。
"""

from __future__ import annotations

import contextlib
import gc
import json
import logging
import os
import sys
import traceback
import time
import types
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy_ctastrategy.base import BacktestingMode
from vnpy.trader.constant import Interval


N_JOBS = 8
CAPITAL = 10_000_000
OUT_ROOT = "./BT_OUT/ModuleA_mid_core_v2_clean_2024_main"
STRATEGY_PATH = "GateMoudleStrategy_ModuleA_MidCoreV2_clean.py"
CLASS_NAME = "GateMoudleStrategy_ModuleA_MidCoreV2"
CALIBRATION_JSON_PATH = "./BT_OUT/V2_PHASE1_CALIB/module_a_v2_score_calibration.json"

VT_RATE = 0.0003
SLIPPAGE = 0.2
SIZE = 300
PRICETICK = 0.2
FIXED_SIZE = 1
TICK_VALUE = SIZE * PRICETICK

EXIT_HALF_SPREAD_MULT = 0.5
EXIT_HALF_SPREAD_MIN_TICKS = 0.5
EXTRA_EXIT_SLIPPAGE_TICKS = 0.0
EXTRA_FEE_TICKS_RT = 0.0

SEGMENTS: List[Tuple[str, str, str, str]] = [
    ("2024-01", "IF2401.CFFEX", "2024-01-02", "2024-01-31"),
    ("2024-02", "IF2402.CFFEX", "2024-02-01", "2024-02-29"),
    ("2024-03", "IF2403.CFFEX", "2024-03-01", "2024-03-28"),
    ("2024-04", "IF2404.CFFEX", "2024-04-01", "2024-04-30"),
    ("2024-05", "IF2405.CFFEX", "2024-05-06", "2024-05-31"),
    ("2024-06", "IF2406.CFFEX", "2024-06-03", "2024-06-28"),
    ("2024-07", "IF2407.CFFEX", "2024-07-01", "2024-07-31"),
    ("2024-08", "IF2408.CFFEX", "2024-08-01", "2024-08-30"),
    ("2024-09", "IF2409.CFFEX", "2024-09-02", "2024-09-30"),
    ("2024-10", "IF2410.CFFEX", "2024-10-08", "2024-10-31"),
    ("2024-11", "IF2411.CFFEX", "2024-11-01", "2024-11-29"),
    ("2024-12", "IF2412.CFFEX", "2024-12-02", "2024-12-31"),
]

BASE_PRESET: Dict[str, Any] = {
    "range_keep_ratio": 0.51,
    "vol_keep_ratio": 0.58,
    "gate_off_confirm_scale": 0.60,
    "module_impulse_s": 2,
    "module_breakout_s": 4,
    "module_move_thresh_ticks": 3.0,
    "module_retrace_max": 0.35,
    "module_flow_s": 2,
    "module_micro_s": 2,
}

# 统一 runner 当前对照集：broad 主线 + broad D8 + strong 主/参考线。
PROFILES: Dict[str, Dict[str, Any]] = {
    "GFIT_R33_V700__NONE": {
        **BASE_PRESET,
        "gate_preset_name": "GFIT_R33_V700",
        "range_on_30s_ticks": 33.0,
        "vol_on_30s": 700.0,
        "v2_enable_score_overlay": 0,
        "v2_overlay_mode": "NONE",
        "family_tag": "broad",
        "candidate_role": "broad_baseline_sanity",
        "run_mode": "DIRECT_PARITY",
    },
    "GFIT_R33_V700__ONLY_D6_PLUS": {
        **BASE_PRESET,
        "gate_preset_name": "GFIT_R33_V700",
        "range_on_30s_ticks": 33.0,
        "vol_on_30s": 700.0,
        "v2_enable_score_overlay": 1,
        "v2_overlay_mode": "ONLY_D6_PLUS",
        "family_tag": "broad",
        "candidate_role": "broad_main",
        "run_mode": "DIRECT_PARITY",
    },
    "GFIT_R33_V700__ONLY_D8_PLUS": {
        **BASE_PRESET,
        "gate_preset_name": "GFIT_R33_V700",
        "range_on_30s_ticks": 33.0,
        "vol_on_30s": 700.0,
        "v2_enable_score_overlay": 1,
        "v2_overlay_mode": "ONLY_D8_PLUS",
        "family_tag": "broad",
        "candidate_role": "broad_high_quality",
        "run_mode": "DIRECT_PARITY",
    },
    "GFIT_R29_V690__NONE": {
        **BASE_PRESET,
        "gate_preset_name": "GFIT_R29_V690",
        "range_on_30s_ticks": 29.0,
        "vol_on_30s": 690.0,
        "v2_enable_score_overlay": 0,
        "v2_overlay_mode": "NONE",
        "family_tag": "strong",
        "candidate_role": "strong_baseline",
        "run_mode": "DIRECT_PARITY",
    },
    "GFIT_R29_V690__VETO_BOTTOM_20PCT": {
        **BASE_PRESET,
        "gate_preset_name": "GFIT_R29_V690",
        "range_on_30s_ticks": 29.0,
        "vol_on_30s": 690.0,
        "v2_enable_score_overlay": 1,
        "v2_overlay_mode": "VETO_BOTTOM_20PCT",
        "family_tag": "strong",
        "candidate_role": "strong_quality_veto20",
        "run_mode": "DIRECT_PARITY",
    },
    "GFIT_R29_V690__ONLY_D8_PLUS": {
        **BASE_PRESET,
        "gate_preset_name": "GFIT_R29_V690",
        "range_on_30s_ticks": 29.0,
        "vol_on_30s": 690.0,
        "v2_enable_score_overlay": 1,
        "v2_overlay_mode": "ONLY_D8_PLUS",
        "family_tag": "strong",
        "candidate_role": "strong_high_quality",
        "run_mode": "DIRECT_PARITY",
    },
}

HORIZONS = "1,2,3,5,8,13,21,30,38,45"
BOOK_DEPTH_WEIGHTS = "1.0,0.8,0.6,0.4,0.2"
LOW_TRADE_THRESHOLD = 10
RUNNER_BUILD = "MODULEA_MID_CORE_V2_CLEAN_20260327"


def _script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _walk_find_file(root: str, basename: str, max_depth: int = 4) -> List[str]:
    hits: List[str] = []
    root = os.path.abspath(root)
    if not os.path.isdir(root):
        return hits
    base_depth = root.rstrip(os.sep).count(os.sep)
    for dirpath, dirnames, filenames in os.walk(root):
        depth = dirpath.rstrip(os.sep).count(os.sep) - base_depth
        if depth > max_depth:
            dirnames[:] = []
            continue
        if basename in filenames:
            hits.append(os.path.join(dirpath, basename))
    return hits


# PITFALL:
# 过去多次出现 runner 指向旧文件名/旧路径，导致 12 段全部失败。
# 所以这里保留环境变量覆盖 + 多级搜索，并在 manifest/selfcheck 中回写 resolved path。
def _resolve_existing_path(path_str: str, env_name: str = "") -> Tuple[str, List[str]]:
    if env_name:
        env_override = os.environ.get(env_name, "").strip()
        if env_override:
            path_str = env_override
    basename = os.path.basename(path_str)
    script_dir = _script_dir()
    parent1 = os.path.dirname(script_dir)
    parent2 = os.path.dirname(parent1)
    cwd = os.getcwd()

    candidates: List[str] = []
    search_roots: List[str] = [cwd, script_dir, parent1, parent2]
    if os.path.isabs(path_str):
        candidates.append(path_str)
    else:
        for root in search_roots:
            candidates.append(os.path.abspath(os.path.join(root, path_str)))
            candidates.append(os.path.abspath(os.path.join(root, basename)))

    seen = set()
    uniq: List[str] = []
    for c in candidates:
        if c and c not in seen:
            uniq.append(c)
            seen.add(c)
    for c in uniq:
        if os.path.exists(c):
            return c, uniq

    for root in search_roots:
        for c in _walk_find_file(root, basename, max_depth=4):
            if c not in seen:
                uniq.append(c)
                seen.add(c)
            if os.path.exists(c):
                return c, uniq
    return (uniq[0] if uniq else path_str), uniq


def _ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def _quiet_vnpy() -> None:
    logging.getLogger().setLevel(logging.CRITICAL)
    for name in ["vnpy", "vnpy_ctastrategy", "vnpy.trader", "vnpy.event", "concurrent", "asyncio"]:
        logging.getLogger(name).setLevel(logging.CRITICAL)


def load_strategy_class(strategy_path: str, class_name: str):
    resolved, tried = _resolve_existing_path(strategy_path, env_name="MODULEA_UNIFIED_STRATEGY_PATH")
    if not resolved or not os.path.isfile(resolved):
        raise RuntimeError(
            "Cannot find strategy file. "
            f"strategy_path={strategy_path!r}. Tried={tried!r}. "
            "也可以设置 MODULEA_UNIFIED_STRATEGY_PATH 为绝对路径。"
        )
    module_name = f"moduleA_unified_{os.getpid()}_{int(time.time()*1000)}"
    import importlib.util
    spec = importlib.util.spec_from_file_location(module_name, resolved)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import strategy file: {resolved}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    cls = getattr(module, class_name)
    return cls, resolved


def _safe_mean(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna()
    return float(x.mean()) if len(x) else float("nan")


def _safe_median(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna()
    return float(x.median()) if len(x) else float("nan")


def _safe_prob_gt(s: pd.Series, threshold: float) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna()
    return float((x > threshold).mean()) if len(x) else float("nan")


def _read_csv_rows(path: str) -> int:
    if not path or not os.path.exists(path):
        return 0
    try:
        df = pd.read_csv(path)
        return int(len(df))
    except Exception:
        return 0


def _read_json(path: str) -> Optional[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


@dataclass
class SegmentResult:
    segment: str
    ok: bool
    err_count: int
    err_msg: str
    task_dirs: List[str]
    task_debug: List[str]
    tracebacks: List[str]


def _run_one_task(segment: str, vt_symbol: str, start_s: str, end_s: str,
                  profile_name: str, profile: Dict[str, Any], out_dir: str,
                  calibration_json_abs: str) -> Dict[str, Any]:
    strategy_cls, resolved_strategy_path = load_strategy_class(STRATEGY_PATH, CLASS_NAME)
    _ensure_dir(out_dir)

    event_csv = os.path.abspath(os.path.join(out_dir, "event_records.csv"))
    event_enriched_csv = os.path.abspath(os.path.join(out_dir, "event_records_enriched.csv"))
    candidate_csv = os.path.abspath(os.path.join(out_dir, "candidate_records.csv"))
    signal_trace_csv = os.path.abspath(os.path.join(out_dir, "signal_trace.csv"))
    meta_json = os.path.abspath(os.path.join(out_dir, "event_meta.json"))
    trade_csv = os.path.abspath(os.path.join(out_dir, "strategy_trade_log.csv"))
    shadow_trade_csv = os.path.abspath(os.path.join(out_dir, "shadow_fast_trade_log.csv"))
    monitor_json = os.path.abspath(os.path.join(out_dir, "unified_monitor.json"))
    gate_summary_csv = os.path.abspath(os.path.join(out_dir, "gate_summary.csv"))
    gate_probe_json = os.path.abspath(os.path.join(out_dir, "gate_probe.json"))
    gate_bucket_csv = os.path.abspath(os.path.join(out_dir, "gate_bucket.csv"))
    gate_diag_csv = os.path.abspath(os.path.join(out_dir, "gate_diag.csv"))
    gate_opp_csv = os.path.abspath(os.path.join(out_dir, "gate_opp.csv"))

    setting: Dict[str, Any] = dict(profile)
    setting.update({
        "gate_diag_only": 0,
        "gate_diag_enable": 0,
        "opp_enable": 0,
        "save_keep_diag": 0,
        "segment_label": segment,
        "event_horizons_s": HORIZONS,
        "study_max_path_s": 45,
        "book_depth_weights": BOOK_DEPTH_WEIGHTS,
        "exit_time_holds_s": "21,30",
        "exit_two_stage_min_holds_s": "8,13",
        "exit_two_stage_hard_cap_s": 30,
        "exit_two_stage_giveback_ticks": "2,3",
        "exit_two_stage_reversal_ticks": "2,3",
        "exit_two_stage_stall_windows_s": "4,5",
        "output_event_csv": event_csv,
        "output_event_enriched_csv": event_enriched_csv,
        "output_candidate_csv": candidate_csv,
        "output_signal_trace_csv": signal_trace_csv,
        "output_trade_csv": trade_csv,
        "output_shadow_trade_csv": shadow_trade_csv,
        "output_meta_json": meta_json,
        "output_unified_monitor_json": monitor_json,
        "gate_summary_filename": gate_summary_csv,
        "gate_probe_filename": gate_probe_json,
        "gate_bucket_filename": gate_bucket_csv,
        "gate_diag_filename": gate_diag_csv,
        "opp_filename": gate_opp_csv,
        "spread_max_ticks": 4.0,
        "depth_min": 12.0,
        "exec_rearm_s": 0,
        "exec_dynamic_enable": 0,
        "research_enable_postprocess": 1,
        "research_emit_shadow_fast": 0,
        "research_exit_half_spread_mult": EXIT_HALF_SPREAD_MULT,
        "research_exit_half_spread_min_ticks": EXIT_HALF_SPREAD_MIN_TICKS,
        "research_extra_exit_slippage_ticks": EXTRA_EXIT_SLIPPAGE_TICKS,
        "research_extra_fee_ticks_rt": EXTRA_FEE_TICKS_RT,
        "research_fixed_size": FIXED_SIZE,
        "research_tick_value": TICK_VALUE,
        "v2_calibration_json_path": calibration_json_abs,
        "v2_profile_name": profile_name,
        "v2_cohort_col": "cohort_BASE_ALL_VETO_BAD",
        "v2_exit_rule": "MH8_STALLW4_CAP30",
        "v2_hard_stop_ticks": 16.0,
        "v2_cooldown_s": 5,
    })

    start = datetime.fromisoformat(start_s)
    end = datetime.fromisoformat(end_s)
    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol=vt_symbol,
        interval=Interval.TICK,
        start=start,
        end=end,
        rate=VT_RATE,
        slippage=SLIPPAGE,
        size=SIZE,
        pricetick=PRICETICK,
        capital=CAPITAL,
        mode=BacktestingMode.TICK,
    )
    engine.add_strategy(strategy_cls, setting)
    with contextlib.redirect_stdout(open(os.devnull, "w", encoding="utf-8")), \
         contextlib.redirect_stderr(open(os.devnull, "w", encoding="utf-8")):
        engine.load_data()
        engine.run_backtesting()
        with contextlib.suppress(Exception):
            engine.calculate_result()
        with contextlib.suppress(Exception):
            engine.calculate_statistics(output=False)

    monitor = _read_json(monitor_json) or {}
    return {
        "resolved_strategy_path": resolved_strategy_path,
        "event_rows": _read_csv_rows(event_csv),
        "event_enriched_rows": _read_csv_rows(event_enriched_csv),
        "candidate_rows": _read_csv_rows(candidate_csv),
        "signal_trace_rows": _read_csv_rows(signal_trace_csv),
        "trade_rows": _read_csv_rows(trade_csv),
        "shadow_rows": _read_csv_rows(shadow_trade_csv),
        "event_total_count": monitor.get("event_rows"),
        "selected_rows": monitor.get("selected_rows"),
        "coverage_ratio": monitor.get("coverage_ratio"),
        "trade_count_monitor": monitor.get("trade_rows"),
        "monitor_json": monitor_json,
    }


def _run_one_segment(segment: str, vt_symbol: str, start_s: str, end_s: str, out_root: str,
                     calibration_json_abs: str) -> SegmentResult:
    task_dirs: List[str] = []
    task_debug: List[str] = []
    tracebacks: List[str] = []
    ok = True
    err_count = 0
    err_msg = ""

    for profile_name, profile in PROFILES.items():
        task_dir = _ensure_dir(os.path.join(out_root, "segments", segment, profile_name))
        task_dirs.append(task_dir)
        try:
            res = _run_one_task(segment, vt_symbol, start_s, end_s, profile_name, profile, task_dir, calibration_json_abs)
            task_debug.append(
                f"{profile_name}: events={res.get('event_rows')} enriched={res.get('event_enriched_rows')} "
                f"candidates={res.get('candidate_rows')} selected={res.get('selected_rows')} "
                f"trades={res.get('trade_rows')} strategy={os.path.basename(str(res.get('resolved_strategy_path', '')))}"
            )
        except Exception as e:
            ok = False
            err_count += 1
            err_msg = str(e)
            tb = traceback.format_exc()
            tracebacks.append(tb)
            with open(os.path.join(task_dir, "task_exception.txt"), "w", encoding="utf-8") as f:
                f.write(tb)
            task_debug.append(f"{profile_name}: ERROR {e}")

    return SegmentResult(segment=segment, ok=ok, err_count=err_count, err_msg=err_msg,
                         task_dirs=task_dirs, task_debug=task_debug, tracebacks=tracebacks)


def _aggregate_named_csv(out_root: str, filename: str) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for segment, *_ in SEGMENTS:
        for profile_name in PROFILES.keys():
            p = os.path.join(out_root, "segments", segment, profile_name, filename)
            if os.path.exists(p):
                try:
                    df = pd.read_csv(p)
                    if not df.empty:
                        frames.append(df)
                except Exception:
                    continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _aggregate_monitor_json(out_root: str) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for segment, *_ in SEGMENTS:
        for profile_name, profile in PROFILES.items():
            p = os.path.join(out_root, "segments", segment, profile_name, "unified_monitor.json")
            payload = _read_json(p)
            if not payload:
                continue
            payload = dict(payload)
            payload["segment"] = segment
            payload["family_tag"] = profile.get("family_tag")
            payload["candidate_role"] = profile.get("candidate_role")
            rows.append(payload)
    return pd.DataFrame(rows)


def _max_drawdown_ticks(net_ticks: pd.Series) -> float:
    if net_ticks is None or len(net_ticks) == 0:
        return 0.0
    x = pd.to_numeric(net_ticks, errors="coerce").fillna(0.0)
    equity = x.cumsum()
    peak = equity.cummax()
    dd = equity - peak
    return float(dd.min()) if len(dd) else 0.0


def _build_strategy_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    out = trades.copy()
    out["dt0"] = pd.to_datetime(out["dt0"], errors="coerce")
    out["month"] = out["dt0"].dt.to_period("M").astype(str)
    for strategy_name, g in out.groupby("strategy_name", dropna=False):
        month_pnl = g.groupby("month", dropna=False)["net_ret_ticks"].sum().reset_index(name="month_ticks")
        sample = g.iloc[0]
        rows.append({
            "strategy_name": strategy_name,
            "gate_preset": sample.get("gate_preset"),
            "v2_overlay_mode": sample.get("v2_overlay_mode"),
            "cohort_col": sample.get("cohort_col"),
            "exit_rule": sample.get("exit_rule"),
            "trade_count": int(len(g)),
            "total_net_ticks": float(g["net_ret_ticks"].sum()),
            "total_pnl_cny": float(g["pnl_cny"].sum()),
            "mean_net_ticks": _safe_mean(g["net_ret_ticks"]),
            "median_net_ticks": _safe_median(g["net_ret_ticks"]),
            "win_rate": _safe_prob_gt(g["net_ret_ticks"], 0.0),
            "mean_hold_s": _safe_mean(g["hold_s"]),
            "stop_hit_rate": _safe_mean(g["stop_hit"]),
            "mean_mfe_ticks": _safe_mean(g["mfe_ticks"]),
            "mean_mae_ticks": _safe_mean(g["mae_ticks"]),
            "capture_vs_mfe_realized": (
                float(g["net_ret_ticks"].mean()) / float(g["mfe_ticks"].mean())
                if len(g["mfe_ticks"].dropna()) and abs(float(g["mfe_ticks"].mean())) > 1e-12 else float("nan")
            ),
            "max_drawdown_ticks": _max_drawdown_ticks(g["net_ret_ticks"]),
            "months_pos": int((month_pnl["month_ticks"] > 0).sum()),
            "months_neg": int((month_pnl["month_ticks"] < 0).sum()),
        })
    return pd.DataFrame(rows).sort_values(["total_net_ticks", "mean_net_ticks"], ascending=[False, False]).reset_index(drop=True)


def _build_monitor_summary(mon_df: pd.DataFrame) -> pd.DataFrame:
    if mon_df.empty:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for profile_name, g in mon_df.groupby("profile_name", dropna=False):
        rows.append({
            "profile_name": profile_name,
            "gate_preset": g["gate_preset"].iloc[0],
            "overlay_mode": g["overlay_mode"].iloc[0],
            "family_tag": g["family_tag"].iloc[0],
            "candidate_role": g["candidate_role"].iloc[0],
            "run_mode": g["run_mode"].iloc[0],
            "segments": int(g["segment"].astype(str).nunique()),
            "event_rows": int(pd.to_numeric(g["event_rows"], errors="coerce").fillna(0).sum()),
            "candidate_rows": int(pd.to_numeric(g["candidate_rows"], errors="coerce").fillna(0).sum()),
            "selected_rows": int(pd.to_numeric(g["selected_rows"], errors="coerce").fillna(0).sum()),
            "coverage_ratio_mean": _safe_mean(g["coverage_ratio"]),
            "trade_rows": int(pd.to_numeric(g["trade_rows"], errors="coerce").fillna(0).sum()),
            "total_net_ticks_monitor": float(pd.to_numeric(g["total_net_ticks"], errors="coerce").fillna(0.0).sum()),
            "mean_net_ticks_monitor": _safe_mean(g["mean_net_ticks"]),
            "win_rate_monitor": _safe_mean(g["win_rate"]),
        })
    return pd.DataFrame(rows).sort_values(["total_net_ticks_monitor", "mean_net_ticks_monitor"], ascending=[False, False]).reset_index(drop=True)


def _write_report(report_path: str, summary_df: pd.DataFrame, monitor_df: pd.DataFrame, manifest_df: pd.DataFrame) -> None:
    lines: List[str] = []
    lines.append("ModuleA_mid_core_v2_clean")
    lines.append("=" * 72)
    lines.append("")
    lines.append("Unified core：ModuleA 候选生成 + overlay + 执行后处理合一。")
    lines.append("当前 profile 集：broad NONE/D6/D8 + strong NONE/VETO20/D8。")
    lines.append("")
    if not summary_df.empty:
        lines.append("Strategy summary:")
        cols = [
            "strategy_name", "gate_preset", "v2_overlay_mode", "trade_count",
            "total_net_ticks", "mean_net_ticks", "win_rate", "mean_hold_s",
            "max_drawdown_ticks", "months_pos", "months_neg",
        ]
        lines.append(summary_df[cols].to_string(index=False))
        lines.append("")
    if not monitor_df.empty:
        lines.append("Unified monitor summary:")
        cols = [
            "profile_name", "gate_preset", "overlay_mode", "event_rows", "candidate_rows",
            "selected_rows", "coverage_ratio_mean", "trade_rows", "total_net_ticks_monitor",
        ]
        lines.append(monitor_df[cols].to_string(index=False))
        lines.append("")
    if not manifest_df.empty:
        bad = manifest_df.loc[~manifest_df["ok"].fillna(False)]
        lines.append(f"segments_ok={int(manifest_df['ok'].fillna(False).sum())}/{len(manifest_df)}")
        if not bad.empty:
            lines.append("segment_errors:")
            lines.extend(bad[["segment", "err_msg"]].astype(str).apply(lambda r: f"- {r['segment']}: {r['err_msg']}", axis=1).tolist())
        lines.append("")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main() -> None:
    _quiet_vnpy()
    out_root = _ensure_dir(OUT_ROOT)
    calibration_json_abs, calibration_tried = _resolve_existing_path(CALIBRATION_JSON_PATH, env_name="MODULEA_CALIBRATION_JSON")

    runner_selfcheck = {
        "runner_build": RUNNER_BUILD,
        "strategy_path": STRATEGY_PATH,
        "resolved_strategy_path": _resolve_existing_path(STRATEGY_PATH, env_name="MODULEA_UNIFIED_STRATEGY_PATH")[0],
        "calibration_json_input": CALIBRATION_JSON_PATH,
        "resolved_calibration_json": calibration_json_abs,
        "calibration_candidates": calibration_tried,
        "out_root": os.path.abspath(out_root),
        "profiles": list(PROFILES.keys()),
        "segments": SEGMENTS,
    }
    with open(os.path.join(out_root, "runner_selfcheck.json"), "w", encoding="utf-8") as f:
        json.dump(runner_selfcheck, f, ensure_ascii=False, indent=2)

    futures = []
    manifest_rows: List[Dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=N_JOBS) as ex:
        for segment, vt_symbol, start_s, end_s in SEGMENTS:
            futures.append(ex.submit(_run_one_segment, segment, vt_symbol, start_s, end_s, out_root, calibration_json_abs))

        done = 0
        total = len(futures)
        for fu in as_completed(futures):
            done += 1
            res = fu.result()
            manifest_rows.append({
                "segment": res.segment,
                "ok": bool(res.ok),
                "err_count": int(res.err_count),
                "err_msg": str(res.err_msg),
                "task_dirs": " | ".join(res.task_dirs),
                "task_debug": " || ".join(res.task_debug),
                "tracebacks": "\n\n".join(res.tracebacks),
            })
            print(f"[{done:02d}/{total:02d}] {res.segment} ok={int(res.ok)}")
            gc.collect()

    manifest_df = pd.DataFrame(manifest_rows).sort_values("segment").reset_index(drop=True)
    manifest_df.to_csv(os.path.join(out_root, "run_manifest.csv"), index=False, encoding="utf-8-sig")

    event_df = _aggregate_named_csv(out_root, "event_records.csv")
    enriched_df = _aggregate_named_csv(out_root, "event_records_enriched.csv")
    candidate_df = _aggregate_named_csv(out_root, "candidate_records.csv")
    signal_trace_df = _aggregate_named_csv(out_root, "signal_trace.csv")
    trade_df = _aggregate_named_csv(out_root, "strategy_trade_log.csv")
    shadow_df = _aggregate_named_csv(out_root, "shadow_fast_trade_log.csv")
    monitor_seg_df = _aggregate_monitor_json(out_root)

    event_df.to_csv(os.path.join(out_root, "a_mid_core_v2_event_records.csv"), index=False, encoding="utf-8-sig")
    enriched_df.to_csv(os.path.join(out_root, "a_mid_core_v2_event_enriched.csv"), index=False, encoding="utf-8-sig")
    candidate_df.to_csv(os.path.join(out_root, "a_mid_core_v2_candidate_records.csv"), index=False, encoding="utf-8-sig")
    signal_trace_df.to_csv(os.path.join(out_root, "a_mid_core_v2_signal_trace.csv"), index=False, encoding="utf-8-sig")
    trade_df.to_csv(os.path.join(out_root, "a_mid_core_v2_trade_log.csv"), index=False, encoding="utf-8-sig")
    shadow_df.to_csv(os.path.join(out_root, "a_mid_core_v2_shadow_trade_log.csv"), index=False, encoding="utf-8-sig")
    monitor_seg_df.to_csv(os.path.join(out_root, "a_mid_core_v2_monitor_segment.csv"), index=False, encoding="utf-8-sig")

    summary_df = _build_strategy_summary(trade_df)
    monitor_summary_df = _build_monitor_summary(monitor_seg_df)
    summary_df.to_csv(os.path.join(out_root, "a_mid_core_v2_strategy_summary.csv"), index=False, encoding="utf-8-sig")
    monitor_summary_df.to_csv(os.path.join(out_root, "a_mid_core_v2_monitor_summary.csv"), index=False, encoding="utf-8-sig")
    _write_report(os.path.join(out_root, "a_mid_core_v2_report.txt"), summary_df, monitor_summary_df, manifest_df)


if __name__ == "__main__":
    if os.name == "nt":
        sys.modules["talib"] = types.ModuleType("talib")
    main()
