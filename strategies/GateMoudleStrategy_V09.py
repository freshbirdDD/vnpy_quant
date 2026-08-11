"""
GateModuleStrategy_V09  (vn.py 4.3.x / vnpy_ctastrategy)
=========================================================
目标：实现全新的 Gate 逻辑（GateOn / ExecOK / Panic 三线分权），并加入更易触发的动态 ExecOK 分档；并保留 StepA/StepB 扫描所需的
gate_bucket / gate_opp / gate_diag / gate_keep_diag.json 输出能力。

设计要点（v2）：
1) GateOn（门是否开）
   - 开门：仅由 range_30s_ticks 与 vol_30s 决定（gate_open_mode: AND/OR）
   - 开门后最小持有：按 gate_strength 分段映射到 gate_min_hold_s1/s2/s3
   - 最小持有结束后：由 keep 维持（固定 OR）
       maintain = (range >= range_on*RK) OR (vol >= vol_on*VK)
   - 关门确认：固定基准 + scale
       confirm_s = round(gate_confirm_base_s * gate_off_confirm_scale)
   - 关门冷却：gate_cooldown_s（默认 30s）

2) ExecOK（是否允许开仓/挂单）
   - spread/depth（可选 vol_min_30s）只影响 ExecOK，不再拥有 GateOn 的关门权限
   - ExecOK 发生塌缩后，需 ExecOK 连续 OK 达到 exec_rearm_s（默认 10s）才允许再次开仓

3) Panic（极端环境覆盖）
   - 覆盖整个过程（即使处于 GateOn 最小持有也可触发）
   - 仅包含：数据异常类 + 波动/跳变类（不包含流动性塌缩；塌缩只影响 ExecOK）
   - 两档：L1 关门 + lockout；L2 关门 +（可选）强平 + lockout

备注：
- 本策略默认支持 gate_diag_only=1 的“门控诊断/机会密度”模式（用于 StepA/StepB 扫描）。
- 如需在 V02 中继续接入 ModuleA 下单逻辑，建议在 gate_diag_only=0 分支中按需扩展。
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, time, timedelta, date
from typing import Deque, Dict, List, Optional, Tuple
import csv
import json
import os

from vnpy_ctastrategy import (
    CtaTemplate,
    TickData,
    BarData,
    TradeData,
    OrderData,
    StopOrder,
)


# -----------------------------
# Small utils
# -----------------------------
def _clamp_int(x: int, lo: int, hi: int) -> int:
    return lo if x < lo else hi if x > hi else x


def _safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _clip01(x: float) -> float:
    x = float(x)
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _parse_int_list(s: str) -> List[int]:
    """Parse '15,30,60' -> [15,30,60]."""
    if not s:
        return []
    out: List[int] = []
    for part in str(s).replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(float(part)))
    return out


def _parse_float_list(s: str) -> List[float]:
    """Parse '10,12.5' -> [10.0, 12.5]."""
    if not s:
        return []
    out: List[float] = []
    for part in str(s).replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(float(part))
    return out


def _parse_hms(s: str, default: time) -> time:
    """Parse 'HH:MM:SS' into datetime.time; return default on failure."""
    try:
        parts = [int(x) for x in str(s).strip().split(":")]
        if len(parts) == 2:
            return time(parts[0], parts[1], 0)
        if len(parts) == 3:
            return time(parts[0], parts[1], parts[2])
    except Exception:
        pass
    return default


def _time_to_seconds(t0: time) -> int:
    return int(t0.hour) * 3600 + int(t0.minute) * 60 + int(t0.second)



class _OppWindow:
    """Forward range labeling window for opportunity density."""

    def __init__(
        self,
        ticks: Optional[Deque[Tuple[datetime, float, bool, bool, bool]]] = None,
        maxq: Optional[Deque[Tuple[datetime, float]]] = None,
        minq: Optional[Deque[Tuple[datetime, float]]] = None,
        total: int = 0,
        gate: int = 0,
        gate_exec: int = 0,
        forced: int = 0,
        opp_total: Optional[Dict[float, int]] = None,
        opp_gate: Optional[Dict[float, int]] = None,
        opp_gate_exec: Optional[Dict[float, int]] = None,
        opp_forced: Optional[Dict[float, int]] = None,
    ) -> None:
        self.ticks = ticks if ticks is not None else deque()
        self.maxq = maxq if maxq is not None else deque()
        self.minq = minq if minq is not None else deque()
        self.total = int(total)
        self.gate = int(gate)
        self.gate_exec = int(gate_exec)
        self.forced = int(forced)
        self.opp_total = opp_total if opp_total is not None else {}
        self.opp_gate = opp_gate if opp_gate is not None else {}
        self.opp_gate_exec = opp_gate_exec if opp_gate_exec is not None else {}
        self.opp_forced = opp_forced if opp_forced is not None else {}


class GateModuleStrategy_V09(CtaTemplate):
    """
    GateModuleStrategy_V09

    重点：实现 Gate v2 状态机 + opp/bucket/diag 输出。
    """

    author: str = "DF Quant"

    # =========================
    # Parameters (scan-friendly)
    # =========================
    # base
    pricetick: float = 0.2
    tick_interval_sec: float = 1.0

    # data cleaning
    dv_spike_max: float = 999999.0

    # GateOn thresholds (open)
    range_on_30s_ticks: float = 40.0
    vol_on_30s: float = 660.0
    gate_open_mode: str = "AND"   # AND/OR

    # keep ratios (fixed OR)
    range_keep_ratio: float = 0.63
    vol_keep_ratio: float = 0.66
    gate_hold_mode: str = "OR"    # accepted for compatibility; v2 uses OR by spec

    # strength thresholds (also used for min-hold mapping)
    gate_s_thresh1: float = 1.10
    gate_s_thresh2: float = 1.30

    # GateOn min-hold (NEW)
    gate_min_hold_s1: int = 60
    gate_min_hold_s2: int = 120
    gate_min_hold_s3: int = 150

    # confirm (fixed base + scale)
    gate_confirm_mode: str = "FIXED"   # FIXED/STRENGTH (compat)
    gate_confirm_base_s: int = 30
    gate_off_confirm_scale: float = 1.0
    # strength-mode confirm (compat; used when gate_confirm_mode=STRENGTH)
    gate_off_confirm_s1: int = 60
    gate_off_confirm_s2: int = 180
    gate_off_confirm_s3: int = 300

    # gate close cooldown
    gate_cooldown_s: int = 30

    # ExecOK (entry permission)
    spread_max_ticks: float = 4.0
    depth_min: float = 12.0
    vol_min_30s: float = 15.0
    exec_use_vol_min_30s: int = 0  # 0/1
    exec_bad_debounce_s: int = 1
    exec_rearm_s: int = 0

    # Dynamic ExecOK (根据环境强度动态放宽 spread / 放低 depth)
    exec_dynamic_enable: int = 0
    exec_dynamic_opp_follow_main: int = 1
    exec_dyn_trigger_mode: str = "RATIO_OR"   # SCORE / RATIO_OR
    exec_dyn_score_w_range: float = 0.60
    exec_dyn_score_w_vol: float = 0.40
    exec_dyn_fast_thresh: float = 0.20
    exec_dyn_burst_thresh: float = 0.60
    exec_dyn_fast_range_mult: float = 1.02
    exec_dyn_fast_vol_mult: float = 1.05
    exec_dyn_burst_range_mult: float = 1.08
    exec_dyn_burst_vol_mult: float = 1.15
    exec_dyn_spread_base: float = 2.0
    exec_dyn_spread_fast: float = 4.0
    exec_dyn_spread_burst: float = 4.0
    exec_dyn_depth_base: float = 15.0
    exec_dyn_depth_fast: float = 12.0
    exec_dyn_depth_burst: float = 12.0
    exec_dyn_spread_cap_hard: float = 4.0
    exec_dyn_depth_floor_hard: float = 10.0

    # Session scheduler (intraday only: no carry across lunch/overnight)
    session_enable: int = 1
    session_entry_cutoff_s: int = 30
    session_lunch_flat_time: str = "11:29:55"
    session_eod_flat_time: str = "14:59:55"
    session_am_open_time: str = "09:30:00"
    session_lunch_start_time: str = "11:30:00"
    session_pm_open_time: str = "13:00:00"
    session_close_time: str = "15:00:00"

    # Open force window at session open (baseline 60s gate, then follow keep/confirm)
    open_force_enable: int = 1
    open_force_seconds: int = 60  # recommended
    open_force_minutes: int = 0   # deprecated (kept for compatibility; ignored if open_force_seconds>0)
    open_entry_block_s: int = 3
    open_force_apply_pm: int = 1

    # Gate minute diag & bucket & opp output
    gate_diag_enable: int = 1
    gate_diag_only: int = 1
    gate_diag_filename: str = ""
    gate_bucket_filename: str = ""
    gate_summary_filename: str = ""
    gate_probe_filename: str = ""
    gate_probe_max_samples: int = 50

    opp_enable: int = 1
    opp_filename: str = ""
    opp_horizons_s: str = "15"
    opp_thresholds_ticks: str = "10"
    opp_exec_spread_max_ticks: float = 4.0
    opp_exec_depth_min: float = 12.0

    # keep diag json
    save_keep_diag: int = 1
    gate_diag_max_transitions: int = 0  # 0 => only counters/mins (small)

    # Panic (no liquidity collapse here; only data/jump/range)
    panic_enable: int = 1
    panic_lockout_s_l1: int = 60
    panic_lockout_s_l2: int = 180
    panic_level2_flatten: int = 0  # v2 scan mode often no position
    panic_tick_gap_s: int = 5
    panic_jump_ticks_l1: float = 20.0
    panic_jump_ticks_l2: float = 40.0
    panic_range3_ticks_l1: float = 30.0
    panic_range3_ticks_l2: float = 60.0
    panic_dv_spike_n: int = 5

    parameters: List[str] = [
        # base
        "pricetick", "tick_interval_sec",
        # data cleaning
        "dv_spike_max",
        # gate open/keep
        "range_on_30s_ticks", "vol_on_30s", "gate_open_mode",
        "range_keep_ratio", "vol_keep_ratio", "gate_hold_mode",
        "gate_s_thresh1", "gate_s_thresh2",
        "gate_min_hold_s1", "gate_min_hold_s2", "gate_min_hold_s3",
        "gate_confirm_mode", "gate_confirm_base_s", "gate_off_confirm_scale",
        "gate_off_confirm_s1", "gate_off_confirm_s2", "gate_off_confirm_s3",
        "gate_cooldown_s",
        # exec
        "spread_max_ticks", "depth_min", "vol_min_30s",
        "exec_use_vol_min_30s", "exec_bad_debounce_s", "exec_rearm_s",
        "exec_dynamic_enable", "exec_dynamic_opp_follow_main",
        "exec_dyn_trigger_mode",
        "exec_dyn_score_w_range", "exec_dyn_score_w_vol",
        "exec_dyn_fast_thresh", "exec_dyn_burst_thresh",
        "exec_dyn_fast_range_mult", "exec_dyn_fast_vol_mult",
        "exec_dyn_burst_range_mult", "exec_dyn_burst_vol_mult",
        "exec_dyn_spread_base", "exec_dyn_spread_fast", "exec_dyn_spread_burst",
        "exec_dyn_depth_base", "exec_dyn_depth_fast", "exec_dyn_depth_burst",
        "exec_dyn_spread_cap_hard", "exec_dyn_depth_floor_hard",
        # open force
        # session & open force
        "session_enable", "session_entry_cutoff_s", "session_lunch_flat_time", "session_eod_flat_time",
        "session_am_open_time", "session_lunch_start_time", "session_pm_open_time", "session_close_time",
        "open_force_enable", "open_force_seconds", "open_force_minutes", "open_entry_block_s", "open_force_apply_pm",
        "gate_diag_enable", "gate_diag_only", "gate_diag_filename", "gate_bucket_filename", "gate_summary_filename",
        "gate_probe_filename", "gate_probe_max_samples",
        "opp_enable", "opp_filename", "opp_horizons_s", "opp_thresholds_ticks",
        "opp_exec_spread_max_ticks", "opp_exec_depth_min",
        # keep diag json
        "save_keep_diag", "gate_diag_max_transitions",
        # panic
        "panic_enable", "panic_lockout_s_l1", "panic_lockout_s_l2", "panic_level2_flatten",
        "panic_tick_gap_s", "panic_jump_ticks_l1", "panic_jump_ticks_l2",
        "panic_range3_ticks_l1", "panic_range3_ticks_l2", "panic_dv_spike_n",
    ]

    # =========================
    # Variables (UI visible)
    # =========================
    gate_on: int = 0
    gate_strength: float = 0.0
    range_30s: float = 0.0
    vol_30s: float = 0.0
    exec_ok: int = 0
    entry_allowed: int = 0
    panic_lockout: int = 0

    variables: List[str] = [
        "gate_on", "gate_strength", "range_30s", "vol_30s", "exec_ok", "entry_allowed", "panic_lockout"
    ]

    # =========================
    # Lifecycle
    # =========================
    def __init__(self, cta_engine, strategy_name: str, vt_symbol: str, setting: dict):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.tps: int = max(1, int(round(1.0 / float(self.tick_interval_sec))))
        self.w30: int = 30 * self.tps
        self.w3: int = 3 * self.tps
        self.w1: int = 1 * self.tps

        # rolling buffers
        self.mid_buf: Deque[float] = deque(maxlen=self.w30)
        self.mid_dt_buf: Deque[datetime] = deque(maxlen=self.w30)

        self.dv30_buf: Deque[float] = deque(maxlen=self.w30)
        self.sum_dv30: float = 0.0

        # last tick references
        self.last_dt: Optional[datetime] = None
        self.last_volume: Optional[float] = None

        # gate state
        self._gate_on: bool = False
        self._gate_open_time: Optional[datetime] = None
        self._gate_strength_on: float = 0.0
        self._gate_hold_until: Optional[datetime] = None
        self._gate_off_start: Optional[datetime] = None
        self._gate_cooldown_until: Optional[datetime] = None
        self._forced_on_eff: bool = False  # open-force effective at this tick

        # exec rearm state
        self._exec_need_rearm: bool = False
        self._exec_bad_start: Optional[datetime] = None
        self._exec_ok_since: Optional[datetime] = None

        # dynamic exec thresholds / score
        self._exec_fast_score: float = 0.0
        self._exec_dynamic_state: str = "static"
        self._eff_spread_max: float = float(self.spread_max_ticks)
        self._eff_depth_min: float = float(self.depth_min)

        # panic lockout
        self._panic_lockout_until: Optional[datetime] = None

        # session scheduler parsed times / state
        self._t_am_open: time = _parse_hms(self.session_am_open_time, time(9, 30, 0))
        self._t_lunch_start: time = _parse_hms(self.session_lunch_start_time, time(11, 30, 0))
        self._t_pm_open: time = _parse_hms(self.session_pm_open_time, time(13, 0, 0))
        self._t_close: time = _parse_hms(self.session_close_time, time(15, 0, 0))
        self._t_lunch_flat: time = _parse_hms(self.session_lunch_flat_time, time(11, 29, 55))
        self._t_eod_flat: time = _parse_hms(self.session_eod_flat_time, time(14, 59, 55))

        self._session_gate_block: bool = False
        self._session_entry_block: bool = False
        self._session_block_reason: str = ""
        self._did_lunch_flat_date: Optional[date] = None
        self._did_eod_flat_date: Optional[date] = None

        self._last_bid: float = 0.0
        self._last_ask: float = 0.0

        # keep-diag
        self._keep_diag_counters: Dict[str, int] = {
            "gate_open_events": 0,
            "gate_close_events": 0,
            "close_by_keep": 0,
            "close_by_forced": 0,
            "close_by_session_lunch": 0,
            "close_by_session_eod": 0,
            "close_by_panic_l1": 0,
            "close_by_panic_l2": 0,
            "panic_events_l1": 0,
            "panic_events_l2": 0,
            "maintain_true_ticks": 0,
            "maintain_false_ticks": 0,
            "off_timer_started": 0,
            "off_timer_hit_confirm": 0,
            "off_timer_aborted": 0,
        }
        self._keep_diag_mins: Dict[str, float] = {
            "min_margin_range": 1e18,
            "min_margin_vol": 1e18,
        }
        self._keep_diag_transitions: List[dict] = []

        # gate bucket
        self._range_bucket_edges = [0.0, 10.0, 20.0, 40.0, 80.0, 1e18]  # in ticks
        n_bucket = len(self._range_bucket_edges) - 1
        self._bucket_total = [0] * n_bucket
        self._bucket_gate_on = [0] * n_bucket
        self._bucket_force_on = [0] * n_bucket
        self._bucket_forced_off = [0] * n_bucket

        # gate minute diag
        self._gate_diag_fp = None
        self._gate_diag_writer = None
        self._gate_diag_minute: Optional[datetime] = None
        self._gate_diag_min_counts = {
            "ticks": 0,
            "gate_on_ticks": 0,
            "force_on_ticks": 0,
            "forced_off_ticks": 0,
            "exec_ok_ticks": 0,
            "entry_allowed_ticks": 0,
        }

        self._summary_counts: Dict[str, int] = {
            "ticks": 0,
            "gate_on_ticks": 0,
            "force_on_ticks": 0,
            "forced_off_ticks": 0,
            "raw_exec_ok_ticks": 0,
            "raw_exec_ok_in_gate_ticks": 0,
            "exec_ok_ticks": 0,
            "exec_ok_in_gate_ticks": 0,
            "entry_allowed_ticks": 0,
            "blocked_entry_ticks": 0,
            "entry_block_open_ticks": 0,
            "entry_block_session_ticks": 0,
            "rearm_block_ticks": 0,
            "rearm_block_in_gate_ticks": 0,
            "bad_spread_ticks": 0,
            "bad_spread_in_gate_ticks": 0,
            "bad_depth_ticks": 0,
            "bad_depth_in_gate_ticks": 0,
            "bad_vol_ticks": 0,
            "bad_vol_in_gate_ticks": 0,
            "dyn_state_base_ticks": 0,
            "dyn_state_fast_ticks": 0,
            "dyn_state_burst_ticks": 0,
            "dyn_state_base_in_gate_ticks": 0,
            "dyn_state_fast_in_gate_ticks": 0,
            "dyn_state_burst_in_gate_ticks": 0,
            "dyn_relaxed_ticks": 0,
            "dyn_relaxed_in_gate_ticks": 0,
        }

        self._summary_sec_counts: Dict[str, int] = {
            "secs": 0,
            "gate_on_secs": 0,
            "force_on_secs": 0,
            "raw_exec_ok_secs": 0,
            "raw_exec_ok_in_gate_secs": 0,
            "exec_ok_secs": 0,
            "exec_ok_in_gate_secs": 0,
            "entry_allowed_secs": 0,
            "blocked_entry_secs": 0,
            "entry_block_open_secs": 0,
            "entry_block_session_secs": 0,
            "rearm_block_secs": 0,
            "rearm_block_in_gate_secs": 0,
            "bad_spread_secs": 0,
            "bad_spread_in_gate_secs": 0,
            "bad_depth_secs": 0,
            "bad_depth_in_gate_secs": 0,
            "bad_vol_secs": 0,
            "bad_vol_in_gate_secs": 0,
            "dyn_state_base_secs": 0,
            "dyn_state_fast_secs": 0,
            "dyn_state_burst_secs": 0,
            "dyn_state_base_in_gate_secs": 0,
            "dyn_state_fast_in_gate_secs": 0,
            "dyn_state_burst_in_gate_secs": 0,
            "dyn_relaxed_secs": 0,
            "dyn_relaxed_in_gate_secs": 0,
        }
        self._summary_sec_bucket: Optional[datetime] = None
        self._summary_sec_flags: Dict[str, int] = {k: 0 for k in self._summary_sec_counts.keys() if k != "secs"}
        self._summary_float_sums: Dict[str, float] = {
            "fast_score_sum": 0.0,
            "fast_score_in_gate_sum": 0.0,
            "eff_spread_sum": 0.0,
            "eff_spread_in_gate_sum": 0.0,
            "eff_depth_sum": 0.0,
            "eff_depth_in_gate_sum": 0.0,
        }

        self._probe_samples: Dict[str, List[Dict[str, object]]] = {
            "raw_exec_ok_in_gate": [],
            "exec_ok_in_gate": [],
            "entry_allowed": [],
            "blocked_entry": [],
            "rearm_block_in_gate": [],
            "bad_spread_in_gate": [],
            "bad_depth_in_gate": [],
            "bad_vol_in_gate": [],
        }

        # opp windows
        self._opp_windows: Dict[int, _OppWindow] = {}
        self._opp_thresholds: List[float] = []
        self._opp_exec_spread_max: float = float(self.opp_exec_spread_max_ticks)
        self._opp_exec_depth_min: float = float(self.opp_exec_depth_min)

        if int(self.opp_enable):
            horizons = _parse_int_list(str(self.opp_horizons_s))
            thresholds = _parse_float_list(str(self.opp_thresholds_ticks))
            self._opp_thresholds = sorted({float(x) for x in thresholds})
            for H in horizons:
                self._opp_windows[int(H)] = _OppWindow(
                    ticks=deque(),
                    maxq=deque(),
                    minq=deque(),
                    opp_total={r: 0 for r in self._opp_thresholds},
                    opp_gate={r: 0 for r in self._opp_thresholds},
                    opp_gate_exec={r: 0 for r in self._opp_thresholds},
                    opp_forced={r: 0 for r in self._opp_thresholds},
                )

    def on_init(self) -> None:
        self.write_log("GateModuleStrategy_V09 init")
        self._open_gate_diag_file()

    def on_start(self) -> None:
        self.write_log("GateModuleStrategy_V09 start")

    def on_stop(self) -> None:
        # flush minute diag
        self._flush_gate_diag_minute(force=True)
        self._close_gate_diag_file()

        self._summary_sec_flush(force=True)

        # outputs
        self._write_gate_bucket_summary()
        self._write_opp_summary()
        self._write_gate_summary()
        self._write_gate_probe()
        self._keep_diag_dump()

        self.write_log("GateModuleStrategy_V09 stop")

    # -----------------------------
    # vn.py callbacks (unused in scan-only mode)
    # -----------------------------
    def on_tick(self, tick: TickData) -> None:
        # TODO debug
        if not hasattr(self, "_debug_count"):
            self._debug_count = 0

        self._debug_count += 1

        dt: datetime = tick.datetime
        if not isinstance(dt, datetime):
            return

        bid = _safe_float(getattr(tick, "bid_price_1", 0.0), 0.0)
        ask = _safe_float(getattr(tick, "ask_price_1", 0.0), 0.0)
        self._last_bid = bid
        self._last_ask = ask

        if bid <= 0 or ask <= 0 or ask <= bid:
            self._handle_panic(dt, level=1, reason="BOOK_INVALID")
            self._reset_rolling(dt)
            self.last_volume = None
            self.last_dt = dt
            return

        vol = _safe_float(getattr(tick, "volume", 0.0), 0.0)

        if self.last_dt is not None:
            gap_s = (dt - self.last_dt).total_seconds()
            if gap_s >= float(self.panic_tick_gap_s):
                if int(self.session_enable) and self._is_planned_gap(self.last_dt, dt):
                    prev_t = self.last_dt.time()
                    now_t = dt.time()
                    if prev_t < self._t_lunch_start and now_t >= self._t_pm_open:
                        self._did_lunch_flat_date = dt.date()
                        self._enforce_session_flat(dt, bid, ask, kind="LUNCH")
                    elif (self.last_dt.date() != dt.date()) or (prev_t >= self._t_eod_flat and now_t >= self._t_am_open):
                        self._did_eod_flat_date = dt.date()
                        self._enforce_session_flat(dt, bid, ask, kind="EOD")
                    else:
                        self._reset_rolling(dt)
                        self.last_volume = None
                else:
                    self._handle_panic(dt, level=1, reason=f"TICK_GAP_{gap_s:.1f}s")
                    self._reset_rolling(dt)
                    self.last_volume = None

        if int(self.session_enable):
            self._update_session_blocks(dt, bid, ask)

        mid = 0.5 * (bid + ask)

        if self.last_volume is None:
            dV = 0.0
        else:
            dV = vol - self.last_volume
        self.last_volume = vol
        self.last_dt = dt

        if dV < 0:
            self._handle_panic(dt, level=2, reason="DV_NEG")
            self._reset_rolling(dt)
            self.last_volume = None
            return

        dV_eff = float(dV)
        if dV_eff > float(self.dv_spike_max):
            dV_eff = 0.0
            self._spike_count = getattr(self, "_spike_count", 0) + 1
            if int(self.panic_enable) and self._spike_count >= int(self.panic_dv_spike_n):
                self._handle_panic(dt, level=1, reason="DV_SPIKE_STREAK")
                self._spike_count = 0
        else:
            self._spike_count = 0

        self._update_rolling(dt, mid, dV_eff)

        self.range_30s = self._calc_range_30s_ticks()
        self.vol_30s = float(self.sum_dv30)

        if int(self.panic_enable):
            self._panic_check_jump_range(dt, mid)

        spread_ticks_now = (ask - bid) / float(self.pricetick) if (bid > 0 and ask > 0) else 1e9
        depth_total_now = _safe_float(getattr(tick, "bid_volume_1", 0.0), 0.0) + _safe_float(getattr(tick, "ask_volume_1", 0.0), 0.0)
        spread_eff, depth_eff, fast_score, dyn_state = self._get_exec_effective_thresholds()
        self._eff_spread_max = float(spread_eff)
        self._eff_depth_min = float(depth_eff)
        self._exec_fast_score = float(fast_score)
        self._exec_dynamic_state = str(dyn_state)

        bad_spread = spread_ticks_now > float(spread_eff)
        bad_depth = depth_total_now < float(depth_eff)
        bad_vol = bool(int(self.exec_use_vol_min_30s) and (self.vol_30s < float(self.vol_min_30s)))
        raw_exec_ok = (not bad_spread) and (not bad_depth) and (not bad_vol)

        exec_ok_entry = self._update_exec_state(dt, tick, spread_eff, depth_eff)
        rearm_block = bool(raw_exec_ok and (not exec_ok_entry))
        self.exec_ok = 1 if exec_ok_entry else 0

        self._forced_on_eff = (
            self._in_open_force_window(dt)
            and int(self.open_force_enable)
            and (not self._in_panic_lockout(dt))
            and (not self._in_gate_cooldown(dt))
            and (not bool(getattr(self, "_session_gate_block", False)))
        )
        self._update_gate_state_v2(dt)

        self.gate_on = 1 if self._gate_on else 0
        self.gate_strength = float(self._gate_strength_on)
        open_entry_block = bool(self._in_open_entry_block(dt))
        session_entry_block = bool(getattr(self, "_session_entry_block", False))
        self.entry_allowed = 1 if (
            self._gate_on
            and exec_ok_entry
            and (not open_entry_block)
            and (not session_entry_block)
        ) else 0
        self.panic_lockout = 1 if self._in_panic_lockout(dt) else 0
        blocked_entry = bool(self._gate_on and exec_ok_entry and (not self.entry_allowed))

        self._summary_counts["ticks"] += 1
        self._update_dynamic_exec_counters(self._gate_on, dyn_state, fast_score, spread_eff, depth_eff)
        if self._gate_on:
            self._summary_counts["gate_on_ticks"] += 1
        if self._forced_on_eff:
            self._summary_counts["force_on_ticks"] += 1
        if raw_exec_ok:
            self._summary_counts["raw_exec_ok_ticks"] += 1
        if self._gate_on and raw_exec_ok:
            self._summary_counts["raw_exec_ok_in_gate_ticks"] += 1
        if exec_ok_entry:
            self._summary_counts["exec_ok_ticks"] += 1
        if self._gate_on and exec_ok_entry:
            self._summary_counts["exec_ok_in_gate_ticks"] += 1
        if blocked_entry:
            self._summary_counts["blocked_entry_ticks"] += 1
            if open_entry_block:
                self._summary_counts["entry_block_open_ticks"] += 1
            if session_entry_block:
                self._summary_counts["entry_block_session_ticks"] += 1
        if self.entry_allowed:
            self._summary_counts["entry_allowed_ticks"] += 1
        if rearm_block:
            self._summary_counts["rearm_block_ticks"] += 1
            if self._gate_on:
                self._summary_counts["rearm_block_in_gate_ticks"] += 1
        if bad_spread:
            self._summary_counts["bad_spread_ticks"] += 1
            if self._gate_on:
                self._summary_counts["bad_spread_in_gate_ticks"] += 1
        if bad_depth:
            self._summary_counts["bad_depth_ticks"] += 1
            if self._gate_on:
                self._summary_counts["bad_depth_in_gate_ticks"] += 1
        if bad_vol:
            self._summary_counts["bad_vol_ticks"] += 1
            if self._gate_on:
                self._summary_counts["bad_vol_in_gate_ticks"] += 1

        self._summary_sec_update(
            dt,
            gate_on=self._gate_on,
            force_on=self._forced_on_eff,
            raw_exec_ok=raw_exec_ok,
            raw_exec_ok_in_gate=(self._gate_on and raw_exec_ok),
            exec_ok=exec_ok_entry,
            exec_ok_in_gate=(self._gate_on and exec_ok_entry),
            entry_allowed=bool(self.entry_allowed),
            blocked_entry=blocked_entry,
            entry_block_open=(blocked_entry and open_entry_block),
            entry_block_session=(blocked_entry and session_entry_block),
            rearm_block=rearm_block,
            rearm_block_in_gate=(self._gate_on and rearm_block),
            bad_spread=bad_spread,
            bad_spread_in_gate=(self._gate_on and bad_spread),
            bad_depth=bad_depth,
            bad_depth_in_gate=(self._gate_on and bad_depth),
            bad_vol=bad_vol,
            bad_vol_in_gate=(self._gate_on and bad_vol),
            dyn_state_base=(dyn_state == "base"),
            dyn_state_fast=(dyn_state == "fast"),
            dyn_state_burst=(dyn_state == "burst"),
            dyn_state_base_in_gate=(self._gate_on and dyn_state == "base"),
            dyn_state_fast_in_gate=(self._gate_on and dyn_state == "fast"),
            dyn_state_burst_in_gate=(self._gate_on and dyn_state == "burst"),
            dyn_relaxed=((float(spread_eff) > float(self.exec_dyn_spread_base) + 1e-12) or (float(depth_eff) < float(self.exec_dyn_depth_base) - 1e-12)),
            dyn_relaxed_in_gate=(self._gate_on and ((float(spread_eff) > float(self.exec_dyn_spread_base) + 1e-12) or (float(depth_eff) < float(self.exec_dyn_depth_base) - 1e-12))),
        )

        self._probe_capture(
            dt=dt,
            spread_ticks_now=spread_ticks_now,
            depth_total_now=depth_total_now,
            bad_spread=bad_spread,
            bad_depth=bad_depth,
            bad_vol=bad_vol,
            raw_exec_ok=raw_exec_ok,
            exec_ok_entry=exec_ok_entry,
            rearm_block=rearm_block,
            open_entry_block=open_entry_block,
            session_entry_block=session_entry_block,
            blocked_entry=blocked_entry,
            fast_score=fast_score,
            eff_spread_max=spread_eff,
            eff_depth_min=depth_eff,
            dyn_state=dyn_state,
        )

        self._update_gate_bucket(self.range_30s, self._gate_on, self._forced_on_eff, forced_off_tick=False)
        self._gate_diag_update_tick(dt, self._gate_on, self._forced_on_eff, exec_ok_entry, bool(self.entry_allowed))

        if int(self.opp_enable):
            if int(self.exec_dynamic_opp_follow_main):
                exec_ok_for_opp = raw_exec_ok
            else:
                exec_ok_for_opp = (spread_ticks_now <= float(self._opp_exec_spread_max)) and (depth_total_now >= float(self._opp_exec_depth_min))
            self._opp_update_tick(dt, mid, self._gate_on, exec_ok_for_opp, self._forced_on_eff)

        if int(self.gate_diag_only):
            return

        return

    def on_bar(self, bar: BarData) -> None:
        pass

    def on_trade(self, trade: TradeData) -> None:
        pass

    def on_order(self, order: OrderData) -> None:
        pass

    def on_stop_order(self, stop_order: StopOrder) -> None:
        pass

    # =========================
    # Rolling / metrics
    # =========================
    def _reset_rolling(self, now: datetime) -> None:
        self.mid_buf.clear()
        self.mid_dt_buf.clear()
        self.dv30_buf.clear()
        self.sum_dv30 = 0.0
        self._gate_off_start = None

    def _update_rolling(self, dt: datetime, mid: float, dV_eff: float) -> None:
        # mid
        self.mid_buf.append(float(mid))
        self.mid_dt_buf.append(dt)

        # dV rolling sum
        if len(self.dv30_buf) == self.dv30_buf.maxlen:
            self.sum_dv30 -= float(self.dv30_buf[0])
        self.dv30_buf.append(float(dV_eff))
        self.sum_dv30 += float(dV_eff)

    def _calc_range_30s_ticks(self) -> float:
        if not self.mid_buf:
            return 0.0
        pt = float(self.pricetick)
        hi = max(self.mid_buf)
        lo = min(self.mid_buf)
        return (float(hi) - float(lo)) / pt if pt > 0 else 0.0

    def _calc_range_3s_ticks(self) -> float:
        if not self.mid_buf:
            return 0.0
        pt = float(self.pricetick)
        n = min(len(self.mid_buf), self.w3)
        if n <= 1:
            return 0.0
        tail = list(self.mid_buf)[-n:]
        return (max(tail) - min(tail)) / pt if pt > 0 else 0.0

    def _mid_1s_ago(self) -> Optional[float]:
        """Return approximate mid 1s ago using buffer timestamps."""
        if not self.mid_dt_buf:
            return None
        # walk from right to left until >=1s
        dt_now = self.mid_dt_buf[-1]
        target = dt_now - timedelta(seconds=1)
        for i in range(len(self.mid_dt_buf) - 1, -1, -1):
            if self.mid_dt_buf[i] <= target:
                return float(self.mid_buf[i])
        return float(self.mid_buf[0])


    def _exec_dynamic_score(self) -> float:
        if not int(self.exec_dynamic_enable):
            return 0.0
        r_on = max(float(self.range_on_30s_ticks), 1e-9)
        v_on = max(float(self.vol_on_30s), 1e-9)

        s_range = _clip01((float(self.range_30s) / r_on) - 1.0)
        s_vol = _clip01((float(self.vol_30s) / v_on) - 1.0)

        w_range = max(0.0, float(self.exec_dyn_score_w_range))
        w_vol = max(0.0, float(self.exec_dyn_score_w_vol))
        w_sum = w_range + w_vol
        if w_sum <= 1e-12:
            w_range = 0.5
            w_vol = 0.5
            w_sum = 1.0
        return _clip01((w_range * s_range + w_vol * s_vol) / w_sum)

    def _get_exec_effective_thresholds(self) -> Tuple[float, float, float, str]:
        if not int(self.exec_dynamic_enable):
            spread_eff = float(self.spread_max_ticks)
            depth_eff = float(self.depth_min)
            fast_score = 0.0
            state = "static"
        else:
            fast_score = self._exec_dynamic_score()
            mode = str(self.exec_dyn_trigger_mode).upper().strip()

            if mode == "RATIO_OR":
                r_on = max(float(self.range_on_30s_ticks), 1e-9)
                v_on = max(float(self.vol_on_30s), 1e-9)
                range_ratio = float(self.range_30s) / r_on
                vol_ratio = float(self.vol_30s) / v_on

                if (range_ratio >= float(self.exec_dyn_burst_range_mult)) or (vol_ratio >= float(self.exec_dyn_burst_vol_mult)):
                    state = "burst"
                    spread_eff = float(self.exec_dyn_spread_burst)
                    depth_eff = float(self.exec_dyn_depth_burst)
                elif (range_ratio >= float(self.exec_dyn_fast_range_mult)) or (vol_ratio >= float(self.exec_dyn_fast_vol_mult)):
                    state = "fast"
                    spread_eff = float(self.exec_dyn_spread_fast)
                    depth_eff = float(self.exec_dyn_depth_fast)
                else:
                    state = "base"
                    spread_eff = float(self.exec_dyn_spread_base)
                    depth_eff = float(self.exec_dyn_depth_base)
            else:
                if fast_score >= float(self.exec_dyn_burst_thresh):
                    state = "burst"
                    spread_eff = float(self.exec_dyn_spread_burst)
                    depth_eff = float(self.exec_dyn_depth_burst)
                elif fast_score >= float(self.exec_dyn_fast_thresh):
                    state = "fast"
                    spread_eff = float(self.exec_dyn_spread_fast)
                    depth_eff = float(self.exec_dyn_depth_fast)
                else:
                    state = "base"
                    spread_eff = float(self.exec_dyn_spread_base)
                    depth_eff = float(self.exec_dyn_depth_base)

            spread_cap = float(self.exec_dyn_spread_cap_hard)
            if spread_cap > 0:
                spread_eff = min(spread_eff, spread_cap)

            depth_floor = float(self.exec_dyn_depth_floor_hard)
            if depth_floor > 0:
                depth_eff = max(depth_eff, depth_floor)

        return max(0.0, float(spread_eff)), max(0.0, float(depth_eff)), float(fast_score), state
    def _update_dynamic_exec_counters(self, gate_on: bool, state: str, fast_score: float, spread_eff: float, depth_eff: float) -> None:
        key = state if state in ("base", "fast", "burst") else "base"
        self._summary_counts[f"dyn_state_{key}_ticks"] += 1

        relaxed = int(
            (float(spread_eff) > float(self.exec_dyn_spread_base) + 1e-12)
            or (float(depth_eff) < float(self.exec_dyn_depth_base) - 1e-12)
        )
        if relaxed:
            self._summary_counts["dyn_relaxed_ticks"] += 1

        if gate_on:
            self._summary_counts[f"dyn_state_{key}_in_gate_ticks"] += 1
            if relaxed:
                self._summary_counts["dyn_relaxed_in_gate_ticks"] += 1

        fs = self._summary_float_sums
        fs["fast_score_sum"] += float(fast_score)
        fs["eff_spread_sum"] += float(spread_eff)
        fs["eff_depth_sum"] += float(depth_eff)
        if gate_on:
            fs["fast_score_in_gate_sum"] += float(fast_score)
            fs["eff_spread_in_gate_sum"] += float(spread_eff)
            fs["eff_depth_in_gate_sum"] += float(depth_eff)
    def _session_state(self, now: datetime) -> str:
        """Return one of: AM, LUNCH, PM, CLOSED."""
        t = now.time()
        if self._t_am_open <= t < self._t_lunch_start:
            return "AM"
        if self._t_lunch_start <= t < self._t_pm_open:
            return "LUNCH"
        if self._t_pm_open <= t < self._t_close:
            return "PM"
        return "CLOSED"

    def _in_pre_flat_zone(self, now: datetime) -> str:
        """Return '' or 'LUNCH'/'EOD' for [flat, boundary) windows."""
        t = now.time()
        if self._t_lunch_flat <= t < self._t_lunch_start:
            return "LUNCH"
        if self._t_eod_flat <= t < self._t_close:
            return "EOD"
        return ""

    def _is_planned_gap(self, prev_dt: datetime, now_dt: datetime) -> bool:
        """Planned gaps: lunch break + overnight/cross-day."""
        if prev_dt.date() != now_dt.date():
            return True
        prev_t = prev_dt.time()
        now_t = now_dt.time()
        if prev_t < self._t_lunch_start and now_t >= self._t_pm_open and prev_t >= self._t_lunch_flat:
            return True
        if prev_t >= self._t_eod_flat and now_t >= self._t_am_open:
            return True
        if prev_t < self._t_am_open and now_t >= self._t_am_open:
            return True
        return False

    def _flatten_position(self, now: datetime, bid: float, ask: float, reason: str) -> None:
        """Flatten position (best-effort). Only used when gate_diag_only==0."""
        try:
            self.cancel_all()
        except Exception:
            pass
        if self.pos == 0:
            return
        vol = abs(int(self.pos))
        if self.pos > 0:
            px = float(bid) if bid > 0 else float(ask)
            try:
                self.sell(px, vol)
            except Exception:
                pass
        else:
            px = float(ask) if ask > 0 else float(bid)
            try:
                self.cover(px, vol)
            except Exception:
                pass
        self.write_log(f"[SESSION_FLATTEN] {reason} pos={self.pos} at {now}")

    def _enforce_session_flat(self, now: datetime, bid: float, ask: float, kind: str) -> None:
        """Hard flatten + gate off. kind in {'LUNCH','EOD'}."""
        if not int(self.session_enable):
            return
        if self._gate_on:
            self._close_gate(now, category=("session_lunch" if kind == "LUNCH" else "session_eod"), reason=f"session_{kind.lower()}_flat")
        self._reset_rolling(now)
        self.last_volume = None
        if not int(self.gate_diag_only):
            self._flatten_position(now, bid, ask, reason=f"session_{kind.lower()}_flat")

    def _update_session_blocks(self, now: datetime, bid: float, ask: float) -> None:
        """Update session-based gate/entry blocks and perform hard flatten at boundaries."""
        self._session_gate_block = False
        self._session_entry_block = False
        self._session_block_reason = ""
        if not int(self.session_enable):
            return
        st = self._session_state(now)
        pre = self._in_pre_flat_zone(now)

        if pre == "LUNCH":
            if self._did_lunch_flat_date != now.date():
                self._did_lunch_flat_date = now.date()
                self._enforce_session_flat(now, bid, ask, kind="LUNCH")
            self._session_gate_block = True
            self._session_entry_block = True
            self._session_block_reason = "pre_lunch_flat"
            return

        if pre == "EOD":
            if self._did_eod_flat_date != now.date():
                self._did_eod_flat_date = now.date()
                self._enforce_session_flat(now, bid, ask, kind="EOD")
            self._session_gate_block = True
            self._session_entry_block = True
            self._session_block_reason = "pre_eod_flat"
            return

        if st == "LUNCH":
            self._session_gate_block = True
            self._session_entry_block = True
            self._session_block_reason = "lunch_lockout"
            if self._gate_on:
                self._close_gate(now, category="session_lunch", reason="lunch_lockout")
            self._reset_rolling(now)
            self.last_volume = None
            return

        if st == "CLOSED":
            self._session_gate_block = True
            self._session_entry_block = True
            self._session_block_reason = "overnight_lockout"
            if self._gate_on:
                self._close_gate(now, category="session_eod", reason="overnight_lockout")
            self._reset_rolling(now)
            self.last_volume = None
            return

        cutoff = max(0, int(self.session_entry_cutoff_s))
        if cutoff > 0:
            now_s = _time_to_seconds(now.time())
            if st == "AM":
                flat_s = _time_to_seconds(self._t_lunch_flat)
                if (flat_s - cutoff) <= now_s < flat_s:
                    self._session_entry_block = True
                    self._session_block_reason = "entry_cutoff"
            elif st == "PM":
                flat_s = _time_to_seconds(self._t_eod_flat)
                if (flat_s - cutoff) <= now_s < flat_s:
                    self._session_entry_block = True
                    self._session_block_reason = "entry_cutoff"

    def _in_gate_cooldown(self, now: datetime) -> bool:
        return self._gate_cooldown_until is not None and now < self._gate_cooldown_until

    def _in_panic_lockout(self, now: datetime) -> bool:
        return self._panic_lockout_until is not None and now < self._panic_lockout_until

    def _open_condition(self) -> bool:
        r_ok = self.range_30s >= float(self.range_on_30s_ticks)
        v_ok = self.vol_30s >= float(self.vol_on_30s)
        mode = str(self.gate_open_mode or "AND").upper()
        if mode == "OR":
            return r_ok or v_ok
        return r_ok and v_ok

    def _strength(self) -> float:
        r = float(self.range_on_30s_ticks)
        v = float(self.vol_on_30s)
        if r <= 0 or v <= 0:
            return 0.0
        return max(self.range_30s / r, self.vol_30s / v)

    def _strength_to_min_hold(self, s: float) -> int:
        if s < float(self.gate_s_thresh1):
            return int(self.gate_min_hold_s1)
        if s < float(self.gate_s_thresh2):
            return int(self.gate_min_hold_s2)
        return int(self.gate_min_hold_s3)

    def _strength_to_confirm(self, s: float) -> int:
        if s < float(self.gate_s_thresh1):
            base = int(self.gate_off_confirm_s1)
        elif s < float(self.gate_s_thresh2):
            base = int(self.gate_off_confirm_s2)
        else:
            base = int(self.gate_off_confirm_s3)
        sec = int(round(float(base) * float(self.gate_off_confirm_scale)))
        return max(1, sec)

    def _fixed_confirm(self) -> int:
        sec = int(round(float(self.gate_confirm_base_s) * float(self.gate_off_confirm_scale)))
        return max(1, sec)

    def _keep_thresholds(self) -> Tuple[float, float]:
        return float(self.range_on_30s_ticks) * float(self.range_keep_ratio), float(self.vol_on_30s) * float(self.vol_keep_ratio)

    def _keep_diag_update_mins(self, margin_r: float, margin_v: float) -> None:
        if margin_r < self._keep_diag_mins["min_margin_range"]:
            self._keep_diag_mins["min_margin_range"] = float(margin_r)
        if margin_v < self._keep_diag_mins["min_margin_vol"]:
            self._keep_diag_mins["min_margin_vol"] = float(margin_v)

    def _keep_diag_add_transition(self, now: datetime, event: str) -> None:
        maxn = int(self.gate_diag_max_transitions)
        if maxn <= 0:
            return
        if len(self._keep_diag_transitions) >= maxn:
            return
        self._keep_diag_transitions.append({
            "dt": now.isoformat(sep=" ", timespec="seconds"),
            "event": event,
            "gate_on": 1 if self._gate_on else 0,
            "range_30s": float(self.range_30s),
            "vol_30s": float(self.vol_30s),
            "gate_strength_on": float(self._gate_strength_on),
        })

    def _open_gate(self, now: datetime, forced: bool) -> None:
        if self._gate_on:
            return
        self._gate_on = True
        self._gate_open_time = now
        s = self._strength()
        self._gate_strength_on = float(s)
        hold = self._strength_to_min_hold(s)
        self._gate_hold_until = now + timedelta(seconds=int(hold))
        self._gate_off_start = None
        self._keep_diag_counters["gate_open_events"] += 1
        self._keep_diag_add_transition(now, "open_forced" if forced else "open")
        self.put_event()

    def _close_gate(self, now: datetime, category: str, reason: str = "") -> None:
        """Close gate and record reason category."""
        if not self._gate_on:
            return
        self._gate_on = False
        self._gate_off_start = None
        self._gate_hold_until = None
        self._gate_open_time = None
        self._gate_cooldown_until = now + timedelta(seconds=int(self.gate_cooldown_s)) if int(self.gate_cooldown_s) > 0 else None
        self._keep_diag_counters["gate_close_events"] += 1

        cat = str(category or "").lower()
        if cat == "keep":
            self._keep_diag_counters["close_by_keep"] += 1
        elif cat == "forced":
            self._keep_diag_counters["close_by_forced"] += 1
        elif cat == "session_lunch":
            self._keep_diag_counters["close_by_session_lunch"] += 1
        elif cat == "session_eod":
            self._keep_diag_counters["close_by_session_eod"] += 1
        elif cat == "panic_l2":
            self._keep_diag_counters["close_by_panic_l2"] += 1
        elif cat == "panic_l1":
            self._keep_diag_counters["close_by_panic_l1"] += 1

        tag = f"close_{cat}"
        if reason:
            tag = f"{tag}_{reason}"
        self._keep_diag_add_transition(now, tag)
        self.put_event()

    def _update_gate_state_v2(self, now: datetime) -> None:
        # Session blocks override gate (no gate during lockout / pre-flat zones)
        if int(self.session_enable) and bool(getattr(self, "_session_gate_block", False)):
            if self._gate_on:
                r = str(getattr(self, "_session_block_reason", "")).lower()
                cat = "session_lunch" if "lunch" in r else "session_eod"
                self._close_gate(now, category=cat, reason=r or "session_block")
            return

        if self._in_panic_lockout(now):
            if self._gate_on:
                self._close_gate(now, category="forced", reason="panic_lockout")
            return

        if self._in_gate_cooldown(now):
            if self._gate_on:
                self._close_gate(now, category="forced", reason="cooldown_violation")
            return

        if self._forced_on_eff:
            self._open_gate(now, forced=True)
            force_until = self._open_force_until(now)
            if force_until:
                self._gate_hold_until = force_until
            return

        if not self._gate_on:
            if self._open_condition():
                self._open_gate(now, forced=False)
            return

        if self._gate_hold_until is not None and now < self._gate_hold_until:
            return

        range_keep, vol_keep = self._keep_thresholds()
        maintain = (self.range_30s >= range_keep) or (self.vol_30s >= vol_keep)
        self._keep_diag_update_mins(self.range_30s - range_keep, self.vol_30s - vol_keep)

        if maintain:
            self._keep_diag_counters["maintain_true_ticks"] += 1
            if self._gate_off_start is not None:
                self._keep_diag_counters["off_timer_aborted"] += 1
            self._gate_off_start = None
            return

        self._keep_diag_counters["maintain_false_ticks"] += 1

        if self._gate_off_start is None:
            self._gate_off_start = now
            self._keep_diag_counters["off_timer_started"] += 1
            return

        confirm_s = max(1, int(round(float(self.gate_confirm_base_s) * float(self.gate_off_confirm_scale))))
        if (now - self._gate_off_start).total_seconds() >= confirm_s:
            self._keep_diag_counters["off_timer_hit_confirm"] += 1
            self._close_gate(now, category="keep", reason="keep_confirm")
            self._gate_off_start = None
            return

    def _update_exec_state(self, now: datetime, tick: TickData, spread_max_eff: Optional[float] = None, depth_min_eff: Optional[float] = None) -> bool:
        bid = _safe_float(getattr(tick, "bid_price_1", 0.0), 0.0)
        ask = _safe_float(getattr(tick, "ask_price_1", 0.0), 0.0)
        spread_ticks = (ask - bid) / float(self.pricetick) if (bid > 0 and ask > 0) else 1e9
        depth_total = _safe_float(getattr(tick, "bid_volume_1", 0.0), 0.0) + _safe_float(getattr(tick, "ask_volume_1", 0.0), 0.0)

        spread_lim = float(spread_max_eff) if spread_max_eff is not None else float(self.spread_max_ticks)
        depth_lim = float(depth_min_eff) if depth_min_eff is not None else float(self.depth_min)
        ok = (spread_ticks <= spread_lim) and (depth_total >= depth_lim)
        if int(self.exec_use_vol_min_30s):
            ok = ok and (self.vol_30s >= float(self.vol_min_30s))

        # rearm logic
        debounce = max(0, int(self.exec_bad_debounce_s))
        rearm_s = max(0, int(self.exec_rearm_s))

        if not ok:
            if self._exec_bad_start is None:
                self._exec_bad_start = now
            if debounce == 0 or (now - self._exec_bad_start).total_seconds() >= float(debounce):
                self._exec_need_rearm = True
                self._exec_ok_since = None
            return False

        # ok
        self._exec_bad_start = None

        if not self._exec_need_rearm or rearm_s == 0:
            self._exec_need_rearm = False
            self._exec_ok_since = None
            return True

        # needs rearm: require ok for rearm_s
        if self._exec_ok_since is None:
            self._exec_ok_since = now
            return False

        if (now - self._exec_ok_since).total_seconds() >= float(rearm_s):
            self._exec_need_rearm = False
            self._exec_ok_since = None
            return True

        return False

    # =========================
    # Open force helpers
    # =========================
    def _in_open_entry_block(self, now: datetime) -> bool:
        # Block entry for first N seconds after session open (AM/PM)
        sec = max(0, int(self.open_entry_block_s))
        if sec <= 0:
            return False
        if self._is_in_first_seconds(now, self._t_am_open, sec):
            return True
        if int(self.open_force_apply_pm) and self._is_in_first_seconds(now, self._t_pm_open, sec):
            return True
        return False

    def _in_open_force_window(self, now: datetime) -> bool:
        if not int(self.open_force_enable):
            return False
        sec = int(self.open_force_seconds) if int(self.open_force_seconds) > 0 else max(0, int(self.open_force_minutes)) * 60
        if sec <= 0:
            return False
        if self._is_in_first_seconds(now, self._t_am_open, sec):
            return True
        if int(self.open_force_apply_pm) and self._is_in_first_seconds(now, self._t_pm_open, sec):
            return True
        return False
    def _open_force_until(self, now: datetime) -> Optional[datetime]:
        """Return end datetime of current open-force window, else None."""
        sec = int(self.open_force_seconds) if int(self.open_force_seconds) > 0 else max(0, int(self.open_force_minutes)) * 60
        if sec <= 0:
            return None
        dt_am0 = now.replace(hour=self._t_am_open.hour, minute=self._t_am_open.minute, second=0, microsecond=0)
        if dt_am0 <= now < dt_am0 + timedelta(seconds=sec):
            return dt_am0 + timedelta(seconds=sec)
        if int(self.open_force_apply_pm):
            dt_pm0 = now.replace(hour=self._t_pm_open.hour, minute=self._t_pm_open.minute, second=0, microsecond=0)
            if dt_pm0 <= now < dt_pm0 + timedelta(seconds=sec):
                return dt_pm0 + timedelta(seconds=sec)
        return None
    def _is_in_first_seconds(self, now: datetime, session_open: time, seconds: int) -> bool:
        dt0 = now.replace(hour=session_open.hour, minute=session_open.minute, second=0, microsecond=0)
        return dt0 <= now < (dt0 + timedelta(seconds=seconds))

    # =========================
    # Panic
    # =========================
    def _panic_check_jump_range(self, now: datetime, mid: float) -> None:
        # jump (1s)
        mid_ago = self._mid_1s_ago()
        if mid_ago is not None:
            jump_ticks = abs(float(mid) - float(mid_ago)) / float(self.pricetick) if float(self.pricetick) > 0 else 0.0
            if jump_ticks >= float(self.panic_jump_ticks_l2):
                self._handle_panic(now, level=2, reason=f"JUMP1S_{jump_ticks:.1f}")
                return
            if jump_ticks >= float(self.panic_jump_ticks_l1):
                self._handle_panic(now, level=1, reason=f"JUMP1S_{jump_ticks:.1f}")
                return

        # range3
        r3 = self._calc_range_3s_ticks()
        if r3 >= float(self.panic_range3_ticks_l2):
            self._handle_panic(now, level=2, reason=f"RANGE3S_{r3:.1f}")
            return
        if r3 >= float(self.panic_range3_ticks_l1):
            self._handle_panic(now, level=1, reason=f"RANGE3S_{r3:.1f}")
            return

    def _handle_panic(self, now: datetime, level: int, reason: str) -> None:
        if not int(self.panic_enable):
            return
        level = 2 if int(level) >= 2 else 1

        if level == 2:
            self._keep_diag_counters["panic_events_l2"] += 1
        else:
            self._keep_diag_counters["panic_events_l1"] += 1

        if self._gate_on:
            self._close_gate(now, category=("panic_l2" if level == 2 else "panic_l1"), reason=str(reason))

        lock_s = int(self.panic_lockout_s_l2) if level == 2 else int(self.panic_lockout_s_l1)
        lock_s = max(0, lock_s)
        if lock_s > 0:
            self._panic_lockout_until = now + timedelta(seconds=lock_s)

        if (not int(self.gate_diag_only)) and (level == 2) and int(self.panic_level2_flatten):
            self._flatten_position(now, float(getattr(self, "_last_bid", 0.0)), float(getattr(self, "_last_ask", 0.0)), reason=f"panic_L2_{reason}")

    # =========================
    # gate bucket & minute diag
    # =========================
    def _range_bucket_index(self, range_ticks: float) -> int:
        x = float(range_ticks)
        edges = self._range_bucket_edges
        for i in range(len(edges) - 1):
            if edges[i] <= x < edges[i + 1]:
                return i
        return len(edges) - 2

    def _update_gate_bucket(self, range_ticks: float, gate_on: bool, forced_on: bool, forced_off_tick: bool) -> None:
        i = self._range_bucket_index(range_ticks)
        self._bucket_total[i] += 1
        if gate_on:
            self._bucket_gate_on[i] += 1
        if forced_on:
            self._bucket_force_on[i] += 1
        if forced_off_tick:
            self._bucket_forced_off[i] += 1

    def _open_gate_diag_file(self) -> None:
        if not int(self.gate_diag_enable):
            return
        fname = str(self.gate_diag_filename or "").strip()
        if not fname or fname == os.devnull:
            return
        try:
            os.makedirs(os.path.dirname(fname), exist_ok=True)
        except Exception:
            pass
        try:
            self._gate_diag_fp = open(fname, "w", newline="", encoding="utf-8")
            self._gate_diag_writer = csv.writer(self._gate_diag_fp)
            self._gate_diag_writer.writerow([
                "minute",
                "ticks",
                "gate_on_ticks",
                "force_on_ticks",
                "forced_off_ticks",
                "exec_ok_ticks",
                "entry_allowed_ticks",
            ])
        except Exception as e:
            self.write_log(f"gate diag open failed: {e}")
            self._gate_diag_fp = None
            self._gate_diag_writer = None

    def _close_gate_diag_file(self) -> None:
        try:
            if self._gate_diag_fp:
                self._gate_diag_fp.close()
        except Exception:
            pass
        self._gate_diag_fp = None
        self._gate_diag_writer = None

    def _gate_diag_update_tick(self, now: datetime, gate_on: bool, forced_on: bool, exec_ok: bool, entry_allowed: bool) -> None:
        if not self._gate_diag_writer:
            return
        minute = now.replace(second=0, microsecond=0)
        if self._gate_diag_minute is None:
            self._gate_diag_minute = minute
        if minute != self._gate_diag_minute:
            self._flush_gate_diag_minute(force=False)
            self._gate_diag_minute = minute
        c = self._gate_diag_min_counts
        c["ticks"] += 1
        if gate_on:
            c["gate_on_ticks"] += 1
        if forced_on:
            c["force_on_ticks"] += 1
        if exec_ok:
            c["exec_ok_ticks"] += 1
        if entry_allowed:
            c["entry_allowed_ticks"] += 1

    def _flush_gate_diag_minute(self, force: bool) -> None:
        if not self._gate_diag_writer or self._gate_diag_minute is None:
            return
        if (not force) and self._gate_diag_min_counts["ticks"] <= 0:
            return
        c = self._gate_diag_min_counts
        try:
            self._gate_diag_writer.writerow([
                self._gate_diag_minute.isoformat(sep=" ", timespec="minutes"),
                c["ticks"],
                c["gate_on_ticks"],
                c["force_on_ticks"],
                c["forced_off_ticks"],
                c["exec_ok_ticks"],
                c["entry_allowed_ticks"],
            ])
            self._gate_diag_fp.flush()
        except Exception:
            pass
        # reset counts
        for k in c:
            c[k] = 0

    def _summary_out_dir(self) -> str:
        for p in [self.gate_summary_filename, self.gate_bucket_filename, self.opp_filename, self.gate_diag_filename]:
            if p and str(p).strip() and str(p).strip() != os.devnull:
                out_dir = os.path.dirname(str(p))
                if out_dir:
                    return out_dir
        return os.getcwd()

    def _summary_sec_flush(self, force: bool = False) -> None:
        if self._summary_sec_bucket is None:
            return
        self._summary_sec_counts["secs"] += 1
        for k, v in list(self._summary_sec_flags.items()):
            if v:
                self._summary_sec_counts[k] += 1
        if force:
            self._summary_sec_bucket = None
            self._summary_sec_flags = {k: 0 for k in self._summary_sec_flags.keys()}

    def _summary_sec_update(self, now: datetime, **flags: bool) -> None:
        sec = now.replace(microsecond=0)
        if self._summary_sec_bucket is None:
            self._summary_sec_bucket = sec
        elif sec != self._summary_sec_bucket:
            self._summary_sec_flush(force=False)
            self._summary_sec_bucket = sec
            self._summary_sec_flags = {k: 0 for k in self._summary_sec_flags.keys()}

        for k, v in flags.items():
            if not v:
                continue
            kk = k if k in self._summary_sec_flags else f"{k}_secs"
            if kk in self._summary_sec_flags:
                self._summary_sec_flags[kk] = 1


    def _probe_sample_add(self, bucket: str, row: Dict[str, object]) -> None:
        try:
            maxn = max(0, int(self.gate_probe_max_samples))
        except Exception:
            maxn = 50
        arr = self._probe_samples.get(bucket)
        if arr is None:
            return
        if maxn <= 0 or len(arr) >= maxn:
            return
        arr.append(row)

    def _probe_capture(
        self,
        dt: datetime,
        spread_ticks_now: float,
        depth_total_now: float,
        bad_spread: bool,
        bad_depth: bool,
        bad_vol: bool,
        raw_exec_ok: bool,
        exec_ok_entry: bool,
        rearm_block: bool,
        open_entry_block: bool,
        session_entry_block: bool,
        blocked_entry: bool,
        fast_score: float = 0.0,
        eff_spread_max: float = 0.0,
        eff_depth_min: float = 0.0,
        dyn_state: str = "base",
    ) -> None:
        row = {
            "dt": dt.isoformat(sep=" ", timespec="seconds"),
            "range_30s_ticks": float(self.range_30s),
            "vol_30s": float(self.vol_30s),
            "spread_ticks": float(spread_ticks_now),
            "depth_total": float(depth_total_now),
            "gate_on": int(bool(self._gate_on)),
            "force_on": int(bool(self._forced_on_eff)),
            "raw_exec_ok": int(bool(raw_exec_ok)),
            "exec_ok_entry": int(bool(exec_ok_entry)),
            "entry_allowed": int(bool(self.entry_allowed)),
            "blocked_entry": int(bool(blocked_entry)),
            "open_entry_block": int(bool(open_entry_block)),
            "session_entry_block": int(bool(session_entry_block)),
            "rearm_block": int(bool(rearm_block)),
            "bad_spread": int(bool(bad_spread)),
            "bad_depth": int(bool(bad_depth)),
            "bad_vol": int(bool(bad_vol)),
        }
        if self._gate_on and raw_exec_ok:
            self._probe_sample_add("raw_exec_ok_in_gate", row)
        if self._gate_on and exec_ok_entry:
            self._probe_sample_add("exec_ok_in_gate", row)
        if self._gate_on and bool(self.entry_allowed):
            self._probe_sample_add("entry_allowed", row)
        if blocked_entry:
            self._probe_sample_add("blocked_entry", row)
        if self._gate_on and rearm_block:
            self._probe_sample_add("rearm_block_in_gate", row)
        if self._gate_on and bad_spread:
            self._probe_sample_add("bad_spread_in_gate", row)
        if self._gate_on and bad_depth:
            self._probe_sample_add("bad_depth_in_gate", row)
        if self._gate_on and bad_vol:
            self._probe_sample_add("bad_vol_in_gate", row)

    def _build_gate_summary(self) -> Dict[str, float]:
        c = self._keep_diag_counters
        s = self._summary_counts
        ss = self._summary_sec_counts

        total_ticks = int(s.get("ticks", 0))
        gate_on_ticks = int(s.get("gate_on_ticks", 0))
        raw_exec_ok_ticks = int(s.get("raw_exec_ok_ticks", 0))
        raw_exec_ok_in_gate_ticks = int(s.get("raw_exec_ok_in_gate_ticks", 0))
        exec_ok_ticks = int(s.get("exec_ok_ticks", 0))
        exec_ok_in_gate_ticks = int(s.get("exec_ok_in_gate_ticks", 0))
        entry_allowed_ticks = int(s.get("entry_allowed_ticks", 0))
        blocked_entry_ticks = int(s.get("blocked_entry_ticks", 0))
        entry_block_open_ticks = int(s.get("entry_block_open_ticks", 0))
        entry_block_session_ticks = int(s.get("entry_block_session_ticks", 0))
        rearm_block_ticks = int(s.get("rearm_block_ticks", 0))
        rearm_block_in_gate_ticks = int(s.get("rearm_block_in_gate_ticks", 0))
        bad_spread_ticks = int(s.get("bad_spread_ticks", 0))
        bad_spread_in_gate_ticks = int(s.get("bad_spread_in_gate_ticks", 0))
        bad_depth_ticks = int(s.get("bad_depth_ticks", 0))
        bad_depth_in_gate_ticks = int(s.get("bad_depth_in_gate_ticks", 0))
        bad_vol_ticks = int(s.get("bad_vol_ticks", 0))
        bad_vol_in_gate_ticks = int(s.get("bad_vol_in_gate_ticks", 0))

        total_secs = int(ss.get("secs", 0))
        gate_on_secs = int(ss.get("gate_on_secs", 0))
        raw_exec_ok_secs = int(ss.get("raw_exec_ok_secs", 0))
        raw_exec_ok_in_gate_secs = int(ss.get("raw_exec_ok_in_gate_secs", 0))
        exec_ok_secs = int(ss.get("exec_ok_secs", 0))
        exec_ok_in_gate_secs = int(ss.get("exec_ok_in_gate_secs", 0))
        entry_allowed_secs = int(ss.get("entry_allowed_secs", 0))
        blocked_entry_secs = int(ss.get("blocked_entry_secs", 0))
        entry_block_open_secs = int(ss.get("entry_block_open_secs", 0))
        entry_block_session_secs = int(ss.get("entry_block_session_secs", 0))
        rearm_block_secs = int(ss.get("rearm_block_secs", 0))
        rearm_block_in_gate_secs = int(ss.get("rearm_block_in_gate_secs", 0))
        bad_spread_secs = int(ss.get("bad_spread_secs", 0))
        bad_spread_in_gate_secs = int(ss.get("bad_spread_in_gate_secs", 0))
        bad_depth_secs = int(ss.get("bad_depth_secs", 0))
        bad_depth_in_gate_secs = int(ss.get("bad_depth_in_gate_secs", 0))
        bad_vol_secs = int(ss.get("bad_vol_secs", 0))
        bad_vol_in_gate_secs = int(ss.get("bad_vol_in_gate_secs", 0))

        maintain_true_ticks = int(c.get("maintain_true_ticks", 0))
        maintain_false_ticks = int(c.get("maintain_false_ticks", 0))
        maintain_total = maintain_true_ticks + maintain_false_ticks
        maintain_true_ratio = (maintain_true_ticks / maintain_total) if maintain_total > 0 else 0.0

        close_by_keep = int(c.get("close_by_keep", 0))
        close_by_forced = int(c.get("close_by_forced", 0))
        close_by_session_lunch = int(c.get("close_by_session_lunch", 0))
        close_by_session_eod = int(c.get("close_by_session_eod", 0))
        close_by_panic_l1 = int(c.get("close_by_panic_l1", 0))
        close_by_panic_l2 = int(c.get("close_by_panic_l2", 0))

        session_close_count = close_by_session_lunch + close_by_session_eod
        forced_close_count_ex_session = close_by_forced + close_by_panic_l1 + close_by_panic_l2
        keep_close_count_ex_session = close_by_keep
        close_count_ex_session = keep_close_count_ex_session + forced_close_count_ex_session

        forced_close_ratio_ex_session = (forced_close_count_ex_session / close_count_ex_session) if close_count_ex_session > 0 else 0.0
        keep_close_ratio_ex_session = (keep_close_count_ex_session / close_count_ex_session) if close_count_ex_session > 0 else 0.0

        entry_allowed_ratio_when_gate = (entry_allowed_ticks / gate_on_ticks) if gate_on_ticks > 0 else 0.0
        exec_ok_ratio_when_gate = (exec_ok_in_gate_ticks / gate_on_ticks) if gate_on_ticks > 0 else 0.0
        raw_exec_ok_ratio_when_gate = (raw_exec_ok_in_gate_ticks / gate_on_ticks) if gate_on_ticks > 0 else 0.0
        entry_allowed_given_exec_ok = (entry_allowed_ticks / exec_ok_in_gate_ticks) if exec_ok_in_gate_ticks > 0 else 0.0
        blocked_entry_ratio_given_exec_ok = (blocked_entry_ticks / exec_ok_in_gate_ticks) if exec_ok_in_gate_ticks > 0 else 0.0
        rearm_block_ratio_given_raw_exec_ok = (rearm_block_in_gate_ticks / raw_exec_ok_in_gate_ticks) if raw_exec_ok_in_gate_ticks > 0 else 0.0
        bad_spread_ratio_in_gate = (bad_spread_in_gate_ticks / gate_on_ticks) if gate_on_ticks > 0 else 0.0
        bad_depth_ratio_in_gate = (bad_depth_in_gate_ticks / gate_on_ticks) if gate_on_ticks > 0 else 0.0
        bad_vol_ratio_in_gate = (bad_vol_in_gate_ticks / gate_on_ticks) if gate_on_ticks > 0 else 0.0
        gate_on_ratio = (gate_on_ticks / total_ticks) if total_ticks > 0 else 0.0

        gate_on_ratio_sec = (gate_on_secs / total_secs) if total_secs > 0 else 0.0
        entry_allowed_ratio_when_gate_sec = (entry_allowed_secs / gate_on_secs) if gate_on_secs > 0 else 0.0
        exec_ok_ratio_when_gate_sec = (exec_ok_in_gate_secs / gate_on_secs) if gate_on_secs > 0 else 0.0
        raw_exec_ok_ratio_when_gate_sec = (raw_exec_ok_in_gate_secs / gate_on_secs) if gate_on_secs > 0 else 0.0
        entry_allowed_given_exec_ok_sec = (entry_allowed_secs / exec_ok_in_gate_secs) if exec_ok_in_gate_secs > 0 else 0.0
        blocked_entry_ratio_given_exec_ok_sec = (blocked_entry_secs / exec_ok_in_gate_secs) if exec_ok_in_gate_secs > 0 else 0.0
        rearm_block_ratio_given_raw_exec_ok_sec = (rearm_block_in_gate_secs / raw_exec_ok_in_gate_secs) if raw_exec_ok_in_gate_secs > 0 else 0.0
        bad_spread_ratio_in_gate_sec = (bad_spread_in_gate_secs / gate_on_secs) if gate_on_secs > 0 else 0.0
        bad_depth_ratio_in_gate_sec = (bad_depth_in_gate_secs / gate_on_secs) if gate_on_secs > 0 else 0.0
        bad_vol_ratio_in_gate_sec = (bad_vol_in_gate_secs / gate_on_secs) if gate_on_secs > 0 else 0.0

        min_margin_range = float(self._keep_diag_mins.get("min_margin_range", 0.0))
        min_margin_vol = float(self._keep_diag_mins.get("min_margin_vol", 0.0))
        if min_margin_range > 1e17:
            min_margin_range = 0.0
        if min_margin_vol > 1e17:
            min_margin_vol = 0.0

        return {
            "tag": self._build_tag(),
            "total_ticks": total_ticks,
            "gate_on_ticks": gate_on_ticks,
            "gate_on_ratio": gate_on_ratio,
            "raw_exec_ok_ticks": raw_exec_ok_ticks,
            "raw_exec_ok_in_gate_ticks": raw_exec_ok_in_gate_ticks,
            "exec_ok_ticks": exec_ok_ticks,
            "exec_ok_in_gate_ticks": exec_ok_in_gate_ticks,
            "entry_allowed_ticks": entry_allowed_ticks,
            "blocked_entry_ticks": blocked_entry_ticks,
            "entry_block_open_ticks": entry_block_open_ticks,
            "entry_block_session_ticks": entry_block_session_ticks,
            "rearm_block_ticks": rearm_block_ticks,
            "rearm_block_in_gate_ticks": rearm_block_in_gate_ticks,
            "bad_spread_ticks": bad_spread_ticks,
            "bad_spread_in_gate_ticks": bad_spread_in_gate_ticks,
            "bad_depth_ticks": bad_depth_ticks,
            "bad_depth_in_gate_ticks": bad_depth_in_gate_ticks,
            "bad_vol_ticks": bad_vol_ticks,
            "bad_vol_in_gate_ticks": bad_vol_in_gate_ticks,
            "entry_allowed_ratio_when_gate": entry_allowed_ratio_when_gate,
            "exec_ok_ratio_when_gate": exec_ok_ratio_when_gate,
            "raw_exec_ok_ratio_when_gate": raw_exec_ok_ratio_when_gate,
            "entry_allowed_given_exec_ok": entry_allowed_given_exec_ok,
            "blocked_entry_ratio_given_exec_ok": blocked_entry_ratio_given_exec_ok,
            "rearm_block_ratio_given_raw_exec_ok": rearm_block_ratio_given_raw_exec_ok,
            "bad_spread_ratio_in_gate": bad_spread_ratio_in_gate,
            "bad_depth_ratio_in_gate": bad_depth_ratio_in_gate,
            "bad_vol_ratio_in_gate": bad_vol_ratio_in_gate,
            "total_secs": total_secs,
            "gate_on_secs": gate_on_secs,
            "gate_on_ratio_sec": gate_on_ratio_sec,
            "raw_exec_ok_secs": raw_exec_ok_secs,
            "raw_exec_ok_in_gate_secs": raw_exec_ok_in_gate_secs,
            "exec_ok_secs": exec_ok_secs,
            "exec_ok_in_gate_secs": exec_ok_in_gate_secs,
            "entry_allowed_secs": entry_allowed_secs,
            "blocked_entry_secs": blocked_entry_secs,
            "entry_block_open_secs": entry_block_open_secs,
            "entry_block_session_secs": entry_block_session_secs,
            "rearm_block_secs": rearm_block_secs,
            "rearm_block_in_gate_secs": rearm_block_in_gate_secs,
            "bad_spread_secs": bad_spread_secs,
            "bad_spread_in_gate_secs": bad_spread_in_gate_secs,
            "bad_depth_secs": bad_depth_secs,
            "bad_depth_in_gate_secs": bad_depth_in_gate_secs,
            "bad_vol_secs": bad_vol_secs,
            "bad_vol_in_gate_secs": bad_vol_in_gate_secs,
            "entry_allowed_ratio_when_gate_sec": entry_allowed_ratio_when_gate_sec,
            "exec_ok_ratio_when_gate_sec": exec_ok_ratio_when_gate_sec,
            "raw_exec_ok_ratio_when_gate_sec": raw_exec_ok_ratio_when_gate_sec,
            "entry_allowed_given_exec_ok_sec": entry_allowed_given_exec_ok_sec,
            "blocked_entry_ratio_given_exec_ok_sec": blocked_entry_ratio_given_exec_ok_sec,
            "rearm_block_ratio_given_raw_exec_ok_sec": rearm_block_ratio_given_raw_exec_ok_sec,
            "bad_spread_ratio_in_gate_sec": bad_spread_ratio_in_gate_sec,
            "bad_depth_ratio_in_gate_sec": bad_depth_ratio_in_gate_sec,
            "bad_vol_ratio_in_gate_sec": bad_vol_ratio_in_gate_sec,
            "exec_dynamic_enable": int(self.exec_dynamic_enable),
            "exec_dynamic_state_mode": "TIER3",
            "exec_dynamic_opp_follow_main": int(self.exec_dynamic_opp_follow_main),
            "exec_fast_score_mean": (float(self._summary_float_sums.get("fast_score_sum", 0.0)) / total_ticks) if total_ticks > 0 else 0.0,
            "exec_fast_score_mean_in_gate": (float(self._summary_float_sums.get("fast_score_in_gate_sum", 0.0)) / gate_on_ticks) if gate_on_ticks > 0 else 0.0,
            "exec_eff_spread_mean": (float(self._summary_float_sums.get("eff_spread_sum", 0.0)) / total_ticks) if total_ticks > 0 else 0.0,
            "exec_eff_spread_mean_in_gate": (float(self._summary_float_sums.get("eff_spread_in_gate_sum", 0.0)) / gate_on_ticks) if gate_on_ticks > 0 else 0.0,
            "exec_eff_depth_mean": (float(self._summary_float_sums.get("eff_depth_sum", 0.0)) / total_ticks) if total_ticks > 0 else 0.0,
            "exec_eff_depth_mean_in_gate": (float(self._summary_float_sums.get("eff_depth_in_gate_sum", 0.0)) / gate_on_ticks) if gate_on_ticks > 0 else 0.0,
            "dyn_state_base_ratio": (int(s.get("dyn_state_base_ticks", 0)) / total_ticks) if total_ticks > 0 else 0.0,
            "dyn_state_fast_ratio": (int(s.get("dyn_state_fast_ticks", 0)) / total_ticks) if total_ticks > 0 else 0.0,
            "dyn_state_burst_ratio": (int(s.get("dyn_state_burst_ticks", 0)) / total_ticks) if total_ticks > 0 else 0.0,
            "dyn_state_base_ratio_in_gate": (int(s.get("dyn_state_base_in_gate_ticks", 0)) / gate_on_ticks) if gate_on_ticks > 0 else 0.0,
            "dyn_state_fast_ratio_in_gate": (int(s.get("dyn_state_fast_in_gate_ticks", 0)) / gate_on_ticks) if gate_on_ticks > 0 else 0.0,
            "dyn_state_burst_ratio_in_gate": (int(s.get("dyn_state_burst_in_gate_ticks", 0)) / gate_on_ticks) if gate_on_ticks > 0 else 0.0,
            "dyn_relaxed_ratio": (int(s.get("dyn_relaxed_ticks", 0)) / total_ticks) if total_ticks > 0 else 0.0,
            "dyn_relaxed_ratio_in_gate": (int(s.get("dyn_relaxed_in_gate_ticks", 0)) / gate_on_ticks) if gate_on_ticks > 0 else 0.0,
            "dyn_relaxed_ratio_sec": (int(ss.get("dyn_relaxed_secs", 0)) / total_secs) if total_secs > 0 else 0.0,
            "dyn_relaxed_ratio_in_gate_sec": (int(ss.get("dyn_relaxed_in_gate_secs", 0)) / gate_on_secs) if gate_on_secs > 0 else 0.0,
            "gate_open_events": int(c.get("gate_open_events", 0)),
            "gate_close_events": int(c.get("gate_close_events", 0)),
            "close_by_keep": close_by_keep,
            "close_by_forced": close_by_forced,
            "close_by_session_lunch": close_by_session_lunch,
            "close_by_session_eod": close_by_session_eod,
            "close_by_panic_l1": close_by_panic_l1,
            "close_by_panic_l2": close_by_panic_l2,
            "session_close_count": session_close_count,
            "forced_close_count_ex_session": forced_close_count_ex_session,
            "keep_close_count_ex_session": keep_close_count_ex_session,
            "forced_close_ratio_ex_session": forced_close_ratio_ex_session,
            "keep_close_ratio_ex_session": keep_close_ratio_ex_session,
            "maintain_true_ratio": maintain_true_ratio,
            "force_on_ticks": int(s.get("force_on_ticks", 0)),
            "force_on_secs": int(ss.get("force_on_secs", 0)),
            "min_margin_range": min_margin_range,
            "min_margin_vol": min_margin_vol,
        }
    def _write_gate_summary(self) -> None:
        try:
            fname = str(self.gate_summary_filename or "").strip()
            if not fname:
                out_dir = self._summary_out_dir()
                try:
                    os.makedirs(out_dir, exist_ok=True)
                except Exception:
                    pass
                fname = os.path.join(out_dir, f"gate_summary_{self._build_tag()}.csv")
            elif fname == os.devnull:
                return
            else:
                try:
                    os.makedirs(os.path.dirname(fname), exist_ok=True)
                except Exception:
                    pass

            summary = self._build_gate_summary()
            fieldnames = list(summary.keys())
            with open(fname, "w", newline="", encoding="utf-8") as fp:
                w = csv.DictWriter(fp, fieldnames=fieldnames)
                w.writeheader()
                w.writerow(summary)
            self.write_log(f"gate summary saved: {fname}")
        except Exception as e:
            self.write_log(f"gate summary save failed: {e}")

    # =========================
    # outputs
    # =========================

    def _write_gate_probe(self) -> None:
        try:
            fname = str(self.gate_probe_filename or "").strip()
            if not fname or fname == os.devnull:
                out_dir = self._summary_out_dir()
                os.makedirs(out_dir, exist_ok=True)
                fname = os.path.join(out_dir, f"gate_probe_{self._build_tag()}.json")
            else:
                out_dir = os.path.dirname(fname)
                if out_dir:
                    os.makedirs(out_dir, exist_ok=True)

            summary = self._build_gate_summary()
            payload = {
                "tag": self._build_tag(),
                "strategy": "GateModuleStrategy_V09",
                "summary": summary,
                "consistency": {
                    "opp_summary_exec_mismatch": int((float(summary.get("gate_exec_ok_starts", 0)) > 0) and (int(summary.get("exec_ok_in_gate_ticks", 0)) == 0)),
                    "raw_vs_exec_mismatch": int((int(summary.get("raw_exec_ok_in_gate_ticks", 0)) > 0) and (int(summary.get("exec_ok_in_gate_ticks", 0)) == 0)),
                    "entry_zero_given_exec": int((int(summary.get("exec_ok_in_gate_ticks", 0)) > 0) and (int(summary.get("entry_allowed_ticks", 0)) == 0)),
                    "sec_zero_given_tick_positive": int((int(summary.get("exec_ok_in_gate_ticks", 0)) > 0) and (int(summary.get("exec_ok_in_gate_secs", 0)) == 0)),
                },
                "samples": self._probe_samples,
            }
            with open(fname, "w", encoding="utf-8") as fp:
                json.dump(payload, fp, ensure_ascii=False, indent=2)
            self.write_log(f"gate probe saved: {fname}")
        except Exception as e:
            self.write_log(f"gate probe save failed: {e}")

    def _write_gate_bucket_summary(self) -> None:
        fname = str(self.gate_bucket_filename or "").strip() or "gate_bucket.csv"
        if fname == os.devnull:
            return
        try:
            try:
                os.makedirs(os.path.dirname(fname), exist_ok=True)
            except Exception:
                pass
            with open(fname, "w", newline="", encoding="utf-8") as fp:
                w = csv.writer(fp)
                w.writerow([
                    "range_bucket",
                    "range_low",
                    "range_high",
                    "ticks",
                    "gate_on_ticks",
                    "gate_on_ratio",
                    "force_on_ticks",
                    "force_on_ratio",
                    "forced_off_ticks",
                ])
                for i in range(len(self._bucket_total)):
                    total = int(self._bucket_total[i])
                    on = int(self._bucket_gate_on[i])
                    fon = int(self._bucket_force_on[i])
                    foff = int(self._bucket_forced_off[i])
                    onr = on / total if total else 0.0
                    fonr = fon / total if total else 0.0
                    lo = float(self._range_bucket_edges[i])
                    hi = float(self._range_bucket_edges[i + 1])
                    label = f"[{lo},{hi})"
                    w.writerow([label, f"{lo:.4f}", f"{hi:.4f}", total, on, f"{onr:.4f}", fon, f"{fonr:.4f}", foff])
            self.write_log(f"gate bucket saved: {fname}")
        except Exception as e:
            self.write_log(f"gate bucket save failed: {e}")

    def _opp_update_tick(self, dt: datetime, price: float, gate_on: bool, exec_ok: bool, forced_on_eff: bool) -> None:
        """Stream labeling: once (dt - dt0) >= H, label forward-range for tick0."""
        if not self._opp_windows:
            return
        pt = float(self.pricetick)
        thresholds = self._opp_thresholds

        for H, st in self._opp_windows.items():
            st.ticks.append((dt, float(price), bool(gate_on), bool(exec_ok), bool(forced_on_eff)))

            p = float(price)
            while st.maxq and st.maxq[-1][1] <= p:
                st.maxq.pop()
            st.maxq.append((dt, p))
            while st.minq and st.minq[-1][1] >= p:
                st.minq.pop()
            st.minq.append((dt, p))

            while st.ticks:
                _tick0 = st.ticks[0]
                if len(_tick0) < 5:
                    break
                dt0 = _tick0[0]
                p0 = _tick0[1]
                g0 = _tick0[2]
                e0 = _tick0[3]
                f0 = _tick0[4]
                if (dt - dt0).total_seconds() < float(H):
                    break

                if not st.maxq or not st.minq:
                    break
                rng_ticks = (float(st.maxq[0][1]) - float(st.minq[0][1])) / pt if pt > 0 else 0.0

                st.total += 1
                if g0:
                    st.gate += 1
                    if e0:
                        st.gate_exec += 1
                if f0:
                    st.forced += 1

                for r in thresholds:
                    if rng_ticks >= float(r):
                        st.opp_total[r] += 1
                        if g0:
                            st.opp_gate[r] += 1
                            if e0:
                                st.opp_gate_exec[r] += 1
                        if f0:
                            st.opp_forced[r] += 1

                _old_tick = st.ticks.popleft()
                old_dt = _old_tick[0] if len(_old_tick) else None
                if st.maxq and st.maxq[0][0] == old_dt:
                    st.maxq.popleft()
                if st.minq and st.minq[0][0] == old_dt:
                    st.minq.popleft()

    def _write_opp_summary(self) -> None:
        if not self._opp_windows:
            return
        fname = str(self.opp_filename or "").strip() or "gate_opp.csv"
        if fname == os.devnull:
            return
        try:
            try:
                os.makedirs(os.path.dirname(fname), exist_ok=True)
            except Exception:
                pass
            with open(fname, "w", newline="", encoding="utf-8") as fp:
                w = csv.writer(fp)
                w.writerow([
                    "horizon_s",
                    "threshold_ticks",
                    "total_starts",
                    "opp_total",
                    "p0",
                    "gate_starts",
                    "opp_gate",
                    "p1",
                    "lift",
                    "gate_open_ratio",
                    "gate_exec_ok_starts",
                    "opp_gate_exec",
                    "p1_exec",
                    "lift_exec",
                    "exec_ok_ratio_in_gate",
                    "forced_starts",
                    "opp_forced",
                    "p_forced",
                    "forced_ratio",
                    "opp_exec_spread_max_ticks",
                    "opp_exec_depth_min",
                ])

                for H in sorted(self._opp_windows.keys()):
                    st = self._opp_windows[H]
                    total = int(st.total)
                    gate = int(st.gate)
                    gate_exec = int(st.gate_exec)
                    forced = int(st.forced)

                    gate_open_ratio = (gate / total) if total else 0.0
                    exec_ok_ratio = (gate_exec / gate) if gate else 0.0
                    forced_ratio = (forced / total) if total else 0.0

                    for r in self._opp_thresholds:
                        opp_total = int(st.opp_total.get(r, 0))
                        opp_gate = int(st.opp_gate.get(r, 0))
                        opp_gate_exec = int(st.opp_gate_exec.get(r, 0))
                        opp_forced = int(st.opp_forced.get(r, 0))

                        p0 = (opp_total / total) if total else 0.0
                        p1 = (opp_gate / gate) if gate else 0.0
                        lift = (p1 / p0) if (p0 > 0) else 0.0

                        p1_exec = (opp_gate_exec / gate_exec) if gate_exec else 0.0
                        lift_exec = (p1_exec / p0) if (p0 > 0) else 0.0

                        p_forced = (opp_forced / forced) if forced else 0.0

                        w.writerow([
                            int(H),
                            float(r),
                            total,
                            opp_total,
                            f"{p0:.6f}",
                            gate,
                            opp_gate,
                            f"{p1:.6f}",
                            f"{lift:.4f}",
                            f"{gate_open_ratio:.4f}",
                            gate_exec,
                            opp_gate_exec,
                            f"{p1_exec:.6f}",
                            f"{lift_exec:.4f}",
                            f"{exec_ok_ratio:.4f}",
                            forced,
                            opp_forced,
                            f"{p_forced:.6f}",
                            f"{forced_ratio:.4f}",
                            f"{float(self._opp_exec_spread_max):.4f}",
                            f"{float(self._opp_exec_depth_min):.4f}",
                        ])
            self.write_log(f"gate opp saved: {fname}")
        except Exception as e:
            self.write_log(f"gate opp save failed: {e}")

    # =========================
    # keep diag json output
    # =========================
    def _build_tag(self) -> str:
        def f(x) -> str:
            s = str(x)
            return s.replace(".", "p")
        tag = f"R{int(float(self.range_on_30s_ticks))}_V{int(float(self.vol_on_30s))}"
        if int(self.exec_dynamic_enable):
            tag += f"_DY1_{str(self.exec_dyn_trigger_mode).upper()}"
            tag += f"_SB{f(self.exec_dyn_spread_base)}-{f(self.exec_dyn_spread_fast)}-{f(self.exec_dyn_spread_burst)}"
            tag += f"_DB{f(self.exec_dyn_depth_base)}-{f(self.exec_dyn_depth_fast)}-{f(self.exec_dyn_depth_burst)}"
        else:
            tag += f"_DY0_S{int(float(self.spread_max_ticks))}_D{int(float(self.depth_min))}"
        tag += f"_VM{int(float(self.vol_min_30s))}"
        tag += f"_RK{f(self.range_keep_ratio)}_VK{f(self.vol_keep_ratio)}"
        tag += f"_OM{str(self.gate_open_mode).upper()}_CS{f(self.gate_off_confirm_scale)}"
        tag += f"_HB{int(self.gate_confirm_base_s)}_CD{int(self.gate_cooldown_s)}"
        tag += f"_MH{int(self.gate_min_hold_s1)}-{int(self.gate_min_hold_s2)}-{int(self.gate_min_hold_s3)}"
        return tag

    def _keep_diag_dump(self) -> None:
        if not int(self.save_keep_diag):
            return
        try:
            # Prefer to place JSON next to bucket/opp outputs if possible
            out_dir = ""
            for p in [self.gate_bucket_filename, self.opp_filename, self.gate_diag_filename]:
                if p and str(p).strip() and str(p).strip() != os.devnull:
                    out_dir = os.path.dirname(str(p))
                    break
            if not out_dir:
                out_dir = os.getcwd()
            try:
                os.makedirs(out_dir, exist_ok=True)
            except Exception:
                pass

            tag = self._build_tag()
            fname = os.path.join(out_dir, f"gate_keep_diag_{tag}.json")

            payload = {
                "tag": tag,
                "generated_at": datetime.now().isoformat(sep=" ", timespec="seconds"),
                "parameters": {
                    "range_on_30s_ticks": float(self.range_on_30s_ticks),
                    "vol_on_30s": float(self.vol_on_30s),
                    "gate_open_mode": str(self.gate_open_mode).upper(),
                    "range_keep_ratio": float(self.range_keep_ratio),
                    "vol_keep_ratio": float(self.vol_keep_ratio),
                    "gate_confirm_mode": str(self.gate_confirm_mode).upper(),
                    "gate_confirm_base_s": int(self.gate_confirm_base_s),
                    "gate_off_confirm_scale": float(self.gate_off_confirm_scale),
                    "gate_cooldown_s": int(self.gate_cooldown_s),
                    "gate_min_hold_s1": int(self.gate_min_hold_s1),
                    "gate_min_hold_s2": int(self.gate_min_hold_s2),
                    "gate_min_hold_s3": int(self.gate_min_hold_s3),
                    "gate_s_thresh1": float(self.gate_s_thresh1),
                    "gate_s_thresh2": float(self.gate_s_thresh2),
                    "spread_max_ticks": float(self.spread_max_ticks),
                    "depth_min": float(self.depth_min),
                    "vol_min_30s": float(self.vol_min_30s),
                    "exec_use_vol_min_30s": int(self.exec_use_vol_min_30s),
                    "exec_bad_debounce_s": int(self.exec_bad_debounce_s),
                    "exec_rearm_s": int(self.exec_rearm_s),
                    "exec_dynamic_enable": int(self.exec_dynamic_enable),
                    "exec_dynamic_opp_follow_main": int(self.exec_dynamic_opp_follow_main),
                    "exec_dyn_score_w_range": float(self.exec_dyn_score_w_range),
                    "exec_dyn_score_w_vol": float(self.exec_dyn_score_w_vol),
                    "exec_dyn_trigger_mode": str(self.exec_dyn_trigger_mode),
                    "exec_dyn_fast_thresh": float(self.exec_dyn_fast_thresh),
                    "exec_dyn_burst_thresh": float(self.exec_dyn_burst_thresh),
                    "exec_dyn_fast_range_mult": float(self.exec_dyn_fast_range_mult),
                    "exec_dyn_fast_vol_mult": float(self.exec_dyn_fast_vol_mult),
                    "exec_dyn_burst_range_mult": float(self.exec_dyn_burst_range_mult),
                    "exec_dyn_burst_vol_mult": float(self.exec_dyn_burst_vol_mult),
                    "exec_dyn_spread_base": float(self.exec_dyn_spread_base),
                    "exec_dyn_spread_fast": float(self.exec_dyn_spread_fast),
                    "exec_dyn_spread_burst": float(self.exec_dyn_spread_burst),
                    "exec_dyn_depth_base": float(self.exec_dyn_depth_base),
                    "exec_dyn_depth_fast": float(self.exec_dyn_depth_fast),
                    "exec_dyn_depth_burst": float(self.exec_dyn_depth_burst),
                    "exec_dyn_spread_cap_hard": float(self.exec_dyn_spread_cap_hard),
                    "exec_dyn_depth_floor_hard": float(self.exec_dyn_depth_floor_hard),
                },
                "counters": dict(self._keep_diag_counters),
                "mins": dict(self._keep_diag_mins),
                "summary": self._build_gate_summary(),
            }
            if int(self.gate_diag_max_transitions) > 0:
                payload["transitions"] = list(self._keep_diag_transitions)

            with open(fname, "w", encoding="utf-8") as fp:
                json.dump(payload, fp, ensure_ascii=False, indent=2)

            self.write_log(f"keep diag saved: {fname}")
        except Exception as e:
            self.write_log(f"keep diag save failed: {e}")
