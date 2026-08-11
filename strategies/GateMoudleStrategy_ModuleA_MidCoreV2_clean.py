
from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, List, Optional, Tuple
import csv
import json
import os

import pandas as pd

from vnpy_ctastrategy import TickData, TradeData, OrderData, StopOrder, BarData

try:
    from .GateMoudleStrategy_V09 import GateMoudleStrategy_V09, _safe_float
except Exception as e:
    from GateMoudleStrategy_V09 import GateMoudleStrategy_V09, _safe_float


def _parse_float_list_str(s: str) -> List[float]:
    out: List[float] = []
    for part in str(s).replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(float(part))
    return out


def _parse_int_list_str(s: str) -> List[int]:
    out: List[int] = []
    for part in str(s).replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(float(part)))
    return out


class GateMoudleStrategy_ModuleA_PurePrice_EdgeLifetime(GateMoudleStrategy_V09):
    """
    Module A Pure Price Edge Lifetime Study
    --------------------------------
    不下单，只记录纯价格 E0 事件，并在事件结束后一次性产出三层研究所需字段：

    1) PriceShapeStudy
       - 记录触发时价格形状特征：move/retrace/consistency/flow 等
       - 记录未来 1/2/3/5/8 秒的 mid / taker-now 路径

    2) PriceEntryMicroStudy
       - 同一事件下比较三种进场：
         a) taker_now
         b) 1tick pullback rejoin
         c) 1tick micro confirm

    3) PriceExitCounterfactualStudy
       - 以 taker_now 为统一基线，离线比较：
         a) time stop
         b) stall
         c) give-back
         d) reversal

    说明：
    - 只做纯价格 E0，不再做 eff entry 过滤
    - eff / micro / flow 特征仍保留为 sidecar diagnostics，便于后续复盘
    """

    author: str = "DF Quant"

    # 基本：只做诊断，不下单
    gate_diag_only: int = 0
    gate_diag_enable: int = 0
    opp_enable: int = 0
    save_keep_diag: int = 0

    module_event_study_enable: int = 1
    gate_preset_name: str = "ATTACK"
    segment_label: str = "2024-01"

    # A 价格主信号
    module_impulse_s: int = 3
    module_breakout_s: int = 4
    module_move_thresh_ticks: float = 2.0
    module_breakout_buffer_ticks: float = 0.0
    module_retrace_max: float = 0.40

    # A 特征计算
    module_flow_s: int = 2
    module_micro_s: int = 2
    module_min_aggr_vol_norm: float = 0.20
    module_event_buffer_s: int = 18
    module_rank_lookback_n: int = 400

    # study 参数
    event_horizons_s: str = "1,2,3,5,8,13,21,30,38,45"
    event_rearm_s: int = 2
    study_max_path_s: int = 45

    # entry variants
    entry_pullback_ticks: float = 1.0
    entry_confirm_ticks: float = 1.0

    # exit variants
    exit_time_holds_s: str = "3,5,8,13,21,30,38,45"
    exit_stall_windows_s: str = "2,3,4"
    exit_stall_advances_ticks: str = "0,1"
    exit_giveback_ticks: str = "1,2"
    exit_reversal_ticks: str = "1,2"

    # 输出
    output_event_csv: str = ""
    output_meta_json: str = ""

    parameters: List[str] = GateMoudleStrategy_V09.parameters + [
        "module_event_study_enable",
        "gate_preset_name",
        "segment_label",
        "module_impulse_s",
        "module_breakout_s",
        "module_move_thresh_ticks",
        "module_breakout_buffer_ticks",
        "module_retrace_max",
        "module_flow_s",
        "module_micro_s",
        "module_min_aggr_vol_norm",
        "module_event_buffer_s",
        "module_rank_lookback_n",
        "event_horizons_s",
        "event_rearm_s",
        "study_max_path_s",
        "entry_pullback_ticks",
        "entry_confirm_ticks",
        "exit_time_holds_s",
        "exit_stall_windows_s",
        "exit_stall_advances_ticks",
        "exit_giveback_ticks",
        "exit_reversal_ticks",
        "output_event_csv",
        "output_meta_json",
    ]

    variables: List[str] = GateMoudleStrategy_V09.variables + ["event_total_count"]

    event_total_count: int = 0

    def __init__(self, cta_engine, strategy_name: str, vt_symbol: str, setting: dict):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        # feature buffers
        self._prev_bid: float = 0.0
        self._prev_ask: float = 0.0
        self._prev_last: float = 0.0
        self._prev_volume_for_module: Optional[float] = None
        self._mid_events: Deque[Tuple[datetime, float]] = deque()
        self._micro_events: Deque[Tuple[datetime, float]] = deque()
        self._aggr_events: Deque[Tuple[datetime, float, float]] = deque()
        hist_len = max(50, int(self.module_rank_lookback_n))
        self._impact_eff_hist: Deque[float] = deque(maxlen=hist_len)
        self._micro_drift_hist: Deque[float] = deque(maxlen=hist_len)

        # study configs
        self._horizons: List[int] = sorted(set(max(1, int(x)) for x in _parse_int_list_str(self.event_horizons_s)))
        self._max_horizon_s: int = max(self._horizons) if self._horizons else 8
        self._time_holds: List[int] = sorted(set(max(1, int(x)) for x in _parse_int_list_str(self.exit_time_holds_s)))
        self._stall_windows: List[int] = sorted(set(max(1, int(x)) for x in _parse_int_list_str(self.exit_stall_windows_s)))
        self._stall_advances: List[float] = sorted(set(float(x) for x in _parse_float_list_str(self.exit_stall_advances_ticks)))
        self._giveback_ticks: List[float] = sorted(set(float(x) for x in _parse_float_list_str(self.exit_giveback_ticks)))
        self._reversal_ticks: List[float] = sorted(set(float(x) for x in _parse_float_list_str(self.exit_reversal_ticks)))

        # state
        self._event_rows: List[Dict[str, object]] = []
        self._active_events: List[Dict[str, object]] = []
        self._last_fire_dt: Dict[int, datetime] = {}
        self._meta_counts: Dict[str, int] = {}

    # -----------------------------
    # lifecycle
    # -----------------------------
    def on_init(self) -> None:
        super().on_init()

    def on_start(self) -> None:
        super().on_start()

    def on_stop(self) -> None:
        self._finalize_all_active_events(force=True)
        self._write_event_records()
        self._write_event_meta()
        super().on_stop()

    # -----------------------------
    # tick callback
    # -----------------------------
    def on_tick(self, tick: TickData) -> None:
        prev_bid = self._prev_bid
        prev_ask = self._prev_ask
        prev_last = self._prev_last
        prev_vol = self._prev_volume_for_module

        bid = _safe_float(getattr(tick, "bid_price_1", 0.0), 0.0)
        ask = _safe_float(getattr(tick, "ask_price_1", 0.0), 0.0)
        last_px = _safe_float(getattr(tick, "last_price", 0.0), 0.0)
        vol = _safe_float(getattr(tick, "volume", 0.0), 0.0)
        dt: datetime = tick.datetime

        # 1) base gate/session/exec update
        super().on_tick(tick)

        # 2) validity
        if not int(self.module_event_study_enable):
            self._cache_prev_refs(bid, ask, last_px, vol)
            return
        if not isinstance(dt, datetime):
            self._cache_prev_refs(bid, ask, last_px, vol)
            return
        if bid <= 0 or ask <= 0 or ask <= bid:
            self._cache_prev_refs(bid, ask, last_px, vol)
            return

        # 3) feature buffers
        mid = 0.5 * (bid + ask)
        micro = self._calc_microprice(tick, bid, ask)
        dvol = 0.0 if prev_vol is None else max(0.0, vol - prev_vol)
        aggr_buy, aggr_sell = self._infer_aggressor_volume(dvol, last_px, prev_last, prev_bid, prev_ask)
        self._update_module_buffers(dt, mid, micro, aggr_buy, aggr_sell)

        # 4) active events path update
        self._update_active_events(dt, bid, ask, mid, micro)

        # 5) only when entry_allowed
        if not bool(self.entry_allowed):
            self._cache_prev_refs(bid, ask, last_px, vol)
            return

        # 6) pure price E0 only
        f = self._feature_pack(dt)
        side = self._price_event_signal(f)
        if side != 0:
            self._maybe_fire_event(dt, bid, ask, mid, micro, f, side)

        self._cache_prev_refs(bid, ask, last_px, vol)

    def on_bar(self, bar: BarData) -> None:
        pass

    def on_trade(self, trade: TradeData) -> None:
        pass

    def on_order(self, order: OrderData) -> None:
        pass

    def on_stop_order(self, stop_order: StopOrder) -> None:
        pass

    # -----------------------------
    # feature utils
    # -----------------------------
    def _cache_prev_refs(self, bid: float, ask: float, last_px: float, vol: float) -> None:
        self._prev_bid = float(bid)
        self._prev_ask = float(ask)
        self._prev_last = float(last_px)
        self._prev_volume_for_module = float(vol)

    def _calc_microprice(self, tick: TickData, bid: float, ask: float) -> float:
        bv = _safe_float(getattr(tick, "bid_volume_1", 0.0), 0.0)
        av = _safe_float(getattr(tick, "ask_volume_1", 0.0), 0.0)
        denom = bv + av
        if denom <= 1e-12:
            return 0.5 * (bid + ask)
        return (ask * bv + bid * av) / denom

    def _infer_aggressor_volume(self, dvol: float, last_px: float, prev_last: float, prev_bid: float, prev_ask: float) -> Tuple[float, float]:
        if dvol <= 0:
            return 0.0, 0.0
        pt = max(float(self.pricetick), 1e-9)
        eps = 0.25 * pt
        if prev_ask > 0 and last_px >= prev_ask - eps:
            return dvol, 0.0
        if prev_bid > 0 and last_px <= prev_bid + eps:
            return 0.0, dvol
        if prev_last > 0:
            if last_px > prev_last + eps:
                return dvol, 0.0
            if last_px < prev_last - eps:
                return 0.0, dvol
        return 0.5 * dvol, 0.5 * dvol

    def _update_module_buffers(self, dt: datetime, mid: float, micro: float, aggr_buy: float, aggr_sell: float) -> None:
        self._mid_events.append((dt, float(mid)))
        self._micro_events.append((dt, float(micro)))
        self._aggr_events.append((dt, float(aggr_buy), float(aggr_sell)))
        keep_s = max(
            20,
            int(self.module_event_buffer_s),
            int(self.module_breakout_s) + 2,
            int(self.module_impulse_s) + 2,
            int(self.module_micro_s) + 2,
            int(self.study_max_path_s) + 2,
        )
        cutoff = dt - timedelta(seconds=keep_s)
        while self._mid_events and self._mid_events[0][0] < cutoff:
            self._mid_events.popleft()
        while self._micro_events and self._micro_events[0][0] < cutoff:
            self._micro_events.popleft()
        while self._aggr_events and self._aggr_events[0][0] < cutoff:
            self._aggr_events.popleft()

    def _slice_values(self, events: Deque[Tuple[datetime, float]], dt: datetime, window_s: int) -> List[float]:
        cutoff = dt - timedelta(seconds=max(1, int(window_s)))
        return [float(v) for t, v in events if t >= cutoff]

    def _slice_aggr(self, dt: datetime, window_s: int) -> Tuple[float, float]:
        cutoff = dt - timedelta(seconds=max(1, int(window_s)))
        buy = 0.0
        sell = 0.0
        for _evt in self._aggr_events:
            if len(_evt) < 3:
                continue
            t = _evt[0]
            if t >= cutoff:
                buy += float(_evt[1])
                sell += float(_evt[2])
        return buy, sell

    @staticmethod
    def _percentile_rank(x: float, hist: Deque[float]) -> float:
        if len(hist) < 10:
            return 0.5
        vals = list(hist)
        le = sum(1 for v in vals if v <= x)
        return le / max(1, len(vals))

    def _feature_pack(self, dt: datetime) -> Dict[str, float]:
        pt = max(float(self.pricetick), 1e-9)
        mids_imp = self._slice_values(self._mid_events, dt, int(self.module_impulse_s))
        mids_brk = self._slice_values(self._mid_events, dt, int(self.module_breakout_s))
        micros = self._slice_values(self._micro_events, dt, int(self.module_micro_s))
        buy_flow, sell_flow = self._slice_aggr(dt, int(self.module_flow_s))

        if len(mids_imp) < 2:
            move_ticks = 0.0
            retrace_ratio = 1.0
            consistency = 0.0
        else:
            move_ticks = (mids_imp[-1] - mids_imp[0]) / pt
            peak = max(mids_imp)
            trough = min(mids_imp)
            if move_ticks >= 0:
                gross = max((peak - mids_imp[0]) / pt, 1e-9)
                retr = max((peak - mids_imp[-1]) / pt, 0.0)
            else:
                gross = max((mids_imp[0] - trough) / pt, 1e-9)
                retr = max((mids_imp[-1] - trough) / pt, 0.0)
            retrace_ratio = retr / gross if gross > 0 else 1.0

            up = 0
            dn = 0
            for i in range(1, len(mids_imp)):
                d = mids_imp[i] - mids_imp[i - 1]
                if d > 1e-12:
                    up += 1
                elif d < -1e-12:
                    dn += 1
            total_dir = up + dn
            if total_dir > 0:
                if move_ticks > 0:
                    consistency = up / total_dir
                elif move_ticks < 0:
                    consistency = dn / total_dir
                else:
                    consistency = 0.0
            else:
                consistency = 0.0

        breakout_up = False
        breakout_dn = False
        if len(mids_brk) >= 2:
            cur = mids_brk[-1]
            hist = mids_brk[:-1]
            buf = float(self.module_breakout_buffer_ticks) * pt
            breakout_up = cur >= (max(hist) + buf)
            breakout_dn = cur <= (min(hist) - buf)

        tot_flow = buy_flow + sell_flow
        aggr_buy_ratio = (buy_flow / tot_flow) if tot_flow > 1e-12 else 0.5
        aggr_sell_ratio = (sell_flow / tot_flow) if tot_flow > 1e-12 else 0.5
        expected_flow = max(1e-9, float(self.vol_on_30s) * max(1, int(self.module_flow_s)) / 30.0)
        aggr_vol_norm = tot_flow / expected_flow

        long_eff = max(move_ticks, 0.0) / max(buy_flow, 1.0)
        short_eff = max(-move_ticks, 0.0) / max(sell_flow, 1.0)
        long_eff_rank = self._percentile_rank(long_eff, self._impact_eff_hist)
        short_eff_rank = self._percentile_rank(short_eff, self._impact_eff_hist)

        micro_move_ticks = ((micros[-1] - micros[0]) / pt) if len(micros) >= 2 else 0.0
        micro_rank = self._percentile_rank(abs(micro_move_ticks), self._micro_drift_hist)

        if long_eff > 0:
            self._impact_eff_hist.append(long_eff)
        if short_eff > 0:
            self._impact_eff_hist.append(short_eff)
        if abs(micro_move_ticks) > 0:
            self._micro_drift_hist.append(abs(micro_move_ticks))

        burst_speed = move_ticks / max(1.0, float(self.module_impulse_s))
        breakout_margin_ticks = 0.0
        if len(mids_brk) >= 2:
            cur = mids_brk[-1]
            hist = mids_brk[:-1]
            if move_ticks >= 0:
                breakout_margin_ticks = (cur - max(hist)) / pt
            else:
                breakout_margin_ticks = (min(hist) - cur) / pt

        return {
            "move_ticks": float(move_ticks),
            "breakout_up": 1.0 if breakout_up else 0.0,
            "breakout_dn": 1.0 if breakout_dn else 0.0,
            "breakout_margin_ticks": float(breakout_margin_ticks),
            "retrace_ratio": float(retrace_ratio),
            "consistency": float(consistency),
            "burst_speed": float(burst_speed),
            "aggr_buy_ratio": float(aggr_buy_ratio),
            "aggr_sell_ratio": float(aggr_sell_ratio),
            "aggr_vol_norm": float(aggr_vol_norm),
            "long_eff_rank": float(long_eff_rank),
            "short_eff_rank": float(short_eff_rank),
            "micro_move_ticks": float(micro_move_ticks),
            "micro_rank": float(micro_rank),
        }

    def _price_event_signal(self, f: Dict[str, float]) -> int:
        long_price_ok = (
            f["move_ticks"] >= float(self.module_move_thresh_ticks)
            and f["breakout_up"] > 0.5
            and f["retrace_ratio"] <= float(self.module_retrace_max)
        )
        short_price_ok = (
            f["move_ticks"] <= -float(self.module_move_thresh_ticks)
            and f["breakout_dn"] > 0.5
            and f["retrace_ratio"] <= float(self.module_retrace_max)
        )
        if long_price_ok:
            return +1
        if short_price_ok:
            return -1
        return 0

    # -----------------------------
    # event mechanics
    # -----------------------------
    def _maybe_fire_event(self, dt: datetime, bid: float, ask: float, mid: float, micro: float,
                          f: Dict[str, float], side: int) -> None:
        last_fire = self._last_fire_dt.get(int(side))
        if last_fire is not None and (dt - last_fire).total_seconds() < float(self.event_rearm_s):
            return
        self._last_fire_dt[int(side)] = dt
        self._meta_counts["E0_PRICE"] = self._meta_counts.get("E0_PRICE", 0) + 1
        self.event_total_count += 1
        self._active_events.append({
            "event_name": "E0_PRICE",
            "gate_preset": str(self.gate_preset_name),
            "segment": str(self.segment_label),
            "side": int(side),
            "dt0": dt,
            "mid0": float(mid),
            "micro0": float(micro),
            "bid0": float(bid),
            "ask0": float(ask),
            "entry_move_ticks": float(f.get("move_ticks", 0.0)),
            "entry_retrace_ratio": float(f.get("retrace_ratio", 1.0)),
            "entry_consistency": float(f.get("consistency", 0.0)),
            "entry_burst_speed": float(f.get("burst_speed", 0.0)),
            "entry_breakout_margin_ticks": float(f.get("breakout_margin_ticks", 0.0)),
            "aggr_buy_ratio": float(f.get("aggr_buy_ratio", 0.5)),
            "aggr_sell_ratio": float(f.get("aggr_sell_ratio", 0.5)),
            "aggr_vol_norm": float(f.get("aggr_vol_norm", 0.0)),
            "long_eff_rank": float(f.get("long_eff_rank", 0.5)),
            "short_eff_rank": float(f.get("short_eff_rank", 0.5)),
            "micro_move_ticks": float(f.get("micro_move_ticks", 0.0)),
            "micro_rank": float(f.get("micro_rank", 0.5)),
            "path": [(0.0, float(mid), float(bid), float(ask), float(micro))],
        })

    def _update_active_events(self, dt: datetime, bid: float, ask: float, mid: float, micro: float) -> None:
        if not self._active_events:
            return
        remain: List[Dict[str, object]] = []
        for ev in self._active_events:
            dt0: datetime = ev["dt0"]
            age = float((dt - dt0).total_seconds())
            path: List[Tuple[float, float, float, float, float]] = ev["path"]
            if age > path[-1][0]:
                path.append((age, float(mid), float(bid), float(ask), float(micro)))
            if age >= float(self.study_max_path_s):
                self._finalize_one_event(ev)
            else:
                remain.append(ev)
        self._active_events = remain

    @staticmethod
    def _first_obs_at_or_after(path: List[Tuple[float, float, float, float, float]], target_age: float) -> Tuple[float, float, float, float, float]:
        for obs in path:
            if obs[0] >= target_age:
                return obs
        return path[-1]

    @staticmethod
    def _ret_from_mid(side: int, px: float, ref_px: float, pt: float) -> float:
        return float(side) * ((float(px) - float(ref_px)) / pt)

    @staticmethod
    def _ret_from_entry(side: int, exit_mid: float, entry_px: float, pt: float) -> float:
        return float(side) * ((float(exit_mid) - float(entry_px)) / pt)

    def _simulate_entry_variant(
        self,
        variant: str,
        path: List[Tuple[float, float, float, float, float]],
        side: int,
        mid0: float,
        pt: float,
    ) -> Dict[str, object]:
        result: Dict[str, object] = {
            "filled": 0,
            "fill_age_s": "",
            "fill_px": "",
        }
        fill_obs: Optional[Tuple[float, float, float, float, float]] = None
        fill_px: Optional[float] = None

        if variant == "taker_now":
            fill_obs = path[0]
            fill_px = float(path[0][3] if side > 0 else path[0][2])
        elif variant == "pullback1t":
            pullback_ticks = float(self.entry_pullback_ticks)
            best_fav = 0.0
            for obs in path[1:]:
                fav = self._ret_from_mid(side, obs[1], mid0, pt)
                if fav > best_fav:
                    best_fav = fav
                if best_fav >= pullback_ticks and fav <= (best_fav - pullback_ticks):
                    fill_obs = obs
                    fill_px = float(obs[3] if side > 0 else obs[2])
                    break
        elif variant == "micro_confirm1t":
            confirm_ticks = float(self.entry_confirm_ticks)
            for obs in path[1:]:
                fav = self._ret_from_mid(side, obs[1], mid0, pt)
                if fav >= confirm_ticks:
                    fill_obs = obs
                    fill_px = float(obs[3] if side > 0 else obs[2])
                    break
        else:
            raise ValueError(f"unknown entry variant: {variant}")

        if fill_obs is None or fill_px is None:
            for h in self._horizons:
                result[f"ret_{h}s"] = ""
            return result

        fill_age = float(fill_obs[0])
        result["filled"] = 1
        result["fill_age_s"] = fill_age
        result["fill_px"] = fill_px
        for h in self._horizons:
            obs_h = self._first_obs_at_or_after(path, fill_age + float(h))
            result[f"ret_{h}s"] = self._ret_from_entry(side, obs_h[1], fill_px, pt)
        return result

    def _simulate_time_stop(
        self,
        path: List[Tuple[float, float, float, float, float]],
        side: int,
        entry_px: float,
        hold_s: int,
        pt: float,
    ) -> Tuple[float, float]:
        obs = self._first_obs_at_or_after(path, float(hold_s))
        age = float(obs[0])
        ret = self._ret_from_entry(side, obs[1], entry_px, pt)
        return ret, age

    def _simulate_stall(
        self,
        path: List[Tuple[float, float, float, float, float]],
        side: int,
        entry_px: float,
        window_s: int,
        advance_ticks: float,
        pt: float,
    ) -> Tuple[float, float]:
        best_fav = float("-inf")
        for i, obs in enumerate(path):
            age = float(obs[0])
            cur_fav = self._ret_from_entry(side, obs[1], entry_px, pt)
            if cur_fav > best_fav:
                best_fav = cur_fav
            if age < float(window_s):
                continue
            if best_fav < max(1.0, float(advance_ticks)):
                continue
            target_age = age - float(window_s)
            old_obs = self._first_obs_at_or_after(path[: i + 1], target_age)
            old_fav = self._ret_from_entry(side, old_obs[1], entry_px, pt)
            if cur_fav <= (old_fav + float(advance_ticks)):
                return cur_fav, age
        last = path[-1]
        return self._ret_from_entry(side, last[1], entry_px, pt), float(last[0])

    def _simulate_giveback(
        self,
        path: List[Tuple[float, float, float, float, float]],
        side: int,
        entry_px: float,
        giveback_ticks: float,
        pt: float,
    ) -> Tuple[float, float]:
        best_fav = float("-inf")
        for obs in path:
            age = float(obs[0])
            cur_fav = self._ret_from_entry(side, obs[1], entry_px, pt)
            if cur_fav > best_fav:
                best_fav = cur_fav
            if best_fav >= float(giveback_ticks) and (best_fav - cur_fav) >= float(giveback_ticks):
                return cur_fav, age
        last = path[-1]
        return self._ret_from_entry(side, last[1], entry_px, pt), float(last[0])

    def _simulate_reversal(
        self,
        path: List[Tuple[float, float, float, float, float]],
        side: int,
        entry_px: float,
        reversal_ticks: float,
        pt: float,
    ) -> Tuple[float, float]:
        achieved_positive = False
        for obs in path:
            age = float(obs[0])
            cur_fav = self._ret_from_entry(side, obs[1], entry_px, pt)
            if cur_fav >= 1.0:
                achieved_positive = True
            if achieved_positive and cur_fav <= -float(reversal_ticks):
                return cur_fav, age
        last = path[-1]
        return self._ret_from_entry(side, last[1], entry_px, pt), float(last[0])

    def _finalize_one_event(self, ev: Dict[str, object]) -> None:
        path: List[Tuple[float, float, float, float, float]] = ev["path"]
        if not path:
            return

        side = int(ev["side"])
        mid0 = float(ev["mid0"])
        bid0 = float(ev["bid0"])
        ask0 = float(ev["ask0"])
        pt = max(float(self.pricetick), 1e-9)

        # canonical event-path diagnostics
        favorable_list: List[float] = [self._ret_from_mid(side, obs[1], mid0, pt) for obs in path]
        adverse_list: List[float] = [max(0.0, -x) for x in favorable_list]
        move_ticks = max(favorable_list) if favorable_list else 0.0
        final_mid_ret = favorable_list[-1] if favorable_list else 0.0
        retrace_ratio = ""
        if move_ticks > 0:
            retrace_ratio = max(0.0, min(1.0, (move_ticks - final_mid_ret) / move_ticks))
        pos_obs_count = sum(1 for x in favorable_list if x > 0)
        obs_count = len(favorable_list)
        consistency = (pos_obs_count / obs_count) if obs_count > 0 else ""
        mfe_ticks = move_ticks
        mae_ticks = max(adverse_list) if adverse_list else 0.0

        first_hit_p1_s = ""
        first_hit_p2_s = ""
        first_hit_n1_s = ""
        first_hit_n2_s = ""
        for obs, fav, adv in zip(path, favorable_list, adverse_list):
            age = float(obs[0])
            if first_hit_p1_s == "" and fav >= 1.0:
                first_hit_p1_s = f"{age:.6f}"
            if first_hit_p2_s == "" and fav >= 2.0:
                first_hit_p2_s = f"{age:.6f}"
            if first_hit_n1_s == "" and adv >= 1.0:
                first_hit_n1_s = f"{age:.6f}"
            if first_hit_n2_s == "" and adv >= 2.0:
                first_hit_n2_s = f"{age:.6f}"

        row: Dict[str, object] = {
            "segment": ev["segment"],
            "gate_preset": ev["gate_preset"],
            "event_name": ev["event_name"],
            "dt0": ev["dt0"].strftime("%Y-%m-%d %H:%M:%S.%f"),
            "side": side,
            "move_ticks": move_ticks,
            "retrace_ratio": retrace_ratio,
            "consistency": consistency,
            "mfe_ticks": mfe_ticks,
            "mae_ticks": mae_ticks,
            "first_hit_p1_s": first_hit_p1_s,
            "first_hit_p2_s": first_hit_p2_s,
            "first_hit_n1_s": first_hit_n1_s,
            "first_hit_n2_s": first_hit_n2_s,
            "entry_move_ticks": ev["entry_move_ticks"],
            "entry_retrace_ratio": ev["entry_retrace_ratio"],
            "entry_consistency": ev["entry_consistency"],
            "entry_burst_speed": ev["entry_burst_speed"],
            "entry_breakout_margin_ticks": ev["entry_breakout_margin_ticks"],
            "aggr_buy_ratio": ev["aggr_buy_ratio"],
            "aggr_sell_ratio": ev["aggr_sell_ratio"],
            "aggr_vol_norm": ev["aggr_vol_norm"],
            "long_eff_rank": ev["long_eff_rank"],
            "short_eff_rank": ev["short_eff_rank"],
            "micro_move_ticks": ev["micro_move_ticks"],
            "micro_rank": ev["micro_rank"],
        }

        # base horizon returns from event time
        taker_now_entry_px = ask0 if side > 0 else bid0
        for h in self._horizons:
            obs_h = self._first_obs_at_or_after(path, float(h))
            row[f"mid_ret_{h}s"] = self._ret_from_mid(side, obs_h[1], mid0, pt)
            row[f"taker_now_ret_{h}s"] = self._ret_from_entry(side, obs_h[1], taker_now_entry_px, pt)

        # entry study
        for variant in ["taker_now", "pullback1t", "micro_confirm1t"]:
            sim = self._simulate_entry_variant(variant, path, side, mid0, pt)
            row[f"{variant}_filled"] = sim["filled"]
            row[f"{variant}_fill_age_s"] = sim["fill_age_s"]
            row[f"{variant}_fill_px"] = sim["fill_px"]
            for h in self._horizons:
                row[f"{variant}_ret_{h}s"] = sim[f"ret_{h}s"]

        # exit study: unified baseline on taker_now entry
        for hold_s in self._time_holds:
            ret, age = self._simulate_time_stop(path, side, taker_now_entry_px, hold_s, pt)
            row[f"exit_tstop_{hold_s}s_ret"] = ret
            row[f"exit_tstop_{hold_s}s_age_s"] = age

        for w in self._stall_windows:
            for a in self._stall_advances:
                suffix = f"w{int(w)}_a{str(a).replace('.', 'p')}"
                ret, age = self._simulate_stall(path, side, taker_now_entry_px, w, a, pt)
                row[f"exit_stall_{suffix}_ret"] = ret
                row[f"exit_stall_{suffix}_age_s"] = age

        for g in self._giveback_ticks:
            suffix = str(g).replace(".", "p")
            ret, age = self._simulate_giveback(path, side, taker_now_entry_px, g, pt)
            row[f"exit_giveback_{suffix}t_ret"] = ret
            row[f"exit_giveback_{suffix}t_age_s"] = age

        for r in self._reversal_ticks:
            suffix = str(r).replace(".", "p")
            ret, age = self._simulate_reversal(path, side, taker_now_entry_px, r, pt)
            row[f"exit_reversal_{suffix}t_ret"] = ret
            row[f"exit_reversal_{suffix}t_age_s"] = age

        self._event_rows.append(row)

    def _finalize_all_active_events(self, force: bool = False) -> None:
        if not self._active_events:
            return
        remain: List[Dict[str, object]] = []
        for ev in self._active_events:
            if force:
                self._finalize_one_event(ev)
            else:
                remain.append(ev)
        self._active_events = remain

    def _write_event_records(self) -> None:
        path = str(self.output_event_csv).strip()
        if not path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

        cols = [
            "segment", "gate_preset", "event_name", "dt0", "side",
            "move_ticks", "retrace_ratio", "consistency", "mfe_ticks", "mae_ticks",
            "first_hit_p1_s", "first_hit_p2_s", "first_hit_n1_s", "first_hit_n2_s",
            "entry_move_ticks", "entry_retrace_ratio", "entry_consistency",
            "entry_burst_speed", "entry_breakout_margin_ticks",
            "aggr_buy_ratio", "aggr_sell_ratio", "aggr_vol_norm",
            "long_eff_rank", "short_eff_rank", "micro_move_ticks", "micro_rank",
        ]
        for h in self._horizons:
            cols.extend([f"mid_ret_{h}s", f"taker_now_ret_{h}s"])
        for variant in ["taker_now", "pullback1t", "micro_confirm1t"]:
            cols.extend([f"{variant}_filled", f"{variant}_fill_age_s", f"{variant}_fill_px"])
            for h in self._horizons:
                cols.append(f"{variant}_ret_{h}s")
        for hold_s in self._time_holds:
            cols.extend([f"exit_tstop_{hold_s}s_ret", f"exit_tstop_{hold_s}s_age_s"])
        for w in self._stall_windows:
            for a in self._stall_advances:
                suffix = f"w{int(w)}_a{str(a).replace('.', 'p')}"
                cols.extend([f"exit_stall_{suffix}_ret", f"exit_stall_{suffix}_age_s"])
        for g in self._giveback_ticks:
            suffix = str(g).replace(".", "p")
            cols.extend([f"exit_giveback_{suffix}t_ret", f"exit_giveback_{suffix}t_age_s"])
        for r in self._reversal_ticks:
            suffix = str(r).replace(".", "p")
            cols.extend([f"exit_reversal_{suffix}t_ret", f"exit_reversal_{suffix}t_age_s"])

        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for row in self._event_rows:
                w.writerow(row)

    def _write_event_meta(self) -> None:
        path = str(self.output_meta_json).strip()
        if not path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        payload = {
            "segment": str(self.segment_label),
            "gate_preset": str(self.gate_preset_name),
            "event_total_count": int(self.event_total_count),
            "event_counts": self._meta_counts,
            "event_horizons_s": self._horizons,
            "study_max_path_s": int(self.study_max_path_s),
            "entry_variants": ["taker_now", "pullback1t", "micro_confirm1t"],
            "exit_time_holds_s": self._time_holds,
            "exit_stall_windows_s": self._stall_windows,
            "exit_stall_advances_ticks": self._stall_advances,
            "exit_giveback_ticks": self._giveback_ticks,
            "exit_reversal_ticks": self._reversal_ticks,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

# --- Liquidity conditioning subclass (self-contained) ---

def _parse_float_list_str_local(s: str) -> List[float]:
    out: List[float] = []
    for part in str(s).replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(float(part))
    return out


class GateMoudleStrategy_ModuleA_LiquidityConditioningStudy(
    GateMoudleStrategy_ModuleA_PurePrice_EdgeLifetime
):
    """
    Module A Liquidity Conditioning Study
    ------------------------------------
    目标：
    1) 保持 A 的主信号仍然是 pure-price E0
    2) 在事件触发时记录更丰富的流动性快照
    3) 输出可用于条件分层的字段：
       - spread_ticks0
       - 五档加权 depth（front-loaded）
       - same/opp side depth ratio / pressure
       - pre-event 主动成交强度与方向
       - MFE / MAE / first-hit / 多 horizon 截面收益

    说明：
    - 不下单，只做事件研究
    - depth 使用五档加权平均量：
      w = [1.0, 0.8, 0.6, 0.4, 0.2]
      这样比单纯 L1 更稳，又保持“越靠近盘口越重要”
    """

    author: str = "DF Quant"

    book_depth_weights: str = "1.0,0.8,0.6,0.4,0.2"

    parameters: List[str] = GateMoudleStrategy_ModuleA_PurePrice_EdgeLifetime.parameters + [
        "book_depth_weights",
    ]
    variables: List[str] = GateMoudleStrategy_ModuleA_PurePrice_EdgeLifetime.variables

    def __init__(self, cta_engine, strategy_name: str, vt_symbol: str, setting: dict):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        weights = _parse_float_list_str_local(self.book_depth_weights)
        if not weights:
            weights = [1.0, 0.8, 0.6, 0.4, 0.2]
        if len(weights) < 5:
            last = weights[-1]
            weights = weights + [last] * (5 - len(weights))
        self._book_depth_weights: List[float] = [float(max(0.0, w)) for w in weights[:5]]
        self._last_book_snapshot: Optional[Dict[str, float]] = None

    def on_tick(self, tick: TickData) -> None:
        prev_bid = self._prev_bid
        prev_ask = self._prev_ask
        prev_last = self._prev_last
        prev_vol = self._prev_volume_for_module

        bid = _safe_float(getattr(tick, "bid_price_1", 0.0), 0.0)
        ask = _safe_float(getattr(tick, "ask_price_1", 0.0), 0.0)
        last_px = _safe_float(getattr(tick, "last_price", 0.0), 0.0)
        vol = _safe_float(getattr(tick, "volume", 0.0), 0.0)
        dt: datetime = tick.datetime

        super(GateMoudleStrategy_ModuleA_PurePrice_EdgeLifetime, self).on_tick(tick)

        if not int(self.module_event_study_enable):
            self._cache_prev_refs(bid, ask, last_px, vol)
            return
        if not isinstance(dt, datetime):
            self._cache_prev_refs(bid, ask, last_px, vol)
            return
        if bid <= 0 or ask <= 0 or ask <= bid:
            self._cache_prev_refs(bid, ask, last_px, vol)
            return

        mid = 0.5 * (bid + ask)
        micro = self._calc_microprice(tick, bid, ask)
        dvol = 0.0 if prev_vol is None else max(0.0, vol - prev_vol)
        aggr_buy, aggr_sell = self._infer_aggressor_volume(dvol, last_px, prev_last, prev_bid, prev_ask)
        self._update_module_buffers(dt, mid, micro, aggr_buy, aggr_sell)

        self._update_active_events(dt, bid, ask, mid, micro)

        if not bool(self.entry_allowed):
            self._cache_prev_refs(bid, ask, last_px, vol)
            return

        f = self._feature_pack(dt)
        side = self._price_event_signal(f)
        if side != 0:
            self._last_book_snapshot = self._extract_book_snapshot(tick, bid, ask)
            self._maybe_fire_event(dt, bid, ask, mid, micro, f, side)

        self._cache_prev_refs(bid, ask, last_px, vol)

    def on_bar(self, bar: BarData) -> None:
        pass

    def on_trade(self, trade: TradeData) -> None:
        pass

    def on_order(self, order: OrderData) -> None:
        pass

    def on_stop_order(self, stop_order: StopOrder) -> None:
        pass

    def _feature_pack(self, dt: datetime) -> Dict[str, float]:
        f = super()._feature_pack(dt)
        buy_flow, sell_flow = self._slice_aggr(dt, int(self.module_flow_s))
        tot = float(buy_flow + sell_flow)
        imbalance = ((buy_flow - sell_flow) / tot) if tot > 1e-12 else 0.0
        f["pre_aggr_total"] = tot
        f["pre_aggr_imbalance"] = float(imbalance)
        return f

    def _weighted_depth5(self, tick: TickData, side_prefix: str) -> float:
        weights = self._book_depth_weights
        num = 0.0
        den = 0.0
        for i in range(1, 6):
            w = float(weights[i - 1])
            vol = _safe_float(getattr(tick, f"{side_prefix}_volume_{i}", 0.0), 0.0)
            vol = max(0.0, float(vol))
            num += w * vol
            den += w
        if den <= 1e-12:
            return 0.0
        return num / den

    def _extract_book_snapshot(self, tick: TickData, bid: float, ask: float) -> Dict[str, float]:
        pt = max(float(self.pricetick), 1e-9)
        bid_w5 = self._weighted_depth5(tick, "bid")
        ask_w5 = self._weighted_depth5(tick, "ask")
        bid1 = max(0.0, _safe_float(getattr(tick, "bid_volume_1", 0.0), 0.0))
        ask1 = max(0.0, _safe_float(getattr(tick, "ask_volume_1", 0.0), 0.0))
        total = bid_w5 + ask_w5
        return {
            "spread_ticks0": max(0.0, (float(ask) - float(bid)) / pt),
            "bid_wdepth5": float(bid_w5),
            "ask_wdepth5": float(ask_w5),
            "bid_vol_1": float(bid1),
            "ask_vol_1": float(ask1),
            "book_imbalance_w5": float((bid_w5 - ask_w5) / total) if total > 1e-12 else 0.0,
        }

    def _maybe_fire_event(self, dt: datetime, bid: float, ask: float, mid: float, micro: float,
                          f: Dict[str, float], side: int) -> None:
        last_fire = self._last_fire_dt.get(int(side))
        if last_fire is not None and (dt - last_fire).total_seconds() < float(self.event_rearm_s):
            return

        book = self._last_book_snapshot or {}
        bid_w5 = float(book.get("bid_wdepth5", 0.0))
        ask_w5 = float(book.get("ask_wdepth5", 0.0))
        bid1 = float(book.get("bid_vol_1", 0.0))
        ask1 = float(book.get("ask_vol_1", 0.0))
        same_w5 = bid_w5 if side > 0 else ask_w5
        opp_w5 = ask_w5 if side > 0 else bid_w5
        same_l1 = bid1 if side > 0 else ask1
        opp_l1 = ask1 if side > 0 else bid1
        depth_ratio_w5 = same_w5 / max(opp_w5, 1e-9)
        depth_pressure_w5 = (same_w5 - opp_w5) / max(same_w5 + opp_w5, 1e-9)
        pre_aggr_imbalance = float(f.get("pre_aggr_imbalance", 0.0))
        same_side_aggr_imbalance = float(side) * pre_aggr_imbalance
        same_side_aggr_share = float(f.get("aggr_buy_ratio", 0.5)) if side > 0 else float(f.get("aggr_sell_ratio", 0.5))

        self._last_fire_dt[int(side)] = dt
        self._meta_counts["E0_PRICE"] = self._meta_counts.get("E0_PRICE", 0) + 1
        self.event_total_count += 1
        self._active_events.append({
            "event_name": "E0_PRICE",
            "gate_preset": str(self.gate_preset_name),
            "segment": str(self.segment_label),
            "side": int(side),
            "dt0": dt,
            "mid0": float(mid),
            "micro0": float(micro),
            "bid0": float(bid),
            "ask0": float(ask),
            "entry_move_ticks": float(f.get("move_ticks", 0.0)),
            "entry_retrace_ratio": float(f.get("retrace_ratio", 1.0)),
            "entry_consistency": float(f.get("consistency", 0.0)),
            "entry_burst_speed": float(f.get("burst_speed", 0.0)),
            "entry_breakout_margin_ticks": float(f.get("breakout_margin_ticks", 0.0)),
            "aggr_buy_ratio": float(f.get("aggr_buy_ratio", 0.5)),
            "aggr_sell_ratio": float(f.get("aggr_sell_ratio", 0.5)),
            "aggr_vol_norm": float(f.get("aggr_vol_norm", 0.0)),
            "pre_aggr_total": float(f.get("pre_aggr_total", 0.0)),
            "pre_aggr_imbalance": float(pre_aggr_imbalance),
            "same_side_aggr_share": float(same_side_aggr_share),
            "same_side_aggr_imbalance": float(same_side_aggr_imbalance),
            "long_eff_rank": float(f.get("long_eff_rank", 0.5)),
            "short_eff_rank": float(f.get("short_eff_rank", 0.5)),
            "micro_move_ticks": float(f.get("micro_move_ticks", 0.0)),
            "micro_rank": float(f.get("micro_rank", 0.5)),
            "spread_ticks0": float(book.get("spread_ticks0", 0.0)),
            "bid_wdepth5": float(bid_w5),
            "ask_wdepth5": float(ask_w5),
            "depth_same_w5": float(same_w5),
            "depth_opp_w5": float(opp_w5),
            "depth_ratio_w5": float(depth_ratio_w5),
            "depth_pressure_w5": float(depth_pressure_w5),
            "depth_same_l1": float(same_l1),
            "depth_opp_l1": float(opp_l1),
            "path": [(0.0, float(mid), float(bid), float(ask), float(micro))],
        })

    def _finalize_one_event(self, ev: Dict[str, object]) -> None:
        n_before = len(self._event_rows)
        super()._finalize_one_event(ev)
        if len(self._event_rows) <= n_before:
            return
        row = self._event_rows[-1]
        for key in [
            "pre_aggr_total",
            "pre_aggr_imbalance",
            "same_side_aggr_share",
            "same_side_aggr_imbalance",
            "spread_ticks0",
            "bid_wdepth5",
            "ask_wdepth5",
            "depth_same_w5",
            "depth_opp_w5",
            "depth_ratio_w5",
            "depth_pressure_w5",
            "depth_same_l1",
            "depth_opp_l1",
        ]:
            row[key] = ev.get(key, "")

    def _write_event_records(self) -> None:
        import csv
        import os

        path = str(self.output_event_csv).strip()
        if not path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

        cols = [
            "segment", "gate_preset", "event_name", "dt0", "side",
            "move_ticks", "retrace_ratio", "consistency", "mfe_ticks", "mae_ticks",
            "first_hit_p1_s", "first_hit_p2_s", "first_hit_n1_s", "first_hit_n2_s",
            "entry_move_ticks", "entry_retrace_ratio", "entry_consistency",
            "entry_burst_speed", "entry_breakout_margin_ticks",
            "aggr_buy_ratio", "aggr_sell_ratio", "aggr_vol_norm",
            "pre_aggr_total", "pre_aggr_imbalance", "same_side_aggr_share", "same_side_aggr_imbalance",
            "long_eff_rank", "short_eff_rank", "micro_move_ticks", "micro_rank",
            "spread_ticks0", "bid_wdepth5", "ask_wdepth5",
            "depth_same_w5", "depth_opp_w5", "depth_ratio_w5", "depth_pressure_w5",
            "depth_same_l1", "depth_opp_l1",
        ]
        for h in self._horizons:
            cols.extend([f"mid_ret_{h}s", f"taker_now_ret_{h}s"])
        for variant in ["taker_now", "pullback1t", "micro_confirm1t"]:
            cols.extend([f"{variant}_filled", f"{variant}_fill_age_s", f"{variant}_fill_px"])
            for h in self._horizons:
                cols.append(f"{variant}_ret_{h}s")
        for hold_s in self._time_holds:
            cols.extend([f"exit_tstop_{hold_s}s_ret", f"exit_tstop_{hold_s}s_age_s"])
        for w in self._stall_windows:
            for a in self._stall_advances:
                suffix = f"w{int(w)}_a{str(a).replace('.', 'p')}"
                cols.extend([f"exit_stall_{suffix}_ret", f"exit_stall_{suffix}_age_s"])
        for g in self._giveback_ticks:
            suffix = str(g).replace(".", "p")
            cols.extend([f"exit_giveback_{suffix}t_ret", f"exit_giveback_{suffix}t_age_s"])
        for r in self._reversal_ticks:
            suffix = str(r).replace(".", "p")
            cols.extend([f"exit_reversal_{suffix}t_ret", f"exit_reversal_{suffix}t_age_s"])

        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for row in self._event_rows:
                w.writerow(row)



class GateMoudleStrategy_ModuleA_MidLiquidityValidation(GateMoudleStrategy_ModuleA_LiquidityConditioningStudy):
    """
    自包含别名类：供 A-mid 交叉条件 + exit 验证 runner 直接导入。
    逻辑与 LiquidityConditioningStudy 相同，保持两文件包自包含。
    """
    author: str = "DF Quant"


class GateMoudleStrategy_ModuleA_MidRealisticStrategyValidation(GateMoudleStrategy_ModuleA_MidLiquidityValidation):
    """
    A-mid realistic strategy validation (event emitter)
    -------------------------------------------
    目标：
    1) 保持 ATTACK/DEV_MAIN + pure-price + liquidity cohorts 的事件定义不变
    2) 针对 A-mid 研究更合理的“两阶段 exit”
       - 先给 alpha 一个展开窗口（min_hold）
       - 再启用 price exit（giveback / reversal / stall）
       - 同时保留 hard cap time-stop 作为上限
    3) 额外输出：
       - time_to_mfe_s / time_to_mae_s
       - mae_p 分位所需的 event-level 基础字段（runner 汇总）
    """

    author: str = "DF Quant"

    # two-stage exit config
    exit_two_stage_min_holds_s: str = "8,13"
    exit_two_stage_hard_cap_s: int = 30
    exit_two_stage_giveback_ticks: str = "2,3"
    exit_two_stage_reversal_ticks: str = "2,3"
    exit_two_stage_stall_windows_s: str = "4,5"

    parameters: List[str] = GateMoudleStrategy_ModuleA_MidLiquidityValidation.parameters + [
        "exit_two_stage_min_holds_s",
        "exit_two_stage_hard_cap_s",
        "exit_two_stage_giveback_ticks",
        "exit_two_stage_reversal_ticks",
        "exit_two_stage_stall_windows_s",
    ]

    def __init__(self, cta_engine, strategy_name: str, vt_symbol: str, setting: dict):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self._two_stage_min_holds: List[int] = sorted(set(max(1, int(x)) for x in _parse_int_list_str(self.exit_two_stage_min_holds_s)))
        self._two_stage_hard_cap_s: int = max(1, int(self.exit_two_stage_hard_cap_s))
        self._two_stage_givebacks: List[float] = sorted(set(float(x) for x in _parse_float_list_str(self.exit_two_stage_giveback_ticks)))
        self._two_stage_reversals: List[float] = sorted(set(float(x) for x in _parse_float_list_str(self.exit_two_stage_reversal_ticks)))
        self._two_stage_stalls: List[int] = sorted(set(max(1, int(x)) for x in _parse_int_list_str(self.exit_two_stage_stall_windows_s)))

    def _path_until_cap(
        self,
        path: List[Tuple[float, float, float, float, float]],
        hard_cap_s: float,
    ) -> List[Tuple[float, float, float, float, float]]:
        if not path:
            return []
        out: List[Tuple[float, float, float, float, float]] = []
        cap = float(hard_cap_s)
        for obs in path:
            out.append(obs)
            if float(obs[0]) >= cap:
                break
        return out

    def _simulate_giveback_after_min_hold(
        self,
        path: List[Tuple[float, float, float, float, float]],
        side: int,
        entry_px: float,
        min_hold_s: int,
        giveback_ticks: float,
        pt: float,
        hard_cap_s: int,
    ) -> Tuple[float, float]:
        p2 = self._path_until_cap(path, hard_cap_s)
        if not p2:
            return 0.0, 0.0
        best_fav = 0.0
        trigger_allowed = False
        for obs in p2:
            age = float(obs[0])
            cur_fav = self._ret_from_entry(side, obs[1], entry_px, pt)
            if cur_fav > best_fav:
                best_fav = cur_fav
            if age >= float(min_hold_s):
                trigger_allowed = True
            if trigger_allowed and (best_fav - cur_fav) >= float(giveback_ticks):
                return cur_fav, age
        last = p2[-1]
        return self._ret_from_entry(side, last[1], entry_px, pt), float(last[0])

    def _simulate_reversal_after_min_hold(
        self,
        path: List[Tuple[float, float, float, float, float]],
        side: int,
        entry_px: float,
        reversal_ticks: float,
        min_hold_s: int,
        pt: float,
        hard_cap_s: int,
    ) -> Tuple[float, float]:
        p2 = self._path_until_cap(path, hard_cap_s)
        if not p2:
            return 0.0, 0.0
        achieved_positive = False
        for obs in p2:
            age = float(obs[0])
            cur_fav = self._ret_from_entry(side, obs[1], entry_px, pt)
            if cur_fav >= 1.0:
                achieved_positive = True
            if age >= float(min_hold_s) and achieved_positive and cur_fav <= -float(reversal_ticks):
                return cur_fav, age
        last = p2[-1]
        return self._ret_from_entry(side, last[1], entry_px, pt), float(last[0])

    def _simulate_stall_after_min_hold(
        self,
        path: List[Tuple[float, float, float, float, float]],
        side: int,
        entry_px: float,
        stall_window_s: int,
        pt: float,
        min_hold_s: int,
        hard_cap_s: int,
    ) -> Tuple[float, float]:
        p2 = self._path_until_cap(path, hard_cap_s)
        if not p2:
            return 0.0, 0.0
        best_fav = -1e18
        allowed = False
        for obs in p2:
            age = float(obs[0])
            cur_fav = self._ret_from_entry(side, obs[1], entry_px, pt)
            if cur_fav > best_fav:
                best_fav = cur_fav
            if age >= float(min_hold_s):
                allowed = True
            if allowed and age >= float(min_hold_s + stall_window_s):
                recent = [x for x in p2 if (age - float(x[0])) <= float(stall_window_s) + 1e-9]
                recent_best = max(
                    (self._ret_from_entry(side, x[1], entry_px, pt) for x in recent),
                    default=cur_fav,
                )
                if recent_best < best_fav - 1e-12:
                    return cur_fav, age
        last = p2[-1]
        return self._ret_from_entry(side, last[1], entry_px, pt), float(last[0])

    def _finalize_one_event(self, ev: Dict[str, object]) -> None:
        path: List[Tuple[float, float, float, float, float]] = ev["path"]
        side = int(ev["side"])
        bid0 = float(ev["bid0"])
        ask0 = float(ev["ask0"])
        pt = max(float(self.pricetick), 1e-9)

        favorable_list: List[float] = [self._ret_from_mid(side, obs[1], float(ev["mid0"]), pt) for obs in path]
        adverse_list: List[float] = [max(0.0, -x) for x in favorable_list]
        move_ticks = max(favorable_list) if favorable_list else 0.0
        mae_ticks = max(adverse_list) if adverse_list else 0.0

        time_to_mfe_s = ""
        time_to_mae_s = ""
        if favorable_list:
            for obs, fav in zip(path, favorable_list):
                if abs(fav - move_ticks) <= 1e-12:
                    time_to_mfe_s = f"{float(obs[0]):.6f}"
                    break
        if adverse_list:
            for obs, adv in zip(path, adverse_list):
                if abs(adv - mae_ticks) <= 1e-12:
                    time_to_mae_s = f"{float(obs[0]):.6f}"
                    break

        n_before = len(self._event_rows)
        super()._finalize_one_event(ev)
        if len(self._event_rows) <= n_before:
            return

        row = self._event_rows[-1]
        row["time_to_mfe_s"] = time_to_mfe_s
        row["time_to_mae_s"] = time_to_mae_s

        taker_now_entry_px = ask0 if side > 0 else bid0

        # targeted two-stage exits with hard cap
        for mh in self._two_stage_min_holds:
            for gb in self._two_stage_givebacks:
                suffix = f"mh{int(mh)}_gb{str(gb).replace('.', 'p')}_cap{int(self._two_stage_hard_cap_s)}"
                ret, age = self._simulate_giveback_after_min_hold(
                    path, side, taker_now_entry_px, int(mh), float(gb), pt, int(self._two_stage_hard_cap_s)
                )
                row[f"exit_{suffix}_ret"] = ret
                row[f"exit_{suffix}_age_s"] = age

            for rv in self._two_stage_reversals:
                suffix = f"mh{int(mh)}_rv{str(rv).replace('.', 'p')}_cap{int(self._two_stage_hard_cap_s)}"
                ret, age = self._simulate_reversal_after_min_hold(
                    path, side, taker_now_entry_px, float(rv), int(mh), pt, int(self._two_stage_hard_cap_s)
                )
                row[f"exit_{suffix}_ret"] = ret
                row[f"exit_{suffix}_age_s"] = age

            for sw in self._two_stage_stalls:
                suffix = f"mh{int(mh)}_stallw{int(sw)}_cap{int(self._two_stage_hard_cap_s)}"
                ret, age = self._simulate_stall_after_min_hold(
                    path, side, taker_now_entry_px, int(sw), pt, int(mh), int(self._two_stage_hard_cap_s)
                )
                row[f"exit_{suffix}_ret"] = ret
                row[f"exit_{suffix}_age_s"] = age

    def _write_event_records(self) -> None:
        path = str(self.output_event_csv).strip()
        if not path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

        cols = [
            "segment", "gate_preset", "event_name", "dt0", "side",
            "move_ticks", "retrace_ratio", "consistency", "mfe_ticks", "mae_ticks",
            "first_hit_p1_s", "first_hit_p2_s", "first_hit_n1_s", "first_hit_n2_s",
            "time_to_mfe_s", "time_to_mae_s",
            "entry_move_ticks", "entry_retrace_ratio", "entry_consistency",
            "entry_burst_speed", "entry_breakout_margin_ticks",
            "aggr_buy_ratio", "aggr_sell_ratio", "aggr_vol_norm",
            "pre_aggr_total", "pre_aggr_imbalance", "same_side_aggr_share", "same_side_aggr_imbalance",
            "long_eff_rank", "short_eff_rank", "micro_move_ticks", "micro_rank",
            "spread_ticks0", "bid_wdepth5", "ask_wdepth5",
            "depth_same_w5", "depth_opp_w5", "depth_ratio_w5", "depth_pressure_w5",
            "depth_same_l1", "depth_opp_l1",
        ]
        for h in self._horizons:
            cols.extend([f"mid_ret_{h}s", f"taker_now_ret_{h}s"])
        for variant in ["taker_now", "pullback1t", "micro_confirm1t"]:
            cols.extend([f"{variant}_filled", f"{variant}_fill_age_s", f"{variant}_fill_px"])
            for h in self._horizons:
                cols.append(f"{variant}_ret_{h}s")
        for hold_s in self._time_holds:
            cols.extend([f"exit_tstop_{hold_s}s_ret", f"exit_tstop_{hold_s}s_age_s"])
        for w in self._stall_windows:
            for a in self._stall_advances:
                suffix = f"w{int(w)}_a{str(a).replace('.', 'p')}"
                cols.extend([f"exit_stall_{suffix}_ret", f"exit_stall_{suffix}_age_s"])
        for g in self._giveback_ticks:
            suffix = str(g).replace(".", "p")
            cols.extend([f"exit_giveback_{suffix}t_ret", f"exit_giveback_{suffix}t_age_s"])
        for r in self._reversal_ticks:
            suffix = str(r).replace(".", "p")
            cols.extend([f"exit_reversal_{suffix}t_ret", f"exit_reversal_{suffix}t_age_s"])
        for mh in self._two_stage_min_holds:
            for gb in self._two_stage_givebacks:
                suffix = f"mh{int(mh)}_gb{str(gb).replace('.', 'p')}_cap{int(self._two_stage_hard_cap_s)}"
                cols.extend([f"exit_{suffix}_ret", f"exit_{suffix}_age_s"])
            for rv in self._two_stage_reversals:
                suffix = f"mh{int(mh)}_rv{str(rv).replace('.', 'p')}_cap{int(self._two_stage_hard_cap_s)}"
                cols.extend([f"exit_{suffix}_ret", f"exit_{suffix}_age_s"])
            for sw in self._two_stage_stalls:
                suffix = f"mh{int(mh)}_stallw{int(sw)}_cap{int(self._two_stage_hard_cap_s)}"
                cols.extend([f"exit_{suffix}_ret", f"exit_{suffix}_age_s"])

        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for row in self._event_rows:
                w.writerow(row)



# ============================================================================
# Research-pack postprocess: absorb cohort/strategy simulation into strategy file
# ============================================================================
RESEARCH_EXIT_RULES: Dict[str, Dict[str, str]] = {
    "MH8_STALLW4_CAP30": {"ret": "exit_mh8_stallw4_cap30_ret", "age": "exit_mh8_stallw4_cap30_age_s"},
    "MH8_STALLW5_CAP30": {"ret": "exit_mh8_stallw5_cap30_ret", "age": "exit_mh8_stallw5_cap30_age_s"},
    "TSTOP_30S": {"ret": "exit_tstop_30s_ret", "age": "exit_tstop_30s_age_s"},
}

RESEARCH_STRATEGY_VARIANTS: List[Dict[str, object]] = [
    {
        "strategy_name": "AMID_V1_BASE",
        "cohort_col": "cohort_BASE_ALL_VETO_BAD",
        "exit_rule": "MH8_STALLW4_CAP30",
        "hard_stop_ticks": 16.0,
        "cooldown_s": 5,
    },
    {
        "strategy_name": "AMID_V1_BASE_STALLW5",
        "cohort_col": "cohort_BASE_ALL_VETO_BAD",
        "exit_rule": "MH8_STALLW5_CAP30",
        "hard_stop_ticks": 16.0,
        "cooldown_s": 5,
    },
    {
        "strategy_name": "AMID_V1_BASE_CD8",
        "cohort_col": "cohort_BASE_ALL_VETO_BAD",
        "exit_rule": "MH8_STALLW4_CAP30",
        "hard_stop_ticks": 16.0,
        "cooldown_s": 8,
    },
    {
        "strategy_name": "AMID_V1_STRONG_REF",
        "cohort_col": "cohort_GOOD_UNION_2OF3",
        "exit_rule": "MH8_STALLW4_CAP30",
        "hard_stop_ticks": 16.0,
        "cooldown_s": 5,
    },
]

RESEARCH_SHADOW_FAST_EXIT: Dict[str, object] = {
    "ret": "exit_stall_w2_a0p0_ret",
    "age": "exit_stall_w2_a0p0_age_s",
    "hard_stop_ticks": 8.0,
    "cooldown_s": 3,
}
RESEARCH_SHADOW_FAST_NAME: str = "FAST_SHADOW_STALLW2_STOP8_CD3"


class GateMoudleStrategy_ModuleA_MidFullResearchPack(
    GateMoudleStrategy_ModuleA_MidRealisticStrategyValidation
):
    """
    V1 研究版收口层：
    - 事件触发 / path / exit counterfactual 仍沿用既有研究基类
    - cohort / realistic strategy / shadow fast 在策略文件内完成
    - runner 仅做分月主力、回测编排、聚合与 BT_OUT 输出
    """

    author: str = "DF Quant"

    research_enable_postprocess: int = 1
    research_emit_shadow_fast: int = 1

    output_event_enriched_csv: str = ""
    output_trade_csv: str = ""
    output_shadow_trade_csv: str = ""

    research_exit_half_spread_mult: float = 0.5
    research_exit_half_spread_min_ticks: float = 0.5
    research_extra_exit_slippage_ticks: float = 0.0
    research_extra_fee_ticks_rt: float = 0.0
    research_fixed_size: int = 1
    research_tick_value: float = 60.0

    parameters: List[str] = GateMoudleStrategy_ModuleA_MidRealisticStrategyValidation.parameters + [
        "research_enable_postprocess",
        "research_emit_shadow_fast",
        "output_event_enriched_csv",
        "output_trade_csv",
        "output_shadow_trade_csv",
        "research_exit_half_spread_mult",
        "research_exit_half_spread_min_ticks",
        "research_extra_exit_slippage_ticks",
        "research_extra_fee_ticks_rt",
        "research_fixed_size",
        "research_tick_value",
    ]

    def on_stop(self) -> None:
        super().on_stop()
        if not int(self.research_enable_postprocess):
            return
        self._run_research_postprocess()

    def _run_research_postprocess(self) -> None:
        df = self._event_rows_to_df()
        if df.empty:
            self._write_optional_csv(self.output_event_enriched_csv, df)
            self._write_optional_csv(self.output_trade_csv, pd.DataFrame())
            self._write_optional_csv(self.output_shadow_trade_csv, pd.DataFrame())
            return

        enriched = self._prepare_condition_buckets(df)
        self._write_optional_csv(self.output_event_enriched_csv, enriched)

        trade_frames: List[pd.DataFrame] = []
        for spec in RESEARCH_STRATEGY_VARIANTS:
            td = self._simulate_realistic_strategy(
                enriched,
                strategy_name=str(spec["strategy_name"]),
                cohort_col=str(spec["cohort_col"]),
                exit_rule_name=str(spec["exit_rule"]),
                hard_stop_ticks=float(spec["hard_stop_ticks"]),
                cooldown_s=int(spec["cooldown_s"]),
            )
            if not td.empty:
                trade_frames.append(td)
        trades = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
        self._write_optional_csv(self.output_trade_csv, trades)

        if int(self.research_emit_shadow_fast):
            shadow = self._simulate_shadow_fast(enriched)
        else:
            shadow = pd.DataFrame()
        self._write_optional_csv(self.output_shadow_trade_csv, shadow)

    def _event_rows_to_df(self) -> pd.DataFrame:
        if not self._event_rows:
            return pd.DataFrame()
        df = pd.DataFrame(self._event_rows)
        if "dt0" in df.columns:
            df["dt0"] = pd.to_datetime(df["dt0"], errors="coerce")
        if "segment" not in df.columns:
            df["segment"] = str(self.segment_label)
        if "gate_preset" not in df.columns:
            df["gate_preset"] = str(self.gate_preset_name)
        return df

    @staticmethod
    def _write_optional_csv(path_str: str, df: pd.DataFrame) -> None:
        path = str(path_str).strip()
        if not path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        if df is None or df.empty:
            pd.DataFrame().to_csv(path, index=False, encoding="utf-8-sig")
            return
        df.to_csv(path, index=False, encoding="utf-8-sig")

    @staticmethod
    def _spread_bucket(x: float) -> str:
        try:
            x = float(x)
        except Exception:
            return "NA"
        if pd.isna(x):
            return "NA"
        if x <= 1.5:
            return "SP1"
        if x <= 2.5:
            return "SP2"
        if x <= 3.5:
            return "SP3"
        return "SP4P"

    @classmethod
    def _tercile_bucket_within_group(cls, df: pd.DataFrame, value_col: str, out_col: str) -> pd.DataFrame:
        out = df.copy()
        out[out_col] = "NA"
        for _group_key, idx in out.groupby(["segment", "gate_preset"]).groups.items():
            s = pd.to_numeric(out.loc[idx, value_col], errors="coerce")
            valid = s.dropna()
            if valid.empty:
                continue
            ranks = valid.rank(method="average", pct=True)
            bucket = pd.Series(index=valid.index, dtype=object)
            bucket.loc[ranks <= (1.0 / 3.0)] = "LOW"
            bucket.loc[(ranks > (1.0 / 3.0)) & (ranks <= (2.0 / 3.0))] = "MID"
            bucket.loc[ranks > (2.0 / 3.0)] = "HIGH"
            out.loc[bucket.index, out_col] = bucket.astype(str)
        return out

    @classmethod
    def _prepare_condition_buckets(cls, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if "dt0" in out.columns:
            out["dt0"] = pd.to_datetime(out["dt0"], errors="coerce")
        out["spread_bucket"] = pd.to_numeric(out.get("spread_ticks0"), errors="coerce").apply(cls._spread_bucket)
        for src, dst in [
            ("depth_same_w5", "depth_same_bucket"),
            ("depth_opp_w5", "depth_opp_bucket"),
            ("depth_ratio_w5", "depth_ratio_bucket"),
            ("depth_pressure_w5", "depth_pressure_bucket"),
            ("aggr_vol_norm", "aggr_vol_norm_bucket"),
            ("pre_aggr_total", "pre_aggr_total_bucket"),
            ("same_side_aggr_imbalance", "same_side_aggr_imb_bucket"),
        ]:
            if src in out.columns:
                out = cls._tercile_bucket_within_group(out, src, dst)
            else:
                out[dst] = "NA"

        good_sp1 = out["spread_bucket"].eq("SP1")
        good_pre = out["pre_aggr_total_bucket"].eq("HIGH")
        good_depth = out["depth_same_bucket"].eq("HIGH")
        bad_mid = out["depth_ratio_bucket"].eq("MID") | out["depth_pressure_bucket"].eq("MID")
        good_count = good_sp1.astype(int) + good_pre.astype(int) + good_depth.astype(int)

        out["cohort_BASE_ALL"] = True
        out["cohort_BASE_ALL_VETO_BAD"] = ~bad_mid
        out["cohort_GOOD_SP1_AND_PRE_AGGR_HIGH"] = good_sp1 & good_pre
        out["cohort_GOOD_SP1_AND_DEPTH_SAME_HIGH"] = good_sp1 & good_depth
        out["cohort_GOOD_PRE_AGGR_HIGH_AND_DEPTH_SAME_HIGH"] = good_pre & good_depth
        out["cohort_GOOD_UNION_2OF3"] = good_count >= 2
        out["cohort_GOOD_CORE_TRIPLE"] = good_count >= 3
        out["cohort_BAD_EITHER_MID"] = bad_mid
        out["shadow_fast_residual"] = out["cohort_BASE_ALL_VETO_BAD"] & ~out["cohort_GOOD_UNION_2OF3"]
        return out

    def _exit_extra_cost_ticks(self, row: pd.Series) -> float:
        spread0 = pd.to_numeric(pd.Series([row.get("spread_ticks0")]), errors="coerce").iloc[0]
        if pd.isna(spread0):
            spread0 = 1.0
        exit_half = max(
            float(self.research_exit_half_spread_min_ticks),
            float(self.research_exit_half_spread_mult) * float(spread0),
        )
        return float(exit_half + float(self.research_extra_exit_slippage_ticks) + float(self.research_extra_fee_ticks_rt))

    def _simulate_realistic_strategy(
        self,
        df: pd.DataFrame,
        strategy_name: str,
        cohort_col: str,
        exit_rule_name: str,
        hard_stop_ticks: float,
        cooldown_s: int,
    ) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        if exit_rule_name not in RESEARCH_EXIT_RULES:
            raise KeyError(f"unknown exit rule: {exit_rule_name}")
        ret_col = RESEARCH_EXIT_RULES[exit_rule_name]["ret"]
        age_col = RESEARCH_EXIT_RULES[exit_rule_name]["age"]
        if cohort_col not in df.columns:
            raise KeyError(f"missing cohort column: {cohort_col}")

        cand = df.loc[df["gate_preset"].astype(str).eq("ATTACK") & df[cohort_col].fillna(False)].copy()
        if cand.empty:
            return pd.DataFrame()
        cand = cand.sort_values(["dt0", "segment"]).reset_index(drop=True)

        open_until = pd.Timestamp.min
        cooldown_until = {1: pd.Timestamp.min, -1: pd.Timestamp.min}
        trades: List[Dict[str, Any]] = []
        seq = 0
        overlap_skip = 0
        cooldown_skip = 0

        for _, row in cand.iterrows():
            dt0 = pd.to_datetime(row.get("dt0"), errors="coerce")
            if pd.isna(dt0):
                continue
            side = int(pd.to_numeric(pd.Series([row.get("side")]), errors="coerce").fillna(0).iloc[0])
            if side not in (1, -1):
                continue
            if dt0 < open_until:
                overlap_skip += 1
                continue
            if dt0 < cooldown_until[side]:
                cooldown_skip += 1
                continue

            planned_ret = pd.to_numeric(pd.Series([row.get(ret_col)]), errors="coerce").iloc[0]
            planned_age = pd.to_numeric(pd.Series([row.get(age_col)]), errors="coerce").iloc[0]
            mae_ticks = pd.to_numeric(pd.Series([row.get("mae_ticks")]), errors="coerce").iloc[0]
            time_to_mae_s = pd.to_numeric(pd.Series([row.get("time_to_mae_s")]), errors="coerce").iloc[0]
            mfe_ticks = pd.to_numeric(pd.Series([row.get("mfe_ticks")]), errors="coerce").iloc[0]
            if pd.isna(planned_ret) or pd.isna(planned_age):
                continue

            exit_reason = exit_rule_name
            realized_ret = float(planned_ret)
            realized_age = float(planned_age)
            stop_hit = 0
            if pd.notna(mae_ticks) and pd.notna(time_to_mae_s):
                if float(mae_ticks) >= float(hard_stop_ticks) and float(time_to_mae_s) <= float(planned_age):
                    realized_ret = -float(hard_stop_ticks)
                    realized_age = float(time_to_mae_s)
                    exit_reason = f"HSTOP_{int(hard_stop_ticks)}"
                    stop_hit = 1

            extra_cost_ticks = self._exit_extra_cost_ticks(row)
            net_ticks = float(realized_ret) - float(extra_cost_ticks)
            pnl_cny = float(net_ticks) * float(self.research_tick_value) * float(self.research_fixed_size)
            exit_dt = dt0 + timedelta(seconds=float(realized_age))

            seq += 1
            trades.append({
                "trade_seq": seq,
                "segment": row.get("segment"),
                "gate_preset": row.get("gate_preset"),
                "dt0": dt0,
                "side": side,
                "strategy_name": strategy_name,
                "cohort_col": cohort_col,
                "exit_rule": exit_rule_name,
                "hard_stop_ticks": float(hard_stop_ticks),
                "cooldown_s": int(cooldown_s),
                "ret_col": ret_col,
                "age_col": age_col,
                "gross_exit_ret_ticks": float(realized_ret),
                "extra_exit_cost_ticks": float(extra_cost_ticks),
                "net_ret_ticks": float(net_ticks),
                "pnl_cny": float(pnl_cny),
                "hold_s": float(realized_age),
                "exit_reason": exit_reason,
                "stop_hit": int(stop_hit),
                "mfe_ticks": float(mfe_ticks) if pd.notna(mfe_ticks) else float("nan"),
                "mae_ticks": float(mae_ticks) if pd.notna(mae_ticks) else float("nan"),
                "time_to_mfe_s": pd.to_numeric(pd.Series([row.get("time_to_mfe_s")]), errors="coerce").iloc[0],
                "time_to_mae_s": float(time_to_mae_s) if pd.notna(time_to_mae_s) else float("nan"),
                "spread_ticks0": pd.to_numeric(pd.Series([row.get("spread_ticks0")]), errors="coerce").iloc[0],
                "depth_same_w5": pd.to_numeric(pd.Series([row.get("depth_same_w5")]), errors="coerce").iloc[0],
                "depth_opp_w5": pd.to_numeric(pd.Series([row.get("depth_opp_w5")]), errors="coerce").iloc[0],
                "pre_aggr_total": pd.to_numeric(pd.Series([row.get("pre_aggr_total")]), errors="coerce").iloc[0],
                "event_name": row.get("event_name"),
                "entry_move_ticks": pd.to_numeric(pd.Series([row.get("entry_move_ticks")]), errors="coerce").iloc[0],
                "entry_consistency": pd.to_numeric(pd.Series([row.get("entry_consistency")]), errors="coerce").iloc[0],
                "bad_either_mid": int(bool(row.get("cohort_BAD_EITHER_MID", False))),
            })

            open_until = exit_dt
            cooldown_until[side] = exit_dt + timedelta(seconds=int(cooldown_s))

        out = pd.DataFrame(trades)
        if out.empty:
            return out
        out.attrs["overlap_skip"] = overlap_skip
        out.attrs["cooldown_skip"] = cooldown_skip
        return out

    def _simulate_shadow_fast(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        cand = df.loc[df["gate_preset"].astype(str).eq("ATTACK") & df["shadow_fast_residual"].fillna(False)].copy()
        if cand.empty:
            return pd.DataFrame()
        cand = cand.sort_values(["dt0", "segment"]).reset_index(drop=True)

        ret_col = str(RESEARCH_SHADOW_FAST_EXIT["ret"])
        age_col = str(RESEARCH_SHADOW_FAST_EXIT["age"])
        hard_stop_ticks = float(RESEARCH_SHADOW_FAST_EXIT["hard_stop_ticks"])
        cooldown_s = int(RESEARCH_SHADOW_FAST_EXIT["cooldown_s"])

        open_until = pd.Timestamp.min
        cooldown_until = {1: pd.Timestamp.min, -1: pd.Timestamp.min}
        trades: List[Dict[str, Any]] = []
        seq = 0

        for _, row in cand.iterrows():
            dt0 = pd.to_datetime(row.get("dt0"), errors="coerce")
            if pd.isna(dt0):
                continue
            side = int(pd.to_numeric(pd.Series([row.get("side")]), errors="coerce").fillna(0).iloc[0])
            if side not in (1, -1):
                continue
            if dt0 < open_until:
                continue
            if dt0 < cooldown_until[side]:
                continue

            planned_ret = pd.to_numeric(pd.Series([row.get(ret_col)]), errors="coerce").iloc[0]
            planned_age = pd.to_numeric(pd.Series([row.get(age_col)]), errors="coerce").iloc[0]
            mae_ticks = pd.to_numeric(pd.Series([row.get("mae_ticks")]), errors="coerce").iloc[0]
            time_to_mae_s = pd.to_numeric(pd.Series([row.get("time_to_mae_s")]), errors="coerce").iloc[0]
            mfe_ticks = pd.to_numeric(pd.Series([row.get("mfe_ticks")]), errors="coerce").iloc[0]
            if pd.isna(planned_ret) or pd.isna(planned_age):
                continue

            exit_reason = RESEARCH_SHADOW_FAST_NAME
            realized_ret = float(planned_ret)
            realized_age = float(planned_age)
            stop_hit = 0
            if pd.notna(mae_ticks) and pd.notna(time_to_mae_s):
                if float(mae_ticks) >= hard_stop_ticks and float(time_to_mae_s) <= float(planned_age):
                    realized_ret = -hard_stop_ticks
                    realized_age = float(time_to_mae_s)
                    exit_reason = f"HSTOP_{int(hard_stop_ticks)}"
                    stop_hit = 1

            extra_cost_ticks = self._exit_extra_cost_ticks(row)
            net_ticks = float(realized_ret) - float(extra_cost_ticks)
            pnl_cny = float(net_ticks) * float(self.research_tick_value) * float(self.research_fixed_size)
            exit_dt = dt0 + timedelta(seconds=float(realized_age))

            seq += 1
            trades.append({
                "shadow_seq": seq,
                "segment": row.get("segment"),
                "dt0": dt0,
                "side": side,
                "shadow_name": RESEARCH_SHADOW_FAST_NAME,
                "gross_exit_ret_ticks": float(realized_ret),
                "extra_exit_cost_ticks": float(extra_cost_ticks),
                "net_ret_ticks": float(net_ticks),
                "pnl_cny": float(pnl_cny),
                "hold_s": float(realized_age),
                "exit_reason": exit_reason,
                "stop_hit": int(stop_hit),
                "mfe_ticks": float(mfe_ticks) if pd.notna(mfe_ticks) else float("nan"),
                "mae_ticks": float(mae_ticks) if pd.notna(mae_ticks) else float("nan"),
                "bad_either_mid": int(bool(row.get("cohort_BAD_EITHER_MID", False))),
            })
            open_until = exit_dt
            cooldown_until[side] = exit_dt + timedelta(seconds=cooldown_s)

        return pd.DataFrame(trades)


class GateMoudleStrategy_ModuleA_MidFullStrategyV1(GateMoudleStrategy_ModuleA_MidFullResearchPack):
    """A-mid 单模块完整策略 V1（研究版收口）：策略内完成 cohort 与 trade simulation。"""
    pass


# ----------------------------------------------------------------------
# Unified V2 core merged into the V1 reference chain
# ----------------------------------------------------------------------

class GateMoudleStrategy_ModuleA_MidCoreV2(GateMoudleStrategy_ModuleA_MidFullStrategyV1):
    """
    ModuleA unified core
    --------------------
    设计目标：
    1) 保留 V09 作为 gate/session/execok 底座，不在这里重复实现。
    2) 把 ModuleA 的候选生成、治理（score overlay）和执行后处理收敛到唯一真源。
    3) 同一份核心同时支持：
       - RESEARCH: 研究版 / WorkA 风格后处理
       - DIRECT_PARITY: 不走独立 CTA handoff，直接基于同一套 candidate+overlay+exit kernel 做 parity 回测

    说明：
    - 这里的 DIRECT_PARITY 不是“真实 on_trade/on_order 驱动的独立 CTA 生命周期”，
      而是用与 RESEARCH 完全一致的 candidate + overlay + exit 内核做最小偏差验证。
    - 这样做的目的是先把三条漂移分支（V1 / WorkA / Phase6 direct）合成一个可维护的 unified core。
    """

    author: str = "DF Quant"

    run_mode: str = "DIRECT_PARITY"  # RESEARCH / DIRECT_PARITY

    v2_enable_score_overlay: int = 1
    v2_overlay_mode: str = "NONE"   # NONE / ONLY_D6_PLUS / ONLY_D8_PLUS / VETO_BOTTOM_20PCT
    v2_calibration_json_path: str = ""
    v2_overlay_threshold_d6: float = 0.10664366442613715
    v2_overlay_threshold_d8: float = 0.4143542749977217
    v2_overlay_threshold_veto20: float = -0.45027209931283513
    v2_cohort_col: str = "cohort_BASE_ALL_VETO_BAD"
    v2_exit_rule: str = "MH8_STALLW4_CAP30"
    v2_hard_stop_ticks: float = 16.0
    v2_cooldown_s: int = 5
    v2_profile_name: str = "V2_UNIFIED_PROFILE"

    output_candidate_csv: str = ""
    output_signal_trace_csv: str = ""
    output_unified_monitor_json: str = ""

    parameters: List[str] = GateMoudleStrategy_ModuleA_MidFullStrategyV1.parameters + [
        "run_mode",
        "v2_enable_score_overlay",
        "v2_overlay_mode",
        "v2_calibration_json_path",
        "v2_overlay_threshold_d6",
        "v2_overlay_threshold_d8",
        "v2_overlay_threshold_veto20",
        "v2_cohort_col",
        "v2_exit_rule",
        "v2_hard_stop_ticks",
        "v2_cooldown_s",
        "v2_profile_name",
        "output_candidate_csv",
        "output_signal_trace_csv",
        "output_unified_monitor_json",
    ]

    def __init__(self, cta_engine, strategy_name: str, vt_symbol: str, setting: dict):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self._v2_calibration_payload: Optional[Dict[str, Any]] = None

    # ------------------------------------------------------------------
    # calibration / overlay helpers
    # ------------------------------------------------------------------
    def _resolve_local_path(self, path_str: str) -> str:
        p = str(path_str or "").strip()
        if not p:
            return ""
        if os.path.isabs(p):
            return p
        script_dir = os.path.dirname(os.path.abspath(__file__))
        candidates = [
            os.path.abspath(os.path.join(os.getcwd(), p)),
            os.path.abspath(os.path.join(script_dir, p)),
        ]
        for c in candidates:
            if os.path.exists(c):
                return c
        return candidates[0]

    def _load_v2_calibration_payload(self) -> Dict[str, Any]:
        if self._v2_calibration_payload is not None:
            return self._v2_calibration_payload
        path = self._resolve_local_path(self.v2_calibration_json_path)
        if not path or not os.path.exists(path):
            raise FileNotFoundError(f"v2 calibration json not found: {self.v2_calibration_json_path}")
        with open(path, "r", encoding="utf-8") as f:
            self._v2_calibration_payload = json.load(f)
        return self._v2_calibration_payload

    @staticmethod
    def _clip_series(s: pd.Series, low: float, high: float) -> pd.Series:
        x = pd.to_numeric(s, errors="coerce")
        return x.clip(lower=float(low), upper=float(high))

    def _apply_v2_score_overlay(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["v2_score"] = 0.0
        out["v2_score_valid"] = True
        out["v2_overlay_mode"] = str(self.v2_overlay_mode)

        if int(self.v2_enable_score_overlay):
            payload = self._load_v2_calibration_payload()
            feature_rows: List[Dict[str, Any]] = payload.get("features", []) if isinstance(payload, dict) else []
        else:
            feature_rows = []

        for feat in feature_rows:
            name = str(feat.get("name"))
            src = str(feat.get("source_col") or name)
            weight = float(feat.get("weight", 0.0))
            mean = float(feat.get("mean", 0.0))
            std = float(feat.get("std", 1.0))
            low = float(feat.get("clip_low", mean - 6.0 * std))
            high = float(feat.get("clip_high", mean + 6.0 * std))
            raw = pd.to_numeric(out.get(src), errors="coerce")
            clipped = self._clip_series(raw, low, high)
            if abs(std) < 1e-12:
                z = pd.Series(0.0, index=out.index)
            else:
                z = (clipped - mean) / std
            contrib = float(weight) * z.fillna(0.0)
            out[f"v2_raw_{name}"] = raw
            out[f"v2_clip_{name}"] = clipped
            out[f"v2_z_{name}"] = z
            out[f"v2_contrib_{name}"] = contrib
            out["v2_score"] = pd.to_numeric(out["v2_score"], errors="coerce").fillna(0.0) + contrib.fillna(0.0)
            out["v2_score_valid"] = out["v2_score_valid"] & raw.notna()

        d6 = float(self.v2_overlay_threshold_d6)
        d8 = float(self.v2_overlay_threshold_d8)
        veto20 = float(self.v2_overlay_threshold_veto20)
        score = pd.to_numeric(out["v2_score"], errors="coerce")
        out["v2_pass_none"] = True
        out["v2_pass_d6"] = score >= d6
        out["v2_pass_d8"] = score >= d8
        out["v2_pass_veto20"] = score > veto20

        mode = str(self.v2_overlay_mode).strip().upper()
        if mode in ("", "NONE", "NO_SCORE"):
            out["v2_pass_overlay"] = True
        elif mode == "ONLY_D6_PLUS":
            out["v2_pass_overlay"] = out["v2_pass_d6"].fillna(False)
        elif mode == "ONLY_D8_PLUS":
            out["v2_pass_overlay"] = out["v2_pass_d8"].fillna(False)
        elif mode == "VETO_BOTTOM_20PCT":
            out["v2_pass_overlay"] = out["v2_pass_veto20"].fillna(False)
        else:
            raise ValueError(f"unknown v2 overlay mode: {self.v2_overlay_mode}")

        out["v2_profile_name"] = str(self.v2_profile_name)
        out["v2_gate_preset"] = str(self.gate_preset_name)
        out["v2_threshold_d6"] = d6
        out["v2_threshold_d8"] = d8
        out["v2_threshold_veto20"] = veto20
        return out

    # ------------------------------------------------------------------
    # unified postprocess products
    # ------------------------------------------------------------------
    def _build_candidate_records(self, enriched: pd.DataFrame) -> pd.DataFrame:
        if enriched is None or enriched.empty:
            return pd.DataFrame()
        # PITFALL: 这里必须使用当前 profile 对应的 gate_preset_name。
        # 之前从 V1 直接继承时，执行过滤里残留了 ATTACK 硬编码，
        # 导致 selected_rows 已经有了，但 trade_rows 恒为 0。
        gate_name = str(self.gate_preset_name)
        out = enriched.loc[enriched["gate_preset"].astype(str).eq(gate_name)].copy()
        out["base_cohort_pass"] = out[self.v2_cohort_col].fillna(False)
        out["overlay_pass"] = out["v2_pass_overlay"].fillna(False)
        out["trade_eligible_pass"] = out["base_cohort_pass"] & out["overlay_pass"]
        # selected_pass 保持向后兼容，含义等同 trade_eligible_pass
        out["selected_pass"] = out["trade_eligible_pass"]
        cols = [
            "segment", "gate_preset", "event_name", "dt0", "side",
            "base_cohort_pass", "overlay_pass", "trade_eligible_pass", "selected_pass",
            "v2_profile_name", "v2_overlay_mode", "v2_score",
            "move_ticks", "retrace_ratio", "consistency",
            "pre_aggr_total", "depth_same_w5", "depth_pressure_w5",
            "taker_now_ret_21s", "mfe_ticks", "mae_ticks",
        ]
        for c in cols:
            if c not in out.columns:
                out[c] = ""
        return out[cols].sort_values(["dt0", "segment"]).reset_index(drop=True)

    def _build_signal_trace(self, candidates: pd.DataFrame) -> pd.DataFrame:
        if candidates is None or candidates.empty:
            return pd.DataFrame()
        trace = candidates.copy()
        trace["stage"] = "BASE_REJECT"
        trace.loc[trace["base_cohort_pass"].astype(bool), "stage"] = "OVERLAY_REJECT"
        trace.loc[trace["overlay_pass"].astype(bool), "stage"] = "OVERLAY_PASS"
        trace.loc[trace["trade_eligible_pass"].astype(bool), "stage"] = "TRADE_ELIGIBLE"
        cols = [
            "segment", "dt0", "gate_preset", "side", "stage",
            "v2_profile_name", "v2_overlay_mode", "v2_score",
            "base_cohort_pass", "overlay_pass", "trade_eligible_pass", "selected_pass",
        ]
        return trace[cols].sort_values(["dt0", "segment"]).reset_index(drop=True)

    def _write_unified_monitor_json(
        self,
        enriched: pd.DataFrame,
        candidates: pd.DataFrame,
        trades: pd.DataFrame,
        shadow: pd.DataFrame,
    ) -> None:
        path = self._resolve_local_path(self.output_unified_monitor_json)
        if not path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        event_rows = int(len(enriched)) if enriched is not None else 0
        candidate_rows = int(len(candidates)) if candidates is not None else 0
        overlay_rows = int(pd.to_numeric(enriched.get("v2_pass_overlay"), errors="coerce").fillna(0).astype(bool).sum()) if enriched is not None and not enriched.empty else 0
        trade_eligible_rows = int(pd.to_numeric(candidates.get("trade_eligible_pass"), errors="coerce").fillna(0).astype(bool).sum()) if candidates is not None and not candidates.empty else 0
        payload = {
            "run_mode": str(self.run_mode),
            "profile_name": str(self.v2_profile_name),
            "gate_preset": str(self.gate_preset_name),
            "overlay_mode": str(self.v2_overlay_mode),
            "cohort_col": str(self.v2_cohort_col),
            "exit_rule": str(self.v2_exit_rule),
            "hard_stop_ticks": float(self.v2_hard_stop_ticks),
            "cooldown_s": int(self.v2_cooldown_s),
            "event_rows": event_rows,
            "candidate_rows": candidate_rows,
            "selected_rows": overlay_rows,
            "trade_eligible_rows": trade_eligible_rows,
            "coverage_ratio": (float(overlay_rows) / float(candidate_rows)) if candidate_rows > 0 else 0.0,
            "score_mean": float(pd.to_numeric(enriched.get("v2_score"), errors="coerce").dropna().mean()) if enriched is not None and not enriched.empty and "v2_score" in enriched.columns else None,
            "score_std": float(pd.to_numeric(enriched.get("v2_score"), errors="coerce").dropna().std()) if enriched is not None and not enriched.empty and "v2_score" in enriched.columns else None,
            "trade_rows": int(len(trades)) if trades is not None else 0,
            "shadow_rows": int(len(shadow)) if shadow is not None else 0,
            "total_net_ticks": float(pd.to_numeric(trades.get("net_ret_ticks"), errors="coerce").fillna(0.0).sum()) if trades is not None and not trades.empty else 0.0,
            "mean_net_ticks": float(pd.to_numeric(trades.get("net_ret_ticks"), errors="coerce").dropna().mean()) if trades is not None and not trades.empty else None,
            "win_rate": float((pd.to_numeric(trades.get("net_ret_ticks"), errors="coerce").dropna() > 0).mean()) if trades is not None and not trades.empty else None,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def _simulate_realistic_strategy(
        self,
        df: pd.DataFrame,
        strategy_name: str,
        cohort_col: str,
        exit_rule_name: str,
        hard_stop_ticks: float,
        cooldown_s: int,
    ) -> pd.DataFrame:
        """
        对齐 Work Package A：
        - 不再沿用 V1 中 gate_preset == ATTACK 的硬编码过滤
        - 统一使用当前 self.gate_preset_name
        """
        if df.empty:
            return pd.DataFrame()
        if exit_rule_name not in RESEARCH_EXIT_RULES:
            raise KeyError(f"unknown exit rule: {exit_rule_name}")
        ret_col = RESEARCH_EXIT_RULES[exit_rule_name]["ret"]
        age_col = RESEARCH_EXIT_RULES[exit_rule_name]["age"]
        if cohort_col not in df.columns:
            raise KeyError(f"missing cohort column: {cohort_col}")

        gate_name = str(self.gate_preset_name)
        cand = df.loc[df["gate_preset"].astype(str).eq(gate_name) & df[cohort_col].fillna(False)].copy()
        if cand.empty:
            return pd.DataFrame()
        cand = cand.sort_values(["dt0", "segment"]).reset_index(drop=True)

        open_until = pd.Timestamp.min
        cooldown_until = {1: pd.Timestamp.min, -1: pd.Timestamp.min}
        trades: List[Dict[str, Any]] = []
        seq = 0
        overlap_skip = 0
        cooldown_skip = 0

        for _, row in cand.iterrows():
            dt0 = pd.to_datetime(row.get("dt0"), errors="coerce")
            if pd.isna(dt0):
                continue
            side = int(pd.to_numeric(pd.Series([row.get("side")]), errors="coerce").fillna(0).iloc[0])
            if side not in (1, -1):
                continue
            if dt0 < open_until:
                overlap_skip += 1
                continue
            if dt0 < cooldown_until[side]:
                cooldown_skip += 1
                continue

            planned_ret = pd.to_numeric(pd.Series([row.get(ret_col)]), errors="coerce").iloc[0]
            planned_age = pd.to_numeric(pd.Series([row.get(age_col)]), errors="coerce").iloc[0]
            mae_ticks = pd.to_numeric(pd.Series([row.get("mae_ticks")]), errors="coerce").iloc[0]
            time_to_mae_s = pd.to_numeric(pd.Series([row.get("time_to_mae_s")]), errors="coerce").iloc[0]
            mfe_ticks = pd.to_numeric(pd.Series([row.get("mfe_ticks")]), errors="coerce").iloc[0]
            if pd.isna(planned_ret) or pd.isna(planned_age):
                continue

            exit_reason = exit_rule_name
            realized_ret = float(planned_ret)
            realized_age = float(planned_age)
            stop_hit = 0
            if pd.notna(mae_ticks) and pd.notna(time_to_mae_s):
                if float(mae_ticks) >= float(hard_stop_ticks) and float(time_to_mae_s) <= float(planned_age):
                    realized_ret = -float(hard_stop_ticks)
                    realized_age = float(time_to_mae_s)
                    exit_reason = f"HSTOP_{int(hard_stop_ticks)}"
                    stop_hit = 1

            extra_cost_ticks = self._exit_extra_cost_ticks(row)
            net_ticks = float(realized_ret) - float(extra_cost_ticks)
            pnl_cny = float(net_ticks) * float(self.research_tick_value) * float(self.research_fixed_size)
            exit_dt = dt0 + timedelta(seconds=float(realized_age))

            seq += 1
            trades.append({
                "trade_seq": seq,
                "segment": row.get("segment"),
                "gate_preset": row.get("gate_preset"),
                "dt0": dt0,
                "side": side,
                "strategy_name": strategy_name,
                "cohort_col": cohort_col,
                "exit_rule": exit_rule_name,
                "hard_stop_ticks": float(hard_stop_ticks),
                "cooldown_s": int(cooldown_s),
                "ret_col": ret_col,
                "age_col": age_col,
                "gross_exit_ret_ticks": float(realized_ret),
                "extra_exit_cost_ticks": float(extra_cost_ticks),
                "net_ret_ticks": float(net_ticks),
                "pnl_cny": float(pnl_cny),
                "hold_s": float(realized_age),
                "exit_reason": exit_reason,
                "stop_hit": int(stop_hit),
                "mfe_ticks": float(mfe_ticks) if pd.notna(mfe_ticks) else float("nan"),
                "mae_ticks": float(mae_ticks) if pd.notna(mae_ticks) else float("nan"),
                "time_to_mfe_s": pd.to_numeric(pd.Series([row.get("time_to_mfe_s")]), errors="coerce").iloc[0],
                "time_to_mae_s": float(time_to_mae_s) if pd.notna(time_to_mae_s) else float("nan"),
                "spread_ticks0": pd.to_numeric(pd.Series([row.get("spread_ticks0")]), errors="coerce").iloc[0],
                "depth_same_w5": pd.to_numeric(pd.Series([row.get("depth_same_w5")]), errors="coerce").iloc[0],
                "depth_opp_w5": pd.to_numeric(pd.Series([row.get("depth_opp_w5")]), errors="coerce").iloc[0],
                "pre_aggr_total": pd.to_numeric(pd.Series([row.get("pre_aggr_total")]), errors="coerce").iloc[0],
                "event_name": row.get("event_name"),
                "entry_move_ticks": pd.to_numeric(pd.Series([row.get("entry_move_ticks")]), errors="coerce").iloc[0],
                "retrace_ratio": pd.to_numeric(pd.Series([row.get("retrace_ratio")]), errors="coerce").iloc[0],
                "consistency": pd.to_numeric(pd.Series([row.get("consistency")]), errors="coerce").iloc[0],
                "v2_profile_name": row.get("v2_profile_name"),
                "v2_overlay_mode": row.get("v2_overlay_mode"),
                "v2_score": pd.to_numeric(pd.Series([row.get("v2_score")]), errors="coerce").iloc[0],
                "base_cohort_pass": int(bool(row.get(self.v2_cohort_col, False))),
                "overlay_pass": int(bool(row.get("v2_pass_overlay", False))),
                "trade_eligible_pass": int(bool(row.get(cohort_col, False))),
                "bad_either_mid": int(bool(row.get("cohort_BAD_EITHER_MID", False))),
            })
            open_until = exit_dt
            cooldown_until[side] = exit_dt + timedelta(seconds=cooldown_s)

        return pd.DataFrame(trades)

    def _run_research_postprocess(self) -> None:
        df = self._event_rows_to_df()
        if df.empty:
            empty = pd.DataFrame()
            self._write_optional_csv(self.output_event_enriched_csv, empty)
            self._write_optional_csv(self.output_candidate_csv, empty)
            self._write_optional_csv(self.output_signal_trace_csv, empty)
            self._write_optional_csv(self.output_trade_csv, empty)
            self._write_optional_csv(self.output_shadow_trade_csv, empty)
            self._write_unified_monitor_json(empty, empty, empty, empty)
            return

        enriched = self._prepare_condition_buckets(df)
        enriched = self._apply_v2_score_overlay(enriched)
        enriched["cohort_V2_SELECTED"] = enriched[self.v2_cohort_col].fillna(False) & enriched["v2_pass_overlay"].fillna(False)
        self._write_optional_csv(self.output_event_enriched_csv, enriched)

        candidates = self._build_candidate_records(enriched)
        signal_trace = self._build_signal_trace(candidates)
        self._write_optional_csv(self.output_candidate_csv, candidates)
        self._write_optional_csv(self.output_signal_trace_csv, signal_trace)

        trades = self._simulate_realistic_strategy(
            enriched,
            strategy_name=str(self.v2_profile_name),
            cohort_col="cohort_V2_SELECTED",
            exit_rule_name=str(self.v2_exit_rule),
            hard_stop_ticks=float(self.v2_hard_stop_ticks),
            cooldown_s=int(self.v2_cooldown_s),
        )
        self._write_optional_csv(self.output_trade_csv, trades)

        run_mode = str(self.run_mode).strip().upper()
        if run_mode == "RESEARCH" and int(self.research_emit_shadow_fast):
            shadow = self._simulate_shadow_fast(enriched)
        else:
            shadow = pd.DataFrame()
        self._write_optional_csv(self.output_shadow_trade_csv, shadow)
        self._write_unified_monitor_json(enriched, candidates, trades, shadow)

# Compatibility aliases
GateMoudleStrategy_ModuleA_MidUnifiedV2 = GateMoudleStrategy_ModuleA_MidCoreV2
GateMoudleStrategy_ModuleA_MidMergedV2 = GateMoudleStrategy_ModuleA_MidCoreV2
