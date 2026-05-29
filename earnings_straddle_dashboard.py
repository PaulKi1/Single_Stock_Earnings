"""
Assumes the Straddle is sold
Execution modes set entry_price / exit_price:
    Mid          -> entry=mid,           exit=mid
    Half-spread  -> entry=(bid+mid)/2,   exit=(ask+mid)/2     (halfway to the touch)
    Full-spread  -> entry=bid,           exit=ask             (cross the spread twice)
Denominator is always the entry MID, so the three modes are directly comparable.
"""

from __future__ import annotations

import glob
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
from matplotlib.figure import Figure
from matplotlib.colors import Normalize
from matplotlib.patches import Rectangle

# Config
WORKBOOK_PATH = str(Path(__file__).resolve().parent / "data" / "04_straddles" / "earnings_options_final_latest.xlsx")

DEFAULT_COMBOS = ["e3x0", "e3x6", "e1x3", "e0x5"]  # close->open, close->close, -15m->+15m, -30m->+60m
PALETTE = ["#1f77b4", "#ff7f0e", "#9467bd", "#17becf"]  # blue/orange/purple/teal (avoids red/green of the heatmap)
NONE_LABEL = "(none)"

ENTRY_WINDOWS = [(1, "-30m"), (2, "-15m"), (3, "-5m"), (4, "close")]  # straddle index -> label
EXITS = [
    ("open_t2", "open"), ("open_t2_5m", "+5m"), ("open_t2_10m", "+10m"),
    ("open_t2_15m", "+15m"), ("open_t2_30m", "+30m"), ("open_t2_60m", "+60m"),
    ("close_t2", "close"),
]
FLAG_TOKENS = ("missing", "assumed")  # genuine data notes; plain zero bids are valid, not flagged
EXEC_LABELS = {"Mid": "m", "Half-spread": "h", "Full-spread": "f"}

matplotlib.rcParams.update({
    "figure.facecolor": "white", "axes.facecolor": "white",
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.alpha": 0.25, "grid.linewidth": 0.6,
    "font.size": 9, "axes.titlesize": 10, "legend.fontsize": 8,
})


# ---------------------------------------------------------------------------
# Data loading + computation
# ---------------------------------------------------------------------------
def _read_workbooks(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.is_dir():
        files = sorted(glob.glob(str(p / "*.xlsx")))
    elif any(ch in path for ch in "*?["):
        files = sorted(glob.glob(path))
    elif p.exists():
        files = [str(p)]
    else:
        files = sorted(str(x) for x in Path(".").rglob(p.name))
    if not files:
        raise FileNotFoundError(
            f"No workbook found for: {path}\n"
            f"Set WORKBOOK_PATH at the top of this file to the full path of your "
            f"earnings_options_final_latest.xlsx."
        )
    frames = [pd.read_excel(f) for f in files]
    df = pd.concat(frames, ignore_index=True)
    if "event_id" in df.columns:
        df = df.drop_duplicates("event_id", keep="last")
    return df.reset_index(drop=True)


def _num(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return pd.to_numeric(df[name], errors="coerce")
    return pd.Series(np.nan, index=df.index)


def _clean_list(s: pd.Series) -> list:
    return [None if not np.isfinite(v) else round(float(v), 4) for v in s.to_numpy(dtype="float64")]


def _event_quality_flag(df: pd.DataFrame) -> list[bool]:
    flag_cols = [c for c in df.columns if c.endswith("flags")]
    out = []
    for _, row in df.iterrows():
        text = " ".join(str(row[c]) for c in flag_cols if pd.notna(row[c])).lower()
        out.append(any(tok in text for tok in FLAG_TOKENS))
    return out


def build_data(df: pd.DataFrame) -> dict:
    df = df.copy()
    df["earnings_date"] = df["earnings_date"].astype(str)
    df = df.sort_values("earnings_date", kind="mergesort").reset_index(drop=True)

    combos: dict[str, dict] = {}
    for ei, (i, _e) in enumerate(ENTRY_WINDOWS):
        eb, ea, em = _num(df, f"straddle_bid_{i}_timestamp"), _num(df, f"straddle_ask_{i}_timestamp"), _num(df, f"straddle_price_{i}_timestamp")
        entry_mid = em.replace(0.0, np.nan)
        for xi, (xsuf, _x) in enumerate(EXITS):
            xb, xa, xm = _num(df, f"straddle_bid_{i}_{xsuf}"), _num(df, f"straddle_ask_{i}_{xsuf}"), _num(df, f"straddle_price_{i}_{xsuf}")
            combos[f"e{ei}x{xi}"] = {
                "m": _clean_list((em - xm) / entry_mid),
                "h": _clean_list(((eb + em) / 2.0 - (xa + xm) / 2.0) / entry_mid),
                "f": _clean_list((eb - xa) / entry_mid),
            }
    dts = pd.to_datetime(df["earnings_date"], errors="coerce")
    span_days = int((dts.max() - dts.min()).days) if dts.notna().any() else 0
    years_available = max(1, int(np.ceil(span_days / 365.25))) if span_days > 0 else 1
    return {
        "symbols": sorted(df["symbol"].astype(str).unique().tolist()),
        "sym": df["symbol"].astype(str).tolist(),
        "dates": df["earnings_date"].tolist(),
        "qflag": _event_quality_flag(df),
        "entries": [lab for _, lab in ENTRY_WINDOWS],
        "exits": [lab for _, lab in EXITS],
        "combos": combos,
        "years_available": years_available,
    }


# ---------------------------------------------------------------------------
# Labels / helpers
# ---------------------------------------------------------------------------
def ordered_combo_keys(data: dict) -> list[str]:
    return [f"e{ei}x{xi}" for ei in range(len(data["entries"])) for xi in range(len(data["exits"]))]


def key_to_label(key: str, data: dict) -> str:
    ei, xi = int(key[1]), int(key[3])
    return f"{data['entries'][ei]} \u2192 {data['exits'][xi]}"


def _active_indices(data: dict, ticker: str) -> list[int]:
    return [i for i, s in enumerate(data["sym"]) if ticker == "ALL" or s == ticker]


# ---------------------------------------------------------------------------
# Drawing (pure matplotlib -- no Tk; testable headless)
# ---------------------------------------------------------------------------
def draw_dashboard(fig: Figure, data: dict, ticker: str, mode: str, keys, show_flags: bool, years=None) -> None:
    fig.clear()
    idx = _active_indices(data, ticker)
    if years is not None and data["dates"]:
        latest = pd.Timestamp(data["dates"][-1])
        cutoff = latest - pd.DateOffset(years=int(years))
        idx = [i for i in idx if pd.Timestamp(data["dates"][i]) >= cutoff]
    dates = [data["dates"][i] for i in idx]
    xdt = [pd.to_datetime(d) for d in dates]
    sel = [(k, PALETTE[slot]) for slot, k in enumerate(keys) if k]  # colour tied to the slot

    gs = fig.add_gridspec(3, 2, height_ratios=[1.0, 1.0, 0.7], hspace=0.55, wspace=0.22)
    ax_cum = fig.add_subplot(gs[0, 0])
    ax_bar = fig.add_subplot(gs[0, 1])
    ax_box = fig.add_subplot(gs[1, 0])
    ax_heat = fig.add_subplot(gs[1, 1])
    ax_tab = fig.add_subplot(gs[2, :]); ax_tab.axis("off")

    # ---- Cumulative normalized PnL ----
    ax_cum.set_title("Cumulative normalized PnL")
    ax_cum.set_ylabel("cum. PnL ($ / $1)")
    ax_cum.axhline(0, color="0.6", lw=0.8)
    for k, color in sel:
        arr = data["combos"][k][mode]
        xs, ys, run = [], [], 0.0
        for j, i in enumerate(idx):
            v = arr[i]
            if v is None:
                continue
            run += v; xs.append(xdt[j]); ys.append(run)
        ax_cum.plot(xs, ys, "-o", color=color, ms=4, lw=1.6, label=key_to_label(k, data))
    if sel:
        ax_cum.legend(loc="best")
    else:
        ax_cum.text(0.5, 0.5, "select a combination", ha="center", va="center", transform=ax_cum.transAxes, color="0.5")
    ax_cum.tick_params(axis="x", rotation=45, labelsize=7)

    # ---- Per-event normalized PnL (grouped bars) ----
    ax_bar.set_title("Per-event normalized PnL")
    ax_bar.set_ylabel("PnL ($ / $1)")
    ax_bar.axhline(0, color="0.6", lw=0.8)
    n = len(idx)
    x = np.arange(n)
    m = max(len(sel), 1)
    width = 0.8 / m
    for bslot, (k, color) in enumerate(sel):
        arr = data["combos"][k][mode]
        ys = [arr[i] if arr[i] is not None else np.nan for i in idx]
        edge = ["black" if (show_flags and data["qflag"][i]) else "none" for i in idx]
        off = (bslot - (m - 1) / 2.0) * width
        ax_bar.bar(x + off, ys, width=width * 0.95, color=color, edgecolor=edge, linewidth=1.1,
                   label=key_to_label(k, data))
    ax_bar.set_xticks(x)
    ax_bar.set_xticklabels(dates, rotation=90, fontsize=6)
    if sel:
        ax_bar.legend(loc="best")

    # ---- Distribution per combination (box + aligned points) ----
    ax_box.set_title("Distribution per combination")
    ax_box.set_ylabel("PnL ($ / $1)")
    ax_box.axhline(0, color="0.6", lw=0.8)
    box_vals, box_labels, box_colors = [], [], []
    for k, color in sel:
        arr = data["combos"][k][mode]
        vals = [arr[i] for i in idx if arr[i] is not None]
        box_vals.append(vals); box_labels.append(key_to_label(k, data)); box_colors.append(color)
    if box_vals:
        pos = list(range(1, len(box_vals) + 1))
        bp = ax_box.boxplot(box_vals, positions=pos, widths=0.5, patch_artist=True, showfliers=False)
        for patch, color in zip(bp["boxes"], box_colors):
            patch.set_facecolor(color); patch.set_alpha(0.22); patch.set_edgecolor(color)
        for whisk in bp["whiskers"] + bp["caps"]:
            whisk.set_color("0.5")
        for med in bp["medians"]:
            med.set_color("black")
        for p, vals, color in zip(pos, box_vals, box_colors):
            ax_box.scatter([p] * len(vals), vals, color=color, s=16, alpha=0.75, zorder=3, edgecolors="none")
        ax_box.set_xticks(pos)
        ax_box.set_xticklabels(box_labels, rotation=18, fontsize=7, ha="right")
    else:
        ax_box.text(0.5, 0.5, "select a combination", ha="center", va="center", transform=ax_box.transAxes, color="0.5")

    # ---- Entry x exit average normalized PnL (heatmap) ----
    ax_heat.set_title("Entry \u00d7 exit avg normalized PnL")
    E, X = len(data["entries"]), len(data["exits"])
    Z = np.full((E, X), np.nan)
    dropped = np.zeros((E, X), dtype=bool)
    for ei in range(E):
        for xi in range(X):
            arr = data["combos"][f"e{ei}x{xi}"][mode]
            vals = [arr[i] for i in idx if arr[i] is not None]
            if vals:
                Z[ei, xi] = float(np.mean(vals))
            if len(vals) < len(idx):
                dropped[ei, xi] = True
    vmax = float(np.nanmax(np.abs(Z))) if np.isfinite(Z).any() else 1.0
    if vmax <= 0:
        vmax = 1.0
    norm = Normalize(vmin=-vmax, vmax=vmax)
    im = ax_heat.imshow(Z, cmap="RdYlGn", norm=norm, aspect="auto")
    im.format_cursor_data = lambda _data: ""  # toolbar hover readout off (avoids matplotlib overflow)
    ax_heat.set_xticks(range(X)); ax_heat.set_xticklabels(data["exits"], fontsize=8)
    ax_heat.set_yticks(range(E)); ax_heat.set_yticklabels(data["entries"], fontsize=8)
    ax_heat.set_xlabel("exit"); ax_heat.set_ylabel("entry")
    for ei in range(E):
        for xi in range(X):
            if np.isfinite(Z[ei, xi]):
                ax_heat.text(xi, ei, f"{Z[ei, xi]:.2f}", ha="center", va="center", fontsize=7, color="black")
            if show_flags and dropped[ei, xi]:
                ax_heat.add_patch(Rectangle((xi - 0.5, ei - 0.5), 1, 1, fill=False, edgecolor="black", lw=1.6))
    fig.colorbar(im, ax=ax_heat, fraction=0.046, pad=0.04)

    # ---- Stats table ----
    # q1..q4 are the MEANS of the four sorted PnL buckets (0-25 / 25-50 / 50-75 /
    # 75-100%). "total PnL" is the cumulative PnL over the shown events assuming
    # $1 of premium sold per event -- i.e. the end value of the equity curve.
    def _fmt(x):
        return f"{x:.2f}" if np.isfinite(x) else "-"
    cols = ["combination", "mean", "median", "min", "max", "q1", "q2", "q3", "q4", "win%", "total PnL", "n"]
    rows, row_colors = [], []
    for k, color in sel:
        arr = data["combos"][k][mode]
        vals = np.array([v for i in idx if (v := arr[i]) is not None], dtype="float64")
        if vals.size:
            buckets = [b.mean() if b.size else np.nan for b in np.array_split(np.sort(vals), 4)]
            rows.append([
                key_to_label(k, data),
                _fmt(vals.mean()), _fmt(np.percentile(vals, 50)),
                _fmt(vals.min()), _fmt(vals.max()),
                _fmt(buckets[0]), _fmt(buckets[1]), _fmt(buckets[2]), _fmt(buckets[3]),
                f"{(vals > 0).mean() * 100:.0f}", _fmt(float(vals.sum())), str(vals.size),
            ])
        else:
            rows.append([key_to_label(k, data)] + ["-"] * 8 + ["-", "-", "0"])
        row_colors.append(color)
    if rows:
        tab = ax_tab.table(cellText=rows, colLabels=cols, loc="center", cellLoc="center")
        tab.auto_set_font_size(False); tab.set_fontsize(7.5); tab.scale(1, 1.35)
        for r, color in enumerate(row_colors, start=1):
            tab[(r, 0)].get_text().set_color(color)
        for c in range(len(cols)):
            tab[(0, c)].get_text().set_fontweight("bold")
    else:
        ax_tab.text(0.5, 0.5, "Select at least one combination to see stats.", ha="center", va="center",
                    transform=ax_tab.transAxes, color="0.5")

    tlabel = "all tickers (pooled)" if ticker == "ALL" else ticker
    mlabel = {"m": "Mid", "h": "Half-spread", "f": "Full-spread"}[mode]
    note = ""
    if show_flags:
        ndrop = int(dropped.sum())
        nflag = sum(1 for i in idx if data["qflag"][i])
        bits = []
        if ndrop:
            bits.append(f"{ndrop} heatmap cell(s) had a missing leg (boxed; excluded from that average)")
        if nflag:
            bits.append(f"{nflag} event(s) used a missing/assumed-zero leg (outlined in the bar chart)")
        note = "   |   ".join(bits)
    wlabel = "all dates" if years is None else f"past {int(years)}y"
    fig.suptitle(f"Earnings straddle (short)  \u2014  {tlabel}  \u2014  {wlabel}  \u2014  execution: {mlabel}  ({len(idx)} events)", fontsize=12, y=0.995)
    if note:
        fig.text(0.5, 0.005, note, ha="center", va="bottom", fontsize=8, color="0.35")


# ---------------------------------------------------------------------------
# Tk app (interactive)
# ---------------------------------------------------------------------------
class DashboardApp:
    def __init__(self, root, data: dict):
        import tkinter as tk
        from tkinter import ttk
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk

        self.data = data
        self.keys = ordered_combo_keys(data)
        self.label_to_key = {key_to_label(k, data): k for k in self.keys}

        ctrl = ttk.Frame(root, padding=8)
        ctrl.pack(side=tk.TOP, fill=tk.X)

        top = ttk.Frame(ctrl)
        top.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(top, text="Ticker").pack(side=tk.LEFT, padx=(0, 4))
        self.cb_ticker = ttk.Combobox(top, state="readonly", width=16, values=["All (pooled)"] + data["symbols"])
        self.cb_ticker.current(1 if len(data["symbols"]) == 1 else 0)
        self.cb_ticker.pack(side=tk.LEFT, padx=(0, 18))
        ttk.Label(top, text="Execution").pack(side=tk.LEFT, padx=(0, 4))
        self.cb_exec = ttk.Combobox(top, state="readonly", width=12, values=list(EXEC_LABELS.keys()))
        self.cb_exec.current(0)
        self.cb_exec.pack(side=tk.LEFT, padx=(0, 18))
        ttk.Label(top, text="Past years").pack(side=tk.LEFT, padx=(0, 4))
        year_values = ["All"] + [str(y) for y in range(1, data.get("years_available", 1) + 1)]
        self.cb_years = ttk.Combobox(top, state="readonly", width=6, values=year_values)
        self.cb_years.current(0)
        self.cb_years.pack(side=tk.LEFT, padx=(0, 18))
        self.var_flag = tk.BooleanVar(value=True)
        ttk.Checkbutton(top, text="Show data-quality flags", variable=self.var_flag,
                        command=self.refresh).pack(side=tk.LEFT)

        # Combo row: each label sits directly beside its dropdown.
        combo_row = ttk.Frame(ctrl)
        combo_row.pack(side=tk.TOP, fill=tk.X, pady=(8, 0))
        combo_values = [NONE_LABEL] + [key_to_label(k, data) for k in self.keys]
        self.cb_combos = []
        for slot in range(4):
            ttk.Label(combo_row, text=f"Combo {slot + 1}").pack(side=tk.LEFT, padx=(0 if slot == 0 else 18, 4))
            cb = ttk.Combobox(combo_row, state="readonly", width=16, values=combo_values)
            default_label = key_to_label(DEFAULT_COMBOS[slot], data) if slot < len(DEFAULT_COMBOS) else NONE_LABEL
            cb.set(default_label)
            cb.pack(side=tk.LEFT)
            self.cb_combos.append(cb)

        for widget in [self.cb_ticker, self.cb_exec, self.cb_years] + self.cb_combos:
            widget.bind("<<ComboboxSelected>>", lambda _e: self.refresh())

        self.fig = Figure(figsize=(13, 9))
        self.canvas = FigureCanvasTkAgg(self.fig, master=root)
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        NavigationToolbar2Tk(self.canvas, root)
        self.refresh()

    def _selected_keys(self):
        return [self.label_to_key.get(cb.get()) for cb in self.cb_combos]  # None for "(none)"

    def refresh(self):
        ticker = "ALL" if self.cb_ticker.current() == 0 else self.cb_ticker.get()
        mode = EXEC_LABELS[self.cb_exec.get()]
        years = None if self.cb_years.current() == 0 else int(self.cb_years.get())
        draw_dashboard(self.fig, self.data, ticker, mode, self._selected_keys(), self.var_flag.get(), years)
        self.canvas.draw()


def main() -> None:
    import tkinter as tk
    df = _read_workbooks(WORKBOOK_PATH)
    data = build_data(df)
    print(f"Loaded {len(df)} events, {len(data['symbols'])} ticker(s): {', '.join(data['symbols'])}")
    root = tk.Tk()
    root.title("Earnings straddle dashboard (short)")
    root.geometry("1320x980")
    DashboardApp(root, data)
    root.mainloop()


if __name__ == "__main__":
    main()
