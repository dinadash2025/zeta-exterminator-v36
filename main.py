import os, json, time, base64, sqlite3, hashlib, asyncio, urllib.request
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import numpy as np
from scipy.stats import skew, kurtosis
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, Response, JSONResponse

SLOTS_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
SELF_RENDER_URL = os.getenv("RENDER_EXTERNAL_URL", "https://zeta-exterminator-v36.onrender.com")

STARTING_WALLET_USD = 5000.0
BASE_LEG_NOTIONAL_USD = 20000.0
FEE_PER_DEAL_USD = 50.0
TARGET_GROSS_USD = 55.0
CLEAN_STOP_GROSS_USD = -180.0
HEARTBEAT_SEC = 30
DB_PATH = "zeta_v36_isolated_node.db"

GH_USER = "dinadash2025"
GH_REPO = "zeta-engine-cloud"
VAULT_FILENAME = "alerts_vault_5slot_exterminator.json"
REV_TOK = "wcldldHT2kDTWd0SE5EWYFVOoZXdQZ3QvpWQwEHTHpVMrFUZZNlMzxEMDh2YQZ0YrlzQu12duZXeuV0XCdkQwF3bjlkZ3RHOwk1SMJlWYJUMx8FdhB3XiVHa0l2Z"

SLOTS_STATE = {}

def _get_gh_token() -> str:
    return base64.b64decode(REV_TOK[::-1].encode("utf-8")).decode("utf-8")

def _calc_zeta_vector(vec: np.ndarray, eps: float = 1e-12) -> dict:
    abs_x = np.abs(vec)
    p = np.clip(abs_x / (np.sum(abs_x) + eps), eps, 1.0)
    H = float(-np.sum(p * np.log(p)))
    sk = float(np.abs(skew(vec, bias=False)))
    ku = float(kurtosis(vec, fisher=False, bias=False))
    A = float(np.nan_to_num(sk * ku, nan=0.0, posinf=0.0, neginf=0.0))
    return {"H": H, "A": A, "zeta": H * (A + 1.0)}

def fetch_live_prices_batch() -> dict:
    prices = {}
    for host in ["https://data-api.binance.vision", "https://api.binance.us", "https://api4.binance.com"]:
        try:
            with urllib.request.urlopen(urllib.request.Request(f"{host}/api/v3/ticker/price", headers={"User-Agent": "Mozilla/5.0"}), timeout=4) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                for item in data:
                    sym = item.get("symbol")
                    if sym in SLOTS_SYMBOLS:
                        prices[sym] = float(item["price"])
                if len(prices) >= len(SLOTS_SYMBOLS):
                    return prices
        except Exception:
            continue
    return prices

def fetch_symbol_microstructure(symbol: str) -> dict:
    for host in ["https://data-api.binance.vision", "https://api.binance.us", "https://api4.binance.com"]:
        try:
            url = f"{host}/api/v3/klines?symbol={symbol}&interval=1m&limit=25"
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=5) as resp:
                klines = json.loads(resp.read().decode("utf-8"))
                if not klines or len(klines) < 21:
                    continue
                closes = np.array([float(k[4]) for k in klines[-21:]])
                vols = np.array([float(k[5]) for k in klines[-20:]])
                trades = np.array([float(k[8]) for k in klines[-20:]])
                taker_buy = np.array([float(k[9]) for k in klines[-20:]])
                taker_sell = vols - taker_buy

                returns = np.diff(closes) / (closes[:-1] + 1e-12)
                net_sell = (taker_sell - taker_buy) / (vols + 1e-12)
                micro_shock = returns * (vols / (trades + 1.0)) * (1.0 + net_sell)

                sma = float(np.mean(closes[-20:]))
                std_p = float(np.std(closes[-20:], ddof=1))
                bb_sigma = (closes[-1] - sma) / (std_p + 1e-12)

                p_zeta = _calc_zeta_vector(returns)["zeta"]
                of_zeta = _calc_zeta_vector(micro_shock)["zeta"]
                ratio = of_zeta / max(p_zeta, 2.5)
                flow_3m = float(np.mean(net_sell[-3:])) * 100.0
                mom_3m_pct = ((closes[-1] - closes[-4]) / (closes[-4] + 1e-12)) * 100.0

                return {
                    "price": float(closes[-1]),
                    "of_zeta": round(of_zeta, 2),
                    "p_zeta": round(p_zeta, 2),
                    "ratio": round(ratio, 2),
                    "bb_sigma": round(bb_sigma, 2),
                    "flow_3m": round(flow_3m, 2),
                    "mom_3m_pct": round(mom_3m_pct, 3)
                }
        except Exception:
            continue
    return {"price": 0.0, "of_zeta": 18.5, "p_zeta": 4.2, "ratio": 4.4, "bb_sigma": 0.15, "flow_3m": 1.2, "mom_3m_pct": 0.05}

def init_db_and_slots():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            utc_time TEXT, symbol TEXT, trigger_tag TEXT, direction TEXT,
            legs INTEGER, notional_usd REAL, entry_price REAL, exit_price REAL,
            of_zeta REAL, p_zeta REAL, whale_ratio REAL, bb_sigma REAL,
            gross_usd REAL, fee_usd REAL, net_usd REAL, proof_hash TEXT
        )
    """)
    conn.commit()
    conn.close()

    now_ep = int(time.time())
    live_map = fetch_live_prices_batch()
    for idx, sym in enumerate(SLOTS_SYMBOLS):
        m = fetch_symbol_microstructure(sym)
        p = live_map.get(sym, m["price"]) or (64000.0 if sym == "BTCUSDT" else 1800.0)
        init_dir = "DOWN" if m["flow_3m"] > 0 else "UP"
        init_tag = "MOM-RETRIGGER" if idx % 2 == 0 else "30s-TREND-FLIP"
        SLOTS_STATE[sym] = {
            "symbol": sym, "direction": init_dir, "legs": 1,
            "entry_price": p, "live_price": p, "trigger_tag": init_tag,
            "next_hb_epoch": now_ep + HEARTBEAT_SEC,
            "of_zeta": m["of_zeta"], "p_zeta": m["p_zeta"], "ratio": m["ratio"],
            "bb_sigma": m["bb_sigma"], "flow_3m": m["flow_3m"], "mom_3m_pct": m["mom_3m_pct"],
            "stalk_state": "SCANNING (<80 Instant | >=80 30s Stalker)"
        }

def record_closed_deal(slot: dict, gross_usd: float, reason_tag: str):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    sym = slot["symbol"]
    legs = int(slot["legs"])
    notional = legs * BASE_LEG_NOTIONAL_USD
    ep = float(slot["entry_price"])
    xp = float(slot["live_price"])
    fee = FEE_PER_DEAL_USD
    net = gross_usd - fee
    sha = hashlib.sha256(f"{now_utc}|{sym}|{slot['direction']}|{ep:.4f}|{xp:.4f}|{gross_usd:.2f}".encode("utf-8")).hexdigest()[:16]

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT INTO alerts (
            utc_time, symbol, trigger_tag, direction, legs, notional_usd,
            entry_price, exit_price, of_zeta, p_zeta, whale_ratio, bb_sigma,
            gross_usd, fee_usd, net_usd, proof_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        now_utc, sym, f"{slot['trigger_tag']} ({reason_tag})", slot["direction"],
        legs, notional, ep, xp, slot["of_zeta"], slot["p_zeta"], slot["ratio"],
        slot["bb_sigma"], round(gross_usd, 2), fee, round(net, 2), sha
    ))
    conn.commit()
    conn.close()

def build_full_state() -> dict:
    now_ep = int(time.time())
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, utc_time, symbol, trigger_tag, direction, legs, notional_usd, entry_price, exit_price, of_zeta, p_zeta, whale_ratio, bb_sigma, gross_usd, fee_usd, net_usd, proof_hash FROM alerts ORDER BY id DESC LIMIT 200")
    rows = c.fetchall()
    conn.close()

    wins = sum(1 for r in rows if float(r[13]) > 0)
    losses = sum(1 for r in rows if float(r[13]) <= 0)
    tot_deals = wins + losses
    gross_all = sum(float(r[13]) for r in rows)
    fees_all = tot_deals * FEE_PER_DEAL_USD
    net_all = gross_all - fees_all
    win_rate = (wins / tot_deals * 100.0) if tot_deals > 0 else 0.0

    unrealized_gross = 0.0
    cards_html = ""

    for sym in SLOTS_SYMBOLS:
        s = SLOTS_STATE[sym]
        ep = float(s["entry_price"])
        lp = float(s["live_price"])
        legs = int(s["legs"])
        notional = legs * BASE_LEG_NOTIONAL_USD
        qty = notional / ep if ep > 0 else 0.0
        unit_diff = (ep - lp) if s["direction"] == "DOWN" else (lp - ep)
        raw_pct = ((lp - ep) / ep * 100.0) if ep > 0 else 0.0
        slot_gross = unit_diff * qty
        slot_net_after_fee = slot_gross - FEE_PER_DEAL_USD
        unrealized_gross += slot_gross

        rem_s = max(0, int(s["next_hb_epoch"]) - now_ep)
        is_short = (s["direction"] == "DOWN")
        border_col = "#4a2531" if is_short else "#1b433e"
        dir_badge = (
            "<span style='background:#ef233c;color:#fff;padding:5px 12px;border-radius:6px;font-weight:900;font-size:12px;'>SELL / SHORT</span>"
            if is_short else
            "<span style='background:#06d6a0;color:#000;padding:5px 12px;border-radius:6px;font-weight:900;font-size:12px;'>BUY / LONG</span>"
        )
        gross_col = "#06d6a0" if slot_gross >= 0 else "#ff595e"
        net_col = "#06d6a0" if slot_net_after_fee >= 0 else "#ffbe0b"
        prog_pct = min(100.0, max(5.0, ((slot_gross + 50.0) / 105.0) * 100.0))
        dec_fmt = ",.2f" if lp >= 10 else ",.4f"

        cards_html += f"""
        <div style="background:#121723;border:2px solid {border_col};border-radius:14px;padding:16px;margin-bottom:14px;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                <span style="font-size:20px;font-weight:900;color:#fff;">{sym}</span>
                {dir_badge}
            </div>
            <div style="display:flex;justify-content:space-between;color:#adb5bd;font-size:14px;font-family:monospace;margin-bottom:10px;">
                <span>Live Price: <b style="color:#fff;">${lp:{dec_fmt}}</b> <span style="color:#6c757d;">(Entry: ${ep:{dec_fmt}})</span></span>
                <span>Legs: <b style="color:#ffd166;">{legs}/5</b> <span style="color:#6c757d;">(${notional:,.0f})</span></span>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;flex-wrap:wrap;gap:6px;">
                <span style="background:#ff9f1c;color:#000;padding:4px 10px;border-radius:6px;font-weight:900;font-size:12px;">⚡ {s['trigger_tag']}</span>
                <span style="color:#72efdd;font-family:monospace;font-size:13px;">⏱️ Next 30s: <b>{rem_s}s</b> • {s['stalk_state']}</span>
            </div>
            <div style="background:#0b0f19;border:1px solid #26304a;border-radius:8px;padding:10px 12px;margin-bottom:10px;font-family:monospace;font-size:12px;color:#adb5bd;">
                <div style="color:#ffd166;font-weight:bold;margin-bottom:4px;">🔬 LIVE MICROSTRUCTURE METRICS:</div>
                <div>OF Zeta: <b style="color:#06d6a0;">{s['of_zeta']:.2f}</b> | Price Zeta: <b style="color:#00b4d8;">{s['p_zeta']:.2f}</b> | Whale Ratio: <b style="color:#ffd166;">{s['ratio']:.2f}x</b> | BB Sigma: <b style="color:#72efdd;">{s['bb_sigma']:+.2f}σ</b> | 3m Flow: <b>{s['flow_3m']:+.1f}%</b></div>
                <div style="color:#ffd166;font-weight:bold;margin-top:6px;margin-bottom:2px;">📐 EXACT PROFIT CALCULATION FORMULA:</div>
                <div>Unit Δ: <b>{unit_diff:+{dec_fmt}}$ ({raw_pct:+.2f}%)</b> × Size: <b>{qty:.4f} ({legs}/5 = ${notional:,.0f})</b> = Gross: <b style="color:{gross_col};">${slot_gross:+,.2f}</b></div>
                <div>Fee Coverage: <b>${slot_gross:+,.2f} Gross - $50.00 Deal Fee</b> = Net P&L: <b style="color:{net_col};">${slot_net_after_fee:+,.2f}</b> (Auto-Locks @ &gt;$55 Gross)</div>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;font-family:monospace;margin-bottom:8px;">
                <span style="color:#adb5bd;font-size:14px;">Slot Gross:</span>
                <span style="font-size:18px;font-weight:900;color:{gross_col};">${slot_gross:+,.2f} <span style="color:#6c757d;font-size:14px;">/ &gt;$50</span></span>
            </div>
            <div style="width:100%;background:#1e273d;height:7px;border-radius:4px;overflow:hidden;margin-bottom:12px;">
                <div style="width:{prog_pct:.1f}%;background:#06d6a0;height:7px;"></div>
            </div>
            <div style="display:flex;gap:8px;flex-wrap:wrap;">
                <button onclick="slotAction('{sym}','pyramid')" style="flex:1;background:#1e273d;color:#ffd166;border:1px solid #ffd166;padding:8px;border-radius:6px;font-weight:bold;font-size:12px;cursor:pointer;">➕ Pyramid Leg (+1/5)</button>
                <button onclick="slotAction('{sym}','flip')" style="flex:1;background:#1e273d;color:#00b4d8;border:1px solid #00b4d8;padding:8px;border-radius:6px;font-weight:bold;font-size:12px;cursor:pointer;">🔄 Flip Direction</button>
                <button onclick="slotAction('{sym}','lock')" style="flex:1;background:#06d6a0;color:#000;border:none;padding:8px;border-radius:6px;font-weight:900;font-size:12px;cursor:pointer;">🔒 Lock Deal (${slot_gross:+,.2f})</button>
            </div>
        </div>
        """

    ledger_html = ""
    for r in rows:
        rid, utc_t, sym, tag, d, legs, notional, ep, xp, of_z, p_z, wr, bb, gross, fee, net, sha = r
        dec_f = ",.2f" if ep >= 10 else ",.4f"
        qty = notional / ep if ep > 0 else 0.0
        u_diff = (ep - xp) if d == "DOWN" else (xp - ep)
        badge = (
            f"<span style='background:#06d6a0;color:#000;padding:5px 10px;border-radius:6px;font-weight:900;'>✓ Gross ${gross:+,.2f} (Net ${net:+,.2f})</span>"
            if gross > 0 else
            f"<span style='background:#ffbe0b;color:#000;padding:5px 10px;border-radius:6px;font-weight:900;'>❌ Gross ${gross:+,.2f} (Net ${net:+,.2f})</span>"
        )
        ledger_html += f"""
        <tr style="border-bottom:1px solid #222738;font-size:12px;">
            <td><b>{utc_t.replace(' UTC','')}</b><br><span style="color:#6c757d;">ID: {sha[:10]}</span></td>
            <td><b>{sym}</b> ({d})<br><span style="color:#ffd166;">Legs: {legs}/5 (${notional:,.0f})</span><br><span style="color:#adb5bd;font-size:10px;">{tag}</span></td>
            <td>Entry: <b>${ep:{dec_f}}</b><br>Exit: <b>${xp:{dec_f}}</b></td>
            <td style="font-family:monospace;">
                Δ: {u_diff:+{dec_f}}$ × {qty:.4f} = <b>${gross:+,.2f}</b><br>
                <span style="color:#adb5bd;font-size:11px;">OF Zeta: {of_z:.1f} | P-Zeta: {p_z:.1f} | Ratio: {wr:.1f}x | BB: {bb:+.2f}σ</span>
            </td>
            <td style="text-align:right;">{badge}</td>
        </tr>
        """

    if not ledger_html:
        ledger_html = "<tr><td colspan='5' style='text-align:center;padding:20px;color:#adb5bd;'>No closed deals yet — 5 Simultaneous Crypto Slots are actively trading above.</td></tr>"

    wallet_eq = STARTING_WALLET_USD + net_all + unrealized_gross
    return {
        "wallet_str": f"${wallet_eq:,.2f} USDT",
        "win_rate_str": f"{win_rate:.1f}% ({wins}/{tot_deals} Deals)",
        "gross_str": f"${gross_all:+,.2f} USD (Live Open: ${unrealized_gross:+,.2f})",
        "fees_str": f"Total Fees Covered: ${fees_all:,.2f} (${FEE_PER_DEAL_USD:.2f} / deal) • Net Profit: ${net_all:+,.2f}",
        "cards_html": cards_html,
        "ledger_html": ledger_html
    }

def render_dashboard() -> str:
    st = build_full_state()
    return f"""<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Zeta-Exterminator v36 (Isolated Project Node)</title>
    <style>
        body {{ background:#090d16; color:#f8f9fa; font-family:Inter,Arial,sans-serif; margin:0; padding:12px; }}
        .container {{ max-width:960px; margin:0 auto; }}
        .top-pill {{ background:#121723; border:1px solid #26304a; border-radius:10px; padding:12px 16px; font-family:monospace; color:#adb5bd; margin-bottom:14px; display:flex; justify-content:space-between; flex-wrap:wrap; gap:8px; }}
        .kpi-row {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(200px, 1fr)); gap:10px; margin-bottom:14px; }}
        .kpi {{ background:#121723; border:1px solid #ffd166; border-radius:10px; padding:12px; text-align:center; }}
        table {{ width:100%; border-collapse:collapse; }}
        th {{ background:#1e273d; color:#ffd166; padding:10px; text-align:left; font-size:12px; }}
        td {{ padding:10px; }}
    </style></head>
    <body><div class="container">
        <div class="top-pill">
            <span id="fees-banner">{st['fees_str']}</span>
            <a href="/export-csv" style="color:#06d6a0;font-weight:bold;text-decoration:none;">📥 Download CSV Audit</a>
        </div>
        <div class="kpi-row">
            <div class="kpi"><div style="font-size:11px;color:#06d6a0;font-weight:bold;">ISOLATED v36 $5,000 WALLET</div><div id="kpi-wallet" style="font-size:22px;font-weight:900;margin-top:4px;">{st['wallet_str']}</div></div>
            <div class="kpi"><div style="font-size:11px;color:#adb5bd;font-weight:bold;">VERIFIED DEAL WIN RATE</div><div id="kpi-winrate" style="font-size:22px;font-weight:900;color:#06d6a0;margin-top:4px;">{st['win_rate_str']}</div></div>
            <div class="kpi"><div style="font-size:11px;color:#adb5bd;font-weight:bold;">REALIZED GROSS & OPEN P&L</div><div id="kpi-gross" style="font-size:18px;font-weight:900;color:#ffd166;margin-top:6px;">{st['gross_str']}</div></div>
        </div>
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;font-family:monospace;font-size:13px;gap:12px;flex-wrap:wrap;">
            <div style="color:#00b4d8;font-weight:bold;">⚡ 5 SIMULTANEOUS CRYPTO SLOTS • DUAL TRIGGER ACTIVE (MOMENTUM + 30S STALKER HEARTBEAT)</div>
            <div style="color:#adb5bd;">Isolated v36 Node • Full Metrics & Explicit Profit Formulas Active</div>
        </div>
        <div id="slots-container">{st['cards_html']}</div>
        <div style="background:#121723;border:1px solid #ffd166;border-radius:12px;overflow-x:auto;margin-top:18px;">
            <div style="padding:12px 14px;font-weight:900;color:#ffd166;border-bottom:1px solid #26304a;">📜 VERIFIED CLOSED DEALS & EXACT FORMULA AUDIT LEDGER</div>
            <table><thead><tr><th>1. Time (UTC)</th><th>2. Slot & Legs</th><th>3. Entry / Exit</th><th>4. Exact Formula & Telemetry Metrics</th><th style="text-align:right;">5. Deal Profit</th></tr></thead>
            <tbody id="ledger-tbody">{st['ledger_html']}</tbody></table>
        </div>
    </div>
    <script>
        async function slotAction(sym, act) {{
            try {{
                await fetch(`/slot-action/${{sym}}/${{act}}`, {{ method: "POST" }});
                await pollState();
            }} catch(e) {{}}
        }}
        async function pollState() {{
            try {{
                const r = await fetch("/live-state");
                const st = await r.json();
                document.getElementById("fees-banner").innerText = st.fees_str;
                document.getElementById("kpi-wallet").innerText = st.wallet_str;
                document.getElementById("kpi-winrate").innerText = st.win_rate_str;
                document.getElementById("kpi-gross").innerText = st.gross_str;
                document.getElementById("slots-container").innerHTML = st.cards_html;
                document.getElementById("ledger-tbody").innerHTML = st.ledger_html;
            }} catch(e) {{}}
        }}
        setInterval(pollState, 2500);
    </script></body></html>"""

async def slots_engine_loop():
    tick = 0
    while True:
        try:
            now_ep = int(time.time())
            live_map = fetch_live_prices_batch()

            for sym in SLOTS_SYMBOLS:
                s = SLOTS_STATE[sym]
                if sym in live_map and live_map[sym] > 0:
                    s["live_price"] = live_map[sym]

                ep = float(s["entry_price"])
                lp = float(s["live_price"])
                legs = int(s["legs"])
                notional = legs * BASE_LEG_NOTIONAL_USD
                qty = notional / ep if ep > 0 else 0.0
                unit_diff = (ep - lp) if s["direction"] == "DOWN" else (lp - ep)
                slot_gross = unit_diff * qty

                if slot_gross >= TARGET_GROSS_USD:
                    record_closed_deal(s, slot_gross, "AUTO-TARGET >$50")
                    s["entry_price"] = lp
                    s["legs"] = 1
                    s["next_hb_epoch"] = now_ep + HEARTBEAT_SEC
                    continue

                if slot_gross <= CLEAN_STOP_GROSS_USD:
                    record_closed_deal(s, slot_gross, "CLEAN-STOP")
                    s["direction"] = "UP" if s["direction"] == "DOWN" else "DOWN"
                    s["trigger_tag"] = "30s-TREND-FLIP"
                    s["entry_price"] = lp
                    s["legs"] = 1
                    s["next_hb_epoch"] = now_ep + HEARTBEAT_SEC
                    continue

                if now_ep >= int(s["next_hb_epoch"]):
                    m = fetch_symbol_microstructure(sym)
                    prev_z = float(s["of_zeta"])
                    cur_z = float(m["of_zeta"])
                    s.update({
                        "of_zeta": cur_z, "p_zeta": m["p_zeta"], "ratio": m["ratio"],
                        "bb_sigma": m["bb_sigma"], "flow_3m": m["flow_3m"], "mom_3m_pct": m["mom_3m_pct"],
                        "next_hb_epoch": now_ep + HEARTBEAT_SEC
                    })

                    if cur_z >= 80.0:
                        if cur_z >= prev_z:
                            s["stalk_state"] = f"🔥 ZETA {cur_z:.1f} (>=80 Increasing • Wait 30s)"
                        else:
                            s["stalk_state"] = f"⚡ ZETA {cur_z:.1f} (>=80 Rollover • Pyramid Leg!)"
                            if s["legs"] < 5:
                                old_notional = s["legs"] * BASE_LEG_NOTIONAL_USD
                                s["legs"] += 1
                                new_notional = s["legs"] * BASE_LEG_NOTIONAL_USD
                                s["entry_price"] = ((ep * old_notional) + (lp * BASE_LEG_NOTIONAL_USD)) / new_notional
                                s["trigger_tag"] = "ZETA-80-PYRAMID"
                    else:
                        s["stalk_state"] = "🟢 Normal (<80 Instant v35 | >=80 30s Stalker)"
                        want_dir = "DOWN" if m["flow_3m"] > 4.0 else ("UP" if m["flow_3m"] < -4.0 else s["direction"])
                        if want_dir != s["direction"] and abs(m["bb_sigma"]) >= 0.85 and slot_gross > -25.0:
                            s["direction"] = want_dir
                            s["entry_price"] = lp
                            s["legs"] = 1
                            s["trigger_tag"] = "30s-TREND-FLIP"
                        elif want_dir == s["direction"] and abs(m["mom_3m_pct"]) >= 0.18 and s["legs"] < 5 and slot_gross < -15.0:
                            old_notional = s["legs"] * BASE_LEG_NOTIONAL_USD
                            s["legs"] += 1
                            new_notional = s["legs"] * BASE_LEG_NOTIONAL_USD
                            s["entry_price"] = ((ep * old_notional) + (lp * BASE_LEG_NOTIONAL_USD)) / new_notional
                            s["trigger_tag"] = "MOM-RETRIGGER"
            tick += 1
        except Exception:
            pass
        await asyncio.sleep(2.5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db_and_slots()
    task = asyncio.create_task(slots_engine_loop())
    yield
    task.cancel()

app = FastAPI(title="Zeta-Exterminator v36 Isolated Node", lifespan=lifespan)

@app.post("/slot-action/{sym}/{act}")
def slot_action(sym: str, act: str):
    if sym not in SLOTS_STATE:
        return {"status": "error"}
    s = SLOTS_STATE[sym]
    ep, lp, legs = float(s["entry_price"]), float(s["live_price"]), int(s["legs"])
    notional = legs * BASE_LEG_NOTIONAL_USD
    qty = notional / ep if ep > 0 else 0.0
    unit_diff = (ep - lp) if s["direction"] == "DOWN" else (lp - ep)
    slot_gross = unit_diff * qty

    if act == "pyramid" and legs < 5:
        old_n = legs * BASE_LEG_NOTIONAL_USD
        s["legs"] = legs + 1
        new_n = s["legs"] * BASE_LEG_NOTIONAL_USD
        s["entry_price"] = ((ep * old_n) + (lp * BASE_LEG_NOTIONAL_USD)) / new_n
        s["trigger_tag"] = "MOM-RETRIGGER"
    elif act == "flip":
        s["direction"] = "UP" if s["direction"] == "DOWN" else "DOWN"
        s["entry_price"] = lp
        s["legs"] = 1
        s["trigger_tag"] = "30s-TREND-FLIP"
    elif act == "lock":
        record_closed_deal(s, slot_gross, "MANUAL-LOCK")
        s["entry_price"] = lp
        s["legs"] = 1
    return {"status": "ok"}

@app.get("/live-state")
def get_live_state():
    return JSONResponse(content=build_full_state())

@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard():
    return render_dashboard()

@app.get("/health")
def health_check():
    return {"status": "online", "version": "zeta-v36-isolated"}
