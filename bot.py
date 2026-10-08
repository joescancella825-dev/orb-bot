"""
ORB Bot for GitHub Actions: opening-range breakout on stocks in play (playbook v3), Alpaca paper trading.

GitHub runs this in short "modes" on its own computers (times are US Eastern):
  morning  ~8:40 start. Builds the liquid universe, scans 3%+ gap-ups with news at 9:27,
           reads the exact 9:30-9:35 candle at 9:35, then until 10:30 checks prices every
           5 seconds. On a breakout: buys at market and immediately places a stop-loss
           order that lives on Alpaca's servers. Exits after 10:30.
  stops    hourly. Reports any stop-loss that filled.
  close    ~3:35 start. At 3:55 cancels the stops, sells what's left, posts the scorecard.
  check    manual. Checks keys, posts a test update, runs a scan. Places NO orders.

Updates go to a private ntfy channel; Claude's scheduled runs pass them on as Claude
notifications and to the ORB Desk dashboard.

Usage: python bot.py --mode morning|stops|close|check
Needs environment variables ALPACA_KEY, ALPACA_SECRET, NTFY_TOPIC (GitHub secrets).
"""
import json, math, os, subprocess, sys, time, traceback
from datetime import datetime, timedelta, date, time as dtime
from zoneinfo import ZoneInfo

import requests
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (GetAssetsRequest, GetCalendarRequest, GetOrdersRequest,
                                     MarketOrderRequest, StopOrderRequest)
from alpaca.trading.enums import AssetClass, AssetStatus, OrderSide, TimeInForce, QueryOrderStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.historical.news import NewsClient
from alpaca.data.requests import StockBarsRequest, StockSnapshotRequest, NewsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed

ET = ZoneInfo("America/New_York")

# ---------------- playbook v3 rules ----------------
RULES = dict(
    GAP_MIN=0.03,             # gap of at least 3%
    MIN_PRICE=5.0,
    MIN_DOLLAR_VOL=50e6,      # 20-day average dollar volume
    MAX_CANDIDATES=5,         # biggest gaps first
    NEWS_LOOKBACK_HOURS=18,
    RISK_PCT=0.02,            # risk at most 2% of equity per trade
    MAX_POSITION_PCT=0.34,    # about 1/3 of equity per trade
    DAILY_LOSS_PCT=0.03,      # no new entries after -3% on the day
    ENTRY_CUTOFF=dtime(10, 30),
    EXIT_BEFORE_CLOSE_MIN=5,  # sell at 3:55 on a normal day
    STOP_BUFFER=0.0005,       # stop sits 0.05% under the level
)
EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "ARCA", "BATS"}
TAG = "orb"                   # every order this bot places has a client_order_id starting with this


# ---------------- clock helpers (patched in tests) ----------------
def now():
    return datetime.now(ET)

def pause(seconds):
    time.sleep(seconds)

def at(d, hh, mm, ss=0):
    return datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=ET)

def chunks(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i + n]

ROUNDUP_WORDS = ("stock market today", "stocks moving", "movers", "pre-market session", "premarket session",
                 "futures", "mid-day", "midday", "biggest stock", "top gainers", "trending stocks", "stocks to watch",
                 "market update", "why is", "shares are trading", "stocks making", "market wrap")

def is_roundup(headline):
    h = headline.lower()
    return any(w in h for w in ROUNDUP_WORDS)

def money(x):
    return f"{'-' if x < 0 else '+'}${abs(x):,.2f}"

def val(x):
    return str(getattr(x, "value", x))


# ---------------- private update channel ----------------
class Channel:
    def __init__(self, topic, log):
        self.topic, self.log = topic, log

    def post(self, title, text, event=None):
        self.log(f"[{title}] {text}")
        body = json.dumps({"title": title, "text": text, "event": event or {"type": "info"},
                           "date": str(now().date()), "time": f"{now():%H:%M}"})
        self.save_to_repo(body)
        if not self.topic:
            return
        for attempt in range(3):
            try:
                requests.post(f"https://ntfy.sh/{self.topic}", data=body.encode("utf-8"), timeout=10)
                return
            except Exception as e:
                self.log(f"post failed ({attempt + 1}/3): {e}")
                pause(2)

    def save_to_repo(self, line):
        """Append the update to logs/<date>.jsonl and push it, so Claude can read an exact copy."""
        if os.getenv("GITHUB_ACTIONS") != "true":
            return
        path = f"logs/{now():%Y-%m-%d}.jsonl"
        try:
            os.makedirs("logs", exist_ok=True)
            with open(path, "a") as f:
                f.write(line + "\n")
            git = lambda *a: subprocess.run(["git", *a], capture_output=True, text=True, timeout=60)
            git("add", path)
            git("commit", "-q", "-m", f"log {now():%Y-%m-%d %H:%M}")
            for attempt in range(4):
                if git("push", "-q").returncode == 0:
                    return
                git("pull", "-q", "--rebase")
                pause(2)
            self.log("could not push the log file")
        except Exception as e:
            self.log(f"log save failed: {e}")

    def today_events(self):
        """Read back today's updates (the channel keeps about 12 hours)."""
        if not self.topic:
            return []
        try:
            r = requests.get(f"https://ntfy.sh/{self.topic}/json?poll=1&since=12h", timeout=15)
            out = []
            for line in r.text.splitlines():
                try:
                    m = json.loads(line)
                    body = json.loads(m.get("message", "{}"))
                    if body.get("date") == str(now().date()):
                        out.append(body)
                except Exception:
                    continue
            return out
        except Exception as e:
            self.log(f"could not read channel: {e}")
            return []


class Bot:
    def __init__(self, trading, data, news, channel, log, today=None, place_orders=True):
        self.t, self.d, self.n, self.ch, self.log = trading, data, news, channel, log
        self.today = today or now().date()
        self.place_orders = place_orders
        self.candidates, self.setups = [], {}

    # ---------- calendar ----------
    def session(self):
        cal = self.t.get_calendar(GetCalendarRequest(start=self.today, end=self.today))
        if not cal or getattr(cal[0], "date", self.today) != self.today:
            return None
        def to_et(x):
            if isinstance(x, datetime):
                return x.astimezone(ET) if x.tzinfo else x.replace(tzinfo=ET)
            hh, mm = str(x).split(":")[:2]
            return at(self.today, int(hh), int(mm))
        return to_et(cal[0].open), to_et(cal[0].close)

    def sleep_until(self, t):
        while True:
            left = (t - now()).total_seconds()
            if left <= 0:
                return
            if left > 120:
                self.log(f"waiting until {t:%H:%M:%S}")
            pause(min(left, 30))

    # ---------- orders placed today by this bot ----------
    def my_orders_today(self):
        orders = self.t.get_orders(GetOrdersRequest(status=QueryOrderStatus.ALL, after=at(self.today, 0, 0), limit=500))
        return [o for o in orders if str(getattr(o, "client_order_id", "") or "").startswith(f"{TAG}-")
                and str(o.client_order_id).split("-")[2] == self.today.strftime("%Y%m%d")]

    def cid(self, kind, sym, extra=""):
        # e.g. orb-b-20261006-AAPL-g83-0935  (b=buy, s=stop, c=close)
        return f"{TAG}-{kind}-{self.today:%Y%m%d}-{sym}-{extra}{now():%H%M%S}"[:120]

    # ---------- 1. liquid universe ----------
    def build_universe(self):
        assets = self.t.get_all_assets(GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE))
        syms = sorted({a.symbol for a in assets if a.tradable and val(a.exchange).upper() in EXCHANGES
                       and a.symbol.isalpha() and len(a.symbol) <= 5})
        self.log(f"{len(syms)} tradable symbols; loading daily bars")
        start = datetime.combine(self.today - timedelta(days=45), dtime(0), ET)
        end = datetime.combine(self.today - timedelta(days=1), dtime(23, 59), ET)
        uni = {}
        for batch in chunks(syms, 400):
            try:
                bs = self.d.get_stock_bars(StockBarsRequest(symbol_or_symbols=batch, timeframe=TimeFrame.Day,
                                                            start=start, end=end, feed=DataFeed.SIP, adjustment="raw"))
            except Exception as e:
                self.log(f"daily bars batch failed: {e}")
                continue
            for s, bars in (bs.data if hasattr(bs, "data") else bs).items():
                bars = [b for b in bars if b.timestamp.astimezone(ET).date() < self.today]
                if len(bars) < 10:
                    continue
                dv = sum(b.close * b.volume for b in bars[-20:]) / len(bars[-20:])
                if bars[-1].close >= RULES["MIN_PRICE"] and dv >= RULES["MIN_DOLLAR_VOL"]:
                    uni[s] = dict(prev_close=bars[-1].close)
        self.log(f"liquid universe: {len(uni)} stocks")
        return uni

    # ---------- 2. gap + news scan ----------
    def latest_prices(self, syms, feeds=(DataFeed.DELAYED_SIP, DataFeed.IEX)):
        out = {}
        for batch in chunks(list(syms), 200):
            snaps = None
            for feed in feeds:
                try:
                    snaps = self.d.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=batch, feed=feed))
                    break
                except Exception as e:
                    self.log(f"snapshot ({feed.value}) failed: {e}")
            for s, sn in (snaps or {}).items():
                tr = getattr(sn, "latest_trade", None)
                if tr and tr.price and tr.timestamp.astimezone(ET).date() == self.today:
                    out[s] = float(tr.price)
        return out

    def headline(self, sym):
        try:
            res = self.n.get_news(NewsRequest(symbols=sym, start=now() - timedelta(hours=RULES["NEWS_LOOKBACK_HOURS"]), limit=5))
        except Exception as e:
            self.log(f"news lookup failed for {sym}: {e}")
            return None
        items = res.data.get("news", []) if hasattr(res, "data") else res.get("news", [])
        for it in items:
            syms = getattr(it, "symbols", None) or [sym]
            h = getattr(it, "headline", "") or ""
            if sym not in syms or len(syms) > 3 or is_roundup(h):
                continue          # market roundups and "stocks moving" lists don't count as a catalyst
            return h or "news"
        return None

    def scan(self, uni):
        px = self.latest_prices(uni)
        gappers = sorted((dict(symbol=s, pm_price=p, prev_close=uni[s]["prev_close"], gap=p / uni[s]["prev_close"] - 1)
                          for s, p in px.items() if p / uni[s]["prev_close"] - 1 >= RULES["GAP_MIN"]),
                         key=lambda x: -x["gap"])
        self.log(f"{len(gappers)} gap-ups of 3%+ before the news check")
        picks = []
        for g in gappers[:25]:
            h = self.headline(g["symbol"])
            if h:
                g["headline"] = h
                picks.append(g)
            if len(picks) >= RULES["MAX_CANDIDATES"]:
                break
        self.candidates = picks
        return picks

    # ---------- 3. opening range ----------
    def opening_ranges(self, syms):
        start, end = at(self.today, 9, 30), at(self.today, 9, 35)
        bs = self.d.get_stock_bars(StockBarsRequest(symbol_or_symbols=syms, timeframe=TimeFrame.Minute,
                                                    start=start, end=end, feed=DataFeed.IEX))
        out = {}
        for s, bars in (bs.data if hasattr(bs, "data") else bs).items():
            bars = sorted([b for b in bars if start <= b.timestamp.astimezone(ET) < end], key=lambda b: b.timestamp)
            if not bars:
                continue
            vol = sum(b.volume for b in bars) or 1
            vwap = sum((b.vwap or (b.high + b.low + b.close) / 3) * b.volume for b in bars) / vol
            out[s] = dict(open=bars[0].open, high=max(b.high for b in bars), low=min(b.low for b in bars),
                          close=bars[-1].close, vwap=vwap)
        return out

    def arm_setups(self):
        if not self.candidates:
            self.ch.post("Opening range (9:35)", "No 3%+ gap-ups with news this morning. Sitting out.",
                         {"type": "ranges", "items": [], "skipped": []})
            return
        ors = self.opening_ranges([c["symbol"] for c in self.candidates])
        lines, skipped = [], []
        for c in self.candidates:
            s, o = c["symbol"], ors.get(c["symbol"])
            if not o:
                skipped.append(f"{s} (no opening data)"); continue
            if not o["close"] > o["open"]:
                skipped.append(f"{s} (red first candle)"); continue
            entry = round(o["high"] + 0.01, 2)
            level = o["vwap"] if o["low"] < o["vwap"] < entry else o["low"]   # "closer" stop rule
            stop = round(level * (1 - RULES["STOP_BUFFER"]), 2)
            if stop >= entry:
                skipped.append(f"{s} (range too narrow)"); continue
            self.setups[s] = dict(c, or_open=o["open"], or_high=o["high"], or_low=o["low"], entry=entry, stop=stop,
                                  qty=0, status="armed")
            lines.append(f"{s} +{c['gap']*100:.1f}%: buy above {entry:.2f}, stop {stop:.2f}")
        text = "\n".join(lines) if lines else "No green setups."
        if skipped:
            text += "\nSkipped: " + ", ".join(skipped)
        self.ch.post("Opening range (9:35)", text + ("\nWatching for breakouts until 10:30." if lines else ""),
                     {"type": "ranges", "skipped": skipped,
                      "items": [dict(symbol=k, gap=round(v["gap"] * 100, 2), open=v["or_open"], or_high=v["or_high"],
                                     or_low=v["or_low"], entry=v["entry"], stop=v["stop"], headline=v.get("headline", ""))
                                for k, v in self.setups.items()]})

    # ---------- 4. entries ----------
    def size(self, equity, cash_left, price, stop):
        rps = price - stop
        if rps <= 0:
            return 0
        return int(math.floor(min(equity * RULES["RISK_PCT"] / rps, equity * RULES["MAX_POSITION_PCT"] / price, cash_left / price)))

    def wait_fill(self, order_id, tries=30):
        for _ in range(tries):
            o = self.t.get_order_by_id(order_id)
            st = val(o.status)
            if st == "filled":
                return o
            if st in ("canceled", "rejected", "expired"):
                return None
            pause(0.5)
        return None

    def try_entries(self, blocked):
        armed = [s for s, i in self.setups.items() if i["status"] == "armed"]
        if not armed:
            return
        px = self.latest_prices(armed, feeds=(DataFeed.IEX,))
        for s in armed:                                    # already in biggest-gap order
            info, p = self.setups[s], px.get(s)
            if p is None or p <= info["entry"]:
                continue
            if blocked:
                info["status"] = "blocked by daily loss limit"; continue
            acct = self.t.get_account()
            equity = float(acct.equity)
            used = sum(i["qty"] * i["fill"] for i in self.setups.values() if i.get("fill") is not None)
            cash_left = min(float(acct.last_equity) - used, float(acct.buying_power))   # cash-account style
            qty = self.size(equity, cash_left, max(p, info["entry"]), info["stop"])
            if qty < 1:
                info["status"] = "no cash left"; continue
            if not self.place_orders:
                info.update(status="open", qty=qty, fill=p); continue
            try:
                g = int(round(info["gap"] * 1000))         # gap in tenths of a percent, kept in the order id
                od = self.t.submit_order(MarketOrderRequest(symbol=s, qty=qty, side=OrderSide.BUY, time_in_force=TimeInForce.DAY,
                                                            client_order_id=self.cid("b", s, f"g{g}-")))
                f = self.wait_fill(str(od.id))
                if not f:
                    info["status"] = "entry failed"
                    self.ch.post(f"{s} entry failed", "Buy order did not fill.", {"type": "error"}); continue
                fill, q = float(f.filled_avg_price), int(float(f.filled_qty))
                info.update(status="open", qty=q, fill=fill, fill_time=f"{now():%H:%M}")
                self.t.submit_order(StopOrderRequest(symbol=s, qty=q, side=OrderSide.SELL, time_in_force=TimeInForce.GTC,
                                                     stop_price=info["stop"], client_order_id=self.cid("s", s)))
                self.ch.post(f"Bought {s}", f"{q} sh at {fill:.2f} (broke {info['entry']:.2f}). Stop {info['stop']:.2f}, "
                             f"risk ${q * (fill - info['stop']):,.0f}. Holding until the stop or 3:55.",
                             {"type": "buy", "symbol": s, "qty": q, "fill": fill, "entry": info["entry"], "stop": info["stop"],
                              "gap": round(info["gap"] * 100, 2), "time": info["fill_time"]})
            except Exception as e:
                info["status"] = "entry failed"
                self.ch.post(f"{s} order problem", str(e)[:300], {"type": "error"})
                try:
                    if any(p_.symbol == s for p_ in self.t.get_all_positions()) and not self.has_open_stop(s):
                        self.t.close_position(s)               # never leave a position without a stop
                except Exception:
                    pass

    def has_open_stop(self, sym):
        return any(o.symbol == sym and str(o.client_order_id).startswith(f"{TAG}-s-") and val(o.status) in ("new", "accepted", "held")
                   for o in self.my_orders_today())

    # ---------- stops ----------
    def stop_fills(self):
        return [o for o in self.my_orders_today() if str(o.client_order_id).startswith(f"{TAG}-s-") and val(o.status) == "filled"]

    def buy_fills(self):
        return {o.symbol: o for o in self.my_orders_today()
                if str(o.client_order_id).startswith(f"{TAG}-b-") and val(o.status) == "filled"}

    def report_stop(self, o, buys):
        b = buys.get(o.symbol)
        px, q = float(o.filled_avg_price), int(float(o.filled_qty))
        pnl = (px - float(b.filled_avg_price)) * q if b else 0.0
        self.ch.post(f"Stop hit: {o.symbol}", f"Sold {q} at {px:.2f} ({money(pnl)}).",
                     {"type": "stop", "symbol": o.symbol, "qty": q, "exit": px, "pnl": round(pnl, 2), "order_id": str(o.id),
                      "time": f"{o.filled_at.astimezone(ET):%H:%M}" if getattr(o, "filled_at", None) else f"{now():%H:%M}"})

    # ---------- modes ----------
    def run_morning(self):
        sess = self.session()
        if not sess:
            self.log("market closed today"); return
        open_t, close_t = sess
        cutoff = datetime.combine(self.today, RULES["ENTRY_CUTOFF"], ET)
        if now() >= cutoff - timedelta(minutes=5):
            self.log("started too late for entries; nothing to do"); return
        if any(e.get("event", {}).get("type") in ("watchlist", "ranges") for e in self.ch.today_events()):
            self.log("another morning run already handled today; exiting"); return
        self.close_leftovers()
        uni = self.build_universe()
        self.sleep_until(at(self.today, 9, 27))
        picks = self.scan(uni)
        self.ch.post("Watchlist", "\n".join(f"{p['symbol']} +{p['gap']*100:.1f}%: {p['headline'][:90]}" for p in picks)
                     or "No 3%+ gap-ups with news.",
                     {"type": "watchlist", "items": [dict(symbol=p["symbol"], gap=round(p["gap"] * 100, 2), pm_price=p["pm_price"],
                                                          prev_close=p["prev_close"], headline=p["headline"]) for p in picks]})
        self.sleep_until(at(self.today, 9, 35, 10))
        self.arm_setups()
        loss_hit = False
        while now() < cutoff:
            try:
                a = self.t.get_account()
                d = float(a.equity) - float(a.last_equity)
                if not loss_hit and d <= -RULES["DAILY_LOSS_PCT"] * float(a.last_equity):
                    loss_hit = True
                    self.ch.post("Daily loss limit", f"Down {money(d)} today. No new entries.", {"type": "loss_limit"})
                self.try_entries(blocked=loss_hit)
                self.report_new_stops()
                if not any(i["status"] == "armed" for i in self.setups.values()):
                    break
            except Exception as e:
                self.log(f"monitor error: {e}")
            pause(5)
        for i in self.setups.values():
            if i["status"] == "armed":
                i["status"] = "no breakout by 10:30"
        self.log("entry window over: " + ", ".join(f"{k}={v['status']}" for k, v in self.setups.items()))
        # half-day sessions end before the afternoon run, so close out here
        if close_t.hour < 15:
            self.sleep_until(close_t - timedelta(minutes=RULES["EXIT_BEFORE_CLOSE_MIN"]))
            self.run_close(skip_wait=True)

    def report_new_stops(self):
        """Report every filled stop that hasn't been reported yet today."""
        reported = {e["event"].get("order_id") for e in self.ch.today_events() if e.get("event", {}).get("type") == "stop"}
        fills = [o for o in self.stop_fills() if str(o.id) not in reported]
        if fills:
            buys = self.buy_fills()
            for o in fills:
                self.report_stop(o, buys)

    def run_stops(self):
        if not self.session():
            return
        self.report_new_stops()

    def run_close(self, skip_wait=False):
        sess = self.session()
        if not sess:
            self.log("market closed today"); return
        open_t, close_t = sess
        exit_t = close_t - timedelta(minutes=RULES["EXIT_BEFORE_CLOSE_MIN"])
        if not skip_wait:
            if now() >= close_t:
                self.log("after the close; the other close run handles it (or it is a half day)"); return
            if now() < exit_t - timedelta(minutes=40):
                self.log("too early: this is the off-season schedule entry; exiting"); return
            self.sleep_until(exit_t)
        orders = self.my_orders_today()
        buys = self.buy_fills()
        self.report_new_stops()                    # stops that filled since the last hourly check
        # cancel live stops, then sell what is still held
        for o in orders:
            if str(o.client_order_id).startswith(f"{TAG}-s-") and val(o.status) in ("new", "accepted", "held", "partially_filled"):
                try:
                    self.t.cancel_order_by_id(str(o.id))
                except Exception as e:
                    self.log(f"cancel {o.symbol}: {e}")
        pause(2)
        held = {p.symbol: int(float(p.qty)) for p in self.t.get_all_positions()}
        exits = {}
        for s, b in buys.items():
            q = held.get(s, 0)
            if q <= 0:
                continue
            try:
                od = self.t.submit_order(MarketOrderRequest(symbol=s, qty=q, side=OrderSide.SELL, time_in_force=TimeInForce.DAY,
                                                            client_order_id=self.cid("c", s)))
                f = self.wait_fill(str(od.id), tries=60)
                if f:
                    exits[s] = (float(f.filled_avg_price), f"{now():%H:%M}")
            except Exception as e:
                self.log(f"close {s}: {e}")
        # scorecard
        stops = {o.symbol: o for o in self.stop_fills()}
        ranges = next((e["event"] for e in self.ch.today_events() if e.get("event", {}).get("type") == "ranges"), {"items": []})
        trades, rows = [], []
        for s, b in buys.items():
            fill, q = float(b.filled_avg_price), int(float(b.filled_qty))
            g = next((int(p[1:]) / 10 for p in str(b.client_order_id).split("-") if p.startswith("g") and p[1:].isdigit()), None)
            if s in stops:
                ex, kind = float(stops[s].filled_avg_price), "stop"
                et_ = f"{stops[s].filled_at.astimezone(ET):%H:%M}" if getattr(stops[s], "filled_at", None) else ""
            elif s in exits:
                ex, kind, et_ = exits[s][0], "3:55 close", exits[s][1]
            else:
                ex, kind, et_ = float("nan"), "unknown", ""
            pnl = (ex - fill) * q if ex == ex else 0.0
            stop_px = next((float(o.stop_price) for o in orders if o.symbol == s and str(o.client_order_id).startswith(f"{TAG}-s-")
                            and getattr(o, "stop_price", None)), None)
            fill_t = f"{b.filled_at.astimezone(ET):%H:%M}" if getattr(b, "filled_at", None) else ""
            trades.append(dict(symbol=s, qty=q, fill=fill, fill_time=fill_t, exit=None if ex != ex else ex, exit_time=et_,
                               pnl=round(pnl, 2), result=kind, stop=stop_px, gap=g))
            rows.append(f"{s}: in {fill:.2f} out {ex:.2f} {money(pnl)} ({kind})")
        for it in ranges.get("items", []):
            if it["symbol"] not in buys:
                rows.append(f"{it['symbol']}: no breakout")
        a = self.t.get_account()
        d, eq = float(a.equity) - float(a.last_equity), float(a.equity)
        text = ("\n".join(rows) if rows else "No trades today.") + f"\nDay: {money(d)} | Equity ${eq:,.2f}"
        self.ch.post("3:55 scorecard", text, {"type": "scorecard", "day_pnl": round(d, 2), "equity": round(eq, 2),
                                              "start_equity": round(float(a.last_equity), 2), "trades": trades,
                                              "setups": ranges.get("items", []), "skipped": ranges.get("skipped", [])})
        left = self.t.get_all_positions()
        if left:
            self.ch.post("Positions still open", ", ".join(p.symbol for p in left) + " did not close. Check Alpaca.", {"type": "error"})

    def close_leftovers(self):
        """Safety: if anything from an earlier day is still held, sell it now."""
        try:
            pos = self.t.get_all_positions()
        except Exception:
            return
        if pos and self.place_orders:
            self.t.close_all_positions(cancel_orders=True)
            self.ch.post("Leftover positions closed", ", ".join(p.symbol for p in pos) + " were still open from an earlier day and were sold.",
                         {"type": "error"})


# ---------------- entry point ----------------
def make_log():
    def log(msg):
        print(f"{now():%H:%M:%S} {msg}", flush=True)
    return log

def main():
    mode = sys.argv[sys.argv.index("--mode") + 1] if "--mode" in sys.argv else "check"
    key, secret = os.getenv("ALPACA_KEY", "").strip(), os.getenv("ALPACA_SECRET", "").strip()
    paper = os.getenv("ALPACA_PAPER", "true").lower() != "false"
    log = make_log()
    ch = Channel(os.getenv("NTFY_TOPIC", "").strip(), log)
    if not key or not secret:
        log("Missing ALPACA_KEY or ALPACA_SECRET. Add them under Settings > Secrets and variables > Actions."); sys.exit(1)
    trading = TradingClient(key, secret, paper=paper)
    data, news = StockHistoricalDataClient(key, secret), NewsClient(key, secret)

    if mode == "check":
        try:
            a = trading.get_account()
        except Exception as e:
            log(f"CHECK FAILED: Alpaca did not accept the keys or could not be reached ({str(e)[:200]}). "
                "Use the PAPER keys and re-paste both secrets.")
            sys.exit(1)
        log(f"Keys work. {'PAPER' if paper else 'LIVE'} account equity ${float(a.equity):,.2f}")
        ch.post("ORB bot test", f"Setup works. {'Paper' if paper else 'LIVE'} equity ${float(a.equity):,.2f}.",
                {"type": "check", "equity": float(a.equity)})
        bot = Bot(trading, data, news, ch, log, place_orders=False)
        log("Market is open today." if bot.session() else "Market is closed today.")
        picks = bot.scan(bot.build_universe())
        for p in picks:
            log(f"  {p['symbol']} +{p['gap']*100:.1f}%  {p['headline'][:80]}")
        log("Check finished. No orders were placed.")
        return

    bot = Bot(trading, data, news, ch, log)
    try:
        {"morning": bot.run_morning, "stops": bot.run_stops, "close": bot.run_close}[mode]()
    except Exception:
        err = traceback.format_exc()
        log(err)
        ch.post("ORB bot error", f"{mode} run failed: {err[-500:]}", {"type": "error"})
        if mode == "close":
            try:
                trading.close_all_positions(cancel_orders=True)
                ch.post("Safety close", "Closed all positions after the error.", {"type": "error"})
            except Exception:
                pass
        sys.exit(1)

if __name__ == "__main__":
    main()
