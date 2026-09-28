#!/usr/bin/env python3
"""
Arcus ↔ Lighter BTC 价差套利（单文件，Linux）

交易页:
  Arcus:   https://app.arcus.xyz/trade/perpetuals/BTC-USD
  Lighter: https://app.lighter.xyz/trade/BTC?locale=zh

策略（WebSocket 实时 BBO）:

  策略 A（Arcus 多 + Lighter 空）:
    开仓: Lighter 买一 − Arcus 卖一 >= 45 USD，持续 1s
      → Lighter 开空 + Arcus 开多，0.002 BTC
    平仓: Lighter 卖一 − Arcus 买一 <= 5 USD，持续 1s
      → 平 Arcus 多单 + Lighter 空单

  策略 B（Arcus 空 + Lighter 多，仅空仓时可开）:
    开仓: Lighter 卖一 − Arcus 买一 <= 3 USD，持续 1s，且两边无 BTC 仓位
      → Arcus 开空 + Lighter 开多，0.002 BTC
    平仓: Lighter 买一 − Arcus 卖一 >= 45 USD，持续 1s（仅平策略 B 仓位）
      → 平 Arcus 空单 + Lighter 多单

  开仓后核对两边持仓量，多的一边减仓；平仓后清残留仓位。

  盈亏:
    净盈亏 = Arcus平仓盈亏 + Lighter平仓盈亏 − Arcus开仓/平仓手续费
    经 Telegram Bot 推送

依赖:
  pip install cryptography lighter-sdk websockets

环境变量（实盘必填）:
  ARCUS_ADDRESS / ARCUS_API_PRIVATE_KEY / ARCUS_ACCOUNT_INDEX
  LIGHTER_API_PRIVATE_KEY / LIGHTER_ACCOUNT_INDEX
  LIGHTER_API_KEY_INDEX     默认 2；不匹配时自动扫描
  TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID

可选:
  OPEN_SPREAD_THRESHOLD=45        策略 A 开仓 / 策略 B 平仓：L_bid - A_ask >= 该值
  CLOSE_SPREAD_THRESHOLD=5        策略 A 平仓：L_ask - A_bid <= 该值
  REVERSE_OPEN_SPREAD_THRESHOLD=3 策略 B 开仓：L_ask - A_bid <= 该值
  SPREAD_HOLD_SEC=1          价差需持续满足的秒数
  ORDER_SIZE_BTC=0.002       单次开仓量
  MAX_POSITION_BTC=0.002     最大持仓
  ARCUS_TAKER_FEE_BPS=2.5    Arcus taker 手续费（基点）
  SLIPPAGE_BPS=50            Lighter 滑点保护
  DRY_RUN=1                  1=只监控（默认）；0=实盘
  LOG_INTERVAL_SEC=3
  BBO_MAX_AGE_SEC=0.5
  BBO_MAX_SKEW_SEC=0.3
  LEG_SETTLE_SEC=0.8         下单后等待结算再读仓
  ARCUS_SKIP_API_KEY_CHECK=1

用法:
  export ARCUS_ADDRESS=0x...
  export ARCUS_API_PRIVATE_KEY=...
  export LIGHTER_API_PRIVATE_KEY=...
  export LIGHTER_ACCOUNT_INDEX=...
  export TELEGRAM_BOT_TOKEN=...
  export TELEGRAM_CHAT_ID=...
  export DRY_RUN=0
  python3 arcus_lighter_btc_spread_45_5.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, Optional, Tuple

from cryptography.hazmat.primitives.asymmetric import ed25519

try:
    import websockets
except ImportError:
    websockets = None  # type: ignore

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
ARCUS_BASE = os.getenv("ARCUS_BASE_URL", "https://api.arcus.xyz").rstrip("/")
ARCUS_WS = os.getenv("ARCUS_WS_URL", "wss://api.arcus.xyz/v1/ws")
ARCUS_MARKET = os.getenv("ARCUS_MARKET", "BTC-USD")
ARCUS_ADDRESS = os.getenv("ARCUS_ADDRESS", "").strip()
ARCUS_PRIV = os.getenv("ARCUS_API_PRIVATE_KEY", "").strip().removeprefix("0x")
ARCUS_ACCOUNT_INDEX = int(os.getenv("ARCUS_ACCOUNT_INDEX", "0"))

LIGHTER_BASE = os.getenv("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai").rstrip("/")
LIGHTER_WS = os.getenv("LIGHTER_WS_URL", "wss://mainnet.zklighter.elliot.ai/stream")
LIGHTER_PRIV = os.getenv("LIGHTER_API_PRIVATE_KEY", "").strip()
LIGHTER_ACCOUNT_INDEX = os.getenv("LIGHTER_ACCOUNT_INDEX", "").strip()
LIGHTER_API_KEY_INDEX = int(os.getenv("LIGHTER_API_KEY_INDEX", "2"))
LIGHTER_SYMBOL = os.getenv("LIGHTER_SYMBOL", "BTC")

OPEN_SPREAD_THRESHOLD = Decimal(os.getenv("OPEN_SPREAD_THRESHOLD", "-5"))
CLOSE_SPREAD_THRESHOLD = Decimal(os.getenv("CLOSE_SPREAD_THRESHOLD", "-28"))
REVERSE_OPEN_SPREAD_THRESHOLD = Decimal(os.getenv("REVERSE_OPEN_SPREAD_THRESHOLD", "-29"))
SPREAD_HOLD_SEC = float(os.getenv("SPREAD_HOLD_SEC", "1"))
ORDER_SIZE_BTC = Decimal(os.getenv("ORDER_SIZE_BTC", "0.01"))
MAX_POSITION_BTC = Decimal(os.getenv("MAX_POSITION_BTC", "0.03"))
ARCUS_TAKER_FEE_BPS = Decimal(os.getenv("ARCUS_TAKER_FEE_BPS", "2.5"))
SLIPPAGE_BPS = Decimal(os.getenv("SLIPPAGE_BPS", "50"))
DRY_RUN = os.getenv("DRY_RUN", "1").strip() not in ("0", "false", "False", "no")
LOG_INTERVAL_SEC = float(os.getenv("LOG_INTERVAL_SEC", "3"))
BBO_MAX_AGE_SEC = float(os.getenv("BBO_MAX_AGE_SEC", "0.5"))
BBO_MAX_SKEW_SEC = float(os.getenv("BBO_MAX_SKEW_SEC", "0.3"))
HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "5"))
FLAT_EPS_BTC = Decimal(os.getenv("FLAT_EPS_BTC", "0.00001"))
LEG_SETTLE_SEC = float(os.getenv("LEG_SETTLE_SEC", "0.8"))
LEG_REBALANCE_RETRIES = int(os.getenv("LEG_REBALANCE_RETRIES", "5"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
ARCUS_SKIP_API_KEY_CHECK = os.getenv("ARCUS_SKIP_API_KEY_CHECK", "0").strip() not in (
    "0", "false", "False", "no",
)

OP_PLACE, OP_CANCEL = 1, 2
SIDE = {"BUY": 0, "SELL": 1}
TIF = {"GTT": 0, "FOK": 1, "IOC": 2, "ALO": 3}

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("arb-45-5")


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------
def canonical(obj: Any) -> bytes:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True).encode()


def http_json(
    method: str,
    url: str,
    body: Optional[dict] = None,
    headers: Optional[dict] = None,
    query: Optional[dict] = None,
) -> Any:
    q = ""
    if query:
        q = "?" + urllib.parse.urlencode({k: v for k, v in query.items() if v is not None})
    data = canonical(body) if body is not None else None
    last_err: Optional[Exception] = None
    for attempt in range(HTTP_MAX_RETRIES):
        req = urllib.request.Request(
            url + q,
            data=data,
            method=method,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "arcus-lighter-arb-45-5/1.0",
                **(headers or {}),
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read() or b"{}"
                return json.loads(raw.decode())
        except urllib.error.HTTPError as e:
            err_body = e.read().decode(errors="replace")
            if e.code in (429, 500, 502, 503, 504) or "1015" in err_body:
                wait = min(60.0, 1.5 * (2 ** attempt))
                last_err = RuntimeError(f"HTTP {e.code} {method} {url}: {err_body}")
                if attempt + 1 >= HTTP_MAX_RETRIES:
                    break
                log.warning(
                    "HTTP %s 重试 %d/%d，%.1fs 后 | %s %s",
                    e.code, attempt + 1, HTTP_MAX_RETRIES, wait, method, url + q,
                )
                time.sleep(wait)
                continue
            raise RuntimeError(f"HTTP {e.code} {method} {url}: {err_body}") from e
        except urllib.error.URLError as e:
            last_err = e
            if attempt + 1 >= HTTP_MAX_RETRIES:
                break
            wait = min(30.0, 2.0 * (2 ** attempt))
            log.warning(
                "网络错误 重试 %d/%d，%.1fs 后 | %s %s: %s",
                attempt + 1, HTTP_MAX_RETRIES, wait, method, url + q, e,
            )
            time.sleep(wait)
    assert last_err is not None
    raise last_err


def send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("未配置 TELEGRAM_BOT_TOKEN/CHAT_ID，跳过推送")
        return
    if len(text) > 4000:
        text = text[:3990] + "\n…(已截断)"
    chat = TELEGRAM_CHAT_ID
    chat_id: Any = int(chat) if chat.lstrip("-").isdigit() else chat
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    body = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    try:
        http_json("POST", url, body)
        log.info("Telegram 已发送")
    except Exception as exc:
        log.warning("Telegram 发送失败: %s", exc)


def arcus_fee(notional: Decimal) -> Decimal:
    return (abs(notional) * ARCUS_TAKER_FEE_BPS / Decimal("10000")).quantize(Decimal("0.000001"))


# ---------------------------------------------------------------------------
# 盘口
# ---------------------------------------------------------------------------
@dataclass
class TopOfBook:
    bid: Optional[Decimal] = None
    ask: Optional[Decimal] = None
    updated_at: float = 0.0

    def ready(self) -> bool:
        return self.bid is not None and self.ask is not None


class OrderBookHub:
    def __init__(self) -> None:
        self.arcus = TopOfBook()
        self.lighter = TopOfBook()
        self._event = asyncio.Event()

    def touch(self) -> None:
        self._event.set()

    async def wait_update(self, timeout: float = 1.0) -> None:
        try:
            await asyncio.wait_for(self._event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        self._event.clear()


# ---------------------------------------------------------------------------
# Arcus
# ---------------------------------------------------------------------------
@dataclass
class ArcusMarket:
    market_id: int
    name: str
    tick_size: Decimal
    step_size: Decimal
    mark_price: Decimal
    min_order_size: Decimal
    asset_resolution: Decimal


def load_arcus_market(name: str) -> ArcusMarket:
    data = http_json("GET", f"{ARCUS_BASE}/v1/markets", query={"market": name})
    markets = data.get("markets") or data
    if isinstance(markets, dict):
        markets = list(markets.values())
    m = next(x for x in markets if x.get("marketDisplayName") == name)
    return ArcusMarket(
        market_id=int(m["marketId"]),
        name=m["marketDisplayName"],
        tick_size=Decimal(str(m["tickSize"])),
        step_size=Decimal(str(m["stepSize"])),
        mark_price=Decimal(str(m.get("markPrice") or m.get("oraclePrice") or "0")),
        min_order_size=Decimal(str(m.get("minOrderSize") or "0")),
        asset_resolution=Decimal(str(m.get("assetResolution") or "1")),
    )


class ArcusClient:
    def __init__(self, address: str, priv_hex: str, account_index: int) -> None:
        if len(priv_hex) != 64:
            raise ValueError("ARCUS_API_PRIVATE_KEY 应为 64 位 hex")
        self.address = address
        self.addr_l = address.lower()
        self.account_index = account_index
        self.priv = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(priv_hex))
        self.api_key = self.priv.public_key().public_bytes_raw().hex()
        self._q_mode = os.getenv("ARCUS_Q_MODE", "step").strip().lower() or "step"

    def sign_bytes(self, msg: bytes) -> str:
        return self.priv.sign(msg).hex()

    def to_int(self, value: Decimal, unit: Decimal) -> int:
        n = Decimal(str(value)) / Decimal(str(unit))
        if n != n.to_integral_value():
            raise ValueError(f"{value} 不是 {unit} 的整数倍")
        return int(n)

    def to_ticks(self, price: Decimal, tick: Decimal) -> int:
        return self.to_int(price, tick)

    def to_quantums(self, size: Decimal, market: ArcusMarket, mode: Optional[str] = None) -> int:
        m = (mode or self._q_mode).lower()
        if m in ("asset", "assetresolution", "resolution"):
            n = Decimal(str(size)) * market.asset_resolution
            if n != n.to_integral_value():
                raise ValueError("assetResolution 换算非整数")
            return int(n)
        return self.to_int(size, market.step_size)

    def snap_price(self, price: Decimal, tick: Decimal) -> Decimal:
        q = (price / tick).to_integral_value(rounding=ROUND_HALF_UP)
        return (q * tick).quantize(tick)

    def snap_qty(self, qty: Decimal, step: Decimal) -> Decimal:
        q = (qty / step).to_integral_value(rounding=ROUND_DOWN)
        return (q * step).quantize(step)

    def dec_str(self, value: Decimal) -> str:
        s = format(value, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s or "0"

    def good_til_us(self) -> int:
        return int(time.time() * 1_000_000) + 45 * 86_400 * 1_000_000

    def ensure_api_key_registered(self) -> None:
        if ARCUS_SKIP_API_KEY_CHECK:
            log.warning(
                "已跳过 Arcus API Key 启动检查 | address=%s …%s",
                self.address[:10], self.api_key[-8:],
            )
            return
        log.info("正在验证 Arcus API Key | GET %s/v1/apiKeys", ARCUS_BASE)
        data = http_json("GET", f"{ARCUS_BASE}/v1/apiKeys", query={"address": self.address})
        keys = data.get("apiKeys") or []
        if not any((k.get("apiKey") or k.get("publicKey") or "").lower() == self.api_key.lower() for k in keys):
            raise RuntimeError(
                f"Arcus API Key 未注册: address={self.address} derived={self.api_key}\n"
                "请在 Arcus 网站注册 API Key，或设置 ARCUS_SKIP_API_KEY_CHECK=1"
            )
        log.info("Arcus API Key 已验证 …%s", self.api_key[-8:])

    def _place_once(
        self,
        market: ArcusMarket,
        side: str,
        order_type: str,
        qty: Decimal,
        price: Decimal,
        *,
        tif: str,
        reduce_only: bool,
        cid: str,
        q_mode: str,
    ) -> dict:
        ts = time.time_ns()
        gtt = self.good_til_us()
        payload_obj = {
            "ad": self.addr_l,
            "ai": self.account_index,
            "ct": ts,
            "g": gtt * 1000,
            "m": market.market_id,
            "op": OP_PLACE,
            "p": self.to_ticks(price, market.tick_size),
            "q": self.to_quantums(qty, market, q_mode),
            "r": 1 if reduce_only else 0,
            "s": SIDE[side],
            "t": TIF[tif],
            "v": 1,
        }
        if cid:
            payload_obj["c"] = cid
        payload = json.dumps(payload_obj, separators=(",", ":"), sort_keys=True)
        sig = self.sign_bytes(payload.encode())
        body: Dict[str, Any] = {
            "address": self.addr_l,
            "accountIndex": self.account_index,
            "marketId": market.market_id,
            "orderSide": side,
            "orderType": order_type,
            "quantity": self.dec_str(qty),
            "price": self.dec_str(price),
            "timeInForce": tif,
            "goodTilTime": str(gtt),
            "timestamp": ts,
        }
        if reduce_only:
            body["reduceOnly"] = True
        if cid:
            body["clientId"] = cid
        return http_json(
            "POST",
            f"{ARCUS_BASE}/v1/placeOrder",
            body,
            {"X-API-Key": self.api_key, "X-Timestamp": str(ts), "X-Signature": sig},
            query={"address": self.addr_l},
        )

    def place_order(
        self,
        market: ArcusMarket,
        side: str,
        order_type: str,
        qty: Decimal,
        price: Decimal,
        *,
        tif: str = "GTT",
        reduce_only: bool = False,
        client_id: str = "",
    ) -> dict:
        qty = self.snap_qty(qty, market.step_size)
        price = self.snap_price(price, market.tick_size)
        cid = (client_id or "").strip().lower()[:36]
        q_mode = self._q_mode
        if DRY_RUN:
            log.info(
                "[DRY_RUN] Arcus %s %s qty=%s px=%s reduce=%s",
                order_type, side, qty, price, reduce_only,
            )
            return {"status": "DRY_RUN"}
        try:
            return self._place_once(
                market, side, order_type, qty, price,
                tif=tif, reduce_only=reduce_only, cid=cid, q_mode=q_mode,
            )
        except RuntimeError as e:
            if "invalid order signature" not in str(e):
                raise
            alt = "asset" if q_mode == "step" else "step"
            resp = self._place_once(
                market, side, order_type, qty, price,
                tif=tif, reduce_only=reduce_only, cid=cid, q_mode=alt,
            )
            self._q_mode = alt
            return resp

    def market_order(
        self,
        market: ArcusMarket,
        side: str,
        qty: Decimal,
        *,
        reduce_only: bool,
        tag: str,
        ref_price: Optional[Decimal] = None,
    ) -> dict:
        mark = ref_price if ref_price is not None and ref_price > 0 else market.mark_price
        if side == "SELL":
            prot = self.snap_price(mark * Decimal("0.95"), market.tick_size)
        else:
            prot = self.snap_price(mark * Decimal("1.05"), market.tick_size)
        return self.place_order(
            market, side, "MARKET", qty, prot, tif="IOC", reduce_only=reduce_only,
            client_id=f"{tag}{int(time.time())}"[:36],
        )

    def get_positions(self) -> dict:
        return http_json(
            "GET",
            f"{ARCUS_BASE}/v1/positions",
            query={"address": self.address, "accountIndex": self.account_index},
        )


def parse_arcus_position(pos_resp: dict, market: ArcusMarket) -> Decimal:
    """带符号 BTC 仓位（多>0，空<0）。"""
    positions = pos_resp.get("positions") or {}
    p = None
    if isinstance(positions, dict):
        p = positions.get(str(market.market_id)) or positions.get(market.market_id)
    elif isinstance(positions, list):
        p = next((x for x in positions if int(x.get("marketId", -1)) == market.market_id), None)
    if not p:
        return Decimal("0")
    side = str(p.get("side") or "").upper()
    raw = Decimal(str(p.get("size") or "0"))
    notional = p.get("positionValueNotional")
    mark = Decimal(str(p.get("markPx") or market.mark_price or "0"))
    if notional not in (None, "") and mark > 0:
        approx = abs(Decimal(str(notional)) / mark)
        if raw == 0:
            raw = approx
        elif abs(raw) > 1 and market.asset_resolution > 1:
            raw = raw / market.asset_resolution
    elif abs(raw) >= 1000 and market.asset_resolution > 1:
        raw = raw / market.asset_resolution
    if side == "SHORT" and raw > 0:
        raw = -raw
    elif side == "LONG" and raw < 0:
        raw = -raw
    return raw


# ---------------------------------------------------------------------------
# Lighter
# ---------------------------------------------------------------------------
@dataclass
class LighterMarket:
    symbol: str
    market_id: int
    size_decimals: int
    price_decimals: int
    min_base_amount: Decimal
    mark_price: Decimal


def fetch_lighter_btc_market() -> LighterMarket:
    data = http_json("GET", f"{LIGHTER_BASE}/api/v1/orderBookDetails")
    for item in data.get("order_book_details") or []:
        if str(item.get("symbol") or "").upper() != LIGHTER_SYMBOL.upper():
            continue
        if str(item.get("market_type") or "perp").lower() != "perp":
            continue
        return LighterMarket(
            symbol=LIGHTER_SYMBOL.upper(),
            market_id=int(item["market_id"]),
            size_decimals=int(item.get("size_decimals") or item.get("supported_size_decimals") or 0),
            price_decimals=int(item.get("price_decimals") or item.get("supported_price_decimals") or 0),
            min_base_amount=Decimal(str(item.get("min_base_amount") or "0")),
            mark_price=Decimal(str(item.get("mark_price") or "0")),
        )
    raise RuntimeError(f"Lighter 未找到 {LIGHTER_SYMBOL} 永续市场")


def to_base_amount(qty: Decimal, size_decimals: int) -> int:
    scale = Decimal(10) ** size_decimals
    return int((qty * scale).to_integral_value(rounding=ROUND_DOWN))


def to_price_int(price: Decimal, price_decimals: int) -> int:
    scale = Decimal(10) ** price_decimals
    return int((price * scale).to_integral_value(rounding=ROUND_DOWN))


def worst_lighter_price(mark: Decimal, is_ask: bool, price_decimals: int) -> int:
    slip = SLIPPAGE_BPS / Decimal("10000")
    px = mark * (Decimal("1") - slip if is_ask else Decimal("1") + slip)
    if px <= 0:
        px = mark
    return to_price_int(px, price_decimals)


def _lighter_check_client(signer) -> Optional[str]:
    check = getattr(signer, "check_client", None)
    if not callable(check):
        return None
    err = check()
    if asyncio.iscoroutine(err):
        raise RuntimeError("lighter SignerClient.check_client 不应为 async")
    return err


async def _close_lighter_signer(signer) -> None:
    close_fn = getattr(signer, "close", None)
    if callable(close_fn):
        maybe = close_fn()
        if asyncio.iscoroutine(maybe):
            await maybe


async def fetch_lighter_registered_api_keys(account_index: int) -> list:
    import lighter

    client = lighter.ApiClient(lighter.Configuration(host=LIGHTER_BASE))
    try:
        resp = await lighter.AccountApi(client).apikeys(
            account_index=account_index,
            api_key_index=255,
        )
        return list(getattr(resp, "api_keys", None) or [])
    finally:
        await client.close()


async def resolve_lighter_api_key_index(
    account_index: int,
    private_key: str,
    preferred: int,
) -> int:
    import lighter

    priv = private_key.strip().removeprefix("0x")
    registered = await fetch_lighter_registered_api_keys(account_index)
    if not registered:
        raise RuntimeError(
            f"Lighter 账户 index={account_index} 下没有任何已注册 API Key。"
        )

    registered_indices = sorted({
        int(getattr(k, "api_key_index", -1))
        for k in registered
        if getattr(k, "api_key_index", None) is not None
    })

    try_order: list[int] = []
    if preferred not in try_order:
        try_order.append(preferred)
    for idx in registered_indices:
        if idx not in try_order:
            try_order.append(idx)

    last_err = ""
    for idx in try_order:
        signer = lighter.SignerClient(
            url=LIGHTER_BASE,
            api_private_keys={idx: priv},
            account_index=account_index,
        )
        try:
            err = _lighter_check_client(signer)
            if err is None:
                if idx != preferred:
                    log.warning(
                        "LIGHTER_API_KEY_INDEX=%s 无效，已自动匹配 index=%s",
                        preferred, idx,
                    )
                else:
                    log.info("Lighter API Key index=%s 验证通过", idx)
                return idx
            last_err = str(err)
        finally:
            await _close_lighter_signer(signer)

    lines = []
    for k in registered:
        pk = str(getattr(k, "public_key", "") or "")
        lines.append(f"  index={getattr(k, 'api_key_index', '?')} pubkey={pk[:20]}…")
    raise RuntimeError(
        "Lighter 私钥无法匹配 account_index=%s 下任何已注册 API Key。\n"
        "LIGHTER_API_KEY_INDEX=%s\n已注册:\n%s\n最后错误: %s"
        % (account_index, preferred, "\n".join(lines), last_err)
    )


async def fetch_lighter_position(account_index: int, market_id: int) -> Decimal:
    import lighter

    client = lighter.ApiClient(lighter.Configuration(host=LIGHTER_BASE))
    try:
        resp = await lighter.AccountApi(client).account(by="index", value=str(account_index))
        accounts = getattr(resp, "accounts", None) or []
        if not accounts:
            return Decimal("0")
        acc = accounts[0]
        positions = getattr(acc, "positions", None) or []
        for pos in positions:
            mid = int(getattr(pos, "market_id", -1) or getattr(pos, "market_index", -1) or -1)
            if mid != market_id:
                continue
            raw = Decimal(str(getattr(pos, "position", "0") or "0"))
            sign = int(getattr(pos, "sign", 0) or 0)
            if sign < 0 and raw > 0:
                raw = -raw
            elif sign > 0 and raw < 0:
                raw = abs(raw)
            return raw
        return Decimal("0")
    finally:
        await client.close()


class LighterTrader:
    def __init__(self, market: LighterMarket) -> None:
        self.market = market
        self.signer = None
        self.api_key_index = LIGHTER_API_KEY_INDEX
        self._seq = int(time.time() * 1000) % 1_000_000_000
        self._open_base = to_base_amount(ORDER_SIZE_BTC, market.size_decimals)

    async def init(self) -> None:
        if DRY_RUN:
            log.warning("DRY_RUN=1：Lighter 不会真实下单")
            return
        if not LIGHTER_PRIV or not LIGHTER_ACCOUNT_INDEX:
            raise RuntimeError("实盘需要 LIGHTER_API_PRIVATE_KEY 与 LIGHTER_ACCOUNT_INDEX")
        import lighter

        account_index = int(LIGHTER_ACCOUNT_INDEX)
        self.api_key_index = await resolve_lighter_api_key_index(
            account_index, LIGHTER_PRIV, LIGHTER_API_KEY_INDEX,
        )
        self.signer = lighter.SignerClient(
            url=LIGHTER_BASE,
            api_private_keys={self.api_key_index: LIGHTER_PRIV},
            account_index=account_index,
        )
        err = _lighter_check_client(self.signer)
        if err is not None:
            await _close_lighter_signer(self.signer)
            self.signer = None
            raise RuntimeError(f"Lighter check_client 失败: {err}")
        log.info(
            "Lighter Signer 就绪 | account_index=%s api_key_index=%s",
            account_index, self.api_key_index,
        )

    def next_order_index(self) -> int:
        self._seq = (self._seq + 1) % 2_814_749_767_106_55
        return self._seq

    async def market_order(
        self,
        *,
        is_ask: bool,
        qty_btc: Decimal,
        reduce_only: bool,
        tag: str,
        ref_price: Optional[Decimal] = None,
    ) -> None:
        m = self.market
        if qty_btc == ORDER_SIZE_BTC:
            base_amount = self._open_base
        else:
            base_amount = to_base_amount(qty_btc, m.size_decimals)
        if base_amount <= 0:
            raise ValueError(f"Lighter base_amount=0 qty={qty_btc}")
        if DRY_RUN or self.signer is None:
            side = "SELL" if is_ask else "BUY"
            log.info("[DRY_RUN] Lighter MARKET %s qty=%s reduce=%s tag=%s", side, qty_btc, reduce_only, tag)
            return

        idx = self.next_order_index()
        limited = getattr(self.signer, "create_market_order_limited_slippage", None)
        create_market = getattr(self.signer, "create_market_order", None)

        if callable(create_market):
            mark = ref_price if ref_price is not None and ref_price > 0 else m.mark_price
            avg_px = worst_lighter_price(mark, is_ask, m.price_decimals)
            try:
                _, tx_hash, err = await create_market(
                    market_index=m.market_id,
                    client_order_index=idx,
                    base_amount=base_amount,
                    avg_execution_price=avg_px,
                    is_ask=is_ask,
                    reduce_only=reduce_only,
                )
            except TypeError:
                _, tx_hash, err = await create_market(
                    market_index=m.market_id,
                    client_order_index=idx,
                    base_amount=base_amount,
                    avg_execution_price=avg_px,
                    is_ask=is_ask,
                )
        elif callable(limited):
            slip = float(SLIPPAGE_BPS / Decimal("10000"))
            _, tx_hash, err = await limited(
                market_index=m.market_id,
                client_order_index=idx,
                base_amount=base_amount,
                max_slippage=slip,
                is_ask=is_ask,
                reduce_only=reduce_only,
            )
        else:
            mark = ref_price if ref_price is not None and ref_price > 0 else m.mark_price
            avg_px = worst_lighter_price(mark, is_ask, m.price_decimals)
            _, tx_hash, err = await self.signer.create_order(
                market_index=m.market_id,
                client_order_index=idx,
                base_amount=base_amount,
                price=avg_px,
                is_ask=is_ask,
                order_type=self.signer.ORDER_TYPE_MARKET,
                time_in_force=self.signer.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=reduce_only,
                order_expiry=self.signer.DEFAULT_IOC_EXPIRY,
            )
        if err is not None:
            raise RuntimeError(f"Lighter 下单失败: {err} hash={tx_hash}")
        log.info("Lighter 下单成功 tag=%s hash=%s", tag, tx_hash)

    async def close(self) -> None:
        if self.signer is None:
            return
        await _close_lighter_signer(self.signer)
        self.signer = None


# ---------------------------------------------------------------------------
# 策略
# ---------------------------------------------------------------------------
@dataclass
class OpenLot:
    qty: Decimal
    arcus_px: Decimal
    lighter_px: Decimal
    open_spread: Decimal
    open_fee: Decimal  # Arcus 开仓手续费（按本 lot 名义）
    hedge_mode: str    # "a_long" = Arcus多+L空；"a_short" = Arcus空+L多
    ts: float


@dataclass
class SpreadArbBot:
    hub: OrderBookHub
    arcus_client: Optional[ArcusClient]
    arcus_market: ArcusMarket
    lighter: LighterTrader
    running: bool = True
    _armed: bool = False
    _busy: bool = False
    _last_log: float = 0.0
    _local_pos: Decimal = Decimal("0")
    _hedge_mode: Optional[str] = None  # None | "a_long" | "a_short"
    _open_long_since: Optional[float] = None
    _close_long_since: Optional[float] = None
    _open_short_since: Optional[float] = None
    _close_short_since: Optional[float] = None
    _lots: list = field(default_factory=list)
    realized_arcus_pnl: Decimal = Decimal("0")
    realized_lighter_pnl: Decimal = Decimal("0")
    realized_arcus_fee: Decimal = Decimal("0")
    realized_net_pnl: Decimal = Decimal("0")
    trade_rounds: int = 0

    def stop(self, *_a) -> None:
        log.warning("收到停止信号…")
        self.running = False
        self.hub.touch()

    def reset_hold_timers(self, hedge_mode: Optional[str] = None) -> None:
        if hedge_mode in (None, "a_long"):
            self._open_long_since = None
            self._close_long_since = None
        if hedge_mode in (None, "a_short"):
            self._open_short_since = None
            self._close_short_since = None

    async def arcus_position(self) -> Decimal:
        if self.arcus_client is None:
            return Decimal("0")
        pos = await asyncio.to_thread(self.arcus_client.get_positions)
        return parse_arcus_position(pos, self.arcus_market)

    async def lighter_position(self) -> Decimal:
        if not LIGHTER_ACCOUNT_INDEX:
            return Decimal("0")
        return await fetch_lighter_position(int(LIGHTER_ACCOUNT_INDEX), self.lighter.market.market_id)

    async def _read_positions_stable(self, rounds: int = 3) -> Tuple[Decimal, Decimal]:
        a_pos = Decimal("0")
        l_pos = Decimal("0")
        for i in range(max(1, rounds)):
            a_pos, l_pos = await self.arcus_position(), await self.lighter_position()
            if abs(a_pos) > FLAT_EPS_BTC or abs(l_pos) > FLAT_EPS_BTC:
                return a_pos, l_pos
            if i + 1 < rounds:
                await asyncio.sleep(0.35)
        return a_pos, l_pos

    def bbo_ok(self) -> bool:
        now = time.time()
        a, l = self.hub.arcus, self.hub.lighter
        if not a.ready() or not l.ready():
            return False
        if a.updated_at <= 0 or l.updated_at <= 0:
            return False
        if now - a.updated_at > BBO_MAX_AGE_SEC or now - l.updated_at > BBO_MAX_AGE_SEC:
            return False
        if abs(a.updated_at - l.updated_at) > BBO_MAX_SKEW_SEC:
            return False
        return True

    def open_spread(self) -> Optional[Decimal]:
        """Lighter 买一 − Arcus 卖一（开仓可成交价差）。"""
        a, l = self.hub.arcus, self.hub.lighter
        if l.bid is None or a.ask is None:
            return None
        return l.bid - a.ask

    def close_spread(self) -> Optional[Decimal]:
        """Lighter 卖一 − Arcus 买一。"""
        a, l = self.hub.arcus, self.hub.lighter
        if a.bid is None or l.ask is None:
            return None
        return l.ask - a.bid

    def reverse_open_spread(self) -> Optional[Decimal]:
        """Lighter 卖一 − Arcus 买一（策略 B 开仓价差；<= 阈值时开）。"""
        return self.close_spread()

    def long_close_spread(self) -> Optional[Decimal]:
        """策略 A 平仓价差。"""
        return self.close_spread()

    def short_close_spread(self) -> Optional[Decimal]:
        """策略 B 平仓价差（同策略 A 开仓价差）。"""
        return self.open_spread()

    async def _dual_market(
        self,
        *,
        lighter_is_ask: bool,
        arcus_side: str,
        qty: Decimal,
        reduce_only: bool,
        tag: str,
    ) -> Tuple[bool, bool]:
        a, l = self.hub.arcus, self.hub.lighter
        lighter_ref = l.bid if lighter_is_ask else l.ask
        arcus_ref = a.ask if arcus_side == "BUY" else a.bid
        l_ok, a_ok = True, True

        async def _l() -> None:
            nonlocal l_ok
            try:
                await self.lighter.market_order(
                    is_ask=lighter_is_ask,
                    qty_btc=qty,
                    reduce_only=reduce_only,
                    tag=f"l_{tag}",
                    ref_price=lighter_ref,
                )
            except Exception as exc:
                l_ok = False
                log.error("Lighter 下单失败: %s", exc, exc_info=True)

        async def _a() -> None:
            nonlocal a_ok
            if self.arcus_client is None:
                a_ok = False
                return
            try:
                await asyncio.to_thread(
                    self.arcus_client.market_order,
                    self.arcus_market,
                    arcus_side,
                    qty,
                    reduce_only=reduce_only,
                    tag=f"a_{tag}",
                    ref_price=arcus_ref,
                )
            except Exception as exc:
                a_ok = False
                log.error("Arcus 下单失败: %s", exc, exc_info=True)

        await asyncio.gather(_l(), _a())
        if LEG_SETTLE_SEC > 0:
            await asyncio.sleep(LEG_SETTLE_SEC)
        return l_ok, a_ok

    async def _single_leg_lighter(
        self, *, is_ask: bool, qty: Decimal, reduce_only: bool, tag: str,
    ) -> None:
        ref = self.hub.lighter.bid if is_ask else self.hub.lighter.ask
        await self.lighter.market_order(
            is_ask=is_ask, qty_btc=qty, reduce_only=reduce_only, tag=tag, ref_price=ref,
        )
        if LEG_SETTLE_SEC > 0:
            await asyncio.sleep(LEG_SETTLE_SEC)

    async def _single_leg_arcus(
        self, side: str, qty: Decimal, *, reduce_only: bool, tag: str,
    ) -> None:
        if self.arcus_client is None:
            return
        ref = self.hub.arcus.ask if side == "BUY" else self.hub.arcus.bid
        await asyncio.to_thread(
            self.arcus_client.market_order,
            self.arcus_market, side, qty,
            reduce_only=reduce_only, tag=tag, ref_price=ref,
        )
        if LEG_SETTLE_SEC > 0:
            await asyncio.sleep(LEG_SETTLE_SEC)

    def _plan_rebalance(
        self,
        a_pos: Decimal,
        l_pos: Decimal,
        *,
        expect_hedged: bool,
        hedge_mode: Optional[str] = None,
    ) -> Optional[Tuple[str, Any, Decimal, str]]:
        mode = hedge_mode or self._hedge_mode or "a_long"

        if not expect_hedged:
            if abs(a_pos) > FLAT_EPS_BTC and abs(l_pos) <= FLAT_EPS_BTC:
                if a_pos > 0:
                    return ("arcus", "SELL", a_pos, f"残留：平 Arcus 多仓 {a_pos}")
                return ("arcus", "BUY", abs(a_pos), f"残留：平 Arcus 空仓 {a_pos}")
            if abs(l_pos) > FLAT_EPS_BTC and abs(a_pos) <= FLAT_EPS_BTC:
                if l_pos > 0:
                    return ("lighter", True, l_pos, f"残留：平 Lighter 多仓 {l_pos}")
                return ("lighter", False, abs(l_pos), f"残留：平 Lighter 空仓 {l_pos}")
            if abs(a_pos) > FLAT_EPS_BTC and abs(l_pos) > FLAT_EPS_BTC:
                if abs(a_pos) >= abs(l_pos):
                    if a_pos > 0:
                        return ("arcus", "SELL", a_pos, f"残留：平 Arcus {a_pos}")
                    return ("arcus", "BUY", abs(a_pos), f"残留：平 Arcus {a_pos}")
                if l_pos > 0:
                    return ("lighter", True, l_pos, f"残留：平 Lighter {l_pos}")
                return ("lighter", False, abs(l_pos), f"残留：平 Lighter {l_pos}")
            return None

        if mode == "a_short":
            # 期望：Arcus 空 + Lighter 多
            if abs(a_pos) > FLAT_EPS_BTC and abs(l_pos) <= FLAT_EPS_BTC:
                if a_pos < 0:
                    return ("arcus", "BUY", abs(a_pos), f"单腿：平 Arcus 空仓 {a_pos}")
                return ("arcus", "SELL", a_pos, f"单腿：平 Arcus 多仓 {a_pos}")
            if abs(l_pos) > FLAT_EPS_BTC and abs(a_pos) <= FLAT_EPS_BTC:
                if l_pos > 0:
                    return ("lighter", True, l_pos, f"单腿：平 Lighter 多仓 {l_pos}")
                return ("lighter", False, abs(l_pos), f"单腿：平 Lighter 空仓 {l_pos}")
            if a_pos > FLAT_EPS_BTC:
                return ("arcus", "SELL", a_pos, f"平掉 Arcus 意外多仓 {a_pos}")
            if l_pos < -FLAT_EPS_BTC:
                return ("lighter", False, abs(l_pos), f"平掉 Lighter 意外空仓 {l_pos}")

            a_short = max(-a_pos, Decimal("0"))
            l_long = max(l_pos, Decimal("0"))
            diff = a_short - l_long
            if abs(diff) <= FLAT_EPS_BTC:
                return None
            if diff > 0:
                return (
                    "arcus", "BUY", diff,
                    f"减仓 Arcus 多余空仓 {diff}（A空={a_short} / L多={l_long}）",
                )
            return (
                "lighter", True, -diff,
                f"减仓 Lighter 多余多仓 {-diff}（A空={a_short} / L多={l_long}）",
            )

        # 期望：Arcus 多 + Lighter 空
        if abs(a_pos) > FLAT_EPS_BTC and abs(l_pos) <= FLAT_EPS_BTC:
            if a_pos > 0:
                return ("arcus", "SELL", a_pos, f"单腿：平 Arcus 多仓 {a_pos}")
            return ("arcus", "BUY", abs(a_pos), f"单腿：平 Arcus 空仓 {a_pos}")
        if abs(l_pos) > FLAT_EPS_BTC and abs(a_pos) <= FLAT_EPS_BTC:
            if l_pos > 0:
                return ("lighter", True, l_pos, f"单腿：平 Lighter 多仓 {l_pos}")
            return ("lighter", False, abs(l_pos), f"单腿：平 Lighter 空仓 {l_pos}")
        if a_pos < -FLAT_EPS_BTC:
            return ("arcus", "BUY", abs(a_pos), f"平掉 Arcus 意外空仓 {a_pos}")
        if l_pos > FLAT_EPS_BTC:
            return ("lighter", True, l_pos, f"平掉 Lighter 意外多仓 {l_pos}")

        a_long = max(a_pos, Decimal("0"))
        l_short = max(-l_pos, Decimal("0"))
        diff = a_long - l_short
        if abs(diff) <= FLAT_EPS_BTC:
            return None
        if diff > 0:
            return (
                "arcus", "SELL", diff,
                f"减仓 Arcus 多余多仓 {diff}（A多={a_long} / L空={l_short}）",
            )
        return (
            "lighter", False, -diff,
            f"减仓 Lighter 多余空仓 {-diff}（A多={a_long} / L空={l_short}）",
        )

    def _hedged_size(self, a_pos: Decimal, l_pos: Decimal, hedge_mode: str) -> Decimal:
        if hedge_mode == "a_short":
            return min(max(-a_pos, Decimal("0")), max(l_pos, Decimal("0")))
        return min(max(a_pos, Decimal("0")), max(-l_pos, Decimal("0")))

    async def rebalance(
        self,
        *,
        expect_hedged: bool,
        reason: str,
        hedge_mode: Optional[str] = None,
    ) -> None:
        mode = hedge_mode or self._hedge_mode or "a_long"
        if DRY_RUN:
            return
        try:
            before_a, before_l = await self._read_positions_stable(3)
        except Exception as exc:
            log.warning("读持仓失败，跳过平衡: %s", exc)
            return

        action0 = self._plan_rebalance(
            before_a, before_l, expect_hedged=expect_hedged, hedge_mode=mode,
        )
        if action0 is None:
            hedged = self._hedged_size(before_a, before_l, mode) if expect_hedged else Decimal("0")
            self._local_pos = hedged
            return

        log.warning(
            "持仓不平衡 | mode=%s reason=%s | Arcus=%s Lighter=%s | %s",
            mode, reason, before_a, before_l, action0[3],
        )
        await asyncio.to_thread(
            send_telegram,
            f"[Arb45/5 持仓不平衡]\n策略: {mode}\n原因: {reason}\n"
            f"Arcus: {before_a}\nLighter: {before_l}\n将减多的一边…",
        )

        for attempt in range(LEG_REBALANCE_RETRIES):
            try:
                a_pos, l_pos = await self._read_positions_stable(2)
            except Exception as exc:
                log.warning("平衡读仓失败: %s", exc)
                break
            action = self._plan_rebalance(
                a_pos, l_pos, expect_hedged=expect_hedged, hedge_mode=mode,
            )
            if action is None:
                hedged = self._hedged_size(a_pos, l_pos, mode) if expect_hedged else Decimal("0")
                self._local_pos = hedged
                log.warning(
                    "平衡完成 | mode=%s Arcus=%s Lighter=%s | 对冲=%s",
                    mode, a_pos, l_pos, self._local_pos,
                )
                await asyncio.to_thread(
                    send_telegram,
                    f"[Arb45/5 持仓已平衡]\n策略: {mode}\nArcus: {a_pos}\nLighter: {l_pos}\n"
                    f"对冲持仓: {self._local_pos} BTC",
                )
                return
            venue, side_or_ask, qty, desc = action
            log.warning("平衡 %d/%d | %s", attempt + 1, LEG_REBALANCE_RETRIES, desc)
            try:
                if venue == "lighter":
                    await self._single_leg_lighter(
                        is_ask=bool(side_or_ask), qty=qty, reduce_only=True, tag="rebal_l",
                    )
                else:
                    await self._single_leg_arcus(
                        str(side_or_ask), qty, reduce_only=True, tag="rebal_a",
                    )
            except Exception as exc:
                log.error("平衡下单失败: %s", exc, exc_info=True)
                await asyncio.sleep(LEG_SETTLE_SEC)

        try:
            a_pos, l_pos = await self._read_positions_stable(2)
        except Exception as exc:
            log.error("平衡结束后读仓失败: %s", exc)
            return
        hedged = self._hedged_size(a_pos, l_pos, mode) if expect_hedged else Decimal("0")
        self._local_pos = hedged
        if self._plan_rebalance(a_pos, l_pos, expect_hedged=expect_hedged, hedge_mode=mode) is not None:
            msg = (
                f"[Arb45/5 单腿告警]\n策略: {mode}\nArcus: {a_pos}\nLighter: {l_pos}\n"
                f"本地对冲: {self._local_pos} BTC\n请人工检查"
            )
            log.error(msg.replace("\n", " | "))
            await asyncio.to_thread(send_telegram, msg)

    async def is_exchange_flat(self) -> bool:
        try:
            a_pos, l_pos = await self._read_positions_stable(2)
        except Exception:
            return False
        return abs(a_pos) <= FLAT_EPS_BTC and abs(l_pos) <= FLAT_EPS_BTC

    async def sync_position_from_exchange(self) -> None:
        if DRY_RUN:
            log.info("DRY_RUN 启动，本地持仓 0")
            return
        try:
            a_pos, l_pos = await self.arcus_position(), await self.lighter_position()
            if a_pos > FLAT_EPS_BTC and l_pos < -FLAT_EPS_BTC:
                self._hedge_mode = "a_long"
                self._local_pos = min(a_pos, abs(l_pos))
                log.warning("恢复对冲持仓 %.6f BTC（A多+L空）", self._local_pos)
            elif a_pos < -FLAT_EPS_BTC and l_pos > FLAT_EPS_BTC:
                self._hedge_mode = "a_short"
                self._local_pos = min(abs(a_pos), l_pos)
                log.warning("恢复对冲持仓 %.6f BTC（A空+L多）", self._local_pos)
            elif abs(a_pos) > FLAT_EPS_BTC or abs(l_pos) > FLAT_EPS_BTC:
                log.warning("启动时发现非标准仓位 A=%s L=%s，尝试清残留", a_pos, l_pos)
                await self.rebalance(expect_hedged=False, reason="startup_flatten")
            else:
                self._hedge_mode = None
                self._local_pos = Decimal("0")
                log.info("启动时无持仓")
        except Exception as exc:
            log.warning("启动持仓同步失败: %s", exc)

    async def open_long_position(self, trigger: Decimal) -> None:
        a, l = self.hub.arcus, self.hub.lighter
        arcus_px = a.ask or Decimal("0")
        lighter_px = l.bid or Decimal("0")
        l_ok, a_ok = await self._dual_market(
            lighter_is_ask=True,
            arcus_side="BUY",
            qty=ORDER_SIZE_BTC,
            reduce_only=False,
            tag="open_long",
        )
        if not l_ok or not a_ok:
            log.warning("策略A开仓部分失败 | Lighter=%s Arcus=%s | 将平衡", l_ok, a_ok)

        fee_open = arcus_fee(ORDER_SIZE_BTC * arcus_px)
        if l_ok and a_ok:
            self._hedge_mode = "a_long"
            self.realized_arcus_fee += fee_open
            self._lots.append(OpenLot(
                qty=ORDER_SIZE_BTC,
                arcus_px=arcus_px,
                lighter_px=lighter_px,
                open_spread=trigger,
                open_fee=fee_open,
                hedge_mode="a_long",
                ts=time.time(),
            ))
            self._local_pos += ORDER_SIZE_BTC

        await self.rebalance(expect_hedged=True, reason="after_open_long", hedge_mode="a_long")
        self.reset_hold_timers("a_long")

        msg = (
            f"[Arb45/5 策略A开仓]\n"
            f"Lighter 开空 + Arcus 开多\n"
            f"数量: {ORDER_SIZE_BTC} BTC\n"
            f"触发价差 L_bid-A_ask: {trigger} USD (>= {OPEN_SPREAD_THRESHOLD})\n"
            f"估计成交价 A={arcus_px} / L={lighter_px}\n"
            f"Arcus开仓手续费(估): {fee_open:.6f} USD\n"
            f"当前对冲持仓: {self._local_pos} BTC\n"
            f"dry_run={DRY_RUN}"
        )
        log.warning(msg.replace("\n", " | "))
        await asyncio.to_thread(send_telegram, msg)

    async def open_short_position(self, trigger: Decimal) -> None:
        a, l = self.hub.arcus, self.hub.lighter
        arcus_px = a.bid or Decimal("0")
        lighter_px = l.ask or Decimal("0")
        l_ok, a_ok = await self._dual_market(
            lighter_is_ask=False,
            arcus_side="SELL",
            qty=ORDER_SIZE_BTC,
            reduce_only=False,
            tag="open_short",
        )
        if not l_ok or not a_ok:
            log.warning("策略B开仓部分失败 | Lighter=%s Arcus=%s | 将平衡", l_ok, a_ok)

        fee_open = arcus_fee(ORDER_SIZE_BTC * arcus_px)
        if l_ok and a_ok:
            self._hedge_mode = "a_short"
            self.realized_arcus_fee += fee_open
            self._lots.append(OpenLot(
                qty=ORDER_SIZE_BTC,
                arcus_px=arcus_px,
                lighter_px=lighter_px,
                open_spread=trigger,
                open_fee=fee_open,
                hedge_mode="a_short",
                ts=time.time(),
            ))
            self._local_pos += ORDER_SIZE_BTC

        await self.rebalance(expect_hedged=True, reason="after_open_short", hedge_mode="a_short")
        self.reset_hold_timers("a_short")

        msg = (
            f"[Arb45/5 策略B开仓]\n"
            f"Arcus 开空 + Lighter 开多\n"
            f"数量: {ORDER_SIZE_BTC} BTC\n"
            f"触发价差 L_ask-A_bid: {trigger} USD (<= {REVERSE_OPEN_SPREAD_THRESHOLD})\n"
            f"估计成交价 A={arcus_px} / L={lighter_px}\n"
            f"Arcus开仓手续费(估): {fee_open:.6f} USD\n"
            f"当前对冲持仓: {self._local_pos} BTC\n"
            f"dry_run={DRY_RUN}"
        )
        log.warning(msg.replace("\n", " | "))
        await asyncio.to_thread(send_telegram, msg)

    def _realize_close_pnl(
        self,
        qty: Decimal,
        arcus_px: Decimal,
        lighter_px: Decimal,
        hedge_mode: str,
    ) -> Tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
        """FIFO 平仓：返回 (arcus_pnl, lighter_pnl, fee_open, fee_close, net)。"""
        remain = qty
        a_pnl = Decimal("0")
        l_pnl = Decimal("0")
        fee_open = Decimal("0")
        while remain > FLAT_EPS_BTC and self._lots:
            lot: OpenLot = self._lots[0]
            if lot.hedge_mode != hedge_mode:
                break
            take = min(lot.qty, remain)
            ratio = take / lot.qty if lot.qty > 0 else Decimal("1")
            if hedge_mode == "a_short":
                a_pnl += (lot.arcus_px - arcus_px) * take
                l_pnl += (lighter_px - lot.lighter_px) * take
            else:
                a_pnl += (arcus_px - lot.arcus_px) * take
                l_pnl += (lot.lighter_px - lighter_px) * take
            fee_open += lot.open_fee * ratio
            lot.open_fee -= lot.open_fee * ratio
            lot.qty -= take
            remain -= take
            if lot.qty <= FLAT_EPS_BTC:
                self._lots.pop(0)
        fee_close = arcus_fee(qty * arcus_px)
        net = a_pnl + l_pnl - fee_open - fee_close
        return a_pnl, l_pnl, fee_open, fee_close, net

    async def close_long_position(self, reason: str, trigger: Decimal) -> None:
        qty = self._local_pos
        a, l = self.hub.arcus, self.hub.lighter
        arcus_px = a.bid or Decimal("0")
        lighter_px = l.ask or Decimal("0")

        if qty > FLAT_EPS_BTC:
            await self._dual_market(
                lighter_is_ask=False,
                arcus_side="SELL",
                qty=qty,
                reduce_only=True,
                tag="close_long",
            )

        await self.rebalance(expect_hedged=False, reason=f"after_close_long:{reason}")

        a_pnl, l_pnl, fee_open, fee_close, round_net = self._realize_close_pnl(
            qty, arcus_px, lighter_px, "a_long",
        )
        self.realized_arcus_pnl += a_pnl
        self.realized_lighter_pnl += l_pnl
        self.realized_arcus_fee += fee_close
        self.realized_net_pnl += round_net
        self.trade_rounds += 1
        self._local_pos = Decimal("0")
        self._hedge_mode = None
        self.reset_hold_timers("a_long")

        msg = (
            f"[Arb45/5 策略A平仓]\n"
            f"原因: {reason}\n"
            f"数量: {qty} BTC\n"
            f"触发价差 L_ask-A_bid: {trigger} USD (<= {CLOSE_SPREAD_THRESHOLD})\n"
            f"估计平仓价 A={arcus_px} / L={lighter_px}\n"
            f"Arcus盈亏: {a_pnl:+.4f} USD\n"
            f"Lighter盈亏: {l_pnl:+.4f} USD\n"
            f"Arcus手续费(开+平估): {fee_open + fee_close:.6f} USD\n"
            f"本轮净盈亏: {round_net:+.4f} USD\n"
            f"累计净盈亏: {self.realized_net_pnl:+.4f} USD\n"
            f"平仓轮次: {self.trade_rounds}\n"
            f"dry_run={DRY_RUN}"
        )
        log.warning(msg.replace("\n", " | "))
        await asyncio.to_thread(send_telegram, msg)

        if not DRY_RUN:
            await self.rebalance(expect_hedged=False, reason="close_long_residual_check")

    async def close_short_position(self, reason: str, trigger: Decimal) -> None:
        qty = self._local_pos
        a, l = self.hub.arcus, self.hub.lighter
        arcus_px = a.ask or Decimal("0")
        lighter_px = l.bid or Decimal("0")

        if qty > FLAT_EPS_BTC:
            await self._dual_market(
                lighter_is_ask=True,
                arcus_side="BUY",
                qty=qty,
                reduce_only=True,
                tag="close_short",
            )

        await self.rebalance(expect_hedged=False, reason=f"after_close_short:{reason}")

        a_pnl, l_pnl, fee_open, fee_close, round_net = self._realize_close_pnl(
            qty, arcus_px, lighter_px, "a_short",
        )
        self.realized_arcus_pnl += a_pnl
        self.realized_lighter_pnl += l_pnl
        self.realized_arcus_fee += fee_close
        self.realized_net_pnl += round_net
        self.trade_rounds += 1
        self._local_pos = Decimal("0")
        self._hedge_mode = None
        self.reset_hold_timers("a_short")

        msg = (
            f"[Arb45/5 策略B平仓]\n"
            f"原因: {reason}\n"
            f"数量: {qty} BTC\n"
            f"触发价差 L_bid-A_ask: {trigger} USD (>= {OPEN_SPREAD_THRESHOLD})\n"
            f"估计平仓价 A={arcus_px} / L={lighter_px}\n"
            f"Arcus盈亏: {a_pnl:+.4f} USD\n"
            f"Lighter盈亏: {l_pnl:+.4f} USD\n"
            f"Arcus手续费(开+平估): {fee_open + fee_close:.6f} USD\n"
            f"本轮净盈亏: {round_net:+.4f} USD\n"
            f"累计净盈亏: {self.realized_net_pnl:+.4f} USD\n"
            f"平仓轮次: {self.trade_rounds}\n"
            f"dry_run={DRY_RUN}"
        )
        log.warning(msg.replace("\n", " | "))
        await asyncio.to_thread(send_telegram, msg)

        if not DRY_RUN:
            await self.rebalance(expect_hedged=False, reason="close_short_residual_check")

    def maybe_log_status(self) -> None:
        now = time.monotonic()
        if now - self._last_log < LOG_INTERVAL_SEC:
            return
        self._last_log = now
        a, l = self.hub.arcus, self.hub.lighter
        o = self.open_spread()
        c = self.close_spread()
        r = self.reverse_open_spread()
        log.info(
            "mode=%s pos=%s BTC | L bid/ask=%s/%s | A bid/ask=%s/%s | "
            "A开 L_bid-A_ask=%s (>=%s) | A平 L_ask-A_bid=%s (<=%s) | "
            "B开 L_ask-A_bid=%s (<=%s) | B平 L_bid-A_ask=%s (>=%s) | 累计净盈亏=%s",
            self._hedge_mode, self._local_pos,
            l.bid, l.ask, a.bid, a.ask,
            o, OPEN_SPREAD_THRESHOLD, c, CLOSE_SPREAD_THRESHOLD,
            r, REVERSE_OPEN_SPREAD_THRESHOLD, o, OPEN_SPREAD_THRESHOLD,
            self.realized_net_pnl,
        )

    async def _try_hold_trigger(
        self,
        *,
        since_attr: str,
        condition: bool,
        label: str,
        on_trigger,
    ) -> bool:
        """价差持续 SPREAD_HOLD_SEC 后触发；返回是否已处理（含开始计时）。"""
        now = time.monotonic()
        since = getattr(self, since_attr)
        if not condition:
            setattr(self, since_attr, None)
            return False
        if since is None:
            setattr(self, since_attr, now)
            log.info("%s，开始计时 %.1fs…", label, SPREAD_HOLD_SEC)
            return True
        if now - since < SPREAD_HOLD_SEC:
            return True
        self._busy = True
        self.reset_hold_timers()
        try:
            await on_trigger()
        finally:
            self._busy = False
        return True

    async def evaluate(self) -> None:
        if not self._armed or self._busy or not self.running:
            return
        if not self.bbo_ok():
            self.reset_hold_timers()
            return

        o = self.open_spread()
        c = self.close_spread()
        r = self.reverse_open_spread()
        if o is None or c is None or r is None:
            return

        pos = self._local_pos
        mode = self._hedge_mode

        if mode == "a_long" and pos > FLAT_EPS_BTC:
            if c <= CLOSE_SPREAD_THRESHOLD:
                async def _close() -> None:
                    c2 = self.long_close_spread()
                    if c2 is not None and c2 <= CLOSE_SPREAD_THRESHOLD and self.bbo_ok():
                        await self.close_long_position(
                            f"L_ask-A_bid={c2} <= {CLOSE_SPREAD_THRESHOLD}", c2,
                        )
                    else:
                        self.reset_hold_timers("a_long")

                if await self._try_hold_trigger(
                    since_attr="_close_long_since",
                    condition=True,
                    label=f"策略A平仓条件 L_ask-A_bid={c}",
                    on_trigger=_close,
                ):
                    self._open_long_since = None
                    self._open_short_since = None
                    return
            else:
                self._close_long_since = None
            return

        if mode == "a_short" and pos > FLAT_EPS_BTC:
            if o >= OPEN_SPREAD_THRESHOLD:
                async def _close() -> None:
                    o2 = self.short_close_spread()
                    if o2 is not None and o2 >= OPEN_SPREAD_THRESHOLD and self.bbo_ok():
                        await self.close_short_position(
                            f"L_bid-A_ask={o2} >= {OPEN_SPREAD_THRESHOLD}", o2,
                        )
                    else:
                        self.reset_hold_timers("a_short")

                if await self._try_hold_trigger(
                    since_attr="_close_short_since",
                    condition=True,
                    label=f"策略B平仓条件 L_bid-A_ask={o}",
                    on_trigger=_close,
                ):
                    self._open_long_since = None
                    self._open_short_since = None
                    return
            else:
                self._close_short_since = None
            return

        if mode is not None or pos > FLAT_EPS_BTC:
            return

        # 空仓：策略 A 开仓
        if o >= OPEN_SPREAD_THRESHOLD and pos + ORDER_SIZE_BTC <= MAX_POSITION_BTC:
            async def _open_long() -> None:
                o2 = self.open_spread()
                if (
                    o2 is not None
                    and o2 >= OPEN_SPREAD_THRESHOLD
                    and self.bbo_ok()
                    and self._hedge_mode is None
                    and self._local_pos + ORDER_SIZE_BTC <= MAX_POSITION_BTC
                ):
                    await self.open_long_position(o2)
                else:
                    self.reset_hold_timers("a_long")

            if await self._try_hold_trigger(
                since_attr="_open_long_since",
                condition=True,
                label=f"策略A开仓条件 L_bid-A_ask={o}",
                on_trigger=_open_long,
            ):
                self._open_short_since = None
                return
        else:
            self._open_long_since = None

        # 空仓：策略 B 开仓（需两边无仓位）
        if r <= REVERSE_OPEN_SPREAD_THRESHOLD and pos + ORDER_SIZE_BTC <= MAX_POSITION_BTC:
            async def _open_short() -> None:
                r2 = self.reverse_open_spread()
                if not (
                    r2 is not None
                    and r2 <= REVERSE_OPEN_SPREAD_THRESHOLD
                    and self.bbo_ok()
                    and self._hedge_mode is None
                    and self._local_pos + ORDER_SIZE_BTC <= MAX_POSITION_BTC
                ):
                    self.reset_hold_timers("a_short")
                    return
                if not DRY_RUN and not await self.is_exchange_flat():
                    log.info("策略B开仓跳过：交易所仍有仓位")
                    self.reset_hold_timers("a_short")
                    return
                await self.open_short_position(r2)

            if await self._try_hold_trigger(
                since_attr="_open_short_since",
                condition=True,
                label=f"策略B开仓条件 L_ask-A_bid={r}",
                on_trigger=_open_short,
            ):
                self._open_long_since = None
                return
        else:
            self._open_short_since = None

    async def run_strategy_loop(self) -> None:
        try:
            await self.lighter.init()
            if self.arcus_client and not DRY_RUN:
                await asyncio.to_thread(self.arcus_client.ensure_api_key_registered)
            log.info("正在同步启动持仓…")
            await self.sync_position_from_exchange()
        except Exception as exc:
            log.error("策略启动失败（WebSocket 仍在运行）: %s", exc, exc_info=True)
            return
        self._armed = True
        start_msg = (
            f"[Arb45/5 启动]\n"
            f"策略A 开: L_bid-A_ask >= {OPEN_SPREAD_THRESHOLD} USD 持续 {SPREAD_HOLD_SEC}s\n"
            f"策略A 平: L_ask-A_bid <= {CLOSE_SPREAD_THRESHOLD} USD 持续 {SPREAD_HOLD_SEC}s\n"
            f"策略B 开: L_ask-A_bid <= {REVERSE_OPEN_SPREAD_THRESHOLD} USD 持续 {SPREAD_HOLD_SEC}s（空仓）\n"
            f"策略B 平: L_bid-A_ask >= {OPEN_SPREAD_THRESHOLD} USD 持续 {SPREAD_HOLD_SEC}s\n"
            f"数量: {ORDER_SIZE_BTC} BTC | dry_run={DRY_RUN}"
        )
        log.info(start_msg.replace("\n", " | "))
        await asyncio.to_thread(send_telegram, start_msg)

        while self.running:
            self.maybe_log_status()
            try:
                await asyncio.wait_for(self.hub._event.wait(), timeout=LOG_INTERVAL_SEC)
            except asyncio.TimeoutError:
                pass
            self.hub._event.clear()
            # 持仓期间也靠 WS 驱动 evaluate；此处兜底轮询 hold 计时
            try:
                await self.evaluate()
            except Exception as exc:
                log.error("evaluate 异常: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# WebSocket 行情
# ---------------------------------------------------------------------------
def _parse_level(level: Any) -> Optional[Decimal]:
    if level is None:
        return None
    if isinstance(level, dict):
        raw = level.get("price")
    else:
        raw = level
    if raw in (None, ""):
        return None
    try:
        px = Decimal(str(raw))
        return px if px > 0 else None
    except Exception:
        return None


async def arcus_bbo_feed(
    hub: OrderBookHub,
    market_name: str,
    shutdown: asyncio.Event,
    bot: SpreadArbBot,
) -> None:
    if websockets is None:
        raise RuntimeError("请安装 websockets: pip install websockets")
    sub = {"type": "subscribe", "channel": "bbo", "id": market_name}
    while not shutdown.is_set():
        try:
            async with websockets.connect(ARCUS_WS, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(sub))
                log.info("Arcus BBO WebSocket 已连接 %s", market_name)
                async for raw in ws:
                    if shutdown.is_set():
                        return
                    msg = json.loads(raw)
                    mtype = msg.get("type")
                    if mtype not in ("channel_data", "subscribed"):
                        continue
                    contents = msg.get("contents") or {}
                    bid = _parse_level(contents.get("bestBid"))
                    ask = _parse_level(contents.get("bestAsk"))
                    changed = False
                    if bid is not None:
                        hub.arcus.bid = bid
                        changed = True
                    if ask is not None:
                        hub.arcus.ask = ask
                        changed = True
                    if changed:
                        hub.arcus.updated_at = time.time()
                        try:
                            await bot.evaluate()
                        except Exception as exc:
                            log.error("触发下单异常: %s", exc, exc_info=True)
                        hub.touch()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if shutdown.is_set():
                return
            log.warning("Arcus WS 断开: %s，3s 后重连", exc)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=3)
                return
            except asyncio.TimeoutError:
                pass


async def lighter_bbo_feed(
    hub: OrderBookHub,
    market_id: int,
    shutdown: asyncio.Event,
    bot: SpreadArbBot,
) -> None:
    """订阅 Lighter ticker(BBO)。不用 order_book 增量（会读偏最优价）。"""
    if websockets is None:
        raise RuntimeError("请安装 websockets: pip install websockets")
    sub = {"type": "subscribe", "channel": f"ticker/{market_id}"}
    while not shutdown.is_set():
        try:
            async with websockets.connect(LIGHTER_WS, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(sub))
                log.info("Lighter BBO(ticker) WebSocket 已连接 market_id=%s", market_id)
                async for raw in ws:
                    if shutdown.is_set():
                        return
                    msg = json.loads(raw)
                    mtype = str(msg.get("type") or "")
                    if "ticker" not in mtype:
                        continue
                    ticker = msg.get("ticker") or {}
                    bid = _parse_level(ticker.get("b"))
                    ask = _parse_level(ticker.get("a"))
                    changed = False
                    if bid is not None:
                        hub.lighter.bid = bid
                        changed = True
                    if ask is not None:
                        hub.lighter.ask = ask
                        changed = True
                    if changed:
                        hub.lighter.updated_at = time.time()
                        try:
                            await bot.evaluate()
                        except Exception as exc:
                            log.error("触发下单异常: %s", exc, exc_info=True)
                        hub.touch()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if shutdown.is_set():
                return
            log.warning("Lighter WS 断开: %s，3s 后重连", exc)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=3)
                return
            except asyncio.TimeoutError:
                pass


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------
async def main_async() -> int:
    if websockets is None:
        log.error("缺少依赖 websockets，请运行: pip install websockets")
        return 1

    if not DRY_RUN:
        if not ARCUS_ADDRESS or not ARCUS_PRIV:
            log.error("实盘需要 ARCUS_ADDRESS 与 ARCUS_API_PRIVATE_KEY")
            return 1
        if not LIGHTER_PRIV or not LIGHTER_ACCOUNT_INDEX:
            log.error("实盘需要 LIGHTER_API_PRIVATE_KEY 与 LIGHTER_ACCOUNT_INDEX")
            return 1

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("未配置 Telegram，开平仓/盈亏推送将跳过")

    arcus_market = load_arcus_market(ARCUS_MARKET)
    lighter_market = fetch_lighter_btc_market()
    log.info(
        "市场 | Arcus %s id=%s | Lighter %s id=%s mark=%s/%s",
        arcus_market.name, arcus_market.market_id,
        lighter_market.symbol, lighter_market.market_id,
        arcus_market.mark_price, lighter_market.mark_price,
    )

    if ORDER_SIZE_BTC < arcus_market.min_order_size:
        log.warning("ORDER_SIZE_BTC=%s < Arcus min=%s", ORDER_SIZE_BTC, arcus_market.min_order_size)
    if ORDER_SIZE_BTC < lighter_market.min_base_amount:
        log.warning("ORDER_SIZE_BTC=%s < Lighter min=%s", ORDER_SIZE_BTC, lighter_market.min_base_amount)

    arcus_client: Optional[ArcusClient] = None
    if ARCUS_PRIV and ARCUS_ADDRESS:
        arcus_client = ArcusClient(ARCUS_ADDRESS, ARCUS_PRIV, ARCUS_ACCOUNT_INDEX)

    hub = OrderBookHub()
    bot = SpreadArbBot(
        hub=hub,
        arcus_client=arcus_client,
        arcus_market=arcus_market,
        lighter=LighterTrader(lighter_market),
    )

    shutdown = asyncio.Event()
    tasks: list[asyncio.Task] = []

    def request_shutdown(*_a) -> None:
        log.warning("收到停止信号，正在退出…")
        bot.running = False
        hub.touch()
        shutdown.set()
        for t in tasks:
            t.cancel()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
        except NotImplementedError:
            pass

    tasks.extend([
        asyncio.create_task(arcus_bbo_feed(hub, ARCUS_MARKET, shutdown, bot)),
        asyncio.create_task(lighter_bbo_feed(hub, lighter_market.market_id, shutdown, bot)),
        asyncio.create_task(bot.run_strategy_loop()),
    ])
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        bot.running = False
        shutdown.set()
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await bot.lighter.close()
        end_msg = (
            f"[Arb45/5 退出]\n"
            f"累计净盈亏: {bot.realized_net_pnl:+.4f} USD\n"
            f"平仓轮次: {bot.trade_rounds}\n"
            f"剩余对冲仓: {bot._local_pos} BTC ({bot._hedge_mode or 'flat'})"
        )
        log.info(end_msg.replace("\n", " | "))
        await asyncio.to_thread(send_telegram, end_msg)
        log.info("脚本已退出")
    return 0


def main() -> int:
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        log.info("用户中断")
        return 0


if __name__ == "__main__":
    sys.exit(main())
