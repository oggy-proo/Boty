# ====================== NODE CLIENT ======================
# 16 Heroku Nodes + 20 VPS Nodes - Load Balancer
# =========================================================

from __future__ import annotations
import asyncio, aiohttp, time, re, random
from dataclasses import dataclass, field
from urllib.parse import quote
from yarl import URL as _URL

# ====================== CONFIG ============================
_CHECK_TIMEOUT   = 25
_PROBE_TIMEOUT   = 5
_WORKERS_CAP     = 600
_CONN_PER_HOST   = 64
_DNS_TTL         = 600
_KEEPALIVE       = 120
_COOLDOWN_STEPS  = [30, 60, 120]
_HEALTH_STALE    = 8
_SOFT_CAP        = 0.90
_JITTER          = 0.001

# ====================== NODE LISTS ========================
_VPS_CONFIGS = [
    {
        "name": f"VPS-{i:02d}",
        "url": f"http://5.83.136.15:{8080 + i}",
        "ssh_host": "5.83.136.15", "ssh_port": 22, "ssh_user": "root", "ssh_pass": "",
        "type": "vps", "weight": 1.0,
    }
    for i in range(1, 21)
]

_HEROKU_CONFIGS = [
    {"name": "HRK-1",  "url": "https://shopify-node-01-fa804790b066.herokuapp.com"},
    {"name": "HRK-2",  "url": "https://shopify-node-02-5d80fe0fae9d.herokuapp.com"},
    {"name": "HRK-3",  "url": "https://shopify-node-03-7b915aee260c.herokuapp.com"},
    {"name": "HRK-4",  "url": "https://shopify-node-04-9f54617b5f51.herokuapp.com"},
    {"name": "HRK-5",  "url": "https://shopify-node-05-e694c739af29.herokuapp.com"},
    {"name": "HRK-6",  "url": "https://shopify-node-06-4a2c59639117.herokuapp.com"},
    {"name": "HRK-7",  "url": "https://shopify-node-07-af78dc9279cc.herokuapp.com"},
    {"name": "HRK-8",  "url": "https://shopify-node-08-c3cbe9a9bc28.herokuapp.com"},
    {"name": "HRK-9",  "url": "https://shopify-node-09-d8c5a66ff0cc.herokuapp.com"},
    {"name": "HRK-10", "url": "https://shopify-node-10-74a7f7b6baa2.herokuapp.com"},
    {"name": "HRK-11", "url": "https://shopify-node-11-10fe41f6dad6.herokuapp.com"},
    {"name": "HRK-12", "url": "https://shopify-node-12-0c4c280f0204.herokuapp.com"},
    {"name": "HRK-13", "url": "https://shopify-node-13-37a98de842ac.herokuapp.com"},
    {"name": "HRK-14", "url": "https://shopify-node-14-ac40e074c105.herokuapp.com"},
    {"name": "HRK-15", "url": "https://shopify-node-15-428b56f43e62.herokuapp.com"},
    {"name": "HRK-16", "url": "https://shopify-node-16-b1cd83a7d6da.herokuapp.com"},
]

# ====================== NODE STATE ========================
@dataclass
class _Node:
    name:      str
    url:       str
    node_type: str   = "vps"
    weight:    float = 1.0
    ssh_host:  str   = ""
    ssh_port:  int   = 0
    ssh_user:  str   = ""
    ssh_pass:  str   = ""

    enabled:              bool  = True
    active:               int   = 0
    workers_busy:         int   = 0
    workers_cap:          int   = _WORKERS_CAP
    total:                int   = 0
    fail_streak:          int   = 0
    timeout_streak:       int   = 0
    consecutive_timeouts: int   = 0
    cooldown_until:       float = 0.0
    health_ts:            float = 0.0
    probing:              bool  = False
    last_ok:              float = field(default_factory=time.monotonic)
    avg_rtt_ms:           float = 0.0
    warmed:               bool  = False

    @property
    def probe_timeout(self) -> int: return 5
    @property
    def check_timeout(self) -> int: return 20

    @property
    def on_cooldown(self) -> bool: return time.monotonic() < self.cooldown_until
    @property
    def cooldown_expired(self) -> bool: return self.cooldown_until > 0.0 and not self.on_cooldown and not self.probing
    @property
    def available(self) -> bool: return self.enabled and not self.on_cooldown and not self.probing
    @property
    def cooldown_remaining(self) -> int: return max(0, int(self.cooldown_until - time.monotonic()))

    def start_cooldown(self):
        self.consecutive_timeouts += 1
        steps = [15, 30, 60] if self.node_type == "vps" else _COOLDOWN_STEPS
        secs = steps[min(self.consecutive_timeouts - 1, len(steps) - 1)]
        self.cooldown_until = time.monotonic() + secs

    def mark_ok(self):
        self.last_ok = time.monotonic()
        self.fail_streak = self.timeout_streak = self.consecutive_timeouts = 0
        self.cooldown_until = 0.0
        self.probing = False
        self.warmed = True

    def update_rtt(self, ms: float):
        self.avg_rtt_ms = ms if self.avg_rtt_ms == 0.0 else 0.8 * self.avg_rtt_ms + 0.2 * ms

    def record_timeout(self):
        self.active = max(0, self.active - 1)
        self.fail_streak += 1
        self.timeout_streak += 1
        if self.timeout_streak >= (2 if self.node_type == "vps" else 1):
            self.timeout_streak = 0
            self.start_cooldown()

    def record_error(self):
        self.active = max(0, self.active - 1)
        self.fail_streak += 1
        self.timeout_streak = 0
        if self.fail_streak >= (3 if self.node_type == "vps" else 2):
            self.start_cooldown()

# ====================== HELPERS ===========================
def _to_api_proxy(proxy: str) -> str:
    p = (proxy or "").strip()
    if not p: return ""
    p = re.sub(r'^(https?|socks[45])://', '', p)
    if '@' in p:
        creds, hostport = p.rsplit('@', 1)
        parts = hostport.split(':')
        host, port = parts[0], (parts[1] if len(parts) > 1 else "8080")
        cp = creds.split(':', 1)
        user, pw = cp[0], (cp[1] if len(cp) > 1 else "")
        return f"{host}:{port}:{user}:{pw}"
    return p

def _parse_price(raw: str) -> tuple:
    parts = (raw or "").strip().split()
    if not parts: return "0", "USD"
    return parts[0], (parts[1] if len(parts) > 1 else "USD")

def _parse_bool(val) -> bool:
    return str(val).strip().lower() == "true"

# ====================== MANAGER ===========================
class NodeManager:
    def __init__(self, configs: list, pool_type: str):
        self.pool_type = pool_type
        self._nodes = [
            _Node(
                name=c["name"], url=c["url"].rstrip('/'),
                ssh_host=c.get("ssh_host", ""), ssh_port=c.get("ssh_port", 0),
                ssh_user=c.get("ssh_user", ""), ssh_pass=c.get("ssh_pass", ""),
                node_type=pool_type, weight=c.get("weight", 1.0),
            )
            for c in configs
        ]
        self._session = None

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            conn = aiohttp.TCPConnector(
                limit=0, limit_per_host=_CONN_PER_HOST, ssl=False,
                ttl_dns_cache=_DNS_TTL, use_dns_cache=True,
                keepalive_timeout=_KEEPALIVE, enable_cleanup_closed=True,
            )
            self._session = aiohttp.ClientSession(
                connector=conn, connector_owner=True,
                timeout=aiohttp.ClientTimeout(total=_CHECK_TIMEOUT, connect=5, sock_connect=5, sock_read=_CHECK_TIMEOUT),
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def probe_all(self) -> list[str]:
        sess = await self._sess()
        failed = []
        async def _ping(node: _Node):
            if not node.enabled: return
            try:
                async with sess.get(f"{node.url}/health", timeout=aiohttp.ClientTimeout(total=node.probe_timeout), ssl=False) as r:
                    if r.status < 500:
                        d = await r.json(content_type=None)
                        node.workers_busy = int(d.get("workers_busy", 0))
                        node.workers_cap = int(d.get("workers_cap", _WORKERS_CAP))
                        node.health_ts = time.monotonic()
                        node.warmed = True
                        node.mark_ok()
                        return
            except: pass
            node.fail_streak += 1
            if node.fail_streak >= 2:
                node.start_cooldown()
                failed.append(node.name)
        await asyncio.gather(*[_ping(n) for n in self._nodes])
        return failed

    def get_total_active(self) -> int:
        return sum(n.active for n in self._nodes if n.available)

    def get_total_capacity(self) -> int:
        return sum(n.workers_cap for n in self._nodes if n.available)

    def summary(self) -> dict:
        return {
            "total": len(self._nodes),
            "available": sum(1 for n in self._nodes if n.available),
            "cooldown": sum(1 for n in self._nodes if n.on_cooldown),
            "disabled": sum(1 for n in self._nodes if not n.enabled),
        }

    def _pick(self, exclude: set = None) -> _Node:
        pool = [n for n in self._nodes if not (exclude and n.url in exclude)]
        if not pool: pool = list(self._nodes)
        avail = [n for n in pool if n.available]
        if not avail:
            avail = [n for n in pool if n.enabled]
            if avail: avail.sort(key=lambda n: n.cooldown_until)
            else: avail = pool
        def _score(n: _Node) -> float:
            cap  = max(n.workers_cap, 1)
            busy = n.workers_busy if time.monotonic() - n.health_ts < _HEALTH_STALE else n.active
            util = min(busy / cap, 1.0)
            score = (util ** 2) * n.weight
            score += min(n.fail_streak * 0.10, 0.6)
            score += min(n.avg_rtt_ms / 30_000, 0.2)
            return score + random.uniform(0, _JITTER)
        under_cap  = [n for n in avail if (n.workers_busy / max(n.workers_cap, 1)) < _SOFT_CAP]
        pool_final = under_cap if under_cap else avail
        return sorted(pool_final, key=_score)[0]

    async def check(self, card: str, proxy: str = "", site: str = "", *, retries: int = 2, single: bool = False) -> dict:
        """
        single=True  → ONE node, one attempt, one shot. Best for /sh.
        single=False → up to `retries+1` nodes with failover. For /msh.
        """
        if single:
            retries = 0   # exactly 1 attempt

        proxy_str = _to_api_proxy(proxy)
        tried, last_err = set(), "NODE_UNAVAILABLE"
        sess = await self._sess()

        for attempt in range(retries + 1):
            node = self._pick(exclude=tried)
            if node is None: break
            tried.add(node.url)

            params = card
            if site:      params += f"&site={quote(site, safe='')}"
            if proxy_str: params += f"&proxy={quote(proxy_str, safe=':@')}"
            req_url = f"{node.url}/?{params}"

            node.active += 1
            node.total  += 1
            t0 = time.monotonic()

            try:
                async with sess.get(
                    _URL(req_url, encoded=True),
                    timeout=aiohttp.ClientTimeout(total=node.check_timeout, connect=5, sock_connect=5, sock_read=node.check_timeout),
                    ssl=False,
                ) as r:
                    if r.status == 503:
                        node.active = max(0, node.active - 1)
                        last_err = f"503 on {node.name}"
                        if single: break
                        await asyncio.sleep(0.05)
                        continue

                    body = await r.json(content_type=None)
                    elapsed_ms = (time.monotonic() - t0) * 1000
                    node.active = max(0, node.active - 1)
                    node.mark_ok()
                    node.update_rtt(elapsed_ms)

                    resp = str(body.get("Response", "UNKNOWN"))
                    gate = str(body.get("Gate", "Shopify Payments"))
                    chg  = _parse_bool(body.get("Charged", "False"))
                    apv  = _parse_bool(body.get("Approved", "False"))
                    p_raw = str(body.get("Price", "0 USD"))
                    price, currency = _parse_price(p_raw)

                    return {
                        "Response": resp, "Gate": gate, "Charged": str(chg), "Approved": str(apv),
                        "Price": p_raw, "Time": str(body.get("Time", "")), "CC": str(body.get("CC", card)),
                        "success": chg or apv, "message": resp, "gateway": gate,
                        "price": price, "currency": currency, "_node": node.name, "_rtt_ms": round(elapsed_ms)
                    }

            except asyncio.TimeoutError:
                node.record_timeout()
                last_err = f"Timeout on {node.name}"
            except Exception as e:
                node.record_error()
                last_err = str(e)[:80]

            if single: break

        return {"Response": last_err, "Gate": "Unknown", "Charged": "False", "Approved": "False", "Price": "0 USD", "Time": "", "CC": card, "success": False, "message": last_err, "gateway": "Unknown", "price": "0", "currency": "USD", "_node": "none", "_rtt_ms": 0}

    def set_enabled(self, node_name: str, enabled: bool):
        for n in self._nodes:
            if n.name == node_name:
                n.enabled = enabled
                if enabled:
                    n.cooldown_until = 0.0
                    n.probing = False
                    n.consecutive_timeouts = 0
                    n.fail_streak = 0
                return

    def status(self) -> list[dict]:
        return [
            {
                "name": n.name, "url": n.url, "enabled": n.enabled, "available": n.available,
                "on_cooldown": n.on_cooldown, "active": n.active, "workers_busy": n.workers_busy,
                "workers_cap": n.workers_cap, "cooldown_sec": n.cooldown_remaining, "avg_rtt_ms": round(n.avg_rtt_ms),
            }
            for n in self._nodes
        ]

# ====================== SINGLETONS ========================
NODE_MANAGER_VPS = NodeManager(_VPS_CONFIGS, "vps")
NODE_MANAGER_HEROKU = NodeManager(_HEROKU_CONFIGS, "heroku")

def set_load_handling_vps(enabled: bool): pass
def set_load_handling_heroku(enabled: bool): pass
