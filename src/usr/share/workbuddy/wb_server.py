import os, sys, json, time, ssl, urllib.request, threading, collections, uuid, re
from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler

# 【A2 改造】越狱区专用版: 端口 8318 + 路径自动推导(不依赖 iSH 的 /var/minis)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.environ.get("WB_PORT", "8318"))
TOKENS_FILE = os.path.join(BASE_DIR, "wb_tokens.json")

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

account_pool = []
current_idx = 0
request_logs = []
# 【H6 新增】共享状态互斥锁
# 真因: 改多线程后 request_logs / current_idx 成为跨线程共享的可变状态,
#       两个并发请求可能同时读到同一个 current_idx → 账号跳号、负载不均;
#       request_logs 的"读-改-写"组合也可能让列表短暂超长。
_lock = threading.Lock()

# ============================================================================
# 【A 组】累计计数持久化 —— 对照上游 workbuddy2api-panel internal/usage/usage.go
#   真因: calls/errors 原为纯内存计数, 服务每次重启(含 LaunchDaemon KeepAlive 拉起)
#         全部归零, 面板「累计请求」只代表「本进程启动以来」, 数字不可信。
#   配方(与上游一致): 启动 load() 恢复 + 写入口置脏 + 后台 30s 防抖刷盘
#                    + 原子替换(tmp→os.replace) 防断电半截文件。
# ============================================================================
COUNTER_FILE = os.path.join(BASE_DIR, "counters.json")
# 【V 组】请求记录持久化（与 counters 同配方）：重启后「请求记录」不清零
REQLOG_FILE = os.path.join(BASE_DIR, "reqlogs.json")
REQLOG_MAX = 30                # 【X】内存/面板保留条数（前端只显示最新 30）
# 【X 组】明细全量存档：每来一条「追加」写当天 JSONL，不清空、不重写整个文件。
#   内存仍只留 REQLOG_MAX 条（面板清爽），历史明细按天归档。
#   格式：reqhistory/YYYY-MM-DD.jsonl（每行一条 JSON），保留 REQHIST_KEEP_DAYS 天。
REQHIST_DIR = os.path.join(BASE_DIR, "reqhistory")
REQHIST_KEEP_DAYS = 30
_reqlog_dirty = False
COUNTER_FLUSH_INTERVAL = 30              # 秒（上游 flushInterval = 30s）
_counters = {}                           # {phone: {"calls": n, "errors": m}}
_counters_dirty = False
_counters_lock = threading.Lock()

def load_counters():
    """启动时从 counters.json 恢复累计计数（无文件则以空开始）。"""
    global _counters
    try:
        if os.path.exists(COUNTER_FILE):
            with open(COUNTER_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            _counters = (d or {}).get("accounts") or {}
            log(f"已恢复累计计数: {len(_counters)} 个账号（{COUNTER_FILE}）")
        else:
            log("累计计数文件不存在, 从零开始")
    except Exception as e:
        log(f"累计计数读取失败(从零开始): {e}")

def flush_counters(force=False):
    """原子替换落盘（tmp → os.replace）；无变更不写（上游 flush(force) 等价）。"""
    global _counters_dirty
    with _counters_lock:
        if not _counters_dirty and not force:
            return
        snap = {"version": 1,
                "saved": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "accounts": _counters}
        _counters_dirty = False
    try:
        tmp = COUNTER_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snap, f, ensure_ascii=False, indent=1)
        os.replace(tmp, COUNTER_FILE)     # 原子替换：断电不产生半截文件
    except Exception as e:
        log(f"累计计数落盘失败: {e}")

def append_history(rec):
    """【X 组】把一条明细追加写入当天 JSONL（O(1) 追加，不重写文件）。"""
    try:
        if not os.path.isdir(REQHIST_DIR):
            os.makedirs(REQHIST_DIR, exist_ok=True)
        day = time.strftime('%Y-%m-%d')
        fp = os.path.join(REQHIST_DIR, day + ".jsonl")
        row = dict(rec)
        row["ts"] = time.strftime('%Y-%m-%d %H:%M:%S')
        with open(fp, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as e:
        log(f"明细存档失败: {e}")

def reqhist_stats():
    """【X 组】统计存档：天数 + 总条数 + 今日条数（供面板显示）。"""
    info = {"days": 0, "total": 0, "today": 0}
    try:
        if not os.path.isdir(REQHIST_DIR):
            return info
        today = time.strftime('%Y-%m-%d') + ".jsonl"
        files = [f for f in os.listdir(REQHIST_DIR) if f.endswith(".jsonl")]
        info["days"] = len(files)
        for f in files:
            fp = os.path.join(REQHIST_DIR, f)
            try:
                n = sum(1 for _ in open(fp, "r", encoding="utf-8", errors="replace"))
            except Exception:
                n = 0
            info["total"] += n
            if f == today:
                info["today"] = n
    except Exception:
        pass
    return info

def prune_history():
    """【X 组】清理超过 REQHIST_KEEP_DAYS 天的明细文件。"""
    try:
        if not os.path.isdir(REQHIST_DIR):
            return 0
        cutoff = time.time() - REQHIST_KEEP_DAYS * 86400
        rm = 0
        for f in os.listdir(REQHIST_DIR):
            if not f.endswith(".jsonl"):
                continue
            fp = os.path.join(REQHIST_DIR, f)
            try:
                if os.path.getmtime(fp) < cutoff:
                    os.remove(fp); rm += 1
            except Exception:
                pass
        if rm:
            log(f"明细存档已清理 {rm} 个过期文件（保留 {REQHIST_KEEP_DAYS} 天）")
        return rm
    except Exception:
        return 0

def storage_status():
    """【Z2】持久化存储状态：让面板能一眼看到「什么数据落盘了」。"""
    def _f(path, label, note):
        try:
            if os.path.exists(path):
                st = os.stat(path)
                return {"file": label, "note": note, "ok": True,
                        "size": st.st_size,
                        "saved": time.strftime('%H:%M:%S', time.localtime(st.st_mtime))}
        except Exception:
            pass
        return {"file": label, "note": note, "ok": False, "size": 0, "saved": "—"}

    items = [
        _f(COUNTER_FILE, "counters.json", "累计请求/错误 · 惩罚态(禁用/熔断/暂停)"),
        _f(MSTATS_FILE, "modelstats.json", "模型统计 · 账号耗时排行"),
        _f(REQLOG_FILE, "reqlogs.json", "请求记录（面板显示的最新 30 条）"),
    ]
    # 明细存档目录
    tot = 0; days = 0; last = "—"
    try:
        if os.path.isdir(REQHIST_DIR):
            fs = [f for f in os.listdir(REQHIST_DIR) if f.endswith(".jsonl")]
            days = len(fs)
            for f in fs:
                fp = os.path.join(REQHIST_DIR, f)
                tot += os.path.getsize(fp)
                mt = os.path.getmtime(fp)
                if last == "—" or mt > float(last):
                    pass
            if fs:
                newest = max(os.path.getmtime(os.path.join(REQHIST_DIR, f)) for f in fs)
                last = time.strftime('%H:%M:%S', time.localtime(newest))
    except Exception:
        pass
    items.append({"file": "reqhistory/", "note": "明细全量存档（%d 天 · 保留 %d 天）" % (days, REQHIST_KEEP_DAYS),
                  "ok": days > 0, "size": tot, "saved": last})
    items.append(_f(TOKENS_FILE, "wb_tokens.json", "登录凭据（由配置流程维护）"))
    return items

def load_reqlogs():
    """【V 组】启动时恢复请求记录（最多 REQLOG_MAX 条）。"""
    global request_logs
    try:
        if os.path.exists(REQLOG_FILE):
            with open(REQLOG_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            logs = (d or {}).get("logs") or []
            with _lock:
                request_logs = list(logs)[:REQLOG_MAX]
            log(f"已恢复请求记录: {len(request_logs)} 条（{REQLOG_FILE}）")
        else:
            log("请求记录文件不存在, 从零开始")
    except Exception as e:
        log(f"请求记录读取失败(从零开始): {e}")

def flush_reqlogs(force=False):
    """【V 组】原子替换落盘（tmp → os.replace）；无变更不写。"""
    global _reqlog_dirty
    with _lock:
        if not _reqlog_dirty and not force:
            return
        snap = {"version": 1,
                "saved": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "logs": list(request_logs)}
        _reqlog_dirty = False
    try:
        tmp = REQLOG_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snap, f, ensure_ascii=False, indent=1)
        os.replace(tmp, REQLOG_FILE)
    except Exception as e:
        log(f"请求记录落盘失败: {e}")

def _counters_flusher():
    """后台 30s 防抖刷盘（daemon 线程，随主进程退出）—— 计数 + 请求记录。"""
    _prune_tick = 0
    while True:
        time.sleep(COUNTER_FLUSH_INTERVAL)
        try:
            flush_counters()
            flush_reqlogs()               # 【V 组】请求记录一并落盘
            flush_model_stats()           # 【Y1】模型统计一并落盘
            flush_blocked()               # 【B4】黑名单落盘
            _prune_tick += 1
            if _prune_tick % 120 == 0:    # 【X 组】约每 1 小时清理一次过期明细
                prune_history()
        except Exception as e:
            log(f"状态刷盘异常: {e}")

def bump_counter(phone, key):
    """计数入口：+1 并置脏（写入口统一置脏，照上游 p.dirty.Store(true)）。"""
    global _counters_dirty
    with _counters_lock:
        c = _counters.setdefault(phone, {"calls": 0, "errors": 0})
        c[key] = int(c.get(key, 0)) + 1
        _counters_dirty = True

# ══ 【A1/A2/A3】模型级统计（对照上游 modelstats.go）═══
#   记录每个模型：请求数/成功/失败/首字延迟(TTFT)/平均耗时/token 累计/最近状态。
#   统计只服务展示，不参与调度决策（与上游注释一致）。
_model_stats = {}
_model_stats_lock = threading.Lock()
_ACCT_STATS = {}          # 【A3】账号级统计 {phone: {dur_sum, dur_n, ok, fail}}
# 【Y1】模型统计持久化：重启后「模型分布」不清零（数据量极小，每模型一条）
MSTATS_FILE = os.path.join(BASE_DIR, "modelstats.json")
# 【B4】模型黑名单：命中的模型请求直接拒（对照上游 modelfilter 黑名单）
# ══ 【BP2】运行期可调设置（面板可改，免重启）═══
#   原先超时等常量写死在源码，改一次要重启一次。现改为 settings.json 覆盖。
# ══ 【C7】设备授权登录（OAuth 加号）══
#   流程（实测上游）：① POST /v2/plugin/auth/state?platform=CLI → {state, authUrl}
#                    ② 用户打开 authUrl 完成登录
#                    ③ GET  /v2/plugin/auth/token?state=  → 未完成 code=11217；完成返回 accessToken
#                    ④ GET  /v2/plugin/login/account?state= → 取 uid / nickname
LOGIN_BASE_CN = "https://copilot.tencent.com"
LOGIN_BASE_GLOBAL = "https://www.workbuddy.ai"
LOGIN_UA = "CLI/2.63.2 CodeBuddy/2.63.2"
# ══ 【QM 组 · A1】realm 分流基础设施 ═══════════════════════════════════════
#   背景：此前只有「登录」区分国内/国际（LOGIN_BASE_CN/GLOBAL），
#         转发/换票/账本/模型目录全部写死 copilot.tencent.com。
#         国际版账号 token 发国内域 → 实测 401（www.codebuddy.cn 不认
#         workbuddy.ai 的 token），这就是「能添加账号但用不了」的根因。
#   口径对齐参考项目 internal/auth.Realm()：
#         显式 realm=="global" 优先，否则按 domain 后缀 .workbuddy.ai 推断，
#         都为空 → "cn"（老 CN 凭证零回归）。
#   实测确认：chat 路径 CN/global 相同（/v2/chat/completions），
#             差异仅在 base 主机；/v3/config 两域同路径。
# ═════════════════════════════════════════════════════════════════════════
CHAT_BASE_CN        = "https://copilot.tencent.com"   # 对话 + 换票 + 模型目录（国内）
CHAT_BASE_GLOBAL    = "https://www.workbuddy.ai"      # 对话 + 换票 + 模型目录（国际）
BILLING_BASE_CN     = "https://www.codebuddy.cn"      # 计费 + 官网任务（国内）
BILLING_BASE_GLOBAL = "https://www.workbuddy.ai"      # 计费（国际，无 /v2 前缀）
GLOBAL_ENABLED      = True   # 逃生门：False = 锁死纯 CN（对齐参考项目 config.global.enabled）

def is_global_domain(d):
    """domain 是否指向 www.workbuddy.ai 家族。"""
    d = (d or "").strip().lower()
    return d == "workbuddy.ai" or d.endswith(".workbuddy.ai")

def resolve_realm(realm, domain=""):
    """归一化 realm：显式 global 优先 → domain 后缀推断 → 兜底 cn。"""
    if (realm or "").strip().lower() == "global":
        return "global"
    if is_global_domain(domain):
        return "global"
    return "cn"

def acc_realm(acc):
    """账号生效 realm（受逃生门约束：关闸时恒 cn）。"""
    if not GLOBAL_ENABLED:
        return "cn"
    return resolve_realm(acc.get("realm", ""), acc.get("domain", ""))

def chat_base(acc):
    """按账号 realm 选对话/换票/模型目录 base。"""
    return CHAT_BASE_GLOBAL if acc_realm(acc) == "global" else CHAT_BASE_CN

def billing_base(acc):
    """按账号 realm 选计费 base。"""
    return BILLING_BASE_GLOBAL if acc_realm(acc) == "global" else BILLING_BASE_CN

def web_origin(acc):
    """按账号 realm 选 Origin/Referer 官网域。"""
    return "https://www.workbuddy.ai" if acc_realm(acc) == "global" else "https://www.codebuddy.cn"
# ══ 【QM 组 · A1 结束】═════════════════════════════════════════════════════
_logins = {}          # {state: {"realm":…, "ts":…}}
_logins_lock = threading.Lock()
LOGIN_SESSION_TTL = 900   # 15 分钟过期

def _login_endpoints(realm):
    base = LOGIN_BASE_GLOBAL if realm == "global" else LOGIN_BASE_CN
    origin = "https://www.workbuddy.ai" if realm == "global" else "https://www.codebuddy.cn"
    return (base + "/v2/plugin/auth/state?platform=CLI",
            base + "/v2/plugin/auth/token?state=",
            base + "/v2/plugin/login/account?state=",
            origin)

def _login_headers(origin):
    return {"Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "User-Agent": LOGIN_UA,
            "Origin": origin, "Referer": origin + "/"}

def login_start(realm="cn"):
    """【C7】发起设备授权：返回 state + authUrl（用户在浏览器打开完成登录）。"""
    ep_state, _, _, origin = _login_endpoints(realm)
    req = urllib.request.Request(ep_state, headers=_login_headers(origin), data=b"{}", method="POST")
    with urllib.request.urlopen(req, context=ctx, timeout=25) as r:
        d = json.loads(r.read().decode("utf-8", errors="replace"))
    data = d.get("data") or {}
    st = data.get("state") or ""
    if not st:
        raise Exception("上游未返回 state: %s" % str(d)[:150])
    with _logins_lock:
        _logins[st] = {"realm": realm, "ts": time.time()}
        # 清理过期会话
        now = time.time()
        for k in [k for k, v in _logins.items() if now - v.get("ts", 0) > LOGIN_SESSION_TTL]:
            _logins.pop(k, None)
    return {"state": st, "authUrl": data.get("authUrl") or "", "realm": realm}

def login_poll(state):
    """【C7】轮询登录状态；完成则写入 wb_tokens.json 并重载账号池。"""
    with _logins_lock:
        sess = dict(_logins.get(state) or {})
    if not sess:
        return {"done": False, "message": "state 未知或已过期，请重新发起"}
    realm = sess.get("realm", "cn")
    _, ep_token, ep_acct, origin = _login_endpoints(realm)
    # ① 取 token（未完成时上游返回业务 code 非 0）
    try:
        rq = urllib.request.Request(ep_token + state, headers=_login_headers(origin))
        with urllib.request.urlopen(rq, context=ctx, timeout=25) as r:
            raw = json.loads(r.read().decode("utf-8", errors="replace"))
        code = raw.get("code")
        tok = raw.get("data") or {}
        if code not in (0, None) or not tok.get("accessToken"):
            return {"done": False, "message": raw.get("msg") or "等待登录中…"}
    except Exception as e:
        return {"done": False, "message": "等待登录中… (%s)" % str(e)[:60]}

    at = tok.get("accessToken") or ""
    rt = tok.get("refreshToken") or ""
    exp = int(tok.get("expiresIn") or 0)
    # ② 取账号信息（uid / nickname）
    uid, nick = "", ""
    try:
        rq2 = urllib.request.Request(ep_acct + state,
            headers=dict(_login_headers(origin), Authorization="Bearer " + at))
        with urllib.request.urlopen(rq2, context=ctx, timeout=25) as r:
            acc = json.loads(r.read().decode("utf-8", errors="replace"))
        ad = acc.get("data") or acc
        uid = str(ad.get("uid") or ad.get("userId") or ad.get("uin") or "")
        nick = str(ad.get("nickname") or ad.get("nickName") or ad.get("name") or "")
    except Exception as e:
        log(f"[登录] 账号信息获取失败（不影响入库）: {str(e)[:80]}")

    if not uid:
        return {"done": False, "message": "已授权但未取到 uid，请重试"}

    # ③ 写入 wb_tokens.json（合并）
    try:
        # 【B7】统一走 _tokens_load/_tokens_save（持锁 + 原子替换），
        #   避免与面板导入/换票落盘并发交错导致静默丢号。
        is_new = False

        def _merge_login(cur):
            nonlocal is_new
            is_new = uid not in cur
            old_ent = cur.get(uid) if isinstance(cur.get(uid), dict) else {}
            ent = dict(old_ent)
            ent.update({"refresh_token": rt, "access_token": at,
                        "token_expiry": int(time.time()) + exp if exp else 0,
                        "nickname": nick, "realm": realm})
            cur[uid] = ent
            return cur

        _tokens_mutate(_merge_login)
        load_accounts()
        with _logins_lock:
            _logins.pop(state, None)
        log(f"[登录] {_mask(uid)} 登录成功（{'新增' if is_new else '更新'}）nick={nick}")
        return {"done": True, "uid": _mask(uid), "nickname": nick,
                "is_new": is_new, "message": "登录成功，账号已入库"}
    except Exception as e:
        return {"done": False, "message": "入库失败: %s" % str(e)[:80]}

SETTINGS_FILE = os.path.join(BASE_DIR, "settings.json")
_settings = {}
_settings_lock = threading.Lock()
DEFAULT_SETTINGS = {
    "timeout_header": 90,     # 等响应头（秒）
    "timeout_idle": 45,       # 流式空闲（秒）
    "timeout_connect": 15,    # 建连+DNS（秒）
    "max_rotate": 3,          # 换号重试上限
    "breaker_threshold": 3,   # 连续失败 → 熔断
    "disable_threshold": 8,   # 连续失败 → 自动禁用
    "auto_enable_hours": 24,  # 自动禁用恢复时长
    "pool_timeout_backoff": 5,  # 【BP3】池级连续超时次数 → 退避
    "max_inflight_global": 2,   # 【QM · A10】global 域单号最大在飞数（WAF 更严）
    "panel_token": "",        # 【BP5】面板访问密钥（空=不鉴权）
}

def load_settings():
    """【BP2】读取运行期设置（缺项用默认值补齐）。"""
    global _settings
    s = dict(DEFAULT_SETTINGS)
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                s.update(json.load(f) or {})
    except Exception as e:
        log(f"设置读取失败（用默认值）: {e}")
    with _settings_lock:
        _settings = s
    log(f"运行期设置已加载: 超时头 {s['timeout_header']}s / 空闲 {s['timeout_idle']}s / 换号 {s['max_rotate']} 次")
    return s

def get_setting(key, default=None):
    with _settings_lock:
        v = _settings.get(key, DEFAULT_SETTINGS.get(key, default))
    return v

def _public_settings(cur):
    """【BP4】对外暴露时隐藏密钥明文，只给「是否已设置」标记。"""
    out = dict(cur or {})
    if out.get("panel_token"):
        out["panel_token"] = ""
        out["panel_token_set"] = True
    return out

def save_settings(newd):
    """【BP2】保存设置（原子替换）。"""
    with _settings_lock:
        cur = dict(_settings) if _settings else dict(DEFAULT_SETTINGS)
    for k, v in (newd or {}).items():
        if k in DEFAULT_SETTINGS:
            cur[k] = v
    try:
        tmp = SETTINGS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cur, f, ensure_ascii=False, indent=1)
        os.replace(tmp, SETTINGS_FILE)
    except Exception as e:
        raise Exception("设置落盘失败: %s" % e)
    with _settings_lock:
        _settings.clear(); _settings.update(cur)
    return cur

BLOCKED_FILE = os.path.join(BASE_DIR, "blocked_models.json")
_blocked_models = set()
# 【BP1】模型白名单（非空时只放行列表内；与黑名单互斥判定，黑名单优先）
_allow_models = set()
_blocked_dirty = False
_mstats_dirty = False

def record_model_stat(model, ok, dur_ms, ttft_ms=None, tokens=0, phone=None):
    """【A1/A2/A3】记一次请求：模型维度 + 账号维度。"""
    global _mstats_dirty
    try:
        with _model_stats_lock:
            _mstats_dirty = True          # 【Y1】置脏
            st = _model_stats.setdefault(model, {
                "n": 0, "ok": 0, "fail": 0,
                "ttft_sum": 0.0, "ttft_n": 0,
                "dur_sum": 0, "dur_n": 0, "tok": 0,
                "last_t": "", "last_st": ""})
            st["n"] += 1
            if ok:
                st["ok"] += 1
            else:
                st["fail"] += 1
            if dur_ms:
                st["dur_sum"] += int(dur_ms); st["dur_n"] += 1
            if ttft_ms:
                st["ttft_sum"] += float(ttft_ms); st["ttft_n"] += 1
            if tokens:
                st["tok"] += int(tokens)
            st["last_t"] = time.strftime('%H:%M:%S')
            st["last_st"] = "200 OK" if ok else "失败"
        if phone:
            with _model_stats_lock:
                _mstats_dirty = True
                a = _ACCT_STATS.setdefault(phone, {"dur_sum": 0, "dur_n": 0, "ok": 0, "fail": 0})
                if dur_ms:
                    a["dur_sum"] += int(dur_ms); a["dur_n"] += 1
                if ok:
                    a["ok"] = int(a.get("ok", 0)) + 1
                else:
                    a["fail"] = int(a.get("fail", 0)) + 1
    except Exception:
        pass

def load_blocked():
    """【B4】加载模型黑名单。"""
    global _blocked_models
    try:
        if os.path.exists(BLOCKED_FILE):
            with open(BLOCKED_FILE, "r", encoding="utf-8") as f:
                d = json.load(f) or {}
            _blocked_models = set(d.get("blocked") or [])
            _allow_models.clear()
            _allow_models.update(d.get("allowed") or [])
            if _blocked_models or _allow_models:
                log(f"模型过滤: 黑名单 {len(_blocked_models)} 个 / 白名单 {len(_allow_models)} 个")
    except Exception as e:
        log(f"黑名单读取失败: {e}")

def flush_blocked(force=False):
    global _blocked_dirty
    if not _blocked_dirty and not force:
        return
    _blocked_dirty = False
    try:
        tmp = BLOCKED_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "blocked": sorted(_blocked_models),
                       "allowed": sorted(_allow_models)},
                      f, ensure_ascii=False, indent=1)
        os.replace(tmp, BLOCKED_FILE)
    except Exception as e:
        log(f"黑名单落盘失败: {e}")

def load_model_stats():
    """【Y1】启动时恢复模型/账号统计。"""
    global _model_stats, _ACCT_STATS
    try:
        if os.path.exists(MSTATS_FILE):
            with open(MSTATS_FILE, "r", encoding="utf-8") as f:
                d = json.load(f) or {}
            _model_stats = d.get("models") or {}
            _ACCT_STATS = d.get("accounts") or {}
            log(f"已恢复模型统计: {len(_model_stats)} 个模型 / {len(_ACCT_STATS)} 个账号")
        else:
            log("模型统计文件不存在, 从零开始")
    except Exception as e:
        log(f"模型统计读取失败(从零开始): {e}")

def flush_model_stats(force=False):
    """【Y1】原子替换落盘。"""
    global _mstats_dirty
    with _model_stats_lock:
        if not _mstats_dirty and not force:
            return
        snap = {"version": 1, "saved": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "models": _model_stats, "accounts": _ACCT_STATS}
        _mstats_dirty = False
    try:
        tmp = MSTATS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snap, f, ensure_ascii=False, indent=1)
        os.replace(tmp, MSTATS_FILE)
    except Exception as e:
        log(f"模型统计落盘失败: {e}")

def _credits_text(credits):
    """倍率文本：'x0.11 credits'→' ×0.11'；'x0.00 credits'→'（免费）'；空→''（与 HTTP 层一致）"""
    s = str(credits or '').strip()
    if not s:
        return ''
    raw = s.lower().replace('credits', '').replace('×', '').replace('x', '').strip()
    try:
        v = float(raw)
    except Exception:
        return ''
    if v == 0:
        return '（免费）'
    return ' ×%g' % v

# ══ 【B3】模型显示名（带倍率）查表 ══
#   全站模型名统一走这里：优先实时目录的 name（含倍率），失败退裸 id。
_model_disp_cache = {"ts": 0.0, "map": {}}

def model_display_name(mid):
    """返回模型的展示名（带倍率，如 'Kimi-K2.7-Code ×0.57'）；查不到返回原 id。"""
    if not mid:
        return mid or ""
    now = time.time()
    m = _model_disp_cache["map"]
    if (not m) or (now - _model_disp_cache["ts"] > 300):
        try:
            live, _e = fetch_live_models()
            if live:
                m = {}
                for x in live:
                    _nm = x.get("name") or x.get("id") or ""
                    _pl = _credits_text(x.get("credits"))
                    m[x.get("id")] = (_nm + _pl) if _pl else _nm
                _model_disp_cache["map"] = m
                _model_disp_cache["ts"] = now
        except Exception:
            pass
    return m.get(mid) or mid

def model_stats_snapshot():
    """导出模型统计（含均值），供 /api/monitor 使用。"""
    out = {}
    with _model_stats_lock:
        for m, st in _model_stats.items():
            n = st["n"] or 1
            out[m] = {
                "model": m,                             # 【B3】原始 id
                "name": model_display_name(m),          # 【B3】带倍率的展示名
                "requests": st["n"], "ok": st["ok"], "fail": st["fail"],
                "rate": round(st["ok"] / n * 100, 1),
                "ttft_avg": round(st["ttft_sum"] / st["ttft_n"]) if st["ttft_n"] else None,
                "dur_avg": round(st["dur_sum"] / st["dur_n"]) if st["dur_n"] else None,
                "tokens": st["tok"],
                "last_t": st["last_t"], "last_st": st["last_st"],
            }
    # 【A3】账号耗时排行
    acct = {}
    with _model_stats_lock:
        for p, a in _ACCT_STATS.items():
            if a["dur_n"]:
                _ok = int(a.get("ok", 0)); _fail = int(a.get("fail", 0)); _tt = _ok + _fail
                acct[_mask(p)] = {
                    "dur_avg": round(a["dur_sum"] / a["dur_n"]) if a["dur_n"] else None,
                    "samples": a["dur_n"],
                    "ok": _ok, "fail": _fail,
                    "rate": round(_ok / _tt * 100, 1) if _tt else None}
    # 【AC】全局汇总（全量口径，供顶部统计卡使用，避免与"最近30条"窗口值混淆）
    total = ok = fail = 0
    dsum = dn = tsum = tn = tok = 0
    with _model_stats_lock:
        for st in _model_stats.values():
            total += int(st.get("n", 0))
            ok += int(st.get("ok", 0))
            fail += int(st.get("fail", 0))
            dsum += int(st.get("dur_sum", 0)); dn += int(st.get("dur_n", 0))
            tsum += float(st.get("ttft_sum", 0)); tn += int(st.get("ttft_n", 0))
            tok += int(st.get("tok", 0))
    totals = {
        "requests": total, "ok": ok, "fail": fail,
        "rate": round(ok / total * 100, 1) if total else None,
        "dur_avg": round(dsum / dn) if dn else None,
        "ttft_avg": round(tsum / tn) if tn else None,
        "tokens": tok,
    }
    return {"models": out, "accounts": acct, "total": total, "totals": totals}

# 【W3 新增】上游超时单独计数（用于区分「上游慢」还是「我们慢」）
#   与 errors 分离：超时不是账号的问题（I2 语义），故不计入账号 errors。
# ══ 【BP3】超时明细 + 池级连续超时退避 ══
#   池级退避：连续 N 次超时 → 全池短暂停顿（避风头），【不罚任何账号】
_pool_timeout_streak = 0
_pool_hold_until = 0.0
TIMEOUT_LOG_FILE = os.path.join(BASE_DIR, "timeouts.jsonl")
_timeout_total = 0
_timeout_last = ""
_timeout_lock = threading.Lock()

def bump_pool_timeout():
    """【BP3】连续超时计数；达阈值 → 全池短暂退避。超时不算账号的问题。"""
    global _pool_timeout_streak, _pool_hold_until
    n = int(get_setting("pool_timeout_backoff", 5) or 5)
    _pool_timeout_streak += 1
    if n > 0 and _pool_timeout_streak >= n:
        hold = min(10 * _pool_timeout_streak, 60)      # 10s 起步，封顶 60s
        _pool_hold_until = time.time() + hold
        log(f"⚠️ 连续 {_pool_timeout_streak} 次上游超时 → 全池暂停 {hold}s 避风头（不算账号问题）")
        _pool_timeout_streak = 0

def pool_hold_remaining():
    """【BP3】池级暂停剩余秒数（0=不暂停）。"""
    r = _pool_hold_until - time.time()
    return int(r) if r > 0 else 0

def clear_pool_timeout():
    """成功请求 → 清空连续超时计数。"""
    global _pool_timeout_streak
    _pool_timeout_streak = 0

def append_timeout_log(model, phone, elapsed, err):
    """【BP3】超时明细（JSONL 追加，便于日后对账）。"""
    try:
        row = {"ts": time.strftime('%Y-%m-%d %H:%M:%S'),
               "model": model, "phone": phone,
               "elapsed_s": round(elapsed, 1), "err": str(err)[:200]}
        with open(TIMEOUT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        pass

def bump_timeout(reason=""):
    global _timeout_total, _timeout_last, _counters_dirty
    with _timeout_lock:
        _timeout_total += 1
        _timeout_last = time.strftime('%H:%M:%S')
    # 也持久化到 counters.json（进程级，键名 __global__）
    with _counters_lock:
        g = _counters.setdefault("__global__", {"calls": 0, "errors": 0})
        g["timeouts"] = int(g.get("timeouts", 0)) + 1
        g["timeout_last"] = _timeout_last          # 【Z 修复】原来漏写，重启后时间戳必丢
        _counters_dirty = True

def load_timeouts():
    global _timeout_total, _timeout_last
    g = _counters.get("__global__", {})
    _timeout_total = int(g.get("timeouts", 0))
    _timeout_last = str(g.get("timeout_last", ""))

# 【B1 新增】服务运行元信息（供面板显示 PID / 运行时长 / 日志状态）
START_TIME = time.time()
# 【H4 修复】日志路径自动探测
# 真因: 旧默认 "/rootfs/var/workbuddy/server.log" 是 .deb 安装时 dpkg 创建的 0 字节占位文件,
#       而守护进程 stdout 实际写在 plist StandardOutPath 指向的文件里 (含业务日志、持续增长)。
#       两者 inode 不同, 导致面板日志区永远空白、"清屏"也只清到假文件。
def _resolve_log_file():
    import re as _re
    cands = []
    if os.environ.get("WB_LOG"):
        cands.append(os.environ["WB_LOG"])
    # 首选: 跟随 plist 的 StandardOutPath (stdout 的真实去向)
    for _pl in ("/var/jb/Library/LaunchDaemons/com.jkgl.workbuddy.plist",
                "/Library/LaunchDaemons/com.jkgl.workbuddy.plist"):
        try:
            import plistlib
            with open(_pl, "rb") as _f:
                _d = plistlib.load(_f)
            _p = _d.get("StandardOutPath") or _d.get("StandardErrorPath") or ""
            if _p:
                cands.append(_p)                                        # 原样
                cands.append(_re.sub(r"/\.jbroot-[^/]+", "", _p))       # 剥掉 .jbroot-xxx 前缀
                cands.append(_re.sub(r"/\.jbroot-[^/]+", "/var/jb", _p))  # 换成 /var/jb
        except Exception:
            pass
    # 兜底候选
    cands += ["/var/workbuddy/server.log",
              "/private/var/workbuddy/server.log",
              "/rootfs/var/workbuddy/server.log"]
    _seen, _uniq = set(), []
    for _c in cands:
        if _c and _c not in _seen:
            _seen.add(_c); _uniq.append(_c)
    # 存在且字节数最大的胜出 —— 即真正在写的那一个
    _exist = sorted(((os.path.getsize(_c), _c) for _c in _uniq if os.path.exists(_c)),
                    reverse=True)
    return _exist[0][1] if _exist else _uniq[0]

LOG_FILE = _resolve_log_file()

# 【A5】日志脱敏：写日志前隐去可能混入的凭据片段（对照上游 logredact.go）
#   目的：错误正文里可能带 Bearer token / refresh_token 片段，落盘前统一打码。
_REDACT_PATTERNS = [
    (__import__("re").compile(r'(Bearer\s+)[A-Za-z0-9\-_.=]{12,}'), r'\1***'),
    (__import__("re").compile(r'("?(?:refresh_token|access_token|api_?key|token|secret)"?\s*[:=]\s*"?)([A-Za-z0-9\-_.=]{12,})', __import__("re").I), r'\1***'),
    (__import__("re").compile(r'([\w.\-]+@[\w.\-]+)'), lambda m: m.group(1)[:2] + '***@' + m.group(1).split('@')[-1]),
]

def _redact(s):
    """把凭据类片段打码（仅用于日志展示，不影响业务逻辑）。"""
    try:
        out = str(s)
        for pat, rep_ in _REDACT_PATTERNS:
            out = pat.sub(rep_, out)
        return out
    except Exception:
        return str(s)

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {_redact(msg)}", flush=True)

def append_log(model, phone, status, duration_ms, ttft_ms=None):
    global _reqlog_dirty
    # 【H6】加锁: 多线程下保护 request_logs 的"插入+截断"组合操作
    with _lock:
        _reqlog_dirty = True                          # 【V 组】置脏, 30s 内落盘
        _rec = {
            "time": time.strftime('%H:%M:%S'),
            "model": model,
            "model_name": model_display_name(model),   # 【B3】带倍率的展示名
            "phone": _mask(phone),
            "status": status,
            "duration": f"{duration_ms}ms",
            "ttft": f"{ttft_ms}ms" if ttft_ms else None    # 【A1】首字延迟
        }
        request_logs.insert(0, _rec)
        if len(request_logs) > REQLOG_MAX:
            request_logs.pop()
    append_history(_rec)          # 【X 组】明细全量存档（锁外做 IO，不阻塞请求）

# ============================================================================
# 【I 组】上游容错移植 —— 对照上游 Go 网关 internal/server/{handler,backoff}.go
#
# 背景: 我们网关原本「一次失败即 502, 不换号不重试不区分错误类型」。
#       上游有 7 项容错, 我们全缺。本轮移植其中最关键的 3 项:
#         I1  DNS 偶发失败重试        (上游无直接等价物, 但我们 60% 失败源于此)
#         I2  超时三态区分「不罚号」   (上游 isUpstreamTimeout 语义)
#         I3  网络抖动换号重试+退避    (上游 rotateBackoff/backoffAfter 语义)
#       另加 I4 失败账号短冷却 (上游 CooldownSoftRate 的最小等价物)。
# ============================================================================

# 退避参数(对齐上游 backoffAfter 的 500ms·2^n 封顶 8s, ±25% 抖动)
BACKOFF_BASE = 0.5      # 秒
BACKOFF_CAP = 8.0       # 秒
BACKOFF_JITTER = 0.25   # ±25%
# DNS 重试参数(DNS 是"明确可重试"的错误, 给独立的小退避)
DNS_RETRIES = 3
DNS_BASE_DELAY = 0.8    # 秒
# 换号重试上限(对齐上游「换号直到全部试遍」的精神, 但限 3 次防长尾)
MAX_ROTATE = 3
# 超时分层(上游把「建连」与「读响应」分离)
UP_TIMEOUT_CONNECT = 15   # 建连+DNS
# 【W1】读超时 90 → 60：客户端(如 Cherry Studio/OpenAI SDK)通常 60s 就自行断开，
#       我们等 90s 毫无意义，只会白占一个线程。降到 60s 与主流客户端对齐。
UP_TIMEOUT_READ = 60      # 兼容旧引用
# 【A4】分层超时（对照上游 debug.go 的 HeaderTimeoutSeconds / IdleTimeoutSeconds）
#   HEADER：请求发完 → 收到响应头的等待上限（思考模型首字慢，给足 90s）
#   IDLE  ：流式读的空闲上限（持续有数据永远不触发；断流 45s 即判死）
#   真因：原先两者共用一个值 —— 思考模型首字 >60s 会被误杀，而流式空闲 60s 又太长。
UP_TIMEOUT_HEADER = 90    # 等响应头
UP_TIMEOUT_IDLE = 45      # 流式空闲


def _is_dns_error(e):
    """【I1】识别 DNS 层失败。
    上游 Go 侧 dial 报 `no such host`; Python urllib 对应:
      socket.gaierror / errno 8(EAI_NONAME, Darwin) /
      文案含 nodename·servname·getaddrinfo。
    DNS 是「明确可重试」的错误 —— 解析偶发失败重试大概率成功。"""
    import socket
    if isinstance(e, socket.gaierror):
        return True
    if getattr(e, "errno", None) == 8:
        return True
    s = str(e).lower()
    return ("nodename" in s) or ("servname" in s) or ("getaddrinfo" in s)


def _is_upstream_timeout(e):
    """【I2】判定「上游超时/停滞」—— 对齐上游 isUpstreamTimeout 的三态语义:
      ① socket.timeout / TimeoutError
      ② 文案含 timed out / timeout(含 "The write/read operation timed out"、
         "The handshake operation timed out")
      ③ 上游停滞(客户端仍在但流被掐) —— Python 侧表现为空读/超时, 归此类
    关键: 超时【不算】账号的问题 —— 上游注释原话:
      「同一份请求换到别的号, 撞上的是同一个慢上游, 只会把客户端拖到
        MaxRotate × header_timeout, 期间还给一串健康号喂连败计数」
    故调用方对超时【不换号、不罚号】, 直接 503 提示重试无用。
    注意: DNS 失败不算超时(那是 I1 的重试域), 必须先判 DNS。"""
    import socket
    if isinstance(e, (socket.timeout, TimeoutError)):
        return True
    s = str(e).lower()
    return ("timed out" in s) or ("timeout" in s)


def _backoff_delay(n):
    """【I3】第 n 次(0 基)轮转失败前的退避时长 —— 对齐上游 backoffAfter:
    base·2^n 封顶 cap, 再施加 ±25% 抖动(防多请求同相位重试聚团)。
    用逐次翻倍而非位移: base 调整后无需同步维护移位上限, 溢出由封顶兜底。"""
    import random
    d = BACKOFF_BASE
    for _ in range(n):
        if d >= BACKOFF_CAP:
            break
        d *= 2
    if d > BACKOFF_CAP:
        d = BACKOFF_CAP
    return d * (1 + (random.random() * 2 - 1) * BACKOFF_JITTER)


def _open_upstream(req, ctx_, timeout=UP_TIMEOUT_READ):
    """【I1】带 DNS 重试的上游请求 —— DNS 偶发失败自动重试 3 次(0.8/1.6/3.2s)。
    只重试 DNS 层错误; 超时/HTTP 错误/连接重置原样抛出, 交给上层分类处理
    (超时换号无用见 I2; HTTP 错误是业务问题见 I4)。"""
    last = None
    for k in range(DNS_RETRIES):
        try:
            return urllib.request.urlopen(req, context=ctx_, timeout=timeout)
        except Exception as e:
            last = e
            if not _is_dns_error(e):
                raise                       # 非 DNS 错误: 原样上抛
            if k < DNS_RETRIES - 1:
                d = DNS_BASE_DELAY * (2 ** k)
                d *= 1 + (__import__("random").random() * 2 - 1) * BACKOFF_JITTER
                log(f"DNS 解析失败(第{k+1}/{DNS_RETRIES}次), {d:.1f}s 后重试: {e}")
                time.sleep(d)
    raise last


# ══════════════════════════════════════════════════════════════════════════
# 【B7】凭据文件统一读写 + 归一化 —— 修复 1.5.6「导入新 JSON 覆盖旧账号」
#
#   根因（实测铁证）：
#     1.5.6 的 import_tokens 用 `for ph, item in newd.items()` 把 JSON 的
#     【顶层键】当成手机号。面板给的是 {account:{...}, auth:{...}} 这种嵌套
#     结构，顶层键只有 "account"/"auth" 两个 → 所有账号都撞在 "auth" 这一个
#     键上 → 导入第 2 个账号时 cur["auth"] = ... 直接把第 1 个覆盖掉。
#     复现：导入 A 后 cur = {"auth": A}，导入 B 后 cur = {"auth": B}，只剩 1 条。
#
#     面板上那条 `#1 aut****auth` 就是 "auth" 被 _mask 当手机号切的结果。
#
#   修法：账号键一律从【对象内部】取（uid 优先，其次手机号），绝不用外层键；
#         字段同时兼容驼峰与蛇形；时间统一归一为秒级 epoch。
# ══════════════════════════════════════════════════════════════════════════

_TOKENS_LOCK = threading.RLock()


def _tokens_load():
    """【B7】统一读取凭据文件。任何写操作都必须持 _TOKENS_LOCK。"""
    with _TOKENS_LOCK:
        if not os.path.exists(TOKENS_FILE):
            return {}
        try:
            with open(TOKENS_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            return d if isinstance(d, dict) else {}
        except Exception as e:
            log(f"[B7] 读取凭据失败: {e}")
            return {}


def _tokens_save(cur):
    """【B7】统一原子写盘。"""
    with _TOKENS_LOCK:
        tmp = TOKENS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cur, f, ensure_ascii=False, indent=1)
        os.replace(tmp, TOKENS_FILE)


def _tokens_mutate(fn):
    """【B7 · 并发修复】把「读 → 改 → 写」整段放进同一把锁里执行。

    第一版只在 _tokens_load / _tokens_save 各自内部加锁，结果并发导入时：
      T1 load → T2 load（都读到同一份旧值）→ T1 save → T2 save
    后写者覆盖先写者 → 8 线程实测只剩 2 条，全部请求却都返回成功。
    锁必须覆盖整个事务，而不是两次 IO 各自一段。
    """
    with _TOKENS_LOCK:
        cur = _tokens_load()
        new_cur = fn(cur)
        if new_cur is not None:
            _tokens_save(new_cur)
        return new_cur


def _norm_expiry(v):
    """【B7】把各种到期时间统一成「秒级 epoch」。
    兼容 0 / 秒(1795097981) / 毫秒(1795097981000) / 相对秒(1800)。"""
    try:
        e = float(v or 0)
    except Exception:
        return 0
    if e <= 0:
        return 0
    if e > 1e12:                 # 毫秒 → 秒
        e = e / 1000.0
    if e < 1e9:                  # 相对秒（expiresIn）
        e = time.time() + e
    return int(e)


_IMPORT_RT_KEYS = ("refresh_token", "refreshToken", "refresh", "rt")
_IMPORT_AT_KEYS = ("access_token", "accessToken", "access", "at")
_IMPORT_EXP_KEYS = ("expires_at", "expiresAt", "token_expiry", "expiry", "expire", "exp")
_IMPORT_DOM_KEYS = ("domain", "host")
_IMPORT_NAME_KEYS = ("nickname", "nickName", "name", "displayName")
# 可能在内部再套一层账号信息的容器名
_IMPORT_LAYERS = ("auth", "token", "tokens", "credential", "credentials",
                  "oauth", "account", "user", "profile", "info")
# 「键 → 账号对象」遍历时，这些外层键名不可能是账号标识（是真容器名）
_IMPORT_SKIP_KEYS = ("auth", "account", "token", "tokens", "data", "list",
                     "accounts", "result", "items", "credential", "credentials",
                     "extra", "user", "profile", "info", "payload")


def _import_first(d, keys):
    """按优先级取第一个非空的数值/字符串字段。"""
    if not isinstance(d, dict):
        return ""
    for k in keys:
        v = d.get(k)
        if isinstance(v, bool):
            continue
        if isinstance(v, (int, float)):
            if v:
                return str(int(v)) if float(v).is_integer() else str(v)
        elif isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _import_pick_id(obj):
    """【核心】从账号对象内部取真实标识 —— 绝不用外层字典键。

    优先级：uid / userId / uin / accountId  →  phone / mobile  →  username / nickname
    这样导出的键与 login_poll 写入的键（uid）一致，
    同一账号用「授权链接」登录时会命中同一条记录 → 更新而非重复。
    """
    if not isinstance(obj, dict):
        return ""
    subs = [obj]
    for k in _IMPORT_LAYERS:
        v = obj.get(k)
        if isinstance(v, dict):
            subs.append(v)
    for k in ("uid", "userId", "user_id", "uin", "accountId", "account_id"):
        for s in subs:
            v = s.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
            if isinstance(v, int) and not isinstance(v, bool) and v:
                return str(v)
    for k in ("phone", "mobile", "username", "nickname", "name"):
        for s in subs:
            v = s.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
            if isinstance(v, int) and not isinstance(v, bool) and v:
                return str(v)
    return ""


def _import_flatten(obj):
    """把任意一层嵌套的账号对象压平成一条凭据记录。
    返回 (record, reason)；record 为 None 表示这不是账号对象。"""
    if not isinstance(obj, dict):
        return None, "非对象"
    layers = [obj]
    for sub in _IMPORT_LAYERS:
        v = obj.get(sub)
        if isinstance(v, dict):
            layers.append(v)

    rt = at = dom = nick = ""
    exp = 0
    for L in layers:
        if not rt:
            rt = _import_first(L, _IMPORT_RT_KEYS)
        if not at:
            at = _import_first(L, _IMPORT_AT_KEYS)
        if not exp:
            for k in _IMPORT_EXP_KEYS:
                v = L.get(k)
                if isinstance(v, bool):
                    continue
                if isinstance(v, (int, float)) and v > 0:
                    exp = float(v)
                    break
                if isinstance(v, str) and v.strip().replace(".", "", 1).isdigit():
                    exp = float(v.strip())
                    break
        if not dom:
            dom = _import_first(L, _IMPORT_DOM_KEYS)
        if not nick:
            nick = _import_first(L, _IMPORT_NAME_KEYS)
    if not rt:
        return None, "无 refresh_token"

    expi = _norm_expiry(exp)
    realm = ""
    for L in layers:
        realm = _import_first(L, ("realm",))
        if realm:
            break
    if not realm:
        realm = "global" if str(dom).lower().endswith(".workbuddy.ai") else "cn"

    rec = {"refresh_token": rt}
    if at:
        rec["access_token"] = at
    if expi:
        rec["token_expiry"] = expi
    if dom:
        rec["domain"] = str(dom).strip().lower()
    rec["realm"] = str(realm).strip().lower()
    if nick and nick != rt[:12]:
        rec["nickname"] = nick
    return rec, ""


def _normalize_import_payload(payload):
    """把面板粘贴的任何形态归一成 [(账号键, 记录), ...]。

    支持 7 种形态：
      1) 原生      {"<uid或手机号>": {"refresh_token": ...}}
      2) 嵌套单对象 {"account": {...}, "auth": {...}}
      3) 嵌套数组  {"account": {...}, "auth": [{...}, {...}]}
      4) 扁平蛇形  {"uid": ..., "refresh_token": ...}
      5) 扁平驼峰  {"uid": ..., "refreshToken": ...}
      6) 裸数组    [{"uid": ..., "refreshToken": ...}, ...]
      7) 信封      {"accounts": [...]} / {"data": [...]} / {"list": [...]}
    """
    out = []
    seen = set()

    def push(key, rec):
        k = str(key or "").strip()
        if not k or not rec:
            return
        if k in seen:
            return
        seen.add(k)
        out.append((k, rec))

    def pick_key(inner, outer_key):
        """对象内部标识优先；内部没有再退外层键（且外层键不能是容器名）。"""
        kid = _import_pick_id(inner)
        if kid:
            return kid
        lk = str(outer_key or "").strip()
        if lk and lk.lower() not in _IMPORT_SKIP_KEYS:
            return lk
        return ""

    def walk(node, outer_key=""):
        """递归收集所有账号对象，外层标识可被内层记录继承。"""
        if isinstance(node, list):
            for it in node:
                walk(it, outer_key)
            return
        if not isinstance(node, dict):
            return
        # ① 自身就是一条记录？（含 RT）
        rec, _why = _import_flatten(node)
        if rec:
            push(pick_key(node, outer_key), rec)
            return
        # ② 不是记录 → 是容器，继续下探。
        #    本节点若带标识（如 {"account":{"uid":X}, "auth":[...]} 里的 X），
        #    则作为「上下文标识」传给子层，供没有自身标识的凭据片段使用。
        ctx = _import_pick_id(node) or outer_key
        for k, v in node.items():
            if not isinstance(v, (dict, list)):
                continue
            # 容器名不当作标识传递；非容器名（手机号/uid 形式）才作为键
            nk = ctx if str(k).lower() in _IMPORT_SKIP_KEYS else k
            walk(v, nk)

    walk(payload)
    return out


def load_accounts():
    global account_pool
    if not os.path.exists(TOKENS_FILE):
        log(f"凭据文件未就绪: {TOKENS_FILE}")
        return
    try:
        with open(TOKENS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        new_pool = []
        for phone, item in data.items():
            if not isinstance(item, dict):
                continue
            rt = item.get("refresh_token") or item.get("refreshToken", "")
            if rt:
                new_pool.append({
                    "phone": phone,
                    "refresh_token": rt,
                    # 【B7】兼容驼峰 accessToken —— 1.5.6 只读蛇形，
                    #   导致驼峰来源的记录 AT 恒空 → 每次选号都强制换票
                    #   → 放大 RT 单次轮换损耗（换一次旧 RT 就废）。
                    "access_token": (item.get("access_token")
                                     or item.get("accessToken") or ""),
                    # 【B7】到期时间兼容 expires_at / expiresAt，
                    #   并统一归一到秒级 epoch（原样透传会让毫秒值被判成 1970）。
                    "token_expiry": _norm_expiry(item.get("token_expiry")
                                                 or item.get("expires_at")
                                                 or item.get("expiresAt") or 0),
                    # 【QM · A2】读回 realm/domain —— 登录时已写入(见 login_poll)，
                    #   此前读回丢弃 → 账号池永远不知道自己是国际版 → 转发撞国内域 401。
                    #   老凭证无此字段 → "" → resolve_realm 判 cn，行为零回归。
                    "realm": (item.get("realm") or "").strip().lower(),
                    "domain": (item.get("domain") or "").strip().lower(),
                    # 【B7】带上昵称（1.5.6 读盘时丢弃，面板只能显示脱敏号）
                    "nickname": str(item.get("nickname") or "").strip(),
                    "credits": 0.0,
                    "used": 0.0,
                    "pkg_count": 0,
                    "packages": [],
                    "status": "active",
                    # 【A 组】从持久化恢复，不再写死 0（原来每次启动/重载都清零）
                    "calls": int(_counters.get(phone, {}).get("calls", 0)),
                    "errors": int(_counters.get(phone, {}).get("errors", 0)),
                    # 【I4 新增】失败短冷却: 网络类失败后 30s 内不再被选中,
                    # 避免连续请求反复撞同一个坏账号(上游 CooldownSoftRate 的最小等价物)
                    "cool_until": 0.0,
                    # 【R 组】惩罚态（从 _counters 恢复，跨重启保留）
                    "fails": int(_counters.get(phone, {}).get("fails", 0)),
                    "breaker_until": 0.0,
                    "disabled": bool(_counters.get(phone, {}).get("disabled", False)),
                    "disabled_until": float(_counters.get(phone, {}).get("disabled_until", 0.0)),
                    "disabled_auto": bool(_counters.get(phone, {}).get("disabled_auto", False)),
                    # 【B1】暂停：临时让位（不参与选号，但照常刷新余额/保活），
                    #      与「禁用」的区别是不判死、无需人工启用
                    "paused": False,   # 【QM3 · C4】暂停功能已移除（强制清 False，防老数据静默跳过账号）
                    "last_active": "已就绪"
                })
        account_pool = new_pool
        log(f"已成功加载 {len(account_pool)} 个账号到轮询池！")
    except Exception as e:
        log(f"加载账号池失败: {e}")

# ============================================================================
# 【R 组】熔断 / 连败 / 自动禁用 —— 对照上游 pool.NoteError / NoteSuccess /
#         sessionDeadThreshold（连败才禁用，防一次失败误杀）+ Disable。
#   真因: 原实现坏号只有固定 30s 冷却，不区分「偶发抖动」与「账号已坏」，
#         既不退避也不隔离，坏号会反复被选中、拖慢请求。
# ============================================================================
BREAKER_THRESHOLD = 3             # 连续失败达此值 → 熔断（指数退避）
BREAKER_BASE      = 30            # 退避基数：30 → 60 → 120 → 240 → 480s
BREAKER_MAX       = 480
DISABLE_THRESHOLD = 8             # 连续失败达此值 → 自动禁用
AUTO_ENABLE_AFTER = 24 * 3600     # 自动禁用 24 小时后自动启用

def persist_account_state(acc):
    """【R 组】把账号的惩罚态写进 _counters（复用 A 组的 30s 防抖刷盘），
    否则「禁用」一重启就复活。"""
    global _counters_dirty
    if not acc:
        return
    with _counters_lock:
        c = _counters.setdefault(acc["phone"], {"calls": 0, "errors": 0})
        c["fails"] = int(acc.get("fails", 0))
        c["disabled"] = bool(acc.get("disabled", False))
        c["disabled_auto"] = bool(acc.get("disabled_auto", False))
        c["disabled_until"] = float(acc.get("disabled_until", 0.0))
        c["paused"] = bool(acc.get("paused", False))          # 【B1】
        _counters_dirty = True

def record_failure(acc, reason=""):
    """【R 组】失败记账：连败 +1 → 熔断（指数退避）或自动禁用 24h。"""
    if not acc:
        return
    acc["errors"] += 1
    bump_counter(acc["phone"], "errors")
    acc["fails"] = int(acc.get("fails", 0)) + 1
    f = acc["fails"]
    if f >= DISABLE_THRESHOLD:
        acc["disabled"] = True
        acc["disabled_auto"] = True
        acc["disabled_until"] = time.time() + AUTO_ENABLE_AFTER
        acc["fails"] = 0
        log(f"账号 {acc['phone']} 连续失败 {f} 次 → 自动禁用 24 小时（{reason}）")
    elif f >= BREAKER_THRESHOLD:
        d = min(BREAKER_BASE * (2 ** (f - BREAKER_THRESHOLD)), BREAKER_MAX)
        acc["breaker_until"] = time.time() + d
        log(f"账号 {acc['phone']} 连续失败 {f} 次 → 熔断 {d}s（{reason}）")
    persist_account_state(acc)

def record_success(acc):
    """【R 组】成功记账：一次成功清空全部惩罚态（照上游 NoteSuccess）。"""
    if not acc:
        return
    if int(acc.get("fails", 0)) or float(acc.get("breaker_until", 0)):
        acc["fails"] = 0
        acc["breaker_until"] = 0.0
        persist_account_state(acc)

# ══ 【B5】模型属性探测 ══
#   目的（对照上游 probe.go）：让「从未被调度过」的账号也能知道某模型对它是否免费，
#   不必等客户端请求碰巧轮到它。做法：发一条最小请求，读取 usage.credit 观测扣费。
PROBE_FILE = os.path.join(BASE_DIR, "model_probes.json")
_probes = {}          # {model: {"credit": float, "per1k": float, "ts":…, "n": int}}
_probes_lock = threading.Lock()

def load_probes():
    global _probes
    try:
        if os.path.exists(PROBE_FILE):
            with open(PROBE_FILE, "r", encoding="utf-8") as f:
                _probes = json.load(f) or {}
            log(f"已加载模型探测: {len(_probes)} 个模型")
    except Exception as e:
        log(f"模型探测读取失败: {e}")

def flush_probes():
    try:
        tmp = PROBE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "saved": time.strftime("%Y-%m-%dT%H:%M:%S"),
                       "models": _probes}, f, ensure_ascii=False, indent=1)
        os.replace(tmp, PROBE_FILE)
    except Exception as e:
        log(f"模型探测落盘失败: {e}")

def probe_one_model(model, account=None):
    """【B5】用指定账号（默认池内任意可用号）探一次某模型的最小请求。
    返回观测结果：credit 是否为 0（免费）。不参与调度决策，仅作知识积累。"""
    acc = account or get_next_valid_account()
    if not acc:
        return {"ok": False, "msg": "无可用账号"}
    payload = {"model": model, "stream": True,
               "messages": [{"role": "user", "content": "hi"}],
               "max_tokens": 1}
    try:
        # 【QM · A8】探针也按账号 realm 选域（原写死国内域）
        _origin = web_origin(acc)
        rq = urllib.request.Request(chat_base(acc) + "/v2/chat/completions",
            headers={"Authorization": "Bearer " + acc["access_token"],
                     "Content-Type": "application/json",
                     "Origin": _origin,
                     "Referer": _origin + "/",
                     "User-Agent": "Mozilla/5.0"},
            data=json.dumps(payload).encode("utf-8"), method="POST")
        resp = _open_upstream(rq, ctx, timeout=int(get_setting("timeout_header", 90)))
        credit = None; ptok = ctok = 0
        while True:
            line = resp.readline()
            if not line:
                break
            s = line.decode("utf-8", errors="replace").strip()
            if s.startswith("data:") and "usage" in s:
                try:
                    j = json.loads(s[5:].strip())
                    u = j.get("usage")
                    if isinstance(u, dict) and ("credit" in u or "total_tokens" in u):
                        if u.get("credit") is not None:
                            credit = float(u["credit"])
                        ptok = int(u.get("prompt_tokens") or 0)
                        ctok = int(u.get("completion_tokens") or 0)
                except Exception:
                    pass
            if "[DONE]" in s:
                break
        tt = ptok + ctok
        per1k = (credit / tt * 1000) if (credit is not None and tt > 0) else None
        with _probes_lock:
            _probes[model] = {"credit": credit, "per1k": round(per1k, 4) if per1k is not None else None,
                              "tokens": tt, "ts": time.strftime('%m-%d %H:%M'),
                              "phone": _mask(acc["phone"]),
                              "free": (credit is not None and credit <= 0)}
        flush_probes()
        log(f"[探测] {model} → credit={credit} per1k={per1k}")
        return {"ok": True, "model": model, "credit": credit,
                "per1k": round(per1k, 4) if per1k is not None else None,
                "free": (credit is not None and credit <= 0)}
    except Exception as e:
        return {"ok": False, "msg": str(e)[:120]}

def _mask(p):
    """【B7】脱敏显示。

    1.5.6 只有「前3 + **** + 后4」一条路，且 len<7 时原样返回。
    当账号键是 uid（uuid，36 字符）时能看，但当键是 "auth" 这种 4 字符垃圾键时，
    面板内联的 [ :3]+"****"+[-4:] 会拼出 "aut****auth" —— 既不像手机号也无法识别，
    这正是老大截图里 #1 那条的来历。这里按长度分档，短键不再产生这种噪声。
    """
    p = str(p or "")
    if not p:
        return "—"
    if len(p) >= 12:                      # uuid / 长 uid
        return p[:8] + "…" + p[-4:]
    if len(p) >= 7:                       # 手机号
        return p[:3] + "****" + p[-4:]
    if len(p) >= 4:
        return p[:2] + "***" + p[-1:]
    return p

def _match_acc(phone):
    """按真实号或脱敏号查找账号（前端传的是脱敏号）。

    【B7】统一走 _mask —— 1.5.6 这里内联了一套前3后4，
    与 _mask 一旦漂移就会出现「面板看得见、按钮点不动」的幽灵账号。
    """
    if not phone:
        return None
    for a in account_pool:
        p = a["phone"]
        if phone == p or phone == _mask(p):
            return a
    return None

def _auto_enable_loop():
    """【R 组】每 30s 检查：自动禁用到期的账号自动启用（手动禁用不在此列）。"""
    while True:
        time.sleep(30)
        try:
            now = time.time()
            for acc in list(account_pool):
                if (acc.get("disabled") and acc.get("disabled_auto")
                        and 0 < float(acc.get("disabled_until", 0)) <= now):
                    acc["disabled"] = False
                    acc["disabled_auto"] = False
                    acc["disabled_until"] = 0.0
                    acc["fails"] = 0
                    acc["breaker_until"] = 0.0
                    persist_account_state(acc)
                    log(f"账号 {acc['phone']} 自动禁用已到期（24h）→ 自动启用")
        except Exception as e:
            log(f"自动启用巡检异常: {e}")

def _detect_plist_key(key):
    """【D3 新增】探测 LaunchDaemon plist 中的布尔开关是否开启

    真因: 面板设置页的「开机自启 / 自动自愈」原本是写死的「已开启」，
          需要从真实 plist 读取 RunAtLoad / KeepAlive 才能反映真相。
    """
    for cand in (
        "/var/jb/Library/LaunchDaemons/com.jkgl.workbuddy.plist",
        "/Library/LaunchDaemons/com.jkgl.workbuddy.plist",
    ):
        try:
            if os.path.exists(cand):
                with open(cand, "r", encoding="utf-8", errors="replace") as f:
                    txt = f.read()
                i = txt.find("<key>%s</key>" % key)
                if i < 0:
                    return False
                seg = txt[i:i + 80]
                return "<true/>" in seg
        except Exception:
            continue
    return None


def _spawn_delayed(op):
    """【G1 新增】延迟 2 秒执行 launchctl 操作

    设计要点：
      * 必须延迟 —— 否则服务在 HTTP 响应发出前就被杀，浏览器拿到的是连接中断；
      * 用 nohup 独立子进程执行，**不引入 threading**（iOS 单进程架构铁律）；
      * 2 秒足够让本次响应 + 后续几帧轮询落地。
    """
    PLIST = "/var/jb/Library/LaunchDaemons/com.jkgl.workbuddy.plist"
    LABEL = "system/com.jkgl.workbuddy"
    if op == "kickstart":
        cmd = "launchctl kickstart -k %s" % LABEL
    elif op == "bootstrap":
        cmd = "launchctl bootstrap system %s" % PLIST
    elif op == "bootout":
        cmd = "launchctl bootout %s" % LABEL
    else:
        cmd = ""
    if not cmd:
        return
    try:
        os.system('( sleep 2; %s ) >/dev/null 2>&1 &' % cmd)
        log("[面板控制] 已排入延迟队列: %s" % cmd)
    except Exception as e:
        log("排入延迟队列失败: %s" % e)


def sync_all_accounts():
    """【B4 新增】同步全部账号的官方账本（供面板 /api/control 的 sync 动作调用）

    逻辑与 main() 启动时一致：
      无 access_token 的先换票，有票的直接拉官方资源账本。
    """
    n = 0
    for acc in account_pool:
        try:
            if not acc.get("access_token"):
                refresh_account_token(acc)
            else:
                update_account_official_resources(acc)
            n += 1
        except Exception as e:
            log(f"同步账号 {acc.get('phone','?')} 异常: {e}")
    log(f"账本同步完成，共处理 {n} 个账号")
    return n


def refresh_account_token(acc):
    try:
        # 【QM · A3】按账号 realm 换票 —— 原写死 copilot.tencent.com，
        #   国际版账号拿 refresh_token 去国内域换票必 401（实测铁证）。
        _cbase = chat_base(acc)
        _origin = web_origin(acc)
        req = urllib.request.Request(_cbase + "/v2/plugin/auth/token/refresh", headers={
            "X-Refresh-Token": acc["refresh_token"],
            "X-Auth-Refresh-Source": "plugin",
            "Content-Type": "application/json",
            "Origin": _origin,
            "Referer": _origin + "/",
        }, data=b"{}", method="POST")
        with urllib.request.urlopen(req, context=ctx, timeout=8) as resp:
            data = json.loads(resp.read().decode())
            new_at = data.get("data", {}).get("accessToken")
            new_rt = data.get("data", {}).get("refreshToken")
            # 【QM · A3】上游可能回 domain（用于 realm 回落判定），回写并持久化
            _dom = data.get("data", {}).get("domain") or data.get("data", {}).get("realm")
            if _dom:
                acc["domain"] = str(_dom).strip().lower()
            if new_at:
                acc["access_token"] = new_at
                acc["token_expiry"] = time.time() + 1800
                acc["status"] = "active"
                if new_rt:
                    acc["refresh_token"] = new_rt
                # 【B7 修复】换票结果必须整体落盘（RT/AT/expiry/realm/domain）。
                #   原实现只调 _persist_acc_realm，它只写 realm/domain，
                #   新 RT 全在内存里 → 重启即失效。
                _persist_account_tokens(acc)
                update_account_official_resources(acc)
                return True
    except Exception as e:
        acc["status"] = "expired"
        log(f"账号 {acc['phone']} 换票失败: {e}")
    return False

def _persist_account_tokens(acc):
    """【B7】把换票结果写回 wb_tokens.json。

    修复 1.5.6 缺陷：refresh_account_token 拿到新 RT 后只改内存
    `acc["refresh_token"] = new_rt`，从不落盘。而上游 RT 是【单次轮换】的
    —— 第一次换票成功后旧 RT 立即作废，磁盘上却仍是旧 RT
    ⇒ 服务一重启，load_accounts 读回作废的 RT ⇒ 所有账号 401。
    这是「重启后账号全掉线 / 换号就少号」的第二根钉子。
    """
    try:
        k = acc.get("phone")
        if not k:
            return
        def _merge_tokens(cur):
            ent = cur.get(k)
            if not isinstance(ent, dict):
                ent = {}
            if acc.get("refresh_token"):
                ent["refresh_token"] = acc["refresh_token"]
            if acc.get("access_token"):
                ent["access_token"] = acc["access_token"]
            if acc.get("token_expiry"):
                ent["token_expiry"] = int(acc["token_expiry"])
            if acc.get("realm"):
                ent["realm"] = acc["realm"]
            if acc.get("domain"):
                ent["domain"] = acc["domain"]
            if acc.get("nickname"):
                ent["nickname"] = acc["nickname"]
            cur[k] = ent
            return cur

        _tokens_mutate(_merge_tokens)
    except Exception as e:
        log(f"[B7] 回写 {acc.get('phone', '?')} 凭据失败: {e}")


def _persist_acc_realm(acc):
    """【QM · A2/A3】把账号的 realm/domain 回写 wb_tokens.json（供下次启动读取）。

    【B7】改走统一读写助手：持锁 + 原子替换，并与 _persist_account_tokens 共用
    同一把 _TOKENS_LOCK，杜绝并发写互相覆盖。
    """
    try:
        k = acc.get("phone")
        if not k:
            return
        def _merge_realm(cur):
            ent = cur.get(k)
            if not isinstance(ent, dict):
                return None                 # 老逻辑：不在文件里就不写，保持不变
            if acc.get("realm"):
                ent["realm"] = acc["realm"]
            if acc.get("domain"):
                ent["domain"] = acc["domain"]
            cur[k] = ent
            return cur

        _tokens_mutate(_merge_realm)
    except Exception as e:
        log(f"[realm] 回写 {acc.get('phone','?')} 失败: {e}")

def update_account_official_resources(acc):
    if not acc["access_token"]:
        return
    try:
        # 【QM · A4】按账号 realm 拉账本 —— 原写死国内域；
        #   global 计费在 www.workbuddy.ai（参考项目 billingMeterPath，无 /v2 前缀）。
        #   CN 现路径本就是无 /v2 版且线上可用 → 仅换主机，零回归。
        req = urllib.request.Request(billing_base(acc) + "/billing/meter/get-user-resource", headers={
            "Authorization": f"Bearer {acc['access_token']}",
            "User-Agent": "Mozilla/5.0",
            "Origin": web_origin(acc),
            "Referer": web_origin(acc) + "/",
        }, data=b"{}")
        with urllib.request.urlopen(req, context=ctx, timeout=8) as resp:
            data = json.loads(resp.read().decode())
            d = data.get("data", {}).get("Response", {}).get("Data", {})
            acc["pkg_count"] = d.get("TotalCount", 0)
            accounts = d.get("Accounts", [])
            
            used_sum = 0.0
            remain_sum = 0.0
            pkg_map = {}
            for a in accounts:
                p_name = a.get("PackageName", "基础包")
                # 【J1】到期日：CycleEndTime 形如 "2026-10-18 12:03:10"，空串=长期有效
                p_exp = str(a.get("CycleEndTime") or "").strip()
                u = float(a.get("CycleCapacityUsedPrecise", a.get("CycleCapacityUsed", 0)))
                r = float(a.get("CycleCapacityRemainPrecise", a.get("CycleCapacityRemain", 0)))
                used_sum += u
                remain_sum += r
                
                # 【J1】同包不同到期日不合并（到期分布需要按批次区分）
                k = p_name + "\x00" + p_exp
                if k not in pkg_map:
                    pkg_map[k] = {"name": p_name, "expire": p_exp, "used": 0.0, "remain": 0.0, "count": 0}
                pkg_map[k]["used"] += u
                pkg_map[k]["remain"] += r
                pkg_map[k]["count"] += 1
                
            pkgs = []
            for k, v in pkg_map.items():
                pkgs.append({
                    "name": v["name"],
                    "expire": v["expire"],   # 【J1】到期日，供面板做到期分布
                    "used": round(v["used"], 2),
                    "remain": round(v["remain"], 2),
                    "count": v["count"]
                })
                
            acc["used"] = round(used_sum, 2)
            acc["credits"] = round(remain_sum, 2)
            acc["packages"] = pkgs
    except Exception as e:
        log(f"拉取账号 {acc['phone']} 官方账本异常: {e}")

# ============================================================================
# 【K1 新增】模型目录实时查询 —— 直连上游双端点（企业模型表 + IDE v3/config）
#
# 背景: 旧 /api/models 只读 BASE_DIR/official_models.json 快照（2026-09-22 冻结），
#       上游改倍率/加模型后界面不更新 → 用户察觉"不真实"。
# 本实现: 请求时实时双路探测（企业端点 + v3/config 合并），结果缓存 300s；
#       ?live=1 强制刷新；两路全失败时降级读快照文件（响应头标记来源）。
# ============================================================================
_MODELS_LIVE = {"ts": 0.0, "data": None, "errors": [], "fetched_at": ""}
_MODELS_LIVE_TTL = 300
_MODELS_FETCH_LOCK = threading.Lock()
# 【QM · A5】模型 → 首选 realm 映射（每次刷新重建）：chat 转发时据此优先选同区服账号，
#   避免把 global 模型请求派给 CN 号（上游 11102/401）。
_model_realm = {}

_LIVE_MODEL_COMMON = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://www.codebuddy.cn",
    "Referer": "https://www.codebuddy.cn/",
    "X-CodeBuddy-Request": "1",
    "Accept-Language": "zh-CN",
}

def _pick_models_account(realm=None):
    """挑一个可用账号（优先未过期；否则尝试刷新）。

    【QM · A6】realm: None=任意号（原行为）；"cn"/"global"=只挑该区服号。
              否则会拿 CN 号打 global 端点（必然 401）。
    """
    now = time.time()
    def _ok(a):
        if not (a.get("access_token") and now <= a.get("token_expiry", 0)):
            return False
        return realm is None or acc_realm(a) == realm
    for a in account_pool:
        if _ok(a):
            return a
    for a in account_pool:
        try:
            if (realm is None or acc_realm(a) == realm) and refresh_account_token(a):
                return a
        except Exception:
            continue
    return None

def _fetch_model_route(url, ua, token, extra=None):
    h = dict(_LIVE_MODEL_COMMON)
    h["Authorization"] = "Bearer " + token
    h["User-Agent"] = ua
    # 【QM · A5】按目标域自动对齐 Origin/Referer —— global 域必须用 workbuddy.ai，
    #   否则 CN 的 Origin 可能被国际站 WAF 拦。CN url 不含 workbuddy.ai → 保持原值，零回归。
    if "workbuddy.ai" in url:
        h["Origin"] = "https://www.workbuddy.ai"
        h["Referer"] = "https://www.workbuddy.ai/"
    if extra:
        h.update(extra)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, context=ctx, timeout=12) as resp:
        return json.loads(resp.read().decode())

def fetch_live_models(force=False):
    """实时模型目录（双端点合并）。返回 (list|None, errors)。"""
    now = time.time()
    if (not force) and _MODELS_LIVE["data"] is not None and (now - _MODELS_LIVE["ts"]) < _MODELS_LIVE_TTL:
        return _MODELS_LIVE["data"], _MODELS_LIVE["errors"]
    if not _MODELS_FETCH_LOCK.acquire(blocking=False):
        return _MODELS_LIVE["data"], _MODELS_LIVE["errors"]
    try:
        merged, errs = {}, []
        # 【QM · A5】按 realm 分池探测：CN 号打 copilot.tencent.com，
        #   global 号打 www.workbuddy.ai。原实现只挑一个号只打 CN 域 →
        #   国际版模型（gpt-5.4 / gpt-6-astra / gemini-3.5-flash 等）永不出现在列表。
        #   纯 CN（无 global 账号）时仅跑 cn 分支，端点/UA/Header 逐字照抄原值 → 零回归。
        #   ★ 映射重建必须在探测循环【之前】（放到之后曾把刚写入的清空 → 全 0 条目）
        try:
            _model_realm.clear()
        except Exception:
            pass
        try:
            _model_domains.clear()
        except Exception:
            pass
        _any_acc = False
        for _realm in ("cn", "global"):
            _acc = _pick_models_account(realm=_realm)
            if _acc is None:
                continue
            _any_acc = True
            _token = _acc["access_token"]
            _base = chat_base(_acc)
            _host = _base.split("//", 1)[1]
            _origin = web_origin(_acc)
            # 企业模型表：global 用 /v2/enterprises/...（参考项目 globalModelsProbePaths），
            #              CN 用 /console/enterprises/...（原值，逐字保留）
            _paths = (("/v2/enterprises/personal/models", "/console/enterprises/personal/models")
                      if _realm == "global"
                      else ("/console/enterprises/personal/models",))
            _got = False
            for _p in _paths:
                try:
                    j = _fetch_model_route(
                        _base + _p,
                        "WorkBuddy/5.5.4 WorkBuddy/5.5.4 CLI/2.137.1", _token)
                    d = (j.get("data") or {})
                    for m in (d.get("models") or []):
                        mid = m.get("id")
                        if not mid:
                            continue
                        merged[(_realm, mid)] = {
                            "id": mid,
                            "realm": _realm,
                            "name": m.get("name") or mid,
                            "credits": m.get("credits") or "",
                            "description": m.get("descriptionZh") or m.get("description") or "",
                            "maxInputTokens": int(m.get("maxInputTokens") or 0),
                            "supportsReasoning": bool(m.get("supportsReasoning")),
                            "supportsImages": bool(m.get("supportsImages")),
                            "tags": m.get("tags") or [],
                            "vendor": m.get("vendor") or "",
                        }
                        # 【QM2 · A4】记录该模型在此域的倍率（供按价选号）
                        try:
                            _model_domains.setdefault(mid, {})[_realm] = _rate_of(m.get("credits"))
                        except Exception:
                            pass
                    _got = True
                    break
                except Exception as e:
                    errs.append("%s 企业端点 %s: %s" % (_realm, _p, e))
            # 路 2：IDE v3/config（补上下文窗口口径）—— 同域
            try:
                j = _fetch_model_route(
                    _base + "/v3/config",
                    "CodeBuddyIDE/4.12.0 CodeBuddy/4.12.0", _token,
                    {"Accept": "application/json, text/plain, */*",
                     "X-Domain": _host, "X-Product": "SaaS"})
                d = (j.get("data") or {})
                for m in (d.get("models") or []):
                    mid = m.get("id")
                    if not mid:
                        continue
                    cw = m.get("contextWindow") or {}
                    ctx_len = int(cw.get("defaultLength") or m.get("maxInputTokens") or 0)
                    _key = (_realm, mid)
                    base = merged.get(_key)
                    # 【QM2 · A4】v3/config 也参与域倍率记录（企业端点无此模型时它可能有）
                    try:
                        _model_domains.setdefault(mid, {})
                        if _realm not in _model_domains[mid] or _model_domains[mid].get(_realm) is None:
                            _model_domains[mid][_realm] = _rate_of(m.get("credits"))
                    except Exception:
                        pass
                    if base is None:
                        merged[_key] = {
                            "id": mid, "realm": _realm, "name": m.get("name") or mid,
                            "credits": m.get("credits") or "",
                            "description": m.get("descriptionZh") or "",
                            "maxInputTokens": ctx_len,
                            "supportsReasoning": bool(m.get("supportsReasoning")),
                            "supportsImages": bool(m.get("supportsImages")),
                            "tags": m.get("tags") or [],
                            "vendor": m.get("vendor") or "",
                        }
                    elif ctx_len > int(base.get("maxInputTokens") or 0):
                        base["maxInputTokens"] = ctx_len
            except Exception as e:
                errs.append("%s v3/config: %s" % (_realm, e))
        if not _any_acc:
            return None, ["无可用账号"]
        # 过滤非对话模型（对齐上游 nonChatModel 规则）+ 按 id 去重
        # 【QM · A5 / QM2 · A4】merged 键为 (realm, id)；同 id 跨区服时保留先到的（cn 优先）。
        #   ★ 映射重建已在探测循环【之前】完成（此处严禁再 clear，否则清空刚写入的域映射）。
        out = []
        _seen_ids = set()
        for _key, m in merged.items():
            mid = m.get("id") or ""
            if not mid or mid in _seen_ids:
                continue
            if mid.startswith(("nes-", "completion-", "codewise-")):
                continue
            if any("text-to-image" in str(x) for x in (m.get("tags") or [])):
                continue
            _seen_ids.add(mid)
            out.append(m)
            # 记录该模型首次出现的 realm（兼容旧逻辑）
            try:
                _model_realm.setdefault(mid, m.get("realm") or "cn")
            except Exception:
                pass
        # 【QM2 · A3】剔除已确认不可用的模型（11102 负缓存）→ Minis 里不显示
        try:
            _before = len(out)
            out = [m for m in out if not model_is_dead(m.get("id"))]
            if len(out) != _before:
                errs.append("已过滤 %d 个不可用模型" % (_before - len(out)))
        except Exception:
            pass
        if not out:
            return None, errs or ["上游返回空"]
        # 排序：倍率低在前，无倍率在后
        def _rk(mm):
            s = str(mm.get("credits") or "").lower().replace("credits", "").replace("x", "").strip()
            try:
                return (0, float(s), mm.get("id") or "")
            except Exception:
                return (1, 0.0, mm.get("id") or "")
        out.sort(key=_rk)
        _MODELS_LIVE.update(ts=now, data=out, errors=errs,
                            fetched_at=time.strftime("%H:%M:%S"))
        return out, errs
    finally:
        _MODELS_FETCH_LOCK.release()

def repair_tool_sequence(obj):
    """【BP5a】修复发往上游的 tool_calls/tool 序列（对照上游 toolrepair.go）。

    规则：
      1. 按 tool call ID 做对称裁剪：无结果的调用、无调用的结果、重复项删除；
      2. 结果按原始出现顺序保留，不按调用顺序重排；
      3. 仅清理明显破损的序列，正常序列原样透传（保守策略，避免误改）。
    返回 (obj, 修复报告)。
    """
    rep_info = {"dropped_calls": 0, "dropped_outputs": 0, "changed": False}
    try:
        msgs = obj.get("messages")
        if not isinstance(msgs, list) or not msgs:
            return obj, rep_info
        # 收集所有调用 ID 与结果 ID
        call_ids, result_ids = [], []
        for m in msgs:
            if not isinstance(m, dict):
                continue
            if m.get("role") == "assistant" and isinstance(m.get("tool_calls"), list):
                for c in m["tool_calls"]:
                    if isinstance(c, dict):
                        cid = c.get("id")
                        if cid:
                            call_ids.append(cid)
            if m.get("role") == "tool" and m.get("tool_call_id"):
                result_ids.append(m["tool_call_id"])
        if not call_ids and not result_ids:
            return obj, rep_info          # 没有工具调用 → 原样透传
        cs, rs = set(call_ids), set(result_ids)
        # 对称裁剪：无结果的调用 / 无调用的结果
        drop_calls = cs - rs
        drop_results = rs - cs
        if not drop_calls and not drop_results:
            return obj, rep_info          # 序列完整 → 原样透传（保守）
        new_msgs = []
        seen_res = set()
        for m in msgs:
            if not isinstance(m, dict):
                new_msgs.append(m); continue
            if m.get("role") == "assistant" and isinstance(m.get("tool_calls"), list):
                keep = [c for c in m["tool_calls"]
                        if isinstance(c, dict) and (c.get("id") not in drop_calls)]
                rep_info["dropped_calls"] += len(m["tool_calls"]) - len(keep)
                if keep:
                    m = dict(m); m["tool_calls"] = keep; new_msgs.append(m)
                elif m.get("content"):
                    m = dict(m); m.pop("tool_calls", None); new_msgs.append(m)
                continue
            if m.get("role") == "tool":
                tid = m.get("tool_call_id")
                if tid in drop_results or tid in seen_res:
                    rep_info["dropped_outputs"] += 1
                    continue
                seen_res.add(tid)
                new_msgs.append(m); continue
            new_msgs.append(m)
        obj["messages"] = new_msgs
        rep_info["changed"] = True
        if rep_info["dropped_calls"] or rep_info["dropped_outputs"]:
            log("[工具修复] 丢弃无配对调用 %d 个 / 无配对结果 %d 个"
                % (rep_info["dropped_calls"], rep_info["dropped_outputs"]))
    except Exception as e:
        log(f"[工具修复] 跳过（异常）: {e}")
    return obj, rep_info

def repair_reasoning(obj):
    """【BP5b】DeepSeek thinking 模型的 reasoning 历史修复（对照上游 reasoningrepair.go）。

    规则：assistant 回合若缺 reasoning_content 但模型是 thinking 类，
         补空字符串占位（不捏造思维链）；已有内容不覆盖。
    """
    try:
        model = str(obj.get("model") or "")
        if not model.lower().lstrip().startswith("deepseek"):
            return obj
        msgs = obj.get("messages")
        if not isinstance(msgs, list):
            return obj
        for m in msgs:
            if not isinstance(m, dict) or m.get("role") != "assistant":
                continue
            if "reasoning_content" not in m:
                m["reasoning_content"] = ""
    except Exception:
        pass
    return obj

# ════════════════════════════════════════════════════════════════════════
# 【QM2 · A1】国际站硬约束修复箱
#
# 实测铁证（对 www.workbuddy.ai 逐个真打）：
#   ① 无 system 首条消息 → 400 {"code":11128,"msg":"first message is not system prompt"}
#   ② GPT 系 max_tokens < 16 → 400 {"code":11133,"msg":"Invalid request parameters"}
#      （对照实验：15 拒 / 16 过 / 64 过；不带该字段也过）
#   ③ developer 角色不在上游白名单 → 400 11128
#
# 参考项目等价物：
#   internal/prompt/prompt.go  Rewrite()             —— 替换/插入 system
#   internal/upstream/payload.go clampGPTMinMaxTokens() —— GPT 系 max_tokens 抬到 16
#   internal/upstream/payload.go normalizeRoles()      —— developer → system
#
# 只对 realm==global 的账号生效；CN payload 逐字不动 → 零回归。
# ════════════════════════════════════════════════════════════════════════
GW_SYSTEM_PROMPT = ("You are a helpful assistant. Respond in the user's language, "
                    "follow the user's instructions, and be direct and concise.")
GPT_MIN_MAX_TOKENS = 16

def normalize_roles(obj):
    """developer → system（上游 role 白名单不含 developer，原样发会 400 11128）。"""
    msgs = obj.get("messages")
    if not isinstance(msgs, list):
        return False
    n = 0
    for m in msgs:
        if isinstance(m, dict) and str(m.get("role") or "").strip().lower() == "developer":
            m["role"] = "system"
            n += 1
    return n > 0

def ensure_system_message(obj):
    """国际站要求首条为 system：没有则在最前插入网关提示词。返回是否插入。"""
    msgs = obj.get("messages")
    if not isinstance(msgs, list):
        obj["messages"] = [{"role": "system", "content": GW_SYSTEM_PROMPT}]
        return True
    for m in msgs:
        if isinstance(m, dict) and str(m.get("role") or "").strip().lower() in ("system", "developer"):
            return False
    msgs.insert(0, {"role": "system", "content": GW_SYSTEM_PROMPT})
    return True

def clamp_gpt_max_tokens(obj):
    """GPT 系 max_tokens < 16 → 抬到 16（上游 15 拒 16 过，实测）。返回是否改写。"""
    model = str(obj.get("model") or "").lower()
    if "gpt-" not in model:
        return False
    v = obj.get("max_tokens")
    if not isinstance(v, (int, float)) or isinstance(v, bool):
        return False
    if v < GPT_MIN_MAX_TOKENS:
        obj["max_tokens"] = GPT_MIN_MAX_TOKENS
        return True
    return False

def prepare_global_payload(obj):
    """对 global 账号的出站 payload 做三处修复。返回 (payload_bytes, 修复描述)。"""
    marks = []
    try:
        if normalize_roles(obj):
            marks.append("developer→system")
        if ensure_system_message(obj):
            marks.append("补 system")
        if clamp_gpt_max_tokens(obj):
            marks.append("max_tokens→%d" % GPT_MIN_MAX_TOKENS)
    except Exception as e:
        log(f"[QM2] global 预处理异常（跳过）: {e}")
    return json.dumps(obj).encode("utf-8"), ",".join(marks)

# ══ 【QM2 · A3】不可用模型负缓存（上游目录列了但实际 11102 的模型）══
_model_dead = {}
_MODEL_DEAD_TTL = 6 * 3600

def mark_model_dead(model):
    """标记模型不可用（11102 service info not found）→ 从 /v1/models 剔除。"""
    try:
        _model_dead[str(model)] = time.time() + _MODEL_DEAD_TTL
    except Exception:
        pass

def model_is_dead(model):
    exp = _model_dead.get(str(model))
    if exp is None:
        return False
    if time.time() > exp:
        _model_dead.pop(str(model), None)
        return False
    return True

# ══ 【QM2 · A4】模型 → 各域倍率映射（用于按「更便宜域」选号）══
_model_domains = {}      # {model_id: {"cn": 0.29, "global": 0.0}}

def _rate_of(raw):
    """倍率字符串 → float；无法解析返回 None。"""
    if raw is None or raw == "":
        return None
    s = str(raw).lower().replace("credits", "").replace("x", "").strip()
    try:
        return float(s)
    except Exception:
        return None

def prefer_realm_for(model):
    """按「更便宜域」决定模型首选 realm。返回 None = 不过滤（原行为）。

    单域模型 → 该域；双域模型 → 倍率更低者；同价 → cn（省国际额度）。
    """
    dm = _model_domains.get(str(model))
    if not dm:
        return None
    if len(dm) == 1:
        return next(iter(dm))
    def r(k):
        v = dm.get(k)
        return float("inf") if v is None else v
    rc, rg = r("cn"), r("global")
    if rg < rc:
        return "global"
    return "cn"


def get_next_valid_account(exclude=None, realm=None):
    """挑下一个可用账号。

    【I2 新增】exclude: 已试过的账号集合(phone)。换号重试时传入,
             跳过本轮已失败的账号, 避免重复撞同一个。
    【I4 新增】跳过处于短冷却(cool_until)中的账号 —— 网络类失败后 30s 内
             不再被选中(上游 CooldownSoftRate 的最小等价物)。
    【QM · A6】realm: None=不过滤(原行为)；"cn"/"global"=只挑该区服号。
    """
    global current_idx
    if not account_pool:
        return None
    exclude = exclude or set()
    # 【H6】取号加锁: 原子读取起始下标, 防并发请求读到同一个下标导致跳号
    with _lock:
        idx = current_idx
    n = len(account_pool)
    now = time.time()
    # 轮询与换票在锁外执行 —— refresh_account_token 含网络 IO,
    # 若持锁会让所有并发请求串行化, 彻底抵消多线程收益。
    for _ in range(n):
        acc = account_pool[idx]
        idx = (idx + 1) % n
        # 【I4】冷却中 / 已试过 → 跳过(不提交下标, 继续找下一个)
        # 【R 组】跳过：已试过 / 短冷却 / 熔断中 / 已禁用
        # 【B1】再跳过：已暂停（暂停=临时让位，账号本身仍健康）
        if (acc["phone"] in exclude
                or acc.get("cool_until", 0) > now
                or acc.get("breaker_until", 0) > now
                or acc.get("disabled", False)
                or acc.get("paused", False)):
            continue
        # 【QM · A6】区服过滤：指定 realm 时只挑同区服账号
        if realm is not None and acc_realm(acc) != realm:
            continue
        # 【QM · A10】global 域并发闸：WAF 更严，单号在飞超上限则跳过
        if acc_realm(acc) == "global" and acc.get("_inflight", 0) >= _max_inflight_global():
            continue
        if not acc["access_token"] or time.time() > acc["token_expiry"]:
            if refresh_account_token(acc):
                with _lock:                       # 提交新下标
                    current_idx = idx
                return acc
        else:
            with _lock:                           # 提交新下标
                current_idx = idx
            return acc
    with _lock:
        current_idx = idx
    return None

def _max_inflight_global():
    """【QM · A10】global 域单号最大在飞数（对齐参考项目 MaxInFlightGlobal=2）。"""
    try:
        return max(1, int(get_setting("max_inflight_global", 2) or 2))
    except Exception:
        return 2

# 【E1 修复】客户端提前断开时内核抛出的异常族 —— 统一静默处理，不刷屏
_CLIENT_GONE = (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)

def _quiet_log(msg):
    """仅记录真正的服务端异常，断连类已在入口吞掉"""
    try:
        print("[%s] %s" % (time.strftime('%H:%M:%S'), msg), flush=True)
    except Exception:
        pass

class WorkBuddyHandler(BaseHTTPRequestHandler):
    # 【C1 降噪】轮询类接口不写访问日志，避免淹没业务日志
    QUIET_PATHS = ()

    def handle_one_request(self):
        """【E1 修复】最外层兜底：socketserver 写响应头/收请求时断连也静默处理"""
        try:
            super().handle_one_request()
        except _CLIENT_GONE:
            self.close_connection = True

    def log_message(self, fmt, *args):
        """【E3 修复】默认静默白名单制：只保留真实业务请求(/v1, /chat)的访问日志
        面板轮询、静态资源、manifest 等一律不写 stderr —— 避免淹没业务日志"""
        try:
            p = self.path
            if p.startswith("/v1/") or p.startswith("/chat") or "/chat/completions" in p:
                super().log_message(fmt, *args)
        except Exception:
            pass

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        # 【E1 修复】统一断连兜底：客户端提前断开 socket 时静默处理，不刷屏不中断
        try:
            self._do_GET_inner()
        except _CLIENT_GONE:
            pass
        except Exception as e:
            _quiet_log("do_GET 异常: %r" % (e,))

    def _do_GET_inner(self):
        # 【A2 修复】剥离 ?query 参数后再匹配路由 (供 ?view=chat/models 定位视图使用)
        raw_path = self.path.split("?", 1)[0]
        if raw_path == "/" or raw_path == "/index.html":
            self.serve_html()
        elif raw_path == "/panel" or raw_path == "/panel/":
            self.serve_panel()
        elif raw_path in ("/apple-touch-icon.png", "/favicon.ico",
                          "/icon-192.png", "/icon-512.png", "/icon-1024.png"):
            self.serve_static(raw_path)
        elif raw_path == "/manifest.json":
            self.serve_manifest()
        elif raw_path == "/api/login":
            self.serve_login()
        elif raw_path == "/api/settings":
            self.serve_settings()
        elif raw_path == "/api/monitor":
            self.serve_monitor()
        elif raw_path == "/api/logs":
            self.serve_logs()
        elif raw_path == "/api/files":
            self.serve_files()
        elif raw_path == "/api/file":
            self.serve_file_content()
        elif raw_path == "/api/models":
            self.serve_official_models()
        elif raw_path == "/v1/models":
            self.serve_models_simple()
        elif raw_path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok","accounts":5}')
        else:
            self.send_response(404)
            self.end_headers()

    def serve_html(self):
        html_path = os.path.join(BASE_DIR, "index.html")
        if not os.path.exists(html_path):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"index.html not found")
            return
        with open(html_path, "rb") as f:
            content = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        # 【QM3 修复】根路径此前无缓存头 → 手机浏览器吃旧缓存看不到新版面板
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.end_headers()
        self.wfile.write(content)

    def serve_panel(self):
        # 【A4 新增】管家面板: 越狱区 /panel 路由
        panel_path = os.path.join(BASE_DIR, "panel", "index.html")
        if not os.path.exists(panel_path):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"panel not found")
            return
        with open(panel_path, "rb") as f:
            content = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        # 【G7 修复】iOS Safari 对 no-cache 仍会启发式缓存，改 no-store 彻底禁止
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.end_headers()
        self.wfile.write(content)

    def serve_static(self, path):
        # 【T3】按请求的真实文件名直读（原为 png→apple-touch-icon.png 二元映射）
        fname = os.path.basename(path.split("?")[0]) or "apple-touch-icon.png"
        fpath = os.path.join(BASE_DIR, fname)
        if not os.path.exists(fpath):
            fname = "apple-touch-icon.png" if path.endswith(".png") else "favicon.ico"
            fpath = os.path.join(BASE_DIR, fname)
        if not os.path.exists(fpath):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"icon not found")
            return
        with open(fpath, "rb") as f:
            content = f.read()
        mime = "image/png" if fname.endswith(".png") else "image/x-icon"
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "public, max-age=86400")
        self.end_headers()
        self.wfile.write(content)

    def serve_manifest(self):
        """【F1 新增】PWA manifest: 供添加到主屏幕时读取名称/图标/全屏模式"""
        fpath = os.path.join(BASE_DIR, "manifest.json")
        if not os.path.exists(fpath):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"manifest not found")
            return
        with open(fpath, "rb") as f:
            content = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "application/manifest+json; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "public, max-age=3600")
        self.end_headers()
        self.wfile.write(content)

    def serve_logs(self):
        """【B2 新增 / H2 调整】读取服务日志尾部若干行，供面板实时日志区渲染

        ?lines=N  取最后 N 行（默认 120）
        ?filter=0 关闭前台噪音过滤（默认开启，仅影响显示，不影响文件内容）
        """
        try:
            if "lines=" in self.path:
                n = int(self.path.split("lines=")[-1].split("&")[0])
            else:
                n = 120
        except Exception:
            n = 120
        n = max(10, min(n, 500))

        # 前台噪音过滤：这些请求的访问日志不显示在面板上
        # 【重要】只过滤"显示"，日志文件里依然完整记录
        NOISE = ("GET /panel", "GET /api/monitor", "GET /api/logs",
                 "GET /api/files", "GET /api/file?", "POST /api/control",
                 "GET /health", "GET /apple-touch-icon", "GET /favicon.ico",
                 "GET / ")  # 控制台页面
        do_filter = "filter=0" not in self.path

        lines = []
        if os.path.exists(LOG_FILE):
            try:
                # 【H6 优化】环形缓冲只读文件尾部, 不再把整个日志(已 600KB+)载入内存
                # 旧写法 f.readlines() 每次读取全文件再切片, 日志持续增长后开销越滚越大
                with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                    raw = list(collections.deque(f, maxlen=n * 3))
                # 先取尾部更多行，过滤后再截取目标数量
                raw = raw[-(n * 3):]
                if do_filter:
                    raw = [ln for ln in raw if not any(x in ln for x in NOISE)]
                lines = raw[-n:]
            except Exception as e:
                lines = ["[读取日志失败] %s" % e]
        else:
            lines = ["[日志文件不存在] %s" % LOG_FILE]

        payload = {
            "code": 200,
            "data": {
                "file": LOG_FILE,
                "count": len(lines),
                "filtered": do_filter,
                "lines": [ln.rstrip("\r\n") for ln in lines]
            }
        }
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def serve_files(self):
        """【B3 新增】列出工作目录真实文件（名称/大小/修改时间）"""
        def size_h(n):
            if n < 1024:
                return "%d B" % n
            if n < 1024 * 1024:
                return "%.1f KB" % (n / 1024.0)
            return "%.1f MB" % (n / 1024.0 / 1024.0)

        targets = [
            ("wb_server.py",      os.path.join(BASE_DIR, "wb_server.py"),            "py"),
            ("index.html",        os.path.join(BASE_DIR, "index.html"),              "html"),
            ("panel/index.html",  os.path.join(BASE_DIR, "panel", "index.html"),     "html"),
            ("wbstart",           os.path.join(BASE_DIR, "wbstart"),                 "sh"),
            ("server.log",        LOG_FILE,                                          "log"),
        ]
        items = []
        for name, path, kind in targets:
            ex = os.path.exists(path)
            st = os.stat(path) if ex else None
            items.append({
                "name": name,
                "kind": kind,
                "exists": ex,
                "size": st.st_size if st else 0,
                "size_h": size_h(st.st_size) if st else "—",
                "mtime": time.strftime('%m-%d %H:%M', time.localtime(st.st_mtime)) if st else "—",
                "sensitive": name.endswith("tokens.json")
            })

        payload = {"code": 200, "data": {"dir": BASE_DIR, "files": items}}
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, result):
        """【AM】统一的 JSON 响应助手（供目录/单文件读取复用）。"""
        try:
            body = json.dumps({"code": 200, "data": result}, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except Exception:
            pass

    def serve_file_content(self):
        """【F1 新增】读取指定文件内容，供面板点击查看

        三道安全门禁：
          1. 白名单 —— 只允许读工作目录下已知文件，拒绝目录穿越；
          2. 凭据脱敏 —— wb_tokens.json 的 token 字段一律替换为占位符；
          3. 大小上限 —— 超过 200KB 只返回前段，防止页面卡死。
        """
        import urllib.parse

        ALLOW = (
            "wb_server.py", "index.html", "wbstart",
            "panel/index.html", "wb_tokens.json",
            "server.log", "server.log.1",
            # 【AL】持久化数据文件（面板「数据存储」可点开查看）
            "counters.json", "modelstats.json", "reqlogs.json",
            "blocked_models.json", "manifest.json", "official_models.json",
        )

        name = ""
        try:
            if "name=" in self.path:
                name = urllib.parse.unquote(self.path.split("name=")[-1].split("&")[0])
        except Exception:
            name = ""

        result = {"name": name, "ok": False, "content": "", "size": 0, "msg": ""}

        # 【AM】reqhistory/ 目录：列明细 / 读单日
        if name == "reqhistory/" or name == "reqhistory":
            try:
                fs = []
                if os.path.isdir(REQHIST_DIR):
                    for fn in sorted(os.listdir(REQHIST_DIR), reverse=True):
                        if not fn.endswith(".jsonl"):
                            continue
                        fp = os.path.join(REQHIST_DIR, fn)
                        sz = os.path.getsize(fp)
                        try:
                            n = sum(1 for _ in open(fp, "r", encoding="utf-8", errors="replace"))
                        except Exception:
                            n = 0
                        fs.append((fn, sz, n, time.strftime('%m-%d %H:%M', time.localtime(os.path.getmtime(fp)))))
                if not fs:
                    result.update(ok=True, msg="目录为空", content="（暂无明细存档）", size=0)
                else:
                    lines = ["明细全量存档目录  " + REQHIST_DIR, ""]
                    tot = 0
                    for fn, sz, n, mt in fs:
                        tot += sz
                        lines.append("  %-22s %6.1f KB  %5d 条   %s" % (fn, sz / 1024.0, n, mt))
                    lines.append("")
                    lines.append("共 %d 个文件，合计 %.1f KB" % (len(fs), tot / 1024.0))
                    lines.append("")
                    lines.append("提示：在「文件」页的存储卡里点 reqhistory/ 可看本清单；")
                    lines.append("     单个日期文件可直接通过 /api/file?name=reqhistory/<文件名> 查看。")
                    result.update(ok=True, msg="%d 个文件" % len(fs),
                                  content="\n".join(lines), size=tot)
            except Exception as e:
                result["msg"] = "目录读取失败: %s" % e
            self._send_json(result)
            return

        if name.startswith("reqhistory/"):
            sub = name[len("reqhistory/"):]
            if (not sub) or ("/" in sub) or (".." in sub) or (not sub.endswith(".jsonl")):
                result["msg"] = "非法路径"
            else:
                fp = os.path.join(REQHIST_DIR, sub)
                if not os.path.exists(fp):
                    result["msg"] = "文件不存在"
                else:
                    try:
                        with open(fp, "r", encoding="utf-8", errors="replace") as f:
                            body = f.read()
                        result.update(ok=True, msg="读取成功",
                                      size=os.path.getsize(fp), content=body)
                    except Exception as e:
                        result["msg"] = "读取失败: %s" % e
            self._send_json(result)
            return

        # 门禁 1: 白名单校验（顺带挡住 ../ 与绝对路径）
        if not name or name not in ALLOW:
            result["msg"] = "不允许访问: %s" % (name or "(空)")
        else:
            fpath = os.path.join(BASE_DIR, name)
            if name == "server.log":
                fpath = LOG_FILE
            elif name == "server.log.1":
                fpath = LOG_FILE + ".1"

            try:
                if not os.path.exists(fpath):
                    result["msg"] = "文件不存在"
                else:
                    sz = os.path.getsize(fpath)
                    # 门禁 2: 凭据脱敏
                    if name.endswith("tokens.json"):
                        with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                            data = json.load(f)
                        masked = {}
                        for k, v in (data.items() if isinstance(data, dict) else []):
                            if isinstance(v, dict):
                                masked[k] = {
                                    "refresh_token": "***已脱敏***",
                                    "access_token": "***已脱敏***",
                                    "token_expiry": v.get("token_expiry", 0),
                                }
                            else:
                                masked[k] = "***已脱敏***"
                        result.update(
                            ok=True,
                            content=json.dumps(masked, ensure_ascii=False, indent=2),
                            size=sz,
                            msg="凭据已脱敏显示"
                        )
                    else:
                        # 门禁 3: 大小上限
                        MAX = 200 * 1024
                        with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                            content = f.read(MAX)
                        if sz > MAX:
                            content += "\n\n... (文件过大，仅显示前 200KB)"
                        result.update(ok=True, content=content, size=sz, msg="读取成功")
            except Exception as e:
                result["msg"] = "读取异常: %s" % e

        body = json.dumps({"code": 200, "data": result}, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _check_panel_auth(self):
        """【BP4】面板鉴权：设置里有 panel_token 时，校验 ?token= 或 X-Panel-Token。
        返回 True=放行。仅对 /panel 与 /api/* 生效；/v1/* 网关接口不受影响。"""
        tk = str(get_setting("panel_token", "") or "").strip()
        if not tk:
            return True
        try:
            got = ""
            if "?" in self.path:
                qs = self.path.split("?", 1)[1]
                for kv in qs.split("&"):
                    if kv.startswith("token="):
                        got = urllib.parse.unquote(kv[6:])
                        break
            if not got:
                got = self.headers.get("X-Panel-Token", "") or ""
            return got == tk
        except Exception:
            return False

    def _deny_panel(self):
        body = json.dumps({"code": 403, "error": "需要访问密钥（?token=…）"},
                          ensure_ascii=False).encode("utf-8")
        self.send_response(403)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def serve_login(self):
        """【C7】设备授权登录：?action=start 起会话；?action=poll&state= 轮询。"""
        if not self._check_panel_auth():
            self._deny_panel(); return
        qs = self.path.split("?", 1)[1] if "?" in self.path else ""
        params = {}
        for kv in qs.split("&"):
            if "=" in kv:
                k, v = kv.split("=", 1)
                params[k] = urllib.parse.unquote(v)
        act = params.get("action", "start")
        try:
            if act == "start":
                data = login_start(params.get("realm", "cn"))
            elif act == "poll":
                data = login_poll(params.get("state", ""))
            else:
                data = {"error": "未知 action"}
        except Exception as e:
            data = {"done": False, "error": str(e)[:160]}
        body = json.dumps({"code": 0, "data": data}, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def serve_settings(self):
        """【BP4】读写运行期设置（超时/阈值/密钥等）。"""
        if not self._check_panel_auth():
            self._deny_panel(); return
        if self.command == "GET":
            with _settings_lock:
                cur = dict(_settings) if _settings else dict(DEFAULT_SETTINGS)
            cur = dict(cur)
            if cur.get("panel_token"):
                cur["panel_token_set"] = True
                cur["panel_token"] = ""
            body = json.dumps({"code": 0, "data": cur}, ensure_ascii=False).encode("utf-8")
        else:
            try:
                ln = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(ln) if ln else b"{}"
                req = json.loads(raw.decode("utf-8") or "{}")
                cur = save_settings(req.get("settings") or req)
                body = json.dumps({"code": 0, "data": {"ok": True, "settings": _public_settings(cur)}},
                                  ensure_ascii=False).encode("utf-8")
                log("[面板] 设置已更新: %s" % json.dumps(req.get("settings") or req, ensure_ascii=False)[:200])
            except Exception as e:
                body = json.dumps({"code": 500, "error": str(e)}, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def serve_monitor(self):
        total_credits = 0.0
        total_used = 0.0
        active_count = 0
        accs_data = []
        now = time.time()
        for acc in account_pool:
            total_credits += acc["credits"]
            total_used += acc["used"]
            if acc["status"] == "active":
                active_count += 1
            ttl = max(0, int((acc["token_expiry"] - now) / 60))
            accs_data.append({
                # 【B7】统一 _mask；昵称一并下发供前端优先显示
                "phone": _mask(acc["phone"]),
                "nickname": acc.get("nickname") or "",
                "credits": acc["credits"],
                "used": acc["used"],
                "pkg_count": acc["pkg_count"],
                "packages": acc["packages"],
                "status": acc["status"],
                "ttl_min": ttl,
                "calls": acc["calls"],
                "errors": acc["errors"],
                "fails": acc.get("fails", 0),
                "breaker_until": acc.get("breaker_until", 0.0),
                "disabled": acc.get("disabled", False),
                "disabled_until": acc.get("disabled_until", 0.0),
                "disabled_auto": acc.get("disabled_auto", False),
                "paused": acc.get("paused", False),          # 【B1】
                "realm": acc_realm(acc),                     # 【QM · A9】区服
                "last_active": acc["last_active"]
            })

        result = {
            "summary": {
                "total_accounts": len(account_pool),
                "active_accounts": active_count,
                "total_credits": round(total_credits, 2),
                "total_used": round(total_used, 2),
                "timeouts": int(_timeout_total),          # 【W3】累计上游超时次数
                "model_stats": model_stats_snapshot(),    # 【A1/A2/A3】模型/账号统计
                "reqhist": reqhist_stats(),               # 【X】明细存档统计
                "storage": storage_status(),              # 【Z2】持久化存储状态
                "blocked_models": sorted(_blocked_models),  # 【B4】模型黑名单
                "allowed_models": sorted(_allow_models),   # 【BP1】模型白名单
                "model_probes": _probes,                   # 【B5】模型探测结果
                "pool_hold": pool_hold_remaining(),        # 【BP3】池级暂停剩余秒
                "settings": _public_settings(_settings or DEFAULT_SETTINGS),  # 【BP4】
                "auth_required": bool(str(get_setting("panel_token", "") or "").strip()),
                "timeout_last": _timeout_last,            # 【W3】最近一次超时时间
                "gateway_port": PORT,
                "upstream": "cn+global" if GLOBAL_ENABLED else "copilot.tencent.com",
                "upstream_cn": CHAT_BASE_CN,                 # 【QM · A9】
                "upstream_global": CHAT_BASE_GLOBAL,         # 【QM · A9】
                "global_enabled": GLOBAL_ENABLED,            # 【QM · A9】
                "model_realm": dict(_model_realm),           # 【QM · A5】模型→区服映射
                "model_domains": dict(_model_domains),       # 【QM2 · A4】模型→各域倍率
                "model_dead": sorted(_model_dead.keys()),    # 【QM2 · A3】被拉黑的模型
                "pid": os.getpid(),
                "uptime_sec": int(time.time() - START_TIME),
                "started_at": time.strftime('%m-%d %H:%M', time.localtime(START_TIME)),
                "tokens_ok": os.path.exists(TOKENS_FILE),
                "log_file": LOG_FILE,
                "log_size": os.path.getsize(LOG_FILE) if os.path.exists(LOG_FILE) else 0,
                "auto_start": _detect_plist_key("RunAtLoad"),
                "keep_alive": _detect_plist_key("KeepAlive")
            },
            "accounts": accs_data,
            "logs": list(request_logs)
        }
        out_bytes = json.dumps({"code": 0, "data": result}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out_bytes)))
        self.end_headers()
        self.wfile.write(out_bytes)

    def serve_official_models(self):
        # 【K1】实时优先：?live=1 强制刷新；300s 内用缓存；失败降级快照文件
        q = self.path.split("?", 1)[1] if "?" in self.path else ""
        force = ("live=1" in q) or ("refresh=1" in q)
        src, fetched, body_bytes = "snapshot", "", None
        try:
            live, _errs = fetch_live_models(force=force)
            if live:
                # 【QM3 · C5 补】面板模型页读的是本接口原始条目 →
                #   这里补 realms（该模型支持的全部区服）+ rates（各域倍率），
                #   供「国内/国际/双域」三态胶囊与双域价格对比
                _out = []
                for _m in live:
                    try:
                        _mid = _m.get("id")
                        _dm = _model_domains.get(_mid) or {}
                        _rl = sorted([k for k, v in _dm.items() if v is not None])
                        _m = dict(_m)
                        _m["realms"] = _rl or [_m.get("realm") or "cn"]
                        # 各域倍率文本：{cn: 'x0.29', global: 'x0.00'}
                        _m["rates"] = {k: ("x%g" % v if v else "免费") for k, v in _dm.items()}
                    except Exception:
                        pass
                    _out.append(_m)
                body_bytes = json.dumps(_out, ensure_ascii=False).encode("utf-8")
                src = "live"
                fetched = _MODELS_LIVE.get("fetched_at") or ""
        except Exception:
            pass
        if body_bytes is None:
            cache_path = os.path.join(BASE_DIR, "official_models.json")
            if os.path.exists(cache_path):
                with open(cache_path, "rb") as f:
                    body_bytes = f.read()
            else:
                body_bytes = json.dumps([], ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body_bytes)))
        self.send_header("X-Models-Source", src)
        self.send_header("X-Models-Fetched", fetched)
        self.end_headers()
        self.wfile.write(body_bytes)


    @staticmethod
    def _model_credits_plain(credits):
        """把上游 credits 串规整为可读倍率文本：'x0.11 credits'→'×0.11'；'1.0x'→'×1.0'。"""
        s = str(credits or '').strip()
        if not s:
            return '', None
        raw = s.lower().replace('credits', '').replace('×', '').replace('x', '').strip()
        try:
            v = float(raw)
        except Exception:
            return s, None
        if v == 0:
            return '（免费）', 0.0
        return ' ×%g' % v, v

    @classmethod
    def _model_rich(cls, m):
        """【A1】把实时目录条目扩成富信息（OpenAI 兼容，字段只增不减）。"""
        mid = m.get('id') or ''
        name = m.get('name') or mid
        plain, rate = cls._model_credits_plain(m.get('credits'))
        disp = name if not plain else ('%s%s' % (name, plain))
        ctx = int(m.get('maxInputTokens') or 0)
        caps = {'reasoning': bool(m.get('supportsReasoning')),
                'vision': bool(m.get('supportsImages')),
                'tools': True}
        return {
            'id': mid,
            'object': 'model',
            'created': int(_MODELS_LIVE.get('ts') or time.time()),
            'owned_by': m.get('vendor') or 'tencent',
            # 【QM · B4】区服标记（面板据此显示「国际」标签；非标准字段，客户端会忽略）
            'realm': m.get('realm') or 'cn',
            # 【QM3 · C5】该模型支持的全部区服（双域模型 → ['cn','global']，供面板区分）
            'realms': (sorted([k for k, v in (_model_domains.get(mid) or {}).items() if v is not None])
                       or [m.get('realm') or 'cn']),
            # —— 富信息（Minis 等客户端可读）——
            'name': disp,                    # 【B5】带倍率（Minis / Cherry / OpenWebUI 读这个）
            'display_name': disp,            # 【B5】同步带倍率（LobeChat 等优先读 displayName）
            'credits': m.get('credits') or '',
            'rate': rate,                    # 数字倍率，未知为 null
            'rate_text': plain,              # '×0.11' / '免费' / ''
            # 【B5】描述前加倍率前缀（对齐参考项目 fmtCreditsPrefix：下游面板可直接展示）
            'description': (((plain.strip() or '') + ' ' + (m.get('description') or '')).strip()
                            if plain.strip() else (m.get('description') or '')),
            'context_length': ctx,
            'max_input_tokens': ctx,
            'supports_reasoning': bool(m.get('supportsReasoning')),
            'supports_images': bool(m.get('supportsImages')),
            'capabilities': caps,
            'vendor': m.get('vendor') or '',
            'tags': m.get('tags') or [],
            'free': (rate == 0),
        }

    def serve_models_simple(self):
        # 【K1】优先实时目录（供对话页下拉），失败回退静态列表
        models = []
        try:
            _live, _e = fetch_live_models()
            if _live:
                models = [self._model_rich(_m) for _m in _live]
        except Exception:
            models = []
        if not models:
            # 【A1-3】兜底改用 official_models.json 快照（30 个富信息），不再用旧的 9 个假货
            try:
                snap = os.path.join(BASE_DIR, "official_models.json")
                if os.path.exists(snap):
                    with open(snap, "r", encoding="utf-8") as f:
                        for _m in (json.load(f) or []):
                            models.append(self._model_rich(_m))
            except Exception:
                models = []
        # 【QM2 · A3】统一剔除已拉黑的模型（含兜底路径）→ Minis 里不显示
        try:
            models = [m for m in models if not model_is_dead(m.get("id"))]
        except Exception:
            pass
        if not models:
            models = [
                {"id": "hy3", "object": "model", "owned_by": "tencent",
                 "name": "Hy3（免费）", "display_name": "Hy3（免费）", "context_length": 256000},
                {"id": "deepseek-v4.1-flash", "object": "model", "owned_by": "deepseek",
                 "name": "DeepSeek V4.1 Flash ×0.11", "display_name": "DeepSeek V4.1 Flash ×0.11",
                 "context_length": 1000000},
                {"id": "kimi-k2.7", "object": "model", "owned_by": "moonshot",
                 "name": "Kimi K2.7 ×0.57", "display_name": "Kimi K2.7 ×0.57",
                 "context_length": 131072},
                {"id": "glm-5.3-flash", "object": "model", "owned_by": "zhipu",
                 "name": "GLM 5.3 Flash ×0.06", "display_name": "GLM 5.3 Flash ×0.06",
                 "context_length": 1000000},
                {"id": "minimax-m2.5", "object": "model", "owned_by": "minimax",
                 "name": "MiniMax-M2.5 ×0.18", "display_name": "MiniMax-M2.5 ×0.18",
                 "context_length": 200000},
                {"id": "deepseek-v4-pro", "object": "model", "owned_by": "deepseek",
                 "name": "DeepSeek V4 Pro ×0.51", "display_name": "DeepSeek V4 Pro ×0.51",
                 "context_length": 1000000}
            ]
        out_bytes = json.dumps({"object": "list", "data": models}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out_bytes)))
        self.end_headers()
        self.wfile.write(out_bytes)

    def do_POST(self):
        # 【E1 修复】统一断连兜底：流式对话中途客户端断开时静默处理
        try:
            self._do_POST_inner()
        except _CLIENT_GONE:
            pass
        except Exception as e:
            _quiet_log("do_POST 异常: %r" % (e,))

    def _do_POST_inner(self):
        raw_path = self.path.split("?", 1)[0]
        if raw_path == "/api/control":
            self.handle_control()
            return
        # 【BP4 修复】POST /api/settings —— 原来只在 do_GET 注册，POST 落 404
        if raw_path == "/api/settings":
            self.serve_settings()
            return
        if raw_path in ("/responses", "/v1/responses"):
            # 【QM6】OpenAI Responses API 透传（MT-Agent 等新客户端用这个）
            self.handle_responses()
            return
        if self.path in ["/v1/chat/completions", "/chat/completions"]:
            self.handle_chat()
        else:
            self.send_response(404)
            self.end_headers()

    def handle_control(self):
        """【B4 新增】面板服务控制：只提供安全动作

        安全设计说明：
          - reload : 重新加载本轮询池的凭据文件
          - sync   : 触发一次全账号官方账本同步
          - ping   : 探活
        刻意不提供 stop/restart —— 网页按钮若能杀掉自身所连接的服务，
        将导致页面永久失联且无法从界面恢复，属设计缺陷而非功能。
        需要真·重启请走终端:
          launchctl kickstart -k system/com.jkgl.workbuddy
        """
        try:
            ln = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(ln) if ln else b"{}"
            req = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            req = {}

        action = req.get("action", "")
        result = {"action": action, "ok": False, "msg": ""}

        try:
            if action == "reload":
                load_accounts()
                result.update(ok=True, msg="已重新加载 %d 个账号" % len(account_pool))
            elif action == "sync":
                sync_all_accounts()
                result.update(ok=True, msg="账本同步完成 · %d 个账号" % len(account_pool))
            elif action == "ping":
                result.update(ok=True, msg="服务在线 · PID %d" % os.getpid())
            elif action == "restart":
                _spawn_delayed("kickstart")
                result.update(ok=True, msg="服务将在 2 秒后重启，页面稍后自动重连")
            elif action == "heal":
                _spawn_delayed("kickstart")
                result.update(ok=True, msg="自愈已触发，服务将在 2 秒后重启")
            elif action == "start":
                _spawn_delayed("bootstrap")
                result.update(ok=True, msg="启动指令已下发（2 秒后执行）")
            elif action == "stop":
                # 【安全设计】停止 = 自杀，页面会永久失联，故必须二次确认
                if req.get("confirm") == "yes":
                    _spawn_delayed("bootout")
                    result.update(ok=True, msg="⚠️ 服务将在 2 秒后停止，需在终端手动恢复")
                else:
                    result.update(ok=False, msg="停止操作需二次确认")
            elif action == "account_disable":
                a = _match_acc(req.get("phone"))
                if a:
                    a["disabled"] = True
                    a["disabled_auto"] = False       # 手动禁用：无倒计时
                    a["disabled_until"] = 0.0
                    persist_account_state(a)
                    result.update(ok=True, msg="已禁用 %s（手动，无倒计时）" % _mask(a["phone"]))
                else:
                    result.update(msg="未找到账号")
            elif action == "account_enable":
                a = _match_acc(req.get("phone"))
                if a:
                    a["disabled"] = False
                    a["disabled_auto"] = False
                    a["disabled_until"] = 0.0
                    a["fails"] = 0
                    a["breaker_until"] = 0.0
                    persist_account_state(a)
                    result.update(ok=True, msg="已启用 %s" % _mask(a["phone"]))
                else:
                    result.update(msg="未找到账号")
            elif action == "block_model":
                m = str(req.get("model") or "").strip()
                if m:
                    _blocked_models.add(m)
                    globals()["_blocked_dirty"] = True
                    flush_blocked(force=True)
                    result.update(ok=True, msg="已屏蔽模型 %s" % m)
                else:
                    result.update(msg="缺少 model 参数")
            elif action == "probe_model":
                m = str(req.get("model") or "").strip()
                if not m:
                    result.update(msg="缺少 model 参数")
                else:
                    r = probe_one_model(m)
                    if r.get("ok"):
                        tag = "免费" if r.get("free") else ("Credit %.4f" % (r.get("credit") or 0))
                        result.update(ok=True, msg="探测完成 %s → %s" % (m, tag))
                    else:
                        result.update(msg="探测失败: %s" % r.get("msg"))
            elif action == "probe_all":
                ms = req.get("models") or []
                if not ms:
                    result.update(msg="缺少 models 列表")
                else:
                    ok_n = 0
                    for m in ms[:20]:              # 上限 20 个，防长任务
                        r = probe_one_model(str(m))
                        if r.get("ok"):
                            ok_n += 1
                        time.sleep(0.5)
                    result.update(ok=True, msg="探测完成 %d/%d 个模型" % (ok_n, len(ms[:20])))
            elif action == "allow_model":
                m = str(req.get("model") or "").strip()
                if m:
                    _allow_models.add(m)
                    globals()["_blocked_dirty"] = True
                    flush_blocked(force=True)
                    result.update(ok=True, msg="已加入白名单 %s" % m)
                else:
                    result.update(msg="缺少 model 参数")
            elif action == "unallow_model":
                m = str(req.get("model") or "").strip()
                if m and m in _allow_models:
                    _allow_models.discard(m)
                    globals()["_blocked_dirty"] = True
                    flush_blocked(force=True)
                    result.update(ok=True, msg="已移出白名单 %s" % m)
                else:
                    result.update(msg="该模型不在白名单")
            elif action == "allow_clear":
                n = len(_allow_models)
                _allow_models.clear()
                globals()["_blocked_dirty"] = True
                flush_blocked(force=True)
                result.update(ok=True, msg="白名单已清空（%d 个）" % n)
            elif action == "unblock_model":
                m = str(req.get("model") or "").strip()
                if m and m in _blocked_models:
                    _blocked_models.discard(m)
                    globals()["_blocked_dirty"] = True
                    flush_blocked(force=True)
                    result.update(ok=True, msg="已取消屏蔽 %s" % m)
                else:
                    result.update(msg="该模型不在黑名单")
            elif action == "import_tokens":
                # 【B7】面板粘贴 JSON 导入凭据（合并进 wb_tokens.json）
                #   修复 1.5.6 致命缺陷：旧实现 `for ph, item in newd.items()`
                #   把 JSON 顶层键当手机号。嵌套格式 {account:{...}, auth:{...}}
                #   顶层键恒为 "auth" → 所有账号撞同一键 → 后导入覆盖先导入，
                #   磁盘永远只有 1 条（实测：A 的 RT 被 B 直接盖掉）。
                #   新实现从【对象内部】取 uid/手机号作键，合并而非覆盖。
                raw = req.get("tokens")
                try:
                    newd = json.loads(raw) if isinstance(raw, str) else (raw or {})
                except Exception as e:
                    newd = None
                    result.update(msg="JSON 解析失败: %s" % e)
                if newd is not None:
                    recs = _normalize_import_payload(newd)
                    if not recs:
                        result.update(msg="未识别到有效凭据（需含 refresh_token 的账号对象）")
                    else:
                        try:
                            added = 0
                            updated = 0

                            def _merge(cur):
                                """在 _TOKENS_LOCK 内执行整个读-改-写事务。"""
                                nonlocal added, updated
                                for key, item in recs:
                                    old = cur.get(key)
                                    if isinstance(old, dict):
                                        merged = dict(old)
                                        merged.update(item)
                                        # 本次没带上的富字段，保留旧值
                                        for f in ("nickname", "realm", "domain"):
                                            if not merged.get(f) and old.get(f):
                                                merged[f] = old[f]
                                        cur[key] = merged
                                        updated += 1
                                    else:
                                        cur[key] = item
                                        added += 1
                                return cur

                            _tokens_mutate(_merge)
                            load_accounts()
                            result.update(ok=True,
                                          msg="导入完成：新增 %d 个，更新 %d 个（现有 %d 个）"
                                              % (added, updated, len(account_pool)))
                        except Exception as e:
                            result.update(msg="导入失败: %s" % e)
            elif action == "reset_local":
                # 【BP5d】清空除凭据外的本地数据（排障用，需二次确认）
                if req.get("confirm") != "yes":
                    result.update(msg="重置需二次确认（confirm=yes）")
                else:
                    removed = []
                    for fp in ("counters.json", "modelstats.json", "reqlogs.json",
                               "blocked_models.json", "settings.json", "timeouts.jsonl"):
                        p = os.path.join(BASE_DIR, fp)
                        try:
                            if os.path.exists(p):
                                os.remove(p); removed.append(fp)
                        except Exception:
                            pass
                    try:
                        if os.path.isdir(REQHIST_DIR):
                            for fn in os.listdir(REQHIST_DIR):
                                if fn.endswith(".jsonl"):
                                    os.remove(os.path.join(REQHIST_DIR, fn))
                            removed.append("reqhistory/*")
                    except Exception:
                        pass
                    result.update(ok=True, msg="已重置本地数据: " + (", ".join(removed) or "无文件"))
                    log("[面板] 本地数据已重置: %s" % (", ".join(removed) or "无"))
            elif action == "set_settings":
                try:
                    cur = save_settings(req.get("settings") or {})
                    result.update(ok=True, msg="设置已保存并生效（无需重启）")
                except Exception as e:
                    result.update(msg="保存失败: %s" % e)
            elif action == "account_pause" or action == "account_resume":
                # 【QM3 · C4】暂停功能已移除（与禁用语义重复）→ 兼容旧前端调用，直接同义于禁用/启用
                _p = (action == "account_pause")
                a = _match_acc(req.get("phone"))
                if a:
                    a["disabled"] = _p
                    a["disabled_auto"] = False
                    a["disabled_until"] = 0.0
                    if not _p:
                        a["breaker_until"] = 0.0; a["cool_until"] = 0.0; a["fails"] = 0
                    a["paused"] = False
                    persist_account_state(a)
                    result.update(ok=True, msg=("已禁用 %s" if _p else "已启用 %s") % _mask(a["phone"]))
                else:
                    result.update(msg="未找到账号")
            elif action == "account_revive":
                # 【B2】一键复活：清全部惩罚态（禁用+熔断+冷却+连败）
                a = _match_acc(req.get("phone"))
                if a:
                    a["disabled"] = False
                    a["disabled_auto"] = False
                    a["disabled_until"] = 0.0
                    a["breaker_until"] = 0.0
                    a["cool_until"] = 0.0
                    a["fails"] = 0
                    persist_account_state(a)
                    result.update(ok=True, msg="已复活 %s（清除全部惩罚态）" % _mask(a["phone"]))
                else:
                    result.update(msg="未找到账号")
            elif action == "account_revive_all":
                n = 0
                for a in account_pool:
                    a["disabled"] = False; a["disabled_auto"] = False
                    a["disabled_until"] = 0.0; a["breaker_until"] = 0.0
                    a["cool_until"] = 0.0; a["fails"] = 0; a["paused"] = False
                    persist_account_state(a); n += 1
                result.update(ok=True, msg="已复活全部 %d 个账号" % n)
            elif action == "account_delete":
                # 【QM2 · A5】删除账号：从 wb_tokens.json + 内存池 + 计数器同时移除。
                #   ⚠️ 不可逆（凭据文件真删）—— 面板侧必须二次确认。
                a = _match_acc(req.get("phone"))
                if not a:
                    result.update(msg="未找到账号")
                else:
                    _full = a["phone"]
                    # ① 从凭据文件删除
                    try:
                        # 【B7】统一助手：整段读-改-写持锁
                        def _drop(cur, _k=_full):
                            cur.pop(_k, None)
                            return cur
                        _tokens_mutate(_drop)
                    except Exception as e:
                        result.update(msg="删除凭据失败: %s" % str(e)[:60]); return
                    # ② 从内存池移除
                    try:
                        account_pool.remove(a)
                    except Exception:
                        pass
                    # ③ 清计数器（否则同名号重加会继承旧统计）
                    try:
                        with _counters_lock:
                            _counters.pop(_full, None)
                            _counters_dirty = True
                    except Exception:
                        pass
                    log(f"[账号] 已删除 {_mask(_full)}（凭据+内存+计数）")
                    result.update(ok=True, msg="已删除 %s" % _mask(_full))
            elif action == "clearlog":
                n = 0
                if os.path.exists(LOG_FILE):
                    try:
                        with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                            content = f.read()
                        n = content.count("\n")
                        # 【D1 修复】归档用 copy 而非 rename
                        # 真因: os.rename 会更换 inode，而服务进程的 stdout fd
                        #       仍指向旧 inode，导致清空后新日志继续写进 .1 文件。
                        # 改用 "copy 内容 -> 原地 truncate"，保持 inode 不变。
                        bak = LOG_FILE + ".1"
                        try:
                            with open(bak, "w", encoding="utf-8") as f:
                                f.write(content)
                        except Exception as e:
                            log("归档日志失败: %s" % e)
                        # 原地截断：inode 不变，服务 fd 依然有效
                        with open(LOG_FILE, "w", encoding="utf-8") as f:
                            f.write("")
                    except Exception as e:
                        log("清空日志失败: %s" % e)
                log("日志已清空（归档 %d 行到 server.log.1）" % n)
                result.update(ok=True, msg="已清空 %d 行 · 旧日志归档为 .1" % n)
            else:
                result.update(msg="不支持的操作: %s" % (action or "(空)"))
        except Exception as e:
            result.update(msg="执行异常: %s" % e)

        log("[面板控制] %s -> %s" % (action, result["msg"]))
        body = json.dumps({"code": 200, "data": result}, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_chat(self):
        start_t = time.time()
        content_len = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_len)
        try:
            req_json = json.loads(post_data.decode("utf-8"))
        except Exception:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'{"error": "Invalid JSON"}')
            return

        model_name = req_json.get("model", "deepseek-v4.1-flash")
        # 【BP1】模型过滤：黑名单命中即拒；白名单非空时只放行列表内
        if (model_name in _blocked_models) or (_allow_models and model_name not in _allow_models):
            body = json.dumps({"error": {
                "message": f"模型 {model_name} 已被面板屏蔽", "type": "model_blocked",
                "code": "model_blocked"}}, ensure_ascii=False).encode("utf-8")
            self.send_response(403)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            append_log(model_name, "-", "Blocked(403)", 0)
            return
        client_wants_stream = req_json.get("stream", True)

        req_json["stream"] = True
        # 【BP5】发往上游前做两处修复（对照上游 toolrepair/reasoningrepair）
        try:
            _r = bool(req_json.get("_wb_repair", True))
            if _r:
                req_json, _info = repair_tool_sequence(req_json)
                req_json = repair_reasoning(req_json)
        except Exception as _e:
            log(f"[修复] 跳过: {_e}")
        # 【QM9】客户端兼容层（各客户端字段形态归一 + 指纹清洗）
        try:
            _marks = apply_client_compat(req_json,
                                         efforts_map=_model_efforts(),
                                         sanitize=True)
            if _marks:
                log(f"[compat] {model_name}: {'、'.join(_marks)}")
        except Exception as _e:
            log(f"[compat] 跳过: {_e}")
        req_json.pop("_wb_repair", None)
        upstream_payload = json.dumps(req_json).encode("utf-8")

        # 【I3 新增】网络类失败「换号重试」—— 上游 rotateBackoff 的等价物。
        #   流程: 挑号 → 请求(内含 I1 的 DNS 重试) → 失败则分类:
        #     · 超时     → 【不换号】(I2 语义: 换号撞同一个慢上游, 白换还拖长)
        #     · 其它网络错误 → 换号 + 指数退避(BACKOFF_BASE·2^n 封顶 8s, ±25% 抖动)
        #     · 已试过的号不再选(exclude); 失败账号进 30s 短冷却(I4)
        #   上限 MAX_ROTATE 次, 防长尾把客户端拖死。
        tried = set()
        acc = None
        resp = None
        last_err = None
        _max_rot = int(get_setting("max_rotate", MAX_ROTATE) or MAX_ROTATE)   # 【BP2】可调
        # 【QM · A7】模型 → 首选 realm：优先同区服账号（global 模型别派给 CN 号）。
        # 【QM2 · A4】改为按「更便宜域」选：双域模型挑倍率更低的域
        #   （hy4-preview 国际免费 → 走 global；hy3 同价 → cn 优先省国际额度）。
        try:
            _pref_realm = prefer_realm_for(model_name)
            if _pref_realm == "cn":
                _pref_realm = None   # CN 是默认池，不额外过滤（零回归）
        except Exception:
            _pref_realm = None
        for _attempt in range(_max_rot):
            # 【QM2 修复】偏好区服无号时的回退策略：
            #   · 模型是「国际独占」→ 全程锁定 global，绝不回退 CN 号
            #     （CN 号打国际模型必 400 "only available for authorized users"，白撞还拖长）
            #   · 其它 → 回退任意号（宁可试错也别 503）
            _excl = False
            try:
                _dm = _model_domains.get(model_name) or {}
                _excl = ("global" in _dm) and ("cn" not in _dm)
            except Exception:
                _excl = False
            _lock_realm = _pref_realm
            if _excl and _pref_realm == "global":
                _lock_realm = "global"
            acc = get_next_valid_account(exclude=tried, realm=_lock_realm)
            if acc is None and _lock_realm == "global" and not _excl:
                log(f"模型 {model_name} 偏好区服 global 无可用号，回退任意号")
                acc = get_next_valid_account(exclude=tried)
            elif acc is None and _excl:
                log(f"模型 {model_name} 为国际独占，无可用 global 号 → 不试 CN 号（避免必败 400）")
            if not acc:
                break
            tried.add(acc["phone"])
            acc["calls"] += 1
            bump_counter(acc["phone"], "calls")      # 【A 组】置脏, 30s 内落盘
            acc["last_active"] = time.strftime('%H:%M:%S')

            # 【QM · A7】按账号 realm 选上游域 —— 原写死 copilot.tencent.com，
            #   国际版 token 发国内域必 401（这就是「能添加、用不了」的最终杀手）。
            #   CN 号：chat_base 恒返回原值、Origin 恒原值 → 零回归。
            _origin = web_origin(acc)
            _is_global = acc_realm(acc) == "global"
            # 【QM2 · A2】global 号出站前修三处硬约束（CN 分支 payload 逐字不动）
            if _is_global:
                _payload, _marks = prepare_global_payload(req_json)
                if _marks:
                    log(f"[QM2] global 预处理({acc['phone']}): {_marks}")
            else:
                _payload = upstream_payload
            acc["_inflight"] = acc.get("_inflight", 0) + 1   # 【QM · A10】在飞计数
            upstream_req = urllib.request.Request(chat_base(acc) + "/v2/chat/completions", headers={
                "Authorization": f"Bearer {acc['access_token']}",
                "Content-Type": "application/json",
                "Origin": _origin,
                "Referer": _origin + "/",
                "User-Agent": "Mozilla/5.0"
            }, data=_payload)

            try:
                resp = _open_upstream(upstream_req, ctx,
                                      timeout=int(get_setting("timeout_header", UP_TIMEOUT_HEADER)))  # 【BP2】可调
                try:
                    # 【A4】分层超时：响应头已到 → 后续读改「空闲超时」。
                    #   持续有数据时永不触发；真正断流 UP_TIMEOUT_IDLE 秒才判死。
                    resp.fp.raw._sock.settimeout(int(get_setting("timeout_idle", UP_TIMEOUT_IDLE)))  # 【BP2】可调
                except Exception:
                    pass
                record_success(acc)                     # 【R 组】成功清零连败/熔断
                clear_pool_timeout()                    # 【BP3】成功 → 清连续超时计数
                break                                   # 成功（在飞保持到流结束）
            except Exception as e:
                last_err = e
                # 【QM2 修复】本次尝试失败 → 立即释放在飞计数。
                #   真因：原实现只在最外层成功/异常路径释放，每次失败尝试都 +1 不减，
                #   global 号 _inflight 很快 ≥2 → 被 A10 并发闸永久跳过 → 回退 CN 号 → 400。
                try:
                    acc["_inflight"] = max(0, acc.get("_inflight", 1) - 1)
                except Exception:
                    pass
                if _is_upstream_timeout(e):
                    bump_timeout("读超时")               # 【W3】超时单独计数
                    bump_pool_timeout()                 # 【BP3】池级连续超时 → 退避
                    append_timeout_log(model_name, acc["phone"],
                                       time.time() - start_t, e)  # 【BP3】超时明细
                    log(f"上游超时({acc['phone']}), 不换号(换号撞同一个慢上游): {e}")
                    break                               # 交给 I2 的 except 处理 → 503
                # 【QM2 · 关键修复】统一读一次上游响应体（★ HTTPError 的 body 只能读一次）
                _eb = ""
                try:
                    if isinstance(e, urllib.error.HTTPError):
                        _eb = e.read(800).decode("utf-8", "replace")
                except Exception:
                    _eb = ""
                _code = getattr(e, "code", None)
                if _eb:
                    log(f"[QM2] {model_name} realm={acc_realm(acc)} HTTP {_code} "
                        f"body={_eb.replace(chr(10),' ')[:180]}")
                # 11102 分两种（实测）：
                #   ① "service info not found"              → 真不可用 → 拉黑模型
                #   ② "only available for authorized users"  → 域不对（选号错误，不拉黑）
                _notfound = "service info not found" in _eb
                _noperm = "only available for authorized users" in _eb
                if _notfound:
                    mark_model_dead(model_name)
                    log(f"[QM2] 模型 {model_name} 真不可用(11102 not found)，已拉黑 6h")
                # 【QM2】请求级 4xx（模型/参数/权限问题）→ 不罚账号、不冷却、立即停轮换
                #   对齐参考项目 ErrBadParams/ErrClient/ErrModelBlocked 语义：
                #   「同一个 body 换任何号都会被拒」，罚号只会把健康号全打进冷却。
                if _code == 400 or _noperm or _notfound:
                    log(f"[QM2] {model_name} 请求级 4xx({_code})，不罚账号、停止轮换")
                    break
                # 其余（5xx / 网络类）才按账号问题处理
                record_failure(acc, "转发失败")             # 【R 组】连败/熔断/自动禁用
                acc["cool_until"] = time.time() + 30        # 【I4】30s 短冷却
                if _attempt < MAX_ROTATE - 1:
                    d = _backoff_delay(_attempt)
                    log(f"账号 {acc['phone']} 转发失败, {d:.2f}s 后换号重试: {e}")
                    time.sleep(d)

        if resp is None:
            if acc is None:
                self.send_response(503)
                self.end_headers()
                self.wfile.write(b'{"error": "No available account in pool"}')
                return
            raise last_err if last_err else Exception("no upstream response")

        _t_first = None      # 【A1】首字节时刻（TTFT 基准）
        total_tokens = 0     # 供两个分支共用

        def _mark_first(line):
            """首个非空、非 [DONE] 的行 = 真正开始吐字。"""
            nonlocal _t_first
            if _t_first is None:
                s = (line or b"").strip()
                if s and b"[DONE]" not in s:
                    _t_first = (time.time() - start_t) * 1000

        _ptok = _ctok = 0      # 【AE】prompt/completion 分项（流式补采）
        try:
            if client_wants_stream:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()

                while True:
                    line = resp.readline()
                    if not line:
                        break
                    _mark_first(line)                      # 【A1】TTFT
                    # 【AE 修复】流式分支原先完全不解析 usage → token 永远不累计。
                    #   上游流式帧里出现 "usage" 即为最终统计（通常在最后一帧或 [DONE] 前）。
                    try:
                        if b'"usage"' in line or b"usage" in line:
                            _s = line.decode("utf-8", errors="replace").strip()
                            if _s.startswith("data:"):
                                _j = json.loads(_s[5:].strip())
                                _u = _j.get("usage")
                                if _u and "total_tokens" in _u:
                                    total_tokens = _u["total_tokens"]
                                    if isinstance(_u.get("prompt_tokens"), int) and isinstance(_u.get("completion_tokens"), int):
                                        _ptok = _u["prompt_tokens"]; _ctok = _u["completion_tokens"]
                    except Exception:
                        pass
                    try:
                        self.wfile.write(line)
                        self.wfile.flush()
                    except Exception:
                        break
                    if b"[DONE]" in line:
                        break

            else:
                full_content = ""
                full_reasoning = ""
                chat_id = f"chatcmpl-{int(time.time()*1000)}"

                while True:
                    line = resp.readline()
                    if not line:
                        break
                    s = line.decode("utf-8", errors="replace").strip()
                    if s.startswith("data:") and "[DONE]" not in s:
                        if _t_first is None:
                            _t_first = (time.time() - start_t) * 1000   # 【A1】TTFT
                        try:
                            j = json.loads(s[5:].strip())
                            if "id" in j:
                                chat_id = j["id"]
                            delta = j.get("choices", [{}])[0].get("delta", {})
                            full_content += delta.get("content", "")
                            full_reasoning += delta.get("reasoning_content", "")
                            usage = j.get("usage")
                            if usage and "total_tokens" in usage:
                                total_tokens = usage["total_tokens"]
                        except Exception:
                            pass
                    if "[DONE]" in s:
                        break

                non_stream_resp = {
                    "id": chat_id,
                    "object": "chat.completion",
                    "created": int(time.time()),
                    "model": model_name,
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": full_content,
                                "reasoning_content": full_reasoning if full_reasoning else None
                            },
                            "finish_reason": "stop"
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": len(full_content),
                        "total_tokens": total_tokens if total_tokens else (10 + len(full_content))
                    }
                }
                out_bytes = json.dumps(non_stream_resp, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(out_bytes)))
                self.end_headers()
                self.wfile.write(out_bytes)

            duration = int((time.time() - start_t) * 1000)
            _ttft = int(_t_first) if _t_first else None
            # 【AE 修复】流式分支若上游未回 usage → 保持 0（不编造），但记录已在流式循环里补采
            append_log(model_name, acc["phone"], "200 OK", duration, _ttft)
            record_model_stat(model_name, True, duration, _ttft, total_tokens,
                              acc["phone"] if acc else None)   # 【A1/A2/A3】
            # 【QM · A10】在飞计数释放（成功路径）
            if acc is not None:
                acc["_inflight"] = max(0, acc.get("_inflight", 1) - 1)

        except Exception as e:
            duration = int((time.time() - start_t) * 1000)
            _ph = acc["phone"] if acc else "-"
            # 【QM · A10】在飞计数释放（异常路径）
            if acc is not None:
                acc["_inflight"] = max(0, acc.get("_inflight", 1) - 1)
            # 【I2 修复】超时三态区分 —— 对齐上游 isUpstreamTimeout 语义。
            #   超时不是账号的问题: 同一份请求换到别的号会撞上同一个慢上游,
            #   故【不罚账号】(不累加 errors)、返回 503 + 明确文案,
            #   让客户端知道"重试无用"; 而不是笼统 502。
            #   其余错误(HTTP错误/连接重置等)仍按账号问题处理 → errors+1 + 502。
            if _is_upstream_timeout(e):
                record_model_stat(model_name, False, duration, None, 0, acc["phone"] if acc else None)
                append_log(model_name, _ph, "Timeout(503)", duration)
                log(f"上游超时(不计入账号错误): {e}")
                try:
                    body = json.dumps({
                        "error": {
                            "message": f"上游超时: {e}（换号无用，请稍后重试）",
                            "type": "upstream_timeout",
                            "code": "upstream_timeout",
                        }
                    }, ensure_ascii=False).encode("utf-8")
                    self.send_response(503)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except Exception:
                    pass
            else:
                # I3 已在换号循环里对网络类失败累加过 errors 并置冷却;
                # 走到这里的多是「换号也全失败」或非网络类异常, 故仍按账号问题记一次。
                if acc:
                    record_failure(acc, "换号全失败")          # 【R 组】连败/熔断/自动禁用
                    acc["cool_until"] = time.time() + 30      # 【I4】短冷却
                record_model_stat(model_name, False, duration, None, 0, acc["phone"] if acc else None)
                append_log(model_name, _ph, f"Err: {e}", duration)
                log(f"转发上游失败: {e}")
                try:
                    self.send_response(502)
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": f"Upstream error: {e}"}).encode("utf-8"))
                except Exception:
                    pass

    # ════════════════════════════════════════════════════════════════════════
    # 【QM6 · /responses】OpenAI Responses API 透传
    #
    # 背景：MT-Agent 等新客户端用 OpenAI Responses API（POST /responses），
    #       而我们的网关此前只实现 /chat/completions → 客户端拿 404。
    # 实测（2026-10-20）：
    #   · 上游国内站 copilot.tencent.com **原生支持** /responses（200 + 标准事件流）
    #   · 上游国际站 www.workbuddy.ai 的 /responses 是 404
    #   · body 里 input 必须是「纯字符串」或「带 type:"message" 的对象数组」
    #     （{role,content} 少了 type 会 400 "a message has empty content"）
    # 策略：原样透传（不做格式转换），思考事件/上下文/工具调用全部上游原产 → 品质最高。
    # ════════════════════════════════════════════════════════════════════════
    RESP_PATHS = ("/responses", "/v1/responses")

    def handle_responses(self):
        """【QM8】Responses API → Chat Completions 转换代理。

        上游 /responses 只挂混元后端（实测 20 个模型仅 hy3/hy3-x 可用），
        但 /v2/chat/completions 支持全部模型 → 这里做协议双向转换。
        """
        start_t = time.time()
        content_len = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_len)
        try:
            req_json = json.loads(post_data.decode("utf-8"))
        except Exception:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'{"error": {"message": "Invalid JSON"}}')
            return

        model_name = req_json.get("model") or "hy3"
        try:
            log(f"[responses] 收到 model={model_name!r} stream={req_json.get('stream')} "
                f"keys={sorted(req_json.keys())[:12]}")
        except Exception:
            pass

        # 未知模型兜底（客户端可能填 gpt-4o / claude-3.5 等）
        try:
            _known = model_name in (_model_domains or {})
            if not _known:
                try:
                    _live, _ = fetch_live_models()
                    _known = model_name in set((m.get("id") or "") for m in (_live or []))
                except Exception:
                    pass
            if not _known and model_name:
                log(f"[responses] 未知模型 {model_name!r} → 回退 hy3")
                model_name = "hy3"
                req_json["model"] = "hy3"
        except Exception:
            pass

        # 模型黑名单 / 白名单
        if (model_name in _blocked_models) or (_allow_models and model_name not in _allow_models):
            body = json.dumps({"error": {"message": "模型 %s 已被面板屏蔽" % model_name,
                                         "type": "model_blocked"}}, ensure_ascii=False).encode("utf-8")
            self.send_response(403)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            append_log(model_name, "-", "Blocked(403)", 0)
            return

        # ① 协议转换 Responses → Chat
        try:
            chat_body, _notes = resp_to_chat(req_json)
            if _notes:
                log(f"[responses] 转换: {','.join(_notes)}")
        except Exception as e:
            log(f"[responses] 转换失败: {e}")
            self.send_response(400)
            self.end_headers()
            self.wfile.write(json.dumps({"error": {"message": "请求转换失败: %s" % e}},
                                        ensure_ascii=False).encode("utf-8"))
            return
        # 【QM9】客户端兼容层（Chat 形态上再做一次归一，保证与 /chat/completions 同口径）
        try:
            _mk2 = apply_client_compat(chat_body,
                                       efforts_map=_model_efforts(),
                                       sanitize=True)
            if _mk2:
                log(f"[responses] compat: {'、'.join(_mk2)}")
        except Exception as e:
            log(f"[responses] compat 跳过: {e}")
        chat_body["stream"] = True
        payload = json.dumps(chat_body).encode("utf-8")

        tried = set()
        acc = None
        resp = None
        last_err = None
        _max_rot = int(get_setting("max_rotate", MAX_ROTATE) or MAX_ROTATE)
        try:
            _pref_realm = prefer_realm_for(model_name)
            if _pref_realm == "cn":
                _pref_realm = None
        except Exception:
            _pref_realm = None

        for _attempt in range(_max_rot):
            _excl = False
            try:
                _dm = _model_domains.get(model_name) or {}
                _excl = ("global" in _dm) and ("cn" not in _dm)
            except Exception:
                _excl = False
            _lock_realm = _pref_realm
            if _excl and _pref_realm == "global":
                _lock_realm = "global"
            acc = get_next_valid_account(exclude=tried, realm=_lock_realm)
            if acc is None and _lock_realm == "global" and not _excl:
                acc = get_next_valid_account(exclude=tried)
            if not acc:
                break
            tried.add(acc["phone"])
            acc["calls"] += 1
            bump_counter(acc["phone"], "calls")
            acc["last_active"] = time.strftime('%H:%M:%S')

            _is_global = acc_realm(acc) == "global"
            _origin = web_origin(acc)
            if _is_global:
                try:
                    payload, _mk = prepare_global_payload(chat_body)
                    if _mk:
                        log(f"[responses] global 预处理: {_mk}")
                except Exception:
                    pass

            acc["_inflight"] = acc.get("_inflight", 0) + 1
            upstream_req = urllib.request.Request(chat_base(acc) + "/v2/chat/completions",
                headers={"Authorization": "Bearer " + acc["access_token"],
                         "Content-Type": "application/json",
                         "Accept": "text/event-stream",
                         "Origin": _origin, "Referer": _origin + "/",
                         "User-Agent": "Mozilla/5.0"},
                data=payload)
            try:
                resp = _open_upstream(upstream_req, ctx,
                                      timeout=int(get_setting("timeout_header", UP_TIMEOUT_HEADER)))
                try:
                    resp.fp.raw._sock.settimeout(int(get_setting("timeout_idle", UP_TIMEOUT_IDLE)))
                except Exception:
                    pass
                record_success(acc)
                clear_pool_timeout()
                break
            except Exception as e:
                last_err = e
                try:
                    acc["_inflight"] = max(0, acc.get("_inflight", 1) - 1)
                except Exception:
                    pass
                _eb = ""
                try:
                    if isinstance(e, urllib.error.HTTPError):
                        _eb = e.read(800).decode("utf-8", "replace")
                except Exception:
                    _eb = ""
                _code = getattr(e, "code", None)
                if _eb:
                    log(f"[responses] {model_name} realm={acc_realm(acc)} HTTP {_code} "
                        f"body={_eb.replace(chr(10),' ')[:150]}")
                if _is_upstream_timeout(e):
                    bump_timeout("读超时"); break
                if _code == 400 or "service info not found" in _eb:
                    log(f"[responses] {model_name} 请求级 {_code}，停止轮换")
                    break
                record_failure(acc, "responses 转发失败")
                acc["cool_until"] = time.time() + 30
                if _attempt < MAX_ROTATE - 1:
                    time.sleep(_backoff_delay(_attempt))

        if resp is None:
            if acc is None:
                body = json.dumps({"error": {"message": "No available account in pool",
                                             "type": "no_account"}}, ensure_ascii=False).encode("utf-8")
                self.send_response(503)
            else:
                _m = "上游不可用"
                if isinstance(last_err, urllib.error.HTTPError):
                    try:
                        _m = "%s: %s" % (last_err.code, last_err.read(300).decode("utf-8", "replace"))
                    except Exception:
                        _m = str(last_err)
                body = json.dumps({"error": {"message": _m, "type": "upstream_error"}},
                                  ensure_ascii=False).encode("utf-8")
                self.send_response(502)
            try:
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                pass
            append_log(model_name, acc["phone"] if acc else "-", "Err(/responses)",
                       int((time.time() - start_t) * 1000))
            return

        # ── ② Chat SSE → Responses SSE 事件流转换 ──────────────────────────
        rid = "resp_" + uuid.uuid4().hex[:16]
        created_at = int(time.time())
        _t_first = None
        total_tokens = 0
        full_text = ""
        full_reason = ""
        _reason_item = "item_%s" % uuid.uuid4().hex[:8]
        _msg_item = "item_%s" % uuid.uuid4().hex[:8]
        _reason_opened = False
        _msg_opened = False
        _content_opened = False

        def _resp_obj(status, output=None, usage=None):
            o = {"id": rid, "object": "response", "created_at": created_at,
                 "model": model_name, "status": status,
                 "output": output or []}
            if usage:
                o["usage"] = usage
            return o

        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()

            def emit(ev, data):
                self.wfile.write(sse(ev, data))
                try:
                    self.wfile.flush()
                except Exception:
                    pass

            emit("response.created", {"type": "response.created", "response": _resp_obj("in_progress")})
            emit("response.in_progress", {"type": "response.in_progress", "response": _resp_obj("in_progress")})

            while True:
                line = resp.readline()
                if not line:
                    break
                s = line.decode("utf-8", "replace").strip()
                if not s.startswith("data:"):
                    continue
                d = s[5:].strip()
                if d == "[DONE]":
                    break
                try:
                    j = json.loads(d)
                except Exception:
                    continue
                if _t_first is None:
                    _t_first = (time.time() - start_t) * 1000
                ch = (j.get("choices") or [{}])[0]
                delta = ch.get("delta") or {}
                rc = delta.get("reasoning_content")
                ct = delta.get("content")
                u = j.get("usage")
                if isinstance(u, dict):
                    total_tokens = int(u.get("total_tokens") or total_tokens or 0)

                if rc:
                    if not _reason_opened:
                        emit("response.output_item.added", {
                            "type": "response.output_item.added", "output_index": 0,
                            "item": {"id": _reason_item, "type": "reasoning", "summary": []}})
                        _reason_opened = True
                    full_reason += rc
                    emit("response.reasoning_summary_text.delta", {
                        "type": "response.reasoning_summary_text.delta",
                        "item_id": _reason_item, "output_index": 0, "summary_index": 0,
                        "delta": rc})
                if ct:
                    if _reason_opened and not _msg_opened:
                        emit("response.reasoning_summary_text.done", {
                            "type": "response.reasoning_summary_text.done",
                            "item_id": _reason_item, "output_index": 0,
                            "summary_index": 0, "text": full_reason})
                        emit("response.output_item.done", {
                            "type": "response.output_item.done", "output_index": 0,
                            "item": {"id": _reason_item, "type": "reasoning",
                                     "summary": [{"type": "summary_text", "text": full_reason}]}})
                    if not _msg_opened:
                        emit("response.output_item.added", {
                            "type": "response.output_item.added", "output_index": 1,
                            "item": {"id": _msg_item, "type": "message", "role": "assistant",
                                     "content": []}})
                        emit("response.content_part.added", {
                            "type": "response.content_part.added", "item_id": _msg_item,
                            "output_index": 1, "content_index": 0,
                            "part": {"type": "output_text", "text": ""}})
                        _msg_opened = True
                        _content_opened = True
                    full_text += ct
                    emit("response.output_text.delta", {
                        "type": "response.output_text.delta", "item_id": _msg_item,
                        "output_index": 1, "content_index": 0, "delta": ct})

            # 收尾
            if _reason_opened and not _msg_opened:
                emit("response.reasoning_summary_text.done", {
                    "type": "response.reasoning_summary_text.done",
                    "item_id": _reason_item, "output_index": 0,
                    "summary_index": 0, "text": full_reason})
                emit("response.output_item.done", {
                    "type": "response.output_item.done", "output_index": 0,
                    "item": {"id": _reason_item, "type": "reasoning",
                             "summary": [{"type": "summary_text", "text": full_reason}]}})
            if _content_opened:
                emit("response.output_text.done", {
                    "type": "response.output_text.done", "item_id": _msg_item,
                    "output_index": 1, "content_index": 0, "text": full_text})
                emit("response.content_part.done", {
                    "type": "response.content_part.done", "item_id": _msg_item,
                    "output_index": 1, "content_index": 0,
                    "part": {"type": "output_text", "text": full_text}})
            if _msg_opened:
                emit("response.output_item.done", {
                    "type": "response.output_item.done", "output_index": 1,
                    "item": {"id": _msg_item, "type": "message", "role": "assistant",
                             "content": [{"type": "output_text", "text": full_text}]}})

            out_items = []
            if _reason_opened:
                out_items.append({"id": _reason_item, "type": "reasoning",
                                  "summary": [{"type": "summary_text", "text": full_reason}]})
            if _msg_opened:
                out_items.append({"id": _msg_item, "type": "message", "role": "assistant",
                                  "content": [{"type": "output_text", "text": full_text}]})
            _usage = {"input_tokens": 0, "output_tokens": len(full_text),
                      "total_tokens": total_tokens or len(full_text)}
            emit("response.completed", {"type": "response.completed",
                                        "response": _resp_obj("completed", out_items, _usage)})
        except _CLIENT_GONE:
            pass
        except Exception as e:
            log(f"[responses] 回传中断: {e}")
        finally:
            try:
                resp.close()
            except Exception:
                pass
            if acc is not None:
                acc["_inflight"] = max(0, acc.get("_inflight", 1) - 1)
            _dur = int((time.time() - start_t) * 1000)
            _ttft = int(_t_first) if _t_first else None
            append_log(model_name, acc["phone"], "200 OK", _dur, _ttft)
            record_model_stat(model_name, True, _dur, _ttft, total_tokens,
                              acc["phone"] if acc else None)


# ════════════════════════════════════════════════════════════════════════
# 【QM8】OpenAI Responses API ↔ Chat Completions 双向转换
#
# 背景（实测 2026-10-20）：
#   上游 /responses 只挂了混元后端 —— 全量实测 20 个模型里只有
#   hy3 / hy3-x 返回 200，其余一律 502 "invoke model error"。
#   而 /v2/chat/completions 支持全部 30+ 模型。
#
#   → 不能纯透传。必须在网关做协议转换：
#     客户端 POST /responses（Responses 格式）
#       → 转成 messages[] 发 /v2/chat/completions
#       → 把 chat SSE 转回 Responses SSE 事件序列
#
# 这样 MT-Agent 等 Responses 客户端就能用上全部模型（含 GPT/GLM/Kimi/DeepSeek）。
# ════════════════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════════════════
# 【QM9】客户端兼容层（对照参考项目 internal/upstream/payload.go + sanitize.go）
#
# 背景：各类客户端（Claude Code / Codex / Cursor / LobeChat …）发来的 body
#       字段形态各异，上游只认一种。参考项目用一组 normalize* 函数逐项归一。
#       我们此前只做了 normalizeRoles / ensureSystem / clampGPTMinMaxTokens 三项。
#
# 本组补齐四项（其余三项已有）：
#   N1 translate_max_completion_tokens  —— max_completion_tokens 别名 → max_tokens
#   N2 normalize_reasoning_effort       —— 思考档位按模型支持集降级
#   N3 normalize_image_url              —— 图片 image_url 字符串 → 对象
#   N4 sanitize_fingerprints            —— 客户端指纹串清洗（11128 触发源）
# ════════════════════════════════════════════════════════════════════════

# 思考档位排序（低 → 高），对齐参考项目 effortRank
EFFORT_RANK = {"off": 0, "minimal": 1, "low": 2, "medium": 3, "high": 4, "xhigh": 5, "max": 6}


def translate_max_completion_tokens(obj):
    """【N1】max_completion_tokens（OpenAI 新别名）→ max_tokens。

    规则（对齐参考项目）：
      · 别名无论能否翻译，一律从 body 删除（减少体积与排障噪音）
      · 显式 max_tokens 优先 —— 别名只删不译
      · 只翻译正整数值；0/null/负数/非数值不译（0/null 语义是「未设置」）
    """
    if "max_completion_tokens" not in obj:
        return False
    alias = obj.pop("max_completion_tokens")
    if "max_tokens" in obj:
        return False
    v = None
    if isinstance(alias, bool):
        return False
    if isinstance(alias, int):
        v = alias
    elif isinstance(alias, float):
        if alias == int(alias):
            v = int(alias)
    if v is not None and v > 0:
        obj["max_tokens"] = v
        return True
    return False


def normalize_reasoning_effort(obj, efforts_map=None):
    """【N2】按模型支持的档位集合降级 reasoning_effort。

    · 请求档位模型支持 → 原样透传
    · 请求档位不支持   → 改为 ≤请求档位的最高支持档
    · 支持档全高于请求 → 取最低支持档（偏离最小）
    · 未知模型/未知档位/未携带字段 → 一律透传

    efforts_map: {model: [支持的档位列表]}，为空则直接透传。
    """
    if not efforts_map:
        return False
    model = str(obj.get("model") or "")
    if not model:
        return False
    supported = efforts_map.get(model)
    if not supported:
        return False
    key = None
    if "reasoning_effort" in obj:
        key = "reasoning_effort"
    elif "reasoningEffort" in obj:
        key = "reasoningEffort"
    if not key:
        return False
    req = str(obj.get(key) or "").strip().lower()
    if req not in EFFORT_RANK:
        return False
    req_idx = EFFORT_RANK[req]
    best, best_idx = None, -1
    for s in supported:
        si = EFFORT_RANK.get(str(s).strip().lower())
        if si is not None and si <= req_idx and si > best_idx:
            best, best_idx = s, si
    if best is not None:
        if str(best).strip().lower() != req:
            obj[key] = best
            return True
        return False
    lowest, lowest_idx = None, 1 << 30
    for s in supported:
        si = EFFORT_RANK.get(str(s).strip().lower())
        if si is not None and si < lowest_idx:
            lowest, lowest_idx = s, si
    if lowest is not None:
        obj[key] = lowest
        return True
    return False


def normalize_image_url(obj):
    """【N3】messages[].content[].image_url 字符串形态 → {"url": "..."} 对象形态。

    上游只接受对象形态；部分客户端（以及 Responses→Chat 转换器）发字符串会 400 11101。
    空字符串 / 缺失值不补默认值，让上游返回真实错误。
    """
    msgs = obj.get("messages")
    if not isinstance(msgs, list):
        return False
    n = 0
    for msg in msgs:
        if not isinstance(msg, dict):
            continue
        parts = msg.get("content")
        if not isinstance(parts, list):
            continue
        for part in parts:
            if not isinstance(part, dict):
                continue
            if part.get("type") != "image_url":
                continue
            iu = part.get("image_url")
            if isinstance(iu, str) and iu:
                part["image_url"] = {"url": iu}
                n += 1
    return n > 0


def normalize_tool_choice(obj):
    """【N4】tool_choice 归一（上游该字段只认字符串）。

    · "none" → 删除 tool_choice，并连带删除 tools/functions（上游对 none 的处理是拒绝）
    · 对象 {type:"auto"|"required"} → 字符串
    · 对象 {type:"function", function:{name}} → 该函数名字符串
    · 其它形态 → 删除
    """
    def _suppress():
        obj.pop("tools", None)
        obj.pop("functions", None)

    if "tool_choice" not in obj:
        return False
    tc = obj.get("tool_choice")
    if isinstance(tc, str):
        if tc.strip().lower() == "none":
            obj.pop("tool_choice", None)
            _suppress()
            return True
        return False
    if isinstance(tc, dict):
        typ = str(tc.get("type") or "").strip().lower()
        if typ == "none":
            obj.pop("tool_choice", None)
            _suppress()
            return True
        if typ in ("auto", "required"):
            obj["tool_choice"] = typ
            return True
        if typ == "function":
            name = ""
            fn = tc.get("function")
            if isinstance(fn, dict):
                name = str(fn.get("name") or "")
            if not name:
                name = str(tc.get("name") or "")
            name = name.strip()
            obj["tool_choice"] = name if name else "auto"
            return True
        obj.pop("tool_choice", None)
        return True
    obj.pop("tool_choice", None)
    return True


# ── 【N4b】指纹清洗（对照参考项目 sanitize.go）─────────────────────────────
# 背景：Claude Code / Codex 等 CLI 会在 system prompt 注入固定模板句，
#       上游内容审核按「逐字精确匹配」误杀（400 code=11128）。
#       策略：整段剥离 / 最小改写（换一个词，语义不变）。
#
# ★ 11128 是上游反探测串：请求体里只要出现裸 "11128" 就整单拦截。
#   而 11128 正是本类拦截自身的错误码 —— 上游据此识别「在讨论其内部错误码」的请求。
#   改写为 "11-128"（保留可读性与指代）。
_SANITIZE_REWRITES = [
    ("You are Claude Code, Anthropic's official CLI for Claude",
     "You are Claude Code, Anthropic's official CLI tool for Claude"),
    ("Main branch (you will usually use this for PRs)",
     "Default branch (you will usually use this for PRs)"),
    ("You are a coding agent running in the Codex CLI, a terminal-based coding assistant.",
     "You are a coding agent running in the Codex CLI tool, a terminal-based coding assistant."),
    ("To give feedback, users should report the issue at https://github.com/anthropics/claude-code/issues",
     "To provide feedback, users should report the issue at https://github.com/anthropics/claude-code/issues"),
    ("11128", "11-128"),
]
# header 键值整段剥离（键名即触发，与值无关）
_SANITIZE_HDR_RE = re.compile(r"(?i)x-anthropic-billing-header:[^;\n]*;?\s*")
# 裸键名（无冒号）→ 最小缩写，破坏逐字匹配
_SANITIZE_BARE_RE = re.compile(r"(?i)x-anthropic-billing-header")
# 尾随裸键值 cc_xxx=...;
_SANITIZE_KV_RE = re.compile(r"(?i)\bcc_[a-z0-9_]+=[^;\n]*;?\s*")


def _sanitize_text(s):
    if not isinstance(s, str) or not s:
        return s, False
    changed = False
    for a, b in _SANITIZE_REWRITES:
        if a in s:
            s = s.replace(a, b)
            changed = True
    m = _SANITIZE_HDR_RE.search(s)
    if m:
        s = _SANITIZE_HDR_RE.sub("", s)
        changed = True
    if _SANITIZE_BARE_RE.search(s):
        s = _SANITIZE_BARE_RE.sub("hdr", s)
        changed = True
    if _SANITIZE_KV_RE.search(s):
        s = _SANITIZE_KV_RE.sub("", s)
        changed = True
    return s, changed


def sanitize_fingerprints(obj):
    """【N4b】清洗请求体里的客户端指纹串。返回是否发生改写。"""
    hit = False
    try:
        msgs = obj.get("messages")
        if isinstance(msgs, list):
            for m in msgs:
                if not isinstance(m, dict):
                    continue
                c = m.get("content")
                if isinstance(c, str):
                    nc, ch = _sanitize_text(c)
                    if ch:
                        m["content"] = nc
                        hit = True
                elif isinstance(c, list):
                    for p in c:
                        if not isinstance(p, dict):
                            continue
                        if isinstance(p.get("text"), str):
                            nt, ch = _sanitize_text(p["text"])
                            if ch:
                                p["text"] = nt
                                hit = True
        # tools 描述里也可能带指纹
        tools = obj.get("tools")
        if isinstance(tools, list):
            for t in tools:
                if not isinstance(t, dict):
                    continue
                fn = t.get("function") if isinstance(t.get("function"), dict) else t
                if isinstance(fn.get("description"), str):
                    nd, ch = _sanitize_text(fn["description"])
                    if ch:
                        fn["description"] = nd
                        hit = True
    except Exception:
        pass
    return hit


_EFFORTS_CACHE = {}      # {model: [档位…]}
_EFFORTS_TTL = 1800
_EFFORTS_TS = {"t": 0.0}


def _model_efforts():
    """【QM9】模型 → 支持的 reasoning 档位表（供 normalize_reasoning_effort 降级）。

    上游 /v3/config 的模型条目里带 supportedEfforts / defaultEffort。
    缓存 30 分钟；拉取失败返回空 dict（= 不做降级，原样透传，安全默认）。
    """
    now = time.time()
    if _EFFORTS_CACHE and (now - _EFFORTS_TS["t"]) < _EFFORTS_TTL:
        return _EFFORTS_CACHE
    try:
        acc = _pick_models_account()
        if acc is None:
            return _EFFORTS_CACHE
        _base = chat_base(acc)
        _host = _base.split("//", 1)[1]
        j = _fetch_model_route(_base + "/v3/config",
                               "CodeBuddyIDE/4.12.0 CodeBuddy/4.12.0",
                               acc["access_token"],
                               {"Accept": "application/json, text/plain, */*",
                                "X-Domain": _host, "X-Product": "SaaS"})
        out = {}
        for m in (((j or {}).get("data") or {}).get("models") or []):
            mid = m.get("id")
            eff = m.get("supportedEfforts") or m.get("supported_efforts")
            if mid and isinstance(eff, list) and eff:
                out[mid] = [str(x) for x in eff]
        if out:
            _EFFORTS_CACHE.clear()
            _EFFORTS_CACHE.update(out)
            _EFFORTS_TS["t"] = now
        return _EFFORTS_CACHE
    except Exception as e:
        log(f"[compat] efforts 拉取失败（不降级）: {e}")
        return _EFFORTS_CACHE


def apply_client_compat(obj, efforts_map=None, sanitize=True):
    """一次性应用全部客户端兼容层（幂等，可重复调用）。

    顺序参照参考项目 PrepareBodyOpt：
      1. translate_max_completion_tokens（先翻译别名）
      2. normalize_roles（developer → system）
      3. normalize_tool_choice（对象 → 字符串）
      4. normalize_image_url（字符串 → 对象）
      5. normalize_reasoning_effort（档位降级）
      6. sanitize_fingerprints（指纹清洗，可关）
    返回改写说明列表。
    """
    marks = []
    try:
        if translate_max_completion_tokens(obj):
            marks.append("max_completion_tokens→max_tokens")
    except Exception:
        pass
    try:
        if normalize_roles(obj):
            marks.append("developer→system")
    except Exception:
        pass
    try:
        if normalize_tool_choice(obj):
            marks.append("tool_choice 归一")
    except Exception:
        pass
    try:
        if normalize_image_url(obj):
            marks.append("image_url→对象")
    except Exception:
        pass
    try:
        if normalize_reasoning_effort(obj, efforts_map):
            marks.append("reasoning_effort 降级")
    except Exception:
        pass
    if sanitize:
        try:
            if sanitize_fingerprints(obj):
                marks.append("指纹清洗")
        except Exception:
            pass
    return marks


def resp_to_chat(req):
    """Responses 请求体 → Chat Completions 请求体。返回 (chat_body, 说明列表)。"""
    notes = []
    out = {"stream": True}
    model = req.get("model") or "hy3"
    out["model"] = model

    msgs = []
    # ① instructions（Responses 的系统提示）→ system 消息
    ins = req.get("instructions")
    if isinstance(ins, str) and ins.strip():
        msgs.append({"role": "system", "content": ins})
        notes.append("instructions→system")

    # ② input → messages
    inp = req.get("input")
    if isinstance(inp, str):
        msgs.append({"role": "user", "content": inp})
    elif isinstance(inp, list):
        for it in inp:
            if not isinstance(it, dict):
                continue
            typ = (it.get("type") or "message").lower()
            if typ != "message":
                # function_call / function_call_output 等，尽力映射
                if typ == "function_call_output":
                    msgs.append({"role": "tool",
                                 "tool_call_id": it.get("call_id") or it.get("id") or "",
                                 "content": str(it.get("output") or "")})
                    notes.append("function_call_output→tool")
                continue
            role = (it.get("role") or "user").lower()
            if role == "developer":
                role = "system"
                notes.append("developer→system")
            content = it.get("content")
            if isinstance(content, str):
                msgs.append({"role": role, "content": content})
            elif isinstance(content, list):
                parts = []
                for p in content:
                    if not isinstance(p, dict):
                        continue
                    pt = (p.get("type") or "").lower()
                    if pt in ("input_text", "text", "output_text"):
                        parts.append({"type": "text", "text": p.get("text") or ""})
                    elif pt in ("input_image", "image_url"):
                        url = p.get("image_url")
                        if isinstance(url, dict):
                            url = url.get("url")
                        parts.append({"type": "image_url",
                                      "image_url": {"url": url or ""}})
                        notes.append("image→image_url")
                if parts:
                    if all(x.get("type") == "text" for x in parts):
                        msgs.append({"role": role,
                                     "content": "".join(x["text"] for x in parts)})
                    else:
                        msgs.append({"role": role, "content": parts})
                else:
                    msgs.append({"role": role, "content": ""})
    if not msgs:
        msgs = [{"role": "user", "content": "hi"}]
        notes.append("空 input→占位")
    # 上游要求首条必须是 system（国际站硬约束）
    if msgs[0].get("role") != "system":
        msgs.insert(0, {"role": "system",
                        "content": "You are a helpful assistant. Respond in the user's language."})
        notes.append("补 system")
    out["messages"] = msgs

    # ③ 参数映射
    if req.get("max_output_tokens") is not None:
        out["max_tokens"] = int(req["max_output_tokens"])
        notes.append("max_output_tokens→max_tokens")
    if req.get("temperature") is not None:
        out["temperature"] = req["temperature"]
    if req.get("top_p") is not None:
        out["top_p"] = req["top_p"]
    # reasoning.effort → reasoning_effort
    rsn = req.get("reasoning")
    if isinstance(rsn, dict) and rsn.get("effort"):
        out["reasoning_effort"] = rsn["effort"]
        notes.append("reasoning.effort→reasoning_effort")
    # tools: Responses 扁平函数 → Chat 嵌套
    tools = req.get("tools")
    if isinstance(tools, list) and tools:
        ct = []
        for t in tools:
            if not isinstance(t, dict):
                continue
            if (t.get("type") or "") == "function":
                if "function" in t:            # 已是 chat 形态
                    ct.append(t)
                else:                          # Responses 扁平形态
                    ct.append({"type": "function", "function": {
                        "name": t.get("name") or "",
                        "description": t.get("description") or "",
                        "parameters": t.get("parameters") or {"type": "object", "properties": {}},
                    }})
        if ct:
            out["tools"] = ct
            notes.append("tools→chat 形态")
    # tool_choice: 上游只认字符串
    tc = req.get("tool_choice")
    if tc is not None:
        if isinstance(tc, dict):
            name = tc.get("name") or (tc.get("function") or {}).get("name")
            out["tool_choice"] = "auto" if not name else "auto"
            notes.append("tool_choice 对象→auto")
        else:
            out["tool_choice"] = tc
    # 不支持的字段直接丢弃
    for k in ("input", "instructions", "reasoning", "max_output_tokens", "truncation", "store"):
        out.pop(k, None)
    return out, notes


def sse(event, data):
    """构造一条 SSE 事件（event + data 两行）。"""
    return ("event: %s\ndata: %s\n\n" % (event, json.dumps(data, ensure_ascii=False))).encode("utf-8")


def main():
    load_settings()                                            # 【BP2】加载运行期设置
    load_counters()                                            # 【A 组】先恢复累计计数
    load_reqlogs()                                             # 【V 组】恢复请求记录
    load_timeouts()                                            # 【W3】恢复超时计数
    load_model_stats()                                         # 【Y1】恢复模型统计
    load_blocked()                                             # 【B4】加载模型黑名单
    load_probes()                                              # 【B5】加载模型探测
    load_accounts()
    # 【QM2 · A4 补】启动时后台预热模型域映射（否则重启后映射空 → 国际模型被派给 CN 号 → 400）
    def _warm_domains():
        try:
            time.sleep(3)
            fetch_live_models(force=True)
            log(f"[QM2] 启动预热完成：模型域映射 {len(_model_domains)} 条")
        except Exception as e:
            log(f"[QM2] 启动预热失败（首次请求时会再试）: {e}")
    threading.Thread(target=_warm_domains, daemon=True).start()
    threading.Thread(target=_counters_flusher, daemon=True).start()   # 【A 组】后台 30s 刷盘
    threading.Thread(target=_auto_enable_loop, daemon=True).start()   # 【R 组】自动禁用到期巡检
    log("正在初次同步账号真实数据...")
    for acc in account_pool:
        if not acc["access_token"]:
            refresh_account_token(acc)
        else:
            update_account_official_resources(acc)
            
    log(f"🚀 WorkBuddy AI 网关已启动！")
    log(f"👉 本地控制台地址: http://127.0.0.1:{PORT}")
    
    # 【H6 修复】单线程 → 多线程
    # 真因: HTTPServer 是单线程串行模型, 一个流式对话会独占服务最长 60 秒
    #       (urlopen timeout=60), 期间面板轮询/健康检查全被堵死。
    #       实测: 对话进行中 /health 从 0.002s 飙到 106~120s(超时) —— 面板卡、
    #             反代慢的本质是"所有请求排一条队"。
    #       修正: ThreadingHTTPServer 为每个请求开独立线程, 请求间不再互相阻塞。
    #       注: 历史"禁用 threading"铁律针对 iSH 沙盒(内存受限易 MemoryError),
    #           本服务运行于越狱 LaunchDaemon(标准 Unix 语义, 6 核, 负载 ~2.3),
    #           不适用该限制; 已实测 ThreadingHTTPServer 可正常导入。
    class _WBThreadingServer(ThreadingHTTPServer):
        daemon_threads = True        # 主线程退出时子线程自动结束, 不残留
        allow_reuse_address = True   # 重启时立即复用端口, 免 TIME_WAIT 等待

    server = _WBThreadingServer(("0.0.0.0", PORT), WorkBuddyHandler)
    log("🔧 并发模型: ThreadingHTTPServer (每请求独立线程)")
    try:
        server.serve_forever()
    finally:
        flush_counters(force=True)                             # 【A 组】退出前强制落盘
        flush_reqlogs(force=True)                              # 【V 组】请求记录一并落盘
        flush_model_stats(force=True)                          # 【Y1】模型统计一并落盘
        log("累计计数与请求记录已落盘")

if __name__ == "__main__":
    main()
