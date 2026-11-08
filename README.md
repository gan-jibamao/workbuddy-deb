# WorkBuddy 反代管家（1.5.7）

修复 1.5.6 的致命缺陷：**面板导入新 JSON 会把之前的账号覆盖掉，导致不能多账号登录。**

包名 `com.jkgl.workbuddy`，架构 `iphoneos-arm64e`，兼容 Dopamine rootless 与 RootHide。
安装后访问 `http://127.0.0.1:8318/`（或 `/panel/`）打开管理面板。

---

## 故障现象

- 「凭据导入」粘贴新账号 JSON 后，之前导入的账号消失，账号池里始终只有 1 条。
- 账号启停列表显示 `#1 aut****auth`（既不是手机号也不是 uid 的噪声串）。
- 换号重导看似成功，提示「已导入 0 个新账号」，但磁盘上旧账号已被覆盖。
- 服务重启后账号全部 401，需要重新导入。

## 根本原因（逐条实测）

### 1. `import_tokens` 拿 JSON 顶层键当手机号

1.5.6 源码（`wb_server.py`）：

```python
for ph, item in newd.items():
    ...
    cur[ph] = item          # ← ph 是 JSON 顶层键
```

面板给的是嵌套格式：

```json
{ "account": {"uid": "...", "nickname": "..."},
  "auth": {"refreshToken": "...", "accessToken": "...", "expiresAt": 1795097981} }
```

顶层键只有 `account` / `auth` 两个。`account` 没有 refresh_token 被跳过，
`auth` 有 —— 于是**所有账号都被写进 `cur["auth"]` 这一个键**，
导入第 2 个账号时直接覆盖第 1 个。复现：

```
导入A后 顶层键: ['auth'] 新增: 1
导入B后 顶层键: ['auth'] 新增: 0
磁盘内容条数: 1 → RT 现在是: RT_B
```

`aut****auth` 就是 `"auth"` 这个 4 字符键被面板内联的 `[:3]+"****"+[-4:]` 截出来的结果。

### 2. `load_accounts` 只读蛇形字段，驼峰读不到

```python
"access_token": item.get("access_token", ""),
"token_expiry": item.get("token_expiry", 0),
```

导入的是驼峰 `accessToken` / `expiresAt`，读出来 AT 恒空、expiry 恒 0
→ 选号逻辑 `if not acc["access_token"] or time.time() > acc["token_expiry"]` **恒真**
→ 每个请求都先换票，放大 RT 单次轮换损耗。

### 3. 换票拿到新 RT 只改内存，从不落盘

```python
if new_rt:
    acc["refresh_token"] = new_rt     # 只改内存
_persist_acc_realm(acc)               # 只写 realm/domain
```

上游 RT 是**单次轮换**的，第一次换票后旧 RT 立即作废，而磁盘上仍是旧 RT
→ 服务一重启，所有账号 401。

### 4. 无锁并发写盘

`ThreadingHTTPServer` 每请求一线程，导入、换票落盘、删除各自「读-改-写」，
后写者覆盖先写者 → 又一处静默丢号。

---

## 1.5.7 修复内容

| 编号 | 修复 |
|------|------|
| A1 | 新增 `_normalize_import_payload`：账号键从**对象内部**取 uid/手机号，支持 7 种输入形态；合并而非覆盖 |
| A2 | 新增 `_persist_account_tokens`：换票后 RT/AT/expiry/realm/domain **全量落盘** |
| A3 | `load_accounts` 兼容驼峰 `accessToken` / `expiresAt`，时间统一归一到秒级 epoch |
| A4 | 新增 `_tokens_mutate`：整段「读 → 改 → 写」在同一把 `_TOKENS_LOCK` 里执行 |
| B1 | 前端预检账号数 + 改正误导文案（旧文案暗示只有同号才覆盖） |
| B2 | 统一 `_mask` 分档脱敏，短键不再拼出 `aut****auth`；`_match_acc` 同步走 `_mask` |
| C1 | 版本号 1.5.6 → 1.5.7 |

### 支持的导入格式

```jsonc
// 1. 原生字典（向后兼容，行为不变）
{"13018201883": {"refresh_token": "..."}}

// 2. 嵌套单对象（最常见的面板导出格式）
{"account": {"uid": "...", "nickname": "..."},
 "auth": {"refreshToken": "...", "accessToken": "...", "expiresAt": 1795097981}}

// 3. 嵌套数组
{"account": {"uid": "..."}, "auth": [{"refreshToken": "..."}]}

// 4. 扁平蛇形
{"uid": "...", "refresh_token": "...", "expires_at": 1795097981}

// 5. 扁平驼峰
{"uid": "...", "refreshToken": "...", "expiresAt": 1795097981}

// 6. 裸数组（多账号批量）
[{"uid": "A", "refreshToken": "..."}, {"uid": "B", "refreshToken": "..."}]

// 7. 信封
{"accounts": [{"uid": "A", "refreshToken": "..."}, ...]}
```

账号键优先级：`uid / userId / uin / accountId` → `phone / mobile` → `username / nickname`。
**绝不用外层字典键**（容器名 `auth` / `account` 一律排除）。

---

## 构建

GitHub Actions 自动跑测试 + 打包，产物在 Actions 页的 `workbuddy-deb` artifact，
打 tag 时附到 Release。

本地：

```sh
python3 tests/test_normalize.py   # 归一化单测
python3 tests/test_backend.py     # 后端集成验收（真起服务）
bash build_deb.sh                 # 出 dist/com.jkgl.workbuddy_1.5.7_iphoneos-arm64e.deb
```

## 升级

```sh
dpkg -i com.jkgl.workbuddy_1.5.7_iphoneos-arm64e.deb
```

`wb_tokens.json` 不在包内，升级不会覆盖已有凭据。

**注意**：1.5.6 留下的 `"auth"` 垃圾记录不会被自动清理（避免误删），
可在面板「账号启停」里手动删除。
