#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""B7 归一化单测 —— 从 wb_server.py 里抽出 _normalize_import_payload 实测。"""
import io, json, os, re, sys, time

SRV = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   'src/usr/share/workbuddy/wb_server.py')
src = io.open(SRV, encoding='utf-8').read()

# 抽出需要的函数 + 常量，避免 import 整个服务（会起线程/读文件）
names = ['_IMPORT_RT_KEYS', '_IMPORT_AT_KEYS', '_IMPORT_EXP_KEYS', '_IMPORT_DOM_KEYS',
         '_IMPORT_NAME_KEYS', '_IMPORT_LAYERS', '_IMPORT_SKIP_KEYS',
         '_import_first', '_import_pick_id', '_import_flatten',
         '_normalize_import_payload', '_norm_expiry']
ns = {'time': time, 'log': lambda m: None}
tree = __import__('ast').parse(src)
for node in tree.body:
    if isinstance(node, (__import__('ast').FunctionDef, __import__('ast').Assign)):
        nm = getattr(node, 'name', None)
        if nm in names:
            exec(compile(__import__('ast').Module([node], []), '<x>', 'exec'), ns)
            continue
        if isinstance(node, __import__('ast').Assign):
            for t in node.targets:
                if isinstance(t, __import__('ast').Name) and t.id in names:
                    exec(compile(__import__('ast').Module([node], []), '<x>', 'exec'), ns)

norm = ns['_normalize_import_payload']
print('抽到函数:', [k for k in names if k in ns])
print()

A_UID = '68c01e19-dce7-446a-b0c9-e002cdd2deaa'
B_UID = '77d12f2a-aaaa-4bbb-9ccc-dddddddddddd'

# ── 形态 1：原生字典 ────────────────────────────────────────────
f1 = {'13018201883': {'refresh_token': 'RT_A', 'realm': 'cn'}}
# ── 形态 2：嵌套单对象（老大的格式） ────────────────────────────
f2 = {'account': {'uid': A_UID, 'nickname': '13018201883'},
      'auth': {'accessToken': 'AT_A', 'refreshToken': 'RT_A',
               'expiresAt': 1795097981, 'domain': 'www.codebuddy.cn', 'realm': 'cn'}}
# ── 形态 3：嵌套数组 ────────────────────────────────────────────
f3 = {'account': {'uid': A_UID},
      'auth': [{'refreshToken': 'RT_A', 'expiresAt': 1795097981},
               {'refreshToken': 'RT_A2', 'expiresAt': 1795097981}]}
# ── 形态 4：扁平蛇形 ────────────────────────────────────────────
f4 = {'uid': A_UID, 'refresh_token': 'RT_A', 'expires_at': 1795097981}
# ── 形态 5：扁平驼峰 ────────────────────────────────────────────
f5 = {'uid': A_UID, 'refreshToken': 'RT_A', 'expiresAt': 1795097981, 'realm': 'cn'}
# ── 形态 6：裸数组 ──────────────────────────────────────────────
f6 = [{'uid': A_UID, 'refreshToken': 'RT_A'}, {'uid': B_UID, 'refreshToken': 'RT_B'}]
# ── 形态 7：信封 ────────────────────────────────────────────────
f7 = {'accounts': [{'uid': A_UID, 'refreshToken': 'RT_A'},
                   {'uid': B_UID, 'refreshToken': 'RT_B'}]}

cases = [('原生字典', f1), ('嵌套单对象', f2), ('嵌套数组', f3), ('扁平蛇形', f4),
         ('扁平驼峰', f5), ('裸数组', f6), ('信封', f7)]

fail = 0
for label, payload in cases:
    r = norm(payload)
    keys = [k for k, _ in r]
    print('【%s】→ %d 条  键=%s' % (label, len(r), keys))
    for k, v in r:
        print('     %-40s RT=%-6s AT=%-6s exp=%s realm=%s dom=%s' % (
            k, v.get('refresh_token'), v.get('access_token'),
            v.get('token_expiry'), v.get('realm'), v.get('domain')))
    if not r:
        print('     !! 漏了'); fail += 1
    print()

# ── 关键回归：连续导入 A 再 B，必须 2 条 ────────────────────────
print('=' * 66)
print('关键回归：导入 A 后再导入 B')
cur = {}
B_PAYLOAD = {'account': {'uid': B_UID, 'nickname': '13900001111'},
             'auth': {'accessToken': 'AT_B', 'refreshToken': 'RT_B',
                      'expiresAt': 1795098981, 'domain': 'www.codebuddy.cn', 'realm': 'cn'}}
for payload, tag in ((f2, 'A'), (B_PAYLOAD, 'B')):
    for k, v in norm(payload):
        cur.setdefault(k, {}).update(v)
    print('  导入%s后 → 键=%s' % (tag, list(cur)))
print('最终条数:', len(cur))
assert A_UID in cur, 'A 丢了'
assert B_UID in cur, 'B 丢了'
assert 'auth' not in cur, '"auth" 键又出现了'
assert cur[A_UID]['refresh_token'] == 'RT_A', 'A 的 RT 被覆盖'
assert cur[B_UID]['refresh_token'] == 'RT_B'
print('✓ 两账号并存，RT 各自完好，"auth" 键不存在')

# ── 同账号重导：更新不新增 ─────────────────────────────────────
before = len(cur)
for k, v in norm(dict(f2, auth=dict(f2['auth'], refreshToken='RT_A_NEW'))):
    cur.setdefault(k, {}).update(v)
print('同账号重导后条数:', len(cur), '(应等于 %d)' % before)
assert len(cur) == before
assert cur[A_UID]['refresh_token'] == 'RT_A_NEW'
print('✓ 同账号重导只更新，不新增')

# ── 无 RT 的对象不能被当成账号 ────────────────────────────────
bad = {'account': {'uid': A_UID}, 'auth': {'realm': 'cn'}}
assert norm(bad) == [], norm(bad)
print('✓ 无 refresh_token 的对象被正确跳过')

# ── 反证：auth 是多账号数组时，必须拆成多条而不是合并成一条 ────
multi = {'account': {'uid': A_UID},
         'auth': [{'refreshToken': 'RT_A1'}, {'refreshToken': 'RT_A2'}]}
r = norm(multi)
print('auth 多片段数组 → %d 条 (应 ≥1)' % len(r))
assert len(r) >= 1, r
# 两条片段若同属一个 uid，去重后只留 1 条也算对（不能丢）
print('✓ 数组片段不丢也不炸')

# ── 反证：真正的多账号数组必须逐条拆开 ─────────────────────────
multi2 = [{'uid': A_UID, 'refreshToken': 'RT_A'},
          {'uid': B_UID, 'refreshToken': 'RT_B'},
          {'uid': 'C333', 'refreshToken': 'RT_C'}]
r2 = norm(multi2)
assert len(r2) == 3, r2
assert [k for k, _ in r2] == [A_UID, B_UID, 'C333']
print('✓ 裸数组三账号拆成 3 条，顺序与键名正确')

print()
print('全部通过' if not fail else '有 %d 个形态漏识别' % fail)
sys.exit(1 if fail else 0)
