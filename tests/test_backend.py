#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""B7 后端集成验收 —— 把 wb_server.py 的 import 段、换票落盘、load_accounts 串起来跑。
假上游 + 真 HTTP 面板调用，不碰真网络。"""
import io, json, os, shutil, subprocess, sys, tempfile, threading, time, urllib.request
import ast

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRV = os.path.join(ROOT, 'src/usr/share/workbuddy/wb_server.py')

# ── 把 wb_server.py 拷到临时目录跑，BASE_DIR 落在那儿 ──
WORK = tempfile.mkdtemp(prefix='wbtest_')
shutil.copy(SRV, os.path.join(WORK, 'wb_server.py'))
shutil.copy(os.path.join(ROOT, 'src/usr/share/workbuddy/index.html'),
            os.path.join(WORK, 'index.html'))
os.makedirs(os.path.join(WORK, 'panel'), exist_ok=True)
shutil.copy(os.path.join(ROOT, 'src/usr/share/workbuddy/panel/index.html'),
            os.path.join(WORK, 'panel/index.html'))
os.makedirs(os.path.join(WORK, 'reqhistory'), exist_ok=True)

PORT = 18318
env = dict(os.environ, WB_PORT=str(PORT))
proc = subprocess.Popen([sys.executable, os.path.join(WORK, 'wb_server.py')],
                        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

LOGS = []


def pump():
    for line in proc.stdout:
        LOGS.append(line.decode('utf-8', 'replace').rstrip())


threading.Thread(target=pump, daemon=True).start()


def call(action, **extra):
    body = dict(extra, action=action)
    rq = urllib.request.Request('http://127.0.0.1:%d/api/control' % PORT,
                                data=json.dumps(body).encode(),
                                headers={'Content-Type': 'application/json'},
                                method='POST')
    with urllib.request.urlopen(rq, timeout=15) as r:
        return json.loads(r.read().decode())


def mon():
    with urllib.request.urlopen('http://127.0.0.1:%d/api/monitor' % PORT, timeout=10) as r:
        d = json.loads(r.read().decode())
    return d['data']


def tokens_on_disk():
    p = os.path.join(WORK, 'wb_tokens.json')
    if not os.path.exists(p):
        return {}
    with open(p, encoding='utf-8') as f:
        return json.load(f)


A_UID = '68c01e19-dce7-446a-b0c9-e002cdd2deaa'
B_UID = '77d12f2a-aaaa-4bbb-9ccc-dddddddddddd'

F_A = {'account': {'uid': A_UID, 'nickname': '13018201883'},
       'auth': {'accessToken': 'AT_A', 'refreshToken': 'RT_A',
                'expiresAt': 1795097981, 'domain': 'www.codebuddy.cn', 'realm': 'cn'}}
F_B = {'account': {'uid': B_UID, 'nickname': '13900001111'},
       'auth': {'accessToken': 'AT_B', 'refreshToken': 'RT_B',
                'expiresAt': 1795098981, 'domain': 'www.codebuddy.cn', 'realm': 'cn'}}

fails = []


def chk(name, cond, detail=''):
    print(('  ✓ ' if cond else '  ✗ ') + name + (('  → ' + detail) if detail else ''))
    if not cond:
        fails.append(name)


try:
    # 等服务起来
    for _ in range(60):
        try:
            with urllib.request.urlopen('http://127.0.0.1:%d/health' % PORT, timeout=1):
                break
        except Exception:
            time.sleep(0.25)
    else:
        raise RuntimeError('服务没起来: ' + '\n'.join(LOGS[-40:]))

    print('\n【1】导入 A（嵌套单对象）')
    r = call('import_tokens', tokens=json.dumps(F_A, ensure_ascii=False))
    d = r.get('data') or {}
    print('  返回:', d.get('msg'))
    chk('ok=True', d.get('ok') is True)
    chk('新增 1 个', '新增 1' in (d.get('msg') or ''), d.get('msg'))

    disk = tokens_on_disk()
    chk('磁盘键是 uid 而非 auth', A_UID in disk and 'auth' not in disk, str(list(disk)))
    chk('AT 被映射成蛇形', disk.get(A_UID, {}).get('access_token') == 'AT_A')
    chk('expiry 是秒级', disk.get(A_UID, {}).get('token_expiry') == 1795097981,
        str(disk.get(A_UID, {}).get('token_expiry')))

    print('\n【2】再导入 B（不同账号）—— 1.5.6 在这里把 A 覆盖掉')
    r = call('import_tokens', tokens=json.dumps(F_B, ensure_ascii=False))
    d = r.get('data') or {}
    print('  返回:', d.get('msg'))
    chk('新增 1 个', '新增 1' in (d.get('msg') or ''), d.get('msg'))

    disk = tokens_on_disk()
    chk('磁盘 2 条', len(disk) == 2, str(list(disk)))
    chk('A 的 RT 完好', disk.get(A_UID, {}).get('refresh_token') == 'RT_A')
    chk('B 的 RT 是 RT_B', disk.get(B_UID, {}).get('refresh_token') == 'RT_B')

    m = mon()
    accs = m.get('accounts') or []
    print('  面板账号行:', [a.get('phone') for a in accs])
    chk('内存池 2 个', len(accs) == 2, str(len(accs)))
    chk('不再出现 aut****auth',
        all('aut****' not in (a.get('phone') or '') for a in accs),
        str([a.get('phone') for a in accs]))
    chk('AT 已读进内存（无需换票）',
        all(a.get('ttl_min', 0) > 0 for a in accs),
        str([(a.get('phone'), a.get('ttl_min')) for a in accs]))

    print('\n【3】同账号重导（换 RT）—— 应更新不新增')
    F_A2 = dict(F_A)
    F_A2['auth'] = dict(F_A['auth'], refreshToken='RT_A_NEW')
    r = call('import_tokens', tokens=json.dumps(F_A2, ensure_ascii=False))
    d = r.get('data') or {}
    print('  返回:', d.get('msg'))
    chk('更新 1 个', '更新 1' in (d.get('msg') or ''), d.get('msg'))
    disk = tokens_on_disk()
    chk('仍 2 条', len(disk) == 2, str(list(disk)))
    chk('A 的 RT 已更新', disk.get(A_UID, {}).get('refresh_token') == 'RT_A_NEW')

    print('\n【4】批量导入：裸数组 5 个账号')
    bulk = [{'uid': 'bulk-%d' % i, 'refreshToken': 'RT_%d' % i,
             'expiresAt': 1795097981 + i} for i in range(5)]
    r = call('import_tokens', tokens=json.dumps(bulk, ensure_ascii=False))
    d = r.get('data') or {}
    print('  返回:', d.get('msg'))
    chk('新增 5 个', '新增 5' in (d.get('msg') or ''), d.get('msg'))
    disk = tokens_on_disk()
    chk('磁盘 7 条', len(disk) == 7, str(len(disk)))

    print('\n【5】原生格式零回归：{手机号: {refresh_token}}')
    native = {'13800138000': {'refresh_token': 'RT_NATIVE', 'realm': 'cn'}}
    r = call('import_tokens', tokens=json.dumps(native, ensure_ascii=False))
    d = r.get('data') or {}
    chk('新增 1 个', '新增 1' in (d.get('msg') or ''), d.get('msg'))
    disk = tokens_on_disk()
    chk('键仍是手机号', '13800138000' in disk, str(list(disk)))
    chk('手机号脱敏正确',
        any(a.get('phone') == '138****8000' for a in mon()['accounts']),
        str([a.get('phone') for a in mon()['accounts']]))

    print('\n【6】无效输入不炸')
    r = call('import_tokens', tokens='{"foo": "bar"}')
    d = r.get('data') or {}
    chk('无 RT 提示', '未识别' in (d.get('msg') or ''), d.get('msg'))
    r = call('import_tokens', tokens='not json')
    d = r.get('data') or {}
    chk('坏 JSON 提示', '解析失败' in (d.get('msg') or ''), d.get('msg'))
    r = call('import_tokens', tokens='{"account":{"uid":"x"},"auth":{"realm":"cn"}}')
    d = r.get('data') or {}
    chk('无 RT 嵌套不写入', '未识别' in (d.get('msg') or ''), d.get('msg'))

    print('\n【7】并发导入 8 个账号 —— 验证 _TOKENS_LOCK 防丢号')
    results = [None] * 8

    def do_import(i):
        p = {'uid': 'conc-%d' % i, 'refreshToken': 'RT_CONC_%d' % i}
        try:
            r = call('import_tokens', tokens=json.dumps(p))
            results[i] = (r.get('data') or {}).get('ok')
        except Exception as e:
            results[i] = str(e)

    ts = [threading.Thread(target=do_import, args=(i,)) for i in range(8)]
    for t in ts: t.start()
    for t in ts: t.join(timeout=30)
    disk = tokens_on_disk()
    conc = [k for k in disk if str(k).startswith('conc-')]
    chk('8 个并发账号全部落盘', len(conc) == 8, '实际 %d 个: %s' % (len(conc), sorted(conc)))
    chk('并发请求全部成功', all(r is True for r in results), str(results))

    print('\n【8】删除账号仍能正常工作')
    r = call('account_delete', phone='conc-3')
    d = r.get('data') or {}
    print('  返回:', d.get('msg'))
    chk('删除成功', d.get('ok') is True, d.get('msg'))
    disk = tokens_on_disk()
    chk('conc-3 已移除', 'conc-3' not in disk)
    chk('其它并发账号未受影响', sum(1 for k in disk if str(k).startswith('conc-')) == 7)

    print('\n【9】RT 轮换落盘（直接调 _persist_account_tokens）')
    # 用 py 直接加载服务模块的该函数 + 临时 TOKENS_FILE，验证写回
    src = io.open(SRV, encoding='utf-8').read()
    ns = {'time': time, 'os': os, 'json': json, 'threading': threading,
          'TOKENS_FILE': os.path.join(WORK, 'wb_tokens.json'),
          'log': lambda m: None, 'threading': threading}
    tree = ast.parse(src)
    want = {'_TOKENS_LOCK', '_tokens_load', '_tokens_save', '_tokens_mutate',
            '_persist_account_tokens', '_norm_expiry'}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in want:
            exec(compile(ast.Module([node], []), '<x>', 'exec'), ns)
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id in want:
                    exec(compile(ast.Module([node], []), '<x>', 'exec'), ns)
    ns['_persist_account_tokens']({'phone': A_UID, 'refresh_token': 'RT_ROTATED_1',
                                   'access_token': 'AT_ROT_1', 'token_expiry': 1890000000})
    disk = tokens_on_disk()
    chk('新 RT 已落盘', disk.get(A_UID, {}).get('refresh_token') == 'RT_ROTATED_1')
    chk('新 AT 已落盘', disk.get(A_UID, {}).get('access_token') == 'AT_ROT_1')
    ns['_persist_account_tokens']({'phone': A_UID, 'refresh_token': 'RT_ROTATED_2',
                                   'access_token': 'AT_ROT_2', 'token_expiry': 1890000600})
    disk = tokens_on_disk()
    chk('第二次轮换也落盘', disk.get(A_UID, {}).get('refresh_token') == 'RT_ROTATED_2')
    chk('domain 没丢', disk.get(A_UID, {}).get('domain') == 'www.codebuddy.cn')
    chk('realm 没丢', disk.get(A_UID, {}).get('realm') == 'cn')

    print('\n【10】脱敏函数分档')
    m = ns
    # 抽 _mask
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == '_mask':
            exec(compile(ast.Module([node], []), '<x>', 'exec'), ns)
    mask = ns['_mask']
    samples = [('13800138000', '138****8000'), ('auth', 'au***h'),
               (A_UID, A_UID[:8] + '…' + A_UID[-4:]), ('', '—'), ('abc', 'abc')]
    for inp, exp in samples:
        got = mask(inp)
        chk('_mask(%r)=%r' % (inp, exp), got == exp, 'got %r' % got)

finally:
    try:
        proc.terminate()
        proc.wait(timeout=8)
    except Exception:
        proc.kill()

print('\n' + '=' * 60)
print('失败 %d 项' % len(fails) if fails else '全部通过')
if fails:
    for f in fails:
        print('  -', f)
    print('\n服务日志尾部:')
    print('\n'.join(LOGS[-40:]))
print('工作目录:', WORK)
sys.exit(1 if fails else 0)
