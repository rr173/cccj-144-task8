"""计量工器具检定与批次召回端到端测试。

覆盖：证书版本沿革（登记/换版/已用不可覆盖/暂停）、准备清单资格（临近到期/暂停/换版/
电压不适用/无证，含可核对拒绝原因）、锁后换版与召回的现场闸门、批次召回四种处置
（库内隔离/预留撤资格/现场冻结/归档只加标记）、召回与换版/许可并发的持久化唯一裁决、
相同召回幂等、复检部分恢复（被其他召回覆盖不恢复）、重启后恢复证书/召回/冻结/复检。
"""
import time

import pytest
from fastapi.testclient import TestClient

from server import create_app


@pytest.fixture()
def c(tmp_path):
    dbp = str(tmp_path / 'm.db')
    app = create_app(dbp)
    with TestClient(app) as client:
        yield client, dbp


def client(cf):
    return cf[0]


def dbpath(cf):
    return cf[1]


DAY = 86400_000


def make_ticket(cl, work=('F2',), plan_hours=8, voltage=10.0, title='检修'):
    now = int(time.time() * 1000)
    r = cl.post('/api/tickets', json={
        'title': title, 'work_area': list(work), 'created_by': '调度A',
        'voltage_kv': voltage, 'plan_start_ms': now, 'plan_end_ms': now + plan_hours * 3600_000})
    assert r.status_code == 201, r.text
    return r.json()


def lock(cl, tid, serials=None, expected=201):
    r = cl.post(f'/api/tickets/{tid}/tool-locks', json={'serials': serials})
    assert r.status_code == expected, r.text
    return r if expected != 201 else r.json()


def lock_body(cl, tid, serials=None):
    return lock(cl, tid, serials, expected=201)


def metro_serials(cl, code, n=999):
    inv = cl.get('/api/tools/inventory').json()
    return [x['serial'] for x in inv['tools'] if x['code'] == code][:n]


def cert_id_of(sn, ver=None):
    return f'C-{sn}' if ver in (None, 1) else f'C-{sn}-V{ver}'


def register(cl, sn, valid_days=365, vmin=0.4, vmax=35, amend=False, expected=201,
             valid_from_ms=None, valid_to_ms=None):
    body = {'serial': sn, 'valid_days': valid_days, 'voltage_min': vmin, 'voltage_max': vmax,
            'force_amend': amend}
    if valid_from_ms is not None:
        body['valid_from'] = valid_from_ms
    if valid_to_ms is not None:
        body['valid_to'] = valid_to_ms
        body.pop('valid_days')
    r = cl.post('/api/metro/certificates', json=body)
    assert r.status_code == expected, r.text
    return r.json()


def history(cl, sn):
    return cl.get(f'/api/metro/tools/{sn}').json()


# ---------------------------------------------------------------- 证书版本沿革

def test_seed_certificates_exist_for_metro_tools(c):
    cl = client(c)
    bd = cl.get('/api/metro/board').json()
    metro = {t['serial']: t for t in bd['tools'] if t['metro']}
    # VDT 4 件 + WRENCH 3 件
    assert len(metro) == 7
    for sn, t in metro.items():
        assert t['latest_cert'] and t['latest_cert']['status'] == 'ACTIVE'
        assert t['latest_cert']['ver'] == 1


def test_non_metro_code_rejects_cert(c):
    cl = client(c)
    r = register(cl, 'GLOVE-B2401-01', expected=422 if False else 201) if False else \
        cl.post('/api/metro/certificates', json={'serial': 'GLOVE-B2401-01', 'valid_days': 10})
    assert r.status_code == 400
    assert r.json()['detail']['reason_code'] == 'NOT_METROLOGY_TOOL'


def test_cert_supersede_creates_successor_version(c):
    cl = client(c)
    sn = 'VDT-B2311-01'
    t = make_ticket(cl)
    lock(cl, t['id'])                          # 证书 v1 用于现场任务 → 固化
    h1 = history(cl, sn)
    assert h1['versions'][0]['used'] is True
    # 登记后继版本：旧版置 SUPERSEDED 原样保留，新增 v2
    r = register(cl, sn, valid_days=180)
    assert r['cert']['ver'] == 2 and r['cert']['cert_id'] == cert_id_of(sn, 2)
    h2 = history(cl, sn)
    assert [v['ver'] for v in h2['versions']] == [1, 2]
    assert h2['versions'][0]['status'] == 'SUPERSEDED' and h2['versions'][1]['status'] == 'ACTIVE'
    # 旧版行内容（生效区间）原样保留，没有被覆盖
    assert h2['versions'][0]['valid_to'] != h2['versions'][1]['valid_to']


def test_used_cert_cannot_be_overwritten_only_superseded(c):
    cl = client(c)
    sn = 'VDT-B2311-01'
    t = make_ticket(cl)
    lock(cl, t['id'])
    # force_amend 试图覆盖已使用证书 → 拒绝
    r = cl.post('/api/metro/certificates', json={
        'serial': sn, 'valid_days': 10, 'force_amend': True})
    assert r.status_code == 409
    assert r.json()['detail']['reason_code'] == 'CERT_IN_USE_IMMUTABLE'
    # 未使用的新证书允许笔误更正（v2 尚未使用）
    register(cl, sn, valid_days=200)
    r = cl.post('/api/metro/certificates', json={
        'serial': sn, 'valid_days': 220, 'force_amend': True})
    assert r.status_code == 200 and r.json()['amended'] is True
    assert history(cl, sn)['versions'][-1]['ver'] == 2


def test_suspend_and_resume_cert(c):
    cl = client(c)
    sn = 'WRENCH-B2401-01'
    r = cl.post('/api/metro/certificates/suspend',
                json={'cert_id': cert_id_of(sn), 'serial': sn, 'reason': '封印异常'})
    assert r.status_code == 200 and r.json()['cert']['status'] == 'SUSPENDED'
    # 重复暂停幂等
    r2 = cl.post('/api/metro/certificates/suspend',
                 json={'cert_id': cert_id_of(sn), 'serial': sn})
    assert r2.json().get('duplicate') is True
    r = cl.post('/api/metro/certificates/resume',
                json={'cert_id': cert_id_of(sn), 'serial': sn})
    assert r.status_code == 200 and r.json()['cert']['status'] == 'ACTIVE'


# ---------------------------------------------------------------- 准备清单资格

def test_eligibility_preview_lists_reject_reasons(c):
    cl = client(c)
    t = make_ticket(cl)
    r = cl.get(f"/api/tickets/{t['id']}/tool-eligibility")
    assert r.status_code == 200
    j = r.json()
    assert j['eligible'] is True
    assert all(x['ok'] for x in j['accepted']) and j['rejected'] == []


def test_lock_rejects_cert_expiring_before_job_end(c):
    cl = client(c)
    # 作业 48 小时；给一件验电器换一个昨日生效、1 日后到期的证书（作业结束前到期）
    sn = 'VDT-B2311-01'
    now = int(time.time() * 1000)
    t = make_ticket(cl, plan_hours=48)
    register(cl, sn, valid_from_ms=now - DAY, valid_to_ms=now + DAY)
    r = lock(cl, t['id'], {'VDT': [sn]}, expected=409)
    assert r.json()['detail']['reason_code'] == 'CERT_EXPIRES_BEFORE_JOB_END'
    assert '作业结束前到期' in r.json()['detail']['detail']
    # 整批原子回滚：没有任何锁留下
    assert cl.get(f"/api/tickets/{t['id']}/tool-locks").json()['lines'] == []


def test_lock_rejects_suspended_cert(c):
    cl = client(c)
    sn = 'VDT-B2311-02'
    t = make_ticket(cl)
    cl.post('/api/metro/certificates/suspend',
            json={'cert_id': cert_id_of(sn), 'serial': sn, 'reason': '待复检'})
    r = lock(cl, t['id'], {'VDT': [sn]}, expected=409)
    assert r.json()['detail']['reason_code'] == 'CERT_SUSPENDED'


def test_lock_rejects_superseded_cert_via_unused_latest(c):
    cl = client(c)
    # 未使用过的 v1 换新后，v1 SUPERSEDED；自动选件不会选到只持旧版的件
    sn = 'VDT-B2311-03'
    now = int(time.time() * 1000)
    register(cl, sn, valid_from_ms=now - DAY, valid_to_ms=now + 2 * DAY)  # v2 两日后到期
    t = make_ticket(cl, plan_hours=72)         # 作业 3 日 → v2 也在结束前到期
    r = cl.post(f'/api/tickets/{t["id"]}/tool-locks', json={})
    # 库存合格 VDT 不足（其余件可能也到期）→ 带原因的整批失败或成功选到其他件
    if r.status_code == 409:
        assert 'CERT_EXPIRES_BEFORE_JOB_END' in r.text
    else:
        # 自动选件成功则先释放整批锁，再验证指定坏件必被拒（整批原子回滚）
        cl.request('DELETE', f'/api/tickets/{t["id"]}/tool-locks', json={'actor': '库管员'})
    r2 = lock(cl, t['id'], {'VDT': [sn]}, expected=409)
    assert r2.json()['detail']['reason_code'] == 'CERT_EXPIRES_BEFORE_JOB_END'


def test_lock_rejects_voltage_not_applicable(c):
    cl = client(c)
    sn = 'VDT-B2311-01'
    t = make_ticket(cl, voltage=110.0)
    r = lock(cl, t['id'], {'VDT': [sn]}, expected=409)
    assert r.json()['detail']['reason_code'] == 'CERT_NOT_APPLICABLE_VOLTAGE'
    assert '110' in r.json()['detail']['detail']
    # 为全部 4 件验电器登记宽适用证书（0.4~220kV，覆盖 110kV）后可整批锁定
    now = int(time.time() * 1000)
    for i in range(1, 5):
        register(cl, f'VDT-B2311-0{i}', valid_days=365, vmin=0.4, vmax=220,
                 valid_from_ms=now - DAY)
    assert lock(cl, t['id'], {'VDT': ['VDT-B2311-01']}, expected=201)


def test_auto_pick_skips_ineligible_and_reports_reasons(c):
    cl = client(c)
    sn_bad = 'VDT-B2311-04'
    now = int(time.time() * 1000)
    register(cl, sn_bad, valid_from_ms=now - DAY, valid_to_ms=now + DAY)   # 即将到期
    t = make_ticket(cl, plan_hours=48)
    # 自动选件：4 件 VDT 中仅 3 件合格，需求 3 件 → 成功且不选坏件
    lines = lock(cl, t['id'], expected=201)['lines']
    vdts = [l['serial'] for l in lines if l['code'] == 'VDT']
    assert sn_bad not in vdts
    # 资格预览给出坏件的可核对原因
    elig = cl.get(f"/api/tickets/{t['id']}/tool-eligibility?code=VDT").json()
    bad = next(x for x in elig['rejected'] if x['serial'] == sn_bad)
    assert bad['reason_code'] == 'CERT_EXPIRES_BEFORE_JOB_END'


def test_lock_freezes_cert_version_and_records_it(c):
    cl = client(c)
    sn = 'VDT-B2311-01'
    t = make_ticket(cl)
    lines = lock(cl, t['id'])['lines']
    line = next(l for l in lines if l['serial'] == sn)
    assert line['cert']['cert_id'] == cert_id_of(sn) and line['cert']['ver'] == 1


# ---------------------------------------------------------------- 召回处置

def _full_kit(cl, tid, box):
    lines = lock(cl, tid)
    b = cl.post(f'/api/tickets/{tid}/tool-boxes',
                json={'box_id': box}).json()
    for l in b['lines']:
        r = cl.post(f'/api/tool-boxes/{box}/scan', json={'serial': l['serial']})
        assert r.status_code == 200 and r.json()['ok'], r.text
    return b


def test_recall_in_stock_quarantined(c):
    cl = client(c)
    sn = 'VDT-B2311-01'
    r = cl.post('/api/metro/recalls', json={
        'scope_type': 'SERIAL', 'scope_value': [sn], 'reason': '验电器批次性缺陷'})
    assert r.status_code == 201, r.text
    j = r.json()
    item = j['affected'][0]
    assert item['disposition'] == 'QUARANTINED'
    inv = {x['serial']: x['state'] for x in cl.get('/api/tools/inventory').json()['tools']}
    assert inv[sn] == 'QUARANTINED'
    # 隔离件不能再锁定
    t = make_ticket(cl)
    rr = lock(cl, t['id'], {'VDT': [sn]}, expected=409)
    assert rr.json()['detail']['reason_code'] == 'ITEM_QUARANTINED'


def test_recall_reserved_not_issued_revokes_qualification(c):
    cl = client(c)
    t = make_ticket(cl)
    lines = lock(cl, t['id'])['lines']
    sn = next(l['serial'] for l in lines if l['code'] == 'VDT')
    r = cl.post('/api/metro/recalls', json={
        'scope_type': 'SERIAL', 'scope_value': [sn], 'reason': '力矩超差'})
    j = r.json()
    assert j['affected'][0]['disposition'] == 'LOCK_REVOKED'
    # 锁行保留但撤销资格，工具箱视图含 disqualified
    b = cl.post(f'/api/tickets/{t["id"]}/tool-boxes', json={'box_id': 'BXRV'})
    assert b.status_code == 409
    assert b.json()['detail']['reason_code'] == 'LOCK_REVOKED_BY_RECALL'
    lv = cl.get(f'/api/tickets/{t["id"]}/tool-locks').json()['lines']
    # 撤销件仍可查但 metro_state=DISQUALIFIED（锁行保留审计、不再具资格）
    row = next(l for l in lv if l['serial'] == sn)
    assert row['metro_state'] == 'DISQUALIFIED' and row['metro_reason']
    bd = cl.get('/api/metro/board').json()
    dq = next(x for x in bd['disqualified_locks'] if x['item_serial'] == sn)
    assert dq['ticket'] == t['id']


def test_recall_field_in_use_freezes_but_keeps_holding(c):
    cl = client(c)
    t = make_ticket(cl)
    b = _full_kit(cl, t['id'], 'BXFD')
    line = next(l for l in b['lines'] if l['code'] == 'VDT')
    sn = line['serial']
    cl.post(f'/api/tool-boxes/BXFD/handover', json={'leader': '王负责', 'team': '甲班'})
    r = cl.post('/api/metro/recalls', json={
        'scope_type': 'SERIAL', 'scope_value': [sn], 'reason': '现场使用中召回'})
    j = r.json()
    assert j['affected'][0]['disposition'] == 'FIELD_FROZEN'
    # 工具仍是 ISSUED（真实持有记录保留），但 metro_frozen=1
    inv = {x['serial']: x for x in cl.get('/api/tools/inventory').json()['tools']}
    assert inv[sn]['state'] == 'ISSUED' and inv[sn]['metro_frozen'] in (1, True)
    # 领用记录（当时证书）原样可查
    bv = cl.get('/api/tool-boxes/BXFD').json()
    co = next(x for x in bv['checkouts'] if x['serial'] == sn)
    assert co['cert_id'] and co['cert_ver'] == 1
    # 受影响任务列出本票
    assert t['id'] in j['tasks']


def test_recall_archived_history_only_marks(c):
    cl = client(c)
    t = make_ticket(cl)
    b = _full_kit(cl, t['id'], 'BXAR')
    cl.post(f'/api/tool-boxes/BXAR/handover', json={'leader': '王负责', 'team': '甲班'})
    cl.post('/api/tool-boxes/BXAR/return', json={
        'items': [{'serial': l['serial'], 'code': l['code'], 'wear': 'OK', 'seal_ok': True}
                  for l in b['lines']]})
    sn = next(l['serial'] for l in b['lines'] if l['code'] == 'VDT')
    cert_then = next(x for x in cl.get('/api/tool-boxes/BXAR').json()['checkouts']
                     if x['serial'] == sn)
    r = cl.post('/api/metro/recalls', json={
        'scope_type': 'SERIAL', 'scope_value': [sn], 'reason': '归档后追溯召回'})
    j = r.json()
    assert j['affected'][0]['disposition'] == 'ARCHIVE_MARK' or \
        j['disposition_counts'].get('QUARANTINED') == 1
    marks = [m for m in j['marks'] if m['serial'] == sn and m['kind'] == 'ARCHIVE_MARK']
    assert marks, '归档历史必须追加召回标记'
    assert cert_then['cert_id'] in marks[0]['detail']
    # 当时领用记录使用的证书不被改写
    co_now = next(x for x in cl.get('/api/tool-boxes/BXAR').json()['checkouts']
                  if x['serial'] == sn)
    assert co_now['cert_id'] == cert_then['cert_id'] and co_now['cert_ver'] == cert_then['cert_ver']
    h = history(cl, sn)
    assert any(m['kind'] == 'ARCHIVE_MARK' for m in h['marks'])


def test_batch_recall_covers_all_serials(c):
    cl = client(c)
    r = cl.post('/api/metro/recalls', json={
        'scope_type': 'BATCH', 'scope_value': ['B2311'], 'reason': 'B2311 批次绝缘缺陷'})
    j = r.json()
    # B2311：VDT 4 + ROD 5（含维修件）；召回范围按批号覆盖全部器具
    serials = {x['serial'] for x in cl.get('/api/tools/inventory').json()['tools']
               if x['batch_id'] == 'B2311'}
    assert {i['serial'] for i in j['affected']} == serials
    assert j['disposition_counts'].get('QUARANTINED', 0) >= 4
    # 维修件登记为 OUT_OF_SERVICE，不二次处置
    maint = next(i for i in j['affected'] if i['serial'] == 'ROD-B2311-05')
    assert maint['disposition'] == 'OUT_OF_SERVICE'


def test_cert_version_recall_scope(c):
    cl = client(c)
    sn = 'VDT-B2311-01'
    register(cl, sn, valid_days=300)              # v2
    r = cl.post('/api/metro/recalls', json={
        'scope_type': 'CERT',
        'scope_value': [f'{cert_id_of(sn, 1)}@1'],   # 仅召回 v1 覆盖的器具
        'reason': 'v1 检定装置失准'})
    assert r.status_code == 201
    assert any(i['serial'] == sn for i in r.json()['affected'])


# ---------------------------------------------------------------- 幂等与并发唯一裁决

def test_duplicate_recall_returns_original(c):
    cl = client(c)
    sn = 'WRENCH-B2401-01'
    body = {'scope_type': 'SERIAL', 'scope_value': [sn], 'reason': '重复召回测试'}
    r1 = cl.post('/api/metro/recalls', json=body).json()
    r2 = cl.post('/api/metro/recalls', json=body).json()
    assert r2['duplicate'] is True and r2['id'] == r1['id']
    # 没有两条召回、没有两条相同标记/审计
    bd = cl.get('/api/metro/board').json()
    assert len([r for r in bd['recalls'] if r['id'] == r1['id']]) == 1
    marks = [m for r in bd['recalls'] for m in r['marks'] if r['id'] == r1['id']]
    assert len(marks) == len(set((m['kind'], m['ref_type'], m['ref_id']) for m in marks))
    evs = cl.get('/api/events', params={'limit': 200}).json()
    assert sum(1 for e in evs if e['type'] == 'METRO_RECALLED'
               and sn in (e['payload'] or '')) == 1


def test_idem_key_recall_returns_first(c):
    cl = client(c)
    sn = 'WRENCH-B2401-02'
    r1 = cl.post('/api/metro/recalls', json={
        'scope_type': 'SERIAL', 'scope_value': [sn], 'reason': 'A', 'key': 'K-1'}).json()
    r2 = cl.post('/api/metro/recalls', json={
        'scope_type': 'SERIAL', 'scope_value': [sn], 'reason': '不同原因', 'key': 'K-1'}).json()
    assert r2['id'] == r1['id'] and r2['duplicate'] is True


def test_persisted_version_decides_cert_supersede_vs_permit(c):
    """锁后换版：现场闸门按持久化版本唯一裁决——

    - 已领用件：换版后重复扫码仍返回首次原结果（持久化领用记录、当时证书保留）；
    - 另一件被锁件：其固化证书自身未失效，仍可出场（版本裁决逐件独立）；
    - 暂停该件锁后所依据的证书版本后，扫码立即按持久化版本被拒。
    """
    cl = client(c)
    t = make_ticket(cl)
    lines = lock(cl, t['id'])['lines']
    b = cl.post(f'/api/tickets/{t["id"]}/tool-boxes', json={'box_id': 'BXCV'}).json()
    first_vdt = next(l['serial'] for l in b['lines'] if l['code'] == 'VDT')
    other = next(l['serial'] for l in b['lines']
                 if l['code'] == 'VDT' and l['serial'] != first_vdt)
    # 先领用一件
    r1 = cl.post('/api/tool-boxes/BXCV/scan', json={'serial': first_vdt})
    assert r1.json()['ok']
    # 锁后证书换版（旧 v1 SUPERSEDED）
    register(cl, first_vdt, valid_days=365)
    # 已领用结果不变（持久化领用记录、当时证书保留）
    rdup = cl.post('/api/tool-boxes/BXCV/scan', json={'serial': first_vdt})
    assert rdup.json()['duplicate'] is True
    # 另一件的固化版本未失效，仍可领用
    r2 = cl.post('/api/tool-boxes/BXCV/scan', json={'serial': other})
    assert r2.json()['ok'] is True
    # 另一件的固化版本被暂停 → 现场闸门立刻按持久化版本拒绝（不可凭任何版本出场）
    other2 = next(l['serial'] for l in b['lines']
                  if l['code'] == 'VDT' and l['serial'] not in (first_vdt, other))
    cl.post('/api/metro/certificates/suspend',
            json={'cert_id': cert_id_of(other2), 'serial': other2, 'reason': '锁后暂停'})
    r3 = cl.post('/api/tool-boxes/BXCV/scan', json={'serial': other2})
    assert r3.json()['ok'] is False and r3.json()['reason_code'] == 'CERT_SUSPENDED'


def test_suspend_after_lock_blocks_field_gate(c):
    cl = client(c)
    t = make_ticket(cl)
    lines = lock(cl, t['id'])['lines']
    b = cl.post(f'/api/tickets/{t["id"]}/tool-boxes', json={'box_id': 'BXSP'}).json()
    sn = next(l['serial'] for l in b['lines'] if l['code'] == 'VDT')
    cl.post('/api/metro/certificates/suspend',
            json={'cert_id': cert_id_of(sn), 'serial': sn, 'reason': '锁后暂停'})
    r = cl.post('/api/tool-boxes/BXSP/scan', json={'serial': sn})
    assert r.json()['ok'] is False and r.json()['reason_code'] == 'CERT_SUSPENDED'


def test_recall_concurrent_with_scan_only_one_outcome(c):
    """召回与现场许可并发：同一件先扫码成功则召回为现场冻结；先召回则扫码被拒。
    两种顺序各只有一个确定结果（持久化版本决定）。"""
    # 顺序 A：召回先于扫码 → 扫码被拒
    cl = client(c)
    t = make_ticket(cl)
    lines = lock(cl, t['id'])['lines']
    b = cl.post(f'/api/tickets/{t["id"]}/tool-boxes', json={'box_id': 'BXA'}).json()
    sn = next(l['serial'] for l in b['lines'] if l['code'] == 'VDT')
    cl.post('/api/metro/recalls', json={'scope_type': 'SERIAL', 'scope_value': [sn],
                                        'reason': '并发召回A'})
    r = cl.post('/api/tool-boxes/BXA/scan', json={'serial': sn})
    assert r.json()['ok'] is False
    assert r.json()['reason_code'] in ('LOCK_REVOKED_BY_RECALL', 'ITEM_QUARANTINED')
    bd = cl.get('/api/metro/board').json()
    assert next(x for x in bd['tools'] if x['serial'] == sn)['state'] == 'QUARANTINED'

    # 顺序 B：扫码先于召回 → 持有成功，召回只冻结
    # 票一无物理步骤，先作废未移交箱（回滚库存锁，隔离件保持隔离），撤回释放控制权后在同区开新票
    cl.post('/api/tool-boxes/BXA/abort', json={'actor': '库管员', 'reason': '并发演示换票'})
    cl.post(f"/api/tickets/{t['id']}/withdraw", json={'actor': '调度A', 'reason': '并发演示换票'})
    t2 = make_ticket(cl, title='票二')
    lines2 = lock(cl, t2['id'])
    b2 = cl.post(f'/api/tickets/{t2["id"]}/tool-boxes', json={'box_id': 'BXB'}).json()
    sn2 = next(l['serial'] for l in b2['lines'] if l['code'] == 'VDT')
    assert cl.post('/api/tool-boxes/BXB/scan', json={'serial': sn2}).json()['ok']
    cl.post('/api/tool-boxes/BXB/handover', json={'leader': '李负责', 'team': '乙班'})
    j = cl.post('/api/metro/recalls', json={'scope_type': 'SERIAL', 'scope_value': [sn2],
                                            'reason': '并发召回B'}).json()
    assert j['affected'][0]['disposition'] == 'FIELD_FROZEN'


# ---------------------------------------------------------------- 复检与解除（部分恢复）

def test_release_requires_pass_recheck(c):
    cl = client(c)
    sn = 'VDT-B2311-01'
    rc = cl.post('/api/metro/recalls', json={
        'scope_type': 'SERIAL', 'scope_value': [sn], 'reason': '复检前置测试'}).json()
    r = cl.post(f"/api/metro/recalls/{rc['id']}/release", json={'recheck_id': 'NOPE'})
    assert r.status_code == 404
    assert r.json()['detail']['reason_code'] == 'RECHECK_NOT_FOUND'


def test_partial_release_fail_kept_quarantined(c):
    cl = client(c)
    s1, s2 = 'VDT-B2311-01', 'VDT-B2311-02'
    rc = cl.post('/api/metro/recalls', json={
        'scope_type': 'SERIAL', 'scope_value': [s1, s2], 'reason': '部分恢复测试'}).json()
    chk = cl.post(f"/api/metro/recalls/{rc['id']}/rechecks", json={'results': [
        {'serial': s1, 'result': 'PASS'}, {'serial': s2, 'result': 'FAIL'}]}).json()
    rel = cl.post(f"/api/metro/recalls/{rc['id']}/release",
                  json={'recheck_id': chk['recheck_id']}).json()
    assert [x['serial'] for x in rel['restored']] == [s1]
    assert any(k['serial'] == s2 and k['reason'] == 'NO_PASS_RECHECK' for k in rel['kept'])
    inv = {x['serial']: x['state'] for x in cl.get('/api/tools/inventory').json()['tools']}
    assert inv[s1] == 'FREE' and inv[s2] == 'QUARANTINED'
    # 召回仍 ACTIVE（未全部恢复），可继续补复检
    assert cl.get(f"/api/metro/recalls/{rc['id']}").json()['status'] == 'ACTIVE'


def test_release_idempotent_same_recheck(c):
    cl = client(c)
    sn = 'WRENCH-B2401-01'
    rc = cl.post('/api/metro/recalls', json={
        'scope_type': 'SERIAL', 'scope_value': [sn], 'reason': '幂等解除'}).json()
    chk = cl.post(f"/api/metro/recalls/{rc['id']}/rechecks",
                  json={'results': [{'serial': sn, 'result': 'PASS'}]}).json()
    r1 = cl.post(f"/api/metro/recalls/{rc['id']}/release",
                 json={'recheck_id': chk['recheck_id']}).json()
    r2 = cl.post(f"/api/metro/recalls/{rc['id']}/release",
                 json={'recheck_id': chk['recheck_id']}).json()
    assert r2.get('duplicate') is True and r2['restored'] == r1['restored']
    # 状态只恢复一次
    inv = {x['serial']: x['state'] for x in cl.get('/api/tools/inventory').json()['tools']}
    assert inv[sn] == 'FREE'


def test_release_skipped_when_other_active_recall_covers(c):
    cl = client(c)
    sn = 'VDT-B2311-01'
    rc1 = cl.post('/api/metro/recalls', json={
        'scope_type': 'SERIAL', 'scope_value': [sn], 'reason': '召回一'}).json()
    rc2 = cl.post('/api/metro/recalls', json={
        'scope_type': 'SERIAL', 'scope_value': [sn], 'reason': '召回二'}).json()
    # 第二笔相同范围不同原因 → 独立召回（同件被两笔有效召回覆盖）
    assert rc2['id'] != rc1['id'] and not rc2.get('duplicate')
    chk1 = cl.post(f"/api/metro/recalls/{rc1['id']}/rechecks",
                   json={'results': [{'serial': sn, 'result': 'PASS'}]}).json()
    rel = cl.post(f"/api/metro/recalls/{rc1['id']}/release",
                  json={'recheck_id': chk1['recheck_id']}).json()
    # 被 rc2 覆盖 → 本召回解除但器具不恢复
    kept = next(k for k in rel['kept'] if k['serial'] == sn)
    assert kept['reason'] == 'COVERED_BY_OTHER_RECALL' and rc2['id'] in kept['detail']
    inv = {x['serial']: x['state'] for x in cl.get('/api/tools/inventory').json()['tools']}
    assert inv[sn] == 'QUARANTINED'
    # rc2 也复检通过并解除后才恢复
    chk2 = cl.post(f"/api/metro/recalls/{rc2['id']}/rechecks",
                   json={'results': [{'serial': sn, 'result': 'PASS'}]}).json()
    rel2 = cl.post(f"/api/metro/recalls/{rc2['id']}/release",
                   json={'recheck_id': chk2['recheck_id']}).json()
    assert [x['serial'] for x in rel2['restored']] == [sn]
    inv = {x['serial']: x['state'] for x in cl.get('/api/tools/inventory').json()['tools']}
    assert inv[sn] == 'FREE'


def test_field_frozen_partial_release_unfreezes(c):
    cl = client(c)
    t = make_ticket(cl)
    b = _full_kit(cl, t['id'], 'BXFR')
    cl.post(f'/api/tool-boxes/BXFR/handover', json={'leader': '王负责', 'team': '甲班'})
    sn = next(l['serial'] for l in b['lines'] if l['code'] == 'VDT')
    rc = cl.post('/api/metro/recalls', json={
        'scope_type': 'SERIAL', 'scope_value': [sn], 'reason': '现场冻结解除测试'}).json()
    chk = cl.post(f"/api/metro/recalls/{rc['id']}/rechecks",
                  json={'results': [{'serial': sn, 'result': 'PASS'}]}).json()
    rel = cl.post(f"/api/metro/recalls/{rc['id']}/release",
                  json={'recheck_id': chk['recheck_id']}).json()
    disp = next(x['disposition'] for x in rel['restored'] if x['serial'] == sn)
    assert disp == 'FIELD_FROZEN'
    inv = {x['serial']: x for x in cl.get('/api/tools/inventory').json()['tools']}
    assert inv[sn]['metro_frozen'] in (0, False) and inv[sn]['state'] == 'ISSUED'
    # 真实持有/领用记录仍在
    assert any(x['serial'] == sn for x in cl.get('/api/tool-boxes/BXFR').json()['checkouts'])


# ---------------------------------------------------------------- 重启恢复

def test_restart_preserves_certs_recalls_freeze_and_pending_recheck(c):
    cl = client(c)
    path = dbpath(c)
    t = make_ticket(cl)
    b = _full_kit(cl, t['id'], 'BXRR')
    cl.post(f'/api/tool-boxes/BXRR/handover', json={'leader': '王负责', 'team': '甲班'})
    held_sn = next(l['serial'] for l in b['lines'] if l['code'] == 'VDT')
    free_sn = 'WRENCH-B2401-03'
    # 换版 + 现场冻结 + 库内隔离（未完成复检）
    register(cl, held_sn, valid_days=300)
    rc = cl.post('/api/metro/recalls', json={
        'scope_type': 'SERIAL', 'scope_value': [held_sn, free_sn],
        'reason': '重启恢复测试'}).json()
    import server
    with TestClient(server.create_app(path)) as cl2:
        bd = cl2.get('/api/metro/board').json()
        tools = {x['serial']: x for x in bd['tools']}
        # 证书版本沿革恢复（v1 SUPERSEDED + v2 ACTIVE）
        vers = {c['ver']: c['status'] for c in bd['certificates'] if c['serial'] == held_sn}
        assert vers == {1: 'SUPERSEDED', 2: 'ACTIVE'}
        # 冻结原因、隔离状态恢复
        assert tools[held_sn]['metro_frozen'] in (1, True)
        assert tools[held_sn]['freeze_reason'] and rc['id'] in tools[held_sn]['freeze_reason']
        assert tools[free_sn]['state'] == 'QUARANTINED'
        rv = cl2.get(f"/api/metro/recalls/{rc['id']}").json()
        assert set(rv['pending_restore']) == {held_sn, free_sn}
        # 未完成复检可在重启后继续：补 PASS 复检并解除
        chk = cl2.post(f"/api/metro/recalls/{rc['id']}/rechecks", json={'results': [
            {'serial': held_sn, 'result': 'PASS'},
            {'serial': free_sn, 'result': 'PASS'}]}).json()
        rel = cl2.post(f"/api/metro/recalls/{rc['id']}/release",
                       json={'recheck_id': chk['recheck_id']})
        assert rel.status_code == 200, rel.text
        assert rel.json()['status'] == 'RELEASED'


def test_restart_reopen_endpoint_recovers_metro(c):
    cl = client(c)
    sn = 'VDT-B2311-02'
    cl.post('/api/metro/recalls', json={'scope_type': 'SERIAL', 'scope_value': [sn],
                                        'reason': 'reopen 恢复'})
    r = cl.post('/api/admin/reopen')
    assert r.status_code == 200 and '召回' in r.json()['note']
    bd = cl.get('/api/metro/board').json()
    assert next(x for x in bd['tools'] if x['serial'] == sn)['state'] == 'QUARANTINED'
