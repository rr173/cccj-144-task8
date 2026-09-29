"""端到端测试：雷暴撤离与复工闸门。

覆盖：
- 告警触发撤离（名册/带电风险点/工器具占有/票据阶段立即固定，票据冻结）
- 扫码到达：代签、重复扫码、已离场误入、未登记签到、更换避险点仅留最新
- 人员缺失 / 风险点未监护 / 器具去向不明 三条阻断
- 双人放行：同一撤离批次 + 同一环境版本；依据变化旧确认失效（不删事实）
- 第二次告警抢占待复工批次
- 票据收工/撤销/换班与撤离令并发只有一个最终阶段
- 幂等：撤离令/签到/清点/放行/复工重放返回最初结果，不多写记录
- 进程重启后完整还原名册、分布、未清项与有效确认
"""
import threading
import pytest
from fastapi.testclient import TestClient

from server import create_app
import storm


@pytest.fixture()
def c(tmp_path):
    app = create_app(str(tmp_path / 't.db'))
    with TestClient(app) as client:
        yield client


def _ok(r, code=200):
    assert r.status_code == code, r.text
    return r.json()


def prepare(c, mark_left=None):
    """演示准备：F2 检修票已隔离许可、工具锁定并领用两件、4 人名册（可选一人先离场）。"""
    p = _ok(c.post('/api/demo/storm/prepare'), 201)
    tid = p['ticket_id']
    for person in ([mark_left] if isinstance(mark_left, str) else (mark_left or [])):
        _ok(c.post(f'/api/tickets/{tid}/storm/roster/{person}/left',
                   json={'actor': '现场负责人'}))
    return p


def alert(c, areas=('F2',), aid='WX1'):
    return _ok(c.post('/api/storm/weather-alerts',
                      json={'areas': list(areas), 'alert_id': aid}), 201)


def evac(c, p, aid='WX1', actor='值班长', idem_key=None):
    return _ok(c.post(f"/api/tickets/{p['ticket_id']}/storm/evacuation-orders",
                      json={'actor': actor, 'alert_id': aid, 'idem_key': idem_key}), 201)


def batch(c, bid):
    return _ok(c.get(f'/api/storm/batches/{bid}'))


def ci(c, bid, person, token, shelter):
    return c.post(f'/api/storm/batches/{bid}/checkins',
                  json={'person': person, 'token': token, 'shelter': shelter}).json()


def set_sighting(c, bid, serial, where, holder=None):
    return c.post(f'/api/storm/batches/{bid}/tool-sightings',
                  json={'serial': serial, 'where': where, 'holder': holder}).json()


def all_safe(c, p, bid, issued):
    """三人到齐、风险点全部指派并本人确认、两件已领用工具申报去向。"""
    # 风险点监护
    bv = batch(c, bid)
    guardians = ['张三', '李四', '孙七']
    for i, r in enumerate(bv['risks']):
        g = guardians[i % 3]
        _ok(c.post(f"/api/storm/batches/{bid}/risks/{r['line_no']}/guardian",
                   json={'guardian': g, 'actor': '值班长'}))
        _ok(c.post(f"/api/storm/batches/{bid}/risks/{r['line_no']}/confirm",
                   json={'guardian': g, 'token': p['tokens'][g]}))
    # 两件已领用手套申报随人在避险点
    for sn, holder in zip(issued, ['张三', '李四']):
        assert set_sighting(c, bid, sn, 'SHELTER', holder)['ok']
    # 三人扫码到达
    ci(c, bid, '张三', p['tokens']['张三'], 'QR-SH-A')
    ci(c, bid, '李四', p['tokens']['李四'], 'QR-SH-A')
    ci(c, bid, '孙七', p['tokens']['孙七'], 'QR-SH-C')


# ---------------------------------------------------------------- 告警触发撤离

def test_evacuation_fixes_snapshot_and_freezes_ticket(c):
    p = prepare(c)
    alert(c)
    b = evac(c, p)
    tid = p['ticket_id']
    # 名册固定
    snap = {r['person']: r for r in b['roster']}
    assert set(snap) == {'张三', '李四', '赵六', '孙七'}
    assert all(r['status'] == 'ON_DUTY' for r in snap.values())
    # 仍带电风险点：边界开关电源侧（PERMITTED 时工作区已无电，但开关防误合须看守）
    assert {r['kind'] for r in b['risks']} == {'BOUNDARY_SOURCE_SIDE_LIVE'}
    assert len(b['risks']) >= 1
    # 工器具占有快照：2 件已领用 + 其余在库锁定
    poss = {t['serial']: t['possession'] for t in b['tools']}
    assert poss[p['issued_serials'][0]] == 'ISSUED_ON_SITE'
    assert poss[p['issued_serials'][1]] == 'ISSUED_ON_SITE'
    assert any(v == 'LOCKED_AT_STORE' for v in poss.values())
    # 票据阶段固定且已冻结
    assert b['ticket']['status'] == 'PERMITTED'
    assert b['ticket']['frozen'] is True
    t = _ok(c.get(f'/api/tickets/{tid}'))
    assert t['frozen'] == 1
    # 收工/许可/作业全部被闸门阻断
    r = c.post(f'/api/tickets/{tid}/finish-work', json={'actor': '值班长'})
    assert r.status_code == 423
    steps = t['steps']
    # 无新增步骤可做（全部 VERIFIED）；解冻字段本身即阻断
    assert all(s['status'] == 'VERIFIED' for s in steps)


def test_evacuation_risks_include_live_work_area_before_isolation(c):
    """隔离完成前撤离：工作区设备仍带电也被固定为风险点。"""
    # 直接开 F2 票（ISOLATING，不隔离），告警撤离
    r = c.post('/api/tickets', json={'title': '隔离前撤离', 'work_area': ['F2'],
                                    'created_by': '调度A'})
    tid = _ok(r, 201)['id']
    alert(c)
    p = {'ticket_id': tid}
    b = evac(c, p)
    assert any(r['kind'] == 'WORK_AREA_ENERGIZED' for r in b['risks'])


def test_alert_does_not_auto_evacuate(c):
    p = prepare(c)
    a = alert(c)
    # 仅登记告警并给出受影响票，不自动发令
    assert a['affected_tickets'] and a['affected_tickets'][0]['ticket'] == p['ticket_id']
    b = _ok(c.get('/api/storm/batches'))
    assert all(not x['batches'] for x in b['tickets']
               if x['id'] == p['ticket_id'])


# ---------------------------------------------------------------- 签到裁决

def test_checkin_rulings(c):
    p = prepare(c, mark_left='赵六')
    alert(c)
    b = evac(c, p)
    bid = b['id']
    # 未登记人员
    r = ci(c, bid, '外人', 'PW-UNKNOWN', 'QR-SH-A')
    assert r['ok'] is False and r['reason_code'] == 'PERSON_NOT_IN_ROSTER'
    # 代签：口令与本人不符
    r = ci(c, bid, '张三', p['tokens']['李四'], 'QR-SH-A')
    assert r['ok'] is False and r['reason_code'] == 'PROXY_SIGN_REJECTED'
    # 已离场人员误入
    r = ci(c, bid, '赵六', p['tokens']['赵六'], 'QR-SH-A')
    assert r['ok'] is False and r['reason_code'] == 'PERSON_ALREADY_LEFT'
    # 未登记避险点
    r = ci(c, bid, '张三', p['tokens']['张三'], 'QR-NOPE')
    assert r['ok'] is False and r['reason_code'] == 'SHELTER_NOT_FOUND'
    # 正常
    r = ci(c, bid, '张三', p['tokens']['张三'], 'QR-SH-A')
    assert r['ok'] and r['decision'] == 'CURRENT'
    # 重复扫码：首次结果、不多写
    r2 = ci(c, bid, '张三', p['tokens']['张三'], 'QR-SH-A')
    assert r2['duplicate'] and r2['decision'] == 'CURRENT'
    rows = c.get(f'/api/storm/batches/{bid}').json()
    same = [x for x in rows['roster'] if x['person'] == '张三'][0]
    assert same['shelter'] == 'SH-A'
    # 更换避险点：旧去向 SUPERSEDED，仅最新有效
    r3 = ci(c, bid, '张三', p['tokens']['张三'], 'QR-SH-B')
    assert r3['reason_code'] == 'SUPERSEDED'
    bv = batch(c, bid)
    assert bv['shelter_distribution'] == {'SH-B': 1}
    me = _ok(c.get(f"/api/storm/person/{bid}/张三"))
    decisions = [x['decision'] for x in me['checkins']]
    assert decisions.count('CURRENT') == 1
    assert 'SUPERSEDED' in decisions


def test_checkin_after_batch_closed(c):
    p = prepare(c)
    alert(c)
    b = evac(c, p)
    # 票据收工不可行（已冻结）；直接以撤销售票终局后签到被拒
    _ok(c.post(f"/api/tickets/{p['ticket_id']}/withdraw", json={'actor': '调度A'}))
    bv = batch(c, b['id'])
    assert bv['status'] == 'TERMINATED'
    r = ci(c, b['id'], '张三', p['tokens']['张三'], 'QR-SH-A')
    assert r['ok'] is False and r['reason_code'] == 'EVAC_BATCH_NOT_ACTIVE'


# ---------------------------------------------------------------- 三条阻断与双人放行

def test_missing_person_blocks_resume(c):
    p = prepare(c, mark_left='赵六')
    alert(c)
    b = evac(c, p)
    bid = b['id']
    # 只让两人到达（孙七缺失），风险点与器具都就绪
    bv = batch(c, bid)
    for i, r in enumerate(bv['risks']):
        g = ['张三', '李四'][i % 2]
        _ok(c.post(f"/api/storm/batches/{bid}/risks/{r['line_no']}/guardian",
                   json={'guardian': g}))
        _ok(c.post(f"/api/storm/batches/{bid}/risks/{r['line_no']}/confirm",
                   json={'guardian': g, 'token': p['tokens'][g]}))
    for sn, holder in zip(p['issued_serials'], ['张三', '李四']):
        assert set_sighting(c, bid, sn, 'SHELTER', holder)['ok']
    ci(c, bid, '张三', p['tokens']['张三'], 'QR-SH-A')
    ci(c, bid, '李四', p['tokens']['李四'], 'QR-SH-A')
    c.post('/api/storm/weather-clear', json={'areas': ['F2']})
    r = c.post(f'/api/storm/batches/{bid}/confirms/field', json={'leader': '负责人'})
    assert r.status_code == 409 and r.json()['detail']['reason_code'] == 'PERSON_NOT_SAFE'


def test_unguarded_risk_blocks(c):
    p = prepare(c, mark_left='赵六')
    alert(c)
    b = evac(c, p)
    bid = b['id']
    # 人员到齐 + 工具申报，但风险点不监护
    for sn, holder in zip(p['issued_serials'], ['张三', '李四']):
        assert set_sighting(c, bid, sn, 'SHELTER', holder)['ok']
    ci(c, bid, '张三', p['tokens']['张三'], 'QR-SH-A')
    ci(c, bid, '李四', p['tokens']['李四'], 'QR-SH-A')
    ci(c, bid, '孙七', p['tokens']['孙七'], 'QR-SH-C')
    r = c.post(f'/api/storm/batches/{bid}/confirms/field', json={'leader': '负责人'})
    assert r.status_code == 409
    assert r.json()['detail']['reason_code'] == 'RISK_NOT_GUARDED'
    # 监护人代确认（错口令）拒绝
    r0 = b['risks'][0]
    _ok(c.post(f"/api/storm/batches/{bid}/risks/{r0['line_no']}/guardian",
               json={'guardian': '张三'}))
    r2 = c.post(f"/api/storm/batches/{bid}/risks/{r0['line_no']}/confirm",
                json={'guardian': '张三', 'token': 'WRONG'})
    assert r2.status_code == 409 and r2.json()['detail']['reason_code'] == 'GUARDIAN_PROXY_REJECTED'


def test_missing_tool_blocks_and_conflict_rulings(c):
    p = prepare(c, mark_left='赵六')
    alert(c)
    b = evac(c, p)
    bid = b['id']
    s1, s2 = p['issued_serials']
    # 第一件申报在避险点（孙七持有）
    r = set_sighting(c, bid, s1, 'SHELTER', '孙七')
    assert r['ok'] and r['decision'] == 'VALID'
    # 另一人申报同件冲突：先到为准
    r = set_sighting(c, bid, s1, 'SHELTER', '李四')
    assert r['ok'] is False and r['reason_code'] == 'CONFLICT'
    # 持有人改报箱内：覆盖
    r = set_sighting(c, bid, s1, 'BOX', '孙七')
    assert r['decision'] == 'SUPERSEDED'
    # 相同请求重放：返回首次结果（SUPERSEDED）并标记 duplicate，不多写
    r = set_sighting(c, bid, s1, 'BOX', '孙七')
    assert r['duplicate'] and r['decision'] == 'SUPERSEDED'
    # 自然重复扫码语义：持有人对当前有效去向再报一次同去向 → DUPLICATE
    r = c.post(f'/api/storm/batches/{bid}/tool-sightings',
               json={'serial': s1, 'where': 'BOX', 'holder': '孙七',
                     'idem_key': 'NAT-DUP-1'}).json()
    r2 = c.post(f'/api/storm/batches/{bid}/tool-sightings',
                json={'serial': s1, 'where': 'BOX', 'holder': '孙七',
                      'idem_key': 'NAT-DUP-2'}).json()
    assert r2['ok'] and r2['decision'] == 'DUPLICATE'
    # 第二件保持去向不明 → 现场确认阻断
    # 先把人到齐、风险点监护好
    ci(c, bid, '张三', p['tokens']['张三'], 'QR-SH-A')
    ci(c, bid, '李四', p['tokens']['李四'], 'QR-SH-A')
    ci(c, bid, '孙七', p['tokens']['孙七'], 'QR-SH-C')
    for r0 in b['risks']:
        g = '张三'
        c.post(f"/api/storm/batches/{bid}/risks/{r0['line_no']}/guardian", json={'guardian': g})
        c.post(f"/api/storm/batches/{bid}/risks/{r0['line_no']}/confirm",
               json={'guardian': g, 'token': p['tokens'][g]})
    r = c.post(f'/api/storm/batches/{bid}/confirms/field', json={'leader': '负责人'})
    assert r.status_code == 409 and r.json()['detail']['reason_code'] == 'TOOL_LOCATION_UNKNOWN'
    # 在库锁定件不接受现场申报（不属去向不明）
    locked = next(t['serial'] for t in batch(c, bid)['tools']
                  if t['possession'] == 'LOCKED_AT_STORE')
    r = set_sighting(c, bid, locked, 'SHELTER', '张三')
    assert r['ok'] is False and r['reason_code'] == 'TOOL_NOT_ON_SITE'


def test_lost_then_found_by_another_member(c):
    """报去向不明不是占有主张：任何人之后报找回都应覆盖，不能判为冲突。"""
    p = prepare(c, mark_left='赵六')
    alert(c)
    b = evac(c, p)
    bid = b['id']
    s1 = p['issued_serials'][0]
    r = set_sighting(c, bid, s1, 'LOST')
    assert r['decision'] == 'VALID'
    assert batch(c, bid)['tools'][0]['location_known'] is False or \
        not next(t for t in batch(c, bid)['tools'] if t['serial'] == s1)['location_known']
    # 另一成员报找回
    r = set_sighting(c, bid, s1, 'SHELTER', '李四')
    assert r['ok'] and r['decision'] == 'SUPERSEDED'
    t = next(x for x in batch(c, bid)['tools'] if x['serial'] == s1)
    assert t['location_known'] == 1 and t['location_holder'] == '李四'


def test_full_dual_release_flow(c):
    p = prepare(c, mark_left='赵六')
    alert(c)
    b = evac(c, p)
    bid = b['id']
    all_safe(c, p, bid, p['issued_serials'])
    # 调度确认先被气象挡住
    r = c.post(f'/api/storm/batches/{bid}/confirms/dispatch', json={'leader': '调度员B'})
    assert r.status_code == 409 and r.json()['detail']['reason_code'] == 'WEATHER_ALERT_ACTIVE'
    # 现场先确认
    r = _ok(c.post(f'/api/storm/batches/{bid}/confirms/field', json={'leader': '负责人A'}))
    assert r['ok']
    # 单边确认不得复工
    assert batch(c, bid)['status'] == 'ACTIVE'
    # 气象解除（环境版本推进）——旧的现场确认失效
    _ok(c.post('/api/storm/weather-clear', json={'areas': ['F2']}))
    bv = batch(c, bid)
    assert bv['confirms']['FIELD'][0]['status'] == 'SUPERSEDED'
    # 调度确认依据旧环境，仍能成功，但两确认环境不一致 → 不复工
    r = _ok(c.post(f'/api/storm/batches/{bid}/confirms/dispatch', json={'leader': '调度员B'}))
    bv = batch(c, bid)
    assert bv['valid_confirms'].get('DISPATCH')
    assert bv['gate']['can_resume'] is False
    assert any(x['code'] == 'CONFIRM_BASIS_MISMATCH' for x in bv['gate']['reasons'])
    # 现场按新版本重新确认 → 同环境 → 闸门开启（待复工）
    r = _ok(c.post(f'/api/storm/batches/{bid}/confirms/field', json={'leader': '负责人A'}))
    bv = batch(c, bid)
    assert bv['status'] == 'WAITING_RESUME'
    assert bv['gate']['can_resume'] is True
    # 票据已解冻
    assert bv['ticket']['frozen'] is False
    # 正式复工
    _ok(c.post(f'/api/storm/batches/{bid}/resume', json={'actor': '负责人A'}))
    assert batch(c, bid)['status'] == 'RESUMED'


def test_dispatcher_rejects_on_boundary_change(c):
    p = prepare(c)
    alert(c)
    b = evac(c, p)
    bid = b['id']
    all_safe(c, p, bid, p['issued_serials'])
    _ok(c.post('/api/storm/weather-clear', json={'areas': ['F2']}))
    # 拓扑变更（加连线）→ 环境版本变化且边界依据变化
    r = c.post('/sim/topology', json={'op': 'add_edge', 'a': 'F1', 'b': 'F3'})
    assert r.status_code == 200
    r = c.post(f'/api/storm/batches/{bid}/confirms/dispatch', json={'leader': '调度员B'})
    assert r.status_code == 409
    # F1-F3 新增连线不直接改变 F2 隔离开关分位 → 报拓扑版本变化
    assert r.json()['detail']['reason_code'] in ('BOUNDARY_VERSION_CHANGED',)


# ---------------------------------------------------------------- 第二次告警抢占

def test_second_alert_preempts_waiting_batch(c):
    p = prepare(c, mark_left='赵六')
    alert(c, aid='WX1')
    b = evac(c, p, aid='WX1')
    bid = b['id']
    all_safe(c, p, bid, p['issued_serials'])
    _ok(c.post('/api/storm/weather-clear', json={'areas': ['F2']}))
    _ok(c.post(f'/api/storm/batches/{bid}/confirms/field', json={'leader': '负责人A'}))
    _ok(c.post(f'/api/storm/batches/{bid}/confirms/dispatch', json={'leader': '调度员B'}))
    assert batch(c, bid)['status'] == 'WAITING_RESUME'
    # 新告警抢占待复工批次
    a2 = alert(c, aid='WX2')
    assert a2['affected_tickets']
    bv = batch(c, bid)
    assert bv['status'] == 'PREEMPTED'
    assert bv['ticket']['frozen'] is True
    # 旧确认事实保留但已失效
    assert all(x['status'] == 'SUPERSEDED'
               for xs in bv['confirms'].values() for x in xs)
    # 复工被拒
    r = c.post(f'/api/storm/batches/{bid}/resume', json={'actor': '负责人A'})
    assert r.status_code == 409
    # 新一轮撤离令开新批次（签到事实仍查旧批次，新批次重新清点）
    b2 = evac(c, p, aid='WX2')
    assert b2['id'] != bid and b2['seq'] == 2 and b2['status'] == 'ACTIVE'
    # 旧批次的签到事实仍可追溯
    old = batch(c, bid)
    assert old['status'] == 'PREEMPTED'
    assert sum(old['shelter_distribution'].values()) == 3


# ---------------------------------------------------------------- 票据终局并发

def test_withdraw_terminates_open_batch(c):
    p = prepare(c)
    alert(c)
    b = evac(c, p)
    _ok(c.post(f"/api/tickets/{p['ticket_id']}/withdraw", json={'actor': '调度A'}))
    bv = batch(c, b['id'])
    assert bv['status'] == 'TERMINATED'
    # 签到与确认事实保留，但票据已撤销
    t = _ok(c.get(f"/api/tickets/{p['ticket_id']}"))
    assert t['status'] == 'WITHDRAWN'


def test_finish_work_after_resume_then_close_terminates(c):
    p = prepare(c, mark_left='赵六')
    alert(c)
    b = evac(c, p)
    bid = b['id']
    all_safe(c, p, bid, p['issued_serials'])
    _ok(c.post('/api/storm/weather-clear', json={'areas': ['F2']}))
    _ok(c.post(f'/api/storm/batches/{bid}/confirms/field', json={'leader': '负责人A'}))
    _ok(c.post(f'/api/storm/batches/{bid}/confirms/dispatch', json={'leader': '调度员B'}))
    _ok(c.post(f'/api/storm/batches/{bid}/resume', json={'actor': '负责人A'}))
    # 收工并完成恢复步骤 → 票据关闭，批次已 RESUMED 不变
    t = _ok(c.post(f"/api/tickets/{p['ticket_id']}/finish-work", json={'actor': '调度A'}))
    assert t['status'] == 'RESTORING'
    t = _ok(c.get(f"/api/tickets/{p['ticket_id']}"))
    for s in t['steps']:
        if s['phase'] == 'REST':
            if s['kind'] in ('CLOSE_SWITCH', 'OPEN_SWITCH'):
                c.post('/sim/switch', json={'device': s['device'], 'state': s['expected'],
                                           'ticket': p['ticket_id']})
            else:
                c.post('/sim/ground', json={'point': s['point'], 'sw': s['sw'],
                                           'action': 'remove' if s['kind'] == 'REMOVE_GROUND' else 'install',
                                           'ticket': p['ticket_id']})
            c.post(f"/api/tickets/{p['ticket_id']}/steps/{s['id']}/report",
                   json={'actor': '张三', 'scanned': s['device']})
            c.post(f"/api/tickets/{p['ticket_id']}/steps/{s['id']}/verify",
                   json={'actor': '李四'})
    t = _ok(c.get(f"/api/tickets/{p['ticket_id']}"))
    assert t['status'] == 'CLOSED'
    assert batch(c, bid)['status'] == 'RESUMED'


# ---------------------------------------------------------------- 幂等：重放不产生第二条记录

def test_idempotent_replays(c):
    p = prepare(c, mark_left='赵六')
    alert(c)
    # 同一 idem_key 的撤离令（首次 201，重放仍为 201 但返回首次结果）
    b1 = _ok(c.post(f"/api/tickets/{p['ticket_id']}/storm/evacuation-orders",
                    json={'actor': '值班长', 'alert_id': 'WX1', 'idem_key': 'K1'}), 201)
    b2 = _ok(c.post(f"/api/tickets/{p['ticket_id']}/storm/evacuation-orders",
                    json={'actor': '值班长', 'alert_id': 'WX1', 'idem_key': 'K1'}), 201)
    assert b1['id'] == b2['id'] and b2['duplicate'] is True
    bid = b1['id']
    # 带同一幂等键的签到重放
    body = {'person': '张三', 'token': p['tokens']['张三'], 'shelter': 'QR-SH-A',
            'idem_key': 'CI1'}
    r1 = _ok(c.post(f'/api/storm/batches/{bid}/checkins', json=body))
    r2 = _ok(c.post(f'/api/storm/batches/{bid}/checkins', json=body))
    assert r2['duplicate']
    db = c.app.state.db
    n_ci = db.one("SELECT COUNT(*) c FROM storm_checkins WHERE batch=?", (bid,))['c']
    assert n_ci == 1
    # 清点/放行/复工重放
    all_safe(c, p, bid, p['issued_serials'])
    _ok(c.post('/api/storm/weather-clear', json={'areas': ['F2']}))
    f1 = _ok(c.post(f'/api/storm/batches/{bid}/confirms/field',
                    json={'leader': '负责人A', 'idem_key': 'F1'}))
    f2 = _ok(c.post(f'/api/storm/batches/{bid}/confirms/field',
                    json={'leader': '负责人A', 'idem_key': 'F1'}))
    assert f2['duplicate']
    d1 = _ok(c.post(f'/api/storm/batches/{bid}/confirms/dispatch',
                    json={'leader': '调度员B', 'idem_key': 'D1'}))
    d2 = _ok(c.post(f'/api/storm/batches/{bid}/confirms/dispatch',
                    json={'leader': '调度员B', 'idem_key': 'D1'}))
    assert d2['duplicate']
    n_cf = db.one("SELECT COUNT(*) c FROM storm_confirms WHERE batch=?", (bid,))['c']
    assert n_cf == 2
    r1 = _ok(c.post(f'/api/storm/batches/{bid}/resume',
                    json={'actor': '负责人A', 'idem_key': 'R1'}))
    r2 = _ok(c.post(f'/api/storm/batches/{bid}/resume',
                    json={'actor': '负责人A', 'idem_key': 'R1'}))
    assert r2['duplicate'] and r1['status'] == 'RESUMED'
    # 告警/解除重放
    a1 = _ok(c.post('/api/storm/weather-alerts',
                    json={'areas': ['T1'], 'alert_id': 'WXD', 'idem_key': 'WA'}), 201)
    a2 = _ok(c.post('/api/storm/weather-alerts',
                    json={'areas': ['T1'], 'alert_id': 'WXD', 'idem_key': 'WA'}), 201)
    assert a2['duplicate'] and a1['alert_id'] == a2['alert_id']


# ---------------------------------------------------------------- 重启恢复

def test_restart_recovers_full_state(c):
    p = prepare(c, mark_left='赵六')
    alert(c)
    b = evac(c, p)
    bid = b['id']
    # 双确认之间（仅现场确认完成）退出
    all_safe(c, p, bid, p['issued_serials'])
    _ok(c.post('/api/storm/weather-clear', json={'areas': ['F2']}))
    _ok(c.post(f'/api/storm/batches/{bid}/confirms/field', json={'leader': '负责人A'}))
    path = c.app.state.db.path
    # 主站重启
    _ok(c.post('/api/admin/reopen'))
    bv = batch(c, bid)
    # 名册分布、未清项、有效确认全部还原
    assert bv['shelter_distribution']  # 三人签到
    assert bv['valid_confirms']['FIELD']['leader'] == '负责人A'
    assert 'DISPATCH' not in bv['valid_confirms']
    assert bv['gate']['field_confirmed'] is True
    assert bv['gate']['dispatch_confirmed'] is False
    # 重启后流程继续：调度确认 → 复工
    _ok(c.post(f'/api/storm/batches/{bid}/confirms/dispatch', json={'leader': '调度员B'}))
    assert batch(c, bid)['status'] == 'WAITING_RESUME'
    _ok(c.post(f'/api/storm/batches/{bid}/resume', json={'actor': '负责人A'}))
    assert batch(c, bid)['status'] == 'RESUMED'


def test_restart_during_evacuation(c):
    """撤离中（有人未到、器具去向不明）重启，未清项原样还原。"""
    p = prepare(c, mark_left='赵六')
    alert(c)
    b = evac(c, p)
    bid = b['id']
    ci(c, bid, '张三', p['tokens']['张三'], 'QR-SH-A')
    _ok(c.post('/api/admin/reopen'))
    bv = batch(c, bid)
    assert bv['status'] == 'ACTIVE'
    bl = bv['blockers']
    assert set(bl['persons_missing']) == {'李四', '孙七'}  # 赵六已离场不计
    assert len(bl['tools_unknown']) == 2
    assert {r['device'] for r in bl['risks_unguarded']} == {r['device'] for r in bv['risks']}
    me = _ok(c.get(f"/api/storm/person/{bid}/张三"))
    assert me['me']['current_loc'] == 'SAFE' and me['me']['shelter'] == 'SH-A'
    # 重启后继续签到可推进
    ci(c, bid, '李四', p['tokens']['李四'], 'QR-SH-A')
    assert batch(c, bid)['shelter_distribution']['SH-A'] == 2


# ---------------------------------------------------------------- 并发：只有一个最终阶段

def test_concurrent_evacuation_orders_single_batch(c):
    """撤离令、告警与并发重复发令同时到达：同票只有一个开放撤离批次。"""
    p = prepare(c, mark_left='赵六')
    alert(c)
    results = []
    barrier = threading.Barrier(4)

    def order(n):
        barrier.wait()
        r = c.post(f"/api/tickets/{p['ticket_id']}/storm/evacuation-orders",
                   json={'actor': f'值班长{n}', 'alert_id': 'WX1', 'idem_key': f'C{n}'})
        results.append(r.json())

    ts = [threading.Thread(target=order, args=(i,)) for i in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    ids = {r['id'] for r in results}
    assert len(ids) == 1, results
    db = c.app.state.db
    n = db.one("SELECT COUNT(*) c FROM storm_batches WHERE ticket=? AND status IN (?,?)",
               (p['ticket_id'], storm.EV_ACTIVE, storm.EV_WAITING_RESUME))['c']
    assert n == 1


def test_concurrent_evac_and_withdraw_single_final_stage(c):
    """撤离令与票据撤销并发：无论先后，只有一个最终阶段（开放或终局，不并存）。"""
    p = prepare(c)
    alert(c)
    out = {}
    barrier = threading.Barrier(2)

    def evac():
        barrier.wait()
        out['evac'] = c.post(f"/api/tickets/{p['ticket_id']}/storm/evacuation-orders",
                             json={'actor': '值班长', 'alert_id': 'WX1'})

    def withdraw():
        barrier.wait()
        out['wd'] = c.post(f"/api/tickets/{p['ticket_id']}/withdraw",
                           json={'actor': '调度A', 'reason': '并发撤销'})

    t1, t2 = threading.Thread(target=evac), threading.Thread(target=withdraw)
    t1.start(); t2.start(); t1.join(); t2.join()
    # 撤销后票据为 WITHDRAWN；任何批次都必须已终局，不存在开放批次与撤销并存
    t = c.get(f"/api/tickets/{p['ticket_id']}").json()
    assert t['status'] == 'WITHDRAWN'
    db = c.app.state.db
    open_n = db.one("SELECT COUNT(*) c FROM storm_batches WHERE ticket=? AND status IN (?,?)",
                    (p['ticket_id'], storm.EV_ACTIVE, storm.EV_WAITING_RESUME))['c']
    assert open_n == 0
    # 若撤离令先到，其批次必须被随后的撤销终止
    for bid in [r['id'] for r in db.q('SELECT id FROM storm_batches WHERE ticket=?',
                                      (p['ticket_id'],))]:
        assert c.get(f'/api/storm/batches/{bid}').json()['status'] == storm.EV_TERMINATED


def test_concurrent_checkins_single_current(c):
    """同人同时多点扫码：数据库层保证只有一条 CURRENT（最新去向唯一）。"""
    p = prepare(c, mark_left='赵六')
    alert(c)
    b = evac(c, p)
    bid = b['id']
    shelters = ['QR-SH-A', 'QR-SH-B', 'QR-SH-C']
    barrier = threading.Barrier(len(shelters))

    def one(sh):
        barrier.wait()
        c.post(f'/api/storm/batches/{bid}/checkins',
               json={'person': '张三', 'token': p['tokens']['张三'], 'shelter': sh})

    ts = [threading.Thread(target=one, args=(s,)) for s in shelters]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    db = c.app.state.db
    n_cur = db.one("SELECT COUNT(*) c FROM storm_checkins WHERE batch=? AND person=? "
                   "AND decision='CURRENT'", (bid, '张三'))['c']
    assert n_cur == 1


# ---------------------------------------------------------------- 裁决落库可追溯

def test_all_rulings_persisted(c):
    p = prepare(c, mark_left='赵六')
    alert(c)
    b = evac(c, p)
    bid = b['id']
    ci(c, bid, '外人', 'X', 'QR-SH-A')
    ci(c, bid, '张三', p['tokens']['李四'], 'QR-SH-A')
    ci(c, bid, '赵六', p['tokens']['赵六'], 'QR-SH-A')
    ci(c, bid, '张三', p['tokens']['张三'], 'QR-SH-A')
    ci(c, bid, '张三', p['tokens']['张三'], 'QR-SH-B')
    dec = _ok(c.get('/api/storm/decisions', params={'batch': bid}))
    codes = {d['reason_code'] for d in dec}
    assert 'PERSON_NOT_IN_ROSTER' in codes
    assert 'PROXY_SIGN_REJECTED' in codes
    assert 'PERSON_ALREADY_LEFT' in codes
    assert {'CURRENT', 'SUPERSEDED'} & codes
    # 每条裁决带理由与接受标志
    for d in dec:
        assert d['detail'] and isinstance(d['accepted'], int)
