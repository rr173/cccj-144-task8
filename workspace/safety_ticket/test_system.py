"""端到端测试：覆盖安全票全生命周期与全部异常场景。"""
import pytest
from fastapi.testclient import TestClient
from server import create_app


@pytest.fixture()
def c(tmp_path):
    app = create_app(str(tmp_path / 't.db'))
    with TestClient(app) as client:
        yield client


def derive(c, work):
    r = c.post('/api/derive', json={'work_area': work})
    assert r.status_code == 200, r.text
    return r.json()


def create(c, work, title='检修'):
    r = c.post('/api/tickets', json={'title': title, 'work_area': work, 'created_by': '调度A'})
    assert r.status_code == 201, r.text
    return r.json()


def step_of(t, n):
    return t['steps'][n - 1]


def do_step(c, t, n, reporter='张三', verifier='李四'):
    """执行第 n 步：模拟设备操作 → 回报 → 双人复核。"""
    s = step_of(t, n)
    if s['kind'] in ('OPEN_SWITCH', 'CLOSE_SWITCH'):
        r = c.post('/sim/switch', json={'device': s['device'], 'state': s['expected'], 'ticket': t['id']})
    else:
        r = c.post('/sim/ground', json={'point': s['point'], 'sw': s['sw'],
                                        'action': 'install' if s['kind'] == 'INSTALL_GROUND' else 'remove',
                                        'ticket': t['id']})
    assert r.status_code == 200, r.text
    r = c.post(f"/api/tickets/{t['id']}/steps/{s['id']}/report",
               json={'actor': reporter, 'scanned': s['device']})
    assert r.status_code == 200, r.text
    r = c.post(f"/api/tickets/{t['id']}/steps/{s['id']}/verify", json={'actor': verifier})
    assert r.status_code == 200, r.text


def isolate_all(c, t):
    t = c.get(f"/api/tickets/{t['id']}").json()
    for n in range(1, len(t['steps']) + 1):
        do_step(c, t, n)
    return c.get(f"/api/tickets/{t['id']}").json()


def permit(c, t):
    r = c.post(f"/api/tickets/{t['id']}/permit", json={'actor': '调度A'})
    assert r.status_code == 200, r.text
    return r.json()


# ---------------------------------------------------------------- 边界推导

def test_derive_boundary(c):
    b = derive(c, ['F2'])
    assert [s['device'] for s in b['switches']] == ['SW105', 'CB104', 'SW103'] or \
           sorted(s['device'] for s in b['switches']) == ['CB104', 'SW103', 'SW105']
    assert sorted(g['id'] for g in b['grounds']) == ['GND:CB104', 'GND:SW103', 'GND:SW105']
    assert all(g['point'] == 'F2' for g in b['grounds'])
    rules = {f['device']: f['rule'] for f in b['forbidden']}
    assert rules['SW103'] == 'MUST_STAY_OPEN' and rules['GND:SW103'] == 'MUST_STAY_GROUNDED'
    assert b['control_conflict'] is None


def test_derive_impossible(c):
    r = c.post('/api/derive', json={'work_area': ['SRC1']})
    assert r.status_code == 400


# ---------------------------------------------------------------- 全流程

def test_full_lifecycle(c):
    t = create(c, ['F2'])
    assert t['status'] == 'ISOLATING'
    assert len(t['steps']) == 6                       # 3 断开关 + 3 装接地
    assert [s['kind'] for s in t['steps'][:3]] == ['OPEN_SWITCH'] * 3
    assert [s['kind'] for s in t['steps'][3:]] == ['INSTALL_GROUND'] * 3

    # 未满足隔离条件前禁止许可开工
    assert c.post(f"/api/tickets/{t['id']}/permit", json={'actor': '调度A'}).status_code == 409

    t = isolate_all(c, t)
    topo = c.get('/api/topology').json()
    assert 'F2' not in topo['energized']              # 工作区已停电
    assert len(topo['grounds']) == 3                  # 三组接地线

    t = permit(c, t)
    assert t['status'] == 'PERMITTED'

    r = c.post(f"/api/tickets/{t['id']}/finish-work", json={'actor': '调度A'})
    t = r.json()
    assert t['status'] == 'RESTORING'
    rest = [s for s in t['steps'] if s['phase'] == 'REST']
    iso = [s for s in t['steps'] if s['phase'] == 'ISO']
    # 恢复顺序 = 隔离顺序的严格逆序：先拆接地（逆序）后合开关（逆序）
    assert [s['kind'] for s in rest] == ['REMOVE_GROUND'] * 3 + ['CLOSE_SWITCH'] * 3
    assert [s['device'] for s in rest] == [s['device'] for s in reversed(iso)]

    for i in range(len(iso) + 1, len(iso) + len(rest) + 1):
        do_step(c, t, i)
    t = c.get(f"/api/tickets/{t['id']}").json()
    assert t['status'] == 'CLOSED'
    # 控制权全部释放，可以就同一设备再次开票
    t2 = create(c, ['F2'], title='二次检修')
    assert t2['id'] != t['id']


def test_two_person_rule(c):
    t = create(c, ['F2'])
    s = t['steps'][0]
    c.post('/sim/switch', json={'device': s['device'], 'state': 'OPEN', 'ticket': t['id']})
    c.post(f"/api/tickets/{t['id']}/steps/{s['id']}/report", json={'actor': '张三', 'scanned': s['device']})
    r = c.post(f"/api/tickets/{t['id']}/steps/{s['id']}/verify", json={'actor': '张三'})
    assert r.status_code == 400 and '双人复核' in r.json()['detail']
    r = c.post(f"/api/tickets/{t['id']}/steps/{s['id']}/verify", json={'actor': '李四'})
    assert r.status_code == 200


def test_cannot_fake_physical_step(c):
    """未真实操作设备就回报 → 遥测不符，拒绝登记。"""
    t = create(c, ['F2'])
    s = t['steps'][0]
    r = c.post(f"/api/tickets/{t['id']}/steps/{s['id']}/report", json={'actor': '张三', 'scanned': s['device']})
    assert r.status_code == 400 and '伪造' in r.json()['detail']


def test_order_enforced(c):
    t = create(c, ['F2'])
    s2 = t['steps'][1]                                # 跳步执行第 2 步
    c.post('/sim/switch', json={'device': s2['device'], 'state': 'OPEN', 'ticket': t['id']})
    r = c.post(f"/api/tickets/{t['id']}/steps/{s2['id']}/report", json={'actor': '张三', 'scanned': s2['device']})
    assert r.status_code == 409 and '顺序' in r.json()['detail']


def test_restoration_reverse_order_enforced(c):
    t = permit(c, isolate_all(c, create(c, ['F2'])))
    t = c.post(f"/api/tickets/{t['id']}/finish-work", json={'actor': '调度A'}).json()
    rest = [s for s in t['steps'] if s['phase'] == 'REST']
    last = rest[-1]                                   # 最后一步（合 SW103）不能先做
    r = c.post(f"/api/tickets/{t['id']}/steps/{last['id']}/report", json={'actor': '张三', 'scanned': last['device']})
    assert r.status_code == 409


def test_scan_wrong_device_rejected(c):
    t = create(c, ['F2'])
    s = t['steps'][0]
    c.post('/sim/switch', json={'device': s['device'], 'state': 'OPEN', 'ticket': t['id']})
    r = c.post(f"/api/tickets/{t['id']}/steps/{s['id']}/report", json={'actor': '张三', 'scanned': 'CB999'})
    assert r.status_code == 400


# ---------------------------------------------------------------- 控制权唯一

def test_overlap_ticket_rejected(c):
    t1 = create(c, ['F2'])
    r = c.post('/api/tickets', json={'title': '重叠', 'work_area': ['F1'], 'created_by': '调度B'})
    assert r.status_code == 409 and t1['id'] in r.json()['detail']
    evs = c.get('/api/events').json()
    assert any(e['type'] == 'TICKET_REJECTED' for e in evs)


def test_locked_device_refuses_operation(c):
    t = create(c, ['F2'])
    r = c.post('/sim/switch', json={'device': 'SW103', 'state': 'CLOSED'})   # 无票据操作受控设备
    assert r.status_code == 423
    r = c.post('/sim/switch', json={'device': 'CB107', 'state': 'OPEN'})     # 未受控设备自由操作
    assert r.status_code == 200


# ---------------------------------------------------------------- 冻结

def test_freeze_on_telemetry_breach(c):
    t = permit(c, isolate_all(c, create(c, ['F2'])))
    # 模拟误操作：强制合上已隔离的开关
    r = c.post('/sim/switch', json={'device': 'SW103', 'state': 'CLOSED', 'force': True, 'actor': '误操作'})
    assert r.status_code == 200
    t = c.get(f"/api/tickets/{t['id']}").json()
    assert t['frozen'] == 1 and 'SW103' in t['freeze_reason']
    # 冻结期间禁止新增操作（收工也不行）
    assert c.post(f"/api/tickets/{t['id']}/finish-work", json={'actor': '调度A'}).status_code == 423
    # 已完成的物理步骤原样保留，未被回滚或伪造
    assert all(s['status'] == 'VERIFIED' for s in t['steps'] if s['phase'] == 'ISO')
    # 边界问题未消除不能解冻
    assert c.post(f"/api/tickets/{t['id']}/unfreeze", json={'actor': '调度A'}).status_code == 409
    # 恢复性操作（重新断开）→ 解冻 → 继续
    r = c.post('/sim/switch', json={'device': 'SW103', 'state': 'OPEN', 'ticket': t['id']})
    assert r.status_code == 200
    assert c.post(f"/api/tickets/{t['id']}/unfreeze", json={'actor': '调度A'}).status_code == 200
    t = c.post(f"/api/tickets/{t['id']}/finish-work", json={'actor': '调度A'}).json()
    assert t['status'] == 'RESTORING'


def test_freeze_on_topology_change_and_rebaseline(c):
    t = create(c, ['F2'])
    do_step(c, t, 1)                                  # 已完成断开 SW105（或排序后的第 1 个开关）
    r = c.post('/sim/topology', json={'op': 'add_edge', 'a': 'F1', 'b': 'F3'})
    assert r.status_code == 200
    t = c.get(f"/api/tickets/{t['id']}").json()
    assert t['frozen'] == 1 and '拓扑' in t['freeze_reason']
    # 冻结期间回报被禁止
    s2 = [s for s in t['steps'] if s['status'] == 'PENDING'][0]
    assert c.post(f"/api/tickets/{t['id']}/steps/{s2['id']}/report",
                  json={'actor': '张三', 'scanned': s2['device']}).status_code == 423
    # 重新基线：已复核步骤保留，拓扑快照更新，可解冻
    t = c.post(f"/api/tickets/{t['id']}/rebaseline", json={'actor': '调度A'}).json()
    assert t['snapshot'] == c.get('/api/topology').json()['version']
    kept = [s for s in t['steps'] if s['status'] == 'VERIFIED']
    assert len(kept) >= 1
    r = c.post(f"/api/tickets/{t['id']}/unfreeze", json={'actor': '调度A'})
    assert r.status_code == 200, r.text


def test_freeze_on_tag_removed(c):
    t = permit(c, isolate_all(c, create(c, ['F2'])))
    r = c.post('/sim/tag', json={'device': 'SW103', 'tag': f"禁止合闸·{t['id']}", 'action': 'remove'})
    assert r.status_code == 423                       # 系统标示牌受保护
    c.post('/sim/tag', json={'device': 'SW103', 'tag': f"禁止合闸·{t['id']}", 'action': 'remove', 'force': True})
    t = c.get(f"/api/tickets/{t['id']}").json()
    assert t['frozen'] == 1 and '标示牌' in t['freeze_reason']


def test_live_line_forbids_ground(c):
    r = c.post('/sim/ground', json={'point': 'F1', 'sw': 'SW103', 'action': 'install'})
    assert r.status_code == 409 and '带电' in r.json()['detail']


# ---------------------------------------------------------------- 幂等 / 离线 / 重启 / 撤回

def test_duplicate_report_idempotent(c):
    t = create(c, ['F2'])
    s = t['steps'][0]
    c.post('/sim/switch', json={'device': s['device'], 'state': 'OPEN', 'ticket': t['id']})
    body = {'actor': '张三', 'scanned': s['device'], 'key': 'k-1'}
    r1 = c.post(f"/api/tickets/{t['id']}/steps/{s['id']}/report", json=body)
    r2 = c.post(f"/api/tickets/{t['id']}/steps/{s['id']}/report", json=body)
    assert r1.json() == r2.json()                     # 重复提交返回首次结果
    t = c.get(f"/api/tickets/{t['id']}").json()
    assert t['steps'][0]['status'] == 'REPORTED'      # 状态只改变一次
    evs = c.get('/api/events').json()
    assert any(e['type'] == 'DUPLICATE_IGNORED' for e in evs)


def test_offline_sync(c):
    t = create(c, ['F2'])
    s1, s2 = t['steps'][0], t['steps'][1]
    c.post('/sim/switch', json={'device': s1['device'], 'state': 'OPEN', 'ticket': t['id']})
    # 离线缓存的操作乱序补传：服务端按客户端序号排序后幂等执行
    ops = [
        {'seq': 2, 'key': 'o2', 'op': 'verify', 'step': s1['id'], 'actor': '李四'},
        {'seq': 1, 'key': 'o1', 'op': 'report', 'step': s1['id'], 'actor': '张三', 'scanned': s1['device']},
    ]
    r = c.post(f"/api/tickets/{t['id']}/sync", json={'ops': ops})
    assert all(x['ok'] for x in r.json()['results']), r.text
    # 第 1 步复核完成后，现场操作第 2 个开关并再次离线补传
    r = c.post('/sim/switch', json={'device': s2['device'], 'state': 'OPEN', 'ticket': t['id']})
    assert r.status_code == 200, r.text
    ops2 = [{'seq': 3, 'key': 'o3', 'op': 'report', 'step': s2['id'], 'actor': '张三', 'scanned': s2['device']}]
    r = c.post(f"/api/tickets/{t['id']}/sync", json={'ops': ops + ops2})   # 重复补传 → 幂等
    assert all(x['ok'] for x in r.json()['results'])
    t = c.get(f"/api/tickets/{t['id']}").json()
    assert t['steps'][0]['status'] == 'VERIFIED' and t['steps'][1]['status'] == 'REPORTED'
    evs = c.get('/api/events').json()
    assert any(e['type'] == 'OFFLINE_SYNC' for e in evs)
    assert sum(1 for e in evs if e['type'] == 'STEP_REPORTED') == 2        # 重复补传未重复生效


def test_restart_keeps_state(tmp_path):
    db = str(tmp_path / 't.db')
    with TestClient(create_app(db)) as c:
        t = create(c, ['F2'])
        do_step(c, t, 1)
        tid = t['id']
        seq_before = max(e['seq'] for e in c.get('/api/events').json())
    # 主站重启：同一数据库重新拉起
    with TestClient(create_app(db)) as c2:
        t = c2.get(f'/api/tickets/{tid}').json()
        assert t['steps'][0]['status'] == 'VERIFIED'
        assert t['status'] == 'ISOLATING'
        # 控制权仍唯一：重叠开票仍被拒绝
        r = c2.post('/api/tickets', json={'title': 'x', 'work_area': ['F1'], 'created_by': 'B'})
        assert r.status_code == 409
        # 审计序号连续递增、无缺口
        c2.post('/sim/switch', json={'device': 'CB107', 'state': 'OPEN'})
        evs = c2.get('/api/events').json()
        assert max(e['seq'] for e in evs) > seq_before
        seqs = sorted(e['seq'] for e in evs)
        assert seqs == list(range(1, len(seqs) + 1))


def test_withdraw_keeps_physical_control(c):
    t = create(c, ['F2'])
    t = isolate_all(c, t)                             # 开关已断、接地已装（物理状态存在）
    r = c.post(f"/api/tickets/{t['id']}/withdraw", json={'actor': '调度A', 'reason': '计划取消'})
    assert r.status_code == 200
    retained = r.json()['retained_physical']
    assert set(retained) == {'SW103', 'CB104', 'SW105', 'GND:SW103', 'GND:CB104', 'GND:SW105'}
    # 有物理状态的设备控制权仍被锁定，重叠开票被拒
    r = c.post('/api/tickets', json={'title': 'x', 'work_area': ['F1'], 'created_by': 'B'})
    assert r.status_code == 409
    # 物理状态未恢复，不能清理控制权
    assert c.post(f"/api/tickets/{t['id']}/purge-control", json={'actor': '调度A'}).status_code == 409
    # 现场恢复物理状态（拆接地、开关复位）后清理
    for sw in ['SW105', 'CB104', 'SW103']:
        c.post('/sim/ground', json={'point': 'F2', 'sw': sw, 'action': 'remove', 'force': True})
    c.post('/sim/switch', json={'device': 'SW103', 'state': 'CLOSED', 'force': True})
    c.post('/sim/switch', json={'device': 'CB104', 'state': 'CLOSED', 'force': True})
    r = c.post(f"/api/tickets/{t['id']}/purge-control", json={'actor': '调度A'})
    assert r.status_code == 200, r.text
    t2 = create(c, ['F1'], title='重新开票')          # 控制权已完全释放
    assert t2['id']
    # 审计完整：撤回、清理均有记录且全局有序
    types = [e['type'] for e in c.get('/api/events').json()]
    assert 'TICKET_WITHDRAWN' in types and 'CONTROL_PURGED' in types


def test_withdraw_before_physical_steps_releases_all(c):
    t = create(c, ['F2'])
    r = c.post(f"/api/tickets/{t['id']}/withdraw", json={'actor': '调度A', 'reason': '误开票'})
    assert r.json()['retained_physical'] == []
    t2 = create(c, ['F2'], title='重开')              # 立即释放，可重开
    assert t2['id']


def test_audit_trail_complete(c):
    t = permit(c, isolate_all(c, create(c, ['F2'])))
    evs = c.get(f"/api/events?ticket={t['id']}").json()
    types = {e['type'] for e in evs}
    assert {'TICKET_CREATED', 'BOUNDARY_DERIVED', 'STEP_REPORTED', 'STEP_VERIFIED', 'PERMIT_GRANTED'} <= types
    seqs = sorted(e['seq'] for e in c.get('/api/events').json())
    assert seqs == list(range(1, len(seqs) + 1))      # 审计序号从 1 连续无缺口
