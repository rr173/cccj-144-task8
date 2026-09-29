"""审计事件回放与事故分析：六条路径端到端测试。

覆盖：正常重建 / 缺号停止 / 摘要错误拒绝 / 假设分支隔离 / 并发编辑冲突 / 重启恢复，
以及回放固定（新增事件不改变既有回放）、载荷无法应用停止、报告重复生成同一结果。
"""
import pytest
from fastapi.testclient import TestClient
from server import create_app


@pytest.fixture()
def c(tmp_path):
    app = create_app(str(tmp_path / 't.db'))
    with TestClient(app) as client:
        yield client


def prepare(c, work=('F1',)):
    r = c.post('/api/demo/replay/prepare', json={'work_area': list(work)})
    assert r.status_code == 201, r.text
    return r.json()


def make_replay(c, tid, from_seq, to_seq):
    r = c.post(f'/api/tickets/{tid}/replays',
               json={'from_seq': from_seq, 'to_seq': to_seq, 'created_by': '分析员甲'})
    assert r.status_code == 201, r.text
    return r.json()


def first_seq_of(c, replay, etype):
    for e in replay['entries']:
        if e['type'] == etype:
            return e['seq']
    raise AssertionError(f'回放中没有 {etype} 事件')


# ---------------------------------------------------------------- 路径一：正常重建

def test_replay_normal_reconstruction(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    assert rp['status'] == 'OK' and rp['halt'] is None
    assert rp['source_digest']
    # 逐条显示：窗口内每个事件都有前后状态
    assert len(rp['entries']) == p['to_seq'] - p['from_seq'] + 1
    for e in rp['entries']:
        if e['applied']:
            assert e['before'] is not None and e['after'] is not None
    # 上界处重建出票据阶段 / 设备控制权 / 现场证据
    tip = rp['tip']
    assert tip['status'] == 'PERMITTED'
    assert tip['write_owner'] == '甲班'
    assert tip['control'], '应重建出设备控制权'
    assert set(tip['control']) >= {'CB102', 'SW103', 'GND:CB102', 'GND:SW103'}
    assert all(v == 1 for k, v in tip['control'].items() if k.startswith('GND:'))
    assert tip['evidence']['shift_accepted'] == 2
    assert all(s['status'] == 'VERIFIED' for s in tip['steps'].values())
    # 事件前后状态逐步推进：STEP_VERIFIED 之后该步骤为 VERIFIED
    seq_v = first_seq_of(c, rp, 'STEP_VERIFIED')
    entry = next(e for e in rp['entries'] if e['seq'] == seq_v)
    sid = entry['after']['steps'] and [k for k in entry['after']['steps']
                                       if entry['after']['steps'][k].get('status') == 'VERIFIED']
    assert sid and entry['before']['steps'].get(sid[0], {}).get('status') != 'VERIFIED'


def test_replay_baseline_folding(c):
    """窗口下界之前的事件折叠为基线投影，窗口内逐条展示。"""
    p = prepare(c)
    mid = p['from_seq'] + 5
    rp = make_replay(c, p['ticket'], mid, p['to_seq'])
    assert rp['status'] == 'OK'
    assert len(rp['entries']) == p['to_seq'] - mid + 1
    assert rp['baseline']['status'] in ('ISOLATING', 'PERMITTED')
    assert rp['baseline']['steps'], '基线应已应用窗口前的事件'
    assert rp['entries'][0]['seq'] == mid
    assert rp['entries'][0]['before'] == rp['baseline']


def test_replay_pinned_against_new_events(c):
    """回放固定事件上界与来源摘要：后续新增事件不得改变既有回放。"""
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    # 之后票据上继续产生新事件（序号越过上界）
    r = c.post(f"/api/tickets/{p['ticket']}/shift-evidences",
               json={'client_id': 'after:1', 'team': '甲班', 'ts_data': 10 ** 13,
                     'kind': '新增证据'})
    assert r.status_code == 200, r.text
    again = c.get(f"/api/replays/{rp['id']}").json()
    assert again['source_digest'] == rp['source_digest']
    assert again['entries'] == rp['entries']
    assert again['tip'] == rp['tip']
    # 重新核验也不变（新增事件序号越过上界，不进入固定来源）
    rv = c.post(f"/api/replays/{rp['id']}/reverify").json()
    assert rv['status'] == 'OK' and rv['source_digest'] == rp['source_digest']


def test_replay_range_validation(c):
    p = prepare(c)
    r = c.post(f"/api/tickets/{p['ticket']}/replays",
               json={'from_seq': 9, 'to_seq': 3})
    assert r.status_code == 400
    r = c.post(f"/api/tickets/{p['ticket']}/replays",
               json={'from_seq': 1, 'to_seq': 10 ** 9})
    assert r.status_code == 400 and '上界' in r.json()['detail']
    r = c.post('/api/tickets/NOPE/replays', json={'from_seq': 1, 'to_seq': 2})
    assert r.status_code == 404


# ---------------------------------------------------------------- 路径二：缺号停止

def test_replay_gap_halt(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    victim = first_seq_of(c, rp, 'STEP_REPORTED')
    r = c.post('/api/demo/tamper', json={'seq': victim, 'mode': 'delete'})
    assert r.status_code == 200
    rv = c.post(f"/api/replays/{rp['id']}/reverify").json()
    assert rv['status'] == 'HALTED'
    assert rv['halt']['code'] == 'GAP'
    assert rv['halt']['position'] == victim          # 指出具体位置
    assert rv['halt']['at_seq'] == victim - 1        # 停在最后一个有效事件
    assert all(e['seq'] <= victim - 1 for e in rv['entries'])
    assert '缺口' in rv['halt']['detail']


def test_replay_gap_at_creation(c):
    """创建时窗口已有缺口：同样停在最后一个有效事件，不拼出看似完整的状态。"""
    p = prepare(c)
    victim = p['from_seq'] + 2
    c.post('/api/demo/tamper', json={'seq': victim, 'mode': 'delete'})
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    assert rp['status'] == 'HALTED' and rp['halt']['code'] == 'GAP'
    assert rp['halt']['position'] == victim
    assert all(e['seq'] < victim for e in rp['entries'])


# ---------------------------------------------------------------- 路径三：摘要错误拒绝

def test_replay_digest_mismatch(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    victim = first_seq_of(c, rp, 'STEP_VERIFIED')
    c.post('/api/demo/tamper', json={'seq': victim, 'mode': 'corrupt'})
    rv = c.post(f"/api/replays/{rp['id']}/reverify").json()
    assert rv['status'] == 'HALTED'
    assert rv['halt']['code'] == 'DIGEST_MISMATCH'
    assert rv['halt']['position'] == victim
    assert rv['halt']['at_seq'] < victim
    assert '摘要' in rv['halt']['detail']
    # 来源摘要本身不变——它固定于创建时刻
    assert rv['source_digest'] == rp['source_digest']


def test_replay_apply_failed(c):
    """事件载荷无法应用：回放停在最后一个有效事件并指出位置。"""
    p = prepare(c)
    victim = p['from_seq'] + 3
    c.post('/api/demo/tamper', json={'seq': victim, 'mode': 'break_payload'})
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    assert rp['status'] == 'HALTED'
    assert rp['halt']['code'] == 'APPLY_FAILED'
    assert rp['halt']['position'] == victim
    assert rp['halt']['at_seq'] == victim - 1
    assert all(e['seq'] <= victim - 1 for e in rp['entries'])


# ---------------------------------------------------------------- 路径四：假设分支隔离

def make_branch(c, replay, base_seq, name='假设分支'):
    r = c.post(f"/api/replays/{replay['id']}/branches",
               json={'name': name, 'base_seq': base_seq, 'created_by': '分析员乙'})
    assert r.status_code == 201, r.text
    return r.json()


def test_branch_isolation_undo_and_device(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    n_events_before = len(c.get('/api/events', params={'limit': 500}).json())
    base = rp['entries'][2]['seq']
    br = make_branch(c, rp, base)
    assert br['version'] == 0
    undo_seq = first_seq_of(c, rp, 'STEP_VERIFIED')
    # 试算一：撤销一条复核事件 → 工序状态与控制权差异
    r = c.post(f"/api/branches/{br['id']}/ops",
               json={'op_type': 'UNDO_EVENT', 'params': {'seq': undo_seq},
                     'actor': '分析员乙', 'base_version': 0})
    assert r.status_code == 201, r.text
    br = r.json()
    step_diffs = [d for d in br['diff'] if d['aspect'] == '工序状态']
    assert step_diffs and step_diffs[0]['official'] == 'VERIFIED'
    assert step_diffs[0]['hypothetical'] != 'VERIFIED'
    assert any(d['aspect'] == '控制权' for d in br['diff'])
    # 试算二：在上界处变更设备状态（合上已断开的边界开关）→ 工作区复电告警
    br2 = make_branch(c, rp, rp['applied'][-1], name='误合闸假设')
    r = c.post(f"/api/branches/{br2['id']}/ops",
               json={'op_type': 'DEVICE_STATE',
                     'params': {'device': 'CB102', 'state': 'CLOSED'},
                     'actor': '分析员乙', 'base_version': 0})
    assert r.status_code == 201, r.text
    br2 = r.json()
    assert br2['version'] == 1
    assert any(d['aspect'] == '设备状态' and d['target'] == 'CB102' for d in br2['diff'])
    assert br2['safety']['work_area_energized'], '假设合闸后工作区应复电'
    # 隔离性：正式票据与正式审计事件不受分支影响
    t = c.get(f"/api/tickets/{p['ticket']}").json()
    assert t['status'] == 'PERMITTED'
    assert all(s['status'] == 'VERIFIED' for s in t['steps'])
    dev = c.get('/api/devices/CB102').json()
    assert dev['state'] == 'OPEN', '分支试算不得改变正式设备状态'
    n_events_after = len(c.get('/api/events', params={'limit': 500}).json())
    assert n_events_after == n_events_before, '分支操作不得追加正式审计事件'


def test_branch_base_must_be_valid_event(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    r = c.post(f"/api/replays/{rp['id']}/branches",
               json={'name': 'x', 'base_seq': 10 ** 8})
    assert r.status_code == 400
    # 在停止的回放上：基点不得越过最后一个有效事件
    victim = first_seq_of(c, rp, 'STEP_REPORTED')
    c.post('/api/demo/tamper', json={'seq': victim, 'mode': 'delete'})
    rv = c.post(f"/api/replays/{rp['id']}/reverify").json()
    beyond = rv['halt']['at_seq'] + 1
    r = c.post(f"/api/replays/{rp['id']}/branches",
               json={'name': 'x', 'base_seq': beyond})
    assert r.status_code == 400
    r = c.post(f"/api/replays/{rp['id']}/branches",
               json={'name': 'x', 'base_seq': rv['halt']['at_seq']})
    assert r.status_code == 201, '最后一个有效事件可以作为分支基点'


def test_branch_undo_validation(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    br = make_branch(c, rp, rp['entries'][0]['seq'])
    r = c.post(f"/api/branches/{br['id']}/ops",
               json={'op_type': 'UNDO_EVENT', 'params': {'seq': 10 ** 8},
                     'base_version': 0})
    assert r.status_code == 400
    r = c.post(f"/api/branches/{br['id']}/ops",
               json={'op_type': 'DEVICE_STATE', 'params': {'device': 'CB102', 'state': 'BAD'},
                     'base_version': 0})
    assert r.status_code == 400
    r = c.post(f"/api/branches/{br['id']}/ops",
               json={'op_type': 'WHAT', 'params': {}, 'base_version': 0})
    assert r.status_code == 400


# ---------------------------------------------------------------- 路径五：并发编辑冲突

def test_branch_version_conflict(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    br = make_branch(c, rp, rp['entries'][1]['seq'])
    undo_seq = first_seq_of(c, rp, 'STEP_VERIFIED')
    op1 = {'op_type': 'UNDO_EVENT', 'params': {'seq': undo_seq},
           'actor': '分析员甲', 'base_version': 0}
    op2 = {'op_type': 'DEVICE_STATE', 'params': {'device': 'SW103', 'state': 'CLOSED'},
           'actor': '分析员乙', 'base_version': 0}
    # 两人同时基于 v0 提交：先者生效，后者收到确定冲突，已保存假设不被覆盖
    r1 = c.post(f"/api/branches/{br['id']}/ops", json=op1)
    assert r1.status_code == 201
    r2 = c.post(f"/api/branches/{br['id']}/ops", json=op2)
    assert r2.status_code == 409
    assert 'v1' in r2.json()['detail']
    saved = c.get(f"/api/branches/{br['id']}").json()
    assert saved['version'] == 1 and len(saved['ops']) == 1
    assert saved['ops'][0]['op_type'] == 'UNDO_EVENT', '冲突提交不得覆盖已保存的假设'
    # 负方刷新后基于最新版本重试成功
    r3 = c.post(f"/api/branches/{br['id']}/ops",
                json={**op2, 'base_version': saved['version']})
    assert r3.status_code == 201
    assert r3.json()['version'] == 2 and len(r3.json()['ops']) == 2


# ---------------------------------------------------------------- 路径六：重启恢复

def test_restart_recovery(c, tmp_path):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    # 制造校验失败（缺号），让回放停在失败位置
    victim = first_seq_of(c, rp, 'STEP_REPORTED')
    c.post('/api/demo/tamper', json={'seq': victim, 'mode': 'delete'})
    rv = c.post(f"/api/replays/{rp['id']}/reverify").json()
    assert rv['status'] == 'HALTED'
    br = make_branch(c, rv, rv['halt']['at_seq'])
    c.post(f"/api/branches/{br['id']}/ops",
           json={'op_type': 'UNDO_EVENT', 'params': {'seq': p['from_seq']},
                 'base_version': 0})
    rep = c.post(f"/api/replays/{rp['id']}/reports",
                 json={'title': '事故分析', 'branch_ids': [br['id']], 'key': 'rk'}).json()
    # 进程级重启：同一数据库文件重建应用
    db_path = c.app.state.db.path
    with TestClient(create_app(db_path)) as c2:
        rp2 = c2.get(f"/api/replays/{rp['id']}").json()
        assert rp2['status'] == 'HALTED'
        assert rp2['halt'] == rv['halt'], '校验失败位置必须在重启后恢复'
        assert rp2['entries'] == rv['entries'], '回放游标（时间线）必须在重启后恢复'
        br2 = c2.get(f"/api/branches/{br['id']}").json()
        assert br2['version'] == 1 and len(br2['ops']) == 1, '假设分支必须在重启后恢复'
        assert br2['diff'] == c.get(f"/api/branches/{br['id']}").json()['diff']
        rep2 = c2.get(f"/api/reports/{rep['id']}").json()
        assert rep2['report_hash'] == rep['report_hash']
        assert rep2['content'] == rep['content']


# ---------------------------------------------------------------- 事故分析报告

def test_report_fixed_source_and_idempotent(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    br = make_branch(c, rp, rp['entries'][2]['seq'])
    undo_seq = first_seq_of(c, rp, 'STEP_VERIFIED')
    c.post(f"/api/branches/{br['id']}/ops",
           json={'op_type': 'UNDO_EVENT', 'params': {'seq': undo_seq}, 'base_version': 0})
    rep = c.post(f"/api/replays/{rp['id']}/reports",
                 json={'title': '事故分析报告', 'branch_ids': [br['id']],
                       'key': 'report-key-1', 'created_by': '分析员丙'}).json()
    content = rep['content']
    # 报告保留固定来源摘要、事件范围与分支差异
    assert content['source_digest'] == rp['source_digest']
    assert content['event_range'] == {'from_seq': p['from_seq'], 'to_seq': p['to_seq']}
    assert content['replay_status'] == 'OK'
    assert len(content['branches']) == 1
    assert content['branches'][0]['diff'], '报告必须保留分支差异'
    # 同一幂等键重复生成 → 同一结果
    again = c.post(f"/api/replays/{rp['id']}/reports",
                   json={'title': '事故分析报告', 'branch_ids': [br['id']],
                         'key': 'report-key-1'}).json()
    assert again['id'] == rep['id'] and again.get('duplicate')
    assert again['report_hash'] == rep['report_hash']
    # 显式重复生成 → 同一结果
    regen = c.post(f"/api/reports/{rep['id']}/regenerate").json()
    assert regen['identical'] and regen['integrity_ok']
    assert regen['report']['content'] == content
    # 报告生成后新增事件不改变报告
    c.post(f"/api/tickets/{p['ticket']}/shift-evidences",
           json={'client_id': 'later:1', 'team': '甲班', 'ts_data': 10 ** 13, 'kind': '补充'})
    rep2 = c.get(f"/api/reports/{rep['id']}").json()
    assert rep2['content'] == content


def test_report_branch_must_belong_to_replay(c):
    p = prepare(c)
    rp1 = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    p2 = prepare(c, work=('F3',))
    rp2 = make_replay(c, p2['ticket'], p2['from_seq'], p2['to_seq'])
    br = make_branch(c, rp2, rp2['entries'][0]['seq'])
    r = c.post(f"/api/replays/{rp1['id']}/reports",
               json={'title': 'x', 'branch_ids': [br['id']]})
    assert r.status_code == 400
