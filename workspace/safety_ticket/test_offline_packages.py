"""无网作业授权包端到端测试。

覆盖：
- 授权范围一次性下发（连续指令段/电网版本/执行者/监护者/截止时间/摘要）
- 断网本地证据链（扫码→实况→执行→监护，前一条摘要为父节点）
- 界面四态：未上传 PENDING / 待补前序 WAITING / 已采纳 ACCEPTED / 已驳回 REJECTED
- 缺口补齐后续传、整包重放幂等、整包事务只让唯一合法序列推进
- 在线与无网竞争：确定胜负与可追溯原因，负方不阻断后续物理序列
- 失效点：许可撤销、边界版本改变、重新定界、授权超时、成员替换、设备实况不符
- 已证实物理动作/设备占用/审计不抹除，重启恢复未决同步
"""
import time

import pytest
from fastapi.testclient import TestClient

from server import create_app, evidence_hash, RECORD_FIELDS

FUTURE = lambda ms=3600_000: int(time.time() * 1000) + ms


@pytest.fixture()
def c(tmp_path):
    app = create_app(str(tmp_path / 't.db'))
    with TestClient(app) as client:
        yield client


def make_ticket(c, work=('F2',)):
    r = c.post('/api/tickets', json={'title': '离线检修', 'work_area': list(work),
                                    'created_by': '调度A'})
    assert r.status_code == 201, r.text
    return r.json()


class Chain:
    """模拟移动终端：以前一条摘要为父节点，离线生成四拍证据。"""
    def __init__(self, pkg_id, executor='张三', guardian='李四'):
        self.pid = pkg_id
        self.executor = executor
        self.guardian = guardian
        self.records = []
        self.prev = 'GENESIS'

    def _add(self, rec, actor):
        rec = {'pkg': self.pid, 'seq': len(self.records) + 1, 'actor': actor,
               'scanned': None, 'device_state': None, 'grounded': None,
               'energized': None, **rec}
        payload = {k: rec[k] for k in RECORD_FIELDS}
        rec['prev_hash'] = self.prev
        rec['hash'] = evidence_hash('' if self.prev == 'GENESIS' else self.prev, payload)
        self.records.append(rec)
        self.prev = rec['hash']
        return rec

    def step(self, step_seq, step, actor_exec=None, actor_guard=None,
             live_override=None, ts_base=None, device=None):
        ex = actor_exec or self.executor
        gd = actor_guard or self.guardian
        t0 = ts_base if ts_base is not None else int(time.time() * 1000)
        scanned = device or step['device']
        if step['kind'] in ('INSTALL_GROUND', 'REMOVE_GROUND'):
            scanned = device or step['point']
        self._add({'type': 'SCAN', 'step': step_seq, 'ts': t0, 'scanned': scanned}, ex)
        live = {'type': 'LIVE', 'step': step_seq, 'ts': t0 + 1}
        if live_override:
            live.update(live_override)
        elif step['kind'] in ('OPEN_SWITCH', 'CLOSE_SWITCH'):
            live.update({'device_state': step['expected']})
        elif step['kind'] == 'INSTALL_GROUND':
            live.update({'grounded': True, 'energized': False})
        else:
            live.update({'grounded': False})
        self._add(live, ex)
        self._add({'type': 'EXEC', 'step': step_seq, 'ts': t0 + 2}, ex)
        self._add({'type': 'GUARD', 'step': step_seq, 'ts': t0 + 3}, gd)

    def by_step(self, step_seq):
        return [r for r in self.records if r['step'] == step_seq]


def physical(c, tid, pid, step, action_actor='张三'):
    """断网下持授权包执行一次物理操作（模拟设备）。"""
    if step['kind'] in ('OPEN_SWITCH', 'CLOSE_SWITCH'):
        r = c.post('/sim/switch', json={'device': step['device'], 'state': step['expected'],
                                        'pkg': pid, 'actor': action_actor})
    elif step['kind'] == 'INSTALL_GROUND':
        r = c.post('/sim/ground', json={'point': step['point'], 'sw': step['sw'],
                                        'action': 'install', 'ticket': tid, 'pkg': pid,
                                        'actor': action_actor})
    else:
        r = c.post('/sim/ground', json={'point': step['point'], 'sw': step['sw'],
                                        'action': 'remove', 'ticket': tid, 'pkg': pid,
                                        'actor': action_actor})
    assert r.status_code == 200, r.text
    return r


def issue(c, tid, frm, to, deadline=None, executor='张三', guardian='李四'):
    r = c.post(f'/api/tickets/{tid}/offline-packages',
               json={'seg_from': frm, 'seg_to': to, 'executor': executor,
                     'guardian': guardian, 'deadline': deadline or FUTURE(), 'issued_by': '调度A'})
    assert r.status_code == 201, r.text
    return r.json()


def submit(c, pid, records):
    r = c.post(f'/api/offline-packages/{pid}/submit', json={'records': records})
    assert r.status_code == 200, r.text
    return r.json()


def ticket(c, t):
    return c.get(f"/api/tickets/{t['id']}").json()


# ---------------------------------------------------------------- 授权下发

def test_issue_scope_and_hash(c):
    t = make_ticket(c)
    p = issue(c, t['id'], 1, 2)
    assert p['status'] == 'ACTIVE' and p['records_total'] == 8
    assert p['snapshot'] == 1 and p['executor'] == '张三' and p['guardian'] == '李四'
    assert p['scope_hash'] and p['sync_cursor'] == 0 and p['chain_continuous'] is False
    assert p['by_status']['PENDING'] == 8
    evs = c.get(f"/api/events?ticket={t['id']}").json()
    assert any(e['type'] == 'PKG_ISSUED' for e in evs)


def test_issue_rejects_overlap_and_bad_segment(c):
    t = make_ticket(c)
    issue(c, t['id'], 2, 3)
    r = c.post(f"/api/tickets/{t['id']}/offline-packages",
               json={'seg_from': 1, 'seg_to': 2, 'executor': '张三', 'guardian': '李四',
                     'deadline': FUTURE(), 'issued_by': '调度A'})
    assert r.status_code == 409                          # 授权范围重叠
    r = c.post(f"/api/tickets/{t['id']}/offline-packages",
               json={'seg_from': 1, 'seg_to': 99, 'executor': '张三', 'guardian': '李四',
                     'deadline': FUTURE(), 'issued_by': '调度A'})
    assert r.status_code == 404
    r = c.post(f"/api/tickets/{t['id']}/offline-packages",
               json={'seg_from': 1, 'seg_to': 2, 'executor': '张三', 'guardian': '张三',
                     'deadline': FUTURE(), 'issued_by': '调度A'})
    assert r.status_code == 400                          # 执行者=监护者


def test_issue_rejects_already_started_segment(c):
    t = make_ticket(c)
    s = t['steps'][0]
    c.post('/sim/switch', json={'device': s['device'], 'state': 'OPEN', 'ticket': t['id']})
    c.post(f"/api/tickets/{t['id']}/steps/{s['id']}/report",
           json={'actor': '张三', 'scanned': s['device']})
    r = c.post(f"/api/tickets/{t['id']}/offline-packages",
               json={'seg_from': 1, 'seg_to': 2, 'executor': '张三', 'guardian': '李四',
                     'deadline': FUTURE(), 'issued_by': '调度A'})
    assert r.status_code == 409


# ---------------------------------------------------------------- 断网造证与四态

def test_offline_chain_advances_only_legal_sequence(c):
    t = make_ticket(c)
    p = issue(c, t['id'], 1, 2)
    pid = p['id']
    steps = ticket(c, t)['steps']
    ch = Chain(pid)
    # 段内有序：未证实第1步前，物理操作第2步被闭锁
    physical(c, t['id'], pid, steps[0])
    r = c.post('/sim/switch', json={'device': steps[1]['device'], 'state': 'OPEN',
                                    'pkg': pid, 'actor': '张三'})
    assert r.status_code == 423
    ch.step(1, steps[0])
    v = submit(c, pid, ch.by_step(1))
    assert v['status'] == 'PARTIAL' and v['sync_cursor'] == 4
    assert all(r['status'] == 'ACCEPTED' for r in v['records'])
    # 第1步四拍采纳后，第2步物理解锁
    physical(c, t['id'], pid, steps[1])
    ch.step(2, steps[1], ts_base=int(time.time() * 1000))
    v = submit(c, pid, ch.by_step(2))
    assert v['status'] == 'COMPLETED' and v['sync_cursor'] == 8
    tt = ticket(c, t)
    assert [s['status'] for s in tt['steps'][:2]] == ['VERIFIED', 'VERIFIED']
    assert tt['steps'][0]['reporter'] == '张三' and tt['steps'][0]['verifier'] == '李四'
    # 审计标明证据来自无网授权包
    payloads = [e for e in c.get(f"/api/events?ticket={t['id']}").json()
                if e['type'] in ('STEP_REPORTED', 'STEP_VERIFIED')]
    assert all('OFFLINE_PACKAGE' in e['payload'] for e in payloads)


def test_gap_marks_waiting_then_later_prefix_wakes(c):
    t = make_ticket(c)
    p = issue(c, t['id'], 1, 1)
    pid = p['id']
    steps = ticket(c, t)['steps']
    physical(c, t['id'], pid, steps[0])
    ch = Chain(pid)
    ch.step(1, steps[0])
    # 只传第 3、4 条（执行/监护）：父节点缺口 → WAITING（待补前序），不能跨越
    v = submit(c, pid, [ch.records[2], ch.records[3]])
    st = {r['seq']: r['status'] for r in v['records']}
    assert st == {3: 'WAITING', 4: 'WAITING'}
    assert v['by_status']['WAITING'] == 2 and v['first_gap'] == 1
    # 后到的前序补齐：整包重放，唤醒暂存片段
    v = submit(c, pid, ch.records)
    st = {r['seq']: r['status'] for r in v['records']}
    assert st == {i: 'ACCEPTED' for i in range(1, 5)}
    assert v['status'] == 'COMPLETED'


def test_replay_is_idempotent_no_second_change(c):
    t = make_ticket(c)
    p = issue(c, t['id'], 1, 1)
    pid = p['id']
    steps = ticket(c, t)['steps']
    physical(c, t['id'], pid, steps[0])
    ch = Chain(pid)
    ch.step(1, steps[0])
    submit(c, pid, ch.records)
    v1 = submit(c, pid, ch.records)                 # 整包重放
    v2 = submit(c, pid, ch.records)
    assert all(x['duplicate'] for x in v1['received'])
    assert [x['status'] for x in v2['received']] == ['ACCEPTED'] * 4
    evs = c.get(f"/api/events?ticket={t['id']}").json()
    assert sum(1 for e in evs if e['type'] == 'STEP_REPORTED') == 1   # 无第二次状态变化
    assert sum(1 for e in evs if e['type'] == 'STEP_VERIFIED') == 1


def test_tampered_hash_rejected_and_blocks_followers(c):
    t = make_ticket(c)
    p = issue(c, t['id'], 1, 1)
    steps = ticket(c, t)['steps']
    physical(c, t['id'], p['id'], steps[0])
    ch = Chain(p['id'])
    ch.step(1, steps[0])
    # (a) 内容被改但不重算摘要 → 提交端即拒绝（HASH_MISMATCH）
    bad = [dict(r) for r in ch.records]
    bad[0]['scanned'] = 'CB999'
    r = c.post(f"/api/offline-packages/{p['id']}/submit", json={'records': bad})
    assert r.status_code == 400
    # (b) 链指针被改（指向不存在的父摘要）但自洽 → 裁决驳回且后续级联，绝不跳过
    evil = [dict(r) for r in ch.records]
    evil[0]['prev_hash'] = 'DEADBEEF'
    evil[0]['hash'] = evidence_hash('DEADBEEF', {k: evil[0][k] for k in RECORD_FIELDS})
    v = submit(c, p['id'], evil)
    assert v['records'][0]['status'] == 'REJECTED'
    assert all(r['status'] == 'REJECTED' for r in v['records'])
    tt = ticket(c, t)
    assert tt['steps'][0]['status'] == 'PENDING'                     # 未推进任何工序


def test_conflicting_duplicate_loses_deterministically(c):
    t = make_ticket(c)
    p = issue(c, t['id'], 1, 1)
    steps = ticket(c, t)['steps']
    physical(c, t['id'], p['id'], steps[0])
    ch = Chain(p['id'])
    ch.step(1, steps[0])
    first = dict(ch.records[0])
    submit(c, p['id'], [first])
    # 同序号、不同内容但哈希自洽的后到记录 → 先到为准，后到 DUP_CONFLICT
    evil = {k: first.get(k) for k in RECORD_FIELDS}
    evil['ts'] = evil['ts'] + 999                   # 内容冲突（时间戳不同）
    evil['hash'] = evidence_hash('', evil)
    evil['prev_hash'] = 'GENESIS'
    v = submit(c, p['id'], [evil])
    assert v['received'][0]['status'] == 'DUP_CONFLICT'
    # 先到记录仍保持首次裁决，未被覆盖
    pv = c.get(f"/api/offline-packages/{p['id']}").json()
    assert pv['records'][0]['payload']['scanned'] == first['scanned']


# ---------------------------------------------------------------- 在线 / 无网竞争

def test_online_race_win_is_traceable_and_chain_continues(c):
    t = make_ticket(c)
    p = issue(c, t['id'], 1, 2)
    pid = p['id']
    steps = ticket(c, t)['steps']
    # 在线先把第1步完整做掉（在线操作，无授权包）
    c.post('/sim/switch', json={'device': steps[0]['device'], 'state': 'OPEN', 'ticket': t['id']})
    c.post(f"/api/tickets/{t['id']}/steps/{steps[0]['id']}/report",
           json={'actor': '王五', 'scanned': steps[0]['device']})
    c.post(f"/api/tickets/{t['id']}/steps/{steps[0]['id']}/verify",
           json={'actor': '赵六'})
    # 无网包随后补传：第1步记录竞争负（在线先提交），第2步由包正常推进
    ch = Chain(pid)
    ch.step(1, steps[0])                            # 其 EXEC/GUARD 竞争负
    physical(c, t['id'], pid, steps[1])
    ch.step(2, steps[1])
    v = submit(c, pid, ch.records)
    reasons = {r['seq']: (r['status'], r['reason'] or '') for r in v['records']}
    assert reasons[3][0] == 'REJECTED' and '竞争裁决：在线操作胜' in reasons[3][1]
    assert '审计 #' in reasons[3][1]                # 可追溯的胜负原因
    assert reasons[7][0] == 'ACCEPTED' and reasons[8][0] == 'ACCEPTED'  # 后续不阻断
    assert v['status'] == 'COMPLETED' and v['race_lost'] >= 2
    assert v['sync_cursor'] == 8


def test_online_finishes_all_package_superseded(c):
    t = make_ticket(c)
    p = issue(c, t['id'], 1, 1)
    pid = p['id']
    steps = ticket(c, t)['steps']
    for i in range(1):
        c.post('/sim/switch', json={'device': steps[i]['device'], 'state': 'OPEN', 'ticket': t['id']})
        c.post(f"/api/tickets/{t['id']}/steps/{steps[i]['id']}/report",
               json={'actor': '王五', 'scanned': steps[i]['device']})
        c.post(f"/api/tickets/{t['id']}/steps/{steps[i]['id']}/verify",
               json={'actor': '赵六'})
    ch = Chain(pid)
    ch.step(1, steps[0])
    v = submit(c, pid, ch.records)
    assert v['status'] == 'SUPERSEDED' and '先提交者胜' in v['void_reason']
    exec_guard = [r for r in v['records'] if r['payload']['type'] in ('EXEC', 'GUARD')]
    assert exec_guard and all(r['status'] == 'REJECTED' for r in exec_guard)


# ---------------------------------------------------------------- 失效点

def test_withdraw_voids_package_but_keeps_physical_lock(c):
    t = make_ticket(c)
    p = issue(c, t['id'], 1, 2)
    pid = p['id']
    steps = ticket(c, t)['steps']
    physical(c, t['id'], pid, steps[0])
    ch = Chain(pid)
    ch.step(1, steps[0])
    submit(c, pid, ch.by_step(1))                    # 第1步已采纳（物理动作已证实）
    # 撤销许可
    r = c.post(f"/api/tickets/{t['id']}/withdraw", json={'actor': '调度A', 'reason': '取消'})
    assert r.status_code == 200
    assert steps[0]['device'] in r.json()['retained_physical']     # 现场残留继续锁定
    # 补传第2步：许可失效，旧片段拒收
    physical(c, t['id'], pid, steps[1])
    ch.step(2, steps[1])
    v = submit(c, pid, ch.by_step(2))
    assert v['status'] == 'VOID'
    step2 = [r for r in v['records'] if r['payload']['step'] == 2]
    assert step2 and all(r['status'] == 'REJECTED' and '许可单失效' in (r['reason'] or '')
                         for r in step2)
    # 已采纳的第1步、物理动作、审计均未抹除
    earlier = submit(c, pid, ch.by_step(1))
    earlier_s1 = [r for r in earlier['records'] if r['payload']['step'] == 1]
    assert len(earlier_s1) == 4 and all(r['status'] == 'ACCEPTED' for r in earlier_s1)
    tt = ticket(c, t)
    assert tt['steps'][0]['status'] == 'VERIFIED'
    ctrl = c.get('/api/devices/' + steps[0]['device']).json()
    assert ctrl['controlled_by'] == t['id']
    # 重叠开票仍被残留锁定拒绝（CB104 是 T1 检修的边界开关，仍被锁定）
    assert c.post('/api/tickets', json={'title': 'x', 'work_area': ['T1'],
                                        'created_by': 'B'}).status_code == 409


def test_boundary_version_change_voids_packages(c):
    t = make_ticket(c)
    p = issue(c, t['id'], 1, 1)
    assert c.post('/sim/topology', json={'op': 'add_edge', 'a': 'F1', 'b': 'F3'}).status_code == 200
    steps = ticket(c, t)['steps']
    ch = Chain(p['id'])
    ch.step(1, steps[0])
    v = submit(c, p['id'], ch.records)
    assert v['status'] == 'VOID' and '边界版本改变' in v['void_reason']
    assert all(r['status'] == 'REJECTED' for r in v['records'])


def test_rebaseline_voids_old_authorization(c):
    t = make_ticket(c)
    c.post('/sim/topology', json={'op': 'add_edge', 'a': 'F1', 'b': 'F3'})
    assert ticket(c, t)['frozen'] == 1
    # 在新拓扑版本下下发的授权，随后重新定界 → 同样作废（旧授权范围已不再适用）
    p = issue(c, t['id'], 1, 2)
    r = c.post(f"/api/tickets/{t['id']}/rebaseline", json={'actor': '调度A'})
    assert r.status_code == 200, r.text
    pv = c.get(f"/api/offline-packages/{p['id']}").json()
    assert pv['status'] == 'VOID' and '重新定界' in pv['void_reason']


def test_expired_authorization_rejects(c):
    now = int(time.time() * 1000)
    t = make_ticket(c)
    p = issue(c, t['id'], 1, 1, deadline=now + 1000)
    pid = p['id']
    steps = ticket(c, t)['steps']
    physical(c, t['id'], pid, steps[0])
    ch = Chain(pid)
    ch.step(1, steps[0], ts_base=now + 2000)         # 证据时间晚于截止
    v = submit(c, pid, ch.records)
    assert v['status'] == 'EXPIRED'
    assert all(r['status'] == 'REJECTED' and '超时' in (r['reason'] or '') for r in v['records'])
    assert ticket(c, t)['steps'][0]['status'] == 'PENDING'


def test_member_substitution_rejects_old_fragments(c):
    t = make_ticket(c)
    p = issue(c, t['id'], 1, 1)
    steps = ticket(c, t)['steps']
    physical(c, t['id'], p['id'], steps[0])
    ch = Chain(p['id'])
    ch.step(1, steps[0], actor_exec='孙七')           # 执行者被替换
    v = submit(c, p['id'], ch.records)
    ex = next(r for r in v['records'] if r['seq'] == 3)
    assert ex['status'] == 'REJECTED' and '执行者不符' in ex['reason']
    assert v['records'][3]['status'] == 'REJECTED'   # 后续级联


def test_live_mismatch_rejects(c):
    t = make_ticket(c)
    p = issue(c, t['id'], 1, 1)
    steps = ticket(c, t)['steps']
    # 现场设备实际在分位，但 LIVE 证据谎报合闸位置
    physical(c, t['id'], p['id'], steps[0])
    ch = Chain(p['id'])
    ch.step(1, steps[0], live_override={'device_state': 'CLOSED'})
    v = submit(c, p['id'], ch.records)
    live = v['records'][1]
    assert live['status'] == 'REJECTED' and '设备实况不符' in live['reason']
    assert ticket(c, t)['steps'][0]['status'] == 'PENDING'


# ---------------------------------------------------------------- 持久化 / 重启

def test_restart_recovers_pending_sync(tmp_path):
    db = str(tmp_path / 't.db')
    with TestClient(create_app(db)) as c1:
        t = make_ticket(c1)
        p = issue(c1, t['id'], 1, 1)
        pid = p['id']
        steps = ticket(c1, t)['steps']
        physical(c1, t['id'], pid, steps[0])
        ch = Chain(pid)
        ch.step(1, steps[0])
        # 只补传前两条（扫码+实况），服务重启
        v = submit(c1, pid, ch.records[:2])
        assert v['by_status']['ACCEPTED'] == 2
        tid = t['id']
        recs = ch.records
    with TestClient(create_app(db)) as c2:
        pv = c2.get(f'/api/offline-packages/{pid}').json()
        assert pv['sync_cursor'] == 2 and pv['status'] == 'PARTIAL'
        # 重启后继续补传执行/监护，未决同步恢复推进
        v = submit(c2, pid, recs[2:])
        assert v['status'] == 'COMPLETED'
        tt = c2.get(f'/api/tickets/{tid}').json()
        assert tt['steps'][0]['status'] == 'VERIFIED'


def test_cannot_operate_without_valid_package(c):
    t = make_ticket(c)
    steps = ticket(c, t)['steps']
    # 无授权包、无票据的物理操作 → 闭锁
    r = c.post('/sim/switch', json={'device': steps[0]['device'], 'state': 'OPEN'})
    assert r.status_code == 423
    # 持不存在的授权包
    r = c.post('/sim/switch', json={'device': steps[0]['device'], 'state': 'OPEN',
                                    'pkg': 'PNOPE', 'actor': '张三'})
    assert r.status_code in (404, 423)
