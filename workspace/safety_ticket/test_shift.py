"""值守班组换班接续端到端测试。

覆盖：
- 交班单开具校验（移交方须持权/候选合法/期限未来/水位不低于已入账/一票一单）
- 签收前移交方续送入账、承接方首批数据挂起；签收原子转移写权并唤醒挂起
- 切换水位归属：移交方越线拒收、水位线内合法迟到入账、承接方回填旧历元拒收
- 承接方首条接不上旧链只能挂起，修正基点重传后入账；同 client_id 内容冲突拒绝
- 重复签收幂等；两个候选班组争抢签收只有一方取得写权
- 调度撤单 / 工作票锁定 / 边界重算（拓扑变更）/ 计划取消 / 超时关闭未完成换班
- 既成设备动作与控制锁不回退；进程恢复后写权归属与挂起内容与故障前相同
"""
import threading
import time

import pytest
from fastapi.testclient import TestClient

from server import create_app

NOW = lambda: int(time.time() * 1000)


@pytest.fixture()
def c(tmp_path):
    app = create_app(str(tmp_path / 't.db'))
    with TestClient(app) as client:
        yield client


def prepare(c, team='甲班'):
    """一张隔离完成、许可开工的在执行工作票（持权班组 team）。"""
    r = c.post('/api/demo/shift/prepare', json={'work_area': ['F1'], 'team': team})
    assert r.status_code == 201, r.text
    return r.json()['ticket']['id']


def mk_handover(c, tid, cands=('乙班',), watermark=None, deadline=None, expect=201, **kw):
    body = {'watermark': watermark if watermark is not None else NOW(),
            'candidates': list(cands),
            'deadline': deadline if deadline is not None else NOW() + 600_000}
    body.update(kw)
    r = c.post(f'/api/tickets/{tid}/handovers', json=body)
    assert r.status_code == expect, r.text
    return r.json()


def send(c, tid, team, ts, base=None, payload='巡视', device='F1'):
    body = {'client_id': f'C-{team}-{ts}-{payload}-{time.time_ns()}', 'team': team,
            'device': device, 'kind': '巡检记录', 'ts_data': ts, 'payload': payload}
    if base is not None:
        body['base_seq'] = base
    r = c.post(f'/api/tickets/{tid}/shift-evidences', json=body)
    assert r.status_code == 200, r.text
    return r.json()


def send_id(c, tid, team, ts, client_id, base=None, payload='巡视', expect=200):
    body = {'client_id': client_id, 'team': team, 'device': 'F1', 'kind': '巡检记录',
            'ts_data': ts, 'payload': payload}
    if base is not None:
        body['base_seq'] = base
    r = c.post(f'/api/tickets/{tid}/shift-evidences', json=body)
    assert r.status_code == expect, r.text
    return r.json()


def sign(c, hid, team, expect=200):
    r = c.post(f'/api/handovers/{hid}/sign', json={'team': team})
    assert r.status_code == expect, r.text
    return r.json()


def view(c, tid):
    r = c.get(f'/api/tickets/{tid}/shift')
    assert r.status_code == 200, r.text
    return r.json()


# ---------------------------------------------------------------- 开具校验

def test_handover_create_validation(c):
    tid = prepare(c)
    # 移交方必须持权
    r = mk_handover(c, tid, expect=409, from_team='乙班')
    assert '持权班组' in r['detail']
    # 候选为空 / 候选含移交方 / 候选重复
    assert '候选' in mk_handover(c, tid, cands=(), expect=400)['detail']
    assert '移交方' in mk_handover(c, tid, cands=('甲班',), expect=400)['detail']
    assert '重复' in mk_handover(c, tid, cands=('乙班', '乙班'), expect=400)['detail']
    # 签收期限必须在未来
    assert '签收期限' in mk_handover(c, tid, deadline=NOW() - 1, expect=400)['detail']
    # 正常开具后，一票只允许一张未完成交班单
    h = mk_handover(c, tid)
    assert h['status'] == 'PENDING' and h['from_team'] == '甲班'
    assert '未完成' in mk_handover(c, tid, expect=409)['detail']


def test_handover_watermark_not_below_accepted(c):
    tid = prepare(c)
    w = NOW()
    send(c, tid, '甲班', w + 5000)                      # 无交班单时持权班组正常入账
    r = mk_handover(c, tid, watermark=w + 4000, expect=400)
    assert '水位' in r['detail']


def test_handover_rejected_when_locked(c):
    tid = prepare(c)
    c.post(f'/api/tickets/{tid}/freeze', json={'actor': '调度员', 'reason': '异常'})
    r = mk_handover(c, tid, expect=423)
    assert '锁定' in r['detail']


# ---------------------------------------------------------------- 签收与水位归属

def test_pre_sign_flow_and_atomic_transfer(c):
    """签收前移交方续送入账、承接方首批挂起；签收原子转移写权并唤醒挂起。"""
    tid = prepare(c)
    w = NOW()
    h = mk_handover(c, tid, watermark=w)
    # 签收前：移交方水位线内续送 → 入账
    e1 = send(c, tid, '甲班', w - 2000, payload='交接前巡视1')['evidence']
    e2 = send(c, tid, '甲班', w - 1000, payload='交接前巡视2')['evidence']
    assert e1['status'] == e2['status'] == 'ACCEPTED'
    # 承接方首批数据提前到达 → 挂起（写权仍归移交方）
    e3 = send_id(c, tid, '乙班', w + 1000, 'first-B', base=2, payload='承接首批')['evidence']
    assert e3['status'] == 'SUSPENDED' and '写权仍归移交方' in e3['reason']
    # 签收：写权原子转移，挂起首批唤醒入账，交班单接续完成
    sign(c, h['id'], '乙班')
    v = view(c, tid)
    assert v['ticket']['write_owner'] == '乙班'
    assert v['handover']['status'] == 'COMPLETED'
    woke = [e for e in v['evidences'] if e['client_id'] == 'first-B'][0]
    assert woke['status'] == 'ACCEPTED' and woke['seq'] == 3
    # 承接方续作
    e4 = send(c, tid, '乙班', w + 2000, payload='接班后续作')['evidence']
    assert e4['status'] == 'ACCEPTED' and e4['seq'] == 4


def test_over_line_rejected_and_late_accepted(c):
    """越线后移交方内容拒收；水位线之前的合法迟到内容仍可入账。"""
    tid = prepare(c)
    w = NOW()
    h = mk_handover(c, tid, watermark=w)
    sign(c, h['id'], '乙班')
    over = send(c, tid, '甲班', w + 1000, payload='越线写入')['evidence']
    assert over['status'] == 'REJECTED' and '越线' in over['reason']
    late = send(c, tid, '甲班', w - 1000, payload='迟到补录')['evidence']
    assert late['status'] == 'ACCEPTED' and late['late'] == 1 and '迟到' in late['reason']
    # 承接方回填旧历元同样拒收
    backfill = send(c, tid, '乙班', w - 1000, payload='回填旧历元')['evidence']
    assert backfill['status'] == 'REJECTED' and '越过切换水位' in backfill['reason']


def test_first_content_chain_join(c):
    """承接方首条接不上旧链只能挂起；修正基点重传后入账。"""
    tid = prepare(c)
    w = NOW()
    h = mk_handover(c, tid, watermark=w)
    send(c, tid, '甲班', w - 1000, payload='链上第一条')          # 链头 #1
    sign(c, h['id'], '乙班')
    # 声明过期基点（链头已到 #1，却声明 #0）→ 接不上旧链，挂起
    bad = send_id(c, tid, '乙班', w + 1000, 'join-1', base=0, payload='首条接续')['evidence']
    assert bad['status'] == 'SUSPENDED' and '接不上旧链' in bad['reason']
    # 同 client_id 但内容不同 → 冲突拒绝
    r = c.post(f'/api/tickets/{tid}/shift-evidences',
               json={'client_id': 'join-1', 'team': '乙班', 'ts_data': w + 1000,
                     'payload': '篡改内容', 'base_seq': 1})
    assert r.status_code == 409 and '先到内容为准' in r.json()['detail']
    # 修正基点重传 → 入账上链
    ok = send_id(c, tid, '乙班', w + 1000, 'join-1', base=1, payload='首条接续')['evidence']
    assert ok['status'] == 'ACCEPTED' and ok['seq'] == 2
    # 续作免基点
    nxt = send(c, tid, '乙班', w + 2000, payload='续作')['evidence']
    assert nxt['status'] == 'ACCEPTED' and nxt['seq'] == 3


def test_duplicate_sign_idempotent(c):
    tid = prepare(c)
    h = mk_handover(c, tid)
    r1 = sign(c, h['id'], '乙班')
    r2 = sign(c, h['id'], '乙班')          # 重复签收：幂等，无第二次状态变化
    assert r1['handover']['signer'] == r2['handover']['signer'] == '乙班'
    v = view(c, tid)
    assert v['ticket']['write_owner'] == '乙班'
    evs = c.get(f'/api/events?ticket={tid}').json()
    assert sum(1 for e in evs if e['type'] == 'HANDOVER_SIGNED') == 1


def test_sign_race_single_winner(c):
    """两个候选班组争抢签收：只有一个取得写权。"""
    tid = prepare(c)
    h = mk_handover(c, tid, cands=('乙班', '丙班'))
    results = {}

    def do_sign(team):
        r = c.post(f"/api/handovers/{h['id']}/sign", json={'team': team})
        results[team] = r.status_code

    threads = [threading.Thread(target=do_sign, args=(t,)) for t in ('乙班', '丙班')]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results.values()) == [200, 409]
    winner = [t for t, s in results.items() if s == 200][0]
    v = view(c, tid)
    assert v['ticket']['write_owner'] == winner
    assert v['handover']['signer'] == winner


def test_sign_after_other_signed_409(c):
    tid = prepare(c)
    h = mk_handover(c, tid, cands=('乙班', '丙班'))
    sign(c, h['id'], '乙班')
    r = sign(c, h['id'], '丙班', expect=409)
    assert '乙班' in r['detail'] and '未中签' in r['detail']


def test_loser_first_batch_rejected_on_wake(c):
    """未中签候选的挂起首批在他人签收时终局拒收。"""
    tid = prepare(c)
    w = NOW()
    h = mk_handover(c, tid, cands=('乙班', '丙班'), watermark=w)
    send_id(c, tid, '丙班', w + 1000, 'c-first', base=0, payload='丙班首批')
    sign(c, h['id'], '乙班')
    v = view(c, tid)
    e = [x for x in v['evidences'] if x['client_id'] == 'c-first'][0]
    assert e['status'] == 'REJECTED' and '乙班' in e['reason']


def test_non_party_rejected(c):
    tid = prepare(c)
    w = NOW()
    h = mk_handover(c, tid, cands=('乙班',), watermark=w)
    e = send(c, tid, '丙班', w + 1000)['evidence']
    assert e['status'] == 'REJECTED' and '非交班双方' in e['reason']
    r = sign(c, h['id'], '丙班', expect=403)
    assert '候选' in r['detail']


def test_no_handover_owner_only(c):
    """无交班单时：任何时刻只允许持权班组追加现场证据。"""
    tid = prepare(c)
    ok = send(c, tid, '甲班', NOW())['evidence']
    assert ok['status'] == 'ACCEPTED'
    no = send(c, tid, '乙班', NOW())['evidence']
    assert no['status'] == 'REJECTED' and '无写权' in no['reason']


# ---------------------------------------------------------------- 关闭触发器

def test_dispatcher_cancel(c):
    """调度撤单：交班单关闭、挂起件拒收、签收失败；移交方写权不受影响。"""
    tid = prepare(c)
    w = NOW()
    h = mk_handover(c, tid, watermark=w)
    send_id(c, tid, '乙班', w + 1000, 'wait-1', base=0, payload='挂起待签收')
    r = c.post(f"/api/handovers/{h['id']}/cancel", json={'actor': '调度员', 'reason': '计划调整'})
    assert r.status_code == 200 and r.json()['status'] == 'CLOSED'
    assert '撤单' in r.json()['close_reason']
    v = view(c, tid)
    e = [x for x in v['evidences'] if x['client_id'] == 'wait-1'][0]
    assert e['status'] == 'REJECTED' and '写权未转移' in e['reason']
    assert '已关闭' in sign(c, h['id'], '乙班', expect=409)['detail']
    again = send(c, tid, '乙班', w + 2000)['evidence']
    assert again['status'] == 'REJECTED'
    still = send(c, tid, '甲班', w + 2000, payload='移交方继续')['evidence']
    assert still['status'] == 'ACCEPTED'


def test_cancel_signed_forbidden(c):
    tid = prepare(c)
    h = mk_handover(c, tid)
    sign(c, h['id'], '乙班')
    r = c.post(f"/api/handovers/{h['id']}/cancel", json={'actor': '调度员'})
    assert r.status_code == 409 and '不可撤单' in r.json()['detail']


def test_lock_closes_handover_and_sign_fails(c):
    """工作票锁定关闭未完成换班；锁定期间签收失败、证据拒收；解冻后移交方恢复。"""
    tid = prepare(c)
    w = NOW()
    h = mk_handover(c, tid, watermark=w)
    c.post(f'/api/tickets/{tid}/freeze', json={'actor': '调度员', 'reason': '边界告警'})
    v = view(c, tid)
    assert v['handover'] is None                       # 已关闭的交班单不再参与归属
    closed = v['handovers'][0]
    assert closed['status'] == 'CLOSED' and '锁定' in closed['close_reason']
    assert '锁定' in sign(c, h['id'], '乙班', expect=409)['detail']
    e = send(c, tid, '甲班', w - 1000)['evidence']
    assert e['status'] == 'REJECTED' and '锁定' in e['reason']
    c.post(f'/api/tickets/{tid}/unfreeze', json={'actor': '调度员'})
    ok = send(c, tid, '甲班', w - 500, payload='解冻后移交方续送')['evidence']
    assert ok['status'] == 'ACCEPTED'
    assert view(c, tid)['ticket']['write_owner'] == '甲班'     # 写权从未转移


def test_lock_closes_signed_handover_but_keeps_write_owner(c):
    """已签收未接续的换班被锁定关闭：写权不回退，仍归承接方。"""
    tid = prepare(c)
    w = NOW()
    h = mk_handover(c, tid, watermark=w)
    sign(c, h['id'], '乙班')
    c.post(f'/api/tickets/{tid}/freeze', json={'actor': '调度员', 'reason': '异常'})
    v = view(c, tid)
    assert v['handovers'][0]['status'] == 'CLOSED'
    assert v['ticket']['write_owner'] == '乙班'        # 既成写权转移不回退
    c.post(f'/api/tickets/{tid}/unfreeze', json={'actor': '调度员'})
    ok = send(c, tid, '乙班', w + 1000, payload='解冻后承接方续作')['evidence']
    assert ok['status'] == 'ACCEPTED'


def test_timeout_closes_handover(c):
    tid = prepare(c)
    h = mk_handover(c, tid, deadline=NOW() + 300)
    time.sleep(0.5)
    v = view(c, tid)                                    # 读取触发期限扫描
    assert v['handovers'][0]['status'] == 'CLOSED'
    assert '超时' in v['handovers'][0]['close_reason']
    assert '超时' in sign(c, h['id'], '乙班', expect=409)['detail']


def test_withdraw_closes_handover_no_rollback(c):
    """计划取消关闭换班；既成设备动作与控制锁绝不回退。"""
    tid = prepare(c)
    w = NOW()
    mk_handover(c, tid, watermark=w)
    send(c, tid, '甲班', w - 1000, payload='已入账证据')
    before = c.get('/sim/state').json()
    assert before['devices']['SW103']['state'] == 'OPEN'      # 既成设备动作
    r = c.post(f'/api/tickets/{tid}/withdraw', json={'actor': '调度员', 'reason': '计划取消'})
    assert r.status_code == 200
    v = view(c, tid)
    assert v['handovers'][0]['status'] == 'CLOSED' and '计划取消' in v['handovers'][0]['close_reason']
    after = c.get('/sim/state').json()
    assert after['devices']['SW103']['state'] == 'OPEN'       # 设备动作不回退
    assert any(g[0] == 'F1' for g in after['grounds'])        # 接地线保留
    dev = c.get('/api/devices/SW103').json()
    assert dev['controlled_by'] == tid                        # 控制锁保留（物理残留）
    e = [x for x in v['evidences'] if x['status'] == 'ACCEPTED']
    assert len(e) == 1                                        # 已入账证据不抹除


def test_topology_change_closes_handover(c):
    """边界重算（拓扑变更触发边界重校验）关闭未完成换班。"""
    tid = prepare(c)
    mk_handover(c, tid)
    r = c.post('/sim/topology', json={'op': 'add_edge', 'a': 'F1', 'b': 'F3', 'actor': 'sim'})
    assert r.status_code == 200
    v = view(c, tid)
    assert v['ticket']['frozen'] is True
    assert v['handovers'][0]['status'] == 'CLOSED' and '锁定' in v['handovers'][0]['close_reason']


# ---------------------------------------------------------------- 进程恢复

def test_recovery_preserves_ownership_and_suspended(tmp_path):
    """进程恢复后写权归属与挂起内容必须和故障前相同，换班可继续推进。"""
    db = str(tmp_path / 't.db')
    w = NOW()
    with TestClient(create_app(db)) as c1:
        tid = prepare(c1)
        h = mk_handover(c1, tid, watermark=w)
        send_id(c1, tid, '乙班', w + 1000, 'resume-1', base=0, payload='挂起的首批')
        before = view(c1, tid)
    # 模拟主站进程重启：全新应用实例复用同一 SQLite 文件
    with TestClient(create_app(db)) as c2:
        after = view(c2, tid)
        assert after['ticket']['write_owner'] == before['ticket']['write_owner'] == '甲班'
        assert after['handover']['status'] == 'PENDING'
        sus_b = [e['client_id'] for e in before['evidences'] if e['status'] == 'SUSPENDED']
        sus_a = [e['client_id'] for e in after['evidences'] if e['status'] == 'SUSPENDED']
        assert sus_a == sus_b == ['resume-1']
        # 恢复后继续换班：签收 → 挂起唤醒 → 写权转移
        sign(c2, h['id'], '乙班')
        v = view(c2, tid)
        assert v['ticket']['write_owner'] == '乙班'
        e = [x for x in v['evidences'] if x['client_id'] == 'resume-1'][0]
        assert e['status'] == 'ACCEPTED'


def test_recovery_after_sign_keeps_new_owner(tmp_path):
    db = str(tmp_path / 't.db')
    w = NOW()
    with TestClient(create_app(db)) as c1:
        tid = prepare(c1)
        h = mk_handover(c1, tid, watermark=w)
        sign(c1, h['id'], '乙班')
    with TestClient(create_app(db)) as c2:
        v = view(c2, tid)
        assert v['ticket']['write_owner'] == '乙班'
        assert v['handover']['status'] == 'SIGNED'
        e = send(c2, tid, '乙班', w + 1000, base=0, payload='恢复后首条接续')['evidence']
        assert e['status'] == 'ACCEPTED'
        assert view(c2, tid)['handover']['status'] == 'COMPLETED'


def test_admin_reopen_endpoint(c):
    tid = prepare(c)
    w = NOW()
    mk_handover(c, tid, watermark=w)
    send_id(c, tid, '乙班', w + 1000, 'ro-1', base=0, payload='挂起件')
    before = view(c, tid)
    r = c.post('/api/admin/reopen')
    assert r.status_code == 200
    after = view(c, tid)
    assert after['ticket']['write_owner'] == before['ticket']['write_owner']
    assert [e['status'] for e in after['evidences']] == [e['status'] for e in before['evidences']]
