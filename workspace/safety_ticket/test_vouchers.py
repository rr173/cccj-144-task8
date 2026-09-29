"""审计资料外发凭证柜端到端测试。

覆盖：正常封存领取查阅 / 超时拒绝 / 主动作废 / 并发领取只有一人成功 /
重复请求幂等（同一凭证不出两份内容）/ 封存后追加记录不改变快照 / 重启恢复，
以及序号段校验、非接收人无权、只读性等规则。
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


def seal(c, replay, from_seq, to_seq, receivers=('监管处', '稽查科'), ttl_ms=3600_000,
         key=None, expect=201):
    body = {'from_seq': from_seq, 'to_seq': to_seq, 'receivers': list(receivers),
            'deadline': 0, 'ttl_ms': ttl_ms, 'created_by': '分析员甲'}
    if key:
        body['key'] = key
    r = c.post(f"/api/replays/{replay['id']}/vouchers", json=body)
    assert r.status_code == expect, r.text
    return r.json()


def claim(c, vid, receiver, expect=200):
    r = c.post(f'/api/vouchers/{vid}/claim', json={'receiver': receiver})
    assert r.status_code == expect, r.text
    return r.json()


def content(c, vid, receiver, expect=200):
    r = c.post(f'/api/vouchers/{vid}/content', json={'receiver': receiver})
    assert r.status_code == expect, r.text
    return r.json()


# ---------------------------------------------------------------- 路径一：正常封存→领取→查阅

def test_voucher_normal_seal_claim_view(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    f, t = rp['applied'][0], rp['applied'][-1]
    v = seal(c, rp, f, t)
    assert v['status'] == 'SEALED' and v['claimer'] is None
    assert v['seal_digest'] and v['source_digest'] == rp['source_digest']
    assert v['record_count'] == t - f + 1
    assert set(v['receivers']) == {'监管处', '稽查科'}
    # 待领取状态不能直接查阅
    content(c, v['id'], '监管处', expect=409)
    # 非接收人无权领取
    r = c.post(f"/api/vouchers/{v['id']}/claim", json={'receiver': '无关单位'})
    assert r.status_code == 403
    # 接收人首次领取成功
    r = claim(c, v['id'], '监管处')
    assert r['ok'] and not r['duplicate']
    v2 = c.get(f"/api/vouchers/{v['id']}").json()
    assert v2['status'] == 'CLAIMED' and v2['claimer'] == '监管处'
    # 中签接收人查阅只读快照
    cv = content(c, v['id'], '监管处')
    assert cv['read_only'] is True and cv['seal_intact'] is True
    snap = cv['snapshot']
    assert snap['source_digest'] == rp['source_digest']
    assert [x['seq'] for x in snap['records']] == list(range(f, t + 1))
    for rec in snap['records']:
        assert rec['before'] is not None and rec['after'] is not None
    # 状态截面 = 窗口末条事件后的投影，票据阶段为 PERMITTED
    assert snap['state_section']['status'] == 'PERMITTED'
    assert snap['records'][-1]['after'] == snap['state_section']


def test_voucher_subrange_and_completeness(c):
    """凭证序号段可以是归约窗口内的连续子段，但必须全部是已应用的本票据事件。"""
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    # 停止的归约不能外发
    c.post('/api/demo/tamper', json={'seq': rp['entries'][3]['seq'], 'mode': 'delete'})
    rv = c.post(f"/api/replays/{rp['id']}/reverify").json()
    assert rv['status'] == 'HALTED'
    r = c.post(f"/api/replays/{rp['id']}/vouchers",
               json={'from_seq': rp['from_seq'], 'to_seq': rp['to_seq'],
                     'receivers': ['监管处'], 'deadline': 10 ** 16})
    assert r.status_code == 409 and r.json()['detail'].startswith('归约结果不完整')
    # 重新建一个完整归约，越过停止点的序号段被拒
    p2 = prepare(c, work=('F3',))
    rp2 = make_replay(c, p2['ticket'], p2['from_seq'], p2['to_seq'])
    r = c.post(f"/api/replays/{rp2['id']}/vouchers",
               json={'from_seq': rp2['from_seq'], 'to_seq': 10 ** 9,
                     'receivers': ['监管处'], 'deadline': 10 ** 16})
    assert r.status_code == 400 and '超出归约窗口' in r.json()['detail']
    r = c.post(f"/api/replays/{rp2['id']}/vouchers",
               json={'from_seq': 5, 'to_seq': 3, 'receivers': ['监管处'],
                     'deadline': 10 ** 16})
    assert r.status_code == 400
    # 截止时刻必须晚于现在
    r = c.post(f"/api/replays/{rp2['id']}/vouchers",
               json={'from_seq': rp2['from_seq'], 'to_seq': rp2['to_seq'],
                     'receivers': ['监管处'], 'deadline': 1})
    assert r.status_code == 400
    # 接收人不能为空/重复
    r = c.post(f"/api/replays/{rp2['id']}/vouchers",
               json={'from_seq': rp2['from_seq'], 'to_seq': rp2['to_seq'],
                     'receivers': [], 'deadline': 10 ** 16})
    assert r.status_code == 400


# ---------------------------------------------------------------- 路径二：超时拒绝

def test_voucher_expired_rejects_claim_and_view(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    f, t = rp['applied'][0], rp['applied'][-1]
    # 封存后把截止时刻直接改到过去（懒扫描在首次访问时判定超时，不依赖墙钟竞速）
    v = seal(c, rp, f, t, ttl_ms=3600_000)
    with c.app.state.db.lock:
        c.app.state.db.x('UPDATE vouchers SET deadline=1 WHERE id=?', (v['id'],))
        c.app.state.db.commit()
    r = c.post(f"/api/vouchers/{v['id']}/claim", json={'receiver': '监管处'})
    assert r.status_code == 410 and '截止时刻' in r.json()['detail']
    v2 = c.get(f"/api/vouchers/{v['id']}").json()
    assert v2['status'] == 'EXPIRED' and v2['invalid_reason']
    # 已领取凭证超时后同样拒绝查阅（不同接收人集合，另开一份凭证）
    v3 = seal(c, rp, f, t, receivers=('监管处', '稽查科', '第三方审计'),
              key='k-long', ttl_ms=3600_000)
    claim(c, v3['id'], '监管处')
    # 直接把截止时刻改到过去，模拟懒扫描（等价于时间流逝后的任何访问）
    with c.app.state.db.lock:
        c.app.state.db.x(f"UPDATE vouchers SET deadline=1 WHERE id=?", (v3['id'],))
        c.app.state.db.commit()
    r = c.post(f"/api/vouchers/{v3['id']}/content", json={'receiver': '监管处'})
    assert r.status_code == 410 and '截止时刻' in r.json()['detail']
    # 超时后不能再主动作废（终态）
    r = c.post(f"/api/vouchers/{v3['id']}/void", json={'reason': 'x'})
    assert r.status_code == 409


# ---------------------------------------------------------------- 路径三：主动作废

def test_voucher_void_rejects_with_reason_but_keeps_snapshot(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    f, t = rp['applied'][0], rp['applied'][-1]
    v = seal(c, rp, f, t)
    claim(c, v['id'], '监管处')
    r = c.post(f"/api/vouchers/{v['id']}/void",
               json={'reason': '外发审批撤销', 'actor': '分析员甲'})
    assert r.status_code == 200 and r.json()['status'] == 'VOID'
    assert r.json()['invalid_reason'] == '外发审批撤销'
    # 作废后领取与查阅都返回作废原因
    r = c.post(f"/api/vouchers/{v['id']}/claim", json={'receiver': '稽查科'})
    assert r.status_code == 410 and '主动作废' in r.json()['detail']
    r = c.post(f"/api/vouchers/{v['id']}/content", json={'receiver': '监管处'})
    assert r.status_code == 410 and '外发审批撤销' in r.json()['detail']
    # 快照不抹除：固化内容仍在库中，封存摘要可审计
    row = c.app.state.db.one('SELECT snapshot, seal_digest FROM vouchers WHERE id=?', (v['id'],))
    assert row['snapshot'] and row['seal_digest']
    # 重复作废幂等
    r2 = c.post(f"/api/vouchers/{v['id']}/void", json={'reason': '另一个原因'})
    assert r2.status_code == 200 and r2.json()['invalid_reason'] == '外发审批撤销'
    assert r2.json().get('duplicate') is True


# ---------------------------------------------------------------- 路径四：并发领取只有一人成功

def test_voucher_concurrent_claims_single_winner(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    f, t = rp['applied'][0], rp['applied'][-1]
    v = seal(c, rp, f, t, receivers=('监管处', '稽查科', '第三方审计'))
    results = []
    for who in ('监管处', '稽查科', '第三方审计'):
        rr = c.post(f"/api/vouchers/{v['id']}/claim", json={'receiver': who})
        results.append((who, rr.status_code, rr.json()))
    wins = [w for w, s, _ in results if s == 200]
    losses = [(w, b) for w, s, b in results if s == 409]
    assert len(wins) == 1, f'应只有一人中签，实际 {wins}'
    assert len(losses) == 2
    winner = wins[0]
    for who, b in losses:
        assert b['detail'].startswith('并发领取')
        assert winner in b['detail']
    v2 = c.get(f"/api/vouchers/{v['id']}").json()
    assert v2['claimer'] == winner
    # 只有中签者能查阅
    content(c, v['id'], winner)
    for who, _ in losses:
        c.post(f"/api/vouchers/{v['id']}/content", json={'receiver': who})
    # 领取记号全部持久化（1 WON + 2 LOST）
    with c.app.state.db.lock:
        rows = c.app.state.db.q('SELECT receiver, result FROM voucher_claims WHERE voucher=?',
                                (v['id'],))
    assert sorted(r['result'] for r in rows) == ['LOST', 'LOST', 'WON']
    assert {r['receiver'] for r in rows} == {'监管处', '稽查科', '第三方审计'}


# ---------------------------------------------------------------- 路径五：重复请求幂等

def test_voucher_idempotent_seal_and_claim(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    f, t = rp['applied'][0], rp['applied'][-1]
    v1 = seal(c, rp, f, t, key='seal-key-1')
    # 同一幂等键再次索取：返回首次凭证，不产生第二份
    again = seal(c, rp, f, t, key='seal-key-1')
    assert again['id'] == v1['id'] and again.get('duplicate') is True
    assert again['seal_digest'] == v1['seal_digest']
    # 不带键但同归约+同序号段+同接收人：仍为同一份内容
    nat = seal(c, rp, f, t)
    assert nat['id'] == v1['id'] and nat.get('duplicate') is True
    # 同序号段但不同接收人：另开一份凭证（不同外发对象）
    other = seal(c, rp, f, t, receivers=('司法机关',), key='seal-key-2')
    assert other['id'] != v1['id'] and 'duplicate' not in other
    # 同一凭证不能出现两份内容：相同自然键（归约+序号段+接收人）的请求未重复创建
    with c.app.state.db.lock:
        cnt = c.app.state.db.one(
            'SELECT COUNT(*) n FROM vouchers WHERE replay=? AND from_seq=? AND to_seq=?',
            (rp['id'], f, t))['n']
    assert cnt == 2  # 两组接收人各一份，同自然键（接收人相同）的请求未重复创建
    # 中签者重复领取：幂等返回首次成功结果
    claim(c, v1['id'], '监管处')
    r2 = c.post(f"/api/vouchers/{v1['id']}/claim", json={'receiver': '监管处'})
    assert r2.status_code == 200 and r2.json().get('duplicate') is True
    # 负方重复领取：返回同一确定失败（幂等）
    rl1 = c.post(f"/api/vouchers/{v1['id']}/claim", json={'receiver': '稽查科'})
    rl2 = c.post(f"/api/vouchers/{v1['id']}/claim", json={'receiver': '稽查科'})
    assert rl1.status_code == rl2.status_code == 409
    assert rl1.json()['detail'] == rl2.json()['detail']
    # 重复查阅：内容逐字节一致
    cv1 = content(c, v1['id'], '监管处')
    cv2 = content(c, v1['id'], '监管处')
    assert cv1['snapshot'] == cv2['snapshot']
    assert cv1['seal_digest'] == cv2['seal_digest']


# ---------------------------------------------------------------- 路径六：追加记录不改变快照

def test_voucher_snapshot_immutable_after_append(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    f, t = rp['applied'][0], rp['applied'][-1]
    v = seal(c, rp, f, t)
    claim(c, v['id'], '监管处')
    before = content(c, v['id'], '监管处')
    max_before = c.get('/api/events', params={'limit': 1}).json()[0]['seq']
    # 封存后正式审计库继续追加新记录（序号必然越过凭证上界）
    for i in range(3):
        r = c.post(f"/api/tickets/{p['ticket']}/shift-evidences",
                   json={'client_id': f'after-voucher:{i}', 'team': '甲班',
                         'ts_data': 10 ** 13 + i, 'kind': f'外发后新增{i}'})
        assert r.status_code == 200, r.text
    # 外发凭证本身的审计事件也追加到了正式序列
    max_after = c.get('/api/events', params={'limit': 1}).json()[0]['seq']
    assert max_after > max_before and max_after > t
    after = content(c, v['id'], '监管处')
    # 已发出的截面不被污染：封存摘要、状态截面、逐条记录完全不变
    assert after['snapshot'] == before['snapshot']
    assert after['seal_digest'] == before['seal_digest']
    assert [x['seq'] for x in after['snapshot']['records']][-1] == t
    assert after['snapshot']['state_section'] == before['snapshot']['state_section']
    # 凭证查阅是只读的：正式票据状态与步骤未被凭证操作改变
    tk = c.get(f"/api/tickets/{p['ticket']}").json()
    assert tk['status'] == 'PERMITTED'
    assert all(s['status'] == 'VERIFIED' for s in tk['steps'])
    # 对正式库的取证篡改也不影响已封存截面（快照根本不回读正式事件）
    victim = after['snapshot']['records'][1]['seq']
    c.post('/api/demo/tamper', json={'seq': victim, 'mode': 'corrupt'})
    tampered = content(c, v['id'], '监管处')
    assert tampered['snapshot'] == before['snapshot']
    assert tampered['seal_intact'] is True


# ---------------------------------------------------------------- 路径七：重启恢复

def test_voucher_restart_recovery(c):
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    f, t = rp['applied'][0], rp['applied'][-1]
    # 凭证1：已领取；凭证2：待领取；凭证3：已作废；凭证4：已超时
    v1 = seal(c, rp, f, t, receivers=('监管处', '稽查科'), key='r1')
    claim(c, v1['id'], '稽查科')
    c.post(f"/api/vouchers/{v1['id']}/claim", json={'receiver': '监管处'})  # 负方记号
    v2 = seal(c, rp, f, rp['applied'][-2], receivers=('监管处',), key='r2')
    v3 = seal(c, rp, rp['applied'][1], rp['applied'][2], receivers=('监管处',), key='r3')
    c.post(f"/api/vouchers/{v3['id']}/void", json={'reason': '撤销外发', 'actor': '分析员甲'})
    v4 = seal(c, rp, rp['applied'][3], rp['applied'][4], receivers=('监管处',), key='r4')
    with c.app.state.db.lock:
        c.app.state.db.x('UPDATE vouchers SET deadline=1 WHERE id=?', (v4['id'],))
        c.app.state.db.commit()

    db_path = c.app.state.db.path
    # 重启前抓一份快照内容用于比对
    snap1 = c.post(f"/api/vouchers/{v1['id']}/content",
                   json={'receiver': '稽查科'}).json()['snapshot']
    with TestClient(create_app(db_path)) as c2:
        # 凭证阶段恢复
        g1 = c2.get(f"/api/vouchers/{v1['id']}").json()
        g2 = c2.get(f"/api/vouchers/{v2['id']}").json()
        g3 = c2.get(f"/api/vouchers/{v3['id']}").json()
        g4 = c2.get(f"/api/vouchers/{v4['id']}").json()
        assert g1['status'] == 'CLAIMED' and g1['claimer'] == '稽查科'
        assert g2['status'] == 'SEALED' and g2['claimer'] is None
        assert g3['status'] == 'VOID' and g3['invalid_reason'] == '撤销外发'
        assert g4['status'] == 'EXPIRED' and '截止时刻' in g4['invalid_reason']
        # 领取记号恢复：WON/LOST 都在
        results = {x['receiver']: x['result'] for x in g1['claims']}
        assert results == {'稽查科': 'WON', '监管处': 'LOST'}
        # 中签者重启后仍可查阅，且封存内容逐字节一致
        cv = c2.post(f"/api/vouchers/{v1['id']}/content", json={'receiver': '稽查科'})
        assert cv.status_code == 200 and cv.json()['snapshot'] == snap1
        # 重启后待领取凭证仍可正常领取（流程可继续）
        r = c2.post(f"/api/vouchers/{v2['id']}/claim", json={'receiver': '监管处'})
        assert r.status_code == 200 and r.json()['ok']
        # 重启后负方再领取仍是同一失败，超时/作废仍拒绝
        r = c2.post(f"/api/vouchers/{v1['id']}/claim", json={'receiver': '监管处'})
        assert r.status_code == 409 and '稽查科' in r.json()['detail']
        assert c2.post(f"/api/vouchers/{v4['id']}/claim",
                       json={'receiver': '监管处'}).status_code == 410
        assert c2.post(f"/api/vouchers/{v3['id']}/content",
                       json={'receiver': '监管处'}).status_code == 410


def test_voucher_list_and_404(c):
    assert c.get('/api/vouchers').json() == []
    r = c.get('/api/vouchers/NOPE')
    assert r.status_code == 404
    p = prepare(c)
    rp = make_replay(c, p['ticket'], p['from_seq'], p['to_seq'])
    v = seal(c, rp, rp['applied'][0], rp['applied'][1])
    lst = c.get('/api/vouchers').json()
    assert [x['id'] for x in lst] == [v['id']]
    by_ticket = c.get(f"/api/tickets/{p['ticket']}/vouchers").json()
    assert [x['id'] for x in by_ticket] == [v['id']]
    assert c.get('/api/tickets/NOPE/vouchers').status_code == 404
