"""工器具借还台端到端测试。

覆盖：需求推导、批次锁定/他票占用/整批回滚、逐件扫码（重复/批次不符/他票占用/未锁定/维修件）、
齐套移交、封签破损/缺件/超期拒绝、并发争抢唯一持权、断网本地队列补送（挂起/唤醒/重复不扣库存）、
归还清点差异（缺件/磨损/封签/串箱）置待复核并禁借、撤回触发库存回滚、进程重启后继续处理、校验串复核。
"""
import threading

import pytest
from fastapi.testclient import TestClient

from server import create_app


@pytest.fixture()
def c(tmp_path):
    dbp = str(tmp_path / 't.db')
    app = create_app(dbp)
    with TestClient(app) as client:
        yield client, dbp


def client(cf):
    return cf[0]


def dbpath(cf):
    return cf[1]


def make_ticket(c, work=('F2',), title='检修'):
    r = c.post('/api/tickets', json={'title': title, 'work_area': list(work), 'created_by': '调度A'})
    assert r.status_code == 201, r.text
    return r.json()


def lock(c, tid, serials=None):
    r = c.post(f'/api/tickets/{tid}/tool-locks', json={'serials': serials})
    assert r.status_code == 201, r.text
    return r.json()['lines']


def make_box(c, tid, box_id, deadline_hours=8, seal='S-OK'):
    r = c.post(f'/api/tickets/{tid}/tool-boxes',
               json={'box_id': box_id, 'deadline_hours': deadline_hours, 'seal': seal})
    assert r.status_code == 201, r.text
    return r.json()


def scan(c, box, serial, client_id='PDA-01', key=None):
    return c.post(f'/api/tool-boxes/{box}/scan',
                  json={'client_id': client_id, 'serial': serial, 'key': key})


def scan_all(c, box_view, client_id='PDA-01'):
    res = []
    for l in box_view['lines']:
        r = scan(c, box_view['id'], l['serial'], client_id)
        assert r.status_code == 200, r.text
        res.append(r.json())
    return res


def handover_ok(c, box, leader, team, seal_intact=True):
    r = c.post(f'/api/tool-boxes/{box}/handover',
               json={'leader': leader, 'team': team, 'seal_intact': seal_intact})
    assert r.status_code == 200, r.text
    return r.json()


def return_ok(c, box, lines, box_seal='S-OK', overrides=None):
    overrides = overrides or {}
    items = []
    for l in lines:
        o = overrides.get(l['serial'], {})
        items.append({'serial': l['serial'], 'code': l['code'],
                      'wear': o.get('wear', 'OK'), 'seal_ok': o.get('seal_ok', True)})
    r = c.post(f'/api/tool-boxes/{box}/return',
               json={'box_seal': box_seal, 'items': items})
    assert r.status_code == 200, r.text
    return r.json()


# ---------------------------------------------------------------- 需求推导与批次锁定

def test_requirements_derived_from_isolation_steps(c):
    cl = client(c)
    t = make_ticket(cl, ('F2',))     # 3 个 OPEN_SWITCH + 3 个 INSTALL_GROUND
    req = cl.get(f"/api/tools/requirements/{t['id']}").json()
    qty = {l['code']: l['qty'] for l in req['lines']}
    assert qty == {'GLOVE': 6, 'VDT': 3, 'ROD': 3, 'GROUNDROD': 3}
    # 每个隔离步骤都带工具映射
    assert all(x['tools'] for x in req['per_step'])


def test_lock_by_batch_and_view(c):
    cl = client(c)
    t = make_ticket(cl)
    lines = lock(cl, t['id'])
    assert len(lines) == 15
    batches = {(l['code'], l['batch']) for l in lines}
    # 自动选批按编号排序，绝缘手套优先取 B2401（6 件）
    glove_b = {b for code, b in batches if code == 'GLOVE'}
    assert glove_b == {'B2401'}
    assert all(l['item_state'] == 'LOCKED' for l in lines)
    board = cl.get('/api/tools/board').json()
    assert len(board['locks']) == 15


def test_lock_conflict_with_other_ticket_atomic_rollback(c):
    cl = client(c)
    t1 = make_ticket(cl, ('F2',), '票一')
    t2 = make_ticket(cl, ('F3',), '票二')
    lock(cl, t1['id'])
    # 票一锁走 GLOVE-B2401 全部 6 件 + 其他若干；票二再锁应整批失败并回滚
    r = cl.post(f"/api/tickets/{t2['id']}/tool-locks", json={})
    assert r.status_code == 409
    body = r.json()['detail']
    assert body['reason_code'] == 'INSUFFICIENT_STOCK'
    # 整批原子：票二没有留下任何锁定行
    assert cl.get(f"/api/tickets/{t2['id']}/tool-locks").json()['lines'] == []


def test_lock_specified_serial_occupied_by_other_ticket(c):
    cl = client(c)
    t1 = make_ticket(cl, ('F2',), '票一')
    lock(cl, t1['id'])
    t2 = make_ticket(cl, ('F3',), '票二')
    r = cl.post(f"/api/tickets/{t2['id']}/tool-locks",
                json={'serials': {'GLOVE': ['GLOVE-B2401-01']}})
    assert r.status_code == 409
    assert r.json()['detail']['reason_code'] == 'ITEM_OCCUPIED_BY_TICKET'
    assert r.json()['detail']['detail'].count(t1['id']) == 1


def test_lock_maintenance_item_rejected(c):
    cl = client(c)
    t = make_ticket(cl)
    r = cl.post(f"/api/tickets/{t['id']}/tool-locks",
                json={'serials': {'ROD': ['ROD-B2311-05']}})   # 维修件
    assert r.status_code == 409
    assert r.json()['detail']['reason_code'] == 'ITEM_IN_MAINTENANCE'


def test_inventory_rollback_on_unlock(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    free_before = cl.get('/api/tools/inventory').json()
    free_before = sum(1 for x in free_before['tools'] if x['state'] == 'FREE')
    r = cl.request('DELETE', f"/api/tickets/{t['id']}/tool-locks", json={'actor': '库管员'})
    assert r.status_code == 200
    assert r.json()['released'] == 15
    inv = cl.get('/api/tools/inventory').json()
    assert sum(1 for x in inv['tools'] if x['state'] == 'FREE') == free_before + 15
    # 释放后可以重新锁定
    assert lock(cl, t['id'])


# ---------------------------------------------------------------- 扫码领用

def test_checkout_complete_and_checksums(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXA')
    out = scan_all(cl, b)
    assert all(x['ok'] and not x['duplicate'] for x in out)
    assert all(x['checksum'].startswith('tlc.') and len(x['checksum']) == 24 for x in out)
    b2 = cl.get('/api/tool-boxes/BXA').json()
    assert b2['issued'] == 15 and b2['complete'] is True
    # 库存已扣减为 ISSUED
    inv = cl.get('/api/tools/inventory').json()
    issued = {x['serial'] for x in inv['tools'] if x['state'] == 'ISSUED'}
    assert {l['serial'] for l in b['lines']} <= issued


def test_duplicate_scan_returns_original_result(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXD')
    serial = b['lines'][0]['serial']
    r1 = scan(cl, 'BXD', serial).json()
    r2 = scan(cl, 'BXD', serial).json()
    assert r2['duplicate'] is True
    assert r2['reason_code'] == 'DUPLICATE_SCAN'
    assert r2['checksum'] == r1['checksum']
    # 不同扫码器再扫同一件仍是原结果
    r3 = scan(cl, 'BXD', serial, client_id='PDA-02').json()
    assert r3['duplicate'] and r3['checksum'] == r1['checksum']
    # 该件仍只产生一条领用记录
    rows = cl.get('/api/tool-boxes/BXD').json()['lines']
    assert sum(1 for x in rows if x['serial'] == serial and x['checked_out']) == 1


def test_scan_item_occupied_by_other_ticket(c):
    cl = client(c)
    t1 = make_ticket(cl, ('F2',), '票一')
    lock(cl, t1['id'])
    b1 = make_box(cl, t1['id'], 'BX1')
    serial = b1['lines'][0]['serial']
    assert scan(cl, 'BX1', serial).json()['ok']

    # 现场把另一张票（F3）已锁定的件拿到本箱扫码 → 报具体占用票据
    t2 = make_ticket(cl, ('F3',), '票二')
    # 直接在数据层为票二锁定一件当前空闲的同型工具（模拟库管员已为票二发料）
    db = cl.app.state.db
    code = next(l['code'] for l in b1['lines'] if l['serial'] == serial)
    free_same = next(r['serial'] for r in db.q(
        "SELECT serial FROM tools WHERE code=? AND state='FREE' ORDER BY serial", (code,)))
    with db.lock:
        db.x("INSERT INTO tool_locks(id,line_no,ticket,code,batch_id,item_serial,state,actor,locked_at) "
             "SELECT 'LOTHER',1,?,code,batch_id,serial,'LOCKED','库管员',0 FROM tools WHERE serial=?",
             (t2['id'], free_same))
        db.x("UPDATE tools SET state='LOCKED' WHERE serial=?", (free_same,))
        db.commit()
    r = scan(cl, 'BX1', free_same)
    j = r.json()
    assert j['ok'] is False and j['reason_code'] == 'ITEM_OCCUPIED_BY_TICKET'
    assert j['occupied_by'] == t2['id']
    # 已被票一领走的件，同样可用于票二视角的占用校验
    inv = cl.get('/api/tools/inventory').json()
    assert next(x for x in inv['tools'] if x['serial'] == serial)['state'] == 'ISSUED'


def test_scan_batch_mismatch(c):
    cl = client(c)
    t = make_ticket(cl)
    # 验电器本需 3 件：把该票 3 行锁定编号都改成不存在的伪编号，模拟"实发批次/编号与锁定清单不符"
    lock(cl, t['id'])
    db = cl.app.state.db
    with db.lock:
        db.x("UPDATE tools SET state='FREE' WHERE code='VDT'")
        db.x("UPDATE tool_locks SET item_serial='VDT-B2399-99' WHERE code='VDT'")
        db.commit()
    b = make_box(cl, t['id'], 'BXB')
    # 现场扫到的是真实在库件 VDT-B2311-01，与锁定行（伪编号 ...99）不符 → 批次/编号不符
    r = scan(cl, 'BXB', 'VDT-B2311-01')
    assert r.status_code == 200
    j = r.json()
    assert j['ok'] is False and j['reason_code'] == 'BATCH_MISMATCH'
    assert j['expected_serial'] == 'VDT-B2399-99' and j['scanned_serial'] == 'VDT-B2311-01'
    # 被拒件仍在库（未扣库存）
    inv = cl.get('/api/tools/inventory').json()
    assert next(x for x in inv['tools'] if x['serial'] == 'VDT-B2311-01')['state'] == 'FREE'


def test_scan_not_locked_and_unknown_item(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXC')
    # WRENCH 不在本票锁定清单
    r = scan(cl, 'BXC', 'WRENCH-B2401-01')
    assert r.json()['reason_code'] == 'NOT_LOCKED_FOR_TICKET'
    r = scan(cl, 'BXC', 'NO-SUCH-99')
    assert r.json()['reason_code'] == 'ITEM_NOT_FOUND'


def test_scan_idempotency_key_returns_first_result(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXI')
    serial = b['lines'][0]['serial']
    r1 = scan(cl, 'BXI', serial, key='K-1').json()
    r2 = scan(cl, 'BXI', serial, key='K-1').json()
    assert r2['checksum'] == r1['checksum']


# ---------------------------------------------------------------- 移交：齐套/封签/期限/争抢

def test_handover_requires_complete_kit(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXH')
    # 只领一件就移交 → 缺件
    scan(cl, 'BXH', b['lines'][0]['serial'])
    r = cl.post('/api/tool-boxes/BXH/handover', json={'leader': '王负责', 'team': '甲班'})
    assert r.status_code == 409
    assert r.json()['detail']['reason_code'] == 'BOX_NOT_COMPLETE'
    assert '缺件' in r.json()['detail']['detail']
    bv = cl.get('/api/tool-boxes/BXH').json()
    assert bv['status'] == 'READY'
    assert any(x['reason_code'] == 'BOX_NOT_COMPLETE' for x in bv['reasons'])


def test_handover_seal_broken_blocks_box(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXS')
    scan_all(cl, b)
    r = cl.post('/api/tool-boxes/BXS/handover',
                json={'leader': '王负责', 'team': '甲班', 'seal_intact': False})
    assert r.status_code == 409
    assert r.json()['detail']['reason_code'] == 'SEAL_BROKEN'
    bv = cl.get('/api/tool-boxes/BXS').json()
    assert bv['status'] == 'BLOCKED' and bv['seal_broken'] in (1, True)


def test_handover_deadline_passed(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXT', deadline_hours=0)   # 已过交接期限
    scan_all(cl, b)
    r = cl.post('/api/tool-boxes/BXT/handover', json={'leader': '王负责', 'team': '甲班'})
    assert r.status_code == 409
    assert r.json()['detail']['reason_code'] == 'HANDOVER_DEADLINE_PASSED'
    assert '超过交接期限' in r.json()['detail']['detail']


def test_concurrent_handover_only_one_winner(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXCC')
    scan_all(cl, b)
    # 两个现场负责人线程在同一刻发起移交（TestClient 非线程安全，直接并发调用核心函数）
    import tools as tools_mod
    db = cl.app.state.db
    secret = tools_mod.get_secret(db)
    barrier = threading.Barrier(2)
    outcomes = []

    def grab(leader, team):
        barrier.wait()
        try:
            tools_mod.handover_core(db, 'BXCC', leader, team)
            outcomes.append((leader, 200))
        except tools_mod.RuleError as e:
            outcomes.append((leader, e.http_status))

    th = [threading.Thread(target=grab, args=args)
          for args in (('王负责', '甲班'), ('赵负责', '乙班'))]
    for x in th:
        x.start()
    for x in th:
        x.join()
    winners = [x for x in outcomes if x[1] == 200]
    losers = [x for x in outcomes if x[1] == 409]
    assert len(winners) == 1 and len(losers) == 1
    bv = cl.get('/api/tool-boxes/BXCC').json()
    assert bv['holder'] == winners[0][0]
    conflict = [x for x in bv['reasons'] if x['reason_code'] == 'HOLDER_TAKEN']
    assert conflict and winners[0][0] in conflict[0]['detail']


# ---------------------------------------------------------------- 断网补送

def test_offline_flush_gap_suspend_then_resume(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXO')
    serials = [l['serial'] for l in b['lines'][:4]]
    # 断网期间本地队列产生位置 1..4；恢复后先只补 1,2,4（缺 3）
    r = cl.post('/api/tool-scans/flush', json={'client_id': 'PDA-OFF', 'items': [
        {'client_seq': 1, 'box': 'BXO', 'serial': serials[0]},
        {'client_seq': 2, 'box': 'BXO', 'serial': serials[1]},
        {'client_seq': 4, 'box': 'BXO', 'serial': serials[3]},
    ]})
    j = r.json()
    assert {x['client_seq']: x['status'] for x in j['results']} == {
        1: 'ACCEPTED', 2: 'ACCEPTED', 4: 'PENDING'}
    assert j['woken'] == []
    # 前序 3 补到 → 4 自动链式唤醒
    r = cl.post('/api/tool-scans/flush', json={'client_id': 'PDA-OFF', 'items': [
        {'client_seq': 3, 'box': 'BXO', 'serial': serials[2]},
    ]})
    j = r.json()
    seq3 = next(x for x in j['results'] if x['client_seq'] == 3)
    assert seq3['status'] == 'ACCEPTED'
    assert [x['client_seq'] for x in j['woken']] == [4]
    assert j['woken'][0]['result']['ok'] is True
    # 事件游标视图可查
    evs = cl.get('/api/tool-scans/PDA-OFF').json()
    assert [e['client_seq'] for e in evs] == [1, 2, 3, 4]
    assert all(e['status'] == 'ACCEPTED' for e in evs)


def test_offline_flush_duplicate_no_double_debit(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXDUP')
    serial = b['lines'][0]['serial']
    payload = {'client_id': 'PDA-Q', 'items': [{'client_seq': 1, 'box': 'BXDUP', 'serial': serial}]}
    r1 = cl.post('/api/tool-scans/flush', json=payload).json()
    cs1 = r1['results'][0]['result']['checksum']
    # 整包重复补送
    r2 = cl.post('/api/tool-scans/flush', json=payload).json()
    assert r2['results'][0]['duplicate'] is True
    assert r2['results'][0]['result']['checksum'] == cs1
    # 再来一次仍一致；库存只扣一件
    r3 = cl.post('/api/tool-scans/flush', json=payload).json()
    assert r3['results'][0]['result']['checksum'] == cs1
    bv = cl.get('/api/tool-boxes/BXDUP').json()
    assert sum(1 for l in bv['lines'] if l['checked_out']) == 1
    assert next(x for x in cl.get('/api/tools/inventory').json()['tools']
                if x['serial'] == serial)['state'] == 'ISSUED'


def test_offline_flush_failure_occupies_position_but_not_blocks(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXF')
    # 位置1扫一件不属于本票清单的工具（REJECTED 也算终态，占位但不阻塞后续）
    r = cl.post('/api/tool-scans/flush', json={'client_id': 'PDA-F', 'items': [
        {'client_seq': 1, 'box': 'BXF', 'serial': 'WRENCH-B2401-01'},
        {'client_seq': 2, 'box': 'BXF', 'serial': b['lines'][0]['serial']},
    ]}).json()
    assert r['results'][0]['status'] == 'REJECTED'
    assert r['results'][0]['result']['reason_code'] == 'NOT_LOCKED_FOR_TICKET'
    assert r['results'][1]['status'] == 'ACCEPTED'


# ---------------------------------------------------------------- 归还清点差异

def test_return_damage_and_seal_diff_sets_review(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXR')
    scan_all(cl, b)
    handover_ok(cl, 'BXR', '王负责', '甲班')
    bad_serial = b['lines'][0]['serial']
    seal_serial = b['lines'][1]['serial']
    rt = return_ok(cl, 'BXR', b['lines'], overrides={
        bad_serial: {'wear': 'BAD'},
        seal_serial: {'seal_ok': False},
    })
    assert rt['ok'] is False and rt['status'] == 'REVIEW'
    kinds = {d['kind'] for d in rt['diffs']}
    assert 'DAMAGED' in kinds and 'SEAL_CHANGED' in kinds
    bv = cl.get('/api/tool-boxes/BXR').json()
    assert bv['status'] == 'REVIEW'
    # 差异件被禁用，禁止再次借出
    inv = cl.get('/api/tools/inventory').json()
    states = {x['serial']: x['state'] for x in inv['tools']}
    assert states[bad_serial] == 'BLOCKED' and states[seal_serial] == 'BLOCKED'
    # 待复核箱不能再移交
    r = cl.post('/api/tool-boxes/BXR/handover', json={'leader': '任何人', 'team': '甲班'})
    assert r.status_code == 409 and r.json()['detail']['reason_code'] in (
        'BOX_NOT_READY', 'BOX_UNDER_REVIEW')
    # 复核确认缺损：仅磨损件转维修；封签变化件换封后回库
    cl.post('/api/tool-boxes/BXR/review', json={'action': 'CONFIRM_LOSS'})
    inv = cl.get('/api/tools/inventory').json()
    states = {x['serial']: x['state'] for x in inv['tools']}
    assert states[bad_serial] == 'MAINTENANCE' and states[seal_serial] == 'FREE'


def test_return_missing_item_sets_review(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXM')
    scan_all(cl, b)
    handover_ok(cl, 'BXM', '王负责', '甲班')
    # 少还一件 + 箱封签变化
    items = [{'serial': l['serial'], 'wear': 'OK', 'seal_ok': True}
             for l in b['lines'][1:]]
    r = cl.post('/api/tool-boxes/BXM/return', json={'box_seal': 'S-CUT', 'items': items})
    j = r.json()
    assert j['status'] == 'REVIEW'
    kinds = {d['kind'] for d in j['diffs']}
    assert 'MISSING' in kinds and 'BOX_SEAL_CHANGED' in kinds
    # 复核确认缺损：缺件转维修/报废，其余回库
    rr = cl.post('/api/tool-boxes/BXM/review', json={'action': 'CONFIRM_LOSS'})
    assert rr.status_code == 200 and rr.json()['status'] == 'RETURNED'
    inv = cl.get('/api/tools/inventory').json()
    missing_serial = b['lines'][0]['serial']
    assert next(x for x in inv['tools'] if x['serial'] == missing_serial)['state'] == 'MAINTENANCE'
    # 其余实物完好件（含被换箱封签的）复核后回库
    assert next(x for x in inv['tools'] if x['serial'] == b['lines'][1]['serial'])['state'] == 'FREE'


def test_return_extra_item_rejected(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXE')
    scan_all(cl, b)
    handover_ok(cl, 'BXE', '王负责', '甲班')
    items = [{'serial': l['serial'], 'wear': 'OK', 'seal_ok': True} for l in b['lines']]
    items.append({'serial': 'WRENCH-B2401-01', 'wear': 'OK', 'seal_ok': True})  # 串箱多还
    j = cl.post('/api/tool-boxes/BXE/return', json={'box_seal': 'S-OK', 'items': items}).json()
    assert j['status'] == 'REVIEW'
    assert any(d['kind'] == 'EXTRA' for d in j['diffs'])
    # 复核确认误报后全部回库
    cl.post('/api/tool-boxes/BXE/review', json={'action': 'CLEAR'})
    inv = cl.get('/api/tools/inventory').json()
    assert next(x for x in inv['tools'] if x['serial'] == b['lines'][0]['serial'])['state'] == 'FREE'


def test_clean_return_restocks_and_lock_released(c):
    cl = client(c)
    t = make_ticket(cl)
    lines = lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXOK')
    scan_all(cl, b)
    handover_ok(cl, 'BXOK', '王负责', '甲班')
    rt = return_ok(cl, 'BXOK', b['lines'])
    assert rt['ok'] is True and rt['returned'] == 15
    inv = cl.get('/api/tools/inventory').json()
    states = {x['serial']: x['state'] for x in inv['tools']}
    assert all(states[l['serial']] == 'FREE' for l in lines)
    assert cl.get(f"/api/tickets/{t['id']}/tool-locks").json()['lines'] == []
    # 归还结案后同一票可以重新开新一轮锁定/领用
    assert lock(cl, t['id'])


def test_checksums_remain_queryable_after_return(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXCQ')
    out = scan_all(cl, b)
    handover_ok(cl, 'BXCQ', '王负责', '甲班')
    return_ok(cl, 'BXCQ', b['lines'])
    v = cl.get('/api/tool-boxes/BXCQ').json()
    assert v['status'] == 'RETURNED' and len(v['checkouts']) == 15
    by_sn = {x['serial']: x['checksum'] for x in v['checkouts']}
    for o in out:
        assert by_sn[o['serial']] == o['checksum']


# ---------------------------------------------------------------- 看板

def test_board_shows_all_entities_and_reasons(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXBD')
    scan_all(cl, b)
    r = cl.post('/api/tool-boxes/BXBD/handover', json={'leader': '王负责', 'team': '甲班'})
    assert r.status_code == 200
    # 第二人争抢失败，留下带中签者的原因
    cl.post('/api/tool-boxes/BXBD/handover', json={'leader': '赵负责', 'team': '乙班'})
    bd = cl.get('/api/tools/board').json()
    assert len(bd['tools']) >= 15 and len(bd['locks']) == 15
    box = next(x for x in bd['boxes'] if x['id'] == 'BXBD')
    assert box['holder'] == '王负责' and box['complete'] is True
    scenes = {x['scene'] for x in box['reasons']}
    assert 'HANDOVER_OK' in scenes and 'HANDOVER_CONFLICT' in scenes
    cs = next(l['checksum'] for l in box['lines'] if l.get('checksum'))
    assert cs.startswith('tlc.')
    # 空库看板也不应报错
    import os
    import server as _s
    empty = os.path.join(os.path.dirname(dbpath(c)), 'empty.db')
    with TestClient(_s.create_app(empty)) as cl2:
        j = cl2.get('/api/tools/board').json()
        assert j['boxes'] == [] and j['locks'] == [] and j['tools']


# ---------------------------------------------------------------- 撤回触发库存回滚

def test_abort_uncomplete_box_restocks_and_releases_locks(c):
    cl = client(c)
    t = make_ticket(cl)
    lines = lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXAB')
    scan(cl, 'BXAB', lines[0]['serial'])
    scan(cl, 'BXAB', lines[1]['serial'])
    r = cl.post('/api/tool-boxes/BXAB/abort', json={'actor': '库管员', 'reason': '计划变更'})
    assert r.status_code == 200, r.text
    j = r.json()
    assert j['status'] == 'CANCELLED' and len(j['restocked']) == 2
    inv = cl.get('/api/tools/inventory').json()
    states = {x['serial']: x['state'] for x in inv['tools']}
    assert all(states[l['serial']] == 'FREE' for l in lines)
    assert cl.get(f"/api/tickets/{t['id']}/tool-locks").json()['lines'] == []
    # 已移交持有箱不能作废回滚（必须归还清点）：同票重新锁定/建箱，齐套移交后再试
    lock(cl, t['id'])
    b2 = make_box(cl, t['id'], 'BXAB2')
    scan_all(cl, b2)
    handover_ok(cl, 'BXAB2', '王负责', '甲班')
    r = cl.post('/api/tool-boxes/BXAB2/abort', json={})
    assert r.status_code == 409 and r.json()['detail']['reason_code'] == 'BOX_NOT_ABORTABLE'


def test_withdraw_ticket_rolls_back_locks(c):
    cl = client(c)
    t = make_ticket(cl)
    lines = lock(cl, t['id'])
    r = cl.post(f"/api/tickets/{t['id']}/withdraw", json={'actor': '调度A', 'reason': '计划取消'})
    assert r.status_code == 200
    inv = cl.get('/api/tools/inventory').json()
    states = {x['serial']: x['state'] for x in inv['tools']}
    assert all(states[l['serial']] == 'FREE' for l in lines)
    assert cl.get(f"/api/tickets/{t['id']}/tool-locks").json()['lines'] == []


# ---------------------------------------------------------------- 进程重启恢复

def test_restart_resumes_pending_and_preserves_checksums(c):
    cl = client(c)
    path = dbpath(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXRST')
    serials = [l['serial'] for l in b['lines']]
    # 断网：位置 1、3 先到（3 挂起）
    cl.post('/api/tool-scans/flush', json={'client_id': 'PDA-R', 'items': [
        {'client_seq': 1, 'box': 'BXRST', 'serial': serials[0]},
        {'client_seq': 3, 'box': 'BXRST', 'serial': serials[2]},
    ]})
    cs_before = cl.get('/api/tool-scans/PDA-R').json()[0]['result']['checksum']

    # 模拟服务进程重启：用同一 SQLite 文件重新建 app（全部状态从事务化持久化恢复）
    import server
    with TestClient(server.create_app(path)) as cl2:
        # 挂起事件仍在
        evs = cl2.get('/api/tool-scans/PDA-R').json()
        assert {e['client_seq']: e['status'] for e in evs} == {1: 'ACCEPTED', 3: 'PENDING'}
        # 校验串复核在重启后仍一致（密钥持久化）
        v = cl2.get(f'/api/tool-boxes/BXRST/verify/{serials[0]}').json()
        assert v['valid'] is True and v['stored'] == cs_before
        # 补前序 2 → 3 唤醒，继续处理
        r = cl2.post('/api/tool-scans/flush', json={'client_id': 'PDA-R', 'items': [
            {'client_seq': 2, 'box': 'BXRST', 'serial': serials[1]},
        ]}).json()
        assert [x['client_seq'] for x in r['woken']] == [3]
        assert all(e['status'] == 'ACCEPTED'
                   for e in cl2.get('/api/tool-scans/PDA-R').json())
        # 箱状态也恢复
        bv = cl2.get('/api/tool-boxes/BXRST').json()
        assert bv['issued'] == 3


def test_restart_preserves_held_ownership_and_review(c):
    cl = client(c)
    path = dbpath(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXH2')
    scan_all(cl, b)
    handover_ok(cl, 'BXH2', '王负责', '甲班')
    import server
    with TestClient(server.create_app(path)) as cl2:
        bv = cl2.get('/api/tool-boxes/BXH2').json()
        assert bv['status'] == 'HELD' and bv['holder'] == '王负责'
        # 重启后争抢仍然失败
        r = cl2.post('/api/tool-boxes/BXH2/handover', json={'leader': '赵负责', 'team': '乙班'})
        assert r.status_code == 409 and r.json()['detail']['reason_code'] == 'HOLDER_TAKEN'


# ---------------------------------------------------------------- 校验串复核

def test_checksum_recompute(c):
    cl = client(c)
    t = make_ticket(cl)
    lock(cl, t['id'])
    b = make_box(cl, t['id'], 'BXV')
    out = scan_all(cl, b)
    for l, o in zip(b['lines'], out):
        v = cl.get(f"/api/tool-boxes/BXV/verify/{l['serial']}").json()
        assert v['valid'] is True
        assert v['stored'] == o['checksum']
        assert v['batch'] == l['batch']
