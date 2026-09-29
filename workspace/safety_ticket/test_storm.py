"""雷暴撤离与复工闸门端到端用例。"""
import concurrent.futures
import pytest
from fastapi.testclient import TestClient
from server import create_app


@pytest.fixture()
def c(tmp_path):
    app = create_app(str(tmp_path / 'storm.db'))
    with TestClient(app) as client:
        yield client


def step(client, t, s, reporter='张三', verifier='李四'):
    if s['kind'] in ('OPEN_SWITCH', 'CLOSE_SWITCH'):
        r = client.post('/sim/switch', json={
            'device': s['device'], 'state': s['expected'], 'ticket': t['id']})
    else:
        r = client.post('/sim/ground', json={
            'point': s['point'], 'sw': s['sw'],
            'action': 'install' if s['kind'] == 'INSTALL_GROUND' else 'remove',
            'ticket': t['id']})
    assert r.status_code == 200, r.text
    r = client.post(f"/api/tickets/{t['id']}/steps/{s['id']}/report",
                    json={'actor': reporter, 'scanned': s['device']})
    assert r.status_code == 200, r.text
    r = client.post(f"/api/tickets/{t['id']}/steps/{s['id']}/verify", json={'actor': verifier})
    assert r.status_code == 200, r.text


def permitted_ticket(client, work=('F2',), lock_tools=False):
    t = client.post('/api/tickets', json={
        'title': '雷暴测试', 'work_area': list(work), 'created_by': '调度员'}).json()
    for s in t['steps']:
        step(client, t, s)
    t = client.post(f"/api/tickets/{t['id']}/permit", json={'actor': '调度员'}).json()
    if lock_tools:
        lock = client.post(f"/api/tickets/{t['id']}/tool-locks", json={'actor': '库管员'}).json()
        box = client.post(f"/api/tickets/{t['id']}/tool-boxes", json={'actor': '库管员'}).json()
        client.post(f"/api/tool-boxes/{box['id']}/scan",
                    json={'client_id': 'PDA', 'serial': lock['lines'][0]['serial']})
    return t


def evac(client, t):
    r = client.post('/api/storm/demo/alert-order', json={'ticket': t['id'], 'actor': '值班长'})
    assert r.status_code == 200, r.text
    return r.json()['evacuation']


def settle(client, bid, *, skip_person=None, skip_tools=0, location='一号避险点工器具筐'):
    b = client.get(f'/api/storm/batches/{bid}').json()
    onsite = [p for p in b['roster'] if p['onsite'] and p['name'] != skip_person]
    for p in onsite:
        shelter = 'SH-B' if p['name'] == '李四' else 'SH-A'
        r = client.post(f'/api/storm/batches/{bid}/checkins',
                        json={'person': p['id'], 'shelter': shelter})
        assert r.status_code == 200, r.text
    for risk in b['risks']:
        r = client.post(f'/api/storm/batches/{bid}/risk-guards',
                        json={'risk': risk['id'], 'guardian': 'P-WANG', 'shelter': 'SH-A'})
        assert r.status_code == 200, r.text
    for tool in b['tools'][skip_tools:]:
        r = client.post(f'/api/storm/batches/{bid}/tool-reports', json={
            'serial': tool['serial'], 'location': location,
            'held_by': 'P-WANG', 'shelter': 'SH-A', 'actor': 'P-CHEN'})
        assert r.status_code == 200, r.text
    return client.get(f'/api/storm/batches/{bid}').json()


def clear_approve_resume(client, alert, bid):
    assert client.post(f'/api/storm/alerts/{alert}/clear', json={'actor': '气象员'}).status_code == 200
    r = client.post(f'/api/storm/batches/{bid}/field-confirm', json={'actor': 'P-CHEN'})
    assert r.status_code == 200, r.text
    r = client.post(f'/api/storm/batches/{bid}/dispatch-confirm', json={'actor': '调度员'})
    assert r.status_code == 200, r.text
    r = client.post(f'/api/storm/batches/{bid}/resume', json={'actor': '值班长'})
    assert r.status_code == 200, r.text
    return r.json()


def test_evacuation_snapshots_and_full_double_release(c):
    t = permitted_ticket(c)
    e = c.post('/api/storm/demo/alert-order', json={'ticket': t['id']})
    b = e.json()['evacuation']
    assert b['status'] == 'EVACUATING'
    assert len(b['roster']) == 8
    assert sum(1 for p in b['roster'] if p['onsite']) == 7
    assert b['risks'] and all(x['energized'] for x in b['risks'])
    assert c.get(f"/api/tickets/{t['id']}").json()['frozen']

    b = settle(c, b['id'])
    assert b['all_accounted_for'] and b['all_tools_located'] and b['all_risks_guarded']
    assert {d['name'] for d in b['distribution']} == {'一号避险点', '二号避险点'}

    # 气象未解除不能调度放行；该拒绝原因在另一个批次上演示，避免相同请求重放返回首次裁决。
    before_clear = c.post(f"/api/storm/batches/{b['id']}/dispatch-confirm", json={'actor': '气象未解除校验'})
    assert before_clear.status_code == 409
    assert before_clear.json()['detail']['reason_code'] == 'DISPATCH_CONFIRM_DENIED'

    assert c.post(f"/api/storm/alerts/{b['alert']}/clear", json={'actor': '气象员'}).status_code == 200
    assert c.post(f"/api/storm/batches/{b['id']}/field-confirm",
                  json={'actor': 'P-CHEN'}).status_code == 200
    # 旧现场确认仍针对同一撤离批次；气象解除后调度员可核验同一批最新依据。
    assert c.post(f"/api/storm/batches/{b['id']}/dispatch-confirm",
                  json={'actor': '调度员'}).status_code == 200
    resumed = c.post(f"/api/storm/batches/{b['id']}/resume", json={'actor': '值班长'}).json()
    assert resumed['status'] == 'RESUMED'
    assert not c.get(f"/api/tickets/{t['id']}").json()['frozen']


def test_missing_person_blocks_ticket(c):
    t = permitted_ticket(c)
    b = evac(c, t)
    b = settle(c, b['id'], skip_person='赵六')
    r = c.post(f"/api/storm/batches/{b['id']}/field-confirm", json={'actor': 'P-CHEN'})
    assert r.status_code == 409
    assert r.json()['detail']['reason_code'] == 'FIELD_CHECK_INCOMPLETE'
    assert '赵六' in r.json()['detail']['detail']


def test_proxy_offsite_unregistered_and_shelter_change_are_adjudicated(c):
    t = permitted_ticket(c)
    b = evac(c, t)
    proxy = c.post(f"/api/storm/batches/{b['id']}/checkins",
                   json={'person': 'P-ZHANG', 'shelter': 'SH-A', 'claimed_by': 'P-LI'})
    assert proxy.status_code == 200
    assert proxy.json()['reason_code'] == 'PROXY_SIGN_FORBIDDEN'
    offsite = c.post(f"/api/storm/batches/{b['id']}/checkins",
                     json={'person': 'P-ZHOU', 'shelter': 'SH-A'})
    assert offsite.json()['reason_code'] == 'PERSON_ALREADY_OFFSITE'
    ghost = c.post(f"/api/storm/batches/{b['id']}/checkins",
                   json={'person': '不存在的人', 'shelter': 'SH-A'})
    assert ghost.status_code == 404
    assert ghost.json()['detail']['reason_code'] == 'PERSON_NOT_REGISTERED'

    first = c.post(f"/api/storm/batches/{b['id']}/checkins",
                   json={'person': 'P-ZHANG', 'shelter': 'SH-A'}).json()
    dup = c.post(f"/api/storm/batches/{b['id']}/checkins",
                 json={'person': 'P-ZHANG', 'shelter': 'SH-A'}).json()
    assert dup['idempotent'] is True and dup['checkin_id'] == first['checkin_id']
    changed = c.post(f"/api/storm/batches/{b['id']}/correct-destination",
                     json={'person': 'P-ZHANG', 'shelter': 'SH-B'}).json()
    view = c.get(f"/api/storm/batches/{b['id']}").json()
    valid = [x for x in view['checkins'] if x['person'] == 'P-ZHANG']
    assert len(valid) == 1 and valid[0]['shelter'] == 'SH-B'
    assert changed['checkin_id'] > first['checkin_id']


def test_tool_conflict_requires_field_leader_resolution(c):
    t = permitted_ticket(c, lock_tools=True)
    b = evac(c, t)
    b = c.get(f"/api/storm/batches/{b['id']}").json()
    for tool in b['tools'][1:]:
        c.post(f"/api/storm/batches/{b['id']}/tool-reports", json={
            'serial': tool['serial'], 'location': '现场工具箱',
            'held_by': 'P-WANG', 'shelter': 'SH-A', 'actor': 'P-CHEN'})
    serial = b['tools'][0]['serial']
    assert any(x['kind'] == 'ISSUED' for x in b['tools'])
    r1 = c.post(f"/api/storm/batches/{b['id']}/tool-reports", json={
        'serial': serial, 'location': '一号避险点', 'held_by': 'P-WANG', 'actor': 'P-WANG'})
    r2 = c.post(f"/api/storm/batches/{b['id']}/tool-reports", json={
        'serial': serial, 'location': '车辆旁', 'held_by': 'P-LI', 'actor': 'P-LI'})
    assert r1.status_code == 200 and r2.status_code == 200
    assert r2.json()['reason_code'] == 'TOOL_LOCATION_CONFLICT'
    # 其他人不能强制覆盖，必须现场负责人裁决。
    not_leader = c.post(f"/api/storm/batches/{b['id']}/tool-reports", json={
        'serial': serial, 'location': '二号避险点', 'held_by': 'P-LI',
        'actor': 'P-LI', 'force_correct': True})
    assert not_leader.json()['ok'] is False
    resolved = c.post(f"/api/storm/batches/{b['id']}/tool-reports", json={
        'serial': serial, 'location': '一号避险点工器具筐', 'held_by': 'P-WANG',
        'shelter': 'SH-A', 'actor': 'P-CHEN', 'force_correct': True})
    assert resolved.json()['reason_code'] == 'TOOL_LOCATION_CORRECTED'
    view = c.get(f"/api/storm/batches/{b['id']}").json()
    assert view['all_tools_located'] is True


def test_environment_change_invalidates_old_confirms(c):
    t = permitted_ticket(c)
    b = evac(c, t)
    b = settle(c, b['id'])
    c.post(f"/api/storm/alerts/{b['alert']}/clear", json={'actor': '气象员'})
    assert c.post(f"/api/storm/batches/{b['id']}/field-confirm",
                  json={'actor': 'P-CHEN'}).status_code == 200
    r = c.post('/sim/topology', json={'op': 'add_edge', 'a': 'N1', 'b': 'N2'})
    assert r.status_code == 200
    view = c.get(f"/api/storm/batches/{b['id']}").json()
    assert view['field_confirm']['status'] == 'INVALID'
    assert view['field_confirm']['reason_code'] == 'ENVIRONMENT_CHANGED'
    denied = c.post(f"/api/storm/batches/{b['id']}/dispatch-confirm", json={'actor': '调度员'})
    assert denied.status_code == 409
    assert '现场负责人尚未确认' in denied.json()['detail']['detail']


def test_second_alert_preempts_waiting_batch_but_keeps_facts(c):
    t = permitted_ticket(c)
    b = evac(c, t)
    bid = b['id']
    settle(c, bid)
    c.post(f"/api/storm/alerts/{b['alert']}/clear", json={'actor': '气象员'})
    c.post(f"/api/storm/batches/{bid}/field-confirm", json={'actor': 'P-CHEN'})
    c.post(f"/api/storm/batches/{bid}/dispatch-confirm", json={'actor': '调度员'})
    a2 = c.post('/api/storm/alerts', json={
        'ticket': t['id'], 'title': '第二次雷暴', 'areas': ['F2'], 'actor': '值班长'}).json()
    view = c.get(f'/api/storm/batches/{bid}').json()
    assert view['status'] == 'SUPERSEDED'
    assert len(view['checkins']) == 7
    assert c.post(f"/api/storm/batches/{bid}/resume", json={'actor': '值班长'}).status_code == 409
    assert c.get(f"/api/tickets/{t['id']}").json()['frozen']
    new_order = c.post(f"/api/tickets/{t['id']}/storm-evacuation",
                       json={'alert': a2['alert_id'], 'field_leader': '陈七'})
    assert new_order.status_code == 200
    assert new_order.json()['seq'] == 2


def test_replays_do_not_duplicate_records(c):
    t = permitted_ticket(c)
    b = evac(c, t)
    payload = {'person': 'P-WANG', 'shelter': 'SH-A', 'key': 'fixed-key'}
    r1 = c.post(f"/api/storm/batches/{b['id']}/checkins", json=payload).json()
    r2 = c.post(f"/api/storm/batches/{b['id']}/checkins", json=payload).json()
    assert r1['checkin_id'] == r2['checkin_id']
    assert c.post(f"/api/storm/batches/{b['id']}/risk-guards",
                  json={'risk': b['risks'][0]['id'], 'guardian': 'P-WANG',
                        'shelter': 'SH-A', 'key': 'g1'}).status_code == 200
    assert c.post(f"/api/storm/batches/{b['id']}/risk-guards",
                  json={'risk': b['risks'][0]['id'], 'guardian': 'P-WANG',
                        'shelter': 'SH-A', 'key': 'g1'}).status_code == 200
    view = c.get(f"/api/storm/batches/{b['id']}").json()
    assert len(view['checkins']) == 1
    assert len(view['risk_guards']) == 1


def test_restart_restores_batch_and_flow_continues(c):
    t = permitted_ticket(c)
    b = evac(c, t)
    bid = b['id']
    alert = b['alert']
    settle(c, bid)
    c.post('/api/admin/reopen')
    restored = c.get(f'/api/storm/batches/{bid}').json()
    assert restored['id'] == bid and len(restored['checkins']) == 7
    clear_approve_resume(c, alert, bid)
    assert not c.get(f"/api/tickets/{t['id']}").json()['frozen']


# ------------------------------------------------------------ 并发最终阶段
def test_concurrent_orders_and_confirms_have_single_winner(c):
    t = permitted_ticket(c)
    alert = c.post('/api/storm/alerts', json={
        'ticket': t['id'], 'areas': ['F2'], 'actor': '值班长'}).json()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(
            c.post, f"/api/tickets/{t['id']}/storm-evacuation",
            json={'alert': alert['alert_id'], 'field_leader': '陈七', 'issued_by': f'值班长{i}'})
            for i in range(5)]
        statuses = [f.result().status_code for f in futures]
    assert statuses.count(200) == 5
    board = c.get('/api/storm/board').json()
    batches = [b for b in board['batches'] if b['ticket'] == t['id']]
    assert len(batches) == 1
    assert all(f.result().json().get('id') == batches[0]['id'] for f in futures)

    bid = batches[0]['id']
    settle(c, bid)
    alert = batches[0]['alert']
    c.post(f'/api/storm/alerts/{alert}/clear', json={'actor': '气象员'})
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        field_futures = [pool.submit(c.post, f'/api/storm/batches/{bid}/field-confirm',
                                     json={'actor': 'P-CHEN', 'key': f'field-parallel-{i}'}) for i in range(3)]
        field_results = [f.result() for f in field_futures]
        dispatch_futures = [pool.submit(c.post, f'/api/storm/batches/{bid}/dispatch-confirm',
                                        json={'actor': '调度员', 'key': f'dispatch-parallel-{i}'}) for i in range(3)]
        dispatch_results = [f.result() for f in dispatch_futures]
        assert len({r.json().get('confirm_id') for r in field_results if r.status_code == 200}) == 1
        assert len({r.json().get('confirm_id') for r in dispatch_results if r.status_code == 200}) == 1
    assert c.post(f'/api/storm/batches/{bid}/dispatch-confirm',
                  json={'actor': '调度员'}).status_code == 200
    view = c.get(f'/api/storm/batches/{bid}').json()
    assert view['status'] == 'DISPATCH_CONFIRMED'
