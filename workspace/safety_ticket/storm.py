"""雷暴撤离与复工闸门。

该模块只依赖 server.DB 的 SQLite 连接/锁与少量纯函数，负责把气象告警、撤离批次、
人员避险签到、风险监护、工器具去向和双角色复工确认全部事务化落库。
"""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from typing import Any, Callable, Optional

from fastapi import HTTPException

# 为避免与 server.py 形成循环导入，拓扑/票据辅助函数由 create_app 注册时注入。
load_topo: Optional[Callable] = None
get_ticket: Optional[Callable] = None
get_steps: Optional[Callable] = None
ev: Optional[Callable] = None
close_handovers: Optional[Callable] = None


def register_helpers(*, load_topo_fn, get_ticket_fn, get_steps_fn, ev_fn, close_handovers_fn=None):
    global load_topo, get_ticket, get_steps, ev, close_handovers
    load_topo = load_topo_fn
    get_ticket = get_ticket_fn
    get_steps = get_steps_fn
    ev = ev_fn
    close_handovers = close_handovers_fn


import domain
import tools as tools_mod

# ------------------------------------------------------------ 阶段常量
ALERT_ACTIVE = 'ACTIVE'
ALERT_CLEARED = 'CLEARED'
ALERT_SUPERSEDED = 'SUPERSEDED'

B_EVACUATING = 'EVACUATING'      # 已发撤离令，仍有人/器/风险未闭环
B_WAITING_RETURN = 'WAITING_RETURN'
B_DISPATCH_CONFIRMED = 'DISPATCH_CONFIRMED'
B_RESUMED = 'RESUMED'
B_SUPERSEDED = 'SUPERSEDED'
B_TICKET_TERMINAL = 'TICKET_TERMINAL'
B_OPEN_STATES = (B_EVACUATING, B_WAITING_RETURN, B_DISPATCH_CONFIRMED)

DEC_ACCEPTED = 'ACCEPTED'
DEC_REJECTED = 'REJECTED'
DEC_SUPERSEDED = 'SUPERSEDED'
DEC_INVALID = 'INVALID'

TOOL_ISSUED = 'ISSUED'
TOOL_RESERVED = 'RESERVED'
TOOL_UNKNOWN = 'UNKNOWN'
TOOL_CONFLICT = 'CONFLICT'
TOOL_LOCATED = 'LOCATED'


SCHEMA = """
CREATE TABLE IF NOT EXISTS storm_alerts(
    id TEXT PRIMARY KEY, ticket TEXT, severity TEXT, title TEXT, areas TEXT,
    env_version INTEGER, status TEXT DEFAULT 'ACTIVE', cleared_by TEXT,
    issued_by TEXT, issued_at REAL, cleared_at REAL, reason TEXT, digest TEXT);
CREATE TABLE IF NOT EXISTS storm_shelters(
    id TEXT PRIMARY KEY, name TEXT, qr TEXT, active INTEGER DEFAULT 1, created_at REAL);
CREATE TABLE IF NOT EXISTS storm_roster(
    id TEXT PRIMARY KEY, name TEXT, role TEXT, onsite INTEGER DEFAULT 1,
    active INTEGER DEFAULT 1, updated_at REAL, UNIQUE(name));
CREATE TABLE IF NOT EXISTS storm_batches(
    id TEXT PRIMARY KEY, ticket TEXT, alert TEXT, seq INTEGER, field_leader TEXT,
    dispatcher TEXT, status TEXT, snapshot_topo INTEGER, env_version INTEGER,
    env_hash TEXT, roster_json TEXT, risks_json TEXT, tools_json TEXT,
    issued_by TEXT, issued_at REAL, resumed_by TEXT, resumed_at REAL,
    terminal_reason TEXT, ticket_stage TEXT, UNIQUE(ticket, seq));
CREATE UNIQUE INDEX IF NOT EXISTS idx_storm_open_batch
    ON storm_batches(ticket) WHERE status IN ('EVACUATING','WAITING_RETURN','DISPATCH_CONFIRMED');
CREATE TABLE IF NOT EXISTS storm_checkins(
    id INTEGER PRIMARY KEY AUTOINCREMENT, batch TEXT, person TEXT, shelter TEXT,
    claimed_by TEXT, safe INTEGER DEFAULT 1, status TEXT DEFAULT 'VALID', superseded_by INTEGER,
    key TEXT, reason_code TEXT, detail TEXT, created_at REAL);
CREATE UNIQUE INDEX IF NOT EXISTS idx_storm_valid_checkin
    ON storm_checkins(batch, person) WHERE status='VALID';
CREATE UNIQUE INDEX IF NOT EXISTS idx_storm_checkin_key
    ON storm_checkins(key) WHERE key IS NOT NULL;
CREATE TABLE IF NOT EXISTS storm_risk_guards(
    id INTEGER PRIMARY KEY AUTOINCREMENT, batch TEXT, risk_id TEXT, guardian TEXT,
    shelter TEXT, key TEXT, status TEXT DEFAULT 'VALID', superseded_by INTEGER,
    reason_code TEXT, detail TEXT, created_at REAL);
CREATE UNIQUE INDEX IF NOT EXISTS idx_storm_valid_guard
    ON storm_risk_guards(batch, risk_id) WHERE status='VALID';
CREATE UNIQUE INDEX IF NOT EXISTS idx_storm_guard_key
    ON storm_risk_guards(key) WHERE key IS NOT NULL;
CREATE TABLE IF NOT EXISTS storm_tool_reports(
    id INTEGER PRIMARY KEY AUTOINCREMENT, batch TEXT, serial TEXT, location TEXT,
    held_by TEXT, shelter TEXT, key TEXT, status TEXT DEFAULT 'VALID',
    superseded_by INTEGER, reason_code TEXT, detail TEXT, created_at REAL);
CREATE UNIQUE INDEX IF NOT EXISTS idx_storm_valid_tool
    ON storm_tool_reports(batch, serial) WHERE status='VALID';
CREATE UNIQUE INDEX IF NOT EXISTS idx_storm_tool_key
    ON storm_tool_reports(key) WHERE key IS NOT NULL;
CREATE TABLE IF NOT EXISTS storm_field_confirms(
    id INTEGER PRIMARY KEY AUTOINCREMENT, batch TEXT, actor TEXT,
    env_version INTEGER, env_hash TEXT, status TEXT DEFAULT 'VALID',
    key TEXT UNIQUE, reason_code TEXT, detail TEXT, created_at REAL);
CREATE UNIQUE INDEX IF NOT EXISTS idx_storm_field_valid
    ON storm_field_confirms(batch) WHERE status='VALID';
CREATE TABLE IF NOT EXISTS storm_dispatch_confirms(
    id INTEGER PRIMARY KEY AUTOINCREMENT, batch TEXT, actor TEXT,
    field_confirm_id INTEGER, env_version INTEGER, env_hash TEXT,
    weather_version INTEGER, topo_version INTEGER, ticket_status TEXT,
    boundary_hash TEXT, status TEXT DEFAULT 'VALID', key TEXT UNIQUE,
    reason_code TEXT, detail TEXT, created_at REAL);
CREATE UNIQUE INDEX IF NOT EXISTS idx_storm_dispatch_valid
    ON storm_dispatch_confirms(batch) WHERE status='VALID';
CREATE TABLE IF NOT EXISTS storm_decisions(
    id INTEGER PRIMARY KEY AUTOINCREMENT, batch TEXT, scene TEXT, natural_key TEXT,
    result TEXT, reason_code TEXT, detail TEXT, response TEXT, actor TEXT,
    created_at REAL, UNIQUE(scene, natural_key));
CREATE INDEX IF NOT EXISTS idx_storm_decisions_batch ON storm_decisions(batch, id);
"""


class StormError(HTTPException):
    def __init__(self, status_code: int, reason_code: str, detail: str, batch: Optional[str] = None):
        super().__init__(status_code, {'reason_code': reason_code, 'detail': detail, 'batch': batch})
        self.reason_code = reason_code
        self.detail_text = detail
        self.batch = batch


# ------------------------------------------------------------ 基础工具

def ensure_schema(db):
    with db.lock:
        db.conn.executescript(SCHEMA)
        cols = [r['name'] for r in db.conn.execute('PRAGMA table_info(storm_batches)')]
        if 'ticket_stage' not in cols:
            db.conn.execute('ALTER TABLE storm_batches ADD COLUMN ticket_stage TEXT')
        db.commit()


def _now():
    return time.time()


def _new_id(prefix: str) -> str:
    return prefix + time.strftime('%H%M%S') + uuid.uuid4().hex[:8].upper()


def _loads(v, default):
    if not v:
        return default
    return json.loads(v)


def _j(v):
    return json.dumps(v, ensure_ascii=False, sort_keys=True)


def _digest(obj: Any) -> str:
    return hashlib.sha256(_j(obj).encode()).hexdigest()


def _audit(db, batch, actor, etype, **payload):
    ticket = None
    if batch:
        r = db.one('SELECT ticket FROM storm_batches WHERE id=?', (batch,))
        ticket = r['ticket'] if r else None
    ev(db, ticket, actor, etype, batch=batch, **payload)


def _store_decision(db, *, batch, scene, natural_key, result, reason_code, detail, response, actor):
    """以业务自然键保存裁决；重放返回最初响应。"""
    try:
        db.x(
            'INSERT INTO storm_decisions(batch,scene,natural_key,result,reason_code,detail,response,actor,created_at) '
            'VALUES(?,?,?,?,?,?,?,?,?)',
            (batch, scene, natural_key, result, reason_code, detail,
             json.dumps(response, ensure_ascii=False), actor, _now()))
    except Exception:
        # 唯一冲突时由调用方读取首次结果；不要让审计写入覆盖业务事务
        db.rollback()
        row = db.one('SELECT response FROM storm_decisions WHERE scene=? AND natural_key=?',
                     (scene, natural_key))
        return json.loads(row['response']), True
    return response, False


def _first_or_run(db, scene, natural_key, producer):
    row = db.one('SELECT response FROM storm_decisions WHERE scene=? AND natural_key=?',
                 (scene, natural_key))
    if row:
        return json.loads(row['response']), True
    result, reason_code, detail, response = producer()
    return _store_decision(
        db, batch=response.get('batch_id'), scene=scene, natural_key=natural_key,
        result=result, reason_code=reason_code, detail=detail, response=response,
        actor=response.get('actor', 'system'))[0], False


def _stored_raise(db, scene, natural_key, producer):
    """执行业务并持久化拒绝裁决；同一请求重放时抛出首次状态与原因。"""
    row = db.one('SELECT response FROM storm_decisions WHERE scene=? AND natural_key=?',
                 (scene, natural_key))
    if row:
        old = json.loads(row['response'])
        if not old.get('ok', True):
            raise StormError(old.get('http_status', 409), old.get('reason_code', 'STALE_REPLAY'),
                            old.get('detail', '相同请求重放，返回首次裁决'), old.get('batch_id'))
        return old, True
    try:
        result, reason_code, detail, response = producer()
    except StormError as e:
        resp = {'ok': False, 'http_status': e.status_code, 'batch_id': e.batch,
                'reason_code': e.reason_code, 'detail': e.detail_text}
        _store_decision(db, batch=e.batch, scene=scene, natural_key=natural_key,
                        result=DEC_REJECTED, reason_code=e.reason_code, detail=e.detail_text,
                        response=resp, actor='system')
        db.commit()
        raise
    return _store_decision(
        db, batch=response.get('batch_id'), scene=scene, natural_key=natural_key,
        result=result, reason_code=reason_code, detail=detail, response=response,
        actor=response.get('actor', 'system'))[0], False


def seed_storm(db, force=False):
    with db.lock:
        db.conn.executescript(SCHEMA)
        batch_cols = [r['name'] for r in db.conn.execute('PRAGMA table_info(storm_batches)')]
        if 'ticket_stage' not in batch_cols:
            db.conn.execute('ALTER TABLE storm_batches ADD COLUMN ticket_stage TEXT')
        if force or not db.one('SELECT 1 FROM storm_shelters LIMIT 1'):
            if force:
                db.x('DELETE FROM storm_shelters')
            for sid, name in [('SH-A', '一号避险点'), ('SH-B', '二号避险点')]:
                db.x('INSERT OR IGNORE INTO storm_shelters(id,name,qr,active,created_at) VALUES(?,?,?,1,?)',
                     (sid, name, f'QR-{sid}', _now()))
        if force or not db.one('SELECT 1 FROM storm_roster LIMIT 1'):
            if force:
                db.x('DELETE FROM storm_roster')
            people = [
                ('P-ZHANG', '张三', 'WORKER'), ('P-LI', '李四', 'WORKER'),
                ('P-WANG', '王五', 'WORKER'), ('P-ZHAO', '赵六', 'WORKER'),
                ('P-CHEN', '陈七', 'FIELD_LEADER'), ('P-SUN', '孙八', 'WORKER'),
                ('P-ZHOU', '周九', 'OFFSITE'), ('P-WU', '吴十', 'WORKER'),
            ]
            for pid, name, role in people:
                onsite = 0 if role == 'OFFSITE' else 1
                db.x('INSERT OR IGNORE INTO storm_roster(id,name,role,onsite,active,updated_at) VALUES(?,?,?,1,1,?)',
                     (pid, name, role, _now()))
                db.x('UPDATE storm_roster SET onsite=? WHERE id=?', (onsite, pid))
        db.commit()


# ------------------------------------------------------------ 环境版本

def _next_env(db) -> int:
    try:
        v = int(db.one("SELECT v FROM meta WHERE k='storm_env_version'")['v']) + 1
    except Exception:
        v = 1
    db.set_meta('storm_env_version', str(v))
    return v


def _active_alert(db, ticket: str):
    r = db.one('SELECT * FROM storm_alerts WHERE ticket=? AND status=? ORDER BY issued_at DESC,rowid DESC LIMIT 1',
               (ticket, ALERT_ACTIVE))
    return dict(r) if r else None


def _boundary_problem_hash(db, t, topo):
    problems = domain.boundary_problems(topo, t, get_steps(db, t['id']))
    return problems, hashlib.sha256(_j(problems).encode()).hexdigest()[:16]


def _current_env(db, t):
    """复工依据的当前版本：气象、拓扑、票据阶段、隔离边界共同构成。"""
    topo = load_topo(db)
    problems, boundary_hash = _boundary_problem_hash(db, t, topo)
    alert = _active_alert(db, t['id'])
    weather_version = alert['env_version'] if alert else 0
    env_obj = {
        'weather': weather_version,
        'topology': topo.version,
        'ticket_status': t['status'],
        'frozen': int(bool(t['frozen'])),
        'boundary_hash': boundary_hash,
        'boundary_problems': problems,
        'active_alert': alert['id'] if alert else None,
    }
    env_hash = _digest(env_obj)
    return topo, env_obj, env_hash, problems, weather_version


def _invalidate_confirms(db, batch_id: int, reason_code: str, detail: str, new_env: int):
    """依据变化使旧确认失效；事实记录不删除。"""
    db.x('UPDATE storm_batches SET env_version=? WHERE id=?', (new_env, batch_id))
    for table, col in [('storm_field_confirms', 'id'), ('storm_dispatch_confirms', 'id')]:
        rows = db.q(f"SELECT {col} FROM {table} WHERE batch=? AND status='VALID'", (batch_id,))
        for r in rows:
            db.x(f"UPDATE {table} SET status=?, reason_code=?, detail=? WHERE {col}=?",
                 (DEC_INVALID, reason_code, detail, r[col]))
    if db.one("SELECT 1 FROM storm_field_confirms WHERE batch=? AND status='VALID'", (batch_id,)):
        # 只有现场确认仍有效，才可能停留在“待调度”；调度确认有效时两项都还在。
        db.x("UPDATE storm_batches SET status=? WHERE id=?", (B_WAITING_RETURN, batch_id))
    else:
        db.x("UPDATE storm_batches SET status=? WHERE id=?", (B_EVACUATING, batch_id))


def refresh_environments(db, actor: str = 'system', reason: str = '复工依据变化', ticket: Optional[str] = None):
    """拓扑/气象/票据变化后刷新未结束批次，令旧确认失效。调用方持锁并负责提交。"""
    sql = "SELECT * FROM storm_batches WHERE status IN (?,?,?)"
    args: list[Any] = list(B_OPEN_STATES)
    if ticket:
        sql += ' AND ticket=?'
        args.append(ticket)
    changed = []
    for r in db.q(sql, args):
        b = dict(r)
        try:
            t = get_ticket(db, b['ticket'])
        except HTTPException:
            continue
        _, env_obj, env_hash, _, _ = _current_env(db, t)
        if env_hash != b['env_hash'] or env_obj['topology'] != b['snapshot_topo']:
            nv = _next_env(db)
            db.x('UPDATE storm_batches SET env_version=?, env_hash=?, snapshot_topo=? WHERE id=?',
                 (nv, env_hash, env_obj['topology'], b['id']))
            _invalidate_confirms(db, b['id'], 'ENVIRONMENT_CHANGED', reason, nv)
            _audit(db, b['id'], actor, 'STORM_ENV_CHANGED', old_hash=b['env_hash'],
                   new_hash=env_hash, env_version=nv, reason=reason, env=env_obj)
            changed.append(b['id'])
    return changed


def mark_ticket_terminal(db, ticket: str, actor: str, reason: str):
    rows = db.q("SELECT * FROM storm_batches WHERE ticket=? AND status IN (?,?,?)",
                (ticket, *B_OPEN_STATES))
    for r in rows:
        b = dict(r)
        nv = _next_env(db)
        db.x("UPDATE storm_batches SET status=?, terminal_reason=?, env_version=? WHERE id=?",
             (B_TICKET_TERMINAL, reason, nv, b['id']))
        for table in ('storm_field_confirms', 'storm_dispatch_confirms'):
            db.x(f"UPDATE {table} SET status=?, reason_code=?, detail=? WHERE batch=? AND status='VALID'",
                 (DEC_SUPERSEDED, 'TICKET_TERMINAL', reason, b['id']))
        _audit(db, b['id'], actor, 'STORM_BATCH_TERMINAL', reason=reason)
    db.commit()


# ------------------------------------------------------------ 气象告警/撤离

def _roster_rows(db):
    return [dict(r) for r in db.q('SELECT * FROM storm_roster WHERE active=1 ORDER BY id')]


def _risk_snapshot(db, t, topo):
    energized = topo.energized()
    risks = []
    for sw in t['boundary'].get('switches', []):
        dev = sw['device']
        # 断开的边界开关本身不在带电可达集合内，但其电源侧端子可能仍带电；
        # 撤离时把“仍有带电侧、且必须保持断开”的边界固定为风险点。
        live_sides = [m for m in topo.adj.get(dev, ()) if m in energized]
        if dev in energized or live_sides or topo.devices.get(dev, {}).get('state') != 'OPEN':
            risks.append({
                'id': f'RISK:{dev}', 'type': 'ENERGIZED_BOUNDARY_SWITCH',
                'device': dev, 'side': sw.get('side'), 'state': topo.devices[dev]['state'],
                'energized': dev in energized or bool(live_sides),
                'live_sides': sorted(live_sides),
                'rule': '保持断开并有人监护，禁止恢复作业前合闸',
            })
    # 已装接地线若上游反送电，是最高优先级带电风险
    for point, sw, tid in topo.grounds:
        if tid == t['id'] and point in energized:
            risks.append({
                'id': f'RISK:GND:{sw}', 'type': 'ENERGIZED_GROUND',
                'device': f'GND:{sw}', 'point': point, 'state': 'GROUNDED',
                'energized': True, 'rule': '接地点带电，必须立即隔离并监护',
            })
    return risks


def _tool_snapshot(db, ticket: str):
    """固定现场工器具占有：已领出与仍锁定预留均视为本票关键占有物。"""
    out = []
    seen = set()
    for r in db.q('''SELECT c.serial, c.code, c.batch, c.box, c.actor, b.holder, b.holder_team,
                            b.status AS box_status
                     FROM checkouts c JOIN toolboxes b ON b.id=c.box
                     WHERE c.ticket=? ORDER BY c.serial''', (ticket,)):
        r = dict(r)
        seen.add(r['serial'])
        out.append({'serial': r['serial'], 'code': r['code'], 'batch': r['batch'],
                    'kind': TOOL_ISSUED, 'box': r['box'],
                    'holder': r['holder'] or r['actor'], 'holder_team': r['holder_team'],
                    'state_at_evacuation': TOOL_UNKNOWN,
                    'detail': f"已从工具箱 {r['box']} 领出"})
    for r in db.q('''SELECT l.item_serial serial,l.code,l.batch_id batch,l.id lock_id
                     FROM tool_locks l WHERE l.ticket=? AND l.state='LOCKED'
                     ORDER BY l.item_serial''', (ticket,)):
        r = dict(r)
        if r['serial'] in seen:
            continue
        out.append({'serial': r['serial'], 'code': r['code'], 'batch': r['batch'],
                    'kind': TOOL_RESERVED, 'box': None, 'holder': None,
                    'holder_team': None, 'state_at_evacuation': TOOL_UNKNOWN,
                    'detail': f"批次锁 {r['lock_id']} 仍占用，尚未确认现场去向"})
    return out


def create_alert_core(db, ticket: str, severity: str, title: str, areas: list[str],
                      actor: str, reason: Optional[str] = None, auto_order: bool = False):
    seed_storm(db)
    with db.lock:
        t = get_ticket(db, ticket)
        if t['status'] not in ('ISOLATING', 'PERMITTED', 'RESTORING'):
            raise StormError(409, 'TICKET_NOT_ACTIVE', f'票据状态 {t["status"]}，不能触发雷暴撤离')
        digest = _digest({'ticket': ticket, 'severity': severity, 'title': title,
                          'areas': sorted(areas), 'reason': reason})
        duplicate = db.one('SELECT id FROM storm_alerts WHERE ticket=? AND digest=? AND status=? ORDER BY rowid DESC LIMIT 1',
                           (ticket, digest, ALERT_ACTIVE))
        if duplicate:
            db.commit()
            resp = {'ok': True, 'idempotent': True, 'alert_id': duplicate['id'], 'ticket': ticket,
                    'env_version': int(db.one('SELECT env_version v FROM storm_alerts WHERE id=?',
                                              (duplicate['id'],))['v'])}
            if auto_order:
                b = db.one('SELECT id FROM storm_batches WHERE alert=? ORDER BY rowid DESC LIMIT 1',
                           (duplicate['id'],))
                resp['evacuation'] = batch_view(db, b['id']) if b else issue_order_core(
                    db, ticket, duplicate['id'], '陈七', '调度员', field_leader_name='陈七')
            return resp
        aid = _new_id('W')
        envv = _next_env(db)
        db.x('INSERT INTO storm_alerts(id,ticket,severity,title,areas,env_version,status,issued_by,issued_at,reason,digest) '
             'VALUES(?,?,?,?,?,?,?,?,?,?,?)',
             (aid, ticket, severity, title, json.dumps(areas, ensure_ascii=False), envv,
              ALERT_ACTIVE, actor, _now(), reason, digest))
        db.x('UPDATE storm_alerts SET status=?, cleared_at=? WHERE ticket=? AND id!=? AND status=?',
             (ALERT_SUPERSEDED, _now(), ticket, aid, ALERT_ACTIVE))
        # 第二次告警抢占任何待复工/撤离中批次；既成撤离和签到事实保留。
        for r in db.q("SELECT * FROM storm_batches WHERE ticket=? AND status IN (?,?,?)",
                      (ticket, *B_OPEN_STATES)):
            old = dict(r)
            db.x('UPDATE storm_batches SET status=?, terminal_reason=?, env_version=? WHERE id=?',
                 (B_SUPERSEDED, f'新告警 {aid} 抢占待复工批次', envv, old['id']))
            for table in ('storm_field_confirms', 'storm_dispatch_confirms'):
                db.x(f"UPDATE {table} SET status=?, reason_code=?, detail=? WHERE batch=? AND status='VALID'",
                     (DEC_SUPERSEDED, 'SUPERSEDED_BY_NEW_ALERT', f'新告警 {aid} 命中，旧确认失效', old['id']))
            _audit(db, old['id'], actor, 'STORM_BATCH_SUPERSEDED', new_alert=aid)
        db.x("UPDATE tickets SET frozen=1, freeze_reason=? WHERE id=? AND status IN ('ISOLATING','PERMITTED','RESTORING')",
             (f'雷暴告警 {aid}：{title}', ticket))
        if close_handovers:
            close_handovers(db, ticket, 'STORM_ALERT', f'雷暴告警 {aid} 命中作业区域', actor)
        _audit(db, None, actor, 'STORM_ALERT_CREATED', alert_id=aid, severity=severity,
               areas=areas, digest=digest)
        db.commit()
        resp = {'ok': True, 'alert_id': aid, 'ticket': ticket, 'env_version': envv,
                'superseded_batches': [dict(r)['id'] for r in db.q(
                    "SELECT id FROM storm_batches WHERE terminal_reason LIKE ?", (f'%{aid}%',))]}
        if auto_order:
            resp['evacuation'] = issue_order_core(db, ticket, aid, '陈七', '调度员',
                                                  field_leader_name='陈七')
        return resp


def issue_order_core(db, ticket: str, alert_id: Optional[str], field_leader_id: str,
                     issued_by: str, field_leader_name: Optional[str] = None):
    seed_storm(db)
    with db.lock:
        t = get_ticket(db, ticket)
        alert = None
        if alert_id:
            alert = db.one('SELECT * FROM storm_alerts WHERE id=? AND ticket=?', (alert_id, ticket))
            if not alert:
                raise StormError(404, 'ALERT_NOT_FOUND', f'告警 {alert_id} 不存在')
            alert = dict(alert)
            if alert['status'] != ALERT_ACTIVE:
                raise StormError(409, 'ALERT_NOT_ACTIVE', f'告警 {alert_id} 已解除，不能据此撤离')
        else:
            alert = _active_alert(db, ticket)
            if not alert:
                raise StormError(409, 'NO_ACTIVE_ALERT', '气象告警未命中作业区域，值班长不能发布撤离令')
            alert_id = alert['id']
        existing = db.one("SELECT id FROM storm_batches WHERE ticket=? AND alert=? AND status!=?",
                          (ticket, alert_id, B_SUPERSEDED))
        if existing:
            return batch_view(db, existing['id'])
        open_row = db.one("SELECT id FROM storm_batches WHERE ticket=? AND status IN (?,?,?)",
                          (ticket, *B_OPEN_STATES))
        if open_row:
            raise StormError(409, 'BATCH_ALREADY_OPEN', '同一票据已有未结束撤离批次', open_row['id'])
        leader = _person(db, field_leader_id)
        if not leader:
            raise StormError(404, 'LEADER_NOT_FOUND', f'现场负责人 {field_leader_id} 不在有效名册')
        topo, env_obj, env_hash, _, _ = _current_env(db, t)
        roster = _roster_rows(db)
        risks = _risk_snapshot(db, t, topo)
        tools = _tool_snapshot(db, ticket)
        seq = (db.one('SELECT COALESCE(MAX(seq),0)+1 n FROM storm_batches WHERE ticket=?', (ticket,))['n'])
        try:
            bid = _new_id('EB')
            db.x('''INSERT INTO storm_batches(id,ticket,alert,seq,field_leader,dispatcher,status,
                     snapshot_topo,env_version,env_hash,roster_json,risks_json,tools_json,issued_by,issued_at,ticket_stage)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                  (bid, ticket, alert_id, seq, leader['name'], None, B_EVACUATING,
                   topo.version, alert['env_version'], env_hash,
                   json.dumps(roster, ensure_ascii=False), json.dumps(risks, ensure_ascii=False),
                   json.dumps(tools, ensure_ascii=False), issued_by, _now(), t['status']))
        except Exception:
            db.rollback()
            winner = db.one("SELECT id FROM storm_batches WHERE ticket=? AND status IN (?,?,?)",
                            (ticket, *B_OPEN_STATES))
            if winner:
                return batch_view(db, winner['id'])
            raise
        db.x("UPDATE tickets SET frozen=1, freeze_reason=? WHERE id=? AND status IN ('ISOLATING','PERMITTED','RESTORING')",
             (f'雷暴撤离令 {bid}：全员撤离，复工须双角色放行', ticket))
        if close_handovers:
            close_handovers(db, ticket, 'STORM_EVACUATION', f'雷暴撤离令 {bid} 发布', issued_by)
        _audit(db, bid, issued_by, 'STORM_EVACUATION_ORDERED', alert=alert_id,
               roster=[p['id'] for p in roster], onsite=[p['id'] for p in roster if p['onsite']],
               risk_count=len(risks), tool_count=len(tools), topology=topo.version)
        db.commit()
        return batch_view(db, bid)


def clear_alert_core(db, alert_id: str, actor: str):
    with db.lock:
        r = db.one('SELECT * FROM storm_alerts WHERE id=?', (alert_id,))
        if not r:
            raise StormError(404, 'ALERT_NOT_FOUND', f'告警 {alert_id} 不存在')
        a = dict(r)
        if a['status'] == ALERT_CLEARED:
            return {'ok': True, 'alert_id': alert_id, 'status': ALERT_CLEARED, 'idempotent': True}
        nv = _next_env(db)
        affected = [dict(r)['id'] for r in db.q(
            "SELECT id FROM storm_batches WHERE alert=? AND status IN (?,?,?)",
            (alert_id, *B_OPEN_STATES))]
        db.x('UPDATE storm_alerts SET status=?, cleared_by=?, cleared_at=? WHERE id=?',
             (ALERT_CLEARED, actor, _now(), alert_id))
        # 解除气象只推进复工依据，不抹掉“在旧告警环境下尚未形成放行”的现场清点；
        # 调度放行必须在本次核验中看到 ACTIVE_ALERT 已消失。
        for r in db.q("SELECT * FROM storm_batches WHERE alert=? AND status IN (?,?,?)",
                      (alert_id, *B_OPEN_STATES)):
            t = get_ticket(db, r['ticket'])
            _, _, env_hash, _, _ = _current_env(db, t)
            db.x('UPDATE storm_batches SET env_version=?, env_hash=? WHERE id=?',
                 (nv, env_hash, r['id']))
            _audit(db, r['id'], actor, 'STORM_ENV_CHANGED', old_hash=r['env_hash'],
                   new_hash=env_hash, env_version=nv, reason=f'气象告警 {alert_id} 已解除',
                   confirmations_invalidated=False)
        _audit(db, None, actor, 'STORM_ALERT_CLEARED', alert_id=alert_id, env_version=nv,
               affected_batches=affected)
        db.commit()
        return {'ok': True, 'alert_id': alert_id, 'status': ALERT_CLEARED,
                'env_version': nv, 'affected_batches': affected}


# ------------------------------------------------------------ 视图与未清项

def _batch(db, bid):
    r = db.one('SELECT * FROM storm_batches WHERE id=?', (bid,))
    if not r:
        raise StormError(404, 'BATCH_NOT_FOUND', f'撤离批次 {bid} 不存在')
    return dict(r)


def _valid_checkins(db, bid):
    return [dict(r) for r in db.q(
        '''SELECT c.*, rs.name person_name, rs.role person_role, sh.name shelter_name
           FROM storm_checkins c JOIN storm_roster rs ON rs.id=c.person
           JOIN storm_shelters sh ON sh.id=c.shelter
           WHERE c.batch=? AND c.status='VALID' ORDER BY c.id''', (bid,))]


def _all_checkins(db, bid):
    return [dict(r) for r in db.q(
        '''SELECT c.*, rs.name person_name, rs.role person_role, sh.name shelter_name
           FROM storm_checkins c LEFT JOIN storm_roster rs ON rs.id=c.person
           LEFT JOIN storm_shelters sh ON sh.id=c.shelter
           WHERE c.batch=? ORDER BY c.id''', (bid,))]


def _valid_guards(db, bid):
    return [dict(r) for r in db.q(
        '''SELECT g.*, rs.name guardian_name, sh.name shelter_name
           FROM storm_risk_guards g LEFT JOIN storm_roster rs ON rs.id=g.guardian
           LEFT JOIN storm_shelters sh ON sh.id=g.shelter
           WHERE g.batch=? AND g.status='VALID' ORDER BY g.id''', (bid,))]


def _valid_tools(db, bid):
    return [dict(r) for r in db.q(
        '''SELECT * FROM storm_tool_reports WHERE batch=? AND status='VALID' ORDER BY id''', (bid,))]


def _all_tool_reports(db, bid):
    return [dict(r) for r in db.q(
        'SELECT * FROM storm_tool_reports WHERE batch=? ORDER BY id', (bid,))]


def open_items(db, b):
    roster = _loads(b['roster_json'], [])
    snap_tools = _loads(b['tools_json'], [])
    risks = _loads(b['risks_json'], [])
    checkins = _valid_checkins(db, b['id'])
    guards = _valid_guards(db, b['id'])
    tool_reports = _valid_tools(db, b['id'])

    by_person = {c['person']: c for c in checkins}
    missing, safe, offsite = [], [], []
    for p in roster:
        if not p.get('onsite'):
            offsite.append({'person': p['id'], 'name': p['name'], 'reason': '撤离令前已离场，不纳入到齐判定'})
            continue
        c = by_person.get(p['id'])
        if c and c.get('safe', 1):
            safe.append({'person': p['id'], 'name': p['name'], 'shelter': c['shelter'],
                         'shelter_name': c['shelter_name']})
        elif c:
            missing.append({'person': p['id'], 'name': p['name'],
                            'reason': f"已扫码到 {c['shelter_name']}，但尚未确认安全"})
        else:
            missing.append({'person': p['id'], 'name': p['name'], 'reason': '尚未在任一避险点扫码确认安全'})

    by_risk = {g['risk_id']: g for g in guards}
    unguarded = []
    guarded = []
    for risk in risks:
        g = by_risk.get(risk['id'])
        if g:
            guarded.append({**risk, 'guardian': g['guardian'], 'guardian_name': g.get('guardian_name'),
                            'shelter': g.get('shelter'), 'shelter_name': g.get('shelter_name')})
        else:
            unguarded.append({**risk, 'reason': '带电风险点尚未落实监护'})

    by_tool = {r['serial']: r for r in tool_reports}
    unresolved_conflicts = {r['serial']: dict(r) for r in db.q(
        "SELECT serial, detail FROM storm_tool_reports WHERE batch=? AND status='CONFLICT' ORDER BY rowid",
        (b['id'],))}
    unknown_tools, located_tools, conflict_tools = [], [], []
    for t in snap_tools:
        rpt = by_tool.get(t['serial'])
        item = dict(t)
        if t['serial'] in unresolved_conflicts:
            item.update({'conflict': unresolved_conflicts[t['serial']],
                         'reason': unresolved_conflicts[t['serial']]['detail']})
            conflict_tools.append(item)
        elif not rpt:
            item['reason'] = '关键器具撤离后去向未清点'
            unknown_tools.append(item)
        else:
            item.update({'report': dict(rpt)})
            located_tools.append(item)

    return {
        'missing_persons': missing,
        'safe_persons': safe,
        'offsite': offsite,
        'unguarded_risks': unguarded,
        'guarded_risks': guarded,
        'unknown_tools': unknown_tools,
        'located_tools': located_tools,
        'conflict_tools': conflict_tools,
    }


def batch_view(db, bid, include_history=True):
    b = _batch(db, bid)
    t = get_ticket(db, b['ticket'])
    roster = _loads(b['roster_json'], [])
    risks = _loads(b['risks_json'], [])
    tools = _loads(b['tools_json'], [])
    checkins = _valid_checkins(db, b['id'])
    guards = _valid_guards(db, b['id'])
    tool_reports = _valid_tools(db, b['id'])
    items = open_items(db, b)
    field = db.one("SELECT * FROM storm_field_confirms WHERE batch=? ORDER BY id DESC LIMIT 1", (bid,))
    disp = db.one("SELECT * FROM storm_dispatch_confirms WHERE batch=? ORDER BY id DESC LIMIT 1", (bid,))
    distribution = {}
    for c in checkins:
        distribution.setdefault(c['shelter'], {'shelter': c['shelter'], 'name': c['shelter_name'], 'people': []})
        distribution[c['shelter']]['people'].append({'person': c['person'], 'name': c['person_name'],
                                                     'role': c['person_role'], 'at': c['created_at']})
    view = {
        'batch_id': b['id'], 'id': b['id'], 'ticket': b['ticket'], 'alert': b['alert'],
        'seq': b['seq'], 'status': b['status'], 'field_leader': b['field_leader'],
        'dispatcher': b['dispatcher'], 'issued_by': b['issued_by'], 'issued_at': b['issued_at'],
        'snapshot_topo': b['snapshot_topo'], 'env_version': b['env_version'], 'env_hash': b['env_hash'],
        'resumed_by': b['resumed_by'], 'resumed_at': b['resumed_at'], 'terminal_reason': b['terminal_reason'],
        'ticket_status': t['status'], 'ticket_stage': b.get('ticket_stage') or t['status'],
        'ticket_frozen': bool(t['frozen']),
        'roster': roster, 'risks': risks, 'tools': tools,
        'checkins': checkins, 'all_checkins': _all_checkins(db, b['id']),
        'risk_guards': guards, 'tool_reports': tool_reports,
        'all_tool_reports': _all_tool_reports(db, b['id']),
        'distribution': list(distribution.values()),
        'field_confirm': dict(field) if field else None,
        'dispatch_confirm': dict(disp) if disp else None,
        'open_items': items,
        'all_accounted_for': not items['missing_persons'],
        'all_tools_located': not items['unknown_tools'] and not items['conflict_tools'],
        'all_risks_guarded': not items['unguarded_risks'],
        'can_resume': b['status'] == B_DISPATCH_CONFIRMED and not items['missing_persons']
                      and not items['unknown_tools'] and not items['conflict_tools']
                      and not items['unguarded_risks'],
    }
    if include_history:
        view['history'] = decisions(db, b['id'])
    return view


def decisions(db, bid=None, limit=200):
    if bid:
        rows = db.q('SELECT * FROM storm_decisions WHERE batch=? ORDER BY rowid DESC LIMIT ?', (bid, limit))
    else:
        rows = db.q('SELECT * FROM storm_decisions ORDER BY rowid DESC LIMIT ?', (limit,))
    out = []
    for r in rows:
        d = dict(r)
        d['response'] = _loads(d['response'], {})
        out.append(d)
    return out


def board_view(db):
    alerts = [dict(r) for r in db.q('SELECT * FROM storm_alerts ORDER BY rowid DESC LIMIT 100')]
    for a in alerts:
        a['areas'] = _loads(a['areas'], [])
    batches = []
    for r in db.q('SELECT id FROM storm_batches ORDER BY rowid DESC LIMIT 100'):
        batches.append(batch_view(db, r['id'], include_history=False))
    shelters = [dict(r) for r in db.q('SELECT * FROM storm_shelters ORDER BY id')]
    roster = _roster_rows(db)
    return {'alerts': alerts, 'batches': batches, 'shelters': shelters, 'roster': roster}


# ------------------------------------------------------------ 移动签到/纠正/清点

def _person(db, ident: str):
    r = db.one('SELECT * FROM storm_roster WHERE (id=? OR name=?) AND active=1', (ident, ident))
    return dict(r) if r else None


def _shelter(db, ident: str):
    r = db.one('SELECT * FROM storm_shelters WHERE (id=? OR qr=? OR name=?) AND active=1', (ident, ident, ident))
    return dict(r) if r else None


def checkin_core(db, bid: str, person_id: str, shelter_code: str, claimed_by: Optional[str] = None,
                 key: Optional[str] = None, safe: bool = True):
    # 业务自然键负责相同请求幂等；表内 key 只是审计/去重索引，必须保持每次底层尝试唯一。
    key = key or ('chk:' + uuid.uuid4().hex)
    nat = _digest({'batch': bid, 'person': person_id, 'shelter': shelter_code,
                   'claimed_by': claimed_by or person_id, 'safe': bool(safe)})
    with db.lock:
        def produce():
            b = _batch(db, bid)
            if b['status'] not in B_OPEN_STATES:
                raise StormError(409, 'BATCH_NOT_OPEN', f'批次已 {b["status"]}，不能补签到', bid)
            p = _person(db, person_id)
            if not p:
                raise StormError(404, 'PERSON_NOT_REGISTERED', f'{person_id} 未登记，禁止签到', bid)
            sh = _shelter(db, shelter_code)
            if not sh:
                raise StormError(404, 'SHELTER_NOT_FOUND', f'避险点 {shelter_code} 无效', bid)
            # 代签：实际扫码/提交人必须与名册成员本人一致。
            if claimed_by and claimed_by != p['id'] and claimed_by != p['name']:
                row_id = _record_rejected_checkin(db, bid, p, sh, claimed_by, key,
                                                  'PROXY_SIGN_FORBIDDEN',
                                                  f'{claimed_by} 不能代 {p["name"]} 签到')
                resp = _reject_resp(bid, 'PROXY_SIGN_FORBIDDEN', '禁止代签：必须由成员本人扫码确认安全',
                                    row_id, claimed_by)
                return DEC_REJECTED, 'PROXY_SIGN_FORBIDDEN', resp['detail'], resp
            if not p.get('onsite'):
                row_id = _record_rejected_checkin(db, bid, p, sh, claimed_by or p['id'], key,
                                                  'PERSON_ALREADY_OFFSITE',
                                                  f'{p["name"]} 在撤离令名册中标记为已离场')
                resp = _reject_resp(bid, 'PERSON_ALREADY_OFFSITE',
                                    '已离场人员误入名册：撤离令前已离场，不纳入本次签到', row_id, p['name'])
                return DEC_REJECTED, 'PERSON_ALREADY_OFFSITE', resp['detail'], resp
            old = db.one("SELECT * FROM storm_checkins WHERE batch=? AND person=? AND status='VALID'",
                         (bid, p['id']))
            if old:
                old = dict(old)
                if old['shelter'] == sh['id']:
                    if not safe and old['safe']:
                        resp = {'ok': False, 'batch_id': bid, 'checkin_id': old['id'],
                                'person': p['id'], 'name': p['name'], 'shelter': sh['id'],
                                'result': 'UNSAFE_REPORT_IGNORED',
                                'reason_code': 'UNSAFE_REPORT_IGNORED',
                                'detail': '已有本人安全确认；后续未安全报告不改变安全事实',
                                'actor': p['name']}
                        return DEC_REJECTED, 'UNSAFE_REPORT_IGNORED', resp['detail'], resp
                    if safe and not old['safe']:
                        db.x('UPDATE storm_checkins SET safe=1 WHERE id=?', (old['id'],))
                        _audit(db, bid, p['name'], 'STORM_SAFETY_CONFIRMED', person=p['id'],
                               shelter=sh['id'], checkin=old['id'])
                        return (DEC_ACCEPTED, 'SAFETY_CONFIRMED',
                                f'{p["name"]} 已补确认安全，有效去向仍为 {sh["name"]}', {
                            'ok': True, 'batch_id': bid, 'checkin_id': old['id'], 'person': p['id'],
                            'name': p['name'], 'shelter': sh['id'], 'shelter_name': sh['name'],
                            'result': 'SAFETY_CONFIRMED', 'reason_code': 'SAFETY_CONFIRMED',
                            'detail': f'{p["name"]} 已补确认安全，有效去向仍为 {sh["name"]}',
                            'actor': p['name'], 'batch': batch_view(db, bid, include_history=False)})
                    resp = {'ok': True, 'idempotent': True, 'batch_id': bid, 'checkin_id': old['id'],
                            'person': p['id'], 'name': p['name'], 'shelter': sh['id'],
                            'shelter_name': sh['name'], 'result': 'DUPLICATE_SCAN',
                            'reason_code': 'DUPLICATE_SCAN',
                            'detail': f'{p["name"]} 已在 {sh["name"]} 签到，重复扫码返回首次结果',
                            'actor': p['name']}
                    return DEC_ACCEPTED, 'DUPLICATE_SCAN', resp['detail'], resp
                # 换到新避险点：旧去向置为 SUPERSEDED，仅最新一条 VALID。
                db.x("INSERT INTO storm_checkins(batch,person,shelter,claimed_by,safe,status,key,created_at) VALUES(?,?,?,?,?,?,?,?)",
                     (bid, p['id'], sh['id'], p['id'], 1 if safe else 0, 'NEW', key, _now()))
                new = db.one('SELECT MAX(id) id FROM storm_checkins WHERE batch=? AND person=?', (bid, p['id']))
                db.x("UPDATE storm_checkins SET status=?, superseded_by=?, reason_code=?, detail=? WHERE id=?",
                     ('SUPERSEDED', new['id'], 'SHELTER_CHANGED',
                      f'{p["name"]} 改去 {sh["name"]}，仅保留最新有效去向', old['id']))
                db.x("UPDATE storm_checkins SET status='VALID' WHERE id=?", (new['id'],))
                cid = new['id']
                result_code = 'SHELTER_CHANGED'
                detail = f'{p["name"]} 已从旧避险点改至 {sh["name"]}，旧去向保留但失效'
            else:
                db.x("INSERT INTO storm_checkins(batch,person,shelter,claimed_by,safe,status,key,created_at) VALUES(?,?,?,?,?,?,?,?)",
                     (bid, p['id'], sh['id'], p['id'], 1 if safe else 0, 'VALID', key, _now()))
                cid = db.one('SELECT MAX(id) id FROM storm_checkins WHERE batch=? AND person=?', (bid, p['id']))['id']
                if safe:
                    result_code = 'CHECKED_IN'
                    detail = f'{p["name"]} 已在 {sh["name"]} 确认安全'
                else:
                    result_code = 'ARRIVED_NOT_SAFE'
                    detail = f'{p["name"]} 已到达 {sh["name"]}，但尚未本人确认安全'
            _audit(db, bid, p['name'], 'STORM_CHECKIN', person=p['id'], shelter=sh['id'],
                   result=result_code)
            resp = {'ok': True, 'batch_id': bid, 'checkin_id': cid, 'person': p['id'],
                    'name': p['name'], 'shelter': sh['id'], 'shelter_name': sh['name'],
                    'result': result_code, 'reason_code': result_code, 'detail': detail,
                    'actor': p['name'], 'batch': batch_view(db, bid, include_history=False)}
            return DEC_ACCEPTED, result_code, detail, resp
        try:
            resp, duplicate = _stored_raise(db, 'STORM_CHECKIN', nat, produce)
            if duplicate:
                cur = db.one("SELECT c.*, sh.name shelter_name FROM storm_checkins c "
                             "JOIN storm_shelters sh ON sh.id=c.shelter "
                             "WHERE c.batch=? AND c.person=? AND c.status='VALID'",
                             (bid, _person(db, person_id)['id']))
                if cur and cur['shelter'] == _shelter(db, shelter_code)['id']:
                    resp.update({'idempotent': True,
                                 'detail': f'{resp.get("name") or "成员"} 重复扫码，返回首次结果且不新增记录'})
            db.commit()
            return resp
        except Exception:
            db.rollback()
            raise


def _record_rejected_checkin(db, bid, p, sh, claimed_by, key, code, detail):
    # 拒绝事实也落库，但不形成 VALID 到齐记录。
    db.x("INSERT INTO storm_checkins(batch,person,shelter,claimed_by,status,key,reason_code,detail,created_at) "
         "VALUES(?,?,?,?,?,?,?,?,?)",
         (bid, p['id'], sh['id'], claimed_by, DEC_REJECTED, key, code, detail, _now()))
    return db.one('SELECT MAX(id) id FROM storm_checkins WHERE batch=? AND person=?',
                  (bid, p['id']))['id']


def _reject_resp(bid, code, detail, record_id, actor):
    return {'ok': False, 'batch_id': bid, 'record_id': record_id, 'result': DEC_REJECTED,
            'reason_code': code, 'detail': detail, 'actor': actor}


def correct_destination_core(db, bid: str, person_id: str, shelter_code: str, key: Optional[str] = None):
    key = key or ('cor:' + uuid.uuid4().hex)
    nat = _digest({'op': 'correct', 'batch': bid, 'person': person_id, 'shelter': shelter_code})
    with db.lock:
        def produce():
            b = _batch(db, bid)
            p = _person(db, person_id)
            sh = _shelter(db, shelter_code)
            if not p:
                raise StormError(404, 'PERSON_NOT_REGISTERED', f'{person_id} 未登记', bid)
            if not sh:
                raise StormError(404, 'SHELTER_NOT_FOUND', f'避险点 {shelter_code} 无效', bid)
            if b['status'] not in B_OPEN_STATES:
                raise StormError(409, 'BATCH_NOT_OPEN', f'批次已 {b["status"]}，不能纠正去向', bid)
            old = db.one("SELECT * FROM storm_checkins WHERE batch=? AND person=? AND status='VALID'",
                         (bid, p['id']))
            if not old:
                raise StormError(409, 'NO_VALID_CHECKIN', '必须先由本人扫码签到，才能纠正去向', bid)
            if old['shelter'] == sh['id']:
                return DEC_ACCEPTED, 'DUPLICATE_SCAN', '当前去向已是该避险点', {
                    'ok': True, 'idempotent': True, 'batch_id': bid, 'person': p['id'],
                    'name': p['name'], 'shelter': sh['id'], 'result': 'DUPLICATE_SCAN',
                    'reason_code': 'DUPLICATE_SCAN', 'detail': '重复纠正返回当前有效去向',
                    'actor': p['name']}
            db.x("INSERT INTO storm_checkins(batch,person,shelter,claimed_by,safe,status,key,created_at) VALUES(?,?,?,?,?,?,?,?)",
                 (bid, p['id'], sh['id'], p['id'], old['safe'], 'NEW', key, _now()))
            new_id = db.one('SELECT MAX(id) id FROM storm_checkins WHERE batch=? AND person=?',
                            (bid, p['id']))['id']
            db.x("UPDATE storm_checkins SET status=?, superseded_by=?, reason_code=?, detail=? WHERE id=?",
                 ('SUPERSEDED', new_id, 'DESTINATION_CORRECTED',
                  f'移动端纠正为 {sh["name"]}，只保留最新有效去向', old['id']))
            db.x("UPDATE storm_checkins SET status='VALID' WHERE id=?", (new_id,))
            _audit(db, bid, p['name'], 'STORM_DESTINATION_CORRECTED', person=p['id'],
                   old_shelter=old['shelter'], new_shelter=sh['id'])
            resp = {'ok': True, 'batch_id': bid, 'checkin_id': new_id, 'person': p['id'],
                    'name': p['name'], 'shelter': sh['id'], 'shelter_name': sh['name'],
                    'result': 'DESTINATION_CORRECTED', 'reason_code': 'DESTINATION_CORRECTED',
                    'detail': f'{p["name"]} 的有效去向已纠正为 {sh["name"]}', 'actor': p['name'],
                    'batch': batch_view(db, bid, include_history=False)}
            return DEC_ACCEPTED, 'DESTINATION_CORRECTED', resp['detail'], resp
        try:
            resp, _ = _stored_raise(db, 'STORM_CORRECT_DESTINATION', nat, produce)
            db.commit()
            return resp
        except Exception:
            db.rollback()
            raise


def assign_guard_core(db, bid: str, risk_id: str, guardian_id: str, shelter_code: Optional[str] = None,
                      key: Optional[str] = None):
    key = key or ('grd:' + uuid.uuid4().hex)
    nat = _digest({'op': 'guard', 'batch': bid, 'risk': risk_id, 'guardian': guardian_id,
                   'shelter': shelter_code})
    with db.lock:
        def produce():
            b = _batch(db, bid)
            if b['status'] not in B_OPEN_STATES:
                raise StormError(409, 'BATCH_NOT_OPEN', f'批次已 {b["status"]}，不能安排监护', bid)
            risks = _loads(b['risks_json'], [])
            risk = next((x for x in risks if x['id'] == risk_id), None)
            if not risk:
                raise StormError(404, 'RISK_NOT_FOUND', f'风险点 {risk_id} 不在撤离令固定快照中', bid)
            g = _person(db, guardian_id)
            if not g:
                raise StormError(404, 'PERSON_NOT_REGISTERED', f'监护人 {guardian_id} 未登记', bid)
            sh = _shelter(db, shelter_code) if shelter_code else None
            if shelter_code and not sh:
                raise StormError(404, 'SHELTER_NOT_FOUND', f'避险点 {shelter_code} 无效', bid)
            # 同一风险只能有一个有效监护人；换监护人由现场负责人显式纠正。
            old = db.one("SELECT * FROM storm_risk_guards WHERE batch=? AND risk_id=? AND status='VALID'",
                         (bid, risk_id))
            if old and old['guardian'] == g['id'] and (old['shelter'] or None) == (sh['id'] if sh else None):
                return DEC_ACCEPTED, 'DUPLICATE_GUARD', '重复安排返回首次结果', {
                    'ok': True, 'idempotent': True, 'batch_id': bid, 'risk_id': risk_id,
                    'guard_id': old['id'], 'reason_code': 'DUPLICATE_GUARD',
                    'detail': '该风险已由同一监护人在同一避险点监护', 'actor': g['name']}
            db.x("INSERT INTO storm_risk_guards(batch,risk_id,guardian,shelter,key,status,created_at) VALUES(?,?,?,?,?,?,?)",
                 (bid, risk_id, g['id'], sh['id'] if sh else None, key, 'NEW', _now()))
            new_id = db.one('SELECT MAX(id) id FROM storm_risk_guards WHERE batch=? AND risk_id=?',
                            (bid, risk_id))['id']
            if old:
                db.x("UPDATE storm_risk_guards SET status=?, superseded_by=?, reason_code=?, detail=? WHERE id=?",
                     ('SUPERSEDED', new_id, 'GUARD_CHANGED',
                      f'风险 {risk_id} 监护人改由 {g["name"]} 担任', old['id']))
            db.x("UPDATE storm_risk_guards SET status='VALID' WHERE id=?", (new_id,))
            _audit(db, bid, g['name'], 'STORM_RISK_GUARDED', risk=risk_id, guardian=g['id'],
                   shelter=sh['id'] if sh else None)
            resp = {'ok': True, 'batch_id': bid, 'guard_id': new_id, 'risk_id': risk_id,
                    'guardian': g['id'], 'guardian_name': g['name'],
                    'shelter': sh['id'] if sh else None,
                    'shelter_name': sh['name'] if sh else None,
                    'result': 'GUARD_ASSIGNED', 'reason_code': 'GUARD_ASSIGNED',
                    'detail': f'{risk_id} 已由 {g["name"]} 落实监护', 'actor': g['name'],
                    'batch': batch_view(db, bid, include_history=False)}
            return DEC_ACCEPTED, 'GUARD_ASSIGNED', resp['detail'], resp
        try:
            resp, _ = _stored_raise(db, 'STORM_GUARD_ASSIGN', nat, produce)
            db.commit()
            return resp
        except Exception:
            db.rollback()
            raise


def report_tool_core(db, bid: str, serial: str, location: str, held_by: Optional[str],
                     shelter_code: Optional[str] = None, actor_id: Optional[str] = None,
                     key: Optional[str] = None, force_correct: bool = False):
    key = key or ('tol:' + uuid.uuid4().hex)
    nat = _digest({'op': 'tool', 'batch': bid, 'serial': serial, 'location': location,
                   'held_by': held_by, 'shelter': shelter_code, 'force_correct': force_correct})
    with db.lock:
        def produce():
            b = _batch(db, bid)
            if b['status'] not in B_OPEN_STATES:
                raise StormError(409, 'BATCH_NOT_OPEN', f'批次已 {b["status"]}，不能提交器具去向', bid)
            snap_tools = _loads(b['tools_json'], [])
            item = next((x for x in snap_tools if x['serial'] == serial), None)
            if not item:
                raise StormError(404, 'TOOL_NOT_IN_SNAPSHOT',
                                 f'{serial} 不在撤离令固定的本票现场工器具占有清单', bid)
            actor = _person(db, actor_id or held_by or b['field_leader']) if (actor_id or held_by) else None
            if actor_id and not actor:
                raise StormError(404, 'PERSON_NOT_REGISTERED', f'提交人 {actor_id} 未登记', bid)
            if force_correct and (not actor or (actor['name'] != b['field_leader'] and actor['id'] != b['field_leader'])):
                resp = {'ok': False, 'batch_id': bid, 'serial': serial,
                        'result': DEC_REJECTED, 'reason_code': 'NOT_FIELD_LEADER',
                        'detail': '器具去向冲突只能由本批次现场负责人裁决最新去向',
                        'actor': actor['name'] if actor else held_by or b['field_leader']}
                return DEC_REJECTED, 'NOT_FIELD_LEADER', resp['detail'], resp
            sh = _shelter(db, shelter_code) if shelter_code else None
            if shelter_code and not sh:
                raise StormError(404, 'SHELTER_NOT_FOUND', f'避险点 {shelter_code} 无效', bid)
            same = db.one("SELECT * FROM storm_tool_reports WHERE batch=? AND serial=? AND status='VALID' "
                          "AND location=? AND COALESCE(held_by,'')=COALESCE(?,'') "
                          "AND COALESCE(shelter,'')=COALESCE(?,'')",
                          (bid, serial, location, held_by, sh['id'] if sh else None))
            if same:
                return DEC_ACCEPTED, 'DUPLICATE_TOOL_REPORT', '相同器具去向重放返回首次结果', {
                    'ok': True, 'idempotent': True, 'batch_id': bid, 'report_id': same['id'],
                    'serial': serial, 'reason_code': 'DUPLICATE_TOOL_REPORT',
                    'detail': '相同器具清点请求重放，不新增清点记录', 'actor': actor['name'] if actor else b['field_leader']}
            old = db.one("SELECT * FROM storm_tool_reports WHERE batch=? AND serial=? AND status='VALID'",
                         (bid, serial))
            if old and not force_correct:
                # 不同去向先作为冲突事实落库，阻断复工；必须由现场负责人 force_correct 裁决。
                db.x("INSERT INTO storm_tool_reports(batch,serial,location,held_by,shelter,key,status,reason_code,detail,created_at) "
                      "VALUES(?,?,?,?,?,?,?,?,?,?)",
                      (bid, serial, TOOL_CONFLICT, held_by, sh['id'] if sh else None, key,
                       TOOL_CONFLICT, 'TOOL_LOCATION_CONFLICT',
                       f'新报“{location}”与旧报“{old["location"]}”冲突，待现场负责人裁决', _now()))
                rid = db.one('SELECT MAX(id) id FROM storm_tool_reports WHERE batch=? AND serial=?',
                             (bid, serial))['id']
                _audit(db, bid, actor['name'] if actor else b['field_leader'],
                       'STORM_TOOL_CONFLICT', serial=serial, old=old['location'], new=location)
                resp = {'ok': False, 'batch_id': bid, 'report_id': rid, 'serial': serial,
                        'result': TOOL_CONFLICT, 'reason_code': 'TOOL_LOCATION_CONFLICT',
                        'detail': f'器具 {serial} 去向冲突：旧报 {old["location"]}，新报 {location}',
                        'actor': actor['name'] if actor else b['field_leader'],
                        'batch': batch_view(db, bid, include_history=False)}
                return DEC_REJECTED, 'TOOL_LOCATION_CONFLICT', resp['detail'], resp
            db.x("INSERT INTO storm_tool_reports(batch,serial,location,held_by,shelter,key,status,created_at) VALUES(?,?,?,?,?,?,?,?)",
                 (bid, serial, location, held_by, sh['id'] if sh else None, key, 'NEW', _now()))
            rid = db.one('SELECT MAX(id) id FROM storm_tool_reports WHERE batch=? AND serial=?',
                         (bid, serial))['id']
            if old:
                db.x("UPDATE storm_tool_reports SET status=?, superseded_by=?, reason_code=?, detail=? WHERE id=?",
                     ('SUPERSEDED', rid, 'TOOL_LOCATION_CORRECTED',
                      f'现场负责人裁决器具 {serial} 的最新去向为 {location}', old['id']))
                # 冲突记录也由负责人裁决结案。
                db.x("UPDATE storm_tool_reports SET status=?, superseded_by=?, reason_code=?, detail=? WHERE batch=? AND serial=? AND status='CONFLICT'",
                     ('RESOLVED', rid, 'TOOL_CONFLICT_RESOLVED',
                      f'现场负责人确认最新去向 {location}', bid, serial))
                code = 'TOOL_LOCATION_CORRECTED'
                detail = f'{serial} 去向已由现场负责人纠正为 {location}'
            else:
                code = 'TOOL_LOCATED'
                detail = f'{serial} 已清点，去向：{location}'
            db.x("UPDATE storm_tool_reports SET status='VALID' WHERE id=?", (rid,))
            _audit(db, bid, actor['name'] if actor else b['field_leader'], 'STORM_TOOL_REPORTED',
                   serial=serial, location=location, corrected=bool(old))
            resp = {'ok': True, 'batch_id': bid, 'report_id': rid, 'serial': serial,
                    'location': location, 'held_by': held_by,
                    'shelter': sh['id'] if sh else None, 'shelter_name': sh['name'] if sh else None,
                    'result': code, 'reason_code': code, 'detail': detail,
                    'actor': actor['name'] if actor else b['field_leader'],
                    'batch': batch_view(db, bid, include_history=False)}
            return DEC_ACCEPTED, code, detail, resp
        try:
            resp, _ = _stored_raise(db, 'STORM_TOOL_REPORT', nat, produce)
            db.commit()
            return resp
        except Exception:
            db.rollback()
            raise


# ------------------------------------------------------------ 双角色复工

def _assert_leader(db, b, actor_id):
    p = _person(db, actor_id)
    if not p:
        raise StormError(404, 'PERSON_NOT_REGISTERED', f'现场负责人 {actor_id} 未登记', b['id'])
    if p['name'] != b['field_leader'] and p['id'] != b['field_leader']:
        raise StormError(403, 'NOT_FIELD_LEADER',
                         f'{p["name"]} 不是本批次现场负责人 {b["field_leader"]}', b['id'])
    return p


def field_confirm_core(db, bid: str, actor_id: str, key: Optional[str] = None):
    key = key or f'field-confirm:{bid}:{actor_id}'
    nat = _digest({'op': 'field', 'batch': bid, 'actor': actor_id})
    with db.lock:
        def produce():
            b = _batch(db, bid)
            if b['status'] not in B_OPEN_STATES:
                raise StormError(409, 'BATCH_NOT_OPEN', f'批次已 {b["status"]}，不能提交清点', bid)
            p = _assert_leader(db, b, actor_id)
            items = open_items(db, b)
            if items['missing_persons'] or items['unknown_tools'] or items['conflict_tools']:
                missing = [x['name'] for x in items['missing_persons']]
                unknown = [x['serial'] for x in items['unknown_tools']]
                conflict = [x['serial'] for x in items['conflict_tools']]
                detail = []
                if missing: detail.append('人员未确认安全: ' + '、'.join(missing))
                if unknown: detail.append('关键器具去向不明: ' + '、'.join(unknown))
                if conflict: detail.append('器具去向冲突: ' + '、'.join(conflict))
                raise StormError(409, 'FIELD_CHECK_INCOMPLETE', '；'.join(detail), bid)
            t = get_ticket(db, b['ticket'])
            _, _, env_hash, _, _ = _current_env(db, t)
            old = db.one("SELECT * FROM storm_field_confirms WHERE batch=? AND status='VALID' AND actor=?",
                         (bid, p['name']))
            if old and old['env_version'] == b['env_version'] and old['env_hash'] == env_hash:
                return DEC_ACCEPTED, 'DUPLICATE_FIELD_CONFIRM', '相同现场清点重放返回首次结果', {
                    'ok': True, 'idempotent': True, 'batch_id': bid, 'confirm_id': old['id'],
                    'reason_code': 'DUPLICATE_FIELD_CONFIRM',
                    'detail': '相同人员与器具清点请求重放，不多写清点记录', 'actor': p['name']}
            try:
                db.x("INSERT INTO storm_field_confirms(batch,actor,env_version,env_hash,status,key,created_at) "
                     "VALUES(?,?,?,?,?,?,?)",
                     (bid, p['name'], b['env_version'], env_hash, 'VALID', key, _now()))
            except Exception:
                db.rollback()
                first = db.one("SELECT * FROM storm_field_confirms WHERE batch=? AND status='VALID' ORDER BY id LIMIT 1",
                               (bid,))
                if first:
                    return DEC_ACCEPTED, 'DUPLICATE_FIELD_CONFIRM', '并发重复清点返回首次结果', {
                        'ok': True, 'idempotent': True, 'batch_id': bid, 'confirm_id': first['id'],
                        'reason_code': 'DUPLICATE_FIELD_CONFIRM',
                        'detail': '并发/重复的人员与器具清点返回首次结果，不多写记录', 'actor': p['name']}
                raise
            cid = db.one('SELECT MAX(id) id FROM storm_field_confirms WHERE batch=?', (bid,))['id']
            db.x("UPDATE storm_batches SET status=? WHERE id=?", (B_WAITING_RETURN, bid))
            _audit(db, bid, p['name'], 'STORM_FIELD_CONFIRMED', confirm_id=cid,
                   env_version=b['env_version'], env_hash=env_hash)
            resp = {'ok': True, 'batch_id': bid, 'confirm_id': cid, 'actor': p['name'],
                    'status': B_WAITING_RETURN, 'env_version': b['env_version'],
                    'env_hash': env_hash, 'reason_code': 'FIELD_CONFIRMED',
                    'detail': '现场负责人已确认人员到齐与关键器具清点；待调度员核验复工依据',
                    'batch': batch_view(db, bid, include_history=False)}
            return DEC_ACCEPTED, 'FIELD_CONFIRMED', resp['detail'], resp
        try:
            resp, _ = _stored_raise(db, 'STORM_FIELD_CONFIRM', nat, produce)
            db.commit()
            return resp
        except Exception:
            db.rollback()
            raise


def dispatch_confirm_core(db, bid: str, actor: str, key: Optional[str] = None):
    key = key or f'dispatch-confirm:{bid}:{actor}'
    nat = _digest({'op': 'dispatch', 'batch': bid, 'actor': actor})
    with db.lock:
        def produce():
            b = _batch(db, bid)
            if b['status'] not in B_OPEN_STATES:
                raise StormError(409, 'BATCH_NOT_OPEN', f'批次已 {b["status"]}，不能调度放行', bid)
            t = get_ticket(db, b['ticket'])
            topo, env_obj, env_hash, problems, weather_version = _current_env(db, t)
            reasons = []
            field = db.one("SELECT * FROM storm_field_confirms WHERE batch=? AND status='VALID' ORDER BY id DESC LIMIT 1",
                           (bid,))
            if not field:
                reasons.append('现场负责人尚未确认人员与器具清点')
            elif field['env_version'] != b['env_version'] or field['env_hash'] != b['env_hash']:
                reasons.append('现场确认依据的环境版本已变化，旧确认失效')
            if env_obj['active_alert']:
                reasons.append(f"气象告警 {env_obj['active_alert']} 尚未解除")
            if env_obj['topology'] != b['snapshot_topo']:
                reasons.append(f"网络拓扑已变化（撤离快照 v{b['snapshot_topo']}，当前 v{env_obj['topology']}）")
            if problems:
                reasons.append('隔离边界不成立：' + '；'.join(problems))
            items = open_items(db, b)
            if items['missing_persons']:
                reasons.append('仍有人员未确认安全：' + '、'.join(x['name'] for x in items['missing_persons']))
            if items['unknown_tools'] or items['conflict_tools']:
                serials = [x['serial'] for x in items['unknown_tools'] + items['conflict_tools']]
                reasons.append('关键器具未完成有效清点：' + '、'.join(serials))
            if items['unguarded_risks']:
                reasons.append('带电风险点尚未落实监护：' + '、'.join(x['id'] for x in items['unguarded_risks']))
            if reasons:
                raise StormError(409, 'DISPATCH_CONFIRM_DENIED', '；'.join(reasons), bid)
            old = db.one("SELECT * FROM storm_dispatch_confirms WHERE batch=? AND actor=? AND status='VALID'",
                         (bid, actor))
            if old and old['env_version'] == b['env_version']:
                return DEC_ACCEPTED, 'DUPLICATE_DISPATCH_CONFIRM', '相同调度放行重放返回首次结果', {
                    'ok': True, 'idempotent': True, 'batch_id': bid, 'confirm_id': old['id'],
                    'reason_code': 'DUPLICATE_DISPATCH_CONFIRM',
                    'detail': '相同调度核验请求重放，不多写放行记录', 'actor': actor}
            try:
                db.x('''INSERT INTO storm_dispatch_confirms(batch,actor,field_confirm_id,env_version,env_hash,
                         weather_version,topo_version,ticket_status,boundary_hash,status,key,created_at)
                         VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
                     (bid, actor, field['id'], b['env_version'], env_hash, weather_version,
                      topo.version, t['status'], env_obj['boundary_hash'], 'VALID', key, _now()))
            except Exception:
                db.rollback()
                first = db.one("SELECT * FROM storm_dispatch_confirms WHERE batch=? AND status='VALID' ORDER BY id LIMIT 1",
                               (bid,))
                if first:
                    return DEC_ACCEPTED, 'DUPLICATE_DISPATCH_CONFIRM', '并发重复放行返回首次结果', {
                        'ok': True, 'idempotent': True, 'batch_id': bid, 'confirm_id': first['id'],
                        'reason_code': 'DUPLICATE_DISPATCH_CONFIRM',
                        'detail': '并发/重复调度核验返回首次结果，不多写放行记录', 'actor': actor}
                raise
            cid = db.one('SELECT MAX(id) id FROM storm_dispatch_confirms WHERE batch=?', (bid,))['id']
            db.x("UPDATE storm_batches SET status=?, dispatcher=? WHERE id=?",
                 (B_DISPATCH_CONFIRMED, actor, bid))
            _audit(db, bid, actor, 'STORM_DISPATCH_CONFIRMED', confirm_id=cid,
                   field_confirm=field['id'], env_version=b['env_version'], env_hash=env_hash,
                   weather_version=weather_version, topo_version=topo.version,
                   boundary_hash=env_obj['boundary_hash'])
            resp = {'ok': True, 'batch_id': bid, 'confirm_id': cid, 'actor': actor,
                    'status': B_DISPATCH_CONFIRMED, 'field_confirm_id': field['id'],
                    'weather_version': weather_version, 'topology_version': topo.version,
                    'ticket_status': t['status'], 'boundary_hash': env_obj['boundary_hash'],
                    'env_version': b['env_version'], 'env_hash': env_hash,
                    'reason_code': 'DISPATCH_CONFIRMED',
                    'detail': '调度员已核验气象解除、网络拓扑与隔离边界；批次进入可复工状态',
                    'batch': batch_view(db, bid, include_history=False)}
            return DEC_ACCEPTED, 'DISPATCH_CONFIRMED', resp['detail'], resp
        try:
            resp, _ = _stored_raise(db, 'STORM_DISPATCH_CONFIRM', nat, produce)
            db.commit()
            return resp
        except Exception:
            db.rollback()
            raise


def resume_core(db, bid: str, actor: str):
    with db.lock:
        b = _batch(db, bid)
        if b['status'] == B_RESUMED:
            return {'ok': True, 'idempotent': True, 'batch_id': bid, 'status': B_RESUMED,
                    'resumed_by': b['resumed_by'], 'resumed_at': b['resumed_at']}
        if b['status'] != B_DISPATCH_CONFIRMED:
            raise StormError(409, 'NOT_DOUBLE_APPROVED',
                            f'当前批次状态 {b["status"]}，必须双角色针对同一批次和同一环境版本放行', bid)
        items = open_items(db, b)
        if items['missing_persons'] or items['unknown_tools'] or items['conflict_tools'] or items['unguarded_risks']:
            raise StormError(409, 'OPEN_ITEMS_REMAIN', '仍存在未清人员、器具或风险，票据不得恢复作业', bid)
        t = get_ticket(db, b['ticket'])
        if _active_alert(db, b['ticket']):
            raise StormError(409, 'ACTIVE_ALERT', '仍有有效雷暴告警，不能复工', bid)
        if t['status'] not in ('ISOLATING', 'PERMITTED', 'RESTORING'):
            raise StormError(409, 'TICKET_NOT_ACTIVE', f'票据已为 {t["status"]}，不能复工', bid)
        db.x('UPDATE tickets SET frozen=0, freeze_reason=NULL WHERE id=?', (b['ticket'],))
        db.x('UPDATE storm_batches SET status=?, resumed_by=?, resumed_at=? WHERE id=?',
             (B_RESUMED, actor, _now(), bid))
        _audit(db, bid, actor, 'STORM_RESUMED', ticket_status=t['status'])
        db.commit()
        return batch_view(db, bid)


# ------------------------------------------------------------ 演示辅助

def setup_demo_core(db, *, reset=True):
    """创建可直接触发告警的在执行票据、人员名册、避险点和两件已领出工具。"""
    seed_storm(db, force=reset)
    with db.lock:
        # 清掉演示 Storm 状态；主业务票据通过重新创建获得新 id，避免唯一避险批次冲突。
        for table in ('storm_decisions', 'storm_dispatch_confirms', 'storm_field_confirms',
                      'storm_tool_reports', 'storm_risk_guards', 'storm_checkins',
                      'storm_batches', 'storm_alerts'):
            db.x(f'DELETE FROM {table}')
        db.set_meta('storm_env_version', '0')
        db.commit()
    return {'ok': True, 'message': '雷暴闸门演示基础数据已就绪：请先从调度端创建/许可票据，再触发告警'}
