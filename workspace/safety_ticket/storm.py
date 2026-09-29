"""雷暴撤离与复工闸门 — 核心业务模块（不直接依赖 Web 框架）。

气象告警命中作业区域时，值班长发布撤离令，系统在**同一事务内立即固定**：
- 本次人员名册 storm_roster（撤离当时的在册成员、状态与个人签到口令，之后不可改写）
- 仍带电的风险点 storm_risks（工作区内仍带电设备、带电接地、工作侧边界开关）
- 现场工器具占有情况 storm_tools（在库锁定 / 已领用在现场 / 随箱持有；逐件快照）
- 各安全票所处阶段 storm_tickets（票据状态与阶段冻结到名册 JSON）

移动端成员凭**名册内个人口令**扫避险点码到达（checkin_core）：
- 代签（口令与本人不符）、重复扫码（同人同点重放）、已离场人员误入、未登记人员签到，
  全部给出确定原因码并落库（storm_decisions），不得多写签到；
- 同一人更换避险点只保留最新有效去向：旧点置 SUPERSEDED，仅新点 CURRENT。

复工双角色放行（必须针对同一撤离批次与同一环境版本）：
- 现场负责人确认人员与器具清点（field_confirm_core）；
- 调度员核验气象解除、网络拓扑与隔离边界（dispatcher_confirm_core）。
任一依据变化（新告警 / 拓扑变更）使旧确认失效（SUPERSEDED），但**不删除**已发生的
撤离与签到事实。任何人未确认安全、关键器具去向不明或风险点尚未落实监护时，票据不得恢复作业。

撤离令、作业收工、票据撤销、换班与新气象告警可能同时到达：全部裁决在 db.lock 串行化
事务内进行，只有一个最终阶段；相同请求重放返回最初结果（幂等键 + 自然唯一约束），
不多写签到、清点或放行记录。全部状态持久化于 SQLite，进程重启后完整还原。
"""
import hashlib
import json
import time
import uuid

import domain
import tools as tools_mod

# ---------------------------------------------------------------- 告警与环境

# 气象告警级别
WX_THUNDER = 'THUNDER'        # 雷暴橙色/红色：触发撤离
WX_CLEAR = 'CLEAR'            # 解除

# 环境版本依据（两项确认必须针对同一环境版本；任一依据变化即递增）
ENV_BASIS = ('topology_version', 'weather_seq', 'alert_seq')

# ---------------------------------------------------------------- 批次阶段

# 撤离批次
EV_ACTIVE = 'ACTIVE'                  # 撤离中：票据冻结，等待全员到达与清点
EV_WAITING_RESUME = 'WAITING_RESUME' # 待复工：双确认齐全且满足，票据解冻等待恢复作业
EV_RESUMED = 'RESUMED'                # 已复工：票据恢复作业，批次关闭
EV_PREEMPTED = 'PREEMPTED'           # 待复工期间被新告警抢占
EV_TERMINATED = 'TERMINATED'          # 票据收工/撤销等终局：无需复工
EV_OPEN_STATES = (EV_ACTIVE, EV_WAITING_RESUME)
EV_CLOSED_STATES = (EV_RESUMED, EV_PREEMPTED, EV_TERMINATED)

# 确认角色
ROLE_FIELD = 'FIELD'                  # 现场负责人：人员与器具清点
ROLE_DISPATCH = 'DISPATCH'            # 调度员：气象解除/拓扑/隔离边界
# 确认状态
CF_VALID = 'VALID'
CF_SUPERSEDED = 'SUPERSEDED'          # 依据变化后旧确认失效（行不删除）

# 人员名册状态
PR_ON_DUTY = 'ON_DUTY'                # 在册在岗（撤离令当时）
PR_LEFT = 'LEFT'                     # 撤离令前已登记离场：误入名册，禁止签到
# 人员当前去向
LOC_AT_RISK = 'AT_RISK'              # 尚未到达任何避险点
LOC_SAFE = 'SAFE'                    # 已到达某避险点

# 签到裁决（storm_checkins.decision）
CI_CURRENT = 'CURRENT'               # 首次到达：最新有效去向
CI_SUPERSEDED = 'SUPERSEDED'         # 更换避险点后旧去向
CI_DUPLICATE = 'DUPLICATE'           # 重复扫码（同人同点）：回首次结果
# 签到拒绝原因码
R_PROXY = 'PROXY_SIGN_REJECTED'                    # 代签：口令与本人不符
R_NOT_ROSTER = 'PERSON_NOT_IN_ROSTER'             # 未登记人员（名册中查无此人）
R_ALREADY_LEFT = 'PERSON_ALREADY_LEFT'             # 已离场人员误入名册
R_SHELTER_UNKNOWN = 'SHELTER_NOT_FOUND'            # 避险点未登记
R_BATCH_CLOSED = 'EVAC_BATCH_NOT_ACTIVE'           # 批次已结束，不再受理签到

# 监护确认
GD_PENDING = 'PENDING'
GD_ASSIGNED = 'ASSIGNED'             # 已指派监护，待监护人本人确认
GD_GUARDED = 'GUARDED'               # 监护人本人已确认落实
R_GD_PROXY = 'GUARDIAN_PROXY_REJECTED'

# 工器具现场去向申报（storm_tool_sightings.decision）
TS_VALID = 'VALID'                   # 最新有效申报
TS_SUPERSEDED = 'SUPERSEDED'         # 同件后报新去向
TS_DUPLICATE = 'DUPLICATE'           # 同件同去向重放
TS_CONFLICT = 'CONFLICT'             # 同件被两人申报为各自持有：先到为准，后者冲突
TS_NOT_MISSING = 'TOOL_NOT_MISSING'  # 该件本就去向明确（非待查件），无需申报
TS_NOT_ISSUED = 'TOOL_NOT_ON_SITE'   # 该件未领用至现场（仅在库锁定/在库），不属去向不明
# 工器具去向类别（申报）
WHERE_SHELTER = 'SHELTER'            # 随人在避险点
WHERE_BOX = 'BOX'                    # 在工具箱内
WHERE_LOST = 'LOST'                  # 去向不明
# 快照占有类别
POS_LOCKED = 'LOCKED_AT_STORE'       # 批次锁定、在库
POS_ISSUED = 'ISSUED_ON_SITE'        # 已扫码领用（现场）
POS_HELD_BOX = 'HELD_WITH_BOX'       # 随箱持有（箱已移交）
POS_RETURNED = 'RETURNED'            # 已归还

# 复工/清点/确认拒绝原因码
R_BATCH_NOT_OPEN = 'BATCH_NOT_OPEN'
R_FIELD_DUP = 'DUPLICATE_FIELD_CONFIRM'
R_DISPATCH_DUP = 'DUPLICATE_DISPATCH_CONFIRM'
R_ROLE_MISMATCH = 'ROLE_MISMATCH'
R_WEATHER_ACTIVE = 'WEATHER_ALERT_ACTIVE'
R_PERSON_MISSING = 'PERSON_NOT_SAFE'
R_TOOL_MISSING = 'TOOL_LOCATION_UNKNOWN'
R_RISK_UNGUARDED = 'RISK_NOT_GUARDED'
R_BOUNDARY_CHANGED = 'BOUNDARY_VERSION_CHANGED'
R_TICKET_WRONG = 'TICKET_NOT_IN_BATCH'
R_NOT_LEADER = 'NOT_FIELD_LEADER'
R_BASIS_MISMATCH = 'CONFIRM_BASIS_MISMATCH'

# 票据冻结
TF_FROZEN = 'FROZEN'
TF_RESUMED = 'RESUMED'


class RuleError(Exception):
    def __init__(self, http_status, reason_code, detail):
        super().__init__(detail)
        self.http_status = http_status
        self.reason_code = reason_code
        self.detail = detail


# ---------------------------------------------------------------- 小工具

def _now():
    return time.time()


def _now_ms():
    return int(time.time() * 1000)


def _jload(s, default):
    if s is None or s == '':
        return default
    return json.loads(s)


def _ev(db, ticket, actor, etype, **payload):
    db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
         (_now(), ticket, actor, etype, json.dumps(payload, ensure_ascii=False)))


def _decide(db, batch, scene, code, detail, actor, ref=None, accepted=True, extra=None):
    """每条裁决都落 storm_decisions（关键状态与裁决理由可从界面追溯）。"""
    db.x('INSERT INTO storm_decisions(batch,scene,reason_code,detail,actor,ref,accepted,created_at) '
         'VALUES(?,?,?,?,?,?,?,?)',
         (batch, scene, code, detail, actor, ref, 1 if accepted else 0, _now()))
    d = {'accepted': accepted, 'reason_code': code, 'detail': detail}
    if extra:
        d.update(extra)
    return d


def env_version(db):
    """当前环境版本：(拓扑版本, 气象序号, 有效告警序号)。任一依据变化即整体变化。"""
    return {'topology_version': int(db.meta('topo_version')),
            'weather_seq': int(db.meta('storm_weather_seq')),
            'alert_seq': int(db.meta('storm_alert_seq')),
            'digest': ''}


def env_digest(v):
    raw = f"topo={v['topology_version']}|weather={v['weather_seq']}|alert={v['alert_seq']}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def active_alert(db, area):
    """区域当前命中的有效雷暴告警（无则 None）。"""
    return db.one('SELECT * FROM storm_alerts WHERE level=? AND status=\'ACTIVE\' '
                  'AND areas_json LIKE ? ORDER BY seq DESC',
                  (WX_THUNDER, f'%"{area}"%'))


def alerts_hit_area(db, areas):
    out = []
    for r in db.q("SELECT * FROM storm_alerts WHERE level=? AND status='ACTIVE' ORDER BY seq",
                  (WX_THUNDER,)):
        aa = set(_jload(r['areas_json'], []))
        hit = sorted(aa & set(areas))
        if hit:
            d = dict(r)
            d['hit_areas'] = hit
            out.append(d)
    return out


# ---------------------------------------------------------------- 避险点

def list_shelters(db):
    return [{'code': r['code'], 'name': r['name'], 'qr': r['qr_token'],
             'capacity': r['capacity']}
            for r in db.q('SELECT * FROM storm_shelters ORDER BY code')]


def get_shelter(db, code):
    return db.one('SELECT * FROM storm_shelters WHERE code=? OR qr_token=?', (code, code))


# ---------------------------------------------------------------- 名册维护（撤离令之前）

def roster_add_core(db, tid, person, token, actor='现场负责人'):
    with db.lock:
        if not db.one('SELECT id FROM tickets WHERE id=?', (tid,)):
            raise RuleError(404, 'TICKET_NOT_FOUND', f'安全票 {tid} 不存在')
        if db.one('SELECT 1 FROM storm_roster WHERE ticket=? AND person=?', (tid, person)):
            raise RuleError(409, 'ROSTER_PERSON_EXISTS', f'{person} 已在 {tid} 名册中')
        token = token or ('PW' + uuid.uuid4().hex[:10].upper())
        db.x('INSERT INTO storm_roster(ticket,person,token,status,current_loc,updated_at) '
             'VALUES(?,?,?,?,?,?)', (tid, person, token, PR_ON_DUTY, LOC_AT_RISK, _now()))
        _ev(db, tid, actor, 'ROSTER_ADDED', person=person)
        db.commit()
        return {'ok': True, 'ticket': tid, 'person': person, 'token': token}


def roster_mark_left_core(db, tid, person, actor='现场负责人'):
    """撤离令前登记离场：固定名册时将作为「已离场」成员保留，签到一律拒绝。"""
    with db.lock:
        row = db.one('SELECT * FROM storm_roster WHERE ticket=? AND person=?', (tid, person))
        if not row:
            raise RuleError(404, 'PERSON_NOT_IN_ROSTER', f'{person} 不在 {tid} 名册中')
        if row['status'] == PR_LEFT:
            return {'ok': True, 'duplicate': True, 'person': person, 'status': PR_LEFT}
        db.x('UPDATE storm_roster SET status=?, current_loc=?, updated_at=? WHERE ticket=? AND person=?',
             (PR_LEFT, None, _now(), tid, person))
        _ev(db, tid, actor, 'ROSTER_MARKED_LEFT', person=person)
        db.commit()
        return {'ok': True, 'person': person, 'status': PR_LEFT}


# ---------------------------------------------------------------- 风险点计算

def compute_risks(db, t, topo):
    """撤离当时仍带电的风险点（快照固定，之后拓扑变化不影响名册内容）。

    - 工作区内仍带电设备（撤离发生在隔离完成前时，最直接的雷击/反送伤害风险）；
    - 工作区侧边界开关仍带电（隔离未完成）；
    - 边界开关电源侧仍带电（隔离已完成也存在：开关一旦误合即向工作区反送电，
      雷暴撤离期间必须定点看守）；
    - 接地线挂接点带电（恶性事故征兆）。
    """
    en = topo.energized()
    risks = []
    for w in t['work_area']:
        if w in en:
            risks.append({'device': w, 'kind': 'WORK_AREA_ENERGIZED',
                          'detail': f'工作区设备 {w} 仍带电，雷击感应/反送电风险'})
    for sw in t['boundary'].get('switches', []):
        swid, side = sw['device'], sw.get('side')
        if side and side in en:
            risks.append({'device': swid, 'kind': 'BOUNDARY_WORK_SIDE_LIVE',
                          'detail': f'边界开关 {swid} 工作侧 {side} 仍带电（隔离未完成）'})
        # 电源侧（工作区对侧）仍带电：防误合反送，撤离期间须落实监护
        live_src = sorted(m for m in topo.adj.get(swid, ()) if m != side and m in en)
        if live_src:
            risks.append({'device': swid, 'kind': 'BOUNDARY_SOURCE_SIDE_LIVE',
                          'detail': f'边界开关 {swid} 电源侧 {live_src} 仍带电，'
                                    f'须看守防止误合向工作区反送电'})
    for point, _sw, _tid in topo.grounds:
        if point in en:
            risks.append({'device': point, 'kind': 'GROUNDED_POINT_LIVE',
                          'detail': f'接地挂接点 {point} 带电'})
    return risks


# ---------------------------------------------------------------- 工器具占有快照

def tool_snapshot(db, tid):
    """撤离令当时现场工器具占有情况（逐件快照，之后借还台变化不影响名册）。"""
    items = []
    # 1) 生效批次锁定行：在库锁定
    for lk in tools_mod.active_lock_rows(db, tid):
        serial = lk['item_serial']
        trow = db.one('SELECT state FROM tools WHERE serial=?', (serial,))
        co = db.one("SELECT box FROM checkouts WHERE serial=? AND code=? ORDER BY checked_out_at DESC LIMIT 1",
                    (serial, lk['code']))
        if co:
            box = db.one('SELECT * FROM toolboxes WHERE id=?', (co['box'],))
            possession = POS_HELD_BOX if box and box['status'] == tools_mod.BX_HELD else POS_ISSUED
            box_id = co['box']
        else:
            possession = POS_LOCKED
            box_id = None
        items.append({'code': lk['code'], 'name': tools_mod.TOOL_NAMES.get(lk['code'], lk['code']),
                      'batch': lk['batch_id'], 'serial': serial,
                      'possession': possession, 'box': box_id,
                      'item_state': trow['state'] if trow else None})
    # 2) 该票历史领用、锁已释放但仍在现场（箱未归还）的件
    for co in db.q('SELECT DISTINCT serial, code, batch, box FROM checkouts WHERE ticket=? '
                   'ORDER BY serial', (tid,)):
        if any(i['serial'] == co['serial'] for i in items):
            continue
        box = db.one('SELECT * FROM toolboxes WHERE id=?', (co['box'],))
        if box and box['status'] in tools_mod.BX_TERMINAL:
            possession = POS_RETURNED
        elif box and box['status'] == tools_mod.BX_HELD:
            possession = POS_HELD_BOX
        else:
            possession = POS_ISSUED
        items.append({'code': co['code'], 'name': tools_mod.TOOL_NAMES.get(co['code'], co['code']),
                      'batch': co['batch'], 'serial': co['serial'],
                      'possession': possession, 'box': co['box'], 'item_state': None})
    return items


# ---------------------------------------------------------------- 撤离令

def order_evac_core(db, tid, actor='值班长', reason=None, idem_key=None, alert_id=None):
    """发布撤离令：同一事务固定名册/风险点/工器具/票据阶段，票据立即冻结。

    幂等：
    - 票据存在开放批次（撤离中/待复工）→ 返回首个批次（重复撤离令）；
    - 票据最新批次为新告警抢占 → 针对该告警开新批次；
    - 票据最新批次已终局（复工/终止）→ 新开撤离批次（二次撤离）。
    """
    key = idem_key or f'evac:{tid}:{alert_id or "manual"}'
    with db.lock:
        old = db.one('SELECT response FROM idem WHERE key=?', (key,))
        if old:
            resp0 = json.loads(old['response'])
            # 首次撤离的原始响应不打重复标记；重放时标注返回首次结果
            resp0['duplicate'] = True
            resp0.setdefault('reason_code', 'DUPLICATE_EVAC_ORDER')
            _ev(db, tid, 'system', 'DUPLICATE_IGNORED', key=key, scope='evac_order')
            db.commit()
            return resp0

        t_row = db.one('SELECT * FROM tickets WHERE id=?', (tid,))
        if not t_row:
            raise RuleError(404, 'TICKET_NOT_FOUND', f'安全票 {tid} 不存在')
        t = dict(t_row)
        t['work_area'] = json.loads(t['work_area'])
        t['boundary'] = json.loads(t['boundary'])
        if t['status'] not in ('ISOLATING', 'PERMITTED', 'RESTORING'):
            raise RuleError(409, 'TICKET_NOT_ACTIVE',
                            f"安全票 {tid} 已 {t['status']}，撤离令/收工/撤销并发时以票据终局为准，不再开撤离批次")

        open_b = db.one("SELECT * FROM storm_batches WHERE ticket=? AND status IN (?,?) "
                        "ORDER BY created_at DESC", (tid, EV_ACTIVE, EV_WAITING_RESUME))
        if open_b:
            resp = batch_view(db, open_b['id'])
            resp['duplicate'] = True
            resp['reason_code'] = 'DUPLICATE_EVAC_ORDER'
            resp['detail'] = f'撤离批次 {open_b["id"]} 仍在开放中，重复撤离令返回首次结果'
            db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
                 (key, json.dumps(resp, ensure_ascii=False)))
            _decide(db, open_b['id'], 'EVAC_ORDER', 'DUPLICATE_EVAC_ORDER',
                    resp['detail'], actor, accepted=True)
            _ev(db, tid, actor, 'EVAC_ORDER_DUPLICATE', batch=open_b['id'])
            db.commit()
            return resp

        latest = db.one('SELECT * FROM storm_batches WHERE ticket=? ORDER BY created_at DESC', (tid,))
        if latest:
            if latest['status'] in (EV_RESUMED, EV_TERMINATED):
                pass  # 正常二次撤离
            elif latest['status'] == EV_PREEMPTED:
                # 抢占批次针对的是上一轮告警；必须有新的命中告警才能再开
                if not alert_id:
                    raise RuleError(409, 'BATCH_PREEMPTED',
                                    f'待复工批次 {latest["id"]} 已被新告警抢占，请确认新的气象告警后再发撤离令')
            else:
                raise RuleError(409, 'BATCH_NOT_OPEN', f'批次 {latest["id"]} 状态 {latest["status"]} 不可重开')

        import server as srv
        topo = srv.load_topo(db)

        env = env_version(db)
        env['digest'] = env_digest(env)
        roster = [dict(r) for r in db.q(
            'SELECT person,token,status,current_loc FROM storm_roster WHERE ticket=? ORDER BY person',
            (tid,))]
        on_duty = [r for r in roster if r['status'] == PR_ON_DUTY]
        risks = compute_risks(db, t, topo)
        tools = tool_snapshot(db, tid)

        bid = 'EV' + time.strftime('%H%M%S') + uuid.uuid4().hex[:6].upper()
        db.x('INSERT INTO storm_batches(id,ticket,seq,status,alert_id,reason,'
             'env_digest,env_topology,env_weather_seq,env_alert_seq,leader,'
             'created_by,created_at,resumed_at,preempt_reason) '
             'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL)',
             (bid, tid, (latest['seq'] + 1) if latest else 1, EV_ACTIVE, alert_id,
              reason or '雷暴告警命中作业区域，值班长发布撤离令',
              env['digest'], env['topology_version'], env['weather_seq'], env['alert_seq'],
              actor, actor, _now()))
        for p in roster:
            db.x('INSERT INTO storm_roster_snap(batch,ticket,person,token,status,current_loc) '
                 'VALUES(?,?,?,?,?,?)',
                 (bid, tid, p['person'], p['token'], p['status'],
                  LOC_AT_RISK if p['status'] == PR_ON_DUTY else None))
        for i, r in enumerate(risks, 1):
            db.x('INSERT INTO storm_risks(batch,line_no,device,kind,detail,guardian,status) '
                 'VALUES(?,?,?,?,?,?,?)',
                 (bid, i, r['device'], r['kind'], r['detail'], None, GD_PENDING))
        for i, it in enumerate(tools, 1):
            db.x('INSERT INTO storm_tools(batch,line_no,code,name,batch_id_item,serial,possession,'
                  'box,location_known,location_where,location_holder,updated_at) '
                 'VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                 (bid, i, it['code'], it['name'], it['batch'], it['serial'], it['possession'],
                  it['box'],
                  1 if it['possession'] in (POS_LOCKED, POS_HELD_BOX, POS_RETURNED) else 0,
                  None, None, _now()))
        db.x('INSERT INTO storm_tickets(batch,ticket,status_at_order,frozen,frozen_at) '
             'VALUES(?,?,?,1,?)', (bid, tid, t['status'], _now()))
        # 票据立即冻结（撤离中禁止任何新增作业；恢复步骤同样阻断）
        db.x('UPDATE tickets SET frozen=1, freeze_reason=? WHERE id=?',
             (f'雷暴撤离令 {bid} 已发布：全员未确认安全/风险点未监护/器具未清点前禁止作业', tid))
        # 固定各安全票阶段（全部在同事务）
        _ev(db, tid, actor, 'EVAC_ORDERED', batch=bid, alert=alert_id,
            roster=[p['person'] for p in roster], on_duty=[p['person'] for p in on_duty],
            risks=[r['device'] for r in risks], tools=len(tools),
            env=env, ticket_status=t['status'])
        _decide(db, bid, 'EVAC_ORDER', 'EVAC_ORDERED',
                f'已固定名册 {len(on_duty)} 人、带电风险点 {len(risks)} 处、'
                f'工器具 {len(tools)} 件、票据阶段 {t["status"]}；票据已冻结',
                actor, accepted=True,
                extra={'roster_total': len(roster), 'on_duty': len(on_duty),
                       'risks': len(risks), 'tools': len(tools)})
        resp = batch_view(db, bid)
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(resp, ensure_ascii=False)))
        db.commit()
        return resp


# ---------------------------------------------------------------- 签到

def _person_snap(db, batch, person):
    return db.one('SELECT * FROM storm_roster_snap WHERE batch=? AND person=?', (batch, person))


def checkin_core(db, batch, person, token, shelter_code, actor=None,
                 client_id=None, client_seq=None, idem_key=None):
    """移动端扫避险点码到达。调用方已持 db.lock。返回裁决 dict（含 duplicate）。"""
    key = idem_key or f'ci:{batch}:{client_id or person}:{client_seq if client_seq is not None else token}:{shelter_code}'
    old = db.one('SELECT response FROM idem WHERE key=?', (key,))
    if old:
        resp = json.loads(old['response'])
        resp['duplicate'] = True
        _ev(db, None, 'system', 'DUPLICATE_IGNORED', key=key, scope='evac_checkin')
        return resp

    b = db.one('SELECT * FROM storm_batches WHERE id=?', (batch,))
    if not b:
        raise RuleError(404, 'EVAC_BATCH_NOT_FOUND', f'撤离批次 {batch} 不存在')
    actor = actor or person
    sh = get_shelter(db, shelter_code)
    if not sh:
        d = _decide(db, batch, 'CHECKIN', R_SHELTER_UNKNOWN,
                    f'避险点 {shelter_code} 未登记，不能签到', actor, ref=person, accepted=False)
        resp = {'ok': False, **d}
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(resp, ensure_ascii=False)))
        return resp
    if b['status'] not in EV_OPEN_STATES:
        d = _decide(db, batch, 'CHECKIN', R_BATCH_CLOSED,
                    f'撤离批次 {batch} 已结束（{b["status"]}），不再受理签到', actor,
                    ref=person, accepted=False)
        resp = {'ok': False, **d}
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(resp, ensure_ascii=False)))
        return resp

    p = _person_snap(db, batch, person)
    if not p:
        d = _decide(db, batch, 'CHECKIN', R_NOT_ROSTER,
                    f'未登记人员 {person} 不在撤离批次 {batch} 的固定名册中，拒绝签到',
                    actor, ref=person, accepted=False)
        resp = {'ok': False, **d}
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(resp, ensure_ascii=False)))
        return resp
    # 代签：口令必须是该成员本人的名册口令
    if token != p['token']:
        d = _decide(db, batch, 'CHECKIN', R_PROXY,
                    f'{person} 的签到口令与本人名册口令不符，判定代签，拒绝',
                    actor, ref=person, accepted=False, extra={'proxy_by': actor})
        resp = {'ok': False, **d}
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(resp, ensure_ascii=False)))
        return resp
    if p['status'] == PR_LEFT:
        d = _decide(db, batch, 'CHECKIN', R_ALREADY_LEFT,
                    f'{person} 在撤离令前已登记离场，误入名册，拒绝签到',
                    actor, ref=person, accepted=False)
        resp = {'ok': False, **d}
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(resp, ensure_ascii=False)))
        return resp

    # 同人同点重复扫码：回首次结果
    prior = db.one('SELECT * FROM storm_checkins WHERE batch=? AND person=? AND shelter=? '
                   'ORDER BY rowid', (batch, person, sh['code']))
    if prior:
        d = {'accepted': True, 'reason_code': CI_DUPLICATE,
             'detail': f'{person} 在避险点 {sh["code"]} 重复扫码，返回首次结果，不多写签到',
             'person': person, 'shelter': sh['code'], 'decision': prior['decision']}
        _ev(db, b['ticket'], person, 'SHELTER_DUPLICATE_SCAN', batch=batch,
            shelter=sh['code'])
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(d, ensure_ascii=False)))
        return {'ok': True, 'duplicate': True, **d}

    # 更换避险点：旧有效去向置 SUPERSEDED，仅最新一条 CURRENT
    cur = db.one("SELECT * FROM storm_checkins WHERE batch=? AND person=? AND decision='CURRENT'",
                 (batch, person))
    decision = CI_CURRENT
    if cur:
        decision = CI_SUPERSEDED
        db.x("UPDATE storm_checkins SET decision='SUPERSEDED', superseded_at=? WHERE rowid=?",
             (_now(), cur['rowid']))
    db.x('INSERT INTO storm_checkins(batch,ticket,person,shelter,decision,token_ok,actor,'
         'client_id,client_seq,checked_in_at,superseded_at) VALUES(?,?,?,?,?,1,?,?,?,?,NULL)',
         (batch, b['ticket'], person, sh['code'], 'CURRENT', actor,
          client_id, client_seq, _now()))
    db.x("UPDATE storm_roster_snap SET current_loc=? WHERE batch=? AND person=?",
         (LOC_SAFE, batch, person))
    _decide(db, batch, 'CHECKIN', decision,
            (f'{person} 首次到达避险点 {sh["code"]}，记为最新有效去向' if decision == CI_CURRENT
             else f'{person} 从避险点 {cur["shelter"]} 改至 {sh["code"]}：旧去向置 SUPERSEDED，仅保留最新去向'),
            actor, ref=person, accepted=True,
            extra={'person': person, 'shelter': sh['code'], 'decision': 'CURRENT'})
    resp = {'ok': True, 'person': person, 'shelter': sh['code'],
            'decision': 'CURRENT', 'reason_code': decision}
    db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
         (key, json.dumps(resp, ensure_ascii=False)))
    return resp


# ---------------------------------------------------------------- 风险点监护

def assign_guard_core(db, batch, line_no, guardian, actor='值班长'):
    with db.lock:
        b = db.one('SELECT * FROM storm_batches WHERE id=?', (batch,))
        if not b:
            raise RuleError(404, 'EVAC_BATCH_NOT_FOUND', f'撤离批次 {batch} 不存在')
        r = db.one('SELECT * FROM storm_risks WHERE batch=? AND line_no=?', (batch, line_no))
        if not r:
            raise RuleError(404, 'RISK_NOT_FOUND', f'批次 {batch} 无第 {line_no} 号风险点')
        p = _person_snap(db, batch, guardian)
        if not p or p['status'] != PR_ON_DUTY:
            raise RuleError(409, 'GUARDIAN_NOT_AVAILABLE',
                            f'监护人 {guardian} 不在册在岗，不能承担监护')
        db.x('UPDATE storm_risks SET guardian=?, status=? WHERE batch=? AND line_no=?',
             (guardian, GD_ASSIGNED, batch, line_no))
        _ev(db, b['ticket'], actor, 'RISK_GUARD_ASSIGNED', batch=batch, line_no=line_no,
            device=r['device'], guardian=guardian)
        _decide(db, batch, 'RISK_GUARD', 'GUARDIAN_ASSIGNED',
                f'风险点 {r["device"]} 已指派监护人 {guardian}，待其本人确认', actor,
                ref=r['device'])
        db.commit()
        return {'ok': True, 'line_no': line_no, 'device': r['device'],
                'guardian': guardian, 'status': GD_ASSIGNED}


def confirm_guard_core(db, batch, line_no, guardian, token, actor=None, idem_key=None):
    """监护人本人确认已到风险点落实监护（口令校验，防代签）。"""
    key = idem_key or f'gd:{batch}:{line_no}:{guardian}'
    with db.lock:
        old = db.one('SELECT response FROM idem WHERE key=?', (key,))
        if old:
            return {**json.loads(old['response']), 'duplicate': True}
        b = db.one('SELECT * FROM storm_batches WHERE id=?', (batch,))
        if not b:
            raise RuleError(404, 'EVAC_BATCH_NOT_FOUND', f'撤离批次 {batch} 不存在')
        r = db.one('SELECT * FROM storm_risks WHERE batch=? AND line_no=?', (batch, line_no))
        if not r:
            raise RuleError(404, 'RISK_NOT_FOUND', f'批次 {batch} 无第 {line_no} 号风险点')
        p = _person_snap(db, batch, guardian)
        actor = actor or guardian
        if not p or p['status'] != PR_ON_DUTY or token != p['token']:
            _decide(db, batch, 'RISK_GUARD', R_GD_PROXY,
                    f'{guardian} 监护确认口令不符或非在册在岗成员，判定代签，拒绝',
                    actor, ref=r['device'], accepted=False)
            db.commit()
            raise RuleError(409, R_GD_PROXY, '监护确认被拒：口令不符或监护人不在名册')
        if r['guardian'] != guardian:
            raise RuleError(409, 'GUARDIAN_MISMATCH',
                            f"该风险点指派的监护人是 {r['guardian']}，{guardian} 不能代为确认")
        if r['status'] == GD_GUARDED:
            resp = {'ok': True, 'duplicate': True, 'line_no': line_no, 'status': GD_GUARDED}
            db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
                 (key, json.dumps(resp, ensure_ascii=False)))
            db.commit()
            return resp
        db.x('UPDATE storm_risks SET status=? WHERE batch=? AND line_no=?',
             (GD_GUARDED, batch, line_no))
        _ev(db, b['ticket'], guardian, 'RISK_GUARDED', batch=batch, line_no=line_no,
            device=r['device'])
        _decide(db, batch, 'RISK_GUARD', 'GUARDIAN_CONFIRMED',
                f'风险点 {r["device"]} 监护人 {guardian} 本人已确认落实监护',
                guardian, ref=r['device'])
        resp = {'ok': True, 'line_no': line_no, 'device': r['device'],
                'guardian': guardian, 'status': GD_GUARDED}
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(resp, ensure_ascii=False)))
        db.commit()
        return resp


# ---------------------------------------------------------------- 工器具去向申报

def _sighting_result(db, batch, serial, where, holder, actor):
    """同一工具后报裁决：同件同去向重放 DUPLICATE；同人最新覆盖；两人冲突先到为准。

    只负责把既有 VALID 行置 SUPERSEDED，不做新插入；返回 (decision, detail)。
    """
    same = list(db.q('SELECT * FROM storm_tool_sightings WHERE batch=? AND serial=? ORDER BY rowid',
                     (batch, serial)))
    valid = [s for s in same if s['decision'] == TS_VALID]
    if valid:
        last = valid[-1]
        if where != WHERE_LOST and last['location_where'] == where \
                and last['location_holder'] == holder:
            return TS_DUPLICATE, '同件同去向重复申报，返回首次结果，不多写'
        # LOST（去向不明）不是占有主张：任何人之后报找回都可覆盖；
        # 持有冲突仅在两条“明确去向”申报持有人不同且都非遗失时成立。
        if (where != WHERE_LOST and last['location_where'] != WHERE_LOST
                and last['location_holder'] != holder):
            loc_text = {'SHELTER': '避险点', 'BOX': '箱内'}.get(last['location_where'], '?')
            return TS_CONFLICT, (
                f"{serial} 已由 {last['location_holder']} 申报在{loc_text}，"
                f"{actor} 又申报为 {holder} 持有：先到为准，本次冲突拒绝")
        # 持有人改报去向 / 报遗失 / 遗失后报找回：旧 VALID 置 SUPERSEDED
        db.x("UPDATE storm_tool_sightings SET decision='SUPERSEDED', superseded_at=? WHERE rowid=?",
             (_now(), last['rowid']))
        label = {'SHELTER': '随人在避险点', 'BOX': '在工具箱内', 'LOST': '去向不明'}[where]
        return TS_SUPERSEDED, f'{serial} 去向更新为：{label}' + \
            (f'（持有人 {holder}）' if holder else '')
    return TS_VALID, f'{serial} 去向已申报：' + \
        {'SHELTER': '随人在避险点', 'BOX': '在工具箱内', 'LOST': '去向不明'}[where] \
        + (f'（持有人 {holder}）' if holder else '')


def tool_sighting_core(db, batch, serial, where, holder=None, actor=None, idem_key=None):
    """现场申报关键工器具的现场去向（更换/冲突/重放均给出确定裁决）。"""
    key = idem_key or f'ts:{batch}:{serial}:{where}:{holder or actor or "?"}'
    with db.lock:
        old = db.one('SELECT response FROM idem WHERE key=?', (key,))
        if old:
            return {**json.loads(old['response']), 'duplicate': True}
        b = db.one('SELECT * FROM storm_batches WHERE id=?', (batch,))
        if not b:
            raise RuleError(404, 'EVAC_BATCH_NOT_FOUND', f'撤离批次 {batch} 不存在')
        actor = actor or holder or '现场负责人'
        line = db.one('SELECT * FROM storm_tools WHERE batch=? AND serial=?', (batch, serial))
        if not line:
            d = _decide(db, batch, 'TOOL_SIGHTING', 'TOOL_NOT_IN_SNAPSHOT',
                        f'{serial} 不在撤离批次 {batch} 的工器具占有快照中，拒绝申报',
                        actor, ref=serial, accepted=False)
            db.commit()
            raise RuleError(404, 'TOOL_NOT_IN_SNAPSHOT', d['detail'])
        if where not in (WHERE_SHELTER, WHERE_BOX, WHERE_LOST):
            raise RuleError(400, 'BAD_WHERE', "where 须为 SHELTER/BOX/LOST")
        # 仅在现场（已领用/随箱）的件才需要现场去向；在库锁定/已归还不属去向不明
        if line['possession'] in (POS_LOCKED, POS_RETURNED):
            d = _decide(db, batch, 'TOOL_SIGHTING', TS_NOT_ISSUED,
                        f'{serial} 快照占有为「{line["possession"]}」，未领用至现场，无需申报去向',
                        actor, ref=serial, accepted=False)
            resp = {'ok': False, **d}
            db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
                 (key, json.dumps(resp, ensure_ascii=False)))
            db.commit()
            return resp

        decision, detail = _sighting_result(db, batch, serial, where, holder, actor)
        accepted = decision in (TS_VALID, TS_SUPERSEDED, TS_DUPLICATE)
        if decision == TS_CONFLICT:
            # 冲突只留痕（非 VALID 行不受唯一索引限制），不改变工具当前去向
            db.x('INSERT INTO storm_tool_sightings(batch,serial,location_where,location_holder,'
                 'actor,decision,created_at,superseded_at) VALUES(?,?,?,?,?,?,?,NULL)',
                 (batch, serial, where, holder, actor, TS_CONFLICT, _now()))
        elif decision != TS_DUPLICATE:
            db.x('INSERT INTO storm_tool_sightings(batch,serial,location_where,location_holder,'
                 'actor,decision,created_at,superseded_at) VALUES(?,?,?,?,?,?,?,NULL)',
                 (batch, serial, where, holder, actor, 'VALID', _now()))
            known = where != WHERE_LOST
            db.x('UPDATE storm_tools SET location_known=?, location_where=?, location_holder=?, updated_at=? '
                 'WHERE batch=? AND serial=?',
                 (1 if known else 0, None if where == WHERE_LOST else where,
                  holder, _now(), batch, serial))
        _decide(db, batch, 'TOOL_SIGHTING', decision, detail, actor, ref=serial,
                accepted=accepted)
        resp = {'ok': accepted, 'serial': serial, 'decision': decision,
                'reason_code': decision, 'detail': detail}
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(resp, ensure_ascii=False)))
        db.commit()
        return resp


# ---------------------------------------------------------------- 未清项

def open_blockers(db, bid):
    """复工闸门未清项：人员未安全 / 风险点未监护 / 关键器具去向不明。"""
    missing = [r['person'] for r in db.q(
        "SELECT person FROM storm_roster_snap WHERE batch=? AND status=? AND current_loc!=?",
        (bid, PR_ON_DUTY, LOC_SAFE))]
    unguarded = [{'line_no': r['line_no'], 'device': r['device'], 'status': r['status'],
                  'guardian': r['guardian'], 'detail': r['detail']}
                 for r in db.q('SELECT * FROM storm_risks WHERE batch=? AND status!=? ORDER BY line_no',
                               (bid, GD_GUARDED))]
    unknown_tools = [{'line_no': r['line_no'], 'serial': r['serial'], 'name': r['name'],
                      'possession': r['possession'], 'location_where': r['location_where'],
                      'location_holder': r['location_holder']}
                     for r in db.q('SELECT * FROM storm_tools WHERE batch=? AND location_known=0 '
                                   'ORDER BY line_no', (bid,))]
    return missing, unguarded, unknown_tools


# ---------------------------------------------------------------- 双角色确认

def _current_confirm(db, bid, role):
    return db.one('SELECT * FROM storm_confirms WHERE batch=? AND role=? AND status=?',
                  (bid, role, CF_VALID))


def _basis_current(db, b):
    """确认所依据的环境版本当前是否仍成立（气象须解除、拓扑须未变、边界须成立）。"""
    env = env_version(db)
    same = (b['env_topology'] == env['topology_version']
            and b['env_weather_seq'] == env['weather_seq']
            and b['env_alert_seq'] == env['alert_seq'])
    return same, env


def field_confirm_core(db, bid, leader, actor=None, idem_key=None):
    """现场负责人确认人员与器具清点（只有人能提交，未清项给出确定拒绝原因）。

    重放语义：相同幂等键（或同批次同负责人同环境版本）永远返回首次结果，不多写；
    环境版本变化后自然键随之变化，按新版本产生新确认，旧确认置 SUPERSEDED 保留。
    """
    with db.lock:
        b = db.one('SELECT * FROM storm_batches WHERE id=?', (bid,))
        if not b:
            raise RuleError(404, 'EVAC_BATCH_NOT_FOUND', f'撤离批次 {bid} 不存在')
        env0 = env_version(db)
        env0['digest'] = env_digest(env0)
        key = idem_key or f'cf-field:{bid}:{leader}:{env0["digest"]}'
        old = db.one('SELECT response FROM idem WHERE key=?', (key,))
        if old:
            return {**json.loads(old['response']), 'duplicate': True}
        actor = actor or leader
        if b['status'] not in EV_OPEN_STATES:
            raise RuleError(409, R_BATCH_NOT_OPEN, f'批次 {bid} 已 {b["status"]}，不能再确认')
        missing, unguarded, unknown_tools = open_blockers(db, bid)
        problems = []
        if missing:
            problems.append(R_PERSON_MISSING)
        if unknown_tools:
            problems.append(R_TOOL_MISSING)
        if unguarded:
            problems.append(R_RISK_UNGUARDED)
        env = env_version(db)
        env['digest'] = env_digest(env)
        if problems:
            parts = []
            if R_PERSON_MISSING in problems:
                parts.append(f'人员未全员安全 {missing}')
            if R_TOOL_MISSING in problems:
                parts.append(f'关键器具去向不明 {[t["serial"] for t in unknown_tools]}')
            if R_RISK_UNGUARDED in problems:
                parts.append(f'风险点未落实监护 {[u["device"] for u in unguarded]}')
            detail = '清点未完成：' + '；'.join(parts)
            _decide(db, bid, 'FIELD_CONFIRM', problems[0], detail, actor, accepted=False)
            db.commit()
            raise RuleError(409, problems[0], detail)
        # 重放同环境版本：已有有效确认返回首次结果
        ex = _current_confirm(db, bid, ROLE_FIELD)
        if ex and ex['env_digest'] == env['digest']:
            resp = {'ok': True, 'duplicate': True, 'role': ROLE_FIELD,
                    'reason_code': R_FIELD_DUP, 'env': env}
            db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
                 (key, json.dumps(resp, ensure_ascii=False)))
            db.commit()
            return resp
        # 依据已变化：旧确认置失效（行不删除）
        if ex:
            db.x("UPDATE storm_confirms SET status=?, superseded_at=? WHERE rowid=?",
                 (CF_SUPERSEDED, _now(), ex['rowid']))
            _decide(db, bid, 'FIELD_CONFIRM', CF_SUPERSEDED,
                    f'环境版本已变化（{ex["env_digest"]} → {env["digest"]}），现场旧确认失效',
                    actor, accepted=False)
        db.x('INSERT INTO storm_confirms(batch,role,leader,env_digest,env_topology,'
             'env_weather_seq,env_alert_seq,status,created_at,superseded_at) '
             'VALUES(?,?,?,?,?,?,?,?,?,NULL)',
             (bid, ROLE_FIELD, leader, env['digest'], env['topology_version'],
              env['weather_seq'], env['alert_seq'], CF_VALID, _now()))
        _ev(db, b['ticket'], leader, 'FIELD_CONFIRMED', batch=bid, env=env,
            persons_safe=True, tools_known=True)
        _decide(db, bid, 'FIELD_CONFIRM', 'FIELD_CONFIRMED',
                f'现场负责人 {leader} 确认人员与器具清点完成（环境 {env["digest"]}）', actor)
        resp = {'ok': True, 'role': ROLE_FIELD, 'leader': leader, 'env': env}
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(resp, ensure_ascii=False)))
        _maybe_resume(db, bid, actor)
        db.commit()
        return resp


def dispatcher_confirm_core(db, bid, dispatcher, actor=None, idem_key=None):
    """调度员核验气象解除、网络拓扑与隔离边界（三项都须成立）。

    重放语义：相同幂等键（或同批次同调度同环境版本）返回首次结果；环境变化后自然键
    变化，产生新确认，旧确认置 SUPERSEDED 保留。
    """
    with db.lock:
        b = db.one('SELECT * FROM storm_batches WHERE id=?', (bid,))
        if not b:
            raise RuleError(404, 'EVAC_BATCH_NOT_FOUND', f'撤离批次 {bid} 不存在')
        env0 = env_version(db)
        env0['digest'] = env_digest(env0)
        key = idem_key or f'cf-dispatch:{bid}:{dispatcher}:{env0["digest"]}'
        old = db.one('SELECT response FROM idem WHERE key=?', (key,))
        if old:
            return {**json.loads(old['response']), 'duplicate': True}
        actor = actor or dispatcher
        if b['status'] not in EV_OPEN_STATES:
            raise RuleError(409, R_BATCH_NOT_OPEN, f'批次 {bid} 已 {b["status"]}，不能再确认')
        problems = []
        t_row = db.one('SELECT * FROM tickets WHERE id=?', (b['ticket'],))
        t = dict(t_row)
        t['work_area'] = json.loads(t['work_area'])
        t['boundary'] = json.loads(t['boundary'])
        hit = alerts_hit_area(db, t['work_area'])
        if hit:
            problems.append(R_WEATHER_ACTIVE)
        env = env0
        env['digest'] = env_digest(env)
        import server as srv
        topo = srv.load_topo(db)
        if env['topology_version'] != int(b['env_topology']):
            problems.append(R_BOUNDARY_CHANGED)
        # 隔离边界核验（复工专用）：边界开关必须仍在分位、工作区保持无电；
        # 标示牌未挂/缺失不在此判定（撤离可能先于隔离挂牌，属既有状态而非边界被破坏）
        boundary_problems = []
        if t['status'] in ('ISOLATING', 'PERMITTED', 'RESTORING'):
            for sw in t['boundary'].get('switches', []):
                if topo.devices.get(sw['device']) and topo.devices[sw['device']]['state'] != 'OPEN':
                    boundary_problems.append(f"边界开关 {sw['device']} 已不在分位（当前 "
                                            f"{topo.devices[sw['device']]['state']}）")
            if t['status'] == 'PERMITTED' and any(w in topo.energized() for w in t['work_area']):
                boundary_problems.append('工作区设备恢复带电')
            for point, _sw, _tid in topo.grounds:
                if point in topo.energized():
                    boundary_problems.append(f'严重：{point} 处接地线带电')
        if boundary_problems:
            problems.append('BOUNDARY_NOT_HOLD')
        if problems:
            parts = []
            if R_WEATHER_ACTIVE in problems:
                parts.append(f'气象告警仍命中 {[a["alert_id"] for a in hit]}')
            if R_BOUNDARY_CHANGED in problems:
                parts.append(f'拓扑版本变化（撤离时 v{b["env_topology"]}，当前 v{env["topology_version"]}）')
            if 'BOUNDARY_NOT_HOLD' in problems:
                parts.append(f'隔离边界不成立：{boundary_problems}')
            detail = '调度核验未通过：' + '；'.join(parts)
            _decide(db, bid, 'DISPATCH_CONFIRM', problems[0], detail, actor, accepted=False)
            db.commit()
            raise RuleError(409, problems[0], detail)
        ex = _current_confirm(db, bid, ROLE_DISPATCH)
        if ex and ex['env_digest'] == env['digest']:
            resp = {'ok': True, 'duplicate': True, 'role': ROLE_DISPATCH,
                    'reason_code': R_DISPATCH_DUP, 'env': env}
            db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
                 (key, json.dumps(resp, ensure_ascii=False)))
            db.commit()
            return resp
        if ex:
            db.x("UPDATE storm_confirms SET status=?, superseded_at=? WHERE rowid=?",
                 (CF_SUPERSEDED, _now(), ex['rowid']))
            _decide(db, bid, 'DISPATCH_CONFIRM', CF_SUPERSEDED,
                    f'环境版本已变化（{ex["env_digest"]} → {env["digest"]}），调度旧确认失效',
                    actor, accepted=False)
        db.x('INSERT INTO storm_confirms(batch,role,leader,env_digest,env_topology,'
             'env_weather_seq,env_alert_seq,status,created_at,superseded_at) '
             'VALUES(?,?,?,?,?,?,?,?,?,NULL)',
             (bid, ROLE_DISPATCH, dispatcher, env['digest'], env['topology_version'],
              env['weather_seq'], env['alert_seq'], CF_VALID, _now()))
        _ev(db, b['ticket'], dispatcher, 'DISPATCH_CONFIRMED', batch=bid, env=env,
            weather_cleared=True, topology_ok=True, boundary_holds=True)
        _decide(db, bid, 'DISPATCH_CONFIRM', 'DISPATCH_CONFIRMED',
                f'调度员 {dispatcher} 核验气象解除、拓扑 v{env["topology_version"]} 与隔离边界均成立'
                f'（环境 {env["digest"]}）', actor)
        resp = {'ok': True, 'role': ROLE_DISPATCH, 'leader': dispatcher, 'env': env}
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(resp, ensure_ascii=False)))
        _maybe_resume(db, bid, actor)
        db.commit()
        return resp


def _maybe_resume(db, bid, actor):
    """两项确认须针对同一撤离批次与同一环境版本；齐全则唯一过渡到待复工并解冻票据。"""
    b = db.one('SELECT * FROM storm_batches WHERE id=?', (bid,))
    if b['status'] != EV_ACTIVE:
        return
    cf = {r['role']: dict(r) for r in db.q(
        "SELECT * FROM storm_confirms WHERE batch=? AND status='VALID'", (bid,))}
    if ROLE_FIELD not in cf or ROLE_DISPATCH not in cf:
        return
    if cf[ROLE_FIELD]['env_digest'] != cf[ROLE_DISPATCH]['env_digest']:
        # 依据不是同一环境版本：不齐，保留各自有效，等待按新版本重新确认
        return
    db.x('UPDATE storm_batches SET status=?, resumed_at=? WHERE id=?',
         (EV_WAITING_RESUME, _now(), bid))
    db.x('UPDATE storm_tickets SET frozen=0 WHERE batch=?', (bid,))
    db.x('UPDATE tickets SET frozen=0, freeze_reason=NULL WHERE id=?', (b['ticket'],))
    _ev(db, b['ticket'], actor, 'EVAC_GATE_OPENED', batch=bid,
        note='双角色确认同一环境版本，复工闸门开启，票据解冻')
    _decide(db, bid, 'RESUME_GATE', 'GATE_OPENED',
            f"现场与调度确认均针对环境 {cf[ROLE_FIELD]['env_digest']}，复工闸门开启",
            actor)


def resume_work_core(db, bid, actor='现场负责人', idem_key=None):
    """待复工批次正式恢复作业（闸门已开的最后一步；幂等）。"""
    key = idem_key or f'resume:{bid}:{actor}'
    with db.lock:
        old = db.one('SELECT response FROM idem WHERE key=?', (key,))
        if old:
            return {**json.loads(old['response']), 'duplicate': True}
        b = db.one('SELECT * FROM storm_batches WHERE id=?', (bid,))
        if not b:
            raise RuleError(404, 'EVAC_BATCH_NOT_FOUND', f'撤离批次 {bid} 不存在')
        if b['status'] == EV_RESUMED:
            return {'ok': True, 'duplicate': True, 'status': EV_RESUMED}
        if b['status'] != EV_WAITING_RESUME:
            raise RuleError(409, 'GATE_NOT_OPEN',
                            f'批次 {bid} 当前 {b["status"]}：人员未齐/器具去向不明/风险未监护或双确认未齐，禁止复工')
        db.x('UPDATE storm_batches SET status=? WHERE id=?', (EV_RESUMED, bid))
        db.x("UPDATE storm_tickets SET frozen=0, thawed_at=? WHERE batch=?", (_now(), bid))
        _ev(db, b['ticket'], actor, 'WORK_RESUMED', batch=bid,
            note='撤离结束，作业恢复')
        _decide(db, bid, 'RESUME', 'WORK_RESUMED', '复工完成，撤离批次关闭', actor)
        resp = {'ok': True, 'batch': bid, 'status': EV_RESUMED}
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(resp, ensure_ascii=False)))
        db.commit()
        return resp


# ---------------------------------------------------------------- 气象

def weather_alert_core(db, areas, level=WX_THUNDER, actor='气象台', alert_id=None,
                       idem_key=None, invalidate_confirms=True):
    """气象告警进入：仅登记/命中裁决，不自动发令（撤离令由值班长发布）。

    告警进入即构成环境依据变化（alert_seq 递增）：所有在途双确认按新版本失效。
    雷暴告警时由路由统一编排：先抢占待复工批次（保持 PREEMPTED），再失效开放批次确认。
    """
    key = idem_key or f'wx:{alert_id or uuid.uuid4().hex[:10]}'
    with db.lock:
        old = db.one('SELECT response FROM idem WHERE key=?', (key,))
        if old:
            return {**json.loads(old['response']), 'duplicate': True}
        aid = alert_id or ('WX' + time.strftime('%H%M%S') + uuid.uuid4().hex[:6].upper())
        seq = int(db.meta('storm_weather_seq')) + 1
        db.set_meta('storm_weather_seq', seq)
        if level == WX_THUNDER:
            aseq = int(db.meta('storm_alert_seq')) + 1
            db.set_meta('storm_alert_seq', aseq)
        db.x('INSERT INTO storm_alerts(alert_id,seq,level,areas_json,status,issued_by,issued_at,cleared_at) '
             'VALUES(?,?,?,?,?,?,?,NULL)',
             (aid, seq, level, json.dumps(sorted(areas), ensure_ascii=False), 'ACTIVE', actor, _now()))
        affected = []
        if level == WX_THUNDER:
            for t in db.q("SELECT id,title,work_area FROM tickets WHERE status IN ('ISOLATING','PERMITTED','RESTORING')"):
                wa = set(json.loads(t['work_area']))
                hit = sorted(wa & set(areas))
                if hit:
                    affected.append({'ticket': t['id'], 'title': t['title'], 'hit_areas': hit})
            # 先抢占待复工批次（抢占本身不依赖确认失效顺序）
            preempt_for_alerts(db, actor)
            if invalidate_confirms:
                _invalidate_open_confirms(db, 'WEATHER_ALERT',
                                          f'新雷暴告警 {aid} 命中 {sorted(areas)}，在途确认所依据环境已变化',
                                          actor)
        _ev(db, None, actor, 'WEATHER_ALERT', alert=aid, level=level, areas=sorted(areas),
            affected=affected, seq=seq)
        resp = {'ok': True, 'alert_id': aid, 'seq': seq, 'level': level,
                'areas': sorted(areas), 'affected_tickets': affected}
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(resp, ensure_ascii=False)))
        db.commit()
        return resp


def weather_clear_core(db, areas=None, actor='气象台', idem_key=None):
    """气象解除：weather_seq 递增（alert_seq 不回退）；在途确认按新版本失效，须重新核验。"""
    key = idem_key or f'wxc:{areas or "ALL"}:{_now_ms()}'
    with db.lock:
        old = db.one('SELECT response FROM idem WHERE key=?', (key,))
        if old:
            return {**json.loads(old['response']), 'duplicate': True}
        q = "SELECT * FROM storm_alerts WHERE status='ACTIVE' AND level=?"
        args = [WX_THUNDER]
        if areas:
            q += ' AND areas_json LIKE ?'
            args.append(f'%{areas[0]}%')
        rows = db.q(q, args)
        seq = int(db.meta('storm_weather_seq')) + 1
        db.set_meta('storm_weather_seq', seq)
        cleared = []
        for r in rows:
            if areas and not (set(_jload(r['areas_json'], [])) & set(areas)):
                continue
            db.x("UPDATE storm_alerts SET status='CLEARED', cleared_at=? WHERE alert_id=?",
                 (_now(), r['alert_id']))
            cleared.append(r['alert_id'])
        _invalidate_open_confirms(db, 'WEATHER_CLEAR',
                                  f'气象解除（{cleared or "无未解除告警"}），环境版本推进，旧确认失效须重核',
                                  actor)
        _ev(db, None, actor, 'WEATHER_CLEARED', alerts=cleared, seq=seq)
        resp = {'ok': True, 'cleared': cleared, 'weather_seq': seq}
        db.x('INSERT OR IGNORE INTO idem(key,response) VALUES(?,?)',
             (key, json.dumps(resp, ensure_ascii=False)))
        db.commit()
        return resp


def _invalidate_open_confirms(db, scene, detail, actor):
    """环境依据变化：开放批次的 VALID 确认置 SUPERSEDED（不删除）；待复工批次退回撤离中。

    刚被新告警抢占（PREEMPTED）的批次同样只失效确认、不再退回（抢占优先级最高）。
    已 RESUMED/TERMINATED 的批次不受影响（既成事实不回退）。
    """
    for r in db.q("SELECT rowid,* FROM storm_confirms WHERE status='VALID' "
                  "AND batch IN (SELECT id FROM storm_batches WHERE status IN (?,?,?))",
                  (EV_ACTIVE, EV_WAITING_RESUME, EV_PREEMPTED)):
        db.x("UPDATE storm_confirms SET status=?, superseded_at=? WHERE rowid=?",
             (CF_SUPERSEDED, _now(), r['rowid']))
        b = db.one('SELECT * FROM storm_batches WHERE id=?', (r['batch'],))
        _decide(db, r['batch'], scene, CF_SUPERSEDED, detail, actor, accepted=False,
                extra={'role': r['role']})
        _ev(db, b['ticket'], 'system', 'CONFIRM_INVALIDATED', batch=r['batch'],
            role=r['role'], reason=detail)
        if b['status'] == EV_WAITING_RESUME:
            db.x('UPDATE storm_batches SET status=?, resumed_at=NULL WHERE id=?',
                 (EV_ACTIVE, r['batch']))
            db.x('UPDATE storm_tickets SET frozen=1 WHERE batch=?', (r['batch'],))
            db.x('UPDATE tickets SET frozen=1, freeze_reason=? WHERE id=?',
                 (f'复工依据变化，批次 {r["batch"]} 退回撤离中', b['ticket']))
            _ev(db, b['ticket'], 'system', 'RESUME_GATE_REOPENED', batch=r['batch'],
                reason=detail)


def topology_changed(db, actor='system'):
    """拓扑变更钩子：环境版本推进，开放批次的确认失效，待复工退回撤离中。"""
    with db.lock:
        _invalidate_open_confirms(db, 'TOPOLOGY_CHANGED',
                                  f'网络拓扑变更至 v{db.meta("topo_version")}：隔离边界依据变化，旧确认失效',
                                  actor)
        db.commit()


# ---------------------------------------------------------------- 新告警抢占待复工批次

def preempt_for_alerts(db, actor='system'):
    """新告警到达后：命中作业区域的待复工批次被抢占（PREEMPTED），须按新批次重新撤离/复工。

    撤离中的批次继续保持 ACTIVE（同一轮雷暴，人员仍在避险中）；其确认同样已被
    _invalidate_open_confirms 失效。已复工/终止批次不回退。
    """
    with db.lock:
        for b in db.q(f"SELECT * FROM storm_batches WHERE status='{EV_WAITING_RESUME}'"):
            t = db.one('SELECT work_area FROM tickets WHERE id=?', (b['ticket'],))
            wa = set(json.loads(t['work_area']))
            hit = alerts_hit_area(db, wa)
            if hit:
                db.x('UPDATE storm_batches SET status=?, preempt_reason=? WHERE id=?',
                     (EV_PREEMPTED,
                      f"被新告警 {[a['alert_id'] for a in hit]} 抢占（命中 {sorted(set().union(*[set(a['hit_areas']) for a in hit]))}）",
                      b['id']))
                db.x('UPDATE storm_tickets SET frozen=1 WHERE batch=?', (b['id'],))
                db.x('UPDATE tickets SET frozen=1, freeze_reason=? WHERE id=?',
                     (f"待复工批次 {b['id']} 被新雷暴告警抢占，等待新一轮撤离令", b['ticket']))
                _ev(db, b['ticket'], actor, 'EVAC_BATCH_PREEMPTED', batch=b['id'],
                    alerts=[a['alert_id'] for a in hit])
                _decide(db, b['id'], 'PREEMPT', 'BATCH_PREEMPTED',
                        f"待复工批次被新告警 {[a['alert_id'] for a in hit]} 抢占，旧确认与放行全部作废（事实保留）",
                        actor, accepted=False)
        db.commit()


# ---------------------------------------------------------------- 票据终局钩子

def terminate_batch(db, tid, scene, detail, actor='system'):
    """作业收工/票据撤销：该票开放批次终止（不再复工），冻结解除由票据主流程决定。"""
    with db.lock:
        rows = db.q("SELECT * FROM storm_batches WHERE ticket=? AND status IN (?,?)",
                    (tid, EV_ACTIVE, EV_WAITING_RESUME))
        for b in rows:
            db.x('UPDATE storm_batches SET status=?, preempt_reason=? WHERE id=?',
                 (EV_TERMINATED, detail, b['id']))
            db.x('UPDATE storm_tickets SET frozen=0, thawed_at=? WHERE batch=?', (_now(), b['id']))
            _ev(db, tid, actor, 'EVAC_BATCH_TERMINATED', batch=b['id'], scene=scene, detail=detail)
            _decide(db, b['id'], scene, 'BATCH_TERMINATED',
                    f'票据{("收工关闭" if scene == "TICKET_CLOSED" else "撤销")}：'
                    '撤离批次终止，无需复工（事实保留）',
                    actor)
        # 票据终局（关闭/撤销）后冻结标志随主流程一并清零
        if rows and scene in ('TICKET_CLOSED', 'TICKET_WITHDRAWN'):
            db.x('UPDATE tickets SET frozen=0, freeze_reason=NULL WHERE id=?', (tid,))
        db.commit()


# ---------------------------------------------------------------- 视图

def batch_view(db, bid):
    b = db.one('SELECT * FROM storm_batches WHERE id=?', (bid,))
    if not b:
        raise RuleError(404, 'EVAC_BATCH_NOT_FOUND', f'撤离批次 {bid} 不存在')
    tid = b['ticket']
    t = db.one('SELECT title,status,frozen,freeze_reason,work_area FROM tickets WHERE id=?', (tid,))
    roster = []
    for r in db.q('SELECT * FROM storm_roster_snap WHERE batch=? ORDER BY person', (bid,)):
        d = dict(r)
        if d['status'] == PR_ON_DUTY:
            ci = db.one("SELECT shelter,checked_in_at FROM storm_checkins WHERE batch=? AND person=? "
                        "AND decision='CURRENT'", (bid, r['person']))
            d['shelter'] = ci['shelter'] if ci else None
            d['safe'] = ci is not None
        else:
            d['shelter'] = None
            d['safe'] = False
        roster.append(d)
    shelters = {}
    for r in db.q("SELECT shelter,COUNT(*) c FROM storm_checkins WHERE batch=? AND decision='CURRENT' "
                  "GROUP BY shelter", (bid,)):
        shelters[r['shelter']] = r['c']
    risks = [dict(r) for r in db.q('SELECT * FROM storm_risks WHERE batch=? ORDER BY line_no', (bid,))]
    tools_ = []
    for r in db.q('SELECT * FROM storm_tools WHERE batch=? ORDER BY line_no', (bid,)):
        d = dict(r)
        d['sightings'] = [dict(s) for s in db.q(
            'SELECT rowid,location_where,location_holder,actor,decision,created_at FROM storm_tool_sightings '
            'WHERE batch=? AND serial=? ORDER BY rowid', (bid, r['serial']))]
        tools_.append(d)
    missing, unguarded, unknown_tools = open_blockers(db, bid)
    confirms = {}
    for r in db.q('SELECT * FROM storm_confirms WHERE batch=? ORDER BY rowid', (bid,)):
        confirms.setdefault(r['role'], []).append(dict(r))
    env = {'topology_version': b['env_topology'], 'weather_seq': b['env_weather_seq'],
           'alert_seq': b['env_alert_seq'], 'digest': b['env_digest']}
    cur_env = env_version(db)
    cur_env['digest'] = env_digest(cur_env)
    return {
        'id': bid, 'ticket': tid, 'seq': b['seq'], 'status': b['status'],
        'alert_id': b['alert_id'], 'reason': b['reason'], 'leader': b['leader'],
        'created_at': b['created_at'], 'resumed_at': b['resumed_at'],
        'preempt_reason': b['preempt_reason'],
        'env_at_order': env, 'env_current': cur_env,
        'env_stale': env['digest'] != cur_env['digest'],
        'ticket': {'id': tid, 'title': t['title'], 'status': t['status'],
                   'frozen': bool(t['frozen']), 'freeze_reason': t['freeze_reason'],
                   'work_area': json.loads(t['work_area'])},
        'roster': roster,
        'shelter_distribution': shelters,
        'risks': risks,
        'tools': tools_,
        'blockers': {'persons_missing': missing, 'risks_unguarded': unguarded,
                     'tools_unknown': unknown_tools},
        'confirms': confirms,
        'valid_confirms': {role: rows[-1] for role, rows in confirms.items()
                           if rows[-1]['status'] == CF_VALID},
        'gate': gate_status(db, bid),
    }


def gate_status(db, bid):
    """复工闸门：人员/风险点/器具清零 + 双确认同批次同环境版本 + 该版本仍是当前环境。"""
    b = db.one('SELECT * FROM storm_batches WHERE id=?', (bid,))
    missing, unguarded, unknown_tools = open_blockers(db, bid)
    cf = {r['role']: dict(r) for r in db.q(
        "SELECT * FROM storm_confirms WHERE batch=? AND status='VALID'", (bid,))}
    env = {'topology_version': b['env_topology'], 'weather_seq': b['env_weather_seq'],
           'alert_seq': b['env_alert_seq'], 'digest': b['env_digest']}
    cur = env_version(db)
    cur['digest'] = env_digest(cur)
    field_ok = ROLE_FIELD in cf and cf[ROLE_FIELD]['env_digest'] == cur['digest']
    dispatch_ok = ROLE_DISPATCH in cf and cf[ROLE_DISPATCH]['env_digest'] == cur['digest']
    same_env = (ROLE_FIELD in cf and ROLE_DISPATCH in cf and
                cf[ROLE_FIELD]['env_digest'] == cf[ROLE_DISPATCH]['env_digest'])
    clear = {
        'persons_all_safe': not missing,
        'risks_all_guarded': not unguarded,
        'tools_all_known': not unknown_tools,
        'field_confirmed': ROLE_FIELD in cf,
        'dispatch_confirmed': ROLE_DISPATCH in cf,
        'field_confirms_current_env': field_ok,
        'dispatch_confirms_current_env': dispatch_ok,
        'same_env': same_env,
        'env_stale': env['digest'] != cur['digest'],
        'can_confirm_field': b['status'] in EV_OPEN_STATES and not missing and not unknown_tools
                             and not unguarded,
        'can_resume': (b['status'] == EV_WAITING_RESUME
                       and field_ok and dispatch_ok and same_env
                       and not missing and not unguarded and not unknown_tools),
        'reasons': _gate_reasons(b, missing, unguarded, unknown_tools, cf, env, cur, same_env),
    }
    return clear


def _gate_reasons(b, missing, unguarded, unknown_tools, cf, env, cur, same_env):
    reasons = []
    if missing:
        reasons.append({'code': R_PERSON_MISSING, 'detail': f'人员未确认安全：{missing}'})
    if unguarded:
        reasons.append({'code': R_RISK_UNGUARDED,
                        'detail': f'风险点尚未落实监护：{[u["device"] for u in unguarded]}'})
    if unknown_tools:
        reasons.append({'code': R_TOOL_MISSING,
                        'detail': f'关键器具去向不明：{[t["serial"] for t in unknown_tools]}'})
    if env['digest'] != cur['digest']:
        reasons.append({'code': R_BASIS_MISMATCH,
                        'detail': f"撤离批次依据环境 {env['digest']} 已过期，当前 {cur['digest']}，旧确认失效"})
    for role, label, code in ((ROLE_FIELD, '现场负责人', 'FIELD_NOT_CONFIRMED'),
                              (ROLE_DISPATCH, '调度员', 'DISPATCH_NOT_CONFIRMED')):
        if role not in cf:
            reasons.append({'code': code, 'detail': f'{label}尚未确认'})
    if ROLE_FIELD in cf and ROLE_DISPATCH in cf and not same_env:
        reasons.append({'code': R_BASIS_MISMATCH,
                        'detail': f"两项确认依据版本不一致：现场 {cf[ROLE_FIELD]['env_digest']} / "
                                  f"调度 {cf[ROLE_DISPATCH]['env_digest']}"})
    return reasons


def board_view(db, area=None):
    """调度端区域态势：告警、票据、批次、人员分布、未清风险与复工依据总览。"""
    alerts = [dict(r) for r in db.q('SELECT * FROM storm_alerts ORDER BY seq DESC')]
    for a in alerts:
        a['areas'] = _jload(a['areas_json'], [])
    tickets = []
    q = ("SELECT t.* FROM tickets t WHERE t.status IN ('ISOLATING','PERMITTED','RESTORING') "
         "ORDER BY t.created_at DESC")
    for r in db.q(q):
        d = dict(r)
        d['work_area'] = json.loads(d['work_area'])
        d['hit_alerts'] = [{'alert_id': a['alert_id'], 'hit_areas': a['hit_areas']}
                           for a in alerts_hit_area(db, d['work_area'])]
        batches = [batch_view(db, x['id']) for x in db.q(
            'SELECT id FROM storm_batches WHERE ticket=? ORDER BY seq', (r['id'],))]
        d['batches'] = batches
        tickets.append(d)
    return {'alerts': alerts, 'tickets': tickets,
            'shelters': list_shelters(db),
            'env_current': {**env_version(db), 'digest': env_digest(env_version(db))},
            'decisions': [dict(x) for x in db.q(
                'SELECT rowid,batch,scene,reason_code,detail,actor,accepted,created_at '
                'FROM storm_decisions ORDER BY rowid DESC LIMIT 200')]}


def person_view(db, batch, person):
    """移动端：本人批次状态、避险点、签到史、未申报工具、拒绝原因。"""
    bv = batch_view(db, batch)
    me = next((p for p in bv['roster'] if p['person'] == person), None)
    checkins = [dict(r) for r in db.q(
        'SELECT shelter,decision,actor,client_id,client_seq,checked_in_at,superseded_at '
        'FROM storm_checkins WHERE batch=? AND person=? ORDER BY rowid', (batch, person))]
    sightings = [dict(r) for r in db.q(
        'SELECT serial,location_where,location_holder,decision,created_at FROM storm_tool_sightings '
        'WHERE batch=? AND actor=? ORDER BY rowid', (batch, person))]
    mine = []
    for t in bv['tools']:
        if t['location_holder'] == person or (
                not t['location_known'] and t['possession'] in (POS_ISSUED, POS_HELD_BOX)):
            mine.append(t)
    decisions = [dict(r) for r in db.q(
        'SELECT scene,reason_code,detail,accepted,created_at FROM storm_decisions '
        'WHERE batch=? AND (actor=? OR ref=?) ORDER BY rowid DESC LIMIT 50',
        (batch, person, person))]
    return {'batch': bv, 'me': me, 'checkins': checkins, 'my_tool_duties': mine,
            'sightings': sightings, 'decisions': decisions,
            'shelters': list_shelters(db)}
