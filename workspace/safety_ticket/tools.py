"""现场班组工器具借还台 — 核心业务模块（不直接依赖 Web 框架）。

与主系统共用一个 SQLite：
- 调度员从安全票隔离步骤推导必需工具清单（derive_requirements）
- 库管员按批次锁定工具（lock_core），工具被另一张安全票占用时整批锁定原子失败并回滚
- 计量器具（VDT 验电器 / WRENCH 力矩扳手）锁定时按计划作业时段+电压等级校验检定证书资格，
  证书将在作业结束前到期、被暂停、已换版、电压不适用或正被召回的件不得纳入准备清单
- 现场人员扫码逐件领用（scan_flush_core），服务端为每件工具生成可复核校验串（checksum）；
  锁后证书被暂停/换版/召回的，现场闸门按持久化版本拒绝（可核对原因）
- 齐套 + 封签完好 + 未超交接期限才能移交（handover_core），两人争抢以数据库 CAS 保证唯一持权
- 断网扫码进本地队列，恢复后按客户端位置（client_id, client_seq）补送；
  缺前序先挂起，前序补到后链式唤醒；重复补送返回原结果，绝不多扣库存
- 归还逐项对照领用清单（return_core），缺件/磨损/封签变化置工具箱为待复核并禁止再借出
全部状态事务化持久化，进程重启后可继续处理。
"""
import hashlib
import hmac
import json
import sqlite3
import time
import uuid

import metro

# ---------------------------------------------------------------- 常量

# 单件工具状态
ST_FREE = 'FREE'                  # 在库可用
ST_LOCKED = 'LOCKED'              # 已被某张安全票按批次锁定（在库、已预留）
ST_ISSUED = 'ISSUED'              # 已扫码领用出柜
ST_BLOCKED = 'BLOCKED'            # 归还差异/损坏，待复核，禁止再次借出
ST_MAINT = 'MAINTENANCE'          # 维修中，不参与锁定
ST_QUARANTINED = 'QUARANTINED'    # 计量召回隔离（由 metro 模块处置，借还台不得回库/再借）

# 批次锁定行状态
LK_LOCKED = 'LOCKED'              # 生效锁定
LK_RELEASED = 'RELEASED'          # 已释放（库存回滚，审计保留）

# 工具箱状态
BX_READY = 'READY'                # 已装箱待领用/待移交
BX_HELD = 'HELD'                  # 已移交现场负责人（持有权唯一）
BX_RETURNED = 'RETURNED'          # 归还清点无差异，工具回库
BX_REVIEW = 'REVIEW'              # 归还存在差异，待复核；禁止再次借出
BX_BLOCKED = 'BLOCKED'            # 移交前封签破损等，箱被扣留
BX_CANCELLED = 'CANCELLED'        # 库管员作废未齐套箱：领用件回库、库存锁回滚
BX_TERMINAL = (BX_RETURNED, BX_REVIEW, BX_CANCELLED)

# 扫码补送事件状态
EV_PENDING = 'PENDING'            # 已上传但前序缺口，挂起
EV_ACCEPTED = 'ACCEPTED'          # 已采纳（成功领用）
EV_REJECTED = 'REJECTED'          # 已裁决拒绝（带具体原因）
EV_DONE = (EV_ACCEPTED, EV_REJECTED)

# 固定原因码（界面与测试据此展示/断言）
R_DUPLICATE = 'DUPLICATE_SCAN'              # 重复扫码（同箱已领）→ 回原结果
R_ITEM_OTHER_TICKET = 'ITEM_OCCUPIED_BY_TICKET'   # 工具已被另一张安全票占用
R_NOT_LOCKED = 'NOT_LOCKED_FOR_TICKET'      # 工具未按本票批次锁定
R_BATCH_MISMATCH = 'BATCH_MISMATCH'         # 批次不符
R_BOX_NOT_READY = 'BOX_NOT_READY'
R_BOX_DEADLINE = 'HANDOVER_DEADLINE_PASSED'
R_SEAL_BROKEN = 'SEAL_BROKEN'
R_HOLDER_CONFLICT = 'HOLDER_TAKEN'
R_REVIEW_BLOCKED = 'BOX_UNDER_REVIEW'
R_MAINTENANCE = 'ITEM_IN_MAINTENANCE'
R_GAP = 'WAITING_PRIOR_SEQ'                 # 缺前置动作，挂起

# 隔离/恢复步骤 → 必需工器具（每步每种一件，按步骤数汇总）
STEP_TOOLS = {
    'OPEN_SWITCH':    ('GLOVE', 'VDT', 'ROD'),     # 断开关：绝缘手套、验电器、绝缘操作杆
    'CLOSE_SWITCH':   ('GLOVE', 'VDT', 'ROD'),     # 合开关：同上
    'INSTALL_GROUND': ('GLOVE', 'GROUNDROD'),      # 装接地线：绝缘手套、接地棒
    'REMOVE_GROUND':  ('GLOVE', 'ROD'),            # 拆接地线：绝缘手套、绝缘操作杆
}

TOOL_NAMES = {
    'GLOVE': '绝缘手套', 'VDT': '高压验电器', 'ROD': '绝缘操作杆',
    'GROUNDROD': '接地棒', 'WRENCH': '力矩扳手', 'HELMET': '安全帽',
}


class RuleError(Exception):
    def __init__(self, http_status, reason_code, detail):
        super().__init__(detail)
        self.http_status = http_status
        self.reason_code = reason_code
        self.detail = detail


# ---------------------------------------------------------------- 小工具

def _rows(db, sql, args=()):
    return [dict(r) for r in db.q(sql, args)]


def get_secret(db):
    """校验串 HMAC 密钥：首次使用生成并持久化，重启后复核仍一致。"""
    row = db.one("SELECT v FROM meta WHERE k='tool_secret'")
    if row:
        return row['v']
    s = uuid.uuid4().hex
    db.x("INSERT INTO meta(k,v) VALUES('tool_secret',?)", (s,))
    db.commit()
    return s


def checksum_of(secret, box, ticket, code, batch, serial):
    """服务端为每件已领工具生成的可复核校验串：tlc.<hmac-sha256 前 20 位>。"""
    msg = '|'.join([box, ticket, code, batch, serial])
    mac = hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()[:20]
    return 'tlc.' + mac


def now_ms():
    return int(time.time() * 1000)


def reason(db, box, ticket, scene, code, detail, actor, ref_serial=None):
    """写一条可查处理原因（ref_serial 记录涉及的单件工具，便于复核处置查询）。"""
    db.x('INSERT INTO tool_reasons(box,ticket,scene,reason_code,detail,ref_serial,actor,created_at) '
         'VALUES(?,?,?,?,?,?,?,?)',
         (box, ticket, scene, code, detail, ref_serial, actor, now_ms()))


# ---------------------------------------------------------------- 需求推导

def derive_requirements(db, tid):
    """从安全票的隔离/恢复步骤推导必需工具清单（按步骤数汇总数量）。"""
    t = db.one('SELECT id, title, status FROM tickets WHERE id=?', (tid,))
    if not t:
        raise RuleError(404, 'TICKET_NOT_FOUND', f'安全票 {tid} 不存在')
    steps = _rows(db, 'SELECT kind FROM steps WHERE ticket=? ORDER BY seq', (tid,))
    qty = {}
    step_tools = []
    for s in steps:
        codes = STEP_TOOLS.get(s['kind'], ())
        step_tools.append({'kind': s['kind'], 'tools': list(codes)})
        for c in codes:
            qty[c] = qty.get(c, 0) + 1
    lines = []
    for code in sorted(qty):
        free = db.one(
            "SELECT COUNT(*) n FROM tools WHERE code=? AND state=?", (code, ST_FREE))['n']
        lines.append({'code': code, 'name': TOOL_NAMES.get(code, code),
                      'qty': qty[code], 'free': free, 'short': max(0, qty[code] - free)})
    return {'ticket_id': tid, 'title': t['title'], 'ticket_status': t['status'],
            'step_count': len(steps), 'per_step': step_tools, 'lines': lines,
            'derived_at': now_ms()}


# ---------------------------------------------------------------- 批次锁定

def active_lock_rows(db, tid):
    return _rows(db,
                 "SELECT * FROM tool_locks WHERE ticket=? AND state=? ORDER BY line_no",
                 (tid, LK_LOCKED))


def effective_lock_rows(db, tid):
    """仍具资格的生效锁行（召回撤销资格的行保留可查但不计入齐套）。"""
    return [r for r in active_lock_rows(db, tid) if (r['metro_state'] or metro.MS_OK) == metro.MS_OK]


def _ticket_job(db, tid):
    r = db.one('SELECT plan_start_ms, plan_end_ms, voltage_kv FROM tickets WHERE id=?', (tid,))
    if not r:
        raise RuleError(404, 'TICKET_NOT_FOUND', f'安全票 {tid} 不存在')
    return r['plan_start_ms'] or now_ms(), r['plan_end_ms'] or now_ms(), r['voltage_kv']


def _metro_qualify(db, code, serial, tid):
    """计量器具资格校验（按计划作业时段+电压等级）；不合格抛 tools.RuleError。"""
    job_start, job_end, voltage = _ticket_job(db, tid)
    ok, cert, rcode, detail = metro.evaluate_item(db, code, serial, job_end, voltage, job_start)
    if not ok:
        raise RuleError(409, rcode, detail)
    return cert


def lock_core(db, tid, serials=None, actor='库管员', partial=False):
    """按批次为安全票锁定工具：数量取自步骤推导清单；可指定具体编号。

    整批原子：任一工具无空闲（FREE）、被另一张票占用或计量资格不合格，则全部不锁并回滚，
    返回占用/不合格详情；已被他票锁定的件给出具体占用票据。
    计量器具（VDT/WRENCH）须在计划作业时段内持有适用电压等级的有效证书，
    锁定行固化所依据的证书版本（cert_id/cert_ver），并把该证书标记为已用于现场任务（此后只许换版）。
    partial=True 时只锁定显式指定的编号（用于按器具维度的资格/召回演示，不自动凑齐套数）。
    """
    req = derive_requirements(db, tid)
    job_start, job_end, voltage = _ticket_job(db, tid)
    if active_lock_rows(db, tid):
        raise RuleError(409, 'LOCK_EXISTS', f'安全票 {tid} 已有批次锁定，请先释放库存锁再重新锁定')
    box = db.one('SELECT id FROM toolboxes WHERE ticket=? AND status NOT IN (?,?,?)',
                 (tid, BX_RETURNED, BX_REVIEW, BX_CANCELLED))
    if box:
        raise RuleError(409, 'BOX_EXISTS', f'安全票 {tid} 已生成工具箱 {box["id"]}，不能再改锁')

    serials = serials or {}
    lid = 'L' + uuid.uuid4().hex[:10].upper()
    used_certs = {}
    try:
        idx = 0
        for line in req['lines']:
            code, need = line['code'], line['qty']
            wanted = serials.get(code, [])
            if partial and not wanted:
                continue           # 部分锁定：未显式指定编号的型号一律不锁
            if len(wanted) > need:
                raise RuleError(422, 'SERIAL_OVER_SPEC',
                                f'{TOOL_NAMES.get(code, code)} 指定 {len(wanted)} 件，超过需求 {need} 件')
            chosen = []
            cert_of = {}
            for sn in wanted:
                it = db.one('SELECT * FROM tools WHERE code=? AND serial=?', (code, sn))
                if not it:
                    raise RuleError(404, 'ITEM_NOT_FOUND', f'工具编号 {code}/{sn} 不存在')
                if it['state'] == ST_QUARANTINED:
                    raise RuleError(409, metro.R_ITEM_QUARANTINED,
                                    f'{code}/{sn} 已被召回隔离，不得纳入准备清单')
                if it['state'] == ST_MAINT:
                    raise RuleError(409, R_MAINTENANCE, f'{code}/{sn} 维修中，不能锁定')
                if it['state'] != ST_FREE:
                    occ = db.one(
                        "SELECT ticket FROM tool_locks WHERE item_serial=? AND code=? AND state=?",
                        (sn, code, LK_LOCKED))
                    who = occ['ticket'] if occ else '另一张安全票'
                    raise RuleError(409, R_ITEM_OTHER_TICKET,
                                    f'工具 {code}/{sn} 已被安全票 {who} 占用')
                cert = _metro_qualify(db, code, sn, tid)
                chosen.append(sn)
                if cert:
                    cert_of[sn] = cert
            if len(chosen) < need and not partial:
                remain = need - len(chosen)
                if metro.is_metro(code):
                    # 计量器具自动选件：只选资格合格的库内件；不合格件全部给出可核对拒绝原因
                    eligible, rejects = metro.pick_candidates(
                        db, code, remain, job_end, voltage, job_start, exclude=set(chosen))
                    if len(eligible) < remain:
                        detail = (f'{TOOL_NAMES.get(code, code)} 合格库存不足：需要 {need} 件，'
                                  f'在库合格仅 {len(chosen) + len(eligible)} 件')
                        if rejects:
                            detail += '；在库但不合格：' + '；'.join(
                                f"{r['serial']}({r['reason_code']})" for r in rejects[:8])
                        raise RuleError(409, 'INSUFFICIENT_METRO_STOCK', detail)
                    auto = eligible[:remain]
                    for sn in auto:
                        cert_of[sn] = metro.latest_cert(db, sn)
                    chosen.extend(auto)
                else:
                    # 自动选批：优先在同一个批次内凑齐（避免无意跨批混领），不足再跨批补齐
                    prefer = db.one('SELECT batch_id FROM tools WHERE code=? AND serial=?',
                                    (code, chosen[0]))['batch_id'] if chosen else None
                    ordered_batches = ([prefer] if prefer else []) + \
                        [r['batch_id'] for r in db.q(
                            'SELECT DISTINCT batch_id FROM tools WHERE code=? ORDER BY batch_id', (code,))
                         if not prefer or r['batch_id'] != prefer]
                    avail = []
                    for bid in ordered_batches:
                        rows = db.q(
                            "SELECT serial FROM tools WHERE code=? AND state=? AND batch_id=? ORDER BY serial",
                            (code, ST_FREE, bid))
                        avail.extend(r['serial'] for r in rows)
                        if len(avail) >= remain:
                            break
                    if len(avail) < remain:
                        raise RuleError(409, 'INSUFFICIENT_STOCK',
                                        f'{TOOL_NAMES.get(code, code)} 库存不足：需要 {need} 件，'
                                        f'在库空闲仅 {len(chosen) + len(avail)} 件')
                    chosen.extend(avail[:remain])
            for sn in chosen:
                idx += 1
                batch = db.one('SELECT batch_id FROM tools WHERE code=? AND serial=?',
                               (code, sn))['batch_id']
                cert = cert_of.get(sn)
                db.x("INSERT INTO tool_locks(id,line_no,ticket,code,batch_id,item_serial,state,actor,"
                     "locked_at,cert_id,cert_ver,metro_state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                     (lid, idx, tid, code, batch, sn, LK_LOCKED, actor, now_ms(),
                      cert['cert_id'] if cert else None, cert['ver'] if cert else None, metro.MS_OK))
                if cert:
                    used_certs[sn] = (cert['cert_id'], cert['ver'])
                db.x("UPDATE tools SET state=? WHERE code=? AND serial=?",
                     (ST_LOCKED, code, sn))
    except Exception:
        db.rollback()
        raise
    # 证书一经用于现场任务（纳入准备清单）即固化，不得覆盖修改
    for sn, (cid, ver) in used_certs.items():
        db.x('UPDATE metro_certs SET used=1, used_at=COALESCE(used_at,?) WHERE cert_id=? AND serial=?',
             (now_ms(), cid, sn))
    db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
         (time.time(), tid, actor, 'TOOLS_LOCKED',
          json.dumps({'lock_id': lid, 'lines': req['lines'],
                      'metro_certs': used_certs, 'voltage_kv': voltage,
                      'plan_end_ms': job_end}, ensure_ascii=False)))
    db.commit()
    return {'lock_id': lid, 'ticket': tid, 'lines': lock_view(db, tid)}


def release_ticket_locks(db, tid, actor='库管员', reason='调度撤回安全票，库存锁回滚'):
    """库存回滚：释放一张安全票的全部批次锁定，工具退回在库（仅当尚未生成工具箱）。"""
    rows = active_lock_rows(db, tid)
    if not rows:
        return {'ticket': tid, 'released': 0, 'lines': []}
    if db.one('SELECT id FROM toolboxes WHERE ticket=? AND status NOT IN (?,?,?)',
              (tid, BX_RETURNED, BX_REVIEW, BX_CANCELLED)):
        raise RuleError(409, 'BOX_IN_USE', '工具箱已生成并在流转，不能整票释放锁定，请走归还/归还/作废流程')
    out = []
    for r in rows:
        db.x("UPDATE tool_locks SET state=? WHERE id=?", (LK_RELEASED, r['id']))
        it = db.one('SELECT state FROM tools WHERE code=? AND serial=?',
                    (r['code'], r['item_serial']))
        # 召回隔离件保持隔离，不随票据撤回回库
        if it['state'] != ST_QUARANTINED:
            db.x("UPDATE tools SET state=? WHERE code=? AND serial=?",
                 (ST_FREE, r['code'], r['item_serial']))
        out.append({'code': r['code'], 'serial': r['item_serial'], 'batch': r['batch_id']})
    db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
         (time.time(), tid, actor, 'TOOLS_LOCK_ROLLBACK',
          json.dumps({'reason': reason, 'items': out}, ensure_ascii=False)))
    db.commit()
    return {'ticket': tid, 'released': len(out), 'lines': out, 'reason': reason}


def lock_view(db, tid):
    out = []
    for r in active_lock_rows(db, tid):
        it = db.one('SELECT state, wear, seal_ok, metro_frozen, freeze_reason FROM tools '
                    'WHERE code=? AND serial=?', (r['code'], r['item_serial']))
        cert = None
        if r['cert_id']:
            cert = {'cert_id': r['cert_id'], 'ver': r['cert_ver']}
        out.append({'line_no': r['line_no'], 'code': r['code'], 'name': TOOL_NAMES.get(r['code'], r['code']),
                    'batch': r['batch_id'], 'serial': r['item_serial'],
                    'metro': metro.is_metro(r['code']), 'cert': cert,
                    'metro_state': r['metro_state'] or metro.MS_OK,
                    'metro_reason': r['metro_reason'],
                    'item_state': it['state'] if it else None,
                    'wear': it['wear'] if it else None,
                    'seal_ok': bool(it['seal_ok']) if it else None,
                    'metro_frozen': bool(it['metro_frozen']) if it else False})
    return out


# ---------------------------------------------------------------- 工具箱

def get_box(db, box_id):
    b = db.one('SELECT * FROM toolboxes WHERE id=?', (box_id,))
    if not b:
        raise RuleError(404, 'BOX_NOT_FOUND', f'工具箱 {box_id} 不存在')
    return dict(b)


def box_view(db, box_id):
    b = get_box(db, box_id)
    lines, issued = [], 0
    all_locks = active_lock_rows(db, b['ticket'])
    locks = [l for l in all_locks if (l['metro_state'] or metro.MS_OK) == metro.MS_OK]
    disq = [l for l in all_locks if (l['metro_state'] or metro.MS_OK) == metro.MS_DISQUALIFIED]
    for lk in locks:
        co = db.one("SELECT * FROM checkouts WHERE box=? AND serial=? AND code=?",
                    (box_id, lk['item_serial'], lk['code']))
        it = db.one('SELECT wear, seal_ok FROM tools WHERE code=? AND serial=?',
                    (lk['code'], lk['item_serial']))
        line = {'line_no': lk['line_no'], 'code': lk['code'],
                'name': TOOL_NAMES.get(lk['code'], lk['code']),
                'batch': lk['batch_id'], 'serial': lk['item_serial'],
                'checked_out': co is not None,
                'wear_now': it['wear'] if it else None,
                'seal_ok_now': bool(it['seal_ok']) if it else None}
        if co:
            issued += 1
            line.update({'seq': co['seq'], 'checksum': co['checksum'],
                         'checked_out_at': co['checked_out_at'],
                         'seal_ok_at_issue': bool(co['seal_ok_at_issue']),
                         'cert_used': {'cert_id': co['cert_id'], 'ver': co['cert_ver']}
                                       if co['cert_id'] else None})
        lines.append(line)
    b['total'] = len(locks)
    b['issued'] = issued
    b['complete'] = issued == len(locks) and len(locks) > 0
    b['lines'] = lines
    b['disqualified'] = [{'line_no': l['line_no'], 'code': l['code'], 'serial': l['item_serial'],
                          'reason': l['metro_reason']} for l in disq]
    # 已结案（归还/作废）后批次锁已释放，lines 会为空；历史领用与校验串仍可查
    b['checkouts'] = [
        {'seq': r['seq'], 'code': r['code'], 'batch': r['batch'], 'serial': r['serial'],
         'checksum': r['checksum'], 'checked_out_at': r['checked_out_at'],
         'wear_at_issue': r['wear_at_issue'],
         'seal_ok_at_issue': bool(r['seal_ok_at_issue']),
         'cert_id': r['cert_id'], 'cert_ver': r['cert_ver']}
        for r in db.q('SELECT * FROM checkouts WHERE box=? ORDER BY seq', (box_id,))]
    b['deadline_passed'] = b['deadline_ms'] is not None and now_ms() > b['deadline_ms']
    return b


def create_box_core(db, tid, box_id=None, seal='S-OK', deadline_hours=8, actor='库管员'):
    """齐套清单来自该票的批次锁定行；工具箱生成后处于 READY，等待逐件扫码。"""
    if not db.one('SELECT id FROM tickets WHERE id=?', (tid,)):
        raise RuleError(404, 'TICKET_NOT_FOUND', f'安全票 {tid} 不存在')
    locks = active_lock_rows(db, tid)
    effective = effective_lock_rows(db, tid)
    if not effective:
        raise RuleError(409, 'NO_LOCK', '安全票尚无具备资格的批次锁定工具，无法生成工具箱')
    if len(effective) != len(locks):
        revoked = [l['item_serial'] for l in locks
                   if (l['metro_state'] or metro.MS_OK) == metro.MS_DISQUALIFIED]
        raise RuleError(409, 'LOCK_REVOKED_BY_RECALL',
                        f'准备清单中 {revoked} 的预留资格已被召回撤销，请先补锁合规格器具再装箱')
    locks = effective
    existing = db.one('SELECT id FROM toolboxes WHERE ticket=? AND status NOT IN (?,?,?)',
                      (tid, BX_RETURNED, BX_REVIEW, BX_CANCELLED))
    if existing:
        raise RuleError(409, 'BOX_EXISTS', f'安全票 {tid} 已存在工具箱 {existing["id"]}')
    bid = box_id or ('BX' + uuid.uuid4().hex[:8].upper())
    if db.one('SELECT id FROM toolboxes WHERE id=?', (bid,)):
        raise RuleError(409, 'BOX_ID_DUP', f'工具箱编号 {bid} 已存在')
    deadline = now_ms() + int(deadline_hours * 3600_000) if deadline_hours is not None else None
    db.x("INSERT INTO toolboxes(id,ticket,status,seal,seal_broken,holder,holder_team,deadline_ms,"
         "ready_at,handover_at,returned_at,review_reason) VALUES(?,?,?,?,0,NULL,NULL,?,?,NULL,NULL,NULL)",
         (bid, tid, BX_READY, seal, deadline, now_ms()))
    db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
         (time.time(), tid, actor, 'TOOLBOX_READY',
          json.dumps({'box': bid, 'items': len(locks), 'seal': seal,
                      'deadline_ms': deadline}, ensure_ascii=False)))
    db.commit()
    return box_view(db, bid)


# ---------------------------------------------------------------- 扫码领用（含断网补送）

def _rejected(reason_code, detail, **extra):
    r = {'ok': False, 'reason_code': reason_code, 'reason': detail}
    r.update(extra)
    return r


def scan_core(db, secret, client_id, client_seq, box_id, serial, scanned_at=None):
    """单件扫码裁决（在已加库锁的事务内运行）。成功生成校验串；失败返回具体原因，不抛异常。"""
    scanned_at = scanned_at or now_ms()
    b = db.one('SELECT * FROM toolboxes WHERE id=?', (box_id,))
    if not b:
        return _rejected('BOX_NOT_FOUND', f'工具箱 {box_id} 不存在')
    if b['status'] in (BX_REVIEW,):
        return _rejected(R_REVIEW_BLOCKED, f'工具箱 {box_id} 处于待复核，禁止领用', box=box_id)
    if b['status'] in (BX_RETURNED,):
        return _rejected(R_BOX_NOT_READY, f'工具箱 {box_id} 已归还结案', box=box_id)
    if b['status'] == BX_HELD:
        return _rejected(R_BOX_NOT_READY,
                         f'工具箱 {box_id} 已移交 {b["holder_team"] or ""}/{b["holder"] or ""}，不能再扫码',
                         box=box_id)

    it = db.one('SELECT * FROM tools WHERE serial=?', (serial,))
    if not it:
        return _rejected('ITEM_NOT_FOUND', f'工具编号 {serial} 不存在', box=box_id, serial=serial)
    code = it['code']

    # 1) 本箱已领过同一件 → 重复扫码，返回原结果
    co = db.one("SELECT * FROM checkouts WHERE box=? AND serial=?", (box_id, serial))
    if co:
        return {'ok': True, 'duplicate': True, 'reason_code': R_DUPLICATE,
                'reason': f'{serial} 已在本箱领用，重复扫码返回原结果，未重复扣库存',
                'box': box_id, 'ticket': b['ticket'], 'serial': serial, 'code': code,
                'batch': co['batch'], 'checksum': co['checksum'], 'seq': co['seq']}

    # 2) 该件已被另一张安全票领用（仅统计在途箱；已归还/已作废箱的历史领用不构成占用）
    other = db.one(
        "SELECT c.box, c.ticket FROM checkouts c JOIN toolboxes b ON b.id=c.box "
        "WHERE c.serial=? AND c.box!=? AND b.status NOT IN (?,?,?)",
        (serial, box_id, BX_RETURNED, BX_REVIEW, BX_CANCELLED))
    other_box = other['box'] if other else None
    other_ticket = other['ticket'] if other else None
    if not other_ticket:
        lk = db.one("SELECT ticket FROM tool_locks WHERE item_serial=? AND state=? AND ticket!=?",
                    (serial, LK_LOCKED, b['ticket']))
        if lk:
            other_ticket = lk['ticket']
    if other_ticket:
        return _rejected(R_ITEM_OTHER_TICKET,
                         f'工具 {serial} 已被另一张安全票 {other_ticket} 占用'
                         + (f'（其工具箱 {other_box}）' if other_box else '（批次锁定中）'),
                         box=box_id, ticket=b['ticket'], serial=serial, code=code,
                         occupied_by=other_ticket)

    if it['state'] == ST_MAINT:
        return _rejected(R_MAINTENANCE, f'工具 {serial} 维修中，禁止领用',
                         box=box_id, serial=serial, code=code)

    # 3) 本票是否按批次锁定该型号；以及锁定批次/编号与扫码件是否一致
    lk = db.one("SELECT * FROM tool_locks WHERE ticket=? AND code=? AND state=?",
                (b['ticket'], code, LK_LOCKED))
    if not lk:
        return _rejected(R_NOT_LOCKED,
                         f'{code} 不在安全票 {b["ticket"]} 的锁定清单内（未按本票批次锁定）',
                         box=box_id, ticket=b['ticket'], serial=serial, code=code)
    exact = db.one("SELECT * FROM tool_locks WHERE ticket=? AND code=? AND item_serial=? AND state=?",
                   (b['ticket'], code, serial, LK_LOCKED))
    if not exact:
        return _rejected(R_BATCH_MISMATCH,
                         f'批次/编号不符：{TOOL_NAMES.get(code, code)} 本票锁定批次 {lk["batch_id"]} '
                         f'编号 {lk["item_serial"]}，扫码件为批次 {it["batch_id"]} 编号 {serial}',
                         box=box_id, ticket=b['ticket'], code=code,
                         expected_batch=lk['batch_id'], expected_serial=lk['item_serial'],
                         scanned_batch=it['batch_id'], scanned_serial=serial)

    # 3.5) 现场闸门：锁后证书被暂停/换版/到期，或器具被召回（隔离/冻结/撤资格）→ 拒绝出场。
    # 以持久化版本决定唯一结果，返回可核对原因；锁行固化的证书版本写入领用记录。
    gate_ok, gate_code, gate_detail = metro.field_gate(db, code, serial)
    if not gate_ok:
        return _rejected(gate_code, gate_detail, box=box_id, ticket=b['ticket'],
                         serial=serial, code=code)

    # 4) 通过：生成校验串并扣减（状态 → ISSUED）
    nxt = db.one("SELECT COALESCE(MAX(seq),0)+1 n FROM checkouts WHERE box=?", (box_id,))['n']
    cs = checksum_of(secret, box_id, b['ticket'], code, it['batch_id'], serial)
    db.x("INSERT INTO checkouts(box,ticket,seq,code,batch,serial,checksum,actor,checked_out_at,"
         "wear_at_issue,seal_ok_at_issue,cert_id,cert_ver) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
         (box_id, b['ticket'], nxt, code, it['batch_id'], serial, cs, client_id,
          scanned_at, it['wear'], it['seal_ok'],
          exact['cert_id'], exact['cert_ver']))
    db.x("UPDATE tools SET state=? WHERE serial=?", (ST_ISSUED, serial))
    db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
         (scanned_at / 1000.0, b['ticket'], client_id, 'TOOL_SCANNED',
          json.dumps({'box': box_id, 'serial': serial, 'code': code,
                      'batch': it['batch_id'], 'seq': nxt, 'checksum': cs,
                      'client_id': client_id, 'client_seq': client_seq}, ensure_ascii=False)))
    return {'ok': True, 'duplicate': False, 'box': box_id, 'ticket': b['ticket'],
            'serial': serial, 'code': code, 'name': TOOL_NAMES.get(code, code),
            'batch': it['batch_id'], 'seq': nxt, 'checksum': cs,
            'reason_code': 'OK', 'reason': '领用成功'}


def _store_event(db, client_id, client_seq, box_id, serial, scanned_at, status, result):
    db.x("INSERT INTO scan_events(client_id,client_seq,box,serial,scanned_at,status,result,received_at) "
         "VALUES(?,?,?,?,?,?,?,?)",
         (client_id, client_seq, box_id, serial, scanned_at, status,
          json.dumps(result, ensure_ascii=False), now_ms()))


def _wake_pending(db, secret, client_id):
    """前序补到后，按客户端位置顺序唤醒后续仍挂起的事件（链式处理，直到再遇缺口）。"""
    woken = []
    while True:
        nxt = db.one(
            "SELECT MIN(client_seq) m FROM scan_events WHERE client_id=? AND status=?",
            (client_id, EV_PENDING))
        if not nxt or nxt['m'] is None:
            break
        seq = nxt['m']
        if seq > 1 and not db.one(
                "SELECT 1 FROM scan_events WHERE client_id=? AND client_seq=? AND status IN (?,?)",
                (client_id, seq - 1, EV_ACCEPTED, EV_REJECTED)):
            break  # 前序仍缺
        ev = dict(db.one("SELECT * FROM scan_events WHERE client_id=? AND client_seq=?",
                         (client_id, seq)))
        res = scan_core(db, secret, client_id, seq, ev['box'], ev['serial'], ev['scanned_at'])
        db.x("UPDATE scan_events SET status=?, result=? WHERE client_id=? AND client_seq=?",
             (EV_ACCEPTED if res['ok'] else EV_REJECTED,
              json.dumps(res, ensure_ascii=False), client_id, seq))
        woken.append({'client_seq': seq, 'result': res, 'woken': True})
    return woken


def scan_online_core(db, secret, client_id, box_id, serial, key=None):
    """在线扫码：由服务端在该客户端位置上自动定序，立即裁决。

    重复判重统一落在领用记录层：同件已领（任何扫码器/任何幂等键）都返回带 duplicate=true
    的首次结果（原校验串），只在该客户端占用一个位置，绝不多扣库存。
    """
    with db.lock:
        seq = db.one("SELECT COALESCE(MAX(client_seq),0)+1 n FROM scan_events WHERE client_id=?",
                     (client_id,))['n']
        result = scan_core(db, secret, client_id, seq, box_id, serial)
        _store_event(db, client_id, seq, box_id, serial, now_ms(),
                     EV_ACCEPTED if result['ok'] else EV_REJECTED, result)
        if key and result.get('duplicate'):
            db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
                 (time.time(), None, 'system', 'DUPLICATE_IGNORED',
                  json.dumps({'key': key, 'scope': 'tool_scan', 'serial': serial},
                             ensure_ascii=False)))
        resp = {'client_id': client_id, 'client_seq': seq, **result}
        db.commit()
        return resp


def scan_flush_core(db, secret, client_id, items):
    """断网恢复后按客户端位置补送一批扫码动作。

    - (client_id, client_seq) 重复补送：返回首次原结果，绝不多扣库存
    - 缺前置动作（前一位置无终态事件）：后续先挂起 PENDING；前序补到后链式唤醒
    - 每件独立裁决，失败带具体原因（REJECTED），失败也占位置，不阻塞同客户端后续
    """
    # 入库排序，保证按位置处理
    items = sorted(items, key=lambda x: x['client_seq'])
    out, pending_new = [], []
    with db.lock:
        for it in items:
            seq, box_id, serial = it['client_seq'], it['box'], it['serial']
            scanned_at = it.get('scanned_at') or now_ms()
            old = db.one("SELECT * FROM scan_events WHERE client_id=? AND client_seq=?",
                         (client_id, seq))
            if old:
                # 重复补送 / 已挂起事件再次补送：一律回原结果，不重复执行
                out.append({'client_seq': seq, 'duplicate': True,
                            'status': old['status'], 'result': json.loads(old['result'])})
                continue
            prior_ok = (seq == 1 or db.one(
                "SELECT 1 FROM scan_events WHERE client_id=? AND client_seq=? AND status IN (?,?)",
                (client_id, seq - 1, EV_ACCEPTED, EV_REJECTED)))
            if not prior_ok:
                _store_event(db, client_id, seq, box_id, serial, scanned_at, EV_PENDING,
                             _rejected(R_GAP, f'缺前置动作：客户端 {client_id} 位置 {seq - 1} '
                                              f'尚未补送到，本条先挂起', box=box_id, serial=serial))
                pending_new.append(seq)
                out.append({'client_seq': seq, 'duplicate': False, 'status': EV_PENDING,
                            'result': json.loads(db.one(
                                "SELECT result FROM scan_events WHERE client_id=? AND client_seq=?",
                                (client_id, seq))['result'])})
                continue
            res = scan_core(db, secret, client_id, seq, box_id, serial, scanned_at)
            st = EV_ACCEPTED if res['ok'] else EV_REJECTED
            _store_event(db, client_id, seq, box_id, serial, scanned_at, st, res)
            out.append({'client_seq': seq, 'duplicate': False, 'status': st, 'result': res})
        # 前序补到 → 唤醒挂起链
        woken = _wake_pending(db, secret, client_id)
        db.commit()
    return {'client_id': client_id, 'results': out, 'woken': woken,
            'pending': [r['client_seq'] for r in out if r['status'] == EV_PENDING]}


# ---------------------------------------------------------------- 移交（持有权唯一）

def handover_core(db, box_id, leader, team, seal_intact=True, actor=None):
    """齐套后移交工具箱给现场负责人。

    缺一不可：齐套、封签完好、未超交接期限。两人并发争抢以条件更新 CAS 保证只有一人拿到持有权。
    """
    actor = actor or leader
    with db.lock:
        bv = box_view(db, box_id)
        if bv['status'] != BX_READY:
            if bv['status'] == BX_HELD:
                reason(db, box_id, bv['ticket'], 'HANDOVER_CONFLICT', R_HOLDER_CONFLICT,
                       f'{team}/{leader} 与 {bv["holder_team"]}/{bv["holder"]} 争抢同一箱，'
                       f'持有权已归后者，{team}/{leader} 抢占失败', actor)
                db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
                     (time.time(), bv['ticket'], actor, 'TOOL_HANDOVER_REJECTED',
                      json.dumps({'box': box_id, 'leader': leader, 'team': team,
                                  'reason': R_HOLDER_CONFLICT}, ensure_ascii=False)))
                db.commit()
                raise RuleError(409, R_HOLDER_CONFLICT,
                                f'工具箱 {box_id} 持有权已归 {bv["holder_team"]}/{bv["holder"]}，'
                                f'{team}/{leader} 抢占失败')
            raise RuleError(409, R_BOX_NOT_READY,
                            f'工具箱 {box_id} 当前状态 {bv["status"]}，不能移交')
        reasons = []
        if not bv['complete']:
            missing = [f'{l["name"]}({l["serial"]})' for l in bv['lines'] if not l['checked_out']]
            reasons.append('缺件：' + '、'.join(missing))
        if not seal_intact or bv['seal_broken']:
            reasons.append('封签破损：移交前检查封签不完好')
        if bv['deadline_passed']:
            reasons.append('超过交接期限：'
                           f'{time.strftime("%Y-%m-%d %H:%M", time.localtime(bv["deadline_ms"]/1000))} 截止')
        if reasons:
            code = R_SEAL_BROKEN if ('封签' in reasons[0]) else \
                   (R_BOX_DEADLINE if ('期限' in reasons[0]) else 'BOX_NOT_COMPLETE')
            # 封签破损 → 箱扣留 BLOCKED；缺件/超期保持 READY 但记录可查原因
            if not seal_intact:
                db.x("UPDATE toolboxes SET seal_broken=1, status=? WHERE id=?", (BX_BLOCKED, box_id))
            reason(db, box_id, bv['ticket'], 'HANDOVER_REJECT', code,
                   '；'.join(reasons), actor)
            db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
                 (time.time(), bv['ticket'], actor, 'TOOL_HANDOVER_REJECTED',
                  json.dumps({'box': box_id, 'leader': leader, 'team': team,
                              'reasons': reasons}, ensure_ascii=False)))
            db.commit()
            raise RuleError(409, code, '移交被拒：' + '；'.join(reasons))

        # CAS：仅当 holder 仍为空时条件更新，数据库层面保证唯一持权
        cur = db.conn.execute(
            "UPDATE toolboxes SET holder=?, holder_team=?, status=?, handover_at=? "
            "WHERE id=? AND holder IS NULL AND status=?",
            (leader, team, BX_HELD, now_ms(), box_id, BX_READY))
        if cur.rowcount == 0:
            winner = get_box(db, box_id)
            reason(db, box_id, bv['ticket'], 'HANDOVER_CONFLICT', R_HOLDER_CONFLICT,
                   f'{team}/{leader} 与 {winner["holder_team"]}/{winner["holder"]} 争抢，后者已持有',
                   actor)
            db.commit()
            raise RuleError(409, R_HOLDER_CONFLICT,
                            f'工具箱 {box_id} 持有权已被 {winner["holder_team"]}/{winner["holder"]} 取得，'
                            f'{team}/{leader} 抢占失败')
        reason(db, box_id, bv['ticket'], 'HANDOVER_OK', 'OK',
               f'齐套 {bv["issued"]}/{bv["total"]}、封签完好、未超期，持有权移交 {team}/{leader}',
               actor)
        db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
             (time.time(), bv['ticket'], actor, 'TOOL_HANDOVER',
              json.dumps({'box': box_id, 'leader': leader, 'team': team}, ensure_ascii=False)))
        db.commit()
        return box_view(db, box_id)


# ---------------------------------------------------------------- 归还清点

def return_core(db, box_id, items, box_seal=None, actor='现场负责人'):
    """归还清点：归还数量、磨损、封签与领用清单逐项对照。任一差异 → 待复核并禁止再借出。"""
    with db.lock:
        b = get_box(db, box_id)
        if b['status'] != BX_HELD:
            raise RuleError(409, R_BOX_NOT_READY,
                            f'工具箱 {box_id} 状态 {b["status"]}，仅已移交持有状态可归还清点')
        locks = effective_lock_rows(db, b['ticket'])
        expected = {l['item_serial']: dict(l) for l in locks}
        actual = {i['serial']: i for i in items}
        diffs = []

        missing = [s for s in expected if s not in actual]
        for s in missing:
            diffs.append({'serial': s, 'code': expected[s]['code'], 'kind': 'MISSING',
                          'detail': f'{s} 应归还未归还（缺件）'})

        damaged, sealcut = set(), set()
        for s, i in actual.items():
            co = db.one("SELECT * FROM checkouts WHERE box=? AND serial=?", (box_id, s))
            if not co:
                diffs.append({'serial': s, 'code': i.get('code'), 'kind': 'EXTRA',
                              'detail': f'{s} 不属于本箱领用清单（多归还/串箱）'})
                continue
            if i.get('code') and i['code'] != co['code']:
                diffs.append({'serial': s, 'code': i.get('code'), 'kind': 'TYPE_MISMATCH',
                              'detail': f'{s} 型号不符：领用 {co["code"]}，归还报 {i["code"]}'})
            wear_bad = str(i.get('wear', 'OK')).upper() in ('BAD', 'DAMAGED', '磨损')
            if wear_bad:
                damaged.add(s)
                diffs.append({'serial': s, 'code': co['code'], 'kind': 'DAMAGED',
                              'detail': f'{s} 工具磨损/损坏：领用 {co["wear_at_issue"] or "OK"} → 归还 BAD'})
            if i.get('seal_ok') is False:
                sealcut.add(s)
                diffs.append({'serial': s, 'code': co['code'], 'kind': 'SEAL_CHANGED',
                              'detail': f'{s} 单件封签发生变化（完好 → 破损）'})
            # 归还即落实物状态（复核处置以此为准）
            db.x("UPDATE tools SET wear=?, seal_ok=? WHERE serial=?",
                 ('BAD' if wear_bad else str(i.get('wear', 'OK')).upper(),
                  1 if i.get('seal_ok', True) else 0, s))

        seal_changed = box_seal is not None and box_seal != b['seal']
        if seal_changed:
            diffs.append({'serial': None, 'code': None, 'kind': 'BOX_SEAL_CHANGED',
                          'detail': f'箱封签变化：领用 {b["seal"]} → 归还 {box_seal}'})

        if diffs:
            db.x("UPDATE toolboxes SET status=?, returned_at=?, review_reason=? WHERE id=?",
                 (BX_REVIEW, now_ms(),
                  '；'.join(d['detail'] for d in diffs), box_id))
            # 差异涉及的件 / 未归还件 → BLOCKED，禁止再借出；其余已归还件也随箱冻结待复核
            bad_serials = {d['serial'] for d in diffs if d['serial']}
            for s in expected:
                if s in bad_serials or s in missing:
                    db.x("UPDATE tools SET state=? WHERE serial=?", (ST_BLOCKED, s))
                elif s in actual:
                    db.x("UPDATE tools SET state=? WHERE serial=?", (ST_BLOCKED, s))
            for d in diffs:
                reason(db, box_id, b['ticket'], 'RETURN_DIFF', 'RETURN_' + d['kind'],
                       d['detail'], actor, d.get('serial'))
            db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
                 (time.time(), b['ticket'], actor, 'TOOL_RETURN_REVIEW',
                  json.dumps({'box': box_id, 'diffs': diffs}, ensure_ascii=False)))
            db.commit()
            return {'box': box_id, 'ok': False, 'status': BX_REVIEW, 'diffs': diffs,
                    'reason': '归还清点存在差异，工具箱置为待复核，差异件禁止再次借出'}

        # 无差异：逐项回库，锁释放（现场使用中被召回冻结的件不回库，保持冻结隔离待召回流程处理）
        for s, lk in expected.items():
            i = actual[s]
            frozen_row = db.one('SELECT metro_frozen FROM tools WHERE serial=?', (s,))
            if frozen_row and frozen_row['metro_frozen']:
                db.x("UPDATE tools SET wear=?, seal_ok=? WHERE serial=?",
                     (str(i.get('wear', 'OK')).upper(),
                      1 if i.get('seal_ok', True) else 0, s))
                reason(db, box_id, b['ticket'], 'RETURN_RECALL_FROZEN', 'ITEM_RECALL_FROZEN',
                       f'{s} 现场使用中被召回冻结：归还后真实持有记录保留，不回库，待复检解除召回',
                       actor, s)
            else:
                db.x("UPDATE tools SET state=?, wear=?, seal_ok=1 WHERE serial=?",
                     (ST_FREE, str(i.get('wear', 'OK')).upper(), s))
            db.x("UPDATE tool_locks SET state=? WHERE id=?", (LK_RELEASED, lk['id']))
        db.x("UPDATE toolboxes SET status=?, returned_at=?, holder=NULL, holder_team=NULL WHERE id=?",
             (BX_RETURNED, now_ms(), box_id))
        reason(db, box_id, b['ticket'], 'RETURN_OK', 'OK',
               f'归还 {len(expected)}/{len(expected)} 件，数量/磨损/封签逐项对照无差异，工具已回库',
               actor)
        db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
             (time.time(), b['ticket'], actor, 'TOOL_RETURNED',
              json.dumps({'box': box_id, 'items': len(expected)}, ensure_ascii=False)))
        db.commit()
        return {'box': box_id, 'ok': True, 'status': BX_RETURNED, 'diffs': [],
                'returned': len(expected)}


def resolve_review_core(db, box_id, action, actor='库管员'):
    """待复核处置：CONFIRM_LOSS（缺件/损坏报废，锁释放、工具保持禁用）或
    CLEAR（误报/已修复，全部回库可再借）。"""
    with db.lock:
        b = get_box(db, box_id)
        if b['status'] != BX_REVIEW:
            raise RuleError(409, 'NOT_UNDER_REVIEW', f'工具箱 {box_id} 不在待复核状态')
        locks = effective_lock_rows(db, b['ticket'])
        if action == 'CLEAR':
            for lk in locks:
                db.x("UPDATE tools SET state=?, seal_ok=1 WHERE serial=?",
                     (ST_FREE, lk['item_serial']))
                db.x("UPDATE tool_locks SET state=? WHERE id=?", (LK_RELEASED, lk['id']))
        elif action == 'CONFIRM_LOSS':
            # 仅缺件/磨损损坏件转维修（报废）停用；封签变化件、型号不符但实物完好件复核后回库
            lost = {r['ref_serial'] for r in db.q(
                "SELECT ref_serial FROM tool_reasons "
                "WHERE box=? AND reason_code IN ('RETURN_MISSING','RETURN_DAMAGED') AND ref_serial IS NOT NULL",
                (box_id,))}
            for lk in locks:
                sn = lk['item_serial']
                if sn in lost:
                    db.x("UPDATE tools SET state=? WHERE serial=?", (ST_MAINT, sn))
                else:
                    db.x("UPDATE tools SET state=? WHERE serial=?", (ST_FREE, sn))
                db.x("UPDATE tool_locks SET state=? WHERE id=?", (LK_RELEASED, lk['id']))
        else:
            raise RuleError(400, 'BAD_ACTION', 'action 须为 CLEAR 或 CONFIRM_LOSS')
        db.x("UPDATE toolboxes SET status=? WHERE id=?", (BX_RETURNED, box_id))
        reason(db, box_id, b['ticket'], 'REVIEW_RESOLVED', action,
               '差异已复核处置，工具箱结案' +
               ('，可用工具回库' if action == 'CLEAR' else '，缺损件转维修/报废'),
               actor)
        db.commit()
        return box_view(db, box_id)


# ---------------------------------------------------------------- 校验串复核

def abort_box_core(db, box_id, actor='库管员', reason_text='库管员作废工具箱并回滚库存'):
    """作废一只尚未移交（READY/BLOCKED）的工具箱：已领单件全部回库、批次锁整批释放。

    用于领用计划取消/异常演示收尾；已移交持有（HELD）的箱必须走归还清点，禁止从这里回滚。
    """
    with db.lock:
        b = get_box(db, box_id)
        if b['status'] not in (BX_READY, BX_BLOCKED):
            raise RuleError(409, 'BOX_NOT_ABORTABLE',
                            f'工具箱 {box_id} 状态 {b["status"]}：仅未移交箱可作废回滚，'
                            '已移交箱必须归还清点')
        locks = effective_lock_rows(db, b['ticket'])
        returned = []
        for lk in locks:
            co = db.one('SELECT 1 FROM checkouts WHERE box=? AND serial=?',
                        (box_id, lk['item_serial']))
            it = db.one('SELECT state, metro_frozen FROM tools WHERE code=? AND serial=?',
                        (lk['code'], lk['item_serial']))
            # 召回隔离/现场冻结件不得因作废箱回库，保持召回处置状态
            if it['state'] != ST_QUARANTINED and not it['metro_frozen']:
                db.x("UPDATE tools SET state=? WHERE code=? AND serial=?",
                     (ST_FREE, lk['code'], lk['item_serial']))
            db.x("UPDATE tool_locks SET state=? WHERE id=?", (LK_RELEASED, lk['id']))
            if co:
                returned.append(lk['item_serial'])
        db.x("UPDATE toolboxes SET status=? WHERE id=?", (BX_CANCELLED, box_id))
        reason(db, box_id, b['ticket'], 'BOX_ABORT', 'BOX_CANCELLED',
               f'{reason_text}；已领 {len(returned)} 件回库，批次锁整批释放', actor)
        db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
             (time.time(), b['ticket'], actor, 'TOOLBOX_ABORTED',
              json.dumps({'box': box_id, 'restocked': returned, 'reason': reason_text},
                         ensure_ascii=False)))
        db.commit()
        return {'box': box_id, 'status': BX_CANCELLED, 'restocked': returned,
                'locks_released': len(locks)}


# ---------------------------------------------------------------- 校验串复核
def repair_item_core(db, serial, actor='库管员'):
    """维修完成回库：BLOCKED/MAINTENANCE 单件经检修/更换后恢复在库可借状态。"""
    with db.lock:
        it = db.one('SELECT * FROM tools WHERE serial=?', (serial,))
        if not it:
            raise RuleError(404, 'ITEM_NOT_FOUND', f'工具 {serial} 不存在')
        if it['state'] not in (ST_BLOCKED, ST_MAINT):
            raise RuleError(409, 'ITEM_NOT_BLOCKED',
                            f'工具 {serial} 当前状态 {it["state"]}，无需维修回库')
        if it['state'] == ST_QUARANTINED:
            raise RuleError(409, 'ITEM_QUARANTINED',
                            f'{serial} 处于召回隔离，维修回库不能解除召回，须走复检解除流程')
        db.x("UPDATE tools SET state=?, wear='OK', seal_ok=1 WHERE serial=?",
             (ST_FREE, serial))
        reason(db, None, None, 'ITEM_REPAIRED', 'ITEM_REPAIRED',
               f'{serial} 维修/更换完成，恢复在库可借', actor, serial)
        db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
             (time.time(), None, actor, 'TOOL_REPAIRED', json.dumps({'serial': serial})))
        db.commit()
        return {'serial': serial, 'state': ST_FREE}


def verify_checksum(db, box_id, serial):
    """按当前服务端密钥重算校验串，与领用落库值逐项比对。"""
    secret = get_secret(db)
    co = db.one("SELECT * FROM checkouts WHERE box=? AND serial=?", (box_id, serial))
    if not co:
        raise RuleError(404, 'CHECKOUT_NOT_FOUND',
                        f'工具箱 {box_id} 中查无工具 {serial} 的领用记录')
    expect = checksum_of(secret, co['box'], co['ticket'], co['code'], co['batch'], co['serial'])
    return {'box': co['box'], 'ticket': co['ticket'], 'serial': co['serial'],
            'code': co['code'], 'batch': co['batch'], 'stored': co['checksum'],
            'recomputed': expect, 'valid': hmac.compare_digest(expect, co['checksum'])}


# ---------------------------------------------------------------- 全局视图

def board_view(db):
    """借还台总览：工具库存、批次、库存锁、工具箱（含校验串/持有人/原因）。"""
    tools = _rows(db, 'SELECT code, serial, batch_id, state, wear, seal_ok, metro_frozen, '
                      'freeze_reason FROM tools ORDER BY code, serial')
    batches = _rows(db, 'SELECT * FROM tool_batches ORDER BY code, batch_id')
    boxes = []
    for r in db.q("SELECT id FROM toolboxes ORDER BY ready_at"):
        bv = box_view(db, r['id'])
        bv['reasons'] = _rows(db,
                              'SELECT scene, reason_code, detail, ref_serial, actor, created_at '
                              'FROM tool_reasons WHERE box=? ORDER BY rowid', (r['id'],))
        boxes.append(bv)
    locks = []
    for r in db.q("SELECT * FROM tool_locks WHERE state=? ORDER BY ticket, line_no", (LK_LOCKED,)):
        locks.append({'ticket': r['ticket'], 'code': r['code'], 'batch': r['batch_id'],
                      'serial': r['item_serial']})
    events = _rows(db, 'SELECT client_id, client_seq, box, serial, status, result FROM scan_events '
                       'ORDER BY rowid DESC LIMIT 50')
    for e in events:
        e['result'] = json.loads(e['result'])
    for t in tools:
        t['metro'] = metro.is_metro(t['code'])
        t['metro_frozen'] = bool(t['metro_frozen'])
    pending = _rows(db, "SELECT client_id, client_seq, box, serial FROM scan_events "
                        "WHERE status=? ORDER BY client_id, client_seq", (EV_PENDING,))
    return {'tools': tools, 'batches': batches, 'locks': locks, 'boxes': boxes,
            'scan_events': events, 'pending': pending, 'server_time': now_ms()}
