"""计量工器具检定与批次召回 — 核心业务模块（不直接依赖 Web 框架）。

检定员为验电器（VDT）、力矩扳手（WRENCH）等计量器具登记**带生效区间的证书版本**：
- 证书一经用于现场任务（被安全票锁定纳入准备清单）即固化，不得覆盖修改，只能形成后继版本；
- 调度侧按计划作业时段与电压等级校验器具资格：将在作业结束前到期、被暂停、
  已被后继版本取代、不适用于当前电压等级的，不得纳入准备清单，并返回可核对的拒绝原因。

质量负责人按器具编号 / 生产批号 / 证书版本发起召回，一个事务给出受影响清单并按当时状态处置：
- 库内器具      → 隔离（QUARANTINED）
- 已预留未出库  → 撤销资格（锁行 DISQUALIFIED，需另补合规格器具）
- 现场正在使用  → 保留真实持有记录但冻结后续许可（tools.metro_frozen=1）
- 已归档历史    → 只追加召回标记（metro_marks），不改写当时使用的证书

召回与证书换版、任务许可并发时，以持久化版本（DB 行 + db.lock 串行化 + 唯一索引）决定唯一结果；
相同召回再次提交返回原结果，不重复冻结、不追加两条相同审计。
召回解除必须引用一次复检结论（metro_rechecks），只恢复通过复检且未被其他有效召回覆盖的器具。
全部状态事务化持久化于 SQLite，服务重启后完整恢复。
"""
import hashlib
import json
import time
import uuid

# ---------------------------------------------------------------- 常量

# 器具状态（复用 tools.ST_*；隔离为本模块新增）
ST_FREE = 'FREE'
ST_LOCKED = 'LOCKED'
ST_ISSUED = 'ISSUED'
ST_BLOCKED = 'BLOCKED'
ST_MAINT = 'MAINTENANCE'
ST_QUARANTINED = 'QUARANTINED'        # 召回隔离：库内召回件的处置状态

# 证书版本状态
CERT_ACTIVE = 'ACTIVE'
CERT_SUSPENDED = 'SUSPENDED'
CERT_SUPERSEDED = 'SUPERSEDED'        # 已被后继版本取代（旧版本行原样保留，只改状态）

# 锁行资格（tool_locks.metro_state）
MS_OK = 'OK'
MS_DISQUALIFIED = 'DISQUALIFIED'      # 召回撤销资格 / 证书事后失效

# 召回处置（recall_items.disposition）
DP_QUARANTINED = 'QUARANTINED'        # 库内 → 隔离
DP_LOCK_REVOKED = 'LOCK_REVOKED'      # 已预留未出库 → 撤销资格
DP_FIELD_FROZEN = 'FIELD_FROZEN'      # 现场正在使用 → 冻结后续许可，持有记录保留
DP_ARCHIVE_MARK = 'ARCHIVE_MARK'      # 已归档历史 → 只追加标记
DP_OUT_OF_SERVICE = 'OUT_OF_SERVICE'  # 召回时已在维修/待复核：本就停用，仅登记不二次处置

# 召回范围
SCOPE_SERIAL = 'SERIAL'
SCOPE_BATCH = 'BATCH'
SCOPE_CERT = 'CERT'
SCOPES = (SCOPE_SERIAL, SCOPE_BATCH, SCOPE_CERT)

# 召回 / 解除阶段
RC_ACTIVE = 'ACTIVE'
RC_RELEASED = 'RELEASED'

# 可核对的拒绝原因码（界面与测试据此展示/断言）
R_CERT_MISSING = 'CERT_MISSING'                          # 查无有效检定证书
R_CERT_SUSPENDED = 'CERT_SUSPENDED'                      # 证书被暂停
R_CERT_SUPERSEDED = 'CERT_SUPERSEDED'                    # 证书已被后继版本取代
R_CERT_EXPIRES = 'CERT_EXPIRES_BEFORE_JOB_END'           # 证书将在作业结束前到期
R_CERT_VOLTAGE = 'CERT_NOT_APPLICABLE_VOLTAGE'           # 证书不适用于当前电压等级
R_ITEM_QUARANTINED = 'ITEM_QUARANTINED'                  # 器具已被召回隔离
R_ITEM_FROZEN = 'ITEM_RECALL_FROZEN'                     # 器具在现场使用中被召回冻结
R_LOCK_REVOKED = 'LOCK_REVOKED_BY_RECALL'                # 预留资格已被召回撤销

# 需要计量检定证书的器具型号（其他型号如绝缘手套/操作杆/接地棒不强制计量证书）
METRO_CODES = ('VDT', 'WRENCH')

TERMINAL_BOX = ('RETURNED', 'REVIEW', 'CANCELLED')


class RuleError(Exception):
    def __init__(self, http_status, reason_code, detail):
        super().__init__(detail)
        self.http_status = http_status
        self.reason_code = reason_code
        self.detail = detail


# ---------------------------------------------------------------- 小工具

def now_ms():
    return int(time.time() * 1000)


def _rows(db, sql, args=()):
    return [dict(r) for r in db.q(sql, args)]


def _j(v, default):
    if v is None or v == '':
        return default
    return json.loads(v)


def new_id(prefix):
    return prefix + time.strftime('%H%M%S') + uuid.uuid4().hex[:6].upper()


def scope_nat_key(scope_type, scope_value, reason):
    """相同召回（同范围+同原因）的自然键：重复提交返回首次结果，不产生第二份裁决。"""
    return hashlib.sha256(json.dumps(
        {'scope_type': scope_type, 'scope_value': sorted(scope_value),
         'reason': reason}, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()[:16]


def is_metro(code):
    return code in METRO_CODES


# ---------------------------------------------------------------- 种子数据

def seed_metro(db):
    """为库存中的计量器具（VDT/WRENCH）播种首版检定证书。

    默认证书生效区间宽（一年）、适用电压宽（0.4~35kV），保证既有借还台剧本不变；
    临近到期/电压不适用等场景由检定员登记后继版本或演示接口制造。
    """
    if db.one('SELECT 1 FROM metro_certs LIMIT 1'):
        return
    t0 = now_ms()
    for it in db.q('SELECT code, serial, batch_id FROM tools WHERE code IN (?,?) ORDER BY serial',
                   METRO_CODES):
        cid = 'C-' + it['serial']
        db.x('INSERT INTO metro_certs(cert_id,serial,code,batch_id,ver,valid_from,valid_to,'
             'voltage_min,voltage_max,status,used,used_at,registered_by,registered_at,note) '
             'VALUES(?,?,?,?,1,?,?,?,?,?,0,NULL,?,?,?)',
             (cid, it['serial'], it['code'], it['batch_id'],
              t0 - 30 * 86400_000, t0 + 365 * 86400_000, 0.4, 35, CERT_ACTIVE,
              '检定员·种子', t0, '出厂/周期检定首版'))
    db.commit()


# ---------------------------------------------------------------- 证书版本

def latest_cert(db, serial):
    """某器具当前最新版本的证书行（含 SUPERSEDED/SUSPENDED，资格由评估函数判定）。"""
    row = db.one('SELECT * FROM metro_certs WHERE serial=? ORDER BY ver DESC, rowid DESC LIMIT 1',
                 (serial,))
    return dict(row) if row else None


def cert_history(db, serial):
    """证书沿革：逐版本列出状态、生效区间、适用电压、用于何处、换版依据。"""
    rows = [_fmt_cert(dict(r))
            for r in db.q('SELECT * FROM metro_certs WHERE serial=? ORDER BY ver, rowid', (serial,))]
    used_on = _rows(db,
        "SELECT ticket, box, cert_id, cert_ver, checked_out_at FROM checkouts WHERE serial=? "
        "ORDER BY checked_out_at", (serial,))
    locks = _rows(db,
        "SELECT ticket, cert_id, cert_ver, state, metro_state, metro_reason, locked_at FROM tool_locks "
        "WHERE item_serial=? ORDER BY locked_at", (serial,))
    marks = _rows(db, 'SELECT recall, kind, ref_type, ref_id, detail, created_at FROM metro_marks '
                      'WHERE serial=? ORDER BY rowid', (serial,))
    return {'serial': serial, 'versions': rows,
            'used_on': [{'ticket': u['ticket'], 'box': u['box'], 'cert_id': u['cert_id'],
                         'cert_ver': u['cert_ver'], 'checked_out_at': u['checked_out_at']}
                        for u in used_on],
            'locks': locks, 'marks': marks}


def register_cert_core(db, serial, valid_from, valid_to, voltage_min, voltage_max,
                       actor='检定员', note='', force_amend=False):
    """登记证书版本。

    - 器具无证书：建立首版（ver=1）。
    - 已有证书：形成**后继版本**（ver+1），旧 ACTIVE 行置 SUPERSEDED 并原样保留；
      旧版本一旦已用于现场任务（used=1）绝不允许覆盖修改。
    - force_amend 仅允许改"从未使用"的当前证书（演示登记笔误更正）；已使用证书一律拒绝。
    """
    it = db.one('SELECT code, batch_id FROM tools WHERE serial=?', (serial,))
    if not it:
        raise RuleError(404, 'ITEM_NOT_FOUND', f'器具编号 {serial} 不存在')
    if not is_metro(it['code']):
        raise RuleError(400, 'NOT_METROLOGY_TOOL',
                        f'{it["code"]} 不是强制计量检定器具（仅 {"/".join(METRO_CODES)} 登记检定证书）')
    if valid_to <= valid_from:
        raise RuleError(400, 'BAD_VALIDITY', '生效区间非法：valid_to 必须晚于 valid_from')
    if voltage_min < 0 or voltage_max < voltage_min:
        raise RuleError(400, 'BAD_VOLTAGE_RANGE', '适用电压等级区间非法：0 ≤ voltage_min ≤ voltage_max')
    cur = latest_cert(db, serial)
    if cur and force_amend:
        if cur['used']:
            raise RuleError(409, 'CERT_IN_USE_IMMUTABLE',
                            f"证书 {cur['cert_id']}（v{cur['ver']}）已用于现场任务"
                            f"（{cur['used_at']}），一经使用不得覆盖修改；请登记后继版本")
        db.x('UPDATE metro_certs SET valid_from=?, valid_to=?, voltage_min=?, voltage_max=?, '
             'note=?, registered_by=?, registered_at=? WHERE cert_id=? AND serial=?',
             (valid_from, valid_to, voltage_min, voltage_max, note, actor, now_ms(),
              cur['cert_id'], serial))
        db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
             (time.time(), None, actor, 'METRO_CERT_AMENDED',
              json.dumps({'serial': serial, 'cert_id': cur['cert_id'], 'ver': cur['ver'],
                          'valid_to': valid_to, 'voltage': [voltage_min, voltage_max]},
                         ensure_ascii=False)))
        return {'amended': True, 'cert': latest_cert(db, serial)}
    if cur and cur['status'] == CERT_ACTIVE and not force_amend:
        # 正常换版：旧版（无论是否已使用）置 SUPERSEDED 后原样保留，只追加新版本
        db.x("UPDATE metro_certs SET status=? WHERE cert_id=? AND serial=?",
             (CERT_SUPERSEDED, cur['cert_id'], serial))
    ver = (cur['ver'] + 1) if cur else 1
    cid = 'C-' + serial + f'-V{ver}' if cur else 'C-' + serial
    db.x('INSERT INTO metro_certs(cert_id,serial,code,batch_id,ver,valid_from,valid_to,'
         'voltage_min,voltage_max,status,used,used_at,registered_by,registered_at,note) '
         'VALUES(?,?,?,?,?,?,?,?,?,?,0,NULL,?,?,?)',
         (cid, serial, it['code'], it['batch_id'], ver, valid_from, valid_to,
          voltage_min, voltage_max, CERT_ACTIVE, actor, now_ms(), note))
    db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
         (time.time(), None, actor, 'METRO_CERT_REGISTERED',
          json.dumps({'serial': serial, 'cert_id': cid, 'ver': ver,
                      'valid_from': valid_from, 'valid_to': valid_to,
                      'voltage': [voltage_min, voltage_max],
                      'superseded': cur['cert_id'] if cur else None,
                      'immutable_rule': '旧证书一经用于现场任务不得覆盖，仅形成后继版本'},
                     ensure_ascii=False)))
    return {'amended': False, 'cert': latest_cert(db, serial)}


def _set_cert_status(db, cert_id, serial, new_status, actor, reason_text, event_type, code):
    row = db.one('SELECT * FROM metro_certs WHERE cert_id=? AND serial=?', (cert_id, serial))
    if not row:
        raise RuleError(404, 'CERT_NOT_FOUND', f'证书 {cert_id}/{serial} 不存在')
    if row['status'] == new_status:
        return {'duplicate': True, 'cert': dict(row)}
    if new_status == CERT_ACTIVE and row['status'] != CERT_SUSPENDED:
        raise RuleError(409, 'CERT_NOT_SUSPENDED',
                        f"证书状态 {row['status']} 不能恢复，仅被暂停的证书可恢复")
    if new_status == CERT_SUSPENDED and row['status'] != CERT_ACTIVE:
        raise RuleError(409, 'CERT_NOT_ACTIVE',
                        f"证书状态 {row['status']} 不能暂停（仅 ACTIVE 可暂停）")
    db.x('UPDATE metro_certs SET status=? WHERE cert_id=? AND serial=?',
         (new_status, cert_id, serial))
    db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
         (time.time(), None, actor, event_type,
          json.dumps({'serial': serial, 'cert_id': cert_id, 'ver': row['ver'],
                      'code': code, 'reason': reason_text}, ensure_ascii=False)))
    return {'duplicate': False, 'cert': latest_cert(db, serial)}


def suspend_cert_core(db, cert_id, serial, actor='检定员', reason=''):
    return _set_cert_status(db, cert_id, serial, CERT_SUSPENDED, actor, reason,
                            'METRO_CERT_SUSPENDED', reason)


def resume_cert_core(db, cert_id, serial, actor='检定员', reason='复检合格恢复'):
    return _set_cert_status(db, cert_id, serial, CERT_ACTIVE, actor, reason,
                            'METRO_CERT_RESUMED', reason)


def mark_certs_used(db, serials, ticket, used_at=None):
    """证书用于现场任务（纳入安全票准备清单）→ 固化：之后不得覆盖修改，只能换版。"""
    used_at = used_at or now_ms()
    out = []
    for sn in serials:
        row = latest_cert(db, sn)
        if row and not row['used']:
            db.x('UPDATE metro_certs SET used=1, used_at=? WHERE cert_id=? AND serial=?',
                 (used_at, row['cert_id'], sn))
            out.append({'serial': sn, 'cert_id': row['cert_id'], 'ver': row['ver']})
    return out


# ---------------------------------------------------------------- 资格评估

def active_freeze(db, serial):
    """返回仍对该器具有效的现场冻结召回行（FIELD_FROZEN 且召回未解除）。"""
    return db.one(
        "SELECT ri.* FROM recall_items ri JOIN recalls r ON r.id=ri.recall "
        "WHERE ri.serial=? AND ri.disposition=? AND r.status=? AND ri.restored=0 LIMIT 1",
        (serial, DP_FIELD_FROZEN, RC_ACTIVE))


def evaluate_item(db, code, serial, job_end_ms, voltage_kv, job_start_ms=None):
    """单件器具在给定计划作业时段/电压等级下的资格。

    返回 (ok, cert_row_or_None, reason_code, detail)。证书将在作业结束前到期、
    被暂停、被后继版本取代、不适用于当前电压等级，或器具正被召回，均判不合格。
    """
    job_start_ms = job_start_ms or now_ms()
    it = db.one('SELECT * FROM tools WHERE code=? AND serial=?', (code, serial))
    if not it:
        return False, None, 'ITEM_NOT_FOUND', f'器具编号 {serial} 不存在'
    if it['state'] == ST_MAINT:
        return False, None, 'ITEM_IN_MAINTENANCE', f'{serial} 维修中，不具备器具资格'
    if it['state'] == ST_QUARANTINED:
        return False, None, R_ITEM_QUARANTINED, f'{serial} 已被召回隔离，不得纳入准备清单'
    if it['state'] == ST_BLOCKED:
        return False, None, 'ITEM_BLOCKED', f'{serial} 待复核停用中，不得纳入准备清单'
    fz = active_freeze(db, serial)
    if fz:
        return False, None, R_ITEM_FROZEN, (
            f'{serial} 在现场使用中被召回 {fz["recall"]} 冻结后续许可，'
            '真实持有记录保留但不得继续许可')
    # 锁行已被召回撤销资格
    lk = db.one("SELECT * FROM tool_locks WHERE item_serial=? AND state='LOCKED' "
                "AND metro_state=?", (serial, MS_DISQUALIFIED))
    if lk:
        return False, None, R_LOCK_REVOKED, (
            f'{serial} 对安全票 {lk["ticket"]} 的预留资格已被召回撤销，需另补合规格器具')
    if not is_metro(code):
        return True, None, None, None
    cert = latest_cert(db, serial)
    if not cert:
        return False, None, R_CERT_MISSING, f'{serial} 查无检定证书，计量器具无证不得出场'
    if cert['status'] == CERT_SUSPENDED:
        return False, cert, R_CERT_SUSPENDED, (
            f'{serial} 证书 {cert["cert_id"]}（v{cert["ver"]}）已被暂停，暂停期内不具备资格')
    if cert['status'] == CERT_SUPERSEDED:
        nxt = latest_cert(db, serial)
        return False, cert, R_CERT_SUPERSEDED, (
            f'{serial} 证书 {cert["cert_id"]}（v{cert["ver"]}）已被后继版本取代，'
            f'请改用最新有效版本')
    if cert['valid_from'] > job_start_ms:
        return False, cert, 'CERT_NOT_YET_VALID', (
            f'{serial} 证书 {cert["cert_id"]} 于作业开始后才生效，当前计划时段不具备资格')
    if cert['valid_to'] < job_end_ms:
        end_s = time.strftime('%Y-%m-%d %H:%M', time.localtime(cert['valid_to'] / 1000))
        return False, cert, R_CERT_EXPIRES, (
            f'{serial} 证书 {cert["cert_id"]}（v{cert["ver"]}）将于 {end_s} 到期，'
            '早于作业结束时刻，证书将在作业结束前到期，不得纳入准备清单')
    if voltage_kv is not None and not (cert['voltage_min'] <= float(voltage_kv) <= cert['voltage_max']):
        return False, cert, R_CERT_VOLTAGE, (
            f'{serial} 证书 {cert["cert_id"]} 适用电压 '
            f'{cert["voltage_min"]}~{cert["voltage_max"]}kV，不适用于当前电压等级 {voltage_kv}kV')
    return True, cert, None, None


def ticket_job(db, tid):
    t = db.one('SELECT plan_start_ms, plan_end_ms, voltage_kv FROM tickets WHERE id=?', (tid,))
    if not t:
        raise RuleError(404, 'TICKET_NOT_FOUND', f'安全票 {tid} 不存在')
    return {'plan_start_ms': t['plan_start_ms'], 'plan_end_ms': t['plan_end_ms'],
            'voltage_kv': t['voltage_kv']}


def evaluate_for_ticket(db, tid, serials_by_code):
    """准备清单校验：对一批 {code: [serial,...]} 逐件给出可核对的通过/拒绝结论。"""
    job = ticket_job(db, tid)
    accepted, rejected = [], []
    for code, serials in serials_by_code.items():
        for sn in serials:
            ok, cert, rcode, detail = evaluate_item(
                db, code, sn, job['plan_end_ms'], job['voltage_kv'], job['plan_start_ms'])
            item = {'code': code, 'serial': sn, 'ok': ok, 'reason_code': rcode, 'detail': detail}
            if ok:
                item['cert_id'] = cert['cert_id'] if cert else None
                item['cert_ver'] = cert['ver'] if cert else None
                accepted.append(item)
            else:
                rejected.append(item)
    return {'ticket': tid, 'job': job, 'accepted': accepted, 'rejected': rejected,
            'eligible': not rejected}


def pick_candidates(db, code, need, job_end_ms, voltage_kv, job_start_ms, exclude=()):
    """自动选件：只在合格的库内 FREE 器具中挑选；返回 (合格候选, 全部FREE件的拒绝明细)。"""
    eligible, rejects = [], []
    rows = db.q("SELECT serial FROM tools WHERE code=? AND state=? ORDER BY serial", (code, ST_FREE))
    for r in rows:
        sn = r['serial']
        if sn in exclude:
            continue
        ok, cert, rcode, detail = evaluate_item(db, code, sn, job_end_ms, voltage_kv, job_start_ms)
        if ok:
            eligible.append(sn)
        else:
            rejects.append({'serial': sn, 'reason_code': rcode, 'detail': detail})
        if len(eligible) >= need:
            break
    return eligible, rejects


# ---------------------------------------------------------------- 现场许可闸门

def field_gate(db, code, serial):
    """扫码领用/继续使用时的现场闸门：持久化版本决定唯一结果。

    重新读取锁行上固化的证书版本与当前证书状态/召回状态——
    锁后换版（旧版本置 SUPERSEDED）、暂停、到期或召回，一律在此刻被拒（可核对原因）。
    """
    it = db.one('SELECT * FROM tools WHERE code=? AND serial=?', (code, serial))
    if not it:
        return False, 'ITEM_NOT_FOUND', f'器具编号 {serial} 不存在'
    if it['state'] == ST_QUARANTINED:
        return False, R_ITEM_QUARANTINED, f'{serial} 已被召回隔离，禁止领用出场'
    if it['metro_frozen']:
        fz = active_freeze(db, serial)
        who = fz['recall'] if fz else '召回'
        return False, R_ITEM_FROZEN, (
            f'{serial} 已被召回 {who} 冻结（现场使用中冻结：真实持有记录保留，后续许可停止）')
    lk = db.one("SELECT * FROM tool_locks WHERE item_serial=? AND state='LOCKED' "
                "ORDER BY locked_at DESC LIMIT 1", (serial,))
    if lk and lk['metro_state'] == MS_DISQUALIFIED:
        return False, R_LOCK_REVOKED, (
            f'{serial} 的预留资格已被召回撤销（{lk["metro_reason"]}），请补合规格器具')
    if not is_metro(code):
        return True, 'OK', None
    now = now_ms()
    # 准备清单所依据的固化版本优先裁决：锁后换版/暂停/到期均不允许凭旧版本出场
    pinned = None
    if lk and lk['cert_id']:
        pinned = db.one('SELECT * FROM metro_certs WHERE cert_id=? AND serial=?',
                        (lk['cert_id'], serial))
    cert = pinned or latest_cert(db, serial)
    if not cert:
        return False, R_CERT_MISSING, f'{serial} 查无检定证书'
    if cert['status'] == CERT_SUSPENDED:
        return False, R_CERT_SUSPENDED, f'{serial} 证书 {cert["cert_id"]} 已被暂停'
    if cert['status'] == CERT_SUPERSEDED:
        return False, R_CERT_SUPERSEDED, (
            f'{serial} 准备清单固化的证书 {cert["cert_id"]}（v{cert["ver"]}）已被后继版本取代'
            '（持久化版本裁决），新版本需重新纳入准备清单，不得凭旧版出场')
    if cert['valid_to'] < now:
        return False, 'CERT_EXPIRED', f'{serial} 证书 {cert["cert_id"]} 已到期'
    return True, 'OK', None


# ---------------------------------------------------------------- 召回

def _affected_serials(db, scope_type, scope_value):
    """按当时持久化状态计算受影响器具集合（编号/批号/证书版本）。"""
    if scope_type == SCOPE_SERIAL:
        serials = [s for s in scope_value
                   if db.one('SELECT 1 FROM tools WHERE serial=?', (s,))]
        unknown = [s for s in scope_value
                   if not db.one('SELECT 1 FROM tools WHERE serial=?', (s,))]
        return serials, unknown
    if scope_type == SCOPE_BATCH:
        rows = db.q('SELECT serial FROM tools WHERE batch_id IN (%s) ORDER BY serial'
                    % ','.join('?' * len(scope_value)), scope_value)
        return [r['serial'] for r in rows], []
    if scope_type == SCOPE_CERT:
        # scope_value 为证书版本标识：可为 cert_id（整组同号版本）或 cert_id@ver
        cert_ids, idvers = set(), []
        for v in scope_value:
            if '@' in v:
                cid, ver = v.rsplit('@', 1)
                idvers.append((cid, int(ver)))
            else:
                cert_ids.add(v)
        serials = set()
        if cert_ids:
            q = ','.join('?' * len(cert_ids))
            for r in db.q(f'SELECT serial FROM metro_certs WHERE cert_id IN ({q})',
                          tuple(cert_ids)):
                serials.add(r['serial'])
        for cid, ver in idvers:
            for r in db.q('SELECT serial FROM metro_certs WHERE cert_id=? AND ver=?', (cid, ver)):
                serials.add(r['serial'])
        return sorted(serials), []
    raise RuleError(400, 'BAD_SCOPE', f'召回范围类型须为 {SCOPES}')


def _live_box(db, serial):
    return db.one(
        "SELECT b.id, b.ticket, b.status, b.holder_team, b.holder FROM checkouts c "
        "JOIN toolboxes b ON b.id=c.box WHERE c.serial=? AND b.status NOT IN (?,?,?) LIMIT 1",
        (serial, *TERMINAL_BOX))


def _archive_checkouts(db, serial):
    return _rows(db,
        "SELECT c.* FROM checkouts c JOIN toolboxes b ON b.id=c.box "
        "WHERE c.serial=? AND b.status IN (?,?,?) ORDER BY c.checked_out_at",
        (serial, *TERMINAL_BOX))


def recall_core(db, scope_type, scope_value, reason, actor='质量负责人', idem_key=None,
                key=None):
    """批次/编号/证书召回：一个事务给出受影响清单并按当时状态处置。

    幂等：携带 idem_key 或"同范围+同原因"再次提交，一律返回首次裁决原结果，
    不重复冻结、不追加第二条相同审计。调用方负责加 db.lock 与提交/回滚。
    """
    if scope_type not in SCOPES:
        raise RuleError(400, 'BAD_SCOPE', f'召回范围类型须为 {SCOPES}')
    scope_value = sorted(set(scope_value or []))
    if not scope_value:
        raise RuleError(400, 'EMPTY_SCOPE', '召回范围为空')
    nat = scope_nat_key(scope_type, scope_value, reason)
    if idem_key:
        old = db.one('SELECT id FROM recalls WHERE idem_key=?', (idem_key,))
        if old:
            view = recall_view(db, old['id'])
            view['duplicate'] = True
            view['duplicate_reason'] = '幂等键命中：返回首次召回的原结果，未重复处置'
            return view
    old = db.one('SELECT id FROM recalls WHERE nat_key=?', (nat,))
    if old:
        view = recall_view(db, old['id'])
        view['duplicate'] = True
        view['duplicate_reason'] = '相同召回（同范围+同原因）再次提交：返回原结果，不重复冻结/审计'
        return view

    serials, unknown = _affected_serials(db, scope_type, scope_value)
    rid = new_id('RC')
    db.x('INSERT INTO recalls(id,scope_type,scope_value,reason,actor,created_at,status,'
         'released_at,released_by,recheck_id,nat_key,idem_key,result_json) '
         'VALUES(?,?,?,?,?,?,?,NULL,NULL,NULL,?,?,NULL)',
         (rid, scope_type, json.dumps(scope_value, ensure_ascii=False), reason, actor,
          now_ms(), RC_ACTIVE, nat, idem_key))

    items, tasks, archive = [], set(), []
    for sn in serials:
        it = db.one('SELECT * FROM tools WHERE serial=?', (sn,))
        disp, detail = None, ''
        box = _live_box(db, sn)
        lk = db.one("SELECT * FROM tool_locks WHERE item_serial=? AND state='LOCKED' "
                    "ORDER BY locked_at DESC LIMIT 1", (sn,))
        if box or (it['state'] == ST_ISSUED and it['metro_frozen']):
            # 真实持有/领用记录（checkouts）原样保留，只冻结后续许可。
            # 箱已结案但冻结未消（归还的在召回件）同样按现场冻结来源持续冻结，待复检解除。
            disp = DP_FIELD_FROZEN
            detail = (f'现场正在使用（工具箱 {box["id"] if box else "已归还待复检"}'
                      f'/票据 {box["ticket"] if box else "-"}/'
                      f'{box["holder_team"] if box else "待复检"}）：保留真实持有记录，冻结后续许可')
            db.x("UPDATE tools SET state=?, metro_frozen=1, freeze_reason=COALESCE(freeze_reason,?) "
                 "WHERE serial=?",
                 (ST_ISSUED if box else ST_QUARANTINED, f'{rid}:{reason}', sn))
            if box:
                tasks.add(box['ticket'])
            db.x('INSERT OR IGNORE INTO metro_marks(recall,serial,kind,ref_type,ref_id,detail,created_at) '
                 'VALUES(?,?,?,?,?,?,?)',
                 (rid, sn, 'FIELD_FROZEN', 'BOX', box['id'] if box else 'RETURNED', detail, now_ms()))
        elif lk:
            # 已预留但尚未出库：撤销该锁行资格，工具退回库内隔离，需另补合规格器具
            disp = DP_LOCK_REVOKED
            detail = f'已预留票据 {lk["ticket"]} 但尚未出库：撤销预留资格，另补合规格器具'
            db.x("UPDATE tool_locks SET metro_state=?, metro_reason=? WHERE id=? AND line_no=?",
                 (MS_DISQUALIFIED, f'{rid}:{reason}', lk['id'], lk['line_no']))
            db.x("UPDATE tools SET state=? WHERE serial=?", (ST_QUARANTINED, sn))
            tasks.add(lk['ticket'])
            db.x('INSERT OR IGNORE INTO metro_marks(recall,serial,kind,ref_type,ref_id,detail,created_at) '
                 'VALUES(?,?,?,?,?,?,?)',
                 (rid, sn, 'LOCK_REVOKED', 'TICKET', lk['ticket'], detail, now_ms()))
        elif it['state'] in (ST_MAINT, ST_BLOCKED):
            disp = DP_OUT_OF_SERVICE
            detail = f'召回时器具状态 {it["state"]}（本就停用）：登记召回范围，不做二次处置'
        else:
            # 库内器具（FREE，含锁已释放回库）→ 隔离
            disp = DP_QUARANTINED
            detail = '库内器具：转为召回隔离，暂停一切出库/锁定'
            db.x("UPDATE tools SET state=? WHERE serial=?", (ST_QUARANTINED, sn))
        db.x('INSERT INTO recall_items(recall,serial,code,state_before,disposition,detail,'
             'restored,restored_at,created_at) VALUES(?,?,?,?,?,?,0,NULL,?)',
             (rid, sn, it['code'], it['state'], disp, detail, now_ms()))
        items.append({'serial': sn, 'code': it['code'], 'state_before': it['state'],
                      'disposition': disp, 'detail': detail})
        # 已归档历史记录：只追加召回标记，绝不改写当时使用的证书（checkouts 行不动）
        for co in _archive_checkouts(db, sn):
            mark_detail = (f'历史领用归档标记：{co["box"]}/票据 {co["ticket"]} 当时使用证书 '
                           f'{co["cert_id"]}@v{co["cert_ver"]}；历史证书与领用记录不改写')
            db.x('INSERT OR IGNORE INTO metro_marks(recall,serial,kind,ref_type,ref_id,detail,created_at) '
                 'VALUES(?,?,?,?,?,?,?)',
                 (rid, sn, 'ARCHIVE_MARK', 'CHECKOUT', f'{co["box"]}:{sn}', mark_detail, now_ms()))
            archive.append({'serial': sn, 'box': co['box'], 'ticket': co['ticket'],
                           'cert_then': f'{co["cert_id"]}@v{co["cert_ver"]}'})
            tasks.add(co['ticket'])
        # 同件被多召回：第二次只追加本召回的冻结依据，不覆盖首召回原因
        if disp == DP_FIELD_FROZEN:
            db.x('INSERT OR IGNORE INTO metro_marks(recall,serial,kind,ref_type,ref_id,detail,created_at) '
                 'VALUES(?,?,?,?,?,?,?)',
                 (rid, sn, 'FREEZE_BASIS', 'RECALL', rid,
                  f'召回 {rid} 裁决依据：{reason}（持久化版本决定，与既有冻结并存不覆盖）', now_ms()))

    db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
         (time.time(), None, actor, 'METRO_RECALLED',
          json.dumps({'recall': rid, 'scope_type': scope_type, 'scope': scope_value,
                      'reason': reason, 'affected': len(serials), 'unknown': unknown,
                      'dispositions': {d: sum(1 for i in items if i['disposition'] == d)
                                       for d in {i['disposition'] for i in items}},
                      'tasks': sorted(t for t in tasks if t),
                      'archive_marked': len(archive)}, ensure_ascii=False)))
    view = recall_view(db, rid)
    view['unknown_serials'] = unknown
    db.x('UPDATE recalls SET result_json=? WHERE id=?',
         (json.dumps({'affected': view['affected'], 'tasks': view['tasks']}, ensure_ascii=False), rid))
    return view


def recall_view(db, rid):
    r = db.one('SELECT * FROM recalls WHERE id=?', (rid,))
    if not r:
        raise RuleError(404, 'RECALL_NOT_FOUND', f'召回 {rid} 不存在')
    items = _rows(db, 'SELECT * FROM recall_items WHERE recall=? ORDER BY rowid', (rid,))
    marks = _rows(db, 'SELECT serial,kind,ref_type,ref_id,detail,created_at FROM metro_marks '
                      'WHERE recall=? ORDER BY rowid', (rid,))
    tasks = {m['ref_id'] for m in marks if m['ref_type'] == 'TICKET'}
    box_ids = {m['ref_id'] for m in marks if m['ref_type'] == 'BOX'}
    co_refs = [m['ref_id'] for m in marks if m['ref_type'] == 'CHECKOUT']   # box:serial
    if co_refs:
        for box_sn in co_refs:
            box, sn = box_sn.rsplit(':', 1)
            row = db.one('SELECT ticket FROM checkouts WHERE box=? AND serial=?', (box, sn))
            if row:
                tasks.add(row['ticket'])
    if box_ids:
        for bid in box_ids:
            row = db.one('SELECT ticket FROM toolboxes WHERE id=?', (bid,))
            if row:
                tasks.add(row['ticket'])
    counts = {}
    for i in items:
        counts[i['disposition']] = counts.get(i['disposition'], 0) + 1
    pending_restore = [i['serial'] for i in items if not i['restored']
                       and i['disposition'] in (DP_QUARANTINED, DP_FIELD_FROZEN, DP_LOCK_REVOKED)]
    return {'id': rid, 'scope_type': r['scope_type'],
            'scope': _j(r['scope_value'], []), 'reason': r['reason'], 'actor': r['actor'],
            'created_at': r['created_at'], 'status': r['status'],
            'released_at': r['released_at'], 'released_by': r['released_by'],
            'recheck_id': r['recheck_id'], 'affected': items,
            'affected_count': len(items), 'disposition_counts': counts,
            'marks': marks, 'tasks': sorted(t for t in tasks if t),
            'pending_restore': pending_restore}


# ---------------------------------------------------------------- 复检与召回解除

def create_recheck_core(db, recall_id, results, actor='质量负责人', note=''):
    """登记一次复检结论（按器具编号给 PASS/FAIL）。复检单持久化，重启后继续处理。

    一次复检可覆盖召回范围内多件器具；部分通过是常态——解除时只恢复 PASS 件。
    """
    r = db.one('SELECT * FROM recalls WHERE id=?', (recall_id,))
    if not r:
        raise RuleError(404, 'RECALL_NOT_FOUND', f'召回 {recall_id} 不存在')
    chk_id = new_id('CHK')
    out = []
    for item in results:
        sn, verdict = item['serial'], str(item['result']).upper()
        if verdict not in ('PASS', 'FAIL'):
            raise RuleError(400, 'BAD_RECHECK_RESULT', f'{sn} 复检结论须为 PASS/FAIL')
        ri = db.one('SELECT * FROM recall_items WHERE recall=? AND serial=?', (recall_id, sn))
        if not ri:
            raise RuleError(404, 'NOT_IN_RECALL', f'{sn} 不在召回 {recall_id} 的受影响清单内')
        if db.one('SELECT 1 FROM metro_rechecks WHERE recall=? AND serial=? AND result=?',
                  (recall_id, sn, 'PASS')):
            old = dict(db.one('SELECT * FROM metro_rechecks WHERE recall=? AND serial=? AND result=?',
                              (recall_id, sn, 'PASS')))
            out.append({'serial': sn, 'duplicate': True, 'recheck_id': old['id'], 'result': 'PASS'})
            continue
        db.x('INSERT INTO metro_rechecks(id,recall,serial,result,note,inspector,created_at) '
             'VALUES(?,?,?,?,?,?,?)',
             (chk_id, recall_id, sn, verdict, item.get('note') or note, actor, now_ms()))
        out.append({'serial': sn, 'duplicate': False, 'recheck_id': chk_id, 'result': verdict})
    db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
         (time.time(), None, actor, 'METRO_RECHECK',
          json.dumps({'recheck': chk_id, 'recall': recall_id, 'results': out}, ensure_ascii=False)))
    return {'recheck_id': chk_id, 'recall': recall_id, 'results': out}


def _other_active_cover(db, serial, recall_id):
    """是否存在另一笔仍有效、且处置仍未解除的召回覆盖该器具。"""
    return db.one(
        "SELECT ri.recall FROM recall_items ri JOIN recalls r ON r.id=ri.recall "
        "WHERE ri.serial=? AND ri.recall!=? AND r.status=? AND ri.restored=0 "
        "AND ri.disposition IN (?,?,?) LIMIT 1",
        (serial, recall_id, RC_ACTIVE, DP_QUARANTINED, DP_FIELD_FROZEN, DP_LOCK_REVOKED))


def release_core(db, recall_id, recheck_id, actor='质量负责人', serials=None):
    """召回解除：必须引用一次复检结论。

    只恢复"通过复检 且 没有被其他有效召回覆盖"的器具（支持部分恢复）：
    - QUARANTINED 库内隔离件   → 回 FREE（磨损件保持 BLOCKED 走维修流程）
    - FIELD_FROZEN 现场使用件  → 清除冻结标记，后续许可恢复（真实持有记录始终保留）
    - LOCK_REVOKED 预留撤销件  → 锁行恢复 OK，允许该票据继续使用该件
    同一 (召回,复检单) 重复解除返回首次结果；全部处置解除后召回置 RELEASED。
    """
    r = db.one('SELECT * FROM recalls WHERE id=?', (recall_id,))
    if not r:
        raise RuleError(404, 'RECALL_NOT_FOUND', f'召回 {recall_id} 不存在')
    dup = db.one('SELECT result_json FROM metro_releases WHERE recall=? AND recheck_id=?',
                 (recall_id, recheck_id))
    if dup:
        view = json.loads(dup['result_json'])
        view['duplicate'] = True
        view['duplicate_reason'] = '同一复检单的解除裁决已存在：返回首次解除结果'
        return view
    chk_rows = _rows(db, 'SELECT * FROM metro_rechecks WHERE id=? AND recall=? AND result=?',
                     (recheck_id, recall_id, 'PASS'))
    if not chk_rows:
        raise RuleError(404, 'RECHECK_NOT_FOUND',
                        f'复检单 {recheck_id} 不存在或没有 PASS 结论：召回解除必须引用一次合格复检结论')
    pass_serials = {x['serial'] for x in chk_rows}
    if serials is not None:
        pass_serials &= set(serials)
    restored, kept = [], []
    for ri in _rows(db, 'SELECT * FROM recall_items WHERE recall=? '
                        "AND disposition IN (?,?,?) AND restored=0 ORDER BY rowid",
                    (recall_id, DP_QUARANTINED, DP_FIELD_FROZEN, DP_LOCK_REVOKED)):
        sn = ri['serial']
        if sn not in pass_serials:
            kept.append({'serial': sn, 'reason': 'NO_PASS_RECHECK',
                         'detail': '复检未通过或尚未复检：继续隔离/冻结，不予恢复'})
            continue
        other = _other_active_cover(db, sn, recall_id)
        if other:
            kept.append({'serial': sn, 'reason': 'COVERED_BY_OTHER_RECALL',
                         'detail': f'仍被另一笔有效召回 {other["recall"]} 覆盖：本召回解除但该件不恢复'})
            # 本召回对该件的处置行仍标记 restored（本召回不再是冻结来源），但实物不恢复
            db.x('UPDATE recall_items SET restored=1, restored_at=? WHERE recall=? AND serial=?',
                 (now_ms(), recall_id, sn))
            continue
        it = db.one('SELECT * FROM tools WHERE serial=?', (sn,))
        if ri['disposition'] == DP_QUARANTINED:
            target = ST_BLOCKED if (it and it['wear'] == 'BAD') else ST_FREE
            db.x("UPDATE tools SET state=? WHERE serial=?", (target, sn))
        elif ri['disposition'] == DP_FIELD_FROZEN:
            # 仅当没有其他召回仍冻结时清除冻结标记；持有/领用记录一律不动
            if not db.one("SELECT 1 FROM recall_items WHERE serial=? AND recall!=? AND restored=0 "
                          "AND disposition=?", (sn, recall_id, DP_FIELD_FROZEN)):
                db.x("UPDATE tools SET metro_frozen=0, freeze_reason=NULL WHERE serial=?", (sn,))
        else:  # LOCK_REVOKED：资格恢复
            db.x("UPDATE tool_locks SET metro_state=?, metro_reason=NULL "
                  "WHERE item_serial=? AND state='LOCKED' AND metro_state=?",
                  (MS_OK, sn, MS_DISQUALIFIED))
            db.x("UPDATE tools SET state=? WHERE serial=?", (ST_LOCKED, sn))
        db.x('UPDATE recall_items SET restored=1, restored_at=? WHERE recall=? AND serial=?',
             (now_ms(), recall_id, sn))
        db.x('INSERT INTO metro_marks(recall,serial,kind,ref_type,ref_id,detail,created_at) '
             'VALUES(?,?,?,?,?,?,?)',
             (recall_id, sn, 'RELEASED', 'RECHECK', recheck_id,
              f'依据复检 {recheck_id}（PASS）解除召回处置，且无其他有效召回覆盖', now_ms()))
        restored.append({'serial': sn, 'disposition': ri['disposition']})
    # 归档/本就停用的处置行不阻塞召回结案；仍有未解除的实质处置则保持 ACTIVE（可继续部分恢复）
    pending = db.one("SELECT COUNT(*) c FROM recall_items WHERE recall=? AND restored=0 "
                     "AND disposition IN (?,?,?)",
                     (recall_id, DP_QUARANTINED, DP_FIELD_FROZEN, DP_LOCK_REVOKED))['c']
    if pending == 0:
        db.x("UPDATE recalls SET status=?, released_at=?, released_by=?, recheck_id=? WHERE id=?",
             (RC_RELEASED, now_ms(), actor, recheck_id, recall_id))
    db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
         (time.time(), None, actor, 'METRO_RELEASED',
          json.dumps({'recall': recall_id, 'recheck': recheck_id, 'restored': restored,
                      'kept': kept, 'partial': bool(kept),
                      'status': RC_RELEASED if pending == 0 else RC_ACTIVE}, ensure_ascii=False)))
    view = recall_view(db, recall_id)
    view['restored'] = restored
    view['kept'] = kept
    view['partial'] = bool(kept)
    db.x('INSERT INTO metro_releases(recall,recheck_id,result_json,actor,created_at) '
         'VALUES(?,?,?,?,?)',
         (recall_id, recheck_id, json.dumps(view, ensure_ascii=False), actor, now_ms()))
    return view


# ---------------------------------------------------------------- 全局视图

def _fmt_cert(c):
    return {'cert_id': c['cert_id'], 'serial': c['serial'], 'code': c['code'],
            'batch_id': c['batch_id'], 'ver': c['ver'],
            'valid_from': c['valid_from'], 'valid_to': c['valid_to'],
            'voltage_min': c['voltage_min'], 'voltage_max': c['voltage_max'],
            'status': c['status'], 'used': bool(c['used']), 'used_at': c['used_at'],
            'registered_by': c['registered_by'], 'registered_at': c['registered_at'],
            'note': c['note']}


def board_view(db):
    """检定与召回看板：证书沿革/适用范围、召回范围、冻结原因、受影响任务、器具处置、裁决依据。"""
    tools = _rows(db, 'SELECT code, serial, batch_id, state, wear, seal_ok, metro_frozen, '
                      'freeze_reason FROM tools ORDER BY code, serial')
    certs = [_fmt_cert(dict(r)) for r in db.q('SELECT * FROM metro_certs ORDER BY serial, ver')]
    latest = {}
    for c in certs:
        latest[c['serial']] = c
    for t in tools:
        t['metro'] = is_metro(t['code'])
        t['metro_frozen'] = bool(t['metro_frozen'])
        t['latest_cert'] = latest.get(t['serial'])
    recalls = [recall_view(db, r['id'])
               for r in db.q('SELECT id FROM recalls ORDER BY created_at')]
    rechecks = _rows(db, 'SELECT * FROM metro_rechecks ORDER BY created_at')
    disq = _rows(db, "SELECT ticket, code, item_serial, metro_state, metro_reason FROM tool_locks "
                     "WHERE metro_state=?", (MS_DISQUALIFIED,))
    return {'tools': tools, 'certificates': certs, 'recalls': recalls, 'rechecks': rechecks,
            'disqualified_locks': disq, 'metro_codes': list(METRO_CODES),
            'server_time': now_ms()}
