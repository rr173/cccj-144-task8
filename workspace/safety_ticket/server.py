"""配电检修安全票协同系统 — 服务端。

- SQLite 持久化全部状态（票据/步骤/设备控制权/幂等键/审计事件），主站重启后状态完整恢复
- 设备控制权表 control 以设备为主键，从数据库层面保证两台票据不能同时控制同一设备
- 所有变更写入追加式审计事件表 events（全局自增序号保证顺序）
- 现场回报/复核支持幂等键，重复提交返回首次结果；离线操作经 /sync 按序号补传
"""
import hashlib
import json
import sqlite3
import threading
import time
import uuid

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional

import domain
import replay as replay_engine
import tools as tools_mod
import metro
import storm

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS devices(id TEXT PRIMARY KEY, kind TEXT, name TEXT,
                                   x REAL, y REAL, state TEXT, tags TEXT DEFAULT '[]');
CREATE TABLE IF NOT EXISTS edges(a TEXT, b TEXT, PRIMARY KEY(a,b));
CREATE TABLE IF NOT EXISTS grounds(point TEXT, sw TEXT, ticket TEXT, PRIMARY KEY(point, sw));
CREATE TABLE IF NOT EXISTS tickets(id TEXT PRIMARY KEY, title TEXT, work_area TEXT, boundary TEXT,
    snapshot INTEGER, status TEXT, frozen INTEGER DEFAULT 0, freeze_reason TEXT,
    created_by TEXT, created_at REAL, permit_by TEXT, permit_at REAL);
CREATE TABLE IF NOT EXISTS steps(id TEXT PRIMARY KEY, ticket TEXT, seq INTEGER, phase TEXT,
    kind TEXT, device TEXT, point TEXT, sw TEXT, expected TEXT, desc TEXT,
    status TEXT DEFAULT 'PENDING', reporter TEXT, reported_at REAL, verifier TEXT, verified_at REAL);
CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL,
                                  ticket TEXT, actor TEXT, type TEXT, payload TEXT);
CREATE TABLE IF NOT EXISTS idem(key TEXT PRIMARY KEY, response TEXT);
CREATE TABLE IF NOT EXISTS control(device TEXT PRIMARY KEY, ticket TEXT, physical INTEGER DEFAULT 0);
-- 无网作业授权包：调控人员把连续指令段连同电网版本/执行者/监护者/截止时间一次性下发
CREATE TABLE IF NOT EXISTS off_pkgs(id TEXT PRIMARY KEY, ticket TEXT, seg_from INTEGER, seg_to INTEGER,
    phase TEXT, snapshot INTEGER, executor TEXT, guardian TEXT, deadline INTEGER,
    scope_hash TEXT, status TEXT DEFAULT 'ACTIVE', void_reason TEXT,
    issued_by TEXT, issued_at REAL);
-- 本地证据链记录：每条以前一条摘要为父节点（prev_hash），服务端整包事务裁决
CREATE TABLE IF NOT EXISTS evidences(pkg TEXT, seq INTEGER, prev_hash TEXT, hash TEXT,
    status TEXT, reason TEXT, payload TEXT, applied_at REAL, PRIMARY KEY(pkg, seq));
-- 值守班组换班：交班单（调度席为在执行工作票开具，指定切换水位/移交方/候选承接方/签收期限）
CREATE TABLE IF NOT EXISTS handovers(id TEXT PRIMARY KEY, ticket TEXT,
    watermark REAL, from_team TEXT, candidates TEXT, signer TEXT,
    deadline REAL, status TEXT DEFAULT 'PENDING', close_reason TEXT,
    created_by TEXT, created_at REAL, signed_at REAL, closed_at REAL,
    version INTEGER DEFAULT 0);
-- 班组现场证据流：服务端定序入链（seq/prev_hash/hash），按切换水位划分归属
CREATE TABLE IF NOT EXISTS shift_evidences(id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket TEXT, client_id TEXT, team TEXT, device TEXT, kind TEXT,
    ts_data REAL, payload TEXT, base_seq INTEGER,
    seq INTEGER, prev_hash TEXT, hash TEXT, late INTEGER DEFAULT 0,
    status TEXT, reason TEXT, handover TEXT, created_at REAL,
    UNIQUE(ticket, client_id));
-- 审计事件回放：固定事件上界与来源摘要，停止位置（回放游标/校验失败位置）持久化
CREATE TABLE IF NOT EXISTS replays(id TEXT PRIMARY KEY, ticket TEXT,
    from_seq INTEGER, to_seq INTEGER, source_digest TEXT, pinned TEXT,
    baseline TEXT, entries TEXT, tip TEXT, applied TEXT,
    status TEXT, halt TEXT, created_by TEXT, created_at REAL);
-- 假设分支：隔离试算撤销/设备状态变更，version 用于多人并发编辑的版本校验
CREATE TABLE IF NOT EXISTS branches(id TEXT PRIMARY KEY, replay TEXT, ticket TEXT,
    base_seq INTEGER, name TEXT, created_by TEXT, created_at REAL,
    version INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS branch_ops(branch TEXT, ord INTEGER, op_type TEXT,
    params TEXT, actor TEXT, created_at REAL, PRIMARY KEY(branch, ord));
-- 事故分析报告：保留固定来源摘要、事件范围与分支差异，重复生成返回同一结果
CREATE TABLE IF NOT EXISTS reports(id TEXT PRIMARY KEY, replay TEXT, ticket TEXT,
    title TEXT, content TEXT, report_hash TEXT, idem_key TEXT,
    created_by TEXT, created_at REAL);
-- 审计资料外发凭证柜：分析员从一次已完成的归约（回放）选取连续序号封存只读快照；
-- 快照内容创建时整体固化（snapshot），之后正式审计库的任何新增/篡改都不影响已发出的截面。
CREATE TABLE IF NOT EXISTS vouchers(id TEXT PRIMARY KEY, ticket TEXT, replay TEXT,
    from_seq INTEGER, to_seq INTEGER, source_digest TEXT, seal_digest TEXT,
    receivers TEXT, deadline INTEGER, status TEXT DEFAULT 'SEALED',
    invalid_reason TEXT, idem_key TEXT UNIQUE, nat_key TEXT,
    created_by TEXT, created_at REAL, sealed_at REAL, expired_at REAL,
    voided_by TEXT, voided_at REAL, snapshot TEXT);
-- 领取记号：(凭证, 接收人) 唯一，首次领取以 INSERT 原子抢占，重启后领取结果完整恢复
CREATE TABLE IF NOT EXISTS voucher_claims(voucher TEXT, receiver TEXT,
    result TEXT, reason TEXT, claimed_at REAL, PRIMARY KEY(voucher, receiver));
-- 数据库层面保证一张凭证至多一个中签接收人：并发首次领取只有一条 WON 能落库
CREATE UNIQUE INDEX IF NOT EXISTS idx_voucher_winner
    ON voucher_claims(voucher) WHERE result='WON';

-- ============================================================ 工器具借还台
-- 批次：库管员按采购/试验批次管理（同一型号可有多个批次，扫码须与锁定批次一致）
CREATE TABLE IF NOT EXISTS tool_batches(code TEXT, batch_id TEXT, note TEXT,
    PRIMARY KEY(code, batch_id));
-- 单件工具：全局唯一编号（贴码）、所属型号/批次、状态、磨损、单件封签
CREATE TABLE IF NOT EXISTS tools(code TEXT, serial TEXT PRIMARY KEY, batch_id TEXT,
    state TEXT DEFAULT 'FREE', wear TEXT DEFAULT 'OK', seal_ok INTEGER DEFAULT 1);
-- 批次库存锁：一张安全票对具体单件的锁定；LOCKED/RELEASED，整批锁定原子成功或回滚
CREATE TABLE IF NOT EXISTS tool_locks(id TEXT, line_no INTEGER, ticket TEXT, code TEXT,
    batch_id TEXT, item_serial TEXT, state TEXT, actor TEXT, locked_at REAL,
    released_at REAL, PRIMARY KEY(id, line_no));
CREATE INDEX IF NOT EXISTS idx_locks_item ON tool_locks(item_serial, state);
-- 工具箱：封签、持有权（holder 经 CAS 唯一转移）、交接期限、复核原因
CREATE TABLE IF NOT EXISTS toolboxes(id TEXT PRIMARY KEY, ticket TEXT, status TEXT,
    seal TEXT, seal_broken INTEGER DEFAULT 0, holder TEXT, holder_team TEXT,
    deadline_ms INTEGER, ready_at REAL, handover_at REAL, returned_at REAL,
    review_reason TEXT);
-- 逐件领用记录：服务端生成可复核校验串（HMAC，密钥持久化于 meta.tool_secret）
CREATE TABLE IF NOT EXISTS checkouts(box TEXT, ticket TEXT, seq INTEGER, code TEXT,
    batch TEXT, serial TEXT, checksum TEXT, actor TEXT, checked_out_at REAL,
    wear_at_issue TEXT, seal_ok_at_issue INTEGER, PRIMARY KEY(box, serial));
CREATE INDEX IF NOT EXISTS idx_checkouts_serial ON checkouts(serial);
-- 断网补送：按客户端位置 client_seq 严格定序，缺前序挂起，重复补送回原结果
CREATE TABLE IF NOT EXISTS scan_events(id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT, client_seq INTEGER, box TEXT, serial TEXT, scanned_at REAL,
    status TEXT, result TEXT, received_at REAL, UNIQUE(client_id, client_seq));
-- 每条处理原因可查（移交拒绝/抢占负方/归还差异/复核处置/回滚均留痕）
CREATE TABLE IF NOT EXISTS tool_reasons(id INTEGER PRIMARY KEY AUTOINCREMENT,
    box TEXT, ticket TEXT, scene TEXT, reason_code TEXT, detail TEXT,
    ref_serial TEXT, actor TEXT, created_at REAL);

-- ============================================================ 计量工器具检定与批次召回
-- 检定证书版本：器具维度逐版追加，一经用于现场任务（used=1）不得覆盖修改，只能形成后继版本
CREATE TABLE IF NOT EXISTS metro_certs(cert_id TEXT, serial TEXT, code TEXT, batch_id TEXT,
    ver INTEGER, valid_from INTEGER, valid_to INTEGER,
    voltage_min REAL, voltage_max REAL,
    status TEXT DEFAULT 'ACTIVE', used INTEGER DEFAULT 0, used_at REAL,
    registered_by TEXT, registered_at REAL, note TEXT,
    PRIMARY KEY(cert_id, serial));
CREATE INDEX IF NOT EXISTS idx_metro_certs_serial ON metro_certs(serial, ver);
-- 召回单：范围（编号/批号/证书版本）+ 原因构成自然键，相同召回再次提交返回原结果
CREATE TABLE IF NOT EXISTS recalls(id TEXT PRIMARY KEY, scope_type TEXT, scope_value TEXT,
    reason TEXT, actor TEXT, created_at REAL, status TEXT,
    released_at REAL, released_by TEXT, recheck_id TEXT,
    nat_key TEXT UNIQUE, idem_key TEXT UNIQUE, result_json TEXT);
-- 召回处置明细：每件器具按当时状态的处置（隔离/撤资格/现场冻结/历史标记），部分恢复据此推进
CREATE TABLE IF NOT EXISTS recall_items(recall TEXT, serial TEXT, code TEXT,
    state_before TEXT, disposition TEXT, detail TEXT,
    restored INTEGER DEFAULT 0, restored_at REAL, created_at REAL,
    PRIMARY KEY(recall, serial));
-- 召回标记：冻结依据/撤资格/归档历史标记/解除依据；(召回,标记,引用) 唯一保证不重复追加
CREATE TABLE IF NOT EXISTS metro_marks(id INTEGER PRIMARY KEY AUTOINCREMENT,
    recall TEXT, serial TEXT, kind TEXT, ref_type TEXT, ref_id TEXT, detail TEXT, created_at REAL,
    UNIQUE(recall, serial, kind, ref_type, ref_id));
-- 复检结论：解除召回必须引用一次 PASS 复检；复检单持久化，重启后继续处理
CREATE TABLE IF NOT EXISTS metro_rechecks(id TEXT, recall TEXT, serial TEXT, result TEXT,
    note TEXT, inspector TEXT, created_at REAL, PRIMARY KEY(id, recall, serial));
CREATE UNIQUE INDEX IF NOT EXISTS idx_metro_pass ON metro_rechecks(recall, serial) WHERE result='PASS';
-- 召回解除裁决：(召回,复检单) 唯一，重复解除返回首次部分恢复结果
CREATE TABLE IF NOT EXISTS metro_releases(recall TEXT, recheck_id TEXT, result_json TEXT,
    actor TEXT, created_at REAL, PRIMARY KEY(recall, recheck_id));
"""

ACTIVE_STATUS = ('ISOLATING', 'PERMITTED', 'RESTORING')   # 参与边界校验/冻结的状态

# 授权包状态
PKG_ACTIVE = 'ACTIVE'          # 授权有效，等待证据补传
PKG_PARTIAL = 'PARTIAL'        # 已有证据被采纳，仍有缺口
PKG_COMPLETED = 'COMPLETED'    # 段内全部步骤经授权包推进完成
PKG_VOID = 'VOID'              # 授权作废（失效/版本改变/换人/撤销/重新定界）
PKG_EXPIRED = 'EXPIRED'        # 超过截止时间
PKG_SUPERSEDED = 'SUPERSEDED'  # 段内步骤已被在线操作抢先完成，竞争负于在线
PKG_STATES = (PKG_ACTIVE, PKG_PARTIAL, PKG_COMPLETED, PKG_VOID, PKG_EXPIRED, PKG_SUPERSEDED)
PKG_LIVE_STATES = (PKG_ACTIVE, PKG_PARTIAL)               # 仍可补传证据的状态

# 证据裁决状态
EV_PENDING = 'PENDING'        # 未上传
EV_WAITING = 'WAITING'        # 已上传但待补前序（父节点缺口）
EV_ACCEPTED = 'ACCEPTED'      # 已采纳
EV_REJECTED = 'REJECTED'      # 已驳回
EV_DUP_CONFLICT = 'DUP_CONFLICT'  # 同序号后到记录与先到内容冲突
EV_TERMINAL = (EV_ACCEPTED, EV_REJECTED, EV_DUP_CONFLICT)
RACE_MARK = '【竞争负·不阻断】'        # 裁决原因前缀：竞争负于在线但不阻断后续物理序列

# 审计资料外发凭证阶段
VCH_SEALED = 'SEALED'        # 已封存待领取：只读快照已固化，多接收人中尚无人首次领取
VCH_CLAIMED = 'CLAIMED'      # 已领取：唯一接收人完成首次领取，凭证对其开放查阅
VCH_EXPIRED = 'EXPIRED'      # 已超时：超过截止时刻，查阅返回超时原因
VCH_STATES = (VCH_SEALED, VCH_CLAIMED, VCH_EXPIRED, 'VOID')
VCH_VOID = 'VOID'            # 已主动作废：查阅返回作废原因（快照不抹除，仍可审计）
# 领取记号结果
CLM_WON = 'WON'              # 首次领取成功（唯一中签者）
CLM_LOST = 'LOST'            # 并发争抢负方（确定失败原因，可追溯中签者）

# 计入证据摘要/存储的载荷字段（链指针 prev_hash 与摘要 hash 本身不计入）
RECORD_FIELDS = ('pkg', 'seq', 'type', 'step', 'actor', 'scanned', 'device_state',
                 'grounded', 'energized', 'ts')


class DB:
    def __init__(self, path):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self.conn.executescript(SCHEMA)
            self.conn.execute("INSERT OR IGNORE INTO meta(k,v) VALUES('topo_version','1')")
            # 迁移：票据写权班组（任何时刻一张工作票只允许一个班组追加现场证据）
            cols = [r['name'] for r in self.conn.execute('PRAGMA table_info(tickets)')]
            if 'write_owner' not in cols:
                self.conn.execute("ALTER TABLE tickets ADD COLUMN write_owner TEXT")
                self.conn.execute("UPDATE tickets SET write_owner='甲班' WHERE write_owner IS NULL")
            # 迁移：工器具处理原因增加单件引用列（归还复核处置据此定位缺损件）
            rc = [r['name'] for r in self.conn.execute('PRAGMA table_info(tool_reasons)')]
            if 'ref_serial' not in rc:
                self.conn.execute("ALTER TABLE tool_reasons ADD COLUMN ref_serial TEXT")
            # 迁移：安全票计划作业时段与电压等级（器具资格按计划作业时段+电压校验）
            tc = [r['name'] for r in self.conn.execute('PRAGMA table_info(tickets)')]
            if 'plan_start_ms' not in tc:
                self.conn.execute("ALTER TABLE tickets ADD COLUMN plan_start_ms INTEGER")
                self.conn.execute("ALTER TABLE tickets ADD COLUMN plan_end_ms INTEGER")
                self.conn.execute("ALTER TABLE tickets ADD COLUMN voltage_kv REAL DEFAULT 10")
                now = int(time.time() * 1000)
                self.conn.execute("UPDATE tickets SET plan_start_ms=?, plan_end_ms=?, voltage_kv=10 "
                                  "WHERE plan_start_ms IS NULL", (now, now + 8 * 3600_000))
            # 迁移：工具召回冻结标志（现场使用中冻结后续许可，真实持有记录保留）
            tlc = [r['name'] for r in self.conn.execute('PRAGMA table_info(tools)')]
            if 'metro_frozen' not in tlc:
                self.conn.execute("ALTER TABLE tools ADD COLUMN metro_frozen INTEGER DEFAULT 0")
                self.conn.execute("ALTER TABLE tools ADD COLUMN freeze_reason TEXT")
            # 迁移：锁行固化所依据的证书版本与资格状态（锁后换版/召回按持久化版本裁决）
            llc = [r['name'] for r in self.conn.execute('PRAGMA table_info(tool_locks)')]
            if 'cert_id' not in llc:
                self.conn.execute("ALTER TABLE tool_locks ADD COLUMN cert_id TEXT")
                self.conn.execute("ALTER TABLE tool_locks ADD COLUMN cert_ver INTEGER")
                self.conn.execute("ALTER TABLE tool_locks ADD COLUMN metro_state TEXT DEFAULT 'OK'")
                self.conn.execute("ALTER TABLE tool_locks ADD COLUMN metro_reason TEXT")
            # 迁移：领用记录固化当时使用的证书（归档后召回只追加标记，绝不改写）
            ckc = [r['name'] for r in self.conn.execute('PRAGMA table_info(checkouts)')]
            if 'cert_id' not in ckc:
                self.conn.execute("ALTER TABLE checkouts ADD COLUMN cert_id TEXT")
                self.conn.execute("ALTER TABLE checkouts ADD COLUMN cert_ver INTEGER")
            self.conn.commit()

    def reopen(self):
        """模拟主站进程重启：丢弃内存连接，自 SQLite 全量恢复（全部状态本即持久化）。"""
        self.conn.close()
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def q(self, sql, args=()):
        return self.conn.execute(sql, args).fetchall()

    def one(self, sql, args=()):
        return self.conn.execute(sql, args).fetchone()

    def x(self, sql, args=()):
        self.conn.execute(sql, args)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def meta(self, k):
        return self.one('SELECT v FROM meta WHERE k=?', (k,))['v']

    def set_meta(self, k, v):
        self.x('INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v', (k, str(v)))


# ---------------------------------------------------------------- 种子拓扑

SEED_DEVICES = [
    # id, kind, 名称, x, y, 初始状态
    ('SRC1', 'source', '1号主变10kV侧', 40, 100, 'ON'),
    ('CB101', 'switch', '101断路器', 140, 100, 'CLOSED'),
    ('N1', 'bus', '10kV I段母线', 250, 100, '-'),
    ('CB102', 'switch', '102断路器', 360, 100, 'CLOSED'),
    ('F1', 'line', '馈线F1段', 470, 100, '-'),
    ('SW103', 'switch', '103分段开关', 570, 100, 'CLOSED'),
    ('F2', 'line', '馈线F2段', 680, 100, '-'),
    ('CB104', 'switch', '104断路器', 780, 100, 'CLOSED'),
    ('T1', 'transformer', '1号配变', 880, 100, '-'),
    ('SW105', 'switch', '105联络开关', 680, 200, 'OPEN'),
    ('N2', 'bus', '10kV II段母线', 680, 300, '-'),
    ('CB106', 'switch', '106断路器', 570, 300, 'CLOSED'),
    ('SRC2', 'source', '2号主变10kV侧', 460, 300, 'ON'),
    ('CB107', 'switch', '107断路器', 790, 300, 'CLOSED'),
    ('F3', 'line', '馈线F3段', 890, 300, '-'),
    ('CB108', 'switch', '108断路器', 960, 300, 'CLOSED'),
    ('T2', 'transformer', '2号配变', 1060, 300, '-'),
]
SEED_EDGES = [('SRC1', 'CB101'), ('CB101', 'N1'), ('N1', 'CB102'), ('CB102', 'F1'),
              ('F1', 'SW103'), ('SW103', 'F2'), ('F2', 'CB104'), ('CB104', 'T1'),
              ('F2', 'SW105'), ('SW105', 'N2'), ('N2', 'CB106'), ('CB106', 'SRC2'),
              ('N2', 'CB107'), ('CB107', 'F3'), ('F3', 'CB108'), ('CB108', 'T2')]


def seed(db):
    if db.one('SELECT id FROM devices LIMIT 1'):
        return
    for i, k, n, x, y, st in SEED_DEVICES:
        db.x('INSERT INTO devices VALUES(?,?,?,?,?,?,?)', (i, k, n, x, y, st, '[]'))
    for a, b in SEED_EDGES:
        db.x('INSERT INTO edges VALUES(?,?)', (a, b))
    db.commit()


# ---------------------------------------------------------------- 工器具库存种子
# 型号: (名称, 各批次数量)；批次可让“批次不符”场景真实发生
TOOL_SEED = [
    ('GLOVE',    '绝缘手套',     {'B2401': 6, 'B2402': 3}),
    ('VDT',      '高压验电器',   {'B2311': 4}),
    ('ROD',      '绝缘操作杆',   {'B2311': 4, 'B2401': 1}),
    ('GROUNDROD', '接地棒',      {'B2308': 4}),
    ('WRENCH',   '力矩扳手',     {'B2401': 3}),
    ('HELMET',   '安全帽',       {'B2402': 4}),
]


def seed_tools(db):
    if db.one('SELECT serial FROM tools LIMIT 1'):
        return
    for code, name, batches in TOOL_SEED:
        for bid, n in batches.items():
            db.x('INSERT INTO tool_batches VALUES(?,?,?)', (code, bid, f'{name}·批次{bid}'))
            for i in range(1, n + 1):
                serial = f'{code}-{bid}-{i:02d}'
                db.x('INSERT INTO tools(code,serial,batch_id,state,wear,seal_ok) VALUES(?,?,?,?,?,1)',
                     (code, serial, bid, 'FREE', 'OK'))
    # 一件维修件，用于演示维修中不可锁定/领用
    db.x('INSERT INTO tools(code,serial,batch_id,state,wear,seal_ok) VALUES(?,?,?,?,?,1)',
         ('ROD', 'ROD-B2311-05', 'B2311', 'MAINTENANCE', 'BAD'))
    db.commit()


# ---------------------------------------------------------------- 通用助手

def load_topo(db):
    devs = {r['id']: {'kind': r['kind'], 'name': r['name'], 'state': r['state'],
                      'tags': json.loads(r['tags']), 'x': r['x'], 'y': r['y']}
            for r in db.q('SELECT * FROM devices')}
    edges = [(r['a'], r['b']) for r in db.q('SELECT * FROM edges')]
    grounds = [(r['point'], r['sw'], r['ticket']) for r in db.q('SELECT * FROM grounds')]
    return domain.Topo(devs, edges, grounds, int(db.meta('topo_version')))


def ev(db, ticket, actor, etype, **payload):
    db.x('INSERT INTO events(ts,ticket,actor,type,payload) VALUES(?,?,?,?,?)',
         (time.time(), ticket, actor, etype, json.dumps(payload, ensure_ascii=False)))


def get_ticket(db, tid):
    t = db.one('SELECT * FROM tickets WHERE id=?', (tid,))
    if not t:
        raise HTTPException(404, f'票据 {tid} 不存在')
    d = dict(t)
    d['work_area'] = json.loads(d['work_area'])
    d['boundary'] = json.loads(d['boundary'])
    return d


def get_steps(db, tid):
    return [dict(r) for r in db.q('SELECT * FROM steps WHERE ticket=? ORDER BY seq', (tid,))]


def ticket_view(db, tid):
    t = get_ticket(db, tid)
    t['steps'] = get_steps(db, tid)
    return t


def idem(db, key, producer):
    """幂等执行：重复键直接返回首次结果并审计，绝不重复改变物理/票据状态。"""
    row = db.one('SELECT response FROM idem WHERE key=?', (key,))
    if row:
        ev(db, None, 'system', 'DUPLICATE_IGNORED', key=key)
        return json.loads(row['response']), True
    resp = producer()
    db.x('INSERT INTO idem(key,response) VALUES(?,?)', (key, json.dumps(resp, ensure_ascii=False)))
    return resp, False


def revalidate_all(db, actor='system'):
    """拓扑/遥测/挂牌变化后，校验所有在办票据的安全边界；不成立则立即冻结。
    已完成的物理步骤保持原样（只冻结新增操作，不伪造、不回滚）。"""
    topo = load_topo(db)
    for t in db.q(f"SELECT * FROM tickets WHERE status IN {ACTIVE_STATUS}"):
        t = dict(t)
        t['work_area'] = json.loads(t['work_area'])
        problems = domain.boundary_problems(topo, t, get_steps(db, t['id']))
        if problems and not t['frozen']:
            db.x('UPDATE tickets SET frozen=1, freeze_reason=? WHERE id=?',
                 ('；'.join(problems), t['id']))
            ev(db, t['id'], actor, 'FREEZE', reason=problems)
            # 工作票锁定：关闭尚未完成的换班（既成设备动作与控制锁不回退）
            close_handovers(db, t['id'], 'LOCKED', '工作票锁定：' + '；'.join(problems), actor)
        for p in problems:
            if p.startswith('严重'):
                ev(db, t['id'], actor, 'SAFETY_ALARM', detail=p)


def check_control_conflict(db, devices):
    conflict = db.one(
        f"SELECT device, ticket FROM control WHERE device IN ({','.join('?' * len(devices))})",
        devices)
    return dict(conflict) if conflict else None


def acquire_control(db, tid, devices):
    db.x(f"DELETE FROM control WHERE ticket=?", (tid,))
    for d in devices:
        db.x('INSERT INTO control(device, ticket, physical) VALUES(?,?,0)', (d, tid))


def set_tag(db, device, tag, add):
    row = db.one('SELECT tags FROM devices WHERE id=?', (device,))
    tags = json.loads(row['tags'])
    if add and tag not in tags:
        tags.append(tag)
    if not add and tag in tags:
        tags.remove(tag)
    db.x('UPDATE devices SET tags=? WHERE id=?',
         (json.dumps(tags, ensure_ascii=False), device))


class RuleError(Exception):
    """业务规则违例：携带 HTTP 状态码与固定原因码，供在线接口与无网授权包裁决共用。"""
    def __init__(self, http_status, reason, detail=None):
        super().__init__(detail or reason)
        self.http_status = http_status
        self.reason = reason
        self.detail = detail or reason


def http_for(err):
    return HTTPException(err.http_status, err.detail)


def do_report(db, tid, step_id, actor, scanned, key, source='ONLINE'):
    with db.lock:
        try:
            resp, _ = idem(db, key, lambda: report_core(db, tid, step_id, actor, scanned, source))
        except RuleError as e:
            db.rollback()
            raise http_for(e)
        db.commit()
        return resp


def do_verify(db, tid, step_id, actor, key, source='ONLINE'):
    with db.lock:
        try:
            resp, _ = idem(db, key, lambda: verify_core(db, tid, step_id, actor, source))
        except RuleError as e:
            db.rollback()
            raise http_for(e)
        db.commit()
        return resp


def report_core(db, tid, step_id, actor, scanned, source='ONLINE'):
    """回报核心：在线与无网授权包裁决共用。调用方负责加锁/提交。"""
    topo = load_topo(db)
    t = get_ticket(db, tid)
    row = db.one('SELECT * FROM steps WHERE id=? AND ticket=?', (step_id, tid))
    if not row:
        raise RuleError(404, 'STEP_NOT_FOUND', '步骤不存在')
    step = dict(row)
    if t['status'] not in ACTIVE_STATUS:
        raise RuleError(409, 'TICKET_INACTIVE', f"许可单已失效：票据状态 {t['status']}，旧证据按影响范围拒收")
    if t['frozen'] and step['phase'] != 'REST':
        raise RuleError(423, 'TICKET_FROZEN', f"票据已冻结（{t['freeze_reason']}），禁止新增操作")
    if step['status'] != 'PENDING':
        if step['status'] == 'REPORTED':
            raise RuleError(409, 'STEP_ALREADY_REPORTED',
                            '该步骤已被先到的操作回报（先提交者胜），后到的执行确认驳回')
        raise RuleError(409, 'STEP_ALREADY_VERIFIED',
                        '该步骤已复核完成（先提交者胜），后到的执行确认驳回')
    prev = db.one('SELECT COUNT(*) c FROM steps WHERE ticket=? AND seq<? AND status!=?',
                  (tid, step['seq'], 'VERIFIED'))['c']
    if prev:
        raise RuleError(409, 'PREV_STEPS_PENDING', '前序步骤未完成，必须按依赖顺序执行；缺口存在时禁止跨越')
    valid_ids = {step['device'], step.get('point'), step.get('sw')} - {None}
    if scanned not in valid_ids:
        raise RuleError(400, 'SCAN_MISMATCH', f'扫描设备 {scanned} 与步骤要求 {sorted(valid_ids)} 不符')
    if not telemetry_holds(topo, t, step):
        raise RuleError(400, 'LIVE_MISMATCH', '遥测与预期不符，物理条件未满足，拒绝登记（不能伪造物理步骤）')
    db.x('UPDATE steps SET status=?, reporter=?, reported_at=? WHERE id=?',
         ('REPORTED', actor, time.time(), step_id))
    if t['frozen']:
        ev(db, tid, actor, 'RESTORE_UNDER_FREEZE', step=step_id, source=source)
    ev(db, tid, actor, 'STEP_REPORTED', step=step_id, kind=step['kind'],
       device=step['device'], source=source)
    return {'ok': True, 'step': step_id, 'status': 'REPORTED'}


def verify_core(db, tid, step_id, actor, source='ONLINE'):
    """复核核心：在线与无网授权包裁决共用。调用方负责加锁/提交。"""
    topo = load_topo(db)
    t = get_ticket(db, tid)
    row = db.one('SELECT * FROM steps WHERE id=? AND ticket=?', (step_id, tid))
    if not row:
        raise RuleError(404, 'STEP_NOT_FOUND', '步骤不存在')
    step = dict(row)
    if t['status'] not in ACTIVE_STATUS:
        raise RuleError(409, 'TICKET_INACTIVE', f"许可单已失效：票据状态 {t['status']}，旧证据按影响范围拒收")
    if t['frozen'] and step['phase'] != 'REST':
        raise RuleError(423, 'TICKET_FROZEN', f"票据已冻结（{t['freeze_reason']}），禁止新增操作")
    if step['status'] != 'REPORTED':
        if step['status'] == 'VERIFIED':
            raise RuleError(409, 'STEP_ALREADY_VERIFIED',
                            '该步骤已被在线操作抢先复核，包内监护确认在竞争中负于在线（先提交者胜）')
        raise RuleError(409, 'STEP_NOT_REPORTED', f"步骤状态 {step['status']} 不允许复核")
    if actor == step['reporter']:
        raise RuleError(400, 'TWO_PERSON_VIOLATION', '复核人必须与回报人不同（双人复核）')
    if not telemetry_holds(topo, t, step):
        raise RuleError(400, 'LIVE_MISMATCH', '复核时遥测已变化，物理条件不再满足')
    db.x('UPDATE steps SET status=?, verifier=?, verified_at=? WHERE id=?',
         ('VERIFIED', actor, time.time(), step_id))
    # 挂牌/摘牌与控制权物理标记
    if step['kind'] == domain.OPEN_SWITCH:
        set_tag(db, step['device'], domain.tag_for(tid, 'OPEN_SWITCH'), True)
    elif step['kind'] == domain.INSTALL_GROUND:
        set_tag(db, step['point'], domain.tag_for(tid, 'INSTALL_GROUND'), True)
    elif step['kind'] == domain.CLOSE_SWITCH:
        set_tag(db, step['device'], domain.tag_for(tid, 'OPEN_SWITCH'), False)
    elif step['kind'] == domain.REMOVE_GROUND:
        set_tag(db, step['point'], domain.tag_for(tid, 'INSTALL_GROUND'), False)
    if step['phase'] == 'ISO':
        db.x('UPDATE control SET physical=1 WHERE device=? AND ticket=?', (step['device'], tid))
    else:
        db.x('UPDATE control SET physical=0 WHERE device=? AND ticket=?', (step['device'], tid))
    ev(db, tid, actor, 'STEP_VERIFIED', step=step_id, kind=step['kind'],
       device=step['device'], source=source)
    # 恢复步骤全部复核完成 → 自动关闭票据并释放全部控制权
    if t['status'] == 'RESTORING':
        left = db.one("SELECT COUNT(*) c FROM steps WHERE ticket=? AND phase='REST' AND status!='VERIFIED'",
                      (tid,))['c']
        if left == 0:
            db.x("UPDATE tickets SET status='CLOSED' WHERE id=?", (tid,))
            db.x('DELETE FROM control WHERE ticket=?', (tid,))
            ev(db, tid, actor, 'TICKET_CLOSED', note='恢复送电完成，控制权全部释放', source=source)
            # 工作票关闭：关闭尚未完成的换班
            close_handovers(db, tid, 'TICKET_CLOSED', '工作票已关闭', actor)
            storm.mark_ticket_terminal(db, tid, actor, '恢复送电完成，作业收工关闭票据')
    return {'ok': True, 'step': step_id, 'status': 'VERIFIED'}


# ---------------------------------------------------------------- 无网作业授权包

def _normalize(obj):
    """剔除 None、对字典按键排序（与移动终端 JS 端完全一致）；0/false 必须保留。"""
    if isinstance(obj, dict):
        return {k: _normalize(obj[k]) for k in sorted(obj) if obj[k] is not None}
    if isinstance(obj, (list, tuple)):
        return [_normalize(v) for v in obj]
    return obj


def canonical(obj):
    """规范化 JSON 字符串，用于摘要计算与证据载荷去重比对。"""
    return json.dumps(_normalize(obj), separators=(',', ':'), ensure_ascii=False)


def evidence_payload(rec):
    """从记录中提取参与摘要的载荷字段（与下发端/移动终端的字段集合一致）。"""
    return {k: rec.get(k) for k in RECORD_FIELDS}


def evidence_hash(prev_hash, payload):
    """证据链摘要：对载荷规范化后拼接父摘要（首条父节点固定 GENESIS）取 SHA256 前 16 位。"""
    return hashlib.sha256(
        (canonical(payload) + '|' + (prev_hash or 'GENESIS')).encode()
    ).hexdigest()[:16]


def scope_obj(p):
    return {
        'ticket': p['ticket'],
        'seg_from': p['seg_from'],
        'seg_to': p['seg_to'],
        'phase': p['phase'],
        'snapshot': p['snapshot'],
        'executor': p['executor'],
        'guardian': p['guardian'],
        'deadline': p['deadline'],
    }


def scope_hash_of(p):
    return hashlib.sha256(canonical(scope_obj(p)).encode()).hexdigest()[:16]


def get_pkg(db, pid):
    r = db.one('SELECT * FROM off_pkgs WHERE id=?', (pid,))
    if not r:
        raise RuleError(404, 'PKG_NOT_FOUND', f'授权包 {pid} 不存在')
    return dict(r)


def get_evidences(db, pid):
    return [dict(r) for r in db.q('SELECT * FROM evidences WHERE pkg=? ORDER BY seq', (pid,))]


def pkg_step_indexes(p):
    """段内每步在证据链上占用的四个序号（扫码/实况/执行/监护，从 1 起，连续）。"""
    n = p['seg_to'] - p['seg_from'] + 1
    for i in range(n):
        step_seq = p['seg_from'] + i
        base = i * 4
        yield step_seq, {domain.EVID_SCAN: base + 1, domain.EVID_LIVE: base + 2,
                         domain.EVID_EXEC: base + 3, domain.EVID_GUARD: base + 4}


def void_pkg(db, p, reason, actor='system'):
    """作废授权包。只让证据失效，绝不抹除已证实的物理动作、设备占用与审计序列。"""
    if p['status'] in (PKG_VOID, PKG_EXPIRED, PKG_SUPERSEDED, PKG_COMPLETED):
        return p
    db.x('UPDATE off_pkgs SET status=?, void_reason=? WHERE id=?', (PKG_VOID, reason, p['id']))
    p['status'] = PKG_VOID
    p['void_reason'] = reason
    db.x("UPDATE evidences SET status=?, reason=COALESCE(reason,?) WHERE pkg=? AND status NOT IN ('ACCEPTED','REJECTED','DUP_CONFLICT')",
         (EV_REJECTED, reason, p['id']))
    ev(db, p['ticket'], actor, 'PKG_VOIDED', pkg=p['id'], reason=reason)
    return p


def void_pkgs_for_ticket(db, tid, predicate, reason, actor='system'):
    for r in db.q('SELECT * FROM off_pkgs WHERE ticket=?', (tid,)):
        p = dict(r)
        if predicate(p):
            void_pkg(db, p, reason, actor)


def adjudicate(db, p, now_ms=None):
    """整包事务裁决：按证据链序号逐条核验授权范围与父子摘要，只让唯一合法序列推进工序。

    - WAITING：父节点缺口（前序未上传），后到的前序会唤醒它；缺口存在时禁止跨越
    - REJECTED：链断裂后的后续记录一律级联驳回，绝不跳过
    - 竞争（在线已先行）返回确定胜负：先提交者胜，负方带可追溯原因驳回
    必须在 db.lock 内调用。
    """
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    t = get_ticket(db, p['ticket'])
    topo = load_topo(db)
    stored = {r['seq']: dict(r) for r in db.q('SELECT * FROM evidences WHERE pkg=?', (p['id'],))}
    pos, pos_by_step = {}, {}
    for step_seq, m in pkg_step_indexes(p):
        for etype, eseq in m.items():
            pos[eseq] = (step_seq, etype)
            pos_by_step[(step_seq, etype)] = eseq
    steps_by_seq = {s['seq']: dict(s) for s in get_steps(db, p['ticket'])}

    # 授权范围的整包级前置条件（失效点）。命中即作废，未决记录全部带原因拒收。
    pkg_fault = None
    if t['status'] not in ACTIVE_STATUS:
        pkg_fault = ('PERMIT_INVALID', f"许可单失效（{t['status']}），旧片段按影响范围拒收；已证实的物理动作与设备占用保留")
    elif topo.version != p['snapshot']:
        pkg_fault = ('BOUNDARY_VERSION',
                     f"边界版本改变（授权 v{p['snapshot']} → 当前 v{topo.version}），旧授权范围内片段拒收")
    elif now_ms > p['deadline']:
        pkg_fault = ('AUTH_EXPIRED', f"授权已超时（截止 {p['deadline']}），逾期证据拒收")
    if pkg_fault and p['status'] in PKG_LIVE_STATES:
        code, text = pkg_fault
        new_status = PKG_EXPIRED if code == 'AUTH_EXPIRED' else PKG_VOID
        db.x('UPDATE off_pkgs SET status=?, void_reason=? WHERE id=?', (new_status, text, p['id']))
        p['status'] = new_status
        p['void_reason'] = text
        ev(db, p['ticket'], 'system', 'PKG_VOIDED', pkg=p['id'], reason=text, code=code)

    chain_broken = False
    accepted_any = False
    saw_reject = False        # 前序已出现终局驳回（非竞争负）→ 后续级联驳回
    expired_by_evidence = False
    fault_text = None         # 整包级失效原因（许可单/版本/超时），级联记录继承该原因

    def reject(eseq, reason, status=EV_REJECTED):
        db.x('UPDATE evidences SET status=?, reason=?, applied_at=? WHERE pkg=? AND seq=?',
             (status, reason, time.time(), p['id'], eseq))

    def is_race_resolved(row, sseq):
        return (row['status'] == EV_REJECTED and (row['reason'] or '').startswith(RACE_MARK)
                and steps_by_seq[sseq]['status'] == 'VERIFIED')

    for eseq in sorted(pos):
        step_seq, etype = pos[eseq]
        step = steps_by_seq[step_seq]
        r = stored.get(eseq)

        if not r:
            chain_broken = True               # 缺口：后续到达记录只能 WAITING
            continue
        if r['status'] in EV_TERMINAL:
            if r['status'] == EV_REJECTED:
                if is_race_resolved(r, step_seq):
                    pass                       # 竞争负但步骤已闭环：物理序列连续
                else:
                    chain_broken = True
                    saw_reject = True
            elif r['status'] == EV_ACCEPTED:
                accepted_any = True
            continue
        rec = json.loads(r['payload'])

        # 1) 前序已被终局驳回 → 级联驳回（绝不让其越过缺口顶替）
        if saw_reject:
            reject(eseq, fault_text or '前序证据已被终局驳回，链路断裂：本记录级联拒收，绝不跨越')
            continue
        # 2) 父节点缺口（未上传）→ WAITING：后到的前序会唤醒已暂存片段
        if chain_broken:
            if r['status'] != EV_WAITING:
                db.x("UPDATE evidences SET status='WAITING', reason=NULL WHERE pkg=? AND seq=?",
                     (p['id'], eseq))
            continue
        # 3) 整包级失效（许可单/边界版本/包超时）→ 带原因拒收，后续级联继承原因
        if pkg_fault:
            fault_text = pkg_fault[1]
            reject(eseq, fault_text)
            chain_broken = True
            saw_reject = True
            continue
        # 4) 证据级超时：任一证据晚于截止，整包进入 EXPIRED，该条及后续拒收
        if rec.get('ts', 0) > p['deadline']:
            reason = f"证据时间 {rec['ts']} 晚于授权截止 {p['deadline']}，授权超时拒收"
            fault_text = reason
            reject(eseq, reason)
            if p['status'] in PKG_LIVE_STATES:
                db.x('UPDATE off_pkgs SET status=?, void_reason=? WHERE id=?',
                     (PKG_EXPIRED, reason, p['id']))
                ev(db, p['ticket'], 'system', 'PKG_VOIDED', pkg=p['id'],
                    reason=reason, code='AUTH_EXPIRED')
                p['status'] = PKG_EXPIRED
            expired_by_evidence = True
            chain_broken = True
            saw_reject = True
            continue
        # 5) 核验父节点摘要（链指针存于行的 prev_hash/hash 列，首条父节点为 GENESIS）
        prev_hash = 'GENESIS' if eseq == 1 else stored[eseq - 1]['hash']
        parent = '' if prev_hash == 'GENESIS' else prev_hash
        if r['prev_hash'] != prev_hash or r['hash'] != evidence_hash(parent, evidence_payload(rec)):
            reject(eseq, '证据链摘要不连续：prev_hash 与父节点摘要不符（可能被篡改），该记录及后续全部驳回')
            chain_broken = True
            saw_reject = True
            continue
        # 6) 记录类型/归属必须落在授权段的唯一合法序列上
        if rec.get('type') != etype or rec.get('step') != step_seq or rec.get('pkg') != p['id']:
            reject(eseq, f"记录越权/越序：位置 {eseq} 应为 {etype}@step{step_seq}，唯一合法序列禁止跨越")
            chain_broken = True
            saw_reject = True
            continue
        # 7) 授权成员（执行者/监护者替换后旧片段拒收）
        want_actor = p['guardian'] if etype == domain.EVID_GUARD else p['executor']
        if etype in (domain.EVID_EXEC, domain.EVID_GUARD) and rec.get('actor') != want_actor:
            role = '监护者' if etype == domain.EVID_GUARD else '执行者'
            reject(eseq, f"{role}不符：授权{role}为 {want_actor}（成员替换，旧片段拒收）")
            chain_broken = True
            saw_reject = True
            continue

        sid = f"{p['ticket']}:{step_seq}"
        if etype == domain.EVID_SCAN:
            valid = {step['device'], step.get('point'), step.get('sw')} - {None}
            if rec.get('scanned') not in valid:
                reject(eseq, f"扫码设备 {rec.get('scanned')} 与步骤要求 {sorted(valid)} 不符")
                chain_broken = True
                saw_reject = True
                continue
        elif etype == domain.EVID_LIVE:
            if not live_record_ok(topo, p, step, rec):
                target = step['point'] if step['kind'] in (domain.INSTALL_GROUND, domain.REMOVE_GROUND) \
                    else step['device']
                reject(eseq, f"设备实况不符：{target} 现场实况与授权预期/主站遥测不一致")
                chain_broken = True
                saw_reject = True
                continue
        else:
            # EXEC / GUARD 推进工序；与在线竞争时由核心判先后，裁决补可追溯原因
            race_loss = False
            try:
                if etype == domain.EVID_EXEC:
                    scan_rec = json.loads(stored[pos_by_step[(step_seq, domain.EVID_SCAN)]]['payload'])
                    report_core(db, p['ticket'], sid, p['executor'], scan_rec['scanned'],
                                source='OFFLINE_PACKAGE')
                else:
                    verify_core(db, p['ticket'], sid, p['guardian'], source='OFFLINE_PACKAGE')
            except RuleError as e:
                reason = e.detail
                win = winner_event(db, p['ticket'], step_seq, etype)
                step_now = dict(db.one('SELECT * FROM steps WHERE id=?', (sid,)))
                # 步骤已被在线操作越过（回报/复核）：确定的竞争结果，包记录负但不阻断后续物理序列
                online_ahead = (
                    (etype == domain.EVID_EXEC and step_now['status'] in ('REPORTED', 'VERIFIED'))
                    or (etype == domain.EVID_GUARD and step_now['status'] == 'VERIFIED'))
                if win or online_ahead:
                    if win:
                        reason = (f"{RACE_MARK}{reason}｜竞争裁决：在线操作胜"
                                  f"（审计 #{win['seq']} {win['type']} by {win['actor']} 先提交），本包记录负；"
                                  f"工序保持唯一推进，后续授权步骤仍可继续")
                    else:
                        reason = (f"{RACE_MARK}{reason}｜竞争裁决：在线操作已先完成该步骤（先提交者胜），"
                                  f"本包记录负；工序保持唯一推进，后续授权步骤仍可继续")
                    race_loss = True
                reject(eseq, reason)
                if not race_loss:
                    chain_broken = True
                    saw_reject = True
                # 竞争负：步骤已被在线先推进，物理序列连续，后续记录照常裁决
                continue

        db.x("UPDATE evidences SET status='ACCEPTED', reason=NULL, applied_at=? WHERE pkg=? AND seq=?",
             (time.time(), p['id'], eseq))
        accepted_any = True
        ev(db, p['ticket'], rec.get('actor', want_actor), 'PKG_EVIDENCE_ACCEPTED',
           pkg=p['id'], seq=eseq, step=step_seq, evidence=etype)

    # 核心可能已推进步骤状态：重读后更新授权包聚合状态
    stored = {r['seq']: dict(r) for r in db.q('SELECT * FROM evidences WHERE pkg=?', (p['id'],))}
    steps_by_seq = {s['seq']: dict(s) for s in get_steps(db, p['ticket'])}
    cur = get_pkg(db, p['id'])
    if cur['status'] in PKG_LIVE_STATES:
        # 逐步骤判定闭环方式：包推进（EXEC+GUARD 采纳）/ 在线推进（竞争负且步骤 VERIFIED）/ 未闭环
        n_steps = p['seg_to'] - p['seg_from'] + 1
        by_pkg = by_online = 0
        closed_all = True
        for sseq in range(p['seg_from'], p['seg_to'] + 1):
            e_exec = pos_by_step[(sseq, domain.EVID_EXEC)]
            e_guard = pos_by_step[(sseq, domain.EVID_GUARD)]
            def _via(eseq):
                row = stored.get(eseq, {})
                return (row.get('status') == EV_REJECTED and (row.get('reason') or '').startswith(RACE_MARK)
                        and steps_by_seq[sseq]['status'] == 'VERIFIED')
            exec_pkg = stored.get(e_exec, {}).get('status') == EV_ACCEPTED
            guard_pkg = stored.get(e_guard, {}).get('status') == EV_ACCEPTED
            exec_online = _via(e_exec)
            guard_online = _via(e_guard)
            if exec_pkg and guard_pkg:
                by_pkg += 1
            elif (exec_pkg or exec_online) and (guard_pkg or guard_online) and steps_by_seq[sseq]['status'] == 'VERIFIED':
                by_online += 1
            else:
                closed_all = False
        if closed_all and by_pkg == 0:
            db.x('UPDATE off_pkgs SET status=?, void_reason=? WHERE id=?',
                 (PKG_SUPERSEDED, '段内步骤已全部由在线操作完成：在线与无网竞争中在线胜出（先提交者胜），本包零采纳', p['id']))
            ev(db, p['ticket'], 'system', 'PKG_SUPERSEDED', pkg=p['id'],
               reason='在线操作先完成全部段内步骤，本包零采纳')
        elif closed_all:
            db.x("UPDATE off_pkgs SET status='COMPLETED', void_reason=NULL WHERE id=?", (p['id'],))
            ev(db, p['ticket'], 'system', 'PKG_COMPLETED', pkg=p['id'],
               seg=[p['seg_from'], p['seg_to']], package_steps=by_pkg, online_won_steps=by_online)
        elif accepted_any:
            db.x("UPDATE off_pkgs SET status='PARTIAL' WHERE id=?", (p['id'],))
    return get_pkg(db, p['id'])


def live_record_ok(topo, p, step, rec):
    """设备实况记录核验：现场声称的位置须与授权预期一致，且与主站当前遥测交叉复核。"""
    if not domain.live_matches(step, rec.get('device_state'), rec.get('grounded'), rec.get('energized')):
        return False
    if step['kind'] in (domain.OPEN_SWITCH, domain.CLOSE_SWITCH):
        return topo.devices[step['device']]['state'] == rec.get('device_state') == step['expected']
    if step['kind'] == domain.INSTALL_GROUND:
        return ((step['point'], step['sw'], p['ticket']) in topo.grounds
                and step['point'] not in topo.energized()
                and bool(rec.get('grounded')) and not rec.get('energized'))
    if step['kind'] == domain.REMOVE_GROUND:
        return ((step['point'], step['sw'], p['ticket']) not in topo.grounds
                and not rec.get('grounded'))
    return False


def winner_event(db, tid, step_seq, etype):
    """竞争裁决依据：返回先于本包提交、使步骤越过当前位置的在线审计事件（可追溯的胜负原因）。"""
    want = 'STEP_VERIFIED' if etype == domain.EVID_GUARD else 'STEP_REPORTED'
    sid = f'{tid}:{step_seq}'
    for r in db.q("SELECT * FROM events WHERE ticket=? AND type=? ORDER BY seq DESC", (tid, want)):
        payload = json.loads(r['payload'] or '{}')
        if payload.get('step') == sid and payload.get('source', 'ONLINE') == 'ONLINE':
            d = dict(r)
            d['payload_data'] = payload
            return d
    return None


def pkg_view(db, p):
    p = dict(p) if not isinstance(p, dict) else p
    recs = get_evidences(db, p['id'])
    steps_by_seq = {s['seq']: s for s in get_steps(db, p['ticket'])}
    total = (p['seg_to'] - p['seg_from'] + 1) * 4
    by_status = {EV_PENDING: 0, EV_WAITING: 0, EV_ACCEPTED: 0, EV_REJECTED: 0, EV_DUP_CONFLICT: 0}
    first_gap = None
    cursor = 0
    race_loss = 0
    by_seq = {r['seq']: r for r in recs}
    step_of = {}
    for step_seq, m in pkg_step_indexes(p):
        for etype, eseq in m.items():
            step_of[eseq] = step_seq
    chain_ok = True
    for i in range(1, total + 1):
        r = by_seq.get(i)
        if not r:
            by_status[EV_PENDING] += 1
            if first_gap is None:
                first_gap = i
            chain_ok = False
            continue
        st = r['status']
        by_status[st] = by_status.get(st, 0) + 1
        is_race_closed = (st == EV_REJECTED and (r['reason'] or '').startswith(RACE_MARK)
                          and steps_by_seq[step_of[i]]['status'] == 'VERIFIED')
        if st == EV_WAITING or st == EV_DUP_CONFLICT:
            chain_ok = False
        elif st == EV_REJECTED and not is_race_closed:
            chain_ok = False
        if st == EV_ACCEPTED or is_race_closed:
            # 游标只在"此前无缺口/未决"时连续推进
            if chain_ok:
                cursor = i
        elif st == EV_WAITING:
            chain_ok = False
        if is_race_closed:
            race_loss += 1
    # 存在待补前序或缺口时游标停在最后一条连续闭环
    cont_cursor = 0
    for i in range(1, total + 1):
        r = by_seq.get(i)
        if not r:
            break
        is_race_closed = (r['status'] == EV_REJECTED and (r['reason'] or '').startswith(RACE_MARK)
                          and steps_by_seq[step_of[i]]['status'] == 'VERIFIED')
        if r['status'] == EV_ACCEPTED or is_race_closed:
            cont_cursor = i
        else:
            break
    steps = [s for s in get_steps(db, p['ticket'])
             if p['seg_from'] <= s['seq'] <= p['seg_to']]
    records = []
    for r in recs:
        d = dict(r)
        d['payload'] = json.loads(d['payload'])
        records.append(d)
    return {**p, 'records_total': total, 'by_status': by_status,
            'chain_continuous': chain_ok, 'first_gap': first_gap,
            'sync_cursor': cont_cursor, 'race_lost': race_loss,
            'steps': [{'seq': s['seq'], 'id': s['id'], 'kind': s['kind'],
                       'device': s['device'], 'status': s['status'], 'desc': s['desc']} for s in steps],
            'records': records}


def telemetry_holds(topo, ticket, step):
    """回报/复核时核对真实遥测，物理条件不满足则拒绝——不能伪造物理步骤。"""
    if step['kind'] == domain.OPEN_SWITCH:
        return topo.devices[step['device']]['state'] == 'OPEN'
    if step['kind'] == domain.CLOSE_SWITCH:
        return topo.devices[step['device']]['state'] == 'CLOSED'
    if step['kind'] == domain.INSTALL_GROUND:
        g = (step['point'], step['sw'], ticket['id'])
        return g in topo.grounds and step['point'] not in topo.energized()
    if step['kind'] == domain.REMOVE_GROUND:
        return (step['point'], step['sw'], ticket['id']) not in topo.grounds
    return False


def pkg_allows_switch(db, pkg_id, ticket_id, device, state):
    """断网物理操作：有效授权包覆盖该开关步骤，且段内严格有序（前序已证实到位）。"""
    try:
        p = get_pkg(db, pkg_id)
    except RuleError:
        return False, f'授权包 {pkg_id} 不存在或已失效'
    if p['ticket'] != ticket_id or p['status'] not in PKG_LIVE_STATES or p['snapshot'] != int(db.meta('topo_version')):
        return False, '授权包无效/已作废或边界版本不符'
    if int(time.time() * 1000) > p['deadline']:
        return False, '授权已超时'
    steps = get_steps(db, ticket_id)
    by_seq = {s['seq']: s for s in steps}
    for seq in range(p['seg_from'], p['seg_to'] + 1):
        s = by_seq[seq]
        if s['kind'] in (domain.OPEN_SWITCH, domain.CLOSE_SWITCH) and s['device'] == device \
                and s['expected'] == state:
            prior = [by_seq[x] for x in range(p['seg_from'], seq)]
            if all(x['status'] == 'VERIFIED' for x in prior):
                return True, None
            return False, '授权段内前序步骤尚未证实，禁止跨越操作'
    return False, '该物理操作不在授权包范围内'


def pkg_allows_ground(db, pkg_id, ticket_id, point, sw, action):
    """断网物理操作：有效授权包覆盖该接地步骤，且段内严格有序。"""
    kind = domain.INSTALL_GROUND if action == 'install' else domain.REMOVE_GROUND
    try:
        p = get_pkg(db, pkg_id)
    except RuleError:
        return False, f'授权包 {pkg_id} 不存在或已失效'
    if p['ticket'] != ticket_id or p['status'] not in PKG_LIVE_STATES or p['snapshot'] != int(db.meta('topo_version')):
        return False, '授权包无效/已作废或边界版本不符'
    if int(time.time() * 1000) > p['deadline']:
        return False, '授权已超时'
    steps = get_steps(db, ticket_id)
    by_seq = {s['seq']: s for s in steps}
    for seq in range(p['seg_from'], p['seg_to'] + 1):
        s = by_seq[seq]
        if s['kind'] == kind and s['sw'] == sw and s['point'] == point:
            prior = [by_seq[x] for x in range(p['seg_from'], seq)]
            if all(x['status'] == 'VERIFIED' for x in prior):
                return True, None
            return False, '授权段内前序步骤尚未证实，禁止跨越操作'
    return False, '该物理操作不在授权包范围内'


# ---------------------------------------------------------------- 值守班组换班接续
#
# 调度席为在执行工作票开具交班单（切换水位/移交方/候选承接方/签收期限）。
# 签收之前写权仍归移交方；签收在同一事务内 CAS 交班单状态并改写票据 write_owner，
# 原子把写权转给承接方；任何时刻一张工作票只允许一个班组追加现场证据。
# 归属按切换水位划分：水位线内移交方迟到内容仍可入账，越线后移交方内容拒收；
# 承接方首条内容必须声明接续基点接上旧链，接不上只能挂起，修正基点重传后唤醒。
# 工作票锁定、边界重算、计划取消、交班单超时关闭未完成的换班——
# 只关闭交班单与未决证据，既成设备动作、控制锁与已入账证据绝不回退。

HO = domain                       # 交班单/证据状态常量别名
SHIFT_RECORD_FIELDS = ('ticket', 'team', 'device', 'kind', 'ts_data', 'payload',
                       'client_id', 'seq')     # 计入证据摘要的字段


def chain_head(db, tid):
    """当前已入账证据链头：(seq, hash)，空链为 (0, 'GENESIS')。"""
    r = db.one("SELECT seq, hash FROM shift_evidences WHERE ticket=? AND status='ACCEPTED' "
               "ORDER BY seq DESC LIMIT 1", (tid,))
    return (r['seq'], r['hash']) if r else (0, 'GENESIS')


def get_handover(db, hid):
    r = db.one('SELECT * FROM handovers WHERE id=?', (hid,))
    if not r:
        raise RuleError(404, 'HANDOVER_NOT_FOUND', f'交班单 {hid} 不存在')
    return dict(r)


def relevant_handover(db, tid):
    """仍参与水位归属划分的交班单（待签收/已签收/已接续完成中最新的一张）。"""
    r = db.one("SELECT * FROM handovers WHERE ticket=? AND status IN ('PENDING','SIGNED','COMPLETED') "
               "ORDER BY created_at DESC LIMIT 1", (tid,))
    return handover_view(dict(r)) if r else None


def closed_hint(db, tid):
    r = db.one("SELECT * FROM handovers WHERE ticket=? AND status='CLOSED' "
               "ORDER BY closed_at DESC LIMIT 1", (tid,))
    if not r:
        return None
    return f"交班单 {r['id']} 已关闭（{r['close_reason']}）"


def handover_view(h):
    h = dict(h)
    h['candidates'] = json.loads(h['candidates']) if isinstance(h['candidates'], str) else h['candidates']
    return h


def judge_and_apply(db, t, row):
    """按当前票据/交班单状态裁决一条证据并落库；入账即入链。调用方必须持 db.lock。"""
    tid = t['id']
    h = relevant_handover(db, tid)
    head_seq, head_hash = chain_head(db, tid)
    on_chain = db.one("SELECT 1 x FROM shift_evidences WHERE ticket=? AND team=? AND status='ACCEPTED' "
                      "LIMIT 1", (tid, row['team']))
    status, reason, late = domain.decide(
        ticket_active=t['status'] in ACTIVE_STATUS, frozen=bool(t['frozen']),
        write_owner=t['write_owner'], handover=h, closed_hint=closed_hint(db, tid),
        team=row['team'], ts_data=row['ts_data'], base_seq=row['base_seq'],
        head_seq=head_seq, team_on_chain=bool(on_chain))
    hid = h['id'] if h else None
    if status == domain.SE_ACCEPTED:
        seq = head_seq + 1
        payload = {k: (seq if k == 'seq' else row[k]) for k in SHIFT_RECORD_FIELDS}
        digest = evidence_hash('' if head_hash == 'GENESIS' else head_hash, payload)
        db.x('UPDATE shift_evidences SET status=?, reason=?, late=?, seq=?, prev_hash=?, hash=?, handover=? '
             'WHERE id=?', (status, reason, int(late), seq, head_hash, digest, hid, row['id']))
        row.update(seq=seq, hash=digest)
        # 承接方首条越水位内容入账 → 换班接续完成
        if h and h['status'] == domain.HO_SIGNED and row['team'] == h['signer']:
            db.x("UPDATE handovers SET status='COMPLETED', version=version+1 WHERE id=?", (h['id'],))
            ev(db, tid, row['team'], 'HANDOVER_COMPLETED', handover=h['id'],
               note=f"承接方 {row['team']} 首条内容入账（链 #{seq}），换班接续完成")
    else:
        db.x('UPDATE shift_evidences SET status=?, reason=?, late=?, handover=? WHERE id=?',
             (status, reason, int(late), hid, row['id']))
    row.update(status=status, reason=reason, late=int(late), handover=hid)
    return row


def reject_row(db, row, reason):
    db.x("UPDATE shift_evidences SET status='REJECTED', reason=? WHERE id=?", (reason, row['id']))
    row.update(status=domain.SE_REJECTED, reason=reason)


def close_handovers(db, tid, code, reason, actor):
    """关闭票据上尚未完成的换班（待签收/已签收待接续）。
    只关闭交班单与未决证据：既成设备动作、控制权锁、已入账证据绝不回退。"""
    for r in db.q(f"SELECT * FROM handovers WHERE ticket=? AND status IN {tuple(domain.HO_OPEN_STATES)!r}",
                  (tid,)):
        h = dict(r)
        db.x("UPDATE handovers SET status='CLOSED', close_reason=?, closed_at=?, version=version+1 "
             'WHERE id=?', (reason, time.time(), h['id']))
        ev(db, tid, actor, 'HANDOVER_CLOSED', handover=h['id'], code=code, reason=reason,
           note='既成设备动作、控制权锁与已入账证据保持原样，不回退')
        if h['status'] == domain.HO_PENDING:
            # 写权从未转移：等待签收而挂起的承接方首批内容终局拒收
            for row in db.q("SELECT * FROM shift_evidences WHERE handover=? AND status='SUSPENDED'",
                            (h['id'],)):
                reject_row(db, dict(row), f'交班单已关闭（{reason}），写权未转移，挂起内容拒收')


def sweep_handovers(db):
    """签收期限扫描：超期未签收的交班单按超时关闭。调用方必须持 db.lock。"""
    now_ms = int(time.time() * 1000)
    for r in db.q("SELECT * FROM handovers WHERE status='PENDING' AND deadline<?", (now_ms,)):
        close_handovers(db, r['ticket'], 'TIMEOUT', '签收期限已过，交班单超时关闭', 'system')


def sign_core(db, hid, team):
    """签收核心：同一事务内 CAS 交班单并改写票据写权班组，原子转移写权。
    两个候选班组争抢签收时，只有条件更新生效的一方取得写权。调用方负责加锁/提交。"""
    sweep_handovers(db)
    h = get_handover(db, hid)
    if h['status'] in (domain.HO_SIGNED, domain.HO_COMPLETED):
        if h['signer'] == team:
            return {'ok': True, 'duplicate': True, 'handover': handover_view(h),
                    'note': '重复签收：返回首次签收结果，写权归属不变'}
        raise RuleError(409, 'ALREADY_SIGNED',
                        f"交班单已由 {h['signer']} 签收，写权已转移，本班组未中签")
    if h['status'] == domain.HO_CLOSED:
        raise RuleError(409, 'HANDOVER_CLOSED', f"交班单已关闭（{h['close_reason']}），签收失败")
    cands = json.loads(h['candidates'])
    if team not in cands:
        raise RuleError(403, 'NOT_CANDIDATE', f'{team} 不在候选承接班组 {cands} 内')
    t = get_ticket(db, h['ticket'])
    if t['status'] not in ACTIVE_STATUS:
        raise RuleError(409, 'TICKET_INACTIVE', f"票据状态 {t['status']}，签收失败")
    if t['frozen']:
        raise RuleError(423, 'TICKET_LOCKED', f"工作票锁定中（{t['freeze_reason']}），签收失败")
    if int(time.time() * 1000) > h['deadline']:
        close_handovers(db, h['ticket'], 'TIMEOUT', '签收期限已过，交班单超时关闭', 'system')
        raise RuleError(409, 'HANDOVER_TIMEOUT', '签收期限已过，交班单已超时关闭')
    # CAS：只有仍处 PENDING 才能签收成功——进程内由 db.lock 串行化，条件更新兜底跨进程争抢
    cur = db.conn.execute("UPDATE handovers SET status='SIGNED', signer=?, signed_at=?, version=version+1 "
                          'WHERE id=? AND status=?', (team, time.time(), hid, domain.HO_PENDING))
    if cur.rowcount == 0:
        now_h = get_handover(db, hid)
        raise RuleError(409, 'SIGN_RACE_LOST',
                        f"另一候选班组 {now_h['signer']} 已先签收，写权已原子转移，本班组未中签")
    # 同一事务内改写票据写权班组：签收动作把写权原子转给承接方
    db.x('UPDATE tickets SET write_owner=? WHERE id=?', (team, h['ticket']))
    ev(db, h['ticket'], team, 'HANDOVER_SIGNED', handover=hid, from_team=h['from_team'],
       to_team=team, note='写权已原子转移给承接方；移交方仅保留水位线内迟到入账权')
    # 唤醒挂起内容：中签者按当前链头重新裁决；未中签候选的首批内容终局拒收
    t = get_ticket(db, h['ticket'])
    for r in db.q("SELECT * FROM shift_evidences WHERE ticket=? AND status='SUSPENDED' ORDER BY id",
                  (h['ticket'],)):
        row = dict(r)
        if row['team'] == team:
            row = judge_and_apply(db, t, row)
        else:
            reject_row(db, row, f'承接权已由 {team} 签收取得，本班组未获写权，首批内容拒收')
        ev(db, h['ticket'], row['team'], 'SHIFT_EVIDENCE_WOKEN', id=row['id'],
           status=row['status'], reason=row['reason'])
    return {'ok': True, 'duplicate': False, 'handover': handover_view(get_handover(db, hid))}


def shift_view(db, tid):
    """调度席/作业设备共用视图：当前班组、切换水位、交班单阶段、双方数据进度、逐条处理原因。"""
    t = get_ticket(db, tid)
    head_seq, head_hash = chain_head(db, tid)
    hs = [handover_view(dict(r))
          for r in db.q('SELECT * FROM handovers WHERE ticket=? ORDER BY created_at', (tid,))]
    evs = [dict(r) for r in db.q('SELECT * FROM shift_evidences WHERE ticket=? ORDER BY id', (tid,))]
    progress = {}
    for e in evs:
        p = progress.setdefault(e['team'], {'accepted': 0, 'late': 0, 'suspended': 0,
                                            'rejected': 0, 'last_ts': None, 'last_seq': None})
        if e['status'] == domain.SE_ACCEPTED:
            p['accepted'] += 1
            p['late'] += int(e['late'])
            p['last_seq'] = e['seq']
        elif e['status'] == domain.SE_SUSPENDED:
            p['suspended'] += 1
        else:
            p['rejected'] += 1
        p['last_ts'] = e['ts_data'] if p['last_ts'] is None else max(p['last_ts'], e['ts_data'])
    return {'ticket': {'id': t['id'], 'title': t['title'], 'status': t['status'],
                       'frozen': bool(t['frozen']), 'freeze_reason': t['freeze_reason'],
                       'write_owner': t['write_owner']},
            'chain_head': {'seq': head_seq, 'hash': head_hash},
            'handover': relevant_handover(db, tid),
            'handovers': hs,
            'progress': progress,
            'evidences': evs,
            'now': int(time.time() * 1000)}


# ---------------------------------------------------------------- 审计事件回放与事故分析
#
# 分析员选取安全票的一段审计序号构建回放：系统从事件记录重建该时点的票据阶段、
# 设备控制权与现场证据，逐条显示事件前后状态。回放创建即固定事件上界与来源摘要，
# 后续新增事件（序号必然越过上界）改变不了既有回放；序号缺口、摘要校验失败、
# 事件载荷无法应用都会使回放停在最后一个有效事件并指出具体位置。
# 假设分支从任一有效事件出发试算撤销/设备状态变更，只读正式事件、绝不写回正式
# 票据或追加正式审计事件；多人编辑同一分支以 version 做乐观并发校验。
# 回放游标、假设分支、校验失败位置全部事务化持久化，重启后完整恢复。

def events_upto(db, to_seq):
    return [dict(r) for r in db.q('SELECT * FROM events WHERE seq<=? ORDER BY seq', (to_seq,))]


def get_replay(db, rid):
    r = db.one('SELECT * FROM replays WHERE id=?', (rid,))
    if not r:
        raise RuleError(404, 'REPLAY_NOT_FOUND', f'回放 {rid} 不存在')
    return dict(r)


def replay_view(row):
    d = dict(row)
    for k in ('baseline', 'entries', 'tip', 'applied'):
        d[k] = json.loads(d[k]) if d[k] else ([] if k in ('entries', 'applied') else None)
    d['halt'] = json.loads(d['halt']) if d['halt'] else None
    d['pinned_count'] = len(json.loads(row['pinned'])) if row['pinned'] else 0
    d.pop('pinned', None)                       # 固定来源明细不随视图下发（摘要在 source_digest）
    return d


def store_replay(db, rid, tid, from_seq, to_seq, result, created_by):
    db.x('INSERT INTO replays VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
         (rid, tid, from_seq, to_seq, result['source_digest'],
          json.dumps(result['pinned'], ensure_ascii=False),
          json.dumps(result['baseline'], ensure_ascii=False),
          json.dumps(result['entries'], ensure_ascii=False),
          json.dumps(result['tip'], ensure_ascii=False),
          json.dumps(result['applied'], ensure_ascii=False),
          result['status'],
          json.dumps(result['halt'], ensure_ascii=False) if result['halt'] else None,
          created_by, time.time()))


def update_replay(db, rid, result):
    """重新核验后更新回放游标与校验失败位置（固定来源摘要不变）。"""
    db.x('UPDATE replays SET baseline=?, entries=?, tip=?, applied=?, status=?, halt=? '
         'WHERE id=?',
         (json.dumps(result['baseline'], ensure_ascii=False),
          json.dumps(result['entries'], ensure_ascii=False),
          json.dumps(result['tip'], ensure_ascii=False),
          json.dumps(result['applied'], ensure_ascii=False),
          result['status'],
          json.dumps(result['halt'], ensure_ascii=False) if result['halt'] else None,
          rid))


def get_branch(db, bid):
    r = db.one('SELECT * FROM branches WHERE id=?', (bid,))
    if not r:
        raise RuleError(404, 'BRANCH_NOT_FOUND', f'假设分支 {bid} 不存在')
    return dict(r)


def branch_ops(db, bid):
    ops = []
    for r in db.q('SELECT * FROM branch_ops WHERE branch=? ORDER BY ord', (bid,)):
        d = dict(r)
        d['params'] = json.loads(d['params'])
        ops.append(d)
    return ops


def branch_view(db, b):
    """分支视图：在回放有效事件流上试算假设，比较对后续控制权与工序状态的影响。"""
    b = dict(b)
    ops = branch_ops(db, b['id'])
    rp = get_replay(db, b['replay'])
    official = json.loads(rp['tip']) if rp['tip'] else None
    applied = json.loads(rp['applied']) if rp['applied'] else []
    events = [dict(r) for r in db.q(
        'SELECT * FROM events WHERE ticket=? AND seq<=? ORDER BY seq',
        (b['ticket'], rp['to_seq']))]
    hypo = replay_engine.run_hypothetical(events, b['base_seq'], ops, applied)
    diff = replay_engine.diff_projection(official, hypo) if official else []
    # 安全评估：假设设备状态下工作区是否复电（基于当前拓扑连接关系）
    topo = load_topo(db)
    t = get_ticket(db, b['ticket'])
    for dev, st in hypo['devices'].items():
        if dev in topo.devices and topo.devices[dev]['kind'] == 'switch':
            topo.devices[dev]['state'] = st
    en = topo.energized()
    hot = sorted(w for w in t['work_area'] if w in en)
    b.update({'ops': ops, 'replay_status': rp['status'], 'replay_halt':
              json.loads(rp['halt']) if rp['halt'] else None,
              'official_tip': official, 'hypothetical_tip': hypo, 'diff': diff,
              'safety': {'work_area_energized': hot,
                         'note': ('假设状态下工作区恢复带电：' + '、'.join(hot)
                                  if hot else '假设状态下工作区无电')}})
    return b


def report_content(db, rp, title, branch_ids, created_by):
    """报告内容（确定性，不含时间戳）：固定来源摘要、事件范围、停止位置与分支差异。"""
    branches = []
    for bid in branch_ids:
        b = get_branch(db, bid)
        if b['replay'] != rp['id']:
            raise RuleError(400, 'BRANCH_NOT_IN_REPLAY',
                            f"分支 {bid} 不属于回放 {rp['id']}，不能并入同一报告")
        v = branch_view(db, b)
        branches.append({'id': b['id'], 'name': b['name'], 'base_seq': b['base_seq'],
                         'version': b['version'], 'ops': v['ops'], 'diff': v['diff'],
                         'safety': v['safety']})
    return {'title': title, 'ticket': rp['ticket'], 'replay': rp['id'],
            'source_digest': rp['source_digest'],
            'event_range': {'from_seq': rp['from_seq'], 'to_seq': rp['to_seq']},
            'replay_status': rp['status'],
            'halt': json.loads(rp['halt']) if rp['halt'] else None,
            'generated_by': created_by, 'branches': branches}


def report_hash_of(content):
    return hashlib.sha256(canonical(content).encode()).hexdigest()[:16]


def report_view(row):
    d = dict(row)
    d['content'] = json.loads(d['content'])
    return d


def get_report(db, rid):
    r = db.one('SELECT * FROM reports WHERE id=?', (rid,))
    if not r:
        raise RuleError(404, 'REPORT_NOT_FOUND', f'报告 {rid} 不存在')
    return dict(r)


# ---------------------------------------------------------------- 审计资料外发凭证柜
#
# 分析员从一次已完成的归约结果（回放）中选取连续审计序号封存只读快照：
# 封存内容含封存摘要、状态截面与逐条事件前后记录，创建时整体固化于 vouchers.snapshot，
# 此后正式审计库新增事件（序号必然越过上界）或被篡改都不污染已发出的截面。
# 凭证带截止时刻（懒扫描超时）与主动作废标记；超时/作废后查阅返回确定原因。
# 多个接收人同时首次领取时，凭证领取记号以 (凭证,接收人) 唯一约束原子抢占，只有一人成功；
# 领取记号、凭证阶段与作废结果全部事务化持久化，重启后完整恢复。

def get_voucher(db, vid):
    r = db.one('SELECT * FROM vouchers WHERE id=?', (vid,))
    if not r:
        raise RuleError(404, 'VOUCHER_NOT_FOUND', f'外发凭证 {vid} 不存在')
    return dict(r)


def voucher_expiry_text(v):
    dl = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(v['deadline'] / 1000))
    return f'凭证已超过截止时刻（{dl}），停止外发查阅'


def sweep_vouchers(db, now_ms=None):
    """截止时刻扫描：把到期的未作废凭证置为 EXPIRED 并记录原因。调用方必须持 db.lock。"""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    for r in db.q("SELECT * FROM vouchers WHERE status IN ('SEALED','CLAIMED') AND deadline<?",
                  (now_ms,)):
        v = dict(r)
        reason = voucher_expiry_text(v)
        db.x("UPDATE vouchers SET status='EXPIRED', invalid_reason=?, expired_at=? WHERE id=?",
             (reason, time.time(), v['id']))
        ev(db, v['ticket'], 'system', 'VOUCHER_EXPIRED', voucher=v['id'],
           deadline=v['deadline'], reason=reason)


def voucher_seal_digest(content):
    """封存摘要：对只读快照的来源摘要、序号范围、状态截面、逐条前后记录整体取 SHA256。
    只覆盖封存内容字段（不含领取人/截止时刻/阶段等外发管理信息），保证同一归约同一序号段
    封存出的内容摘要一致——同一凭证不可能有两份内容。"""
    return hashlib.sha256(canonical(content).encode()).hexdigest()[:16]


def voucher_nat_key(replay_id, from_seq, to_seq, receivers):
    return hashlib.sha256(canonical(
        {'replay': replay_id, 'from_seq': from_seq, 'to_seq': to_seq,
         'receivers': sorted(receivers)}).encode()).hexdigest()[:16]


def build_voucher_snapshot(rp, from_seq, to_seq):
    """从一次已完成的归约结果截取连续序号，构建只读快照（封存时刻的一份深拷贝）。
    状态截面取窗口末条事件应用后的投影（即末条 entry 的 after）；逐条记录带前后状态。
    rp 为 replay_view（entries/tip 已解析）。"""
    window = [e for e in rp['entries'] if from_seq <= e['seq'] <= to_seq]
    state_section = window[-1]['after'] if window else rp['tip']
    return {
        'replay': rp['id'],
        'ticket': rp['ticket'],
        'from_seq': from_seq,
        'to_seq': to_seq,
        'replay_from_seq': rp['from_seq'],
        'replay_to_seq': rp['to_seq'],
        'source_digest': rp['source_digest'],
        'state_section': state_section,
        'records': window,
        'read_only': True,
    }


def voucher_view(db, vid, include_snapshot=False):
    """凭证柜视图：阶段、封存摘要、序号范围、领取人、失效原因、领取记号。"""
    v = get_voucher(db, vid)
    claims = [dict(r) for r in db.q('SELECT * FROM voucher_claims WHERE voucher=? ORDER BY claimed_at',
                                    (vid,))]
    winner = next((c['receiver'] for c in claims if c['result'] == CLM_WON), None)
    out = {k: v[k] for k in
           ('id', 'ticket', 'replay', 'from_seq', 'to_seq', 'source_digest', 'seal_digest',
            'deadline', 'status', 'invalid_reason', 'created_by', 'created_at',
            'sealed_at', 'expired_at', 'voided_by', 'voided_at')}
    out['receivers'] = json.loads(v['receivers'])
    out['claimer'] = winner
    out['claims'] = claims
    out['record_count'] = v['to_seq'] - v['from_seq'] + 1
    if include_snapshot:
        snap = json.loads(v['snapshot'])
        out['snapshot'] = snap
        # 完整性自检：固化内容必须仍与封存摘要一致（同一凭证只有一份内容）
        out['seal_intact'] = voucher_seal_digest(snap) == v['seal_digest']
    return out


def create_voucher_core(db, replay_id, from_seq, to_seq, receivers, deadline,
                        idem_key, created_by):
    """封存凭证核心。调用方负责加锁/提交。重复创建（幂等键或同归约+同序号段+同接收人）
    返回首次凭证，绝不产生第二份内容。"""
    rp = get_replay(db, replay_id)
    if rp['status'] != 'OK':
        detail = '归约结果不完整（存在停止位置），不能据此外发凭证'
        if rp['halt']:
            h = json.loads(rp['halt'])
            detail += f"：{h['detail']}（停于 #{h['at_seq']}）"
        raise RuleError(409, 'REPLAY_NOT_COMPLETE', detail)
    if from_seq < 1 or to_seq < from_seq:
        raise RuleError(400, 'BAD_RANGE', '凭证序号区间非法：1 ≤ from_seq ≤ to_seq')
    applied = json.loads(rp['applied'])
    # 选取的连续序号必须全部落在归约窗口内、且全部是已成功应用的本票据事件
    if from_seq < rp['from_seq'] or to_seq > rp['to_seq']:
        raise RuleError(400, 'RANGE_OUT_OF_REPLAY',
                        f'凭证序号段 #{from_seq}~#{to_seq} 超出归约窗口 '
                        f'#{rp["from_seq"]}~#{rp["to_seq"]}')
    bad = [s for s in range(from_seq, to_seq + 1) if s not in applied]
    if bad:
        raise RuleError(400, 'RANGE_NOT_APPLIED',
                        f'序号段内 #{bad[0]} 不是归约中已成功应用的本票据事件'
                        '（非本票据事件仅作来源固定，不能外发为逐条记录）')
    recv = [r.strip() for r in receivers if r and r.strip()]
    if not recv:
        raise RuleError(400, 'NO_RECEIVERS', '必须指定至少一个接收人')
    if len(set(recv)) != len(recv):
        raise RuleError(400, 'DUP_RECEIVERS', '接收人重复')
    if deadline <= int(time.time() * 1000):
        raise RuleError(400, 'BAD_DEADLINE', '截止时刻必须晚于当前时间')
    # 幂等：同一幂等键 → 首次凭证
    if idem_key:
        old = db.one('SELECT * FROM vouchers WHERE idem_key=?', (idem_key,))
        if old:
            view = voucher_view(db, old['id'])
            view['duplicate'] = True
            view['duplicate_reason'] = '幂等键命中：返回首次封存的同一份凭证'
            return view
    # 幂等：同一归约 + 同一连续序号段 + 同一接收人集合 → 同一份内容，不允许两份凭证
    nat = voucher_nat_key(replay_id, from_seq, to_seq, recv)
    old = db.one('SELECT * FROM vouchers WHERE nat_key=?', (nat,))
    if old:
        view = voucher_view(db, old['id'])
        view['duplicate'] = True
        view['duplicate_reason'] = '同归约同序号段同接收人已封存：返回首次凭证，同一内容不出两份'
        return view
    rp_view = replay_view(rp)
    snapshot = build_voucher_snapshot(rp_view, from_seq, to_seq)
    seal = voucher_seal_digest(snapshot)
    vid = 'V' + time.strftime('%H%M%S') + uuid.uuid4().hex[:6].upper()
    now = time.time()
    db.x('INSERT INTO vouchers(id,ticket,replay,from_seq,to_seq,source_digest,seal_digest,'
         'receivers,deadline,status,invalid_reason,idem_key,nat_key,created_by,created_at,'
         'sealed_at,expired_at,voided_by,voided_at,snapshot) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
         (vid, rp['ticket'], replay_id, from_seq, to_seq, rp['source_digest'], seal,
          json.dumps(recv, ensure_ascii=False), deadline, VCH_SEALED, None, idem_key, nat,
          created_by, now, now, None, None, None,
          json.dumps(snapshot, ensure_ascii=False)))
    ev(db, rp['ticket'], created_by, 'VOUCHER_SEALED', voucher=vid, replay=replay_id,
       from_seq=from_seq, to_seq=to_seq, source_digest=rp['source_digest'],
       seal_digest=seal, receivers=recv, deadline=deadline)
    return voucher_view(db, vid)


def claim_voucher_core(db, vid, receiver):
    """首次领取核心：凭证领取记号以 (凭证,接收人) 唯一约束原子抢占。
    多个接收人同时首次领取，只有 INSERT 成功的一方中签；负方收到确定失败原因，
    其领取记号同样落库（重启后可恢复）。同一接收人重复请求幂等返回首次结果。"""
    sweep_vouchers(db)
    v = get_voucher(db, vid)
    if v['status'] == VCH_EXPIRED:
        raise RuleError(410, 'VOUCHER_EXPIRED', v['invalid_reason'] or voucher_expiry_text(v))
    if v['status'] == VCH_VOID:
        raise RuleError(410, 'VOUCHER_VOID',
                        f"凭证已主动作废：{v['invalid_reason']}（快照封存不抹除，仅禁止查阅）")
    receivers = json.loads(v['receivers'])
    if receiver not in receivers:
        raise RuleError(403, 'NOT_RECEIVER',
                        f'{receiver} 不在凭证 {vid} 的接收人名单 {receivers} 内，无权领取')
    mine = db.one('SELECT * FROM voucher_claims WHERE voucher=? AND receiver=?', (vid, receiver))
    if mine:
        # 重复请求：返回本人首次领取的确定结果（中签者幂等成功，负方幂等失败）
        if mine['result'] == CLM_WON:
            return {'ok': True, 'duplicate': True, 'voucher': voucher_view(db, vid),
                    'note': '重复领取：返回首次领取结果，凭证阶段与快照内容不变'}
        raise RuleError(409, 'CLAIM_RACE_LOST', mine['reason'])
    now = time.time()
    # 原子抢占：部分唯一索引 idx_voucher_winner 保证并发下只有一条 WON 领取记号能落库
    cur = db.conn.execute(
        "INSERT OR IGNORE INTO voucher_claims(voucher,receiver,result,reason,claimed_at) "
        "VALUES(?,?,?,?,?)",
        (vid, receiver, CLM_WON, None, now))
    if cur.rowcount == 0:
        # rowcount==0 只可能是并发下已有中签者（本人重复在上方已短路）：落本接收人的失败记号
        winner = db.one("SELECT receiver FROM voucher_claims WHERE voucher=? AND result='WON'",
                        (vid,))['receiver']
        reason = (f'并发领取：凭证 {vid} 已由接收人 {winner} 首次领取成功（先到者得），'
                  f'{receiver} 未中签；正式数据不受影响')
        db.x('INSERT OR IGNORE INTO voucher_claims(voucher,receiver,result,reason,claimed_at) '
             'VALUES(?,?,?,?,?)', (vid, receiver, CLM_LOST, reason, time.time()))
        ev(db, v['ticket'], receiver, 'VOUCHER_CLAIM_RACE', voucher=vid,
            receiver=receiver, winner=winner, reason=reason)
        raise RuleError(409, 'CLAIM_RACE_LOST', reason)
    db.x("UPDATE vouchers SET status='CLAIMED' WHERE id=? AND status='SEALED'", (vid,))
    ev(db, v['ticket'], receiver, 'VOUCHER_CLAIMED', voucher=vid, receiver=receiver,
       note='首次领取成功，凭证对中签接收人开放只读查阅')
    return {'ok': True, 'duplicate': False, 'voucher': voucher_view(db, vid)}


def voucher_content_core(db, vid, receiver):
    """查阅凭证只读快照。只有中签接收人能查阅；超时/作废返回确定原因。
    内容直接读取封存时固化的 snapshot——不重新归约、不读正式事件流，
    封存后正式库追加任何记录都不会改变已发出的截面。"""
    sweep_vouchers(db)
    v = get_voucher(db, vid)
    if v['status'] == VCH_EXPIRED:
        raise RuleError(410, 'VOUCHER_EXPIRED', v['invalid_reason'] or voucher_expiry_text(v))
    if v['status'] == VCH_VOID:
        raise RuleError(410, 'VOUCHER_VOID',
                        f"凭证已主动作废：{v['invalid_reason']}（只读快照封存保留，停止查阅）")
    claim = db.one("SELECT * FROM voucher_claims WHERE voucher=? AND receiver=? AND result='WON'",
                   (vid, receiver))
    if not claim:
        if receiver not in json.loads(v['receivers']):
            raise RuleError(403, 'NOT_RECEIVER', f'{receiver} 不是凭证接收人，无权查阅')
        raise RuleError(409, 'NOT_CLAIMED',
                        f'{receiver} 尚未领取凭证 {vid}（或并发领取未中签），不能查阅')
    snap = json.loads(v['snapshot'])
    seal_ok = voucher_seal_digest(snap) == v['seal_digest']
    return {'voucher_id': vid, 'viewer': receiver, 'read_only': True,
            'status': v['status'], 'seal_digest': v['seal_digest'],
            'seal_intact': seal_ok,
            'note': '只读快照：封存后正式审计库追加/篡改记录均不改变本截面，且不能改写正式数据',
            'snapshot': snap}


def void_voucher_core(db, vid, reason, actor):
    """主动作废凭证：快照封存保留（可审计），但一切查阅被拒绝并返回作废原因。
    对已作废凭证重复作废幂等返回首次作废结果。"""
    sweep_vouchers(db)
    v = get_voucher(db, vid)
    if v['status'] == VCH_VOID:
        view = voucher_view(db, vid)
        view['duplicate'] = True
        view['duplicate_reason'] = '凭证已作废：重复作废返回首次作废结果'
        return view
    if v['status'] == VCH_EXPIRED:
        raise RuleError(409, 'VOUCHER_EXPIRED',
                        f"凭证已超时失效（{v['invalid_reason']}），不能再主动作废")
    db.x("UPDATE vouchers SET status='VOID', invalid_reason=?, voided_by=?, voided_at=? WHERE id=?",
         (reason, actor, time.time(), vid))
    ev(db, v['ticket'], actor, 'VOUCHER_VOIDED', voucher=vid, reason=reason,
       note='快照封存不抹除，仅停止外发查阅')
    return voucher_view(db, vid)


# ---------------------------------------------------------------- 应用工厂


def create_app(db_path='safety_ticket.db', with_seed=True):
    db = DB(db_path)
    storm.register_helpers(load_topo_fn=load_topo, get_ticket_fn=get_ticket,
                           get_steps_fn=get_steps, ev_fn=ev, close_handovers_fn=close_handovers)
    if with_seed:
        seed(db)
        seed_tools(db)
        metro.seed_metro(db)
        storm.seed_storm(db)
    app = FastAPI(title='配电检修安全票协同系统')

    # ---- 拓扑与遥测
    @app.get('/api/topology')
    def api_topo():
        topo = load_topo(db)
        return {'version': topo.version,
                'devices': topo.devices,
                'edges': [list(e) for e in db.q('SELECT a,b FROM edges')],
                'grounds': [list(g) for g in topo.grounds],
                'energized': sorted(topo.energized())}

    @app.get('/api/devices/{did}')
    def api_device(did: str):
        """现场扫码：返回设备遥测与当前控制票据（重复扫描无副作用）。"""
        topo = load_topo(db)
        if did not in topo.devices:
            c = db.one('SELECT * FROM control WHERE device=?', (did,))
            if c:
                return {'id': did, 'kind': 'ground_point', 'controlled_by': c['ticket']}
            raise HTTPException(404, '设备不存在')
        c = db.one('SELECT ticket FROM control WHERE device=?', (did,))
        d = dict(topo.devices[did])
        d.update({'id': did,
                  'energized': did in topo.energized(),
                  'grounded': did in topo.grounded_points(),
                  'controlled_by': c['ticket'] if c else None})
        return d

    # ---- 边界推导与票据
    class DeriveReq(BaseModel):
        work_area: list[str]

    @app.post('/api/derive')
    def api_derive(req: DeriveReq):
        topo = load_topo(db)
        try:
            b = topo.derive_boundary(req.work_area)
        except ValueError as e:
            raise HTTPException(400, str(e))
        b['steps'] = domain.isolation_steps(b)
        b['control_conflict'] = check_control_conflict(db, b['control'])
        return b

    class TicketReq(BaseModel):
        title: str
        work_area: list[str]
        created_by: str
        team: Optional[str] = None            # 初始持权班组（默认甲班）
        voltage_kv: float = 10.0              # 计划作业电压等级（器具证书适用范围校验）
        plan_start_ms: Optional[int] = None   # 计划作业开始（默认当前）
        plan_end_ms: Optional[int] = None     # 计划作业结束（默认 8 小时后；证书须覆盖至此时刻）

    @app.post('/api/tickets', status_code=201)
    def api_create(req: TicketReq):
        with db.lock:
            topo = load_topo(db)
            try:
                b = topo.derive_boundary(req.work_area)
            except ValueError as e:
                raise HTTPException(400, str(e))
            conflict = check_control_conflict(db, b['control'])
            if conflict:
                ev(db, None, req.created_by, 'TICKET_REJECTED', reason='控制权冲突',
                   conflict_device=conflict['device'], conflict_ticket=conflict['ticket'])
                db.commit()
                raise HTTPException(409, f"设备 {conflict['device']} 已受票据 {conflict['ticket']} 控制，禁止重叠开票")
            tid = 'T' + time.strftime('%H%M%S') + uuid.uuid4().hex[:6].upper()
            now_ms = int(time.time() * 1000)
            plan_start = req.plan_start_ms or now_ms
            plan_end = req.plan_end_ms or (now_ms + 8 * 3600_000)
            if plan_end < plan_start:
                raise HTTPException(400, '计划作业时段非法：plan_end_ms 必须晚于 plan_start_ms')
            db.x('INSERT INTO tickets(id,title,work_area,boundary,snapshot,status,frozen,freeze_reason,'
                 'created_by,created_at,permit_by,permit_at,write_owner,plan_start_ms,plan_end_ms,voltage_kv) '
                 'VALUES(?,?,?,?,?,?,0,NULL,?,?,NULL,NULL,?,?,?,?)',
                 (tid, req.title, json.dumps(b['work_area']), json.dumps(b, ensure_ascii=False),
                  topo.version, 'ISOLATING', req.created_by, time.time(), req.team or '甲班',
                  plan_start, plan_end, req.voltage_kv))
            for i, s in enumerate(domain.isolation_steps(b), 1):
                db.x('INSERT INTO steps(id,ticket,seq,phase,kind,device,point,sw,expected,desc) VALUES(?,?,?,?,?,?,?,?,?,?)',
                     (f'{tid}:{i}', tid, i, s['phase'], s['kind'], s['device'], s['point'], s['sw'], s['expected'], s['desc']))
            acquire_control(db, tid, b['control'])
            ev(db, tid, req.created_by, 'TICKET_CREATED', title=req.title,
               work_area=b['work_area'], snapshot=topo.version)
            ev(db, tid, 'system', 'BOUNDARY_DERIVED',
               switches=[s['device'] for s in b['switches']],
               grounds=[g['id'] for g in b['grounds']], forbidden=b['forbidden'])
            db.commit()
            return ticket_view(db, tid)

    @app.get('/api/tickets')
    def api_tickets():
        out = []
        for t in db.q('SELECT * FROM tickets ORDER BY created_at DESC'):
            t = dict(t)
            t['work_area'] = json.loads(t['work_area'])
            t['boundary'] = json.loads(t['boundary'])
            steps = get_steps(db, t['id'])
            t['steps_total'] = len(steps)
            t['steps_done'] = sum(1 for s in steps if s['status'] == 'VERIFIED')
            out.append(t)
        return out

    @app.get('/api/tickets/{tid}')
    def api_ticket(tid: str):
        return ticket_view(db, tid)

    class StepReq(BaseModel):
        actor: str = 'unknown'
        scanned: Optional[str] = None
        key: Optional[str] = None

    @app.post('/api/tickets/{tid}/steps/{sid}/report')
    def api_report(tid: str, sid: str, req: StepReq):
        return do_report(db, tid, sid, req.actor, req.scanned or '', req.key or f'{tid}:{sid}:report')

    class VerifyReq(BaseModel):
        actor: str = 'unknown'
        key: Optional[str] = None

    @app.post('/api/tickets/{tid}/steps/{sid}/verify')
    def api_verify(tid: str, sid: str, req: VerifyReq):
        return do_verify(db, tid, sid, req.actor, req.key or f'{tid}:{sid}:verify')

    class ActorReq(BaseModel):
        actor: str = 'unknown'
        reason: Optional[str] = None

    @app.post('/api/tickets/{tid}/permit')
    def api_permit(tid: str, req: ActorReq):
        with db.lock:
            topo = load_topo(db)
            t = get_ticket(db, tid)
            if t['status'] != 'ISOLATING':
                raise HTTPException(409, f'状态 {t["status"]} 不能许可开工')
            if t['frozen']:
                raise HTTPException(423, f"票据已冻结：{t['freeze_reason']}")
            steps = get_steps(db, tid)
            if any(s['status'] != 'VERIFIED' for s in steps if s['phase'] == 'ISO'):
                raise HTTPException(409, '隔离步骤未全部复核完成，禁止许可开工')
            en = topo.energized()
            hot = [w for w in t['work_area'] if w in en]
            if hot:
                raise HTTPException(409, f'工作区仍带电: {hot}')
            db.x("UPDATE tickets SET status='PERMITTED', permit_by=?, permit_at=? WHERE id=?",
                 (req.actor, time.time(), tid))
            ev(db, tid, req.actor, 'PERMIT_GRANTED')
            db.commit()
            return ticket_view(db, tid)

    @app.post('/api/tickets/{tid}/finish-work')
    def api_finish(tid: str, req: ActorReq):
        with db.lock:
            t = get_ticket(db, tid)
            if t['status'] != 'PERMITTED':
                raise HTTPException(409, f'状态 {t["status"]} 不能收工')
            if t['frozen']:
                raise HTTPException(423, f"票据已冻结：{t['freeze_reason']}")
            iso = get_steps(db, tid)
            rest = domain.restoration_steps(iso)
            base = max(s['seq'] for s in iso)
            for i, s in enumerate(rest, 1):
                db.x('INSERT INTO steps(id,ticket,seq,phase,kind,device,point,sw,expected,desc) VALUES(?,?,?,?,?,?,?,?,?,?)',
                     (f'{tid}:R{i}', tid, base + i, s['phase'], s['kind'], s['device'], s['point'], s['sw'], s['expected'], s['desc']))
            db.x("UPDATE tickets SET status='RESTORING' WHERE id=?", (tid,))
            storm.refresh_environments(db, req.actor, '作业收工进入恢复送电阶段，票据状态依据变化', tid)
            ev(db, tid, req.actor, 'WORK_FINISHED',
               restore_order=[s['device'] for s in rest])
            db.commit()
            return ticket_view(db, tid)

    @app.post('/api/tickets/{tid}/freeze')
    def api_freeze(tid: str, req: ActorReq):
        with db.lock:
            t = get_ticket(db, tid)
            if t['status'] not in ACTIVE_STATUS:
                raise HTTPException(409, f'状态 {t["status"]} 无需冻结')
            db.x('UPDATE tickets SET frozen=1, freeze_reason=? WHERE id=?',
                 (req.reason or '调度员手动冻结', tid))
            ev(db, tid, req.actor, 'FREEZE', reason=[req.reason or '调度员手动冻结'])
            # 工作票锁定：关闭尚未完成的换班（既成设备动作与控制锁不回退）
            close_handovers(db, tid, 'LOCKED', f"工作票锁定：{req.reason or '调度员手动冻结'}", req.actor)
            db.commit()
            return ticket_view(db, tid)

    @app.post('/api/tickets/{tid}/unfreeze')
    def api_unfreeze(tid: str, req: ActorReq):
        with db.lock:
            topo = load_topo(db)
            t = get_ticket(db, tid)
            if not t['frozen']:
                return ticket_view(db, tid)
            problems = domain.boundary_problems(topo, t, get_steps(db, tid))
            if problems:
                raise HTTPException(409, '边界问题未消除，不能解冻: ' + '；'.join(problems))
            db.x('UPDATE tickets SET frozen=0, freeze_reason=NULL WHERE id=?', (tid,))
            ev(db, tid, req.actor, 'UNFREEZE')
            db.commit()
            return ticket_view(db, tid)

    @app.post('/api/tickets/{tid}/rebaseline')
    def api_rebaseline(tid: str, req: ActorReq):
        """拓扑变更后重新基线：按当前拓扑重新推导边界。
        已复核且仍在新边界中的物理步骤原样保留（不伪造、不抹除），其余步骤重建。"""
        with db.lock:
            topo = load_topo(db)
            t = get_ticket(db, tid)
            if not t['frozen']:
                raise HTTPException(409, '仅冻结票据可重新基线')
            if t['status'] == 'RESTORING':
                raise HTTPException(409, '恢复送电阶段不可重新基线，请撤回或完成恢复')
            try:
                b = topo.derive_boundary(t['work_area'])
            except ValueError as e:
                raise HTTPException(400, str(e))
            old = get_steps(db, tid)
            kept = {(s['kind'], s['device']): s for s in old
                    if s['phase'] == 'ISO' and s['status'] == 'VERIFIED'}
            conflict = check_control_conflict(db, [d for d in b['control']
                                                   if db.one('SELECT 1 x FROM control WHERE device=? AND ticket=?', (d, tid)) is None])
            if conflict:
                raise HTTPException(409, f"新边界设备 {conflict['device']} 受票据 {conflict['ticket']} 控制")
            db.x("DELETE FROM steps WHERE ticket=? AND phase='ISO'", (tid,))
            acquire_control(db, tid, b['control'])
            carried, rebuilt = [], []
            for i, s in enumerate(domain.isolation_steps(b), 1):
                sid = f'{tid}:{i}'
                old_s = kept.get((s['kind'], s['device']))
                if old_s:
                    db.x('INSERT INTO steps(id,ticket,seq,phase,kind,device,point,sw,expected,desc,status,reporter,reported_at,verifier,verified_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                         (sid, tid, i, s['phase'], s['kind'], s['device'], s['point'], s['sw'], s['expected'], s['desc'],
                          'VERIFIED', old_s['reporter'], old_s['reported_at'], old_s['verifier'], old_s['verified_at']))
                    carried.append(s['device'])
                else:
                    db.x('INSERT INTO steps(id,ticket,seq,phase,kind,device,point,sw,expected,desc) VALUES(?,?,?,?,?,?,?,?,?,?)',
                         (sid, tid, i, s['phase'], s['kind'], s['device'], s['point'], s['sw'], s['expected'], s['desc']))
                    rebuilt.append(s['device'])
            db.x('UPDATE tickets SET boundary=?, snapshot=? WHERE id=?',
                 (json.dumps(b, ensure_ascii=False), topo.version, tid))
            # 重新定界后旧授权全部作废（授权段/边界版本均已改变）；已采纳证据与物理动作保留
            void_pkgs_for_ticket(db, tid,
                                 lambda p: p['status'] in PKG_LIVE_STATES,
                                 f'重新定界（新边界 v{topo.version}）使旧授权作废', req.actor)
            # 边界重算：关闭尚未完成的换班（写权归属与已入账证据不回退）
            close_handovers(db, tid, 'REBASELINED', f'边界重算（新边界 v{topo.version}）', req.actor)
            storm.refresh_environments(db, req.actor, f'重新定界至 v{topo.version}，旧复工确认失效', tid)
            ev(db, tid, req.actor, 'REBASELINE', snapshot=topo.version,
               carried_verified=carried, rebuilt=rebuilt)
            db.commit()
            return ticket_view(db, tid)

    @app.post('/api/tickets/{tid}/withdraw')
    def api_withdraw(tid: str, req: ActorReq):
        """撤回票据（撤销许可）：审计保留；无物理改动的设备控制权立即释放，
        现场仍有物理残留（已断开开关/已装接地，含已执行未监护的）的设备继续锁定，
        直至现场恢复后由 purge-control 清理。已下发的无网授权全部作废。"""
        with db.lock:
            topo = load_topo(db)
            t = get_ticket(db, tid)
            if t['status'] in ('CLOSED', 'WITHDRAWN'):
                raise HTTPException(409, f'票据已是 {t["status"]} 状态')
            orig = {s['device']: s.get('orig', 'CLOSED') for s in t['boundary']['switches']}
            retained = []
            for r in db.q('SELECT * FROM control WHERE ticket=?', (tid,)):
                d = r['device']
                residue = bool(r['physical'])       # 已执行隔离（含初态即分位的常开联络开关，防误合反送电）
                if d.startswith('GND:'):
                    sw = d[4:]
                    residue = residue or any(g[1] == sw and g[2] == tid for g in topo.grounds)
                elif d in topo.devices and topo.devices[d]['kind'] == 'switch':
                    residue = residue or topo.devices[d]['state'] != orig.get(d, 'CLOSED')
                if residue:
                    retained.append(d)
            placeholders = ','.join('?' * len(retained)) if retained else "''"
            if retained:
                db.x(f'DELETE FROM control WHERE ticket=? AND device NOT IN ({placeholders})',
                     (tid, *retained))
            else:
                db.x('DELETE FROM control WHERE ticket=?', (tid,))
            db.x("UPDATE tickets SET status='WITHDRAWN', frozen=0 WHERE id=?", (tid,))
            # 撤销许可：所有在途授权包作废，旧片段按影响范围拒收；残留锁定不变
            void_pkgs_for_ticket(db, tid, lambda p: p['status'] in PKG_LIVE_STATES,
                                 f"许可撤销（{req.reason or '撤回票据'}）：旧授权作废，现场残留状态继续锁定设备",
                                 req.actor)
            # 计划取消：关闭尚未完成的换班；残留物理状态与控制锁不回退
            close_handovers(db, tid, 'WITHDRAWN', f"计划取消：{req.reason or '撤回票据'}", req.actor)
            storm.mark_ticket_terminal(db, tid, req.actor, f'票据撤销：{req.reason or "计划取消"}')
            # 工器具库存锁回滚：票据撤回时尚未出箱的批次锁定整批释放（已生成在途工具箱的不走回滚）
            try:
                tools_mod.release_ticket_locks(
                    db, tid, req.actor, f"安全票撤回（{req.reason or '计划取消'}），批次锁定整批回滚")
            except tools_mod.RuleError:
                pass
            ev(db, tid, req.actor, 'TICKET_WITHDRAWN', reason=req.reason,
               released_immediately=[d for d in t['boundary']['control'] if d not in retained],
               retained_physical=retained)
            db.commit()
            return {'ok': True, 'retained_physical': retained}

    @app.post('/api/tickets/{tid}/purge-control')
    def api_purge(tid: str, req: ActorReq):
        """撤回后清理残留控制权：仅当现场物理状态确已恢复（开关回原位、接地已拆）。"""
        with db.lock:
            topo = load_topo(db)
            t = get_ticket(db, tid)
            if t['status'] != 'WITHDRAWN':
                raise HTTPException(409, '仅已撤回票据可清理控制权')
            orig = {s['device']: s.get('orig', 'CLOSED') for s in t['boundary']['switches']}
            bad = []
            for r in db.q('SELECT device FROM control WHERE ticket=?', (tid,)):
                d = r['device']
                if d.startswith('GND:'):
                    sw = d[4:]
                    if any(g[1] == sw and g[2] == tid for g in topo.grounds):
                        bad.append(f'{d} 接地线未拆')
                elif topo.devices[d]['state'] != orig.get(d, 'CLOSED'):
                    bad.append(f'{d} 未恢复原始位置')
            if bad:
                raise HTTPException(409, '物理状态未恢复: ' + '；'.join(bad))
            db.x('DELETE FROM control WHERE ticket=?', (tid,))
            ev(db, tid, req.actor, 'CONTROL_PURGED')
            db.commit()
            return {'ok': True}

    # ---- 离线补传：现场端缓存的操作按客户端序号排序后幂等重放
    class SyncOp(BaseModel):
        seq: int
        key: str
        op: str                      # report | verify
        step: str
        actor: str
        scanned: Optional[str] = None

    class SyncReq(BaseModel):
        ops: list[SyncOp]

    @app.post('/api/tickets/{tid}/sync')
    def api_sync(tid: str, req: SyncReq):
        results = []
        for op in sorted(req.ops, key=lambda o: o.seq):
            try:
                if op.op == 'report':
                    r = do_report(db, tid, op.step, op.actor, op.scanned or '', op.key)
                elif op.op == 'verify':
                    r = do_verify(db, tid, op.step, op.actor, op.key)
                else:
                    raise HTTPException(400, f'未知操作 {op.op}')
                results.append({'seq': op.seq, 'ok': True, 'result': r})
            except HTTPException as e:
                results.append({'seq': op.seq, 'ok': False, 'error': e.detail})
        ev(db, tid, 'system', 'OFFLINE_SYNC', total=len(req.ops),
           ok=sum(1 for r in results if r['ok']))
        db.commit()
        return {'results': results}

    # ---- 无网作业授权包：一次性下发授权范围，断网本地造证，整包事务核验
    class PkgIssueReq(BaseModel):
        seg_from: int
        seg_to: int
        executor: str
        guardian: str
        deadline: int                       # epoch 毫秒
        issued_by: str = '调度员'

    @app.post('/api/tickets/{tid}/offline-packages', status_code=201)
    def api_pkg_issue(tid: str, req: PkgIssueReq):
        with db.lock:
            topo = load_topo(db)
            t = get_ticket(db, tid)
            steps = get_steps(db, tid)
            if req.seg_from > req.seg_to or req.seg_from < 1:
                raise HTTPException(400, '授权段序号非法：seg_from ≤ seg_to 且从 1 开始')
            seg = [s for s in steps if req.seg_from <= s['seq'] <= req.seg_to]
            if len(seg) != req.seg_to - req.seg_from + 1:
                raise HTTPException(404, '授权段超出该票据现有步骤范围（连续指令段必须完整存在）')
            if len({s['phase'] for s in seg}) != 1:
                raise HTTPException(400, '授权段必须是同一阶段的连续指令段')
            if any(s['status'] != 'PENDING' for s in seg):
                raise HTTPException(409, '授权段含已开始/已完成步骤：授权范围只能覆盖未执行的连续指令段')
            if req.executor == req.guardian:
                raise HTTPException(400, '执行者与监护者必须为不同成员（双人监护）')
            if req.deadline <= int(time.time() * 1000):
                raise HTTPException(400, '截止时间必须晚于当前时间')
            overlap = db.one(
                "SELECT id FROM off_pkgs WHERE ticket=? AND status IN ('ACTIVE','PARTIAL') "
                "AND seg_from<=? AND seg_to>=?",
                (tid, req.seg_to, req.seg_from))
            if overlap:
                raise HTTPException(409, f"该指令段已被在途授权包 {overlap['id']} 覆盖，授权范围不得重叠下发")
            pid = 'P' + time.strftime('%H%M%S') + uuid.uuid4().hex[:6].upper()
            p = {'id': pid, 'ticket': tid, 'seg_from': req.seg_from, 'seg_to': req.seg_to,
                 'phase': seg[0]['phase'], 'snapshot': topo.version,
                 'executor': req.executor, 'guardian': req.guardian, 'deadline': req.deadline}
            sh = scope_hash_of(p)
            db.x('INSERT INTO off_pkgs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                 (pid, tid, req.seg_from, req.seg_to, seg[0]['phase'], topo.version,
                  req.executor, req.guardian, req.deadline, sh, PKG_ACTIVE, None,
                  req.issued_by, time.time()))
            ev(db, tid, req.issued_by, 'PKG_ISSUED', pkg=pid, seg=[req.seg_from, req.seg_to],
               snapshot=topo.version, executor=req.executor, guardian=req.guardian, deadline=req.deadline,
               scope_hash=sh)
            db.commit()
            return pkg_view(db, get_pkg(db, pid))

    @app.get('/api/tickets/{tid}/offline-packages')
    def api_pkg_list(tid: str):
        get_ticket(db, tid)
        return [pkg_view(db, dict(r))
                for r in db.q('SELECT * FROM off_pkgs WHERE ticket=? ORDER BY issued_at', (tid,))]

    @app.get('/api/offline-packages/{pid}')
    def api_pkg_get(pid: str):
        try:
            return pkg_view(db, get_pkg(db, pid))
        except RuleError as e:
            raise http_for(e)

    class PkgRecord(BaseModel):
        pkg: str
        seq: int
        type: str
        step: int
        actor: Optional[str] = None
        scanned: Optional[str] = None
        device_state: Optional[str] = None
        grounded: Optional[bool] = None
        energized: Optional[bool] = None
        ts: int
        prev_hash: str
        hash: str

    # 计入摘要/存储的载荷字段见模块级 RECORD_FIELDS（prev_hash、hash 是链指针与摘要本身，不计入）

    class PkgSubmitReq(BaseModel):
        records: list[PkgRecord]

    @app.post('/api/offline-packages/{pid}/submit')
    def api_pkg_submit(pid: str, req: PkgSubmitReq):
        """整包（或片段）提交：同包反复发送幂等不产生第二次状态变化；缺口补齐后重放即推进。"""
        with db.lock:
            try:
                p = get_pkg(db, pid)
                total = (p['seg_to'] - p['seg_from'] + 1) * 4
                received = []
                for rec in req.records:
                    d = {k: getattr(rec, k) for k in RECORD_FIELDS}
                    payload = canonical(d)
                    if rec.pkg != pid:
                        raise RuleError(400, 'REC_OUT_OF_SCOPE',
                                        f'证据 #{rec.seq} 的 pkg={rec.pkg} 与提交目标 {pid} 不符')
                    if not (1 <= rec.seq <= total):
                        raise RuleError(400, 'REC_OUT_OF_SCOPE',
                                        f'证据序号 {rec.seq} 超出授权链长度 {total}')
                    existing = db.one('SELECT * FROM evidences WHERE pkg=? AND seq=?', (pid, rec.seq))
                    if existing:
                        # 同序号重放：内容一致 → 幂等返回首次裁决；内容冲突 → 先到为准，后到驳回
                        if existing['hash'] == rec.hash and existing['payload'] == payload:
                            received.append({'seq': rec.seq, 'status': existing['status'],
                                             'reason': existing['reason'], 'duplicate': True})
                            continue
                        received.append({'seq': rec.seq, 'status': EV_DUP_CONFLICT,
                                         'reason': '同序号后到记录与先到内容冲突：先到序列为准（确定性裁决），后到驳回',
                                         'duplicate': True})
                        continue
                    # 服务端独立重算摘要，防篡改
                    calc = evidence_hash('' if rec.prev_hash == 'GENESIS' else rec.prev_hash, d)
                    if calc != rec.hash:
                        raise RuleError(400, 'HASH_MISMATCH',
                                        f'证据 #{rec.seq} 摘要校验失败（本地链可能被篡改）')
                    db.x('INSERT INTO evidences(pkg,seq,prev_hash,hash,status,reason,payload,applied_at) '
                          "VALUES(?,?,?,?,'WAITING',NULL,?,NULL)",
                          (pid, rec.seq, rec.prev_hash, rec.hash, payload))
                    received.append({'seq': rec.seq, 'status': EV_WAITING, 'duplicate': False})
                if any(not x.get('duplicate') or x['status'] == EV_WAITING for x in received):
                    p = adjudicate(db, p)
                p = get_pkg(db, pid)
                ev(db, p['ticket'], 'system', 'PKG_SUBMIT', pkg=pid,
                   sent=len(req.records), stored=len(received))
                db.commit()
                view = pkg_view(db, p)
                view['received'] = received
                return view
            except RuleError as e:
                db.rollback()
                raise http_for(e)

    class PkgVoidReq(BaseModel):
        actor: str = '调度员'
        reason: str

    @app.post('/api/offline-packages/{pid}/void')
    def api_pkg_void(pid: str, req: PkgVoidReq):
        """撤销授权包（成员替换等）：未决证据拒收，已采纳证据、物理动作与设备占用不抹除。"""
        with db.lock:
            try:
                p = get_pkg(db, pid)
                p = void_pkg(db, p, req.reason, req.actor)
                db.commit()
                return pkg_view(db, p)
            except RuleError as e:
                db.rollback()
                raise http_for(e)

    # ---- 值守班组换班接续
    class HandoverReq(BaseModel):
        watermark: float                      # 切换水位（数据时标阈值，epoch 毫秒）
        candidates: list[str]                 # 候选承接班组（争抢签收，先到者得）
        deadline: float                       # 签收期限（epoch 毫秒）
        from_team: Optional[str] = None       # 移交方（须为当前持权班组，缺省取之）
        created_by: str = '调度员'

    @app.post('/api/tickets/{tid}/handovers', status_code=201)
    def api_handover_create(tid: str, req: HandoverReq):
        with db.lock:
            try:
                sweep_handovers(db)
                t = get_ticket(db, tid)
                if t['status'] not in ACTIVE_STATUS:
                    raise RuleError(409, 'TICKET_INACTIVE', f"票据状态 {t['status']}，不能开具交班单")
                if t['frozen']:
                    raise RuleError(423, 'TICKET_LOCKED', f"工作票锁定中（{t['freeze_reason']}），不能开具交班单")
                from_team = req.from_team or t['write_owner']
                if from_team != t['write_owner']:
                    raise RuleError(409, 'NOT_WRITE_OWNER',
                                    f"移交方 {from_team} 不是当前持权班组 {t['write_owner']}")
                cands = [c.strip() for c in req.candidates if c and c.strip()]
                if not cands:
                    raise RuleError(400, 'NO_CANDIDATES', '必须指定至少一个候选承接班组')
                if from_team in cands:
                    raise RuleError(400, 'BAD_CANDIDATES', f'承接候选不能包含移交方 {from_team}')
                if len(set(cands)) != len(cands):
                    raise RuleError(400, 'BAD_CANDIDATES', '承接候选班组重复')
                if req.deadline <= int(time.time() * 1000):
                    raise RuleError(400, 'BAD_DEADLINE', '签收期限必须晚于当前时间')
                head_seq, _ = chain_head(db, tid)
                max_ts = db.one('SELECT MAX(ts_data) m FROM shift_evidences WHERE ticket=? AND status=?',
                                (tid, domain.SE_ACCEPTED))['m']
                if max_ts is not None and req.watermark < max_ts:
                    raise RuleError(400, 'BAD_WATERMARK',
                                    f'切换水位不得低于已入账内容的最高时标 {max_ts:.0f}')
                open_h = db.one("SELECT id FROM handovers WHERE ticket=? AND status IN ('PENDING','SIGNED')",
                                (tid,))
                if open_h:
                    raise RuleError(409, 'HANDOVER_EXISTS', f"已存在未完成的交班单 {open_h['id']}")
                hid = 'H' + time.strftime('%H%M%S') + uuid.uuid4().hex[:6].upper()
                db.x('INSERT INTO handovers VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,0)',
                     (hid, tid, req.watermark, from_team, json.dumps(cands, ensure_ascii=False), None,
                      req.deadline, domain.HO_PENDING, None, req.created_by, time.time(), None, None))
                ev(db, tid, req.created_by, 'HANDOVER_CREATED', handover=hid,
                   watermark=req.watermark, from_team=from_team, candidates=cands,
                   deadline=req.deadline, chain_head=head_seq)
                db.commit()
                return handover_view(get_handover(db, hid))
            except RuleError as e:
                db.rollback()
                raise http_for(e)

    @app.get('/api/tickets/{tid}/handovers')
    def api_handover_list(tid: str):
        with db.lock:
            sweep_handovers(db)
            db.commit()
            get_ticket(db, tid)
            return [handover_view(dict(r)) for r in
                    db.q('SELECT * FROM handovers WHERE ticket=? ORDER BY created_at', (tid,))]

    @app.get('/api/handovers/{hid}')
    def api_handover_get(hid: str):
        with db.lock:
            sweep_handovers(db)
            db.commit()
            try:
                return handover_view(get_handover(db, hid))
            except RuleError as e:
                raise http_for(e)

    class SignReq(BaseModel):
        team: str
        key: Optional[str] = None

    @app.post('/api/handovers/{hid}/sign')
    def api_handover_sign(hid: str, req: SignReq):
        """签收：同一事务内 CAS 交班单状态 + 改写票据写权班组，原子转移写权。
        两个候选班组争抢时只有条件更新生效的一方能取得写权。"""
        with db.lock:
            try:
                resp, _ = idem(db, req.key or f'{hid}:sign:{req.team}',
                               lambda: sign_core(db, hid, req.team))
            except RuleError as e:
                db.rollback()
                raise http_for(e)
            db.commit()
            return resp

    @app.post('/api/handovers/{hid}/cancel')
    def api_handover_cancel(hid: str, req: ActorReq):
        """调度撤单：仅待签收的交班单可撤；已签收（写权已转移）不可撤。"""
        with db.lock:
            try:
                sweep_handovers(db)
                h = get_handover(db, hid)
                if h['status'] != domain.HO_PENDING:
                    raise RuleError(409, 'HANDOVER_NOT_PENDING',
                                    f"交班单状态 {h['status']}，不可撤单"
                                    + ('（写权已转移，不可回退）' if h['status'] in domain.HO_RELEVANT_STATES[1:] else ''))
                close_handovers(db, h['ticket'], 'CANCELLED', f"调度撤单：{req.reason or '调度员撤销交班单'}",
                                req.actor)
                db.commit()
                return handover_view(get_handover(db, hid))
            except RuleError as e:
                db.rollback()
                raise http_for(e)

    class ShiftEvidenceReq(BaseModel):
        client_id: str                        # 幂等键（同票据内唯一）；挂起后可用同键修正基点重传
        team: str
        device: str = 'FIELD'
        kind: str = '巡检记录'
        ts_data: float                        # 数据时标（epoch 毫秒），与切换水位比较划分归属
        payload: str = ''
        base_seq: Optional[int] = None        # 承接方首条内容声明的接续基点（链头序号）

    @app.post('/api/tickets/{tid}/shift-evidences')
    def api_shift_evidence(tid: str, req: ShiftEvidenceReq):
        """现场证据入账：按切换水位划分归属，服务端定序入链；同 client_id 重发幂等。"""
        with db.lock:
            try:
                sweep_handovers(db)
                t = get_ticket(db, tid)
                old = db.one('SELECT * FROM shift_evidences WHERE ticket=? AND client_id=?',
                             (tid, req.client_id))
                if old:
                    old = dict(old)
                    same = all(old[k] == getattr(req, k)
                               for k in ('team', 'device', 'kind', 'ts_data', 'payload'))
                    if not same:
                        raise RuleError(409, 'REBASE_CONFLICT',
                                        f"client_id {req.client_id} 已有内容不同的记录（#{old['id']}），"
                                        f"先到内容为准；如需修正请更换 client_id")
                    if old['status'] == domain.SE_SUSPENDED and req.base_seq != old['base_seq']:
                        # 挂起内容修正接续基点重传：重新裁决（可唤醒入账）
                        db.x('UPDATE shift_evidences SET base_seq=? WHERE id=?',
                             (req.base_seq, old['id']))
                        old['base_seq'] = req.base_seq
                        old = judge_and_apply(db, t, old)
                        ev(db, tid, req.team, 'SHIFT_EVIDENCE_REBASED',
                           id=old['id'], base_seq=req.base_seq, status=old['status'],
                           reason=old['reason'])
                    else:
                        ev(db, None, 'system', 'DUPLICATE_IGNORED', key=f'{tid}:{req.client_id}')
                    db.commit()
                    return {'ok': old['status'] == domain.SE_ACCEPTED, 'duplicate': True,
                            'evidence': old}
                row = {'ticket': tid, 'client_id': req.client_id, 'team': req.team,
                       'device': req.device, 'kind': req.kind, 'ts_data': req.ts_data,
                       'payload': req.payload, 'base_seq': req.base_seq}
                db.x('INSERT INTO shift_evidences(ticket,client_id,team,device,kind,ts_data,payload,'
                     "base_seq,status,created_at) VALUES(?,?,?,?,?,?,?,?,'NEW',?)",
                     (tid, req.client_id, req.team, req.device, req.kind, req.ts_data,
                      req.payload, req.base_seq, time.time()))
                row['id'] = db.one('SELECT last_insert_rowid() i')['i']
                row = judge_and_apply(db, t, row)
                ev(db, tid, req.team, 'SHIFT_EVIDENCE', id=row['id'], client_id=req.client_id,
                   device=req.device, kind=req.kind, ts_data=req.ts_data,
                   status=row['status'], reason=row['reason'],
                   handover=row.get('handover'), seq=row.get('seq'))
                db.commit()
                return {'ok': row['status'] == domain.SE_ACCEPTED, 'duplicate': False, 'evidence': row}
            except RuleError as e:
                db.rollback()
                raise http_for(e)

    @app.get('/api/tickets/{tid}/shift')
    def api_shift_view(tid: str):
        with db.lock:
            sweep_handovers(db)
            db.commit()
            get_ticket(db, tid)
            return shift_view(db, tid)

    # ---- 演示辅助：一键准备在执行工作票 / 模拟主站重启
    class DemoPrepReq(BaseModel):
        work_area: list[str] = ['F1']
        team: str = '甲班'
        created_by: str = '调度员'

    @app.post('/api/demo/shift/prepare', status_code=201)
    def api_demo_shift_prepare(req: DemoPrepReq):
        """开具一张工作票并真实执行全部隔离步骤至许可开工，供换班演示使用。
        先复位上一张演示票（恢复物理状态、回收控制权），保证剧本可反复执行。"""
        with db.lock:
            # 演示复位：回收旧演示票的物理状态与控制权（仅限标题为“换班演示·”的票据）
            for old in db.q("SELECT * FROM tickets WHERE title LIKE '换班演示·%' "
                            "AND status IN ('ISOLATING','PERMITTED','RESTORING','WITHDRAWN')"):
                old = dict(old)
                oid = old['id']
                boundary = json.loads(old['boundary'])
                for sw in boundary['switches']:
                    db.x('UPDATE devices SET state=? WHERE id=?', (sw['orig'], sw['device']))
                db.x('DELETE FROM grounds WHERE ticket=?', (oid,))
                db.x('DELETE FROM control WHERE ticket=?', (oid,))
                for d in db.q('SELECT id, tags FROM devices'):
                    tags = [t for t in json.loads(d['tags']) if not t.endswith('·' + oid)]
                    db.x('UPDATE devices SET tags=? WHERE id=?',
                         (json.dumps(tags, ensure_ascii=False), d['id']))
                close_handovers(db, oid, 'DEMO_RESET', '演示复位：回收上一张演示票', 'system')
                if old['status'] != 'WITHDRAWN':
                    db.x("UPDATE tickets SET status='WITHDRAWN', frozen=0 WHERE id=?", (oid,))
                ev(db, oid, 'system', 'DEMO_RESET', note='演示票复位：物理状态恢复、控制权回收')
            topo = load_topo(db)
            try:
                b = topo.derive_boundary(req.work_area)
            except ValueError as e:
                raise HTTPException(400, str(e))
            conflict = check_control_conflict(db, b['control'])
            if conflict:
                raise HTTPException(409, f"设备 {conflict['device']} 已受票据 {conflict['ticket']} 控制")
            tid = 'T' + time.strftime('%H%M%S') + uuid.uuid4().hex[:6].upper()
            db.x('INSERT INTO tickets(id,title,work_area,boundary,snapshot,status,frozen,freeze_reason,'
                 'created_by,created_at,permit_by,permit_at,write_owner) VALUES(?,?,?,?,?,?,0,NULL,?,?,NULL,NULL,?)',
                 (tid, '换班演示·' + '、'.join(req.work_area), json.dumps(b['work_area']),
                  json.dumps(b, ensure_ascii=False), topo.version, 'ISOLATING',
                  req.created_by, time.time(), req.team))
            for i, s in enumerate(domain.isolation_steps(b), 1):
                db.x('INSERT INTO steps(id,ticket,seq,phase,kind,device,point,sw,expected,desc) VALUES(?,?,?,?,?,?,?,?,?,?)',
                     (f'{tid}:{i}', tid, i, s['phase'], s['kind'], s['device'], s['point'], s['sw'], s['expected'], s['desc']))
            acquire_control(db, tid, b['control'])
            ev(db, tid, req.created_by, 'TICKET_CREATED', title='换班演示',
               work_area=b['work_area'], snapshot=topo.version, team=req.team)
            # 真实执行隔离：先改物理状态（模拟一次设备），再回报、双人复核
            try:
                for s in get_steps(db, tid):
                    if s['kind'] == domain.OPEN_SWITCH:
                        db.x("UPDATE devices SET state='OPEN' WHERE id=?", (s['device'],))
                    elif s['kind'] == domain.INSTALL_GROUND:
                        db.x('INSERT OR REPLACE INTO grounds VALUES(?,?,?)', (s['point'], s['sw'], tid))
                    ev(db, tid, 'sim', 'TELEMETRY_CHANGED', device=s['device'], expected=s['expected'])
                    report_core(db, tid, s['id'], '张三', s['device'])
                    verify_core(db, tid, s['id'], '李四')
            except RuleError as e:
                db.rollback()
                raise http_for(e)
            db.x("UPDATE tickets SET status='PERMITTED', permit_by=?, permit_at=? WHERE id=?",
                 (req.created_by, time.time(), tid))
            ev(db, tid, req.created_by, 'PERMIT_GRANTED', demo=True)
            db.commit()
            return shift_view(db, tid)

    @app.post('/api/admin/reopen')
    def api_admin_reopen():
        """模拟主站进程重启：关闭并重建数据库连接，全部状态自 SQLite 恢复。
        恢复后写权归属与挂起内容必须和故障前相同（状态本就事务化落盘）。"""
        with db.lock:
            db.commit()
            db.reopen()
            sweep_handovers(db)
            sweep_vouchers(db)
            db.commit()
            return {'ok': True, 'note': '主站已重启：票据、写权归属、交班单、证据链、挂起内容、'
                                        '外发凭证阶段/领取记号/作废结果、检定证书版本、召回范围、'
                                        '冻结原因与未完成复检均自 SQLite 恢复'}

    # ---- 审计事件回放与事故分析
    class ReplayReq(BaseModel):
        from_seq: int                       # 展示窗口下界（之前本票据事件折叠为基线）
        to_seq: int                         # 事件上界（固定，后续新增事件不得进入）
        created_by: str = '分析员'

    @app.post('/api/tickets/{tid}/replays', status_code=201)
    def api_replay_create(tid: str, req: ReplayReq):
        """创建回放：固定事件上界与来源摘要，从事件记录逐条重建状态。
        序号缺口/载荷无法应用 → 回放停在最后一个有效事件并指出位置。"""
        with db.lock:
            get_ticket(db, tid)
            if req.from_seq < 1 or req.to_seq < req.from_seq:
                raise HTTPException(400, '审计序号区间非法：1 ≤ from_seq ≤ to_seq')
            max_seq = db.one('SELECT MAX(seq) m FROM events')['m'] or 0
            if req.to_seq > max_seq:
                raise HTTPException(400, f'事件上界 #{req.to_seq} 超出当前审计序列（最大 #{max_seq}）')
            events = events_upto(db, req.to_seq)
            if not any(e['ticket'] == tid for e in events):
                raise HTTPException(400, f'事件上界 #{req.to_seq} 之前没有票据 {tid} 的审计事件')
            result = replay_engine.run_replay(tid, req.from_seq, req.to_seq, events)
            rid = 'R' + time.strftime('%H%M%S') + uuid.uuid4().hex[:6].upper()
            store_replay(db, rid, tid, req.from_seq, req.to_seq, result, req.created_by)
            ev(db, tid, req.created_by, 'REPLAY_CREATED', replay=rid,
               from_seq=req.from_seq, to_seq=req.to_seq,
               source_digest=result['source_digest'], status=result['status'])
            db.commit()
            return replay_view(get_replay(db, rid))

    @app.get('/api/tickets/{tid}/replays')
    def api_replay_list(tid: str):
        get_ticket(db, tid)
        return [replay_view(dict(r)) for r in
                db.q('SELECT * FROM replays WHERE ticket=? ORDER BY created_at', (tid,))]

    @app.get('/api/replays/{rid}')
    def api_replay_get(rid: str):
        try:
            return replay_view(get_replay(db, rid))
        except RuleError as e:
            raise http_for(e)

    @app.post('/api/replays/{rid}/reverify')
    def api_replay_reverify(rid: str):
        """重新核验：按固定来源摘要逐条比对当前审计库。
        序号缺口/摘要失验/载荷无法应用 → 回放游标停在最后一个有效事件。"""
        with db.lock:
            try:
                rp = get_replay(db, rid)
                pinned = json.loads(rp['pinned'])
                result = replay_engine.run_replay(rp['ticket'], rp['from_seq'], rp['to_seq'],
                                                  events_upto(db, rp['to_seq']), pinned=pinned)
                update_replay(db, rid, result)
                db.commit()
                return replay_view(get_replay(db, rid))
            except RuleError as e:
                db.rollback()
                raise http_for(e)

    class BranchReq(BaseModel):
        name: str
        base_seq: int                       # 分支基点：必须是回放中的一个有效事件
        created_by: str = '分析员'

    @app.post('/api/replays/{rid}/branches', status_code=201)
    def api_branch_create(rid: str, req: BranchReq):
        """从回放中的任一有效事件创建隔离的假设分支（不写回正式票据/审计）。"""
        with db.lock:
            try:
                rp = get_replay(db, rid)
                applied = json.loads(rp['applied']) if rp['applied'] else []
                if req.base_seq not in applied:
                    raise RuleError(400, 'BASE_NOT_VALID',
                                    f'基点 #{req.base_seq} 不是该回放的有效事件'
                                    + (f'（回放停在 #{rp["halt"] and json.loads(rp["halt"])["at_seq"]}）'
                                       if rp['halt'] else ''))
                bid = 'B' + time.strftime('%H%M%S') + uuid.uuid4().hex[:6].upper()
                db.x('INSERT INTO branches VALUES(?,?,?,?,?,?,?,0)',
                     (bid, rid, rp['ticket'], req.base_seq, req.name,
                      req.created_by, time.time()))
                db.commit()
                return branch_view(db, get_branch(db, bid))
            except RuleError as e:
                db.rollback()
                raise http_for(e)

    @app.get('/api/replays/{rid}/branches')
    def api_branch_list(rid: str):
        try:
            get_replay(db, rid)
            return [branch_view(db, dict(r)) for r in
                    db.q('SELECT * FROM branches WHERE replay=? ORDER BY created_at', (rid,))]
        except RuleError as e:
            raise http_for(e)

    @app.get('/api/branches/{bid}')
    def api_branch_get(bid: str):
        try:
            return branch_view(db, get_branch(db, bid))
        except RuleError as e:
            raise http_for(e)

    class BranchOpReq(BaseModel):
        op_type: str                        # UNDO_EVENT | DEVICE_STATE
        params: dict                        # UNDO_EVENT: {seq}；DEVICE_STATE: {device,state}|{ground,installed}
        actor: str = '分析员'
        base_version: int                   # 版本校验：必须等于分支当前 version

    @app.post('/api/branches/{bid}/ops', status_code=201)
    def api_branch_op(bid: str, req: BranchOpReq):
        """向分支追加一项试算（撤销/设备状态变更）。
        版本校验失败（多人同时编辑）→ 409，已保存的假设不被覆盖；
        分支操作只写分支表，绝不写回正式票据或追加正式审计事件。"""
        with db.lock:
            try:
                b = get_branch(db, bid)
                rp = get_replay(db, b['replay'])
                applied = json.loads(rp['applied']) if rp['applied'] else []
                if req.op_type == replay_engine.OP_UNDO:
                    seq = req.params.get('seq')
                    if seq not in applied:
                        raise RuleError(400, 'UNDO_NOT_VALID',
                                        f'只能撤销回放有效事件，#{seq} 不在有效事件序列中')
                elif req.op_type == replay_engine.OP_DEVICE:
                    p = req.params
                    if 'ground' in p:
                        if not isinstance(p.get('installed'), bool):
                            raise RuleError(400, 'BAD_DEVICE_OP', '接地线变更须给出 installed: true/false')
                    elif p.get('state') not in ('OPEN', 'CLOSED') or not p.get('device'):
                        raise RuleError(400, 'BAD_DEVICE_OP',
                                        '设备状态变更须给出 device 与 state（OPEN/CLOSED）')
                else:
                    raise RuleError(400, 'BAD_OP_TYPE',
                                    f'未知试算类型 {req.op_type}（支持 UNDO_EVENT / DEVICE_STATE）')
                # 版本校验：条件更新生效才允许追加——并发提交不得覆盖已保存的假设
                cur = db.conn.execute('UPDATE branches SET version=version+1 '
                                      'WHERE id=? AND version=?', (bid, req.base_version))
                if cur.rowcount == 0:
                    now = get_branch(db, bid)
                    raise RuleError(409, 'VERSION_CONFLICT',
                                    f'分支版本冲突：提交基于 v{req.base_version}，当前已保存 v{now["version"]}；'
                                    f'已保存的假设不被覆盖，请刷新后基于最新版本重试')
                nxt = (db.one('SELECT MAX(ord) m FROM branch_ops WHERE branch=?', (bid,))['m'] or 0) + 1
                db.x('INSERT INTO branch_ops VALUES(?,?,?,?,?,?)',
                     (bid, nxt, req.op_type, json.dumps(req.params, ensure_ascii=False),
                      req.actor, time.time()))
                db.commit()
                return branch_view(db, get_branch(db, bid))
            except RuleError as e:
                db.rollback()
                raise http_for(e)

    class ReportReq(BaseModel):
        title: str
        branch_ids: list[str] = []          # 并入报告的分支（保留分支差异）
        key: Optional[str] = None           # 幂等键：重复生成返回同一结果
        created_by: str = '分析员'

    @app.post('/api/replays/{rid}/reports')
    def api_report_create(rid: str, req: ReportReq):
        """生成事故分析报告：保留固定来源摘要、事件范围与分支差异。
        携带同一幂等键重复生成 → 返回首次生成的同一报告。"""
        with db.lock:
            try:
                rp = get_replay(db, rid)
                if req.key:
                    old = db.one('SELECT * FROM reports WHERE idem_key=?', (req.key,))
                    if old:
                        v = report_view(dict(old))
                        v['duplicate'] = True
                        return v
                content = report_content(db, rp, req.title, req.branch_ids, req.created_by)
                rid2 = 'AR' + time.strftime('%H%M%S') + uuid.uuid4().hex[:6].upper()
                db.x('INSERT INTO reports VALUES(?,?,?,?,?,?,?,?,?)',
                     (rid2, rid, rp['ticket'], req.title,
                      json.dumps(content, ensure_ascii=False),
                      report_hash_of(content), req.key, req.created_by, time.time()))
                db.commit()
                return report_view(get_report(db, rid2))
            except RuleError as e:
                db.rollback()
                raise http_for(e)

    @app.get('/api/reports/{rid}')
    def api_report_get(rid: str):
        try:
            return report_view(get_report(db, rid))
        except RuleError as e:
            raise http_for(e)

    @app.get('/api/tickets/{tid}/reports')
    def api_report_list(tid: str):
        get_ticket(db, tid)
        return [report_view(dict(r)) for r in
                db.q('SELECT * FROM reports WHERE ticket=? ORDER BY created_at', (tid,))]

    @app.post('/api/reports/{rid}/regenerate')
    def api_report_regenerate(rid: str):
        """重复生成：来源摘要与事件范围已固定，返回与首次完全同一的结果。"""
        with db.lock:
            try:
                row = get_report(db, rid)
                content = json.loads(row['content'])
                # 完整性自检：存储内容必须仍与报告摘要一致
                intact = report_hash_of(content) == row['report_hash']
                return {'identical': True, 'integrity_ok': intact,
                        'note': '来源摘要与事件范围已固定，重复生成返回同一结果',
                        'report': report_view(row)}
            except RuleError as e:
                raise http_for(e)

    # ---- 审计资料外发凭证柜：封存只读快照，多接收人原子首次领取
    class VoucherReq(BaseModel):
        from_seq: int                        # 从已完成归约中选取的连续序号下界
        to_seq: int                          # 连续序号上界（必须全部是已应用本票据事件）
        receivers: list[str]                 # 接收人名单（并发首次领取，先到者得）
        deadline: int                        # 截止时刻（epoch 毫秒）
        ttl_ms: Optional[int] = None         # 或直接给有效期毫秒（与 deadline 二选一）
        key: Optional[str] = None            # 幂等键：重复封存返回首次凭证
        created_by: str = '分析员'

    @app.post('/api/replays/{rid}/vouchers', status_code=201)
    def api_voucher_create(rid: str, req: VoucherReq):
        """分析员从一次已完成的归约结果选取连续序号，封存只读外发凭证。
        创建与再次索取幂等（幂等键或同归约+同序号段+同接收人）：同一凭证不会出现两份内容。"""
        with db.lock:
            try:
                deadline = req.deadline
                if req.ttl_ms is not None:
                    deadline = int(time.time() * 1000) + req.ttl_ms
                view = create_voucher_core(db, rid, req.from_seq, req.to_seq,
                                           req.receivers, deadline, req.key, req.created_by)
                db.commit()
                return view
            except RuleError as e:
                db.rollback()
                raise http_for(e)

    @app.get('/api/tickets/{tid}/vouchers')
    def api_voucher_list_by_ticket(tid: str):
        get_ticket(db, tid)
        with db.lock:
            sweep_vouchers(db)
            db.commit()
            return [voucher_view(db, dict(r)['id']) for r in
                    db.q('SELECT id FROM vouchers WHERE ticket=? ORDER BY created_at', (tid,))]

    @app.get('/api/vouchers')
    def api_voucher_list(ticket: Optional[str] = None):
        """凭证柜清单：阶段、封存摘要、序号范围、领取人、失效原因。"""
        with db.lock:
            sweep_vouchers(db)
            db.commit()
            sql = 'SELECT id FROM vouchers'
            args = ()
            if ticket:
                sql += ' WHERE ticket=?'
                args = (ticket,)
            sql += ' ORDER BY created_at'
            return [voucher_view(db, r['id']) for r in db.q(sql, args)]

    @app.get('/api/vouchers/{vid}')
    def api_voucher_get(vid: str):
        with db.lock:
            try:
                sweep_vouchers(db)
                db.commit()
                return voucher_view(db, vid)
            except RuleError as e:
                db.rollback()
                raise http_for(e)

    class VoucherClaimReq(BaseModel):
        receiver: str

    @app.post('/api/vouchers/{vid}/claim')
    def api_voucher_claim(vid: str, req: VoucherClaimReq):
        """接收人首次领取：多接收人并发只有一人成功（唯一领取记号原子抢占）；
        重复请求幂等返回本人首次结果；负方确定失败原因同样持久化。"""
        with db.lock:
            try:
                resp = claim_voucher_core(db, vid, req.receiver)
                db.commit()
                return resp
            except RuleError as e:
                # 争抢负方的领取记号必须落库（重启后恢复失败结果），不可整体回滚
                if e.reason == 'CLAIM_RACE_LOST':
                    db.commit()
                else:
                    db.rollback()
                raise http_for(e)

    class VoucherViewReq(BaseModel):
        receiver: str

    @app.post('/api/vouchers/{vid}/content')
    def api_voucher_content(vid: str, req: VoucherViewReq):
        """中签接收人查阅只读快照：超时/作废/未领取/非接收人均返回确定原因。
        返回的是封存时刻固化的截面，正式库后续追加记录不改变它，也不能借此改写正式数据。"""
        with db.lock:
            try:
                resp = voucher_content_core(db, vid, req.receiver)
                db.commit()
                return resp
            except RuleError as e:
                db.rollback()
                raise http_for(e)

    class VoucherVoidReq(BaseModel):
        reason: str
        actor: str = '分析员'

    @app.post('/api/vouchers/{vid}/void')
    def api_voucher_void(vid: str, req: VoucherVoidReq):
        """主动作废凭证：快照保留但一切查阅返回作废原因；重复作废幂等。"""
        with db.lock:
            try:
                view = void_voucher_core(db, vid, req.reason, req.actor)
                db.commit()
                return view
            except RuleError as e:
                db.rollback()
                raise http_for(e)

    # ---- 回放演示辅助：一键准备演示票 / 取证篡改模拟
    @app.post('/api/demo/replay/prepare', status_code=201)
    def api_demo_replay_prepare(req: DemoPrepReq):
        """开具一张演示票并真实执行全部隔离步骤至许可开工，再入账两条现场证据，
        形成可供回放的丰富事件流。先复位上一张回放演示票，保证剧本可反复执行。"""
        with db.lock:
            for old in db.q("SELECT * FROM tickets WHERE title LIKE '回放演示·%' "
                            "AND status IN ('ISOLATING','PERMITTED','RESTORING','WITHDRAWN')"):
                old = dict(old)
                oid = old['id']
                boundary = json.loads(old['boundary'])
                for sw in boundary['switches']:
                    db.x('UPDATE devices SET state=? WHERE id=?', (sw['orig'], sw['device']))
                db.x('DELETE FROM grounds WHERE ticket=?', (oid,))
                db.x('DELETE FROM control WHERE ticket=?', (oid,))
                for d in db.q('SELECT id, tags FROM devices'):
                    tags = [t for t in json.loads(d['tags']) if not t.endswith('·' + oid)]
                    db.x('UPDATE devices SET tags=? WHERE id=?',
                         (json.dumps(tags, ensure_ascii=False), d['id']))
                close_handovers(db, oid, 'DEMO_RESET', '演示复位：回收上一张回放演示票', 'system')
                if old['status'] != 'WITHDRAWN':
                    db.x("UPDATE tickets SET status='WITHDRAWN', frozen=0 WHERE id=?", (oid,))
                ev(db, oid, 'system', 'DEMO_RESET', note='回放演示票复位：物理状态恢复、控制权回收')
            topo = load_topo(db)
            try:
                b = topo.derive_boundary(req.work_area)
            except ValueError as e:
                raise HTTPException(400, str(e))
            conflict = check_control_conflict(db, b['control'])
            if conflict:
                raise HTTPException(409, f"设备 {conflict['device']} 已受票据 {conflict['ticket']} 控制")
            tid = 'T' + time.strftime('%H%M%S') + uuid.uuid4().hex[:6].upper()
            db.x('INSERT INTO tickets(id,title,work_area,boundary,snapshot,status,frozen,freeze_reason,'
                 'created_by,created_at,permit_by,permit_at,write_owner) VALUES(?,?,?,?,?,?,0,NULL,?,?,NULL,NULL,?)',
                 (tid, '回放演示·' + '、'.join(req.work_area), json.dumps(b['work_area']),
                  json.dumps(b, ensure_ascii=False), topo.version, 'ISOLATING',
                  req.created_by, time.time(), req.team))
            for i, s in enumerate(domain.isolation_steps(b), 1):
                db.x('INSERT INTO steps(id,ticket,seq,phase,kind,device,point,sw,expected,desc) VALUES(?,?,?,?,?,?,?,?,?,?)',
                     (f'{tid}:{i}', tid, i, s['phase'], s['kind'], s['device'], s['point'], s['sw'], s['expected'], s['desc']))
            acquire_control(db, tid, b['control'])
            ev(db, tid, req.created_by, 'TICKET_CREATED', title='回放演示',
               work_area=b['work_area'], snapshot=topo.version, team=req.team)
            ev(db, tid, 'system', 'BOUNDARY_DERIVED',
               switches=[s['device'] for s in b['switches']],
               grounds=[g['id'] for g in b['grounds']], forbidden=b['forbidden'])
            try:
                for s in get_steps(db, tid):
                    if s['kind'] == domain.OPEN_SWITCH:
                        db.x("UPDATE devices SET state='OPEN' WHERE id=?", (s['device'],))
                    elif s['kind'] == domain.INSTALL_GROUND:
                        db.x('INSERT OR REPLACE INTO grounds VALUES(?,?,?)', (s['point'], s['sw'], tid))
                    ev(db, tid, 'sim', 'TELEMETRY_CHANGED', device=s['device'], expected=s['expected'])
                    report_core(db, tid, s['id'], '张三', s['device'])
                    verify_core(db, tid, s['id'], '李四')
            except RuleError as e:
                db.rollback()
                raise http_for(e)
            db.x("UPDATE tickets SET status='PERMITTED', permit_by=?, permit_at=? WHERE id=?",
                 (req.created_by, time.time(), tid))
            ev(db, tid, req.created_by, 'PERMIT_GRANTED', demo=True)
            # 两条班组现场证据（入账上链），供回放重建“现场证据”投影
            now_ms = int(time.time() * 1000)
            t = get_ticket(db, tid)
            for i, kind in enumerate(('开工检查', '边界复核'), 1):
                row = {'ticket': tid, 'client_id': f'{tid}:demo:{i}', 'team': req.team,
                       'device': 'FIELD', 'kind': kind, 'ts_data': now_ms + i,
                       'payload': f'演示证据{i}', 'base_seq': None}
                db.x('INSERT INTO shift_evidences(ticket,client_id,team,device,kind,ts_data,payload,'
                     "base_seq,status,created_at) VALUES(?,?,?,?,?,?,?,?,'NEW',?)",
                     (tid, row['client_id'], req.team, 'FIELD', kind, row['ts_data'],
                      row['payload'], None, time.time()))
                row['id'] = db.one('SELECT last_insert_rowid() i')['i']
                row = judge_and_apply(db, t, row)
                ev(db, tid, req.team, 'SHIFT_EVIDENCE', id=row['id'], client_id=row['client_id'],
                   device='FIELD', kind=kind, ts_data=row['ts_data'],
                   status=row['status'], reason=row['reason'],
                   handover=row.get('handover'), seq=row.get('seq'))
            seqs = [r['seq'] for r in db.q('SELECT seq FROM events WHERE ticket=?', (tid,))]
            db.commit()
            return {'ticket': tid, 'from_seq': min(seqs), 'to_seq': max(seqs),
                    'events': len(seqs), 'note': '演示票已许可开工，事件流可用于回放'}

    class TamperReq(BaseModel):
        seq: int
        mode: str                           # delete | corrupt | break_payload

    @app.post('/api/demo/tamper')
    def api_demo_tamper(req: TamperReq):
        """取证演示：直接改动审计库（删除事件/篡改内容/破坏载荷），
        用于演示回放核验的缺号停止、摘要错误拒绝与载荷无法应用停止。"""
        with db.lock:
            row = db.one('SELECT * FROM events WHERE seq=?', (req.seq,))
            if not row:
                raise HTTPException(404, f'事件 #{req.seq} 不存在')
            if req.mode == 'delete':
                db.x('DELETE FROM events WHERE seq=?', (req.seq,))
                note = f'事件 #{req.seq} 已删除（制造序号缺口）'
            elif req.mode == 'corrupt':
                db.x("UPDATE events SET actor=actor||'·改' WHERE seq=?", (req.seq,))
                note = f'事件 #{req.seq} 内容已篡改（摘要将失验）'
            elif req.mode == 'break_payload':
                db.x("UPDATE events SET payload='{' WHERE seq=?", (req.seq,))
                note = f'事件 #{req.seq} 载荷已破坏（无法应用）'
            else:
                raise HTTPException(400, 'mode 须为 delete/corrupt/break_payload')
            db.commit()
            return {'ok': True, 'note': note}

    # ---- 审计
    @app.get('/api/events')
    def api_events(since: int = 0, ticket: Optional[str] = None, limit: int = 200):
        sql = 'SELECT * FROM events WHERE seq>?'
        args = [since]
        if ticket:
            sql += ' AND ticket=?'
            args.append(ticket)
        sql += ' ORDER BY seq DESC LIMIT ?'
        args.append(limit)
        return [dict(r) for r in db.q(sql, args)]

    # ---- 模拟设备接口（仿真一次设备，供现场/调度/测试驱动）
    class SimSwitch(BaseModel):
        device: str
        state: str                   # OPEN | CLOSED
        ticket: Optional[str] = None
        pkg: Optional[str] = None    # 无网授权包：断网期间持有效授权操作本设备
        actor: str = 'sim'
        force: bool = False          # 模拟误操作/外力破坏闭锁

    @app.post('/sim/switch')
    def sim_switch(req: SimSwitch):
        with db.lock:
            topo = load_topo(db)
            if req.device not in topo.devices or topo.devices[req.device]['kind'] != 'switch':
                raise HTTPException(404, '开关不存在')
            if req.state not in ('OPEN', 'CLOSED'):
                raise HTTPException(400, '状态须为 OPEN/CLOSED')
            ctrl = db.one('SELECT ticket FROM control WHERE device=?', (req.device,))
            if ctrl and not req.force:
                if req.ticket != ctrl['ticket'] and not req.pkg:
                    db.rollback()
                    raise HTTPException(423, f"设备已闭锁，受票据 {ctrl['ticket']} 控制")
                if req.pkg:
                    ok, why = pkg_allows_switch(db, req.pkg, ctrl['ticket'], req.device, req.state)
                    if not ok:
                        db.rollback()
                        raise HTTPException(423, f'无网授权拒绝物理操作：{why}')
                    ev(db, ctrl['ticket'], req.actor, 'PKG_PHYSICAL_OP',
                       pkg=req.pkg, device=req.device, state=req.state)
                else:
                    t = get_ticket(db, req.ticket)
                    if not t['frozen']:  # 冻结票据允许恢复性操作
                        ok = any(s['device'] == req.device and s['expected'] == req.state
                                 and s['status'] in ('PENDING', 'REPORTED')
                                 and db.one('SELECT COUNT(*) c FROM steps WHERE ticket=? AND seq<? AND status!=?',
                                            (req.ticket, s['seq'], 'VERIFIED'))['c'] == 0
                                 for s in get_steps(db, req.ticket))
                        if not ok:
                            db.rollback()
                            raise HTTPException(423, '设备闭锁：无对应的当前操作步骤')
                    else:
                        ev(db, req.ticket, req.actor, 'RECOVERY_OP', device=req.device, state=req.state)
            if req.force:
                ev(db, ctrl['ticket'] if ctrl else None, req.actor, 'SIM_FORCE_OP',
                   device=req.device, state=req.state)
            # 带接地合闸 = 恶性误操作：非 force 拒绝
            if req.state == 'CLOSED':
                topo.devices[req.device]['state'] = 'CLOSED'
                hot_grounds = topo.grounded_points() & topo.energized()
                if hot_grounds and not req.force:
                    db.rollback()
                    raise HTTPException(409, f'合闸将导致接地点带电: {sorted(hot_grounds)}，拒绝')
            db.x('UPDATE devices SET state=? WHERE id=?', (req.state, req.device))
            ev(db, ctrl['ticket'] if ctrl else None, req.actor, 'TELEMETRY_CHANGED',
               device=req.device, state=req.state)
            revalidate_all(db, req.actor)
            storm.refresh_environments(db, req.actor, f'设备遥测变化（{req.device}={req.state}），复工依据重新核验')
            db.commit()
            return {'ok': True}

    class SimGround(BaseModel):
        point: str
        sw: str
        action: str                  # install | remove
        ticket: Optional[str] = None
        pkg: Optional[str] = None    # 无网授权包
        actor: str = 'sim'
        force: bool = False

    @app.post('/sim/ground')
    def sim_ground(req: SimGround):
        with db.lock:
            topo = load_topo(db)
            gid = f'GND:{req.sw}'
            ctrl = db.one('SELECT ticket FROM control WHERE device=?', (gid,))
            if ctrl and not req.force and req.ticket != ctrl['ticket'] and not req.pkg:
                raise HTTPException(423, f"接地点已闭锁，受票据 {ctrl['ticket']} 控制")
            if ctrl and req.pkg and not req.force:
                ok, why = pkg_allows_ground(db, req.pkg, ctrl['ticket'], req.point, req.sw, req.action)
                if not ok:
                    db.rollback()
                    raise HTTPException(423, f'无网授权拒绝物理操作：{why}')
                ev(db, ctrl['ticket'], req.actor, 'PKG_PHYSICAL_OP',
                   pkg=req.pkg, device=gid, action=req.action, point=req.point)
            if req.action == 'install':
                if req.point in topo.energized() and not req.force:
                    raise HTTPException(409, f'{req.point} 带电，禁止装设接地线')
                if req.point in topo.energized():
                    ev(db, req.ticket, req.actor, 'SAFETY_ALARM', detail=f'带电挂接地线: {req.point}')
                db.x('INSERT OR REPLACE INTO grounds VALUES(?,?,?)',
                     (req.point, req.sw, ctrl['ticket'] if ctrl else req.ticket))
            elif req.action == 'remove':
                db.x('DELETE FROM grounds WHERE point=? AND sw=?', (req.point, req.sw))
            else:
                raise HTTPException(400, 'action 须为 install/remove')
            ev(db, ctrl['ticket'] if ctrl else req.ticket, req.actor, 'TELEMETRY_CHANGED',
               device=gid, action=req.action, point=req.point)
            revalidate_all(db, req.actor)
            storm.refresh_environments(db, req.actor, f'接地状态变化（{gid} {req.action}），复工依据重新核验')
            db.commit()
            return {'ok': True}

    class SimTag(BaseModel):
        device: str
        tag: str
        action: str                  # add | remove
        actor: str = 'sim'
        force: bool = False

    @app.post('/sim/tag')
    def sim_tag(req: SimTag):
        with db.lock:
            topo = load_topo(db)
            if req.device not in topo.devices:
                raise HTTPException(404, '设备不存在')
            is_system = '·T' in req.tag
            if is_system and not req.force:
                raise HTTPException(423, '系统标示牌受票据保护，禁止擅自摘除')
            set_tag(db, req.device, req.tag, req.action == 'add')
            ev(db, None, req.actor, 'TAG_CHANGED', device=req.device, tag=req.tag, action=req.action)
            revalidate_all(db, req.actor)
            storm.refresh_environments(db, req.actor, f'标示牌状态变化（{req.device} {req.action}），复工依据重新核验')
            db.commit()
            return {'ok': True}

    class SimTopo(BaseModel):
        op: str                      # add_edge | remove_edge
        a: str
        b: str
        actor: str = 'sim'

    @app.post('/sim/topology')
    def sim_topo(req: SimTopo):
        with db.lock:
            if req.op == 'add_edge':
                db.x('INSERT OR IGNORE INTO edges VALUES(?,?)', (req.a, req.b))
            elif req.op == 'remove_edge':
                db.x('DELETE FROM edges WHERE (a=? AND b=?) OR (a=? AND b=?)', (req.a, req.b, req.b, req.a))
            else:
                raise HTTPException(400, 'op 须为 add_edge/remove_edge')
            v = int(db.meta('topo_version')) + 1
            db.set_meta('topo_version', v)
            ev(db, None, req.actor, 'TOPOLOGY_CHANGED', op=req.op, a=req.a, b=req.b, version=v)
            # 边界版本改变：全部在途无网授权包作废（旧片段按影响范围拒收，已采纳部分保留）
            for r in db.q("SELECT DISTINCT ticket FROM off_pkgs WHERE status IN ('ACTIVE','PARTIAL')"):
                void_pkgs_for_ticket(db, r['ticket'],
                                     lambda p: p['snapshot'] != v and p['status'] in PKG_LIVE_STATES,
                                     f'边界版本改变（拓扑变更至 v{v}），旧授权作废', req.actor)
            revalidate_all(db, req.actor)
            storm.refresh_environments(db, req.actor, f'网络拓扑变更至 v{v}，旧复工确认失效')
            db.commit()
            return {'ok': True, 'version': v}

    @app.get('/sim/state')
    def sim_state():
        return api_topo()

    # ============================================================== 工器具借还台
    def tool_err(e):
        return HTTPException(e.http_status, {'reason_code': e.reason_code, 'detail': e.detail})

    @app.get('/api/tools/requirements/{tid}')
    def api_tool_requirements(tid: str):
        with db.lock:
            try:
                return tools_mod.derive_requirements(db, tid)
            except tools_mod.RuleError as e:
                raise tool_err(e)

    @app.get('/api/tools/board')
    def api_tool_board():
        with db.lock:
            return tools_mod.board_view(db)

    @app.get('/api/tools/inventory')
    def api_tool_inventory():
        with db.lock:
            return {'batches': [dict(r) for r in db.q('SELECT * FROM tool_batches ORDER BY code,batch_id')],
                    'tools': [dict(r) for r in db.q(
                        'SELECT code,serial,batch_id,state,wear,seal_ok,metro_frozen,freeze_reason '
                        'FROM tools ORDER BY code,serial')]}

    class LockReq(BaseModel):
        serials: Optional[dict] = None       # {CODE: [serial,...]} 指定编号锁定（可省略自动选批）
        actor: str = '库管员'
        partial: bool = False                # 只锁定显式编号（资格/召回按器具演示用）

    @app.post('/api/tickets/{tid}/tool-locks', status_code=201)
    def api_tool_lock(tid: str, req: LockReq):
        with db.lock:
            try:
                return tools_mod.lock_core(db, tid, req.serials, req.actor, req.partial)
            except tools_mod.RuleError as e:
                db.rollback()
                raise tool_err(e)

    class UnlockReq(BaseModel):
        actor: str = '库管员'
        reason: Optional[str] = None

    @app.delete('/api/tickets/{tid}/tool-locks')
    def api_tool_unlock(tid: str, req: UnlockReq):
        with db.lock:
            try:
                return tools_mod.release_ticket_locks(
                    db, tid, req.actor, req.reason or '库管员手动释放批次锁定（库存回滚）')
            except tools_mod.RuleError as e:
                db.rollback()
                raise tool_err(e)

    @app.get('/api/tickets/{tid}/tool-locks')
    def api_tool_lock_view(tid: str):
        with db.lock:
            return {'ticket': tid, 'lines': tools_mod.lock_view(db, tid)}

    class BoxReq(BaseModel):
        box_id: Optional[str] = None
        seal: str = 'S-OK'
        deadline_hours: Optional[float] = 8
        actor: str = '库管员'

    @app.post('/api/tickets/{tid}/tool-boxes', status_code=201)
    def api_box_create(tid: str, req: BoxReq):
        with db.lock:
            try:
                return tools_mod.create_box_core(
                    db, tid, req.box_id, req.seal, req.deadline_hours, req.actor)
            except tools_mod.RuleError as e:
                db.rollback()
                raise tool_err(e)

    @app.get('/api/tool-boxes/{bid}')
    def api_box_view(bid: str):
        with db.lock:
            try:
                v = tools_mod.box_view(db, bid)
                v['reasons'] = [dict(r) for r in db.q(
                    'SELECT scene,reason_code,detail,actor,created_at FROM tool_reasons WHERE box=? ORDER BY rowid',
                    (bid,))]
                return v
            except tools_mod.RuleError as e:
                raise tool_err(e)

    class ScanReq(BaseModel):
        client_id: str = 'PDA-01'
        serial: str
        box: Optional[str] = None
        key: Optional[str] = None

    @app.post('/api/tool-boxes/{bid}/scan')
    def api_scan(bid: str, req: ScanReq):
        secret = tools_mod.get_secret(db)
        return tools_mod.scan_online_core(db, secret, req.client_id, bid, req.serial, req.key)

    class FlushItem(BaseModel):
        client_seq: int
        box: str
        serial: str
        scanned_at: Optional[int] = None

    class FlushReq(BaseModel):
        client_id: str = 'PDA-01'
        items: list[FlushItem]

    @app.post('/api/tool-scans/flush')
    def api_flush(req: FlushReq):
        secret = tools_mod.get_secret(db)
        items = [i.model_dump() for i in req.items]
        return tools_mod.scan_flush_core(db, secret, req.client_id, items)

    @app.get('/api/tool-scans/{client_id}')
    def api_scan_events(client_id: str):
        with db.lock:
            rows = db.q('SELECT client_seq,box,serial,status,result,received_at FROM scan_events '
                        'WHERE client_id=? ORDER BY client_seq', (client_id,))
            return [{'client_seq': r['client_seq'], 'box': r['box'], 'serial': r['serial'],
                     'status': r['status'], 'result': json.loads(r['result']),
                     'received_at': r['received_at']} for r in rows]

    class HandoverReq(BaseModel):
        leader: str
        team: str
        seal_intact: bool = True
        actor: Optional[str] = None

    @app.post('/api/tool-boxes/{bid}/handover')
    def api_handover(bid: str, req: HandoverReq):
        try:
            return tools_mod.handover_core(db, bid, req.leader, req.team, req.seal_intact, req.actor)
        except tools_mod.RuleError as e:
            raise tool_err(e)

    class ReturnItem(BaseModel):
        serial: str
        code: Optional[str] = None
        wear: str = 'OK'
        seal_ok: bool = True

    class ReturnReq(BaseModel):
        box_seal: Optional[str] = None
        items: list[ReturnItem]
        actor: str = '现场负责人'

    @app.post('/api/tool-boxes/{bid}/return')
    def api_return(bid: str, req: ReturnReq):
        try:
            return tools_mod.return_core(db, bid, [i.model_dump() for i in req.items],
                                         req.box_seal, req.actor)
        except tools_mod.RuleError as e:
            db.rollback()
            raise tool_err(e)

    class ReviewReq(BaseModel):
        action: str                       # CLEAR | CONFIRM_LOSS
        actor: str = '库管员'

    @app.post('/api/tool-boxes/{bid}/review')
    def api_review(bid: str, req: ReviewReq):
        try:
            return tools_mod.resolve_review_core(db, bid, req.action, req.actor)
        except tools_mod.RuleError as e:
            db.rollback()
            raise tool_err(e)

    class AbortReq(BaseModel):
        actor: str = '库管员'
        reason: Optional[str] = None

    @app.post('/api/tool-boxes/{bid}/abort')
    def api_box_abort(bid: str, req: AbortReq):
        try:
            return tools_mod.abort_box_core(db, bid, req.actor,
                                            req.reason or '库管员作废工具箱并回滚库存')
        except tools_mod.RuleError as e:
            db.rollback()
            raise tool_err(e)

    @app.post('/api/tools/repair/{serial}')
    def api_tool_repair(serial: str, req: ActorReq):
        with db.lock:
            try:
                return tools_mod.repair_item_core(db, serial, req.actor)
            except tools_mod.RuleError as e:
                db.rollback()
                raise tool_err(e)

    @app.get('/api/tool-boxes/{bid}/verify/{serial}')
    def api_verify_checksum(bid: str, serial: str):
        with db.lock:
            try:
                return tools_mod.verify_checksum(db, bid, serial)
            except tools_mod.RuleError as e:
                raise tool_err(e)

    # ============================================================== 计量检定与批次召回
    def metro_err(e):
        return HTTPException(e.http_status, {'reason_code': e.reason_code, 'detail': e.detail})

    @app.get('/api/metro/board')
    def api_metro_board():
        with db.lock:
            return metro.board_view(db)

    @app.get('/api/metro/tools/{serial}')
    def api_metro_tool(serial: str):
        with db.lock:
            if not db.one('SELECT 1 FROM tools WHERE serial=?', (serial,)):
                raise HTTPException(404, f'器具 {serial} 不存在')
            return metro.cert_history(db, serial)

    @app.get('/api/metro/certificates')
    def api_metro_certs(serial: Optional[str] = None, batch_id: Optional[str] = None):
        with db.lock:
            sql = 'SELECT * FROM metro_certs'
            args, cond = [], []
            if serial:
                cond.append('serial=?'); args.append(serial)
            if batch_id:
                cond.append('batch_id=?'); args.append(batch_id)
            if cond:
                sql += ' WHERE ' + ' AND '.join(cond)
            sql += ' ORDER BY serial, ver'
            return [metro._fmt_cert(dict(r)) for r in db.q(sql, args)]

    class CertReq(BaseModel):
        serial: str
        valid_from: Optional[int] = None       # 生效起点（默认当前；演示历史证书可显式前推）
        valid_days: float = 365.0
        valid_to: Optional[int] = None
        voltage_min: float = 0.4
        voltage_max: float = 35.0
        note: str = ''
        actor: str = '检定员'
        force_amend: bool = False       # 仅允许更正从未使用的当前证书

    @app.post('/api/metro/certificates', status_code=201)
    def api_cert_register(req: CertReq):
        with db.lock:
            try:
                now = int(time.time() * 1000)
                vf = req.valid_from or now
                vt = req.valid_to if req.valid_to is not None else vf + int(req.valid_days * 86400_000)
                resp = metro.register_cert_core(
                    db, req.serial, vf, vt, req.voltage_min, req.voltage_max,
                    req.actor, req.note, req.force_amend)
                db.commit()
                return JSONResponse(resp, status_code=200 if resp.get('amended') else 201)
            except metro.RuleError as e:
                db.rollback()
                raise metro_err(e)

    class CertStatusReq(BaseModel):
        cert_id: str
        serial: str
        actor: str = '检定员'
        reason: str = ''

    @app.post('/api/metro/certificates/suspend')
    def api_cert_suspend(req: CertStatusReq):
        with db.lock:
            try:
                resp = metro.suspend_cert_core(db, req.cert_id, req.serial, req.actor, req.reason)
                db.commit()
                return resp
            except metro.RuleError as e:
                db.rollback()
                raise metro_err(e)

    @app.post('/api/metro/certificates/resume')
    def api_cert_resume(req: CertStatusReq):
        with db.lock:
            try:
                resp = metro.resume_cert_core(db, req.cert_id, req.serial, req.actor,
                                              req.reason or '复检合格恢复')
                db.commit()
                return resp
            except metro.RuleError as e:
                db.rollback()
                raise metro_err(e)

    @app.get('/api/tickets/{tid}/tool-eligibility')
    def api_tool_eligibility(tid: str, code: Optional[str] = None):
        """准备清单资格预览：对该票所需型号的库内可发（FREE）件逐件给出可核对的通过/拒绝原因。"""
        with db.lock:
            try:
                req = tools_mod.derive_requirements(db, tid)
                serials = {}
                for line in req['lines']:
                    if code and line['code'] != code:
                        continue
                    rows = db.q("SELECT serial FROM tools WHERE code=? AND state='FREE' ORDER BY serial",
                                (line['code'],))
                    serials[line['code']] = [r['serial'] for r in rows]
                return metro.evaluate_for_ticket(db, tid, serials)
            except metro.RuleError as e:
                raise metro_err(e)
            except tools_mod.RuleError as e:
                raise tool_err(e)

    class RecallReq(BaseModel):
        scope_type: str                       # SERIAL | BATCH | CERT
        scope_value: list[str]
        reason: str
        actor: str = '质量负责人'
        key: Optional[str] = None

    @app.post('/api/metro/recalls', status_code=201)
    def api_recall_create(req: RecallReq):
        with db.lock:
            try:
                view = metro.recall_core(db, req.scope_type, req.scope_value, req.reason,
                                         req.actor, idem_key=req.key)
                db.commit()
                return view
            except metro.RuleError as e:
                db.rollback()
                raise metro_err(e)

    @app.get('/api/metro/recalls')
    def api_recall_list():
        with db.lock:
            return [metro.recall_view(db, r['id'])
                    for r in db.q('SELECT id FROM recalls ORDER BY created_at')]

    @app.get('/api/metro/recalls/{rid}')
    def api_recall_get(rid: str):
        with db.lock:
            try:
                return metro.recall_view(db, rid)
            except metro.RuleError as e:
                raise metro_err(e)

    class RecheckItem(BaseModel):
        serial: str
        result: str                           # PASS | FAIL
        note: Optional[str] = None

    class RecheckReq(BaseModel):
        results: list[RecheckItem]
        actor: str = '质量负责人'
        note: str = ''

    @app.post('/api/metro/recalls/{rid}/rechecks', status_code=201)
    def api_recheck_create(rid: str, req: RecheckReq):
        with db.lock:
            try:
                resp = metro.create_recheck_core(db, rid, [i.model_dump() for i in req.results],
                                                 req.actor, req.note)
                db.commit()
                return resp
            except metro.RuleError as e:
                db.rollback()
                raise metro_err(e)

    class ReleaseReq(BaseModel):
        recheck_id: str
        actor: str = '质量负责人'
        serials: Optional[list[str]] = None

    @app.post('/api/metro/recalls/{rid}/release')
    def api_recall_release(rid: str, req: ReleaseReq):
        with db.lock:
            try:
                resp = metro.release_core(db, rid, req.recheck_id, req.actor, req.serials)
                db.commit()
                return resp
            except metro.RuleError as e:
                db.rollback()
                raise metro_err(e)

    @app.get('/api/metro/rechecks')
    def api_recheck_list():
        with db.lock:
            return [dict(r) for r in db.q('SELECT * FROM metro_rechecks ORDER BY created_at')]

    @app.post('/api/demo/metro/reset')
    def api_demo_metro_reset():
        """检定/召回演示复位（仅清理演示数据，业务接口不提供此类强制操作）：
        撤回演示票、作废/回收演示箱、解除全部召回、恢复器具与证书版本到种子基线。"""
        with db.lock:
            for bx in db.q("SELECT id FROM toolboxes WHERE id LIKE 'BXR%' OR id IN "
                           "('BXRC','BXCF','BXFRZ','BXFD','BXAR','BXA','BXB','BXCV','BXSP')"):
                bid = bx['id']
                b = db.one('SELECT * FROM toolboxes WHERE id=?', (bid,))
                if not b or b['status'] in ('RETURNED', 'CANCELLED'):
                    continue
                for co in db.q('SELECT serial FROM checkouts WHERE box=?', (bid,)):
                    db.x("UPDATE tools SET state=? WHERE serial=? AND metro_frozen=0",
                         ('FREE', co['serial']))
                db.x("UPDATE toolboxes SET status='CANCELLED', holder=NULL, holder_team=NULL WHERE id=?",
                     (bid,))
            for old in db.q("SELECT id FROM tickets WHERE title LIKE '检定演示·%' "
                            "AND status IN ('ISOLATING','PERMITTED','RESTORING','WITHDRAWN')"):
                db.x("UPDATE tool_locks SET state='RELEASED', metro_state='OK', metro_reason=NULL "
                     "WHERE ticket=?", (old['id'],))
                db.x('DELETE FROM control WHERE ticket=?', (old['id'],))
                db.x("UPDATE tickets SET status='WITHDRAWN', frozen=0 WHERE id=?", (old['id'],))
            # 解除全部召回并恢复器具
            for rid in [r['id'] for r in db.q("SELECT id FROM recalls WHERE status='ACTIVE'")]:
                for sn in [r['serial'] for r in db.q(
                        "SELECT serial FROM recall_items WHERE recall=? AND restored=0 "
                        "AND disposition IN ('QUARANTINED','FIELD_FROZEN','LOCK_REVOKED')", (rid,))]:
                    db.x("UPDATE tools SET state='FREE', metro_frozen=0, freeze_reason=NULL, "
                         "wear='OK', seal_ok=1 WHERE serial=?", (sn,))
                    db.x("UPDATE tool_locks SET metro_state='OK', metro_reason=NULL "
                         "WHERE item_serial=? AND metro_state='DISQUALIFIED'", (sn,))
                    db.x('UPDATE recall_items SET restored=1, restored_at=? WHERE recall=? AND serial=?',
                         (time.time(), rid, sn))
                db.x("UPDATE recalls SET status='RELEASED', released_at=? WHERE id=?",
                     (time.time(), rid))
            # 证书恢复到种子基线：保留 v1，删除换版行并把 v1 复位
            for it in db.q('SELECT serial FROM tools WHERE code IN (?,?)', metro.METRO_CODES):
                sn = it['serial']
                db.x("DELETE FROM metro_certs WHERE serial=? AND ver>1", (sn,))
                db.x("UPDATE metro_certs SET status='ACTIVE', used=0, used_at=NULL WHERE serial=?",
                     (sn,))
                db.x("UPDATE tools SET state='FREE', metro_frozen=0, freeze_reason=NULL, "
                     "wear='OK', seal_ok=1 WHERE serial=?", (sn,))
            db.commit()
            return {'ok': True, 'note': '演示数据已复位：召回全部解除、器具回库、证书回到种子 v1'}

    # ============================================================== 雷暴撤离与复工闸门
    class StormAlertReq(BaseModel):
        ticket: str
        severity: str = 'THUNDERSTORM_RED'
        title: str = '雷暴红色预警命中作业区域'
        areas: list[str] = []
        actor: str = '值班长'
        reason: Optional[str] = None
        auto_order: bool = False
        field_leader: str = '陈七'

    class StormOrderReq(BaseModel):
        alert: Optional[str] = None
        field_leader: str = '陈七'
        issued_by: str = '值班长'

    class StormClearReq(BaseModel):
        actor: str = '气象值班员'

    class StormCheckinReq(BaseModel):
        person: str
        shelter: str
        claimed_by: Optional[str] = None
        safe: bool = True
        key: Optional[str] = None

    class StormCorrectReq(BaseModel):
        person: str
        shelter: str
        key: Optional[str] = None

    class StormGuardReq(BaseModel):
        risk: str
        guardian: str
        shelter: Optional[str] = None
        key: Optional[str] = None

    class StormToolReq(BaseModel):
        serial: str
        location: str
        held_by: Optional[str] = None
        shelter: Optional[str] = None
        actor: Optional[str] = None
        key: Optional[str] = None
        force_correct: bool = False

    class StormActorReq(BaseModel):
        actor: str
        key: Optional[str] = None

    @app.get('/api/storm/board')
    def storm_board():
        with db.lock:
            return storm.board_view(db)

    @app.get('/api/storm/batches/{bid}')
    def storm_batch(bid: str):
        with db.lock:
            return storm.batch_view(db, bid)

    @app.get('/api/storm/decisions')
    def storm_decisions(batch: Optional[str] = None):
        with db.lock:
            return storm.decisions(db, batch)

    @app.post('/api/storm/alerts', status_code=201)
    def storm_alert(req: StormAlertReq):
        t = get_ticket(db, req.ticket)
        areas = req.areas or t['work_area']
        return storm.create_alert_core(db, req.ticket, req.severity, req.title, areas,
                                       req.actor, req.reason, False)

    @app.post('/api/storm/alerts/{aid}/clear')
    def storm_clear(aid: str, req: StormClearReq):
        return storm.clear_alert_core(db, aid, req.actor)

    @app.post('/api/tickets/{tid}/storm-evacuation')
    def storm_order(tid: str, req: StormOrderReq):
        return storm.issue_order_core(db, tid, req.alert, req.field_leader, req.issued_by)

    @app.post('/api/storm/demo/alert-order')
    def storm_demo_alert_order(req: StormAlertReq):
        t = get_ticket(db, req.ticket)
        areas = req.areas or t['work_area']
        return storm.create_alert_core(db, req.ticket, req.severity, req.title, areas,
                                       req.actor, req.reason, True)

    @app.post('/api/storm/batches/{bid}/checkins')
    def storm_checkin(bid: str, req: StormCheckinReq):
        return storm.checkin_core(db, bid, req.person, req.shelter, req.claimed_by, req.key, req.safe)

    @app.post('/api/storm/batches/{bid}/correct-destination')
    def storm_correct(bid: str, req: StormCorrectReq):
        return storm.correct_destination_core(db, bid, req.person, req.shelter, req.key)

    @app.post('/api/storm/batches/{bid}/risk-guards')
    def storm_guard(bid: str, req: StormGuardReq):
        return storm.assign_guard_core(db, bid, req.risk, req.guardian, req.shelter, req.key)

    @app.post('/api/storm/batches/{bid}/tool-reports')
    def storm_tool(bid: str, req: StormToolReq):
        return storm.report_tool_core(db, bid, req.serial, req.location, req.held_by,
                                      req.shelter, req.actor, req.key, req.force_correct)

    @app.post('/api/storm/batches/{bid}/field-confirm')
    def storm_field(bid: str, req: StormActorReq):
        return storm.field_confirm_core(db, bid, req.actor, req.key)

    @app.post('/api/storm/batches/{bid}/dispatch-confirm')
    def storm_dispatch(bid: str, req: StormActorReq):
        return storm.dispatch_confirm_core(db, bid, req.actor, req.key)

    @app.post('/api/storm/batches/{bid}/resume')
    def storm_resume(bid: str, req: StormClearReq):
        return storm.resume_core(db, bid, req.actor)

    # ---- 页面
    import os
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')

    @app.get('/')
    def page_dispatcher():
        return FileResponse(os.path.join(static_dir, 'dispatcher.html'))

    @app.get('/field')
    def page_field():
        return FileResponse(os.path.join(static_dir, 'field.html'))

    @app.get('/shift')
    def page_shift():
        return FileResponse(os.path.join(static_dir, 'shift.html'))

    @app.get('/replay')
    def page_replay():
        return FileResponse(os.path.join(static_dir, 'replay.html'))

    @app.get('/report')
    def page_report():
        return FileResponse(os.path.join(static_dir, 'report.html'))

    @app.get('/voucher')
    def page_voucher():
        return FileResponse(os.path.join(static_dir, 'voucher.html'))

    @app.get('/tools')
    def page_tools():
        return FileResponse(os.path.join(static_dir, 'tools.html'))

    @app.get('/metro')
    def page_metro():
        return FileResponse(os.path.join(static_dir, 'metro.html'))

    @app.get('/storm')
    def page_storm():
        return FileResponse(os.path.join(static_dir, 'storm.html'))

    @app.get('/storm-mobile')
    def page_storm_mobile():
        return FileResponse(os.path.join(static_dir, 'storm_mobile.html'))

    app.mount('/static', StaticFiles(directory=static_dir), name='static')
    app.state.db = db
    return app


app = create_app()

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
