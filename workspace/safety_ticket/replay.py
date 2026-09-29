"""审计事件回放与事故分析引擎（纯函数模块，不依赖 Web 框架）。

核心概念：
- 投影还原：从追加式审计事件逐条重建某时点的票据阶段、设备控制权与现场证据，
  每条事件给出前后状态，绝不跳过无法应用的事件拼出看似完整的状态。
- 来源固定：回放创建时对来源窗口内全部事件逐条取摘要并固定来源摘要（source_digest）
  与事件上界（to_seq）；之后新增的事件序号必然越过上界，改变不了既有回放。
- 停止规则：序号缺口 / 摘要校验失败 / 事件载荷无法应用，回放都停在最后一个
  有效事件并指出具体位置（halt.position）。
- 假设分支：在任一有效事件上试算一项撤销（UNDO_EVENT）或设备状态变更
  （DEVICE_STATE），输出对后续控制权与工序状态的差异；分支只读正式事件，不写回。
"""
import hashlib
import json

# 回放停止原因码
HALT_GAP = 'GAP'                  # 序号缺口
HALT_DIGEST = 'DIGEST_MISMATCH'   # 摘要校验失败
HALT_APPLY = 'APPLY_FAILED'       # 事件载荷无法应用

# 假设分支试算操作
OP_UNDO = 'UNDO_EVENT'            # 撤销一条已应用事件的效果
OP_DEVICE = 'DEVICE_STATE'        # 变更一台设备状态（开关分合 / 接地线装拆）


class ApplyError(Exception):
    """事件载荷无法应用于投影（JSON 损坏 / 缺字段 / 类型未知）。"""


# ---------------------------------------------------------------- 摘要与来源固定

def _canonical(obj):
    """剔除 None、字典按键排序（与 server.canonical 口径一致）；0/False 保留。"""
    if isinstance(obj, dict):
        return {k: _canonical(obj[k]) for k in sorted(obj) if obj[k] is not None}
    if isinstance(obj, (list, tuple)):
        return [_canonical(v) for v in obj]
    return obj


def canonical(obj):
    return json.dumps(_canonical(obj), separators=(',', ':'), ensure_ascii=False)


def event_hash(row):
    """单条审计事件摘要：对 (seq, ts, ticket, actor, type, payload) 规范化取 SHA256 前 16 位。
    payload 为数据库存储的原始字符串，逐字节参与摘要，任何篡改都会失验。"""
    return hashlib.sha256(canonical({
        'seq': row['seq'], 'ts': row['ts'], 'ticket': row['ticket'],
        'actor': row['actor'], 'type': row['type'], 'payload': row['payload'],
    }).encode()).hexdigest()[:16]


def source_digest(pins):
    """来源摘要：固定来源窗口内全部事件 {seq, hash} 序列的整体摘要。"""
    return hashlib.sha256(canonical(pins).encode()).hexdigest()[:16]


# ---------------------------------------------------------------- 状态投影

def new_projection():
    """回放投影：票据阶段 / 冻结 / 写权班组 / 工序 / 控制权 / 设备状态 / 接地线 / 现场证据。"""
    return {
        'status': None,              # 票据阶段 ISOLATING/PERMITTED/RESTORING/CLOSED/WITHDRAWN
        'frozen': False,
        'freeze_reason': None,
        'write_owner': None,         # 当前持权班组
        'steps': {},                 # 步骤 id -> {status, kind, device, reporter, verifier}
        'control': {},               # 设备 -> 0/1（1=有物理残留）
        'devices': {},               # 开关设备 -> OPEN/CLOSED（来自遥测事件）
        'grounds': {},               # 接地线 id -> True/False
        'evidence': {'shift_accepted': 0, 'shift_head': 0, 'pkg_accepted': 0,
                     'suspended': 0, 'rejected': 0},
        'handovers': {},             # 交班单 -> 阶段
        'packages': {},              # 授权包 -> 状态
    }


def _snapshot(proj):
    return json.loads(json.dumps(proj, ensure_ascii=False))


def _req(p, *keys):
    missing = [k for k in keys if k not in p]
    if missing:
        raise ApplyError(f'载荷缺少必需字段 {missing}')


def _phase_of(step_id):
    return 'REST' if ':R' in step_id else 'ISO'


def apply_event(proj, etype, actor, p):
    """把一条审计事件的效果应用到投影。p 为已解析的载荷 dict。
    无法识别/字段缺失时抛 ApplyError——调用方必须停下，不得跳过。"""
    if etype == 'TICKET_CREATED':
        _req(p, 'work_area')
        proj['status'] = 'ISOLATING'
        proj['write_owner'] = p.get('team') or '甲班'
        for d in p['work_area']:
            proj['control'].setdefault(d, 0)
    elif etype == 'BOUNDARY_DERIVED':
        _req(p, 'switches', 'grounds')
        for d in list(p['switches']) + list(p['grounds']):
            proj['control'].setdefault(d, 0)
    elif etype == 'STEP_REPORTED':
        _req(p, 'step', 'kind', 'device')
        s = proj['steps'].setdefault(p['step'], {})
        s.update(status='REPORTED', kind=p['kind'], device=p['device'], reporter=actor)
    elif etype == 'STEP_VERIFIED':
        _req(p, 'step', 'kind', 'device')
        s = proj['steps'].setdefault(p['step'], {})
        s.update(status='VERIFIED', kind=p['kind'], device=p['device'], verifier=actor)
        # 控制权物理标记：隔离步骤复核 → 物理残留；恢复步骤复核 → 残留解除
        proj['control'][p['device']] = 0 if _phase_of(p['step']) == 'REST' else 1
        if p['kind'] == 'OPEN_SWITCH':
            proj['devices'][p['device']] = 'OPEN'
        elif p['kind'] == 'CLOSE_SWITCH':
            proj['devices'][p['device']] = 'CLOSED'
        elif p['kind'] == 'INSTALL_GROUND':
            proj['grounds'][p['device']] = True
        elif p['kind'] == 'REMOVE_GROUND':
            proj['grounds'][p['device']] = False
    elif etype == 'RESTORE_UNDER_FREEZE':
        _req(p, 'step')
    elif etype == 'PERMIT_GRANTED':
        proj['status'] = 'PERMITTED'
    elif etype == 'WORK_FINISHED':
        proj['status'] = 'RESTORING'
    elif etype == 'TICKET_CLOSED':
        proj['status'] = 'CLOSED'
        proj['control'] = {}
    elif etype == 'TICKET_WITHDRAWN':
        _req(p, 'retained_physical')
        proj['status'] = 'WITHDRAWN'
        proj['frozen'] = False
        proj['freeze_reason'] = None
        proj['control'] = {d: 1 for d in p['retained_physical']}
    elif etype == 'CONTROL_PURGED':
        proj['control'] = {}
    elif etype == 'FREEZE':
        _req(p, 'reason')
        proj['frozen'] = True
        r = p['reason']
        proj['freeze_reason'] = '；'.join(r) if isinstance(r, list) else str(r)
    elif etype == 'UNFREEZE':
        proj['frozen'] = False
        proj['freeze_reason'] = None
    elif etype == 'REBASELINE':
        _req(p, 'carried_verified', 'rebuilt')
        rebuilt = set(p['rebuilt'])
        for sid, s in proj['steps'].items():
            if _phase_of(sid) == 'ISO' and s.get('device') in rebuilt:
                proj['steps'][sid] = {'status': 'PENDING', 'kind': s.get('kind'),
                                      'device': s.get('device')}
    elif etype == 'HANDOVER_CREATED':
        _req(p, 'handover')
        proj['handovers'][p['handover']] = 'PENDING'
    elif etype == 'HANDOVER_SIGNED':
        _req(p, 'handover', 'to_team')
        proj['handovers'][p['handover']] = 'SIGNED'
        proj['write_owner'] = p['to_team']
    elif etype == 'HANDOVER_COMPLETED':
        _req(p, 'handover')
        proj['handovers'][p['handover']] = 'COMPLETED'
    elif etype == 'HANDOVER_CLOSED':
        _req(p, 'handover')
        proj['handovers'][p['handover']] = 'CLOSED'
    elif etype == 'SHIFT_EVIDENCE':
        _req(p, 'status')
        _apply_shift(proj, p)
    elif etype in ('SHIFT_EVIDENCE_WOKEN', 'SHIFT_EVIDENCE_REBASED'):
        _req(p, 'status')
        _apply_shift(proj, p)
    elif etype == 'PKG_ISSUED':
        _req(p, 'pkg')
        proj['packages'][p['pkg']] = 'ACTIVE'
    elif etype == 'PKG_EVIDENCE_ACCEPTED':
        _req(p, 'pkg', 'seq')
        proj['evidence']['pkg_accepted'] += 1
    elif etype == 'PKG_PHYSICAL_OP':
        _req(p, 'pkg', 'device')
        if 'state' in p:
            proj['devices'][p['device']] = p['state']
    elif etype == 'PKG_VOIDED':
        _req(p, 'pkg')
        proj['packages'][p['pkg']] = 'EXPIRED' if p.get('code') == 'AUTH_EXPIRED' else 'VOID'
    elif etype == 'PKG_COMPLETED':
        _req(p, 'pkg')
        proj['packages'][p['pkg']] = 'COMPLETED'
    elif etype == 'PKG_SUPERSEDED':
        _req(p, 'pkg')
        proj['packages'][p['pkg']] = 'SUPERSEDED'
    elif etype == 'PKG_SUBMIT':
        _req(p, 'pkg')
    elif etype == 'TELEMETRY_CHANGED':
        _req(p, 'device')
        if 'state' in p:
            proj['devices'][p['device']] = p['state']
        if p.get('action') in ('install', 'remove'):
            proj['grounds'][p['device']] = p['action'] == 'install'
    elif etype in ('SIM_FORCE_OP', 'RECOVERY_OP'):
        _req(p, 'device', 'state')
        proj['devices'][p['device']] = p['state']
    elif etype == 'TAG_CHANGED':
        _req(p, 'device', 'tag', 'action')
    elif etype == 'TOPOLOGY_CHANGED':
        _req(p, 'version')
    elif etype == 'OFFLINE_SYNC':
        _req(p, 'total')
    elif etype == 'DUPLICATE_IGNORED':
        _req(p, 'key')
    elif etype == 'SAFETY_ALARM':
        _req(p, 'detail')
    elif etype == 'TICKET_REJECTED':
        _req(p, 'reason')
    elif etype == 'DEMO_RESET':
        pass
    elif etype == 'REPLAY_CREATED':
        _req(p, 'replay')
    elif etype == 'VOUCHER_SEALED':
        # 外发凭证事件只描述只读快照的外发管理，不改变正式票据/设备/控制权
        _req(p, 'voucher')
    elif etype in ('VOUCHER_CLAIMED', 'VOUCHER_CLAIM_RACE'):
        _req(p, 'voucher', 'receiver')
    elif etype in ('VOUCHER_VOIDED', 'VOUCHER_EXPIRED'):
        _req(p, 'voucher')
    else:
        raise ApplyError(f'未知事件类型 {etype}，回放器无法应用，拒绝跳过')


def _apply_shift(proj, p):
    st = p['status']
    if st == 'ACCEPTED':
        proj['evidence']['shift_accepted'] += 1
        if p.get('seq'):
            proj['evidence']['shift_head'] = max(proj['evidence']['shift_head'], p['seq'])
    elif st == 'SUSPENDED':
        proj['evidence']['suspended'] += 1
    elif st == 'REJECTED':
        proj['evidence']['rejected'] += 1


def summarize(etype, actor, p):
    """事件一句话摘要，供回放时间线展示。"""
    if etype == 'TICKET_CREATED':
        return f"{actor} 创建票据，工作区 {p.get('work_area')}"
    if etype == 'BOUNDARY_DERIVED':
        return f"推导安全边界：开关 {p.get('switches')}，接地线 {p.get('grounds')}"
    if etype == 'STEP_REPORTED':
        return f"{actor} 回报步骤 {p.get('step')}（{p.get('kind')} {p.get('device')}）"
    if etype == 'STEP_VERIFIED':
        return f"{actor} 复核步骤 {p.get('step')}（{p.get('kind')} {p.get('device')}）"
    if etype == 'PERMIT_GRANTED':
        return f'{actor} 许可开工'
    if etype == 'WORK_FINISHED':
        return f'{actor} 收工，进入恢复送电'
    if etype == 'TICKET_CLOSED':
        return '恢复送电完成，票据关闭，控制权全部释放'
    if etype == 'TICKET_WITHDRAWN':
        return f"{actor} 撤回票据，残留锁定 {p.get('retained_physical')}"
    if etype == 'FREEZE':
        return f"票据冻结：{p.get('reason')}"
    if etype == 'UNFREEZE':
        return f'{actor} 解冻票据'
    if etype == 'HANDOVER_CREATED':
        return f"开具交班单 {p.get('handover')}（水位 {p.get('watermark')}）"
    if etype == 'HANDOVER_SIGNED':
        return f"{p.get('to_team')} 签收交班单，写权原子转移"
    if etype == 'HANDOVER_COMPLETED':
        return '承接方首条内容入账，换班接续完成'
    if etype == 'HANDOVER_CLOSED':
        return f"交班单关闭：{p.get('reason')}"
    if etype == 'SHIFT_EVIDENCE':
        return f"现场证据（{p.get('team', actor)} {p.get('kind', '')}）→ {p.get('status')}"
    if etype == 'TELEMETRY_CHANGED':
        return f"遥测变位 {p.get('device')} {p.get('state', p.get('action', ''))}"
    if etype == 'SAFETY_ALARM':
        return f"安全告警：{p.get('detail')}"
    if etype == 'REPLAY_CREATED':
        return f"创建回放 {p.get('replay')}（事件上界 #{p.get('to_seq')}）"
    if etype == 'VOUCHER_SEALED':
        return (f"封存外发凭证 {p.get('voucher')}（#{p.get('from_seq')}~#{p.get('to_seq')}，"
                f"接收人 {p.get('receivers')}，只读快照）")
    if etype == 'VOUCHER_CLAIMED':
        return f"接收人 {p.get('receiver')} 首次领取凭证 {p.get('voucher')}（中签）"
    if etype == 'VOUCHER_CLAIM_RACE':
        return f"接收人 {p.get('receiver')} 并发领取凭证 {p.get('voucher')} 未中签（{p.get('winner')} 先得）"
    if etype == 'VOUCHER_VOIDED':
        return f"凭证 {p.get('voucher')} 主动作废：{p.get('reason')}"
    if etype == 'VOUCHER_EXPIRED':
        return f"凭证 {p.get('voucher')} 超过截止时刻失效"
    return f'{etype} by {actor}'


# ---------------------------------------------------------------- 回放构建 / 核验

def run_replay(ticket, from_seq, to_seq, events, pinned=None):
    """在事件流上构建（或核验）一段回放。

    参数：
      ticket    目标票据
      from_seq  展示窗口下界（之前本票据事件折叠为基线投影，同样计入来源固定）
      to_seq    事件上界（固定，之后新增事件不得进入回放）
      events    全部 seq<=to_seq 的事件行（任意票据，按 seq 升序，dict 列表）
      pinned    创建时固定的 [{seq, hash}]；传入则逐条核验来源是否被改动
    返回 {pinned, source_digest, baseline, entries, tip, applied, halt, status}。
    """
    by_seq = {e['seq']: e for e in events}
    pin_map = {p['seq']: p['hash'] for p in pinned} if pinned is not None else None

    # —— 来源核验预检：固定来源中的每个序号都必须仍然存在且摘要一致
    pin_halt = None
    if pin_map is not None:
        prev = 0
        for pseq in sorted(pin_map):
            row = by_seq.get(pseq)
            if row is None:
                pin_halt = (HALT_GAP, pseq, prev,
                            f'序号缺口：固定来源中的事件 #{pseq} 已不存在（可能被删除）')
                break
            if pin_map[pseq] != event_hash(row):
                pin_halt = (HALT_DIGEST, pseq, prev,
                            f'摘要校验失败：事件 #{pseq} 内容与固定来源摘要不符（可能被篡改）')
                break
            prev = pseq

    proj = new_projection()
    entries = []
    applied = []
    new_pins = []
    halt = None
    last_valid = 0

    def stop(code, position, detail):
        return {'code': code, 'position': position, 'at_seq': last_valid, 'detail': detail}

    def apply_row(row):
        """解析并应用一条本票据事件；失败即抛 ApplyError。"""
        try:
            payload = json.loads(row['payload'] or '{}')
        except (ValueError, TypeError):
            raise ApplyError(f"事件 #{row['seq']} 载荷不是合法 JSON，无法应用")
        if not isinstance(payload, dict):
            raise ApplyError(f"事件 #{row['seq']} 载荷结构非法，无法应用")
        try:
            apply_event(proj, row['type'], row['actor'], payload)
        except ApplyError as e:
            raise ApplyError(f"事件 #{row['seq']}（{row['type']}）{e}")
        return payload

    # —— 基线：from_seq 之前本票据事件，折叠为基线投影
    for e in events:
        seq = e['seq']
        if seq >= from_seq:
            break
        if e['ticket'] != ticket:
            continue
        if pin_halt and seq >= pin_halt[1]:
            halt = stop(pin_halt[0], pin_halt[1], pin_halt[3])
            break
        if pin_map is not None and seq not in pin_map:
            halt = stop(HALT_GAP, seq, f'序号缺口：基线事件 #{seq} 不在固定来源中')
            break
        try:
            apply_row(e)
        except ApplyError as ex:
            halt = stop(HALT_APPLY, seq, str(ex))
            break
        applied.append(seq)
        new_pins.append({'seq': seq, 'hash': event_hash(e)})
        last_valid = seq
    baseline = _snapshot(proj)

    # —— 展示窗口：from_seq..to_seq，逐条给出前后状态；窗口内每个序号都必须存在
    if halt is None:
        for seq in range(from_seq, to_seq + 1):
            if pin_halt and seq >= pin_halt[1]:
                halt = stop(pin_halt[0], pin_halt[1], pin_halt[3])
                break
            row = by_seq.get(seq)
            if row is None:
                halt = stop(HALT_GAP, seq,
                            f'序号缺口：事件 #{seq} 缺失（固定上界内序号必须连续）')
                break
            new_pins.append({'seq': seq, 'hash': event_hash(row)})
            if row['ticket'] != ticket:
                entries.append({'seq': seq, 'ts': row['ts'], 'type': row['type'],
                                'actor': row['actor'], 'ticket': row['ticket'],
                                'applied': False, 'summary': '非本票据事件，仅作来源固定',
                                'before': None, 'after': None})
                last_valid = seq
                continue
            before = _snapshot(proj)
            try:
                payload = apply_row(row)
            except ApplyError as ex:
                halt = stop(HALT_APPLY, seq, str(ex))
                break
            entries.append({'seq': seq, 'ts': row['ts'], 'type': row['type'],
                            'actor': row['actor'], 'ticket': row['ticket'],
                            'applied': True,
                            'summary': summarize(row['type'], row['actor'], payload),
                            'before': before, 'after': _snapshot(proj)})
            applied.append(seq)
            last_valid = seq

    # 核验预检命中但位置在已处理范围之外（如被删的基线事件）：收尾停止
    if halt is None and pin_halt is not None:
        halt = {'code': pin_halt[0], 'position': pin_halt[1],
                'at_seq': last_valid, 'detail': pin_halt[3]}

    return {'pinned': new_pins, 'source_digest': source_digest(new_pins),
            'baseline': baseline, 'entries': entries, 'tip': _snapshot(proj),
            'applied': applied, 'halt': halt,
            'status': 'HALTED' if halt else 'OK'}


# ---------------------------------------------------------------- 假设分支试算

def apply_device_state(proj, params):
    """设备状态变更假设：开关分合或接地线装拆（只改投影，不碰正式状态）。"""
    if 'ground' in params:
        proj['grounds'][params['ground']] = bool(params.get('installed'))
    else:
        _req(params, 'device', 'state')
        proj['devices'][params['device']] = params['state']


def run_hypothetical(events, base_seq, ops, valid_seqs):
    """在回放有效事件流上试算假设分支，返回上界处的假设投影。

    events     本票据事件行（seq 升序）
    base_seq   分支基点（设备状态变更在该事件应用之后生效）
    ops        [{op_type, params}]：UNDO_EVENT 撤销一条事件；DEVICE_STATE 变更设备状态
    valid_seqs 回放有效事件序号集合（越过停止点的事件不参与试算）
    """
    proj = new_projection()
    undone = {o['params'].get('seq') for o in ops if o['op_type'] == OP_UNDO}
    overrides = [o['params'] for o in ops if o['op_type'] == OP_DEVICE]
    valid = set(valid_seqs)
    override_done = False
    for e in events:
        seq = e['seq']
        if seq not in valid:
            continue
        if not override_done and seq > base_seq:
            for p in overrides:
                apply_device_state(proj, p)
            override_done = True
        if seq in undone:
            continue
        try:
            payload = json.loads(e['payload'] or '{}')
        except (ValueError, TypeError):
            continue                    # 正式回放已在该处停止，valid_seqs 不含它
        apply_event(proj, e['type'], e['actor'], payload)
        if seq == base_seq:
            for p in overrides:
                apply_device_state(proj, p)
            override_done = True
    if not override_done:
        for p in overrides:
            apply_device_state(proj, p)
    return proj


def _ctl_label(v):
    return None if v is None else ('受控·有物理残留' if v else '受控')


def diff_projection(official, hypo):
    """比较正式回放与假设分支在上界处的投影，输出控制权/工序状态等差异。"""
    diffs = []

    def add(aspect, target, a, b):
        diffs.append({'aspect': aspect, 'target': target,
                      'official': a, 'hypothetical': b})

    for key, label in (('status', '票据阶段'), ('frozen', '冻结标志'),
                       ('write_owner', '写权班组')):
        if official[key] != hypo[key]:
            add(label, key, official[key], hypo[key])
    for sid in sorted(set(official['steps']) | set(hypo['steps'])):
        a = (official['steps'].get(sid) or {}).get('status')
        b = (hypo['steps'].get(sid) or {}).get('status')
        if a != b:
            add('工序状态', sid, a, b)
    for d in sorted(set(official['control']) | set(hypo['control'])):
        a = official['control'].get(d)
        b = hypo['control'].get(d)
        if a != b:
            add('控制权', d, _ctl_label(a), _ctl_label(b))
    for d in sorted(set(official['devices']) | set(hypo['devices'])):
        a = official['devices'].get(d)
        b = hypo['devices'].get(d)
        if a != b:
            add('设备状态', d, a, b)
    for g in sorted(set(official['grounds']) | set(hypo['grounds'])):
        a = bool(official['grounds'].get(g))
        b = bool(hypo['grounds'].get(g))
        if a != b:
            add('接地线', g, a, b)
    for k in sorted(set(official['evidence']) | set(hypo['evidence'])):
        a = official['evidence'].get(k)
        b = hypo['evidence'].get(k)
        if a != b:
            add('现场证据', k, a, b)
    for h in sorted(set(official['handovers']) | set(hypo['handovers'])):
        a = official['handovers'].get(h)
        b = hypo['handovers'].get(h)
        if a != b:
            add('交班单', h, a, b)
    for p in sorted(set(official['packages']) | set(hypo['packages'])):
        a = official['packages'].get(p)
        b = hypo['packages'].get(p)
        if a != b:
            add('授权包', p, a, b)
    return diffs
