"""配电拓扑与安全边界推导（纯函数模块，不依赖 Web 框架）。

核心概念：
- Topo: 设备（电源/开关/线路/母线/配变）+ 连接关系 + 接地线 + 拓扑版本号
- derive_boundary: 由工作区推导必须断开的开关、接地线、禁止操作边界
- boundary_problems: 检修期间校验原安全边界是否仍然成立
"""
from collections import deque

SOURCE = 'source'        # 电源（主变低压侧）
SWITCH = 'switch'        # 开关（断路器/负荷开关/分段/联络）
LINE = 'line'            # 馈线段
BUS = 'bus'              # 母线
TRANS = 'transformer'    # 配电变压器

# 步骤类型
OPEN_SWITCH = 'OPEN_SWITCH'        # 断开开关
INSTALL_GROUND = 'INSTALL_GROUND'  # 验电后装设接地线
REMOVE_GROUND = 'REMOVE_GROUND'    # 拆除接地线
CLOSE_SWITCH = 'CLOSE_SWITCH'      # 合上开关

# 无网作业证据类型（每步固定四拍：扫码 → 设备实况 → 执行确认 → 监护确认）
EVID_SCAN = 'SCAN'
EVID_LIVE = 'LIVE'
EVID_EXEC = 'EXEC'
EVID_GUARD = 'GUARD'
STEP_EVIDENCE = (EVID_SCAN, EVID_LIVE, EVID_EXEC, EVID_GUARD)


def live_matches(step, device_state, grounded, energized):
    """设备实况记录是否与步骤的预期物理位置一致（执行后现场应观察到的状态）。"""
    if step['kind'] in (OPEN_SWITCH, CLOSE_SWITCH):
        return device_state == step['expected']
    if step['kind'] == INSTALL_GROUND:
        return bool(grounded) and not bool(energized)
    if step['kind'] == REMOVE_GROUND:
        return not bool(grounded)
    return False


class Topo:
    def __init__(self, devices, edges, grounds=(), version=1):
        # devices: {id: {kind, name, state, tags:[], x, y}}
        # edges:   [(a, b)]  电气连接
        # grounds: {(point, sw, ticket)}  point 处由 ticket 装设的接地线（对应边界开关 sw）
        self.devices = devices
        self.version = version
        self.adj = {i: set() for i in devices}
        for a, b in edges:
            self.adj.setdefault(a, set()).add(b)
            self.adj.setdefault(b, set()).add(a)
        self.grounds = set(grounds)

    def grounded_points(self):
        return {p for p, _, _ in self.grounds}

    def energized(self):
        """从所有电源出发、穿过闭合开关可达的设备集合（即带电设备）。"""
        seen, q = set(), deque()
        for i, d in self.devices.items():
            if d['kind'] == SOURCE:
                seen.add(i)
                q.append(i)
        while q:
            n = q.popleft()
            for m in self.adj.get(n, ()):
                if m in seen:
                    continue
                d = self.devices[m]
                if d['kind'] == SWITCH and d.get('state') != 'CLOSED':
                    continue
                seen.add(m)
                q.append(m)
        return seen

    def dist_from_source(self):
        """各设备到最近电源的物理电气距离（与开关分合无关），用于停电顺序排序。"""
        dist, q = {}, deque()
        for i, d in self.devices.items():
            if d['kind'] == SOURCE:
                dist[i] = 0
                q.append(i)
        while q:
            n = q.popleft()
            for m in self.adj.get(n, ()):
                if m not in dist:
                    dist[m] = dist[n] + 1
                    q.append(m)
        return dist

    def derive_boundary(self, work_area):
        """由工作区推导安全边界。

        从工作区向外扩展：线路/母线/配变自由穿过，遇到开关即为边界开关（必须断开），
        若不经开关直接碰到电源则无法隔离。返回边界开关、接地线、禁止操作集、控制设备集。
        """
        work = set(work_area)
        unknown = [w for w in work if w not in self.devices]
        if unknown:
            raise ValueError(f'未知设备: {unknown}')
        if not work:
            raise ValueError('工作区为空')
        for w in work:
            if self.devices[w]['kind'] == SOURCE:
                raise ValueError('工作区不能包含电源')

        visited = set(work)
        q = deque(sorted(work))
        boundary = []   # (开关, 工作区侧设备)
        direct = []     # 无开关直连的电源
        while q:
            n = q.popleft()
            for m in sorted(self.adj.get(n, ())):
                if m in visited:
                    continue
                d = self.devices[m]
                if d['kind'] == SWITCH:
                    boundary.append((m, n))
                    visited.add(m)          # 开关不穿越
                elif d['kind'] == SOURCE:
                    direct.append(m)
                else:
                    visited.add(m)
                    q.append(m)
        if direct:
            raise ValueError(f'工作区与电源 {direct} 之间无开关，无法隔离')

        dist = self.dist_from_source()
        # 停电顺序：先负荷侧（离电源远）后电源侧
        boundary.sort(key=lambda bs: (-dist.get(bs[0], 99), bs[0]))
        sw_set = {sw for sw, _ in boundary}
        grounds = [{'id': f'GND:{sw}', 'sw': sw, 'point': side} for sw, side in boundary]
        forbidden = ([{'device': sw, 'rule': 'MUST_STAY_OPEN', 'label': '禁止合闸'} for sw in sw_set] +
                     [{'device': g['id'], 'rule': 'MUST_STAY_GROUNDED', 'label': '禁止拆除'} for g in grounds])
        control = sorted(work | sw_set | {g['id'] for g in grounds})
        return {
            'work_area': sorted(work),
            'zone': sorted(visited - sw_set),           # 停电隔离区（含工作区）
            'switches': [{'device': sw, 'side': side, 'orig': self.devices[sw]['state']}
                         for sw, side in boundary],
            'grounds': grounds,
            'forbidden': forbidden,
            'control': control,
        }


def isolation_steps(boundary):
    """隔离阶段步骤：先按序断开全部边界开关，再按同序验电装接地线。"""
    steps = []
    for sw in boundary['switches']:
        steps.append({'phase': 'ISO', 'kind': OPEN_SWITCH, 'device': sw['device'],
                      'point': None, 'sw': None, 'expected': 'OPEN',
                      'desc': f"断开 {sw['device']}（{sw['side']} 侧工作）"})
    for g in boundary['grounds']:
        steps.append({'phase': 'ISO', 'kind': INSTALL_GROUND, 'device': g['id'],
                      'point': g['point'], 'sw': g['sw'], 'expected': 'GROUNDED',
                      'desc': f"在 {g['point']}（{g['sw']} 工作侧）验电并装设接地线"})
    return steps


def restoration_steps(iso_steps):
    """收工送电步骤：严格按隔离步骤的依赖逆序——先拆接地（逆序），后合开关（逆序）。"""
    out = []
    for s in reversed(iso_steps):
        if s['kind'] == OPEN_SWITCH:
            out.append({'phase': 'REST', 'kind': CLOSE_SWITCH, 'device': s['device'],
                        'point': None, 'sw': None, 'expected': 'CLOSED',
                        'desc': f"合上 {s['device']}"})
        elif s['kind'] == INSTALL_GROUND:
            out.append({'phase': 'REST', 'kind': REMOVE_GROUND, 'device': s['device'],
                        'point': s['point'], 'sw': s['sw'], 'expected': 'UNGROUNDED',
                        'desc': f"拆除 {s['point']} 处接地线"})
    return out


def tag_for(ticket_id, kind):
    return {'OPEN_SWITCH': '禁止合闸', 'INSTALL_GROUND': '已接地'}[kind] + f'·{ticket_id}'


# ---------------------------------------------------------------- 值守班组换班接续
#
# 交班单阶段
HO_PENDING = 'PENDING'        # 待签收：写权仍归移交方
HO_SIGNED = 'SIGNED'          # 已签收：写权已原子转移给承接方，待其首条越水位内容接续
HO_COMPLETED = 'COMPLETED'    # 接续完成：承接方首条内容已入账上链
HO_CLOSED = 'CLOSED'          # 未完成即关闭（撤单/超时/锁定/边界重算/计划取消/票据关闭）
HO_OPEN_STATES = (HO_PENDING, HO_SIGNED)          # 尚未完成、可被关闭触发器关闭
HO_RELEVANT_STATES = (HO_PENDING, HO_SIGNED, HO_COMPLETED)  # 仍参与水位归属划分

# 现场证据裁决结果
SE_ACCEPTED = 'ACCEPTED'      # 入账上链
SE_SUSPENDED = 'SUSPENDED'    # 挂起（未签收 / 接不上旧链），可唤醒或修正基点重传
SE_REJECTED = 'REJECTED'      # 拒收（终局）


def decide(*, ticket_active, frozen, write_owner, handover, closed_hint,
           team, ts_data, base_seq, head_seq, team_on_chain):
    """裁决一条现场证据的归属与处理（纯函数，不碰数据库）。

    参数：
      ticket_active  票据仍在执行（未关闭/撤回）
      frozen         工作票锁定中
      write_owner    当前持权班组
      handover       相关交班单 dict（status/from_team/candidates/signer/watermark），无则 None
      closed_hint    最近一张已关闭交班单的原因提示（无相关交班单时用于拒绝原因）
      team/ts_data   证据产生班组与数据时标（毫秒）
      base_seq       承接方首条内容声明的接续基点（链头序号），非首条为 None
      head_seq       当前链头序号（空链为 0）
      team_on_chain  该班组是否已有入账内容（承接方续作免基点校验）
    返回 (status, reason, late)：late=True 表示水位线内的迟到入账。
    """
    if not ticket_active:
        return SE_REJECTED, '工作票已关闭/撤回，现场证据按影响范围拒收', False
    if frozen:
        return SE_REJECTED, '工作票锁定中，禁止新增现场证据（已入账内容与设备状态保持原样）', False
    if handover is None:
        if team == write_owner:
            return SE_ACCEPTED, f'当前持权班组 {write_owner} 正常入账', False
        if closed_hint:
            return SE_REJECTED, f'{closed_hint}；写权仍归 {write_owner}', False
        return (SE_REJECTED,
                f'无写权：当前写权归 {write_owner}，同一张工作票任何时刻只允许一个班组追加现场证据',
                False)

    watermark = handover['watermark']
    over_line = ts_data > watermark          # 数据时标越过切换水位
    st = handover['status']

    if st == HO_PENDING:
        # 签收之前写权仍归移交方
        if team == handover['from_team']:
            if over_line:
                return SE_REJECTED, '越过切换水位：移交方越线写入，拒收', False
            return SE_ACCEPTED, '水位线内，移交方（持权班组）正常入账', False
        if team in handover['candidates']:
            return (SE_SUSPENDED,
                    f"承接方尚未签收，写权仍归移交方 {handover['from_team']}，首批内容挂起待签收",
                    False)
        return SE_REJECTED, '非交班双方班组，无写权', False

    # SIGNED / COMPLETED：写权归承接方（越水位历元），移交方仅保留水位线内迟到入账权
    signer = handover['signer']
    if team == handover['from_team']:
        if over_line:
            return SE_REJECTED, '越过切换水位：移交方越线写入，拒收', False
        return SE_ACCEPTED, '迟到内容：数据时标未越切换水位，仍归移交方入账', True
    if team == signer:
        if not over_line:
            return SE_REJECTED, '承接方内容必须越过切换水位（回填旧历元拒收）', False
        if team_on_chain:
            return SE_ACCEPTED, '承接方续作入账', False
        if base_seq is None:
            return SE_SUSPENDED, '承接方首条内容未声明接续基点，挂起待修正', False
        if base_seq != head_seq:
            return (SE_SUSPENDED,
                    f'接不上旧链：声明接续基点 #{base_seq}，当前链头 #{head_seq}，挂起待修正基点重传',
                    False)
        return SE_ACCEPTED, f'首条接续成功：承接方链接旧链链头 #{head_seq}', False
    if team in handover['candidates']:
        return SE_REJECTED, f'写权已由 {signer} 签收取得，本班组未中签', False
    return SE_REJECTED, '非交班双方班组，无写权', False


def boundary_problems(topo, ticket, steps):
    """检修期间校验原安全边界是否仍成立，返回问题列表（空 = 边界成立）。

    只检测与告警，不回滚任何已完成的物理步骤。
    """
    problems = []
    if topo.version != ticket['snapshot']:
        problems.append(f"拓扑已变更（票据快照 v{ticket['snapshot']}，当前 v{topo.version}）")
    en = topo.energized()
    # 许可开工后工作区必须保持无电；隔离过程中尚未停电属正常，恢复阶段逐步复电也属正常
    if ticket.get('status') == 'PERMITTED':
        for w in ticket['work_area']:
            if w in en:
                problems.append(f'工作区设备 {w} 恢复带电')
    # 已被对应恢复步骤（已复核）撤销的隔离步骤不再要求保持
    restored = {s['device'] for s in steps
                if s['phase'] == 'REST' and s['status'] == 'VERIFIED'}
    for s in steps:
        if s['phase'] != 'ISO' or s['status'] != 'VERIFIED' or s['device'] in restored:
            continue
        if s['kind'] == OPEN_SWITCH:
            st = topo.devices[s['device']]['state']
            if st != 'OPEN':
                problems.append(f"隔离开关 {s['device']} 已不在分位（当前 {st}）")
            if tag_for(ticket['id'], OPEN_SWITCH) not in topo.devices[s['device']]['tags']:
                problems.append(f"{s['device']} 的“禁止合闸”标示牌被移除")
        elif s['kind'] == INSTALL_GROUND:
            if (s['point'], s['sw'], ticket['id']) not in topo.grounds:
                problems.append(f"接地线 {s['device']} 缺失")
    # 接地线处带电 = 恶性事故征兆
    for point, sw, tid in topo.grounds:
        if point in en:
            problems.append(f'严重：{point} 处接地线带电（{tid} 装设）')
    return problems
