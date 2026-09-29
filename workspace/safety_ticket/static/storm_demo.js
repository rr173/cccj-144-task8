// 雷暴闸门可复现场景脚本：所有步骤均走真实 HTTP API，结果由 SQLite 裁决。
async function makeTicket(api, work=['F2'], withTool=true) {
  // 清理上一场景遗留的同区域在办演示票；雷暴事实仍保留在已终结批次中。
  const old = await api('GET','/api/tickets');
  for (const t of old.filter(x => x.title.startsWith('雷暴演示') && ['ISOLATING','PERMITTED','RESTORING'].includes(x.status))) {
    try { await api('POST',`/api/tickets/${t.id}/withdraw`,{actor:'演示复位',reason:'清理上一场景'}); } catch(_) {}
  }
  const t = await api('POST','/api/tickets',{title:`雷暴演示 ${Date.now()}`,work_area:work,created_by:'调度员'});
  for (const s of t.steps) {
    if (s.kind === 'OPEN_SWITCH' || s.kind === 'CLOSE_SWITCH') {
      await api('POST','/sim/switch',{device:s.device,state:s.expected,ticket:t.id,actor:'模拟操作'});
    } else {
      await api('POST','/sim/ground',{point:s.point,sw:s.sw,action:s.kind==='INSTALL_GROUND'?'install':'remove',ticket:t.id,actor:'模拟操作'});
    }
    await api('POST',`/api/tickets/${t.id}/steps/${s.id}/report`,{actor:'张三',scanned:s.device});
    await api('POST',`/api/tickets/${t.id}/steps/${s.id}/verify`,{actor:'李四'});
  }
  await api('POST',`/api/tickets/${t.id}/permit`,{actor:'调度员'});
  let serial = null;
  if (withTool) {
    const lock = await api('POST',`/api/tickets/${t.id}/tool-locks`,{actor:'库管员'});
    serial = lock.lines[0].serial;
    const box = await api('POST',`/api/tickets/${t.id}/tool-boxes`,{actor:'库管员'});
    await api('POST',`/api/tool-boxes/${box.id}/scan`,{client_id:'PDA-STORM',serial});
  }
  return {ticket: t, serial};
}

async function evacuate(api, tid) {
  return await api('POST','/api/storm/demo/alert-order',{ticket:tid,actor:'值班长'});
}

async function settle(api, bid, opts={}) {
  const b0 = await api('GET',`/api/storm/batches/${bid}`);
  const onsite = b0.roster.filter(p => p.onsite);
  const missingName = opts.missing;
  for (const p of onsite) {
    if (p.name === missingName) continue;
    await api('POST',`/api/storm/batches/${bid}/checkins`,{person:p.id,shelter:p.name==='李四'?'SH-B':'SH-A'});
  }
  // 李四改去二号点：先在一号点扫码，再由本人纠正到二号点，验证只保留最新去向。
  if (!missingName) {
    await api('POST',`/api/storm/batches/${bid}/checkins`,{person:'P-LI',shelter:'SH-A',key:`demo-first-${bid}`});
    await api('POST',`/api/storm/batches/${bid}/correct-destination`,{person:'P-LI',shelter:'SH-B',key:`demo-correct-${bid}`});
  }
  const b1 = await api('GET',`/api/storm/batches/${bid}`);
  for (const r of b1.risks) {
    await api('POST',`/api/storm/batches/${bid}/risk-guards`,{risk:r.id,guardian:'P-WANG',shelter:'SH-A'});
  }
  for (const tool of b1.tools) {
    if (opts.skipTool && tool.serial === opts.skipTool) continue;
    await api('POST',`/api/storm/batches/${bid}/tool-reports`,{
      serial:tool.serial,location:tool.kind==='ISSUED'?'一号避险点工器具筐':'现场工具箱',held_by:'P-WANG',shelter:'SH-A',actor:'P-CHEN'
    });
  }
  return await api('GET',`/api/storm/batches/${bid}`);
}

async function doubleApprove(api, bid) {
  await api('POST',`/api/storm/batches/${bid}/field-confirm`,{actor:'P-CHEN',key:`field-${bid}`});
  await api('POST',`/api/storm/batches/${bid}/dispatch-confirm`,{actor:'调度员',key:`dispatch-${bid}`});
}

async function wait(ms=300){await new Promise(r=>setTimeout(r,ms))}

window.stormDemo = async function(name, api, reload, toast) {
  try {
    if (name === 'full') {
      const {ticket,serial} = await makeTicket(api);
      const e = await evacuate(api,ticket.id); const bid=e.evacuation.id;
      await settle(api,bid); await api('POST',`/api/storm/alerts/${e.alert_id}/clear`,{actor:'气象值班员'});
      await doubleApprove(api,bid); await api('POST',`/api/storm/batches/${bid}/resume`,{actor:'值班长'});
      await reload(); toast(`全员到齐并双人放行完成：${bid}，票据已解除冻结恢复作业`);
    }
    if (name === 'missing') {
      const {ticket} = await makeTicket(api,['F2'],false);
      const e = await evacuate(api,ticket.id);
      await settle(api,e.evacuation.id,{missing:'赵六'});
      try { await api('POST',`/api/storm/batches/${e.evacuation.id}/field-confirm`,{actor:'P-CHEN'}); }
      catch(err) { await reload(); toast('预期阻断：赵六未确认安全，现场清点确认被拒绝，票据不得恢复作业'); return; }
    }
    if (name === 'conflict') {
      const {ticket} = await makeTicket(api);
      const e = await evacuate(api,ticket.id); const bid=e.evacuation.id;
      await settle(api,bid);
      const b=await api('GET',`/api/storm/batches/${bid}`); const sn=b.tools[0].serial;
      await api('POST',`/api/storm/batches/${bid}/tool-reports`,{serial:sn,location:'二号避险点车辆旁',held_by:'P-LI',shelter:'SH-B',actor:'P-LI',key:`conflict-${bid}`});
      await api('POST',`/api/storm/alerts/${e.alert_id}/clear`,{actor:'气象值班员'});
      try { await api('POST',`/api/storm/batches/${bid}/field-confirm`,{actor:'P-CHEN',key:`conflict-field-${bid}`}); }
      catch(err) {
        await api('POST',`/api/storm/alerts/${e.alert_id}/clear`,{actor:'气象值班员'});
        await api('POST',`/api/storm/batches/${bid}/tool-reports`,{serial:sn,location:'一号避险点工器具筐',held_by:'P-WANG',shelter:'SH-A',actor:'P-CHEN',force_correct:true,key:`resolve-${bid}`});
        await api('POST',`/api/storm/batches/${bid}/field-confirm`,{actor:'P-CHEN',key:`conflict-field2-${bid}`});
        await api('POST',`/api/storm/batches/${bid}/dispatch-confirm`,{actor:'调度员',key:`conflict-dispatch-${bid}`});
        await reload(); toast('冲突已按预期阻断；现场负责人裁决最新去向后，双确认继续。'); return;
      }
    }
    if (name === 'env') {
      const {ticket} = await makeTicket(api);
      const e = await evacuate(api,ticket.id); const bid=e.evacuation.id;
      await settle(api,bid); await api('POST',`/api/storm/alerts/${e.alert_id}/clear`,{actor:'气象值班员'});
      await api('POST',`/api/storm/batches/${bid}/field-confirm`,{actor:'P-CHEN',key:`env-field-${bid}`});
      await api('POST','/sim/topology',{op:'add_edge',a:'N1',b:'N2',actor:'演示拓扑变更'});
      try { await api('POST',`/api/storm/batches/${bid}/dispatch-confirm`,{actor:'调度员',key:`env-old-${bid}`}); }
      catch(err) { await reload(); toast('预期拒绝：拓扑/隔离边界版本已变化，旧现场确认失效，需要重新清点确认。'); return; }
    }
    if (name === 'preempt') {
      const {ticket} = await makeTicket(api);
      const e1 = await evacuate(api,ticket.id); const bid=e1.evacuation.id;
      await settle(api,bid); await api('POST',`/api/storm/alerts/${e1.alert_id}/clear`,{actor:'气象值班员'});
      await doubleApprove(api,bid);
      const a2 = await api('POST','/api/storm/alerts',{ticket:ticket.id,title:'第二次雷暴橙色预警',areas:ticket.work_area,actor:'值班长'});
      await reload(); toast(`待复工批次 ${bid} 已被第二次告警 ${a2.alert_id} 抢占；旧撤离与签到事实保留。`);
    }
    if (name === 'idempotent') {
      const {ticket} = await makeTicket(api);
      const e = await evacuate(api,ticket.id); const bid=e.evacuation.id;
      const r1=await api('POST',`/api/storm/batches/${bid}/checkins`,{person:'P-ZHANG',shelter:'SH-A',key:'idem-demo'});
      const r2=await api('POST',`/api/storm/batches/${bid}/checkins`,{person:'P-ZHANG',shelter:'SH-A',key:'idem-demo'});
      await evacuate(api,ticket.id); // 同告警重放，返回原批次
      await reload(); toast(`重复扫码/撤离重放均返回首次结果：${r1.result} / ${r2.result}`);
    }
    if (name === 'restart') {
      const {ticket} = await makeTicket(api);
      const e = await evacuate(api,ticket.id); const bid=e.evacuation.id;
      await settle(api,bid);
      await api('POST','/api/admin/reopen',{});
      const restored=await api('GET',`/api/storm/batches/${bid}`);
      await api('POST',`/api/storm/alerts/${e.alert_id}/clear`,{actor:'气象值班员'});
      await doubleApprove(api,bid);
      await api('POST',`/api/storm/batches/${bid}/resume`,{actor:'值班长'});
      await reload(); toast(`服务连接重建后还原 ${restored.checkins.length} 条有效签到和全部未清项，流程继续完成。`);
    }
    await wait();
  } catch (e) { toast(e, false); console.error(e); }
};
