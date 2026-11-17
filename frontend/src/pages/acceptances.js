import {store} from '../stores/business.js';import {esc} from '../utils/html.js';import {table} from '../components/common/table.js';import {status} from '../components/common/status.js';import {ACCEPTANCE_STATUS,WAIT_REASONS} from '../constants/enums.js';
function accStatus(s){return `<span class="status ${s}">${ACCEPTANCE_STATUS[s]||s}</span>`;}
function waitCell(a){
 if(a.status!=='pending')return '<small>—</small>';
 const reason=WAIT_REASONS[a.wait_reason]||a.wait_reason||'等待派发';
 return `<small class="wait">${esc(reason)}</small>`;
}
export function acceptancesPage(){
 const canWrite=store.user.role!=='viewer';
 return `<div class="split"><section><h2>可靠受理 · 断线不丢单</h2><p>提交即落库并冻结批次/映射快照；队列断线也保留受理，恢复后自动派发。同一受理号重放不新增运行，内容变化将被拒绝。</p><form id="acceptance-form" ${canWrite?'':'disabled'}><label>受理号（业务幂等键）<input name="acceptance_no" placeholder="ACC-20261117-001" required minlength="3" maxlength="80"></label><label>本次受理名称<input name="name" placeholder="shift-20261117-对账" required minlength="2" maxlength="120"></label><label>字段映射<select name="mapping_id">${store.mappings.map(m=>`<option value="${m.id}">${esc(m.name)}</option>`)}</select></label><button type="submit" ${canWrite?'':'disabled'}>提交受理 →</button></form><ol class="steps"><li>受理号唯一；重放同号同内容返回原单，换内容 422 拒绝</li><li>同批次按受理顺序处理，前项终结后再启后项</li><li>不同批次可并发，故障批次不拖住其他批次</li><li>待派发可取消；取消后迟到消息不会启动迁移</li></ol></section><section><h2>受理台账 <em>${store.acceptances.length}</em></h2>${table(['受理号 / 名称','批次序号','受理状态','等待原因','运行 / 操作'],store.acceptances.map(a=>[`<b>${esc(a.acceptance_no)}</b><small>${esc(a.name)}</small><small>${esc(a.created_at||'')}</small>`,`<small>批次#${a.batch_id} · 第 ${a.seq} 号</small>`,accStatus(a.status),waitCell(a),`<small>运行 #${a.run_id}</small><div>${['pending','running'].includes(a.status)&&canWrite?`<button class="quiet" data-accept-cancel="${a.id}">取消受理</button>`:''}${a.run_id?`<button class="quiet" data-result="${a.run_id}">核对 →</button>`:''}</div>`]))}</section></div>`;}
