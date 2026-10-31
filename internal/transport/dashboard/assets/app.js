'use strict';
const $ = id => document.getElementById(id);
const state = {session:null, cluster:null, topic:'', selected:null, sequence:0, pending:null, busy:false, producerBusy:false, following:false, assigned:false, positions:[], output:[], received:0, timer:null, inspectGeneration:0, next:null};
const el = (tag, text, cls) => {const n=document.createElement(tag); if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;};
function message(id,text,tone=''){const n=$(id);n.textContent=text;n.className='message '+tone;}
async function api(path,body){
 const response=await fetch('/api/'+path,{method:body===undefined?'GET':'POST',headers:body===undefined?{}:{'Content-Type':'application/json'},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(15000)});
 let data;try{data=await response.json();}catch{throw new Error('Response could not be read. The operation may have completed.');}
 if(!response.ok){const e=new Error(data.error||'Request failed');e.status=response.status;e.ambiguous=data.ambiguous;throw e;}return data;
}
function offset(id){const n=$(id),v=n.value.trim();const valid=/^\d+$/.test(v)&&BigInt(v)<=9223372036854775807n;n.setAttribute('aria-invalid',String(!valid));if(!valid){n.focus();throw new Error('Enter an offset from 0 to 9223372036854775807.');}return BigInt(v).toString();}
function setOptions(node,items){const signature=JSON.stringify(items);if(node.dataset.options===signature)return;const old=node.value;node.replaceChildren(...items.map(([value,label])=>{const o=el('option',label);o.value=value;return o;}));if(items.some(i=>i[0]===old))node.value=old;node.dataset.options=signature;}
function buttons(){
 $('send').disabled=!state.session||!state.topic||state.producerBusy;
 $('assign').disabled=!state.session||!state.topic||state.busy||state.following||!!state.pending;
 for(const id of ['poll','follow','seek'])$(id).disabled=!state.assigned||state.busy||state.following||!!state.pending;
 $('pause').disabled=!state.following;
 $('retry').hidden=!state.pending||state.busy;
 $('retry').disabled=!state.pending||state.busy;
 $('seek-partition').disabled=!state.assigned||state.busy||state.following||!!state.pending;
 $('new-session').disabled=state.busy||state.producerBusy||state.following||!!state.pending;
}
async function newSession(){
 state.busy=true;buttons();const old=state.session;
 try{const v=await api('sessions',{});state.session=v.id;state.sequence=0;state.pending=null;state.assigned=false;state.positions=[];state.output=[];state.received=0;renderOutput();renderPositions();$('consumer-state').textContent='No partitions assigned.';$('session-status').textContent='Independent browser session';message('consumer-message','');
 if(old)api('session/delete',{id:old}).catch(()=>{});
 }catch(e){$('session-status').textContent=e.message;}finally{state.busy=false;buttons();}
}
function partitions(){return (state.cluster?.metadata.partitions||[]).filter(p=>p.topic===state.topic);}
function brokerFor(p){return state.cluster?.brokers.find(b=>b.id===p.leader);}
function observedEnd(tp){const m=state.cluster?.metadata.partitions.find(p=>p.topic===tp.topic&&p.partition===tp.partition);if(!m||state.cluster.controllerError)return null;const b=brokerFor(m);if(!b||b.error)return null;return b.partitions?.find(p=>p.topic===tp.topic&&p.partition===tp.partition)?.leo??null;}
function renderCluster(){
 const c=state.cluster;
 $('controller-status').textContent=c.controllerError?'Controller unavailable · metadata stale':'Controller reachable · '+(c.controllerSuccess||'not observed');
 message('cluster-error',c.controllerError?c.controllerError+' · Last successful metadata: '+(c.controllerSuccess||'never'):'','warning');
 $('brokers').replaceChildren(...c.brokers.map(b=>{const box=el('article',undefined,'broker'+(b.error?' stale':''));const head=el('div',undefined,'broker-head');head.append(el('strong','Broker '+b.id),el('span',b.error?'UNAVAILABLE / STALE':'REACHABLE','status'));box.append(head,el('p',b.addr,'addr'),el('p',(b.partitions?.length??0)+' hosted partitions · last success '+(b.lastSuccess?new Date(b.lastSuccess).toLocaleTimeString('en-US'):'never'),'caption'));if(b.error)box.append(el('p',b.error,'error-text'));return box;}));
 if(!c.brokers.length)$('brokers').append(el('p','No brokers observed. Start your controller and broker processes, then this view will refresh.','empty'));
 const topics=[...new Set(c.metadata.partitions.map(p=>p.topic))].sort();
 setOptions($('topic'),topics.length?topics.map(t=>[t,t]):[['','No topics yet']]);
 if(!topics.includes(state.topic))state.topic=$('topic').value;
 $('topic').value=state.topic;renderTopic();renderPositions();buttons();
}
function renderTopic(){
 $('produce-topic').textContent=state.topic||'the selected topic';
 setOptions($('consumer-partition'),[['all','All partitions'],...partitions().map(p=>[String(p.partition),'Partition '+p.partition])]);
 const root=$('lanes');const existing=new Map([...root.querySelectorAll('.lane')].map(n=>[n.dataset.partition,n]));
 const groups=new Map();for(const p of partitions()){if(!groups.has(p.leader))groups.set(p.leader,[]);groups.get(p.leader).push(p);}
 // Reconcile existing buttons, retaining keyboard focus across observation refreshes.
 for(const old of root.querySelectorAll('.empty'))old.remove();
 for(const [leader,ps] of groups){let group=[...root.children].find(n=>n.dataset.owner===String(leader));if(!group){group=el('div');group.dataset.owner=String(leader);group.append(el('p','BROKER '+leader,'owner-label'));root.append(group);}
 for(const p of ps){const key=p.topic+'/'+p.partition;let lane=existing.get(key);if(!lane){lane=el('button',undefined,'lane');lane.type='button';lane.dataset.partition=key;lane.append(el('span','P'+p.partition,'lane-id'),el('span',undefined,'log-track'),el('span',undefined,'lane-end'));lane.addEventListener('click',()=>selectPartition(p));group.append(lane);}existing.delete(key);
 const b=brokerFor(p),end=b?.partitions?.find(v=>v.topic===p.topic&&v.partition===p.partition)?.leo;
 lane.setAttribute('aria-pressed',String(state.selected?.topic===p.topic&&state.selected?.partition===p.partition));lane.setAttribute('aria-label',p.topic+' partition '+p.partition+', broker '+leader+', log end '+(end??'unknown'));
 lane.querySelector('.lane-end').textContent=end===undefined?'missing':('LEO '+end+(b.error?' · stale':''));
 const signature=String(end);const track=lane.querySelector('.log-track');if(track.dataset.end!==signature){track.replaceChildren();if(end!==undefined){const n=BigInt(end),start=n>6n?n-6n:0n;for(let i=start;i<n;i++)track.append(el('span',String(i),'offset-cell'));if(n===0n)track.append(el('span','Empty log','caption'));}track.dataset.end=signature;}
 }}
 for(const lane of existing.values())lane.remove();for(const group of [...root.children])if(group.dataset.owner&&!group.querySelector('.lane'))group.remove();
 if(!groups.size)root.append(el('p','Create a topic to distribute its partitions across the registered brokers.','empty'));
}
async function refresh(){try{state.cluster=await api('cluster');renderCluster();}catch(e){message('cluster-error','Dashboard unavailable: '+e.message+' · displayed observations are stale.','warning');if(state.cluster){state.cluster.controllerError=e.message;for(const b of state.cluster.brokers)b.error=e.message;renderCluster();}}finally{setTimeout(refresh,1000);}}
function selectPartition(p){state.selected={topic:p.topic,partition:p.partition};$('inspect-offset').value='0';$('inspect-title').textContent=p.topic+' / Partition '+p.partition;$('inspect-go').disabled=false;$('inspect-refresh').disabled=false;renderTopic();inspect();}
function recordRow(r,consumer=false){const tr=el('tr');const values=consumer?[r.topic,r.partition,r.offset,r.key,r.value,r.timestamp]:[r.offset,r.key,r.value,r.timestamp];for(const value of values)tr.append(el('td',String(value)));return tr;}
async function inspect(){if(!state.selected)return;let from;try{from=offset('inspect-offset');}catch(e){message('inspect-message',e.message,'error');return;}const generation=++state.inspectGeneration;state.next=null;$('inspect-next').disabled=true;message('inspect-message','Reading partition…');const q=new URLSearchParams({...state.selected,offset:from});try{const v=await api('records?'+q);if(generation!==state.inspectGeneration)return;$('inspect-rows').replaceChildren(...v.records.map(r=>recordRow(r)));const next=BigInt(from)+BigInt(v.records.length);state.next=next<BigInt(v.leo)?String(next):null;$('inspect-next').disabled=state.next===null;message('inspect-message',v.records.length+' records · observed LEO '+v.leo+(v.records.length?'':' · no records at this offset'));}catch(e){if(generation!==state.inspectGeneration)return;message('inspect-message',e.message+' · Previous rows, if any, are stale.','error');}}
function renderPositions(){
 $('positions').replaceChildren(...state.positions.map(p=>{const n=el('div',undefined,'position'),end=observedEnd(p);const backlog=end!==null&&BigInt(end)>=BigInt(p.offset)?String(BigInt(end)-BigInt(p.offset)):'unknown';n.append(el('span',p.topic+' / P'+p.partition),el('span','next '+p.offset+' · behind '+backlog));return n;}));
 setOptions($('seek-partition'),state.positions.map(p=>[JSON.stringify([p.topic,p.partition]),p.topic+' / P'+p.partition]));
}
function renderOutput(){ $('consumer-rows').replaceChildren(...state.output.map(r=>recordRow(r,true)));$('output-count').textContent=state.received+' records received';$('output-note').textContent=state.received>500?'Showing latest 500 records; '+(state.received-500)+' older rows left this view. Seek to replay stored records.':'Records are ordered only within each partition. Pausing retains your next-read positions.';}
function pause(){state.following=false;clearTimeout(state.timer);$('consumer-state').textContent=state.busy?'Pausing after the current bounded poll…':state.assigned?'Paused · positions retained.':'No partitions assigned.';buttons();}
async function consumerOperation(action,fields={}){
 if(state.busy)return;
 state.busy=true;
 if(!state.pending)state.pending={id:state.session,sequence:state.sequence+1,action,...fields};
 buttons();message('consumer-message','Waiting for '+state.pending.action+'…');
 try{const result=await api('consumer',state.pending);state.sequence=result.sequence;state.pending=null;state.assigned=true;state.positions=result.positions;state.received+=result.records.length;state.output.push(...result.records);state.output=state.output.slice(-500);renderPositions();renderOutput();message('consumer-message',result.errors.join('\n')||'Consumer positions updated.',result.errors.length?'warning':'success');}
 catch(e){pause();if(e.status && e.status!==504)state.pending=null;if(e.status===410){state.session=null;state.assigned=false;$('session-status').textContent='Session expired · create a new session';}message('consumer-message',e.message+(state.pending?' Recover the pending operation to retrieve its result before continuing.':''),'error');}
 finally{state.busy=false;$('consumer-state').textContent=state.following?'Following · polls of up to 50 records':state.assigned?'Paused · positions retained.':'No partitions assigned.';buttons();if(state.following)state.timer=setTimeout(()=>consumerOperation('poll'),1000);}
}
$('new-session').addEventListener('click',newSession);
$('topic').addEventListener('change',()=>{state.topic=$('topic').value;renderTopic();buttons();});
$('topic-form').addEventListener('submit',async e=>{e.preventDefault();const name=$('topic-name'),count=$('topic-count');for(const n of [name,count])n.removeAttribute('aria-invalid');let invalid=!name.value.trim()?name:!/^\d+$/.test(count.value)||Number(count.value)<1||Number(count.value)>1000?count:null;if(invalid){invalid.setAttribute('aria-invalid','true');invalid.focus();message('topic-message','Enter a topic name and 1–1000 partitions.','error');return;}const button=e.submitter;button.disabled=true;message('topic-message','Assigning partitions…');try{await api('topics',{name:name.value,partitions:Number(count.value)});message('topic-message','Topic created.','success');state.topic=name.value;state.cluster=await api('cluster');renderCluster();}catch(err){message('topic-message',err.message,'error');}finally{button.disabled=false;}});
$('produce-form').addEventListener('submit',async e=>{e.preventDefault();if(state.producerBusy||!state.session||!state.topic)return;state.producerBusy=true;buttons();message('produce-message','Sending record…');try{const v=await api('produce',{id:state.session,topic:state.topic,key:$('record-key').value,value:$('record-value').value});message('produce-message','Acknowledged · partition '+v.partition+' · offset '+v.offset,'success');}catch(err){const unknown=err.ambiguous||!err.status;message('produce-message',err.message+(unknown?' Outcome unknown: the record may be stored. No automatic retry; sending again may duplicate it.':''),unknown?'warning':'error');if(err.status===410){state.session=null;$('session-status').textContent='Session expired · create a new session';}}finally{state.producerBusy=false;buttons();}});
$('record-value').addEventListener('input',e=>{e.target.style.height='auto';e.target.style.height=Math.min(e.target.scrollHeight,320)+'px';});
$('inspect-form').addEventListener('submit',e=>{e.preventDefault();inspect();});$('inspect-refresh').addEventListener('click',inspect);$('inspect-next').addEventListener('click',()=>{if(state.next!==null){$('inspect-offset').value=state.next;inspect();}});
$('consumer-form').addEventListener('submit',e=>{e.preventDefault();if(state.busy||state.following||state.pending)return;try{const from=offset('consumer-offset');consumerOperation('assign',{topic:state.topic,partitions:$('consumer-partition').value==='all'?partitions().map(p=>p.partition):[Number($('consumer-partition').value)],offset:from});}catch(err){message('consumer-message',err.message,'error');}});
$('poll').addEventListener('click',()=>consumerOperation('poll'));$('follow').addEventListener('click',()=>{state.following=true;consumerOperation('poll');});$('pause').addEventListener('click',pause);$('retry').addEventListener('click',()=>consumerOperation());
$('seek-form').addEventListener('submit',e=>{e.preventDefault();if(state.busy||state.following||state.pending)return;try{const [topic,partition]=JSON.parse($('seek-partition').value);consumerOperation('seek',{topic,partitions:[partition],offset:offset('seek-offset')});}catch(err){message('consumer-message',err.message,'error');}});
newSession();refresh();
