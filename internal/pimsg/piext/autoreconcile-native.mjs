// Explicit isolated installed-Pi probe. No model turn, credentials, discovery,
// XMPP account or live config is used. Invoked by TestAutoreconcileNative.
import { readFileSync, writeFileSync, openSync, closeSync, mkdtempSync, mkdirSync, statSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import assert from 'node:assert/strict';
const pkg = resolve(process.argv[2]);
const { SessionManager } = await import(pathToFileURL(join(pkg,'dist/core/session-manager.js')));
const root=mkdtempSync(join(tmpdir(),'pi-msg-native-idle-'));
const agentDir=join(root,'agent');mkdirSync(agentDir,{mode:0o700});
writeFileSync(join(agentDir,'models.json'),JSON.stringify({providers:{probe:{baseUrl:'http://127.0.0.1:1',apiKey:'offline-test-not-a-credential',api:'openai-completions',models:[{id:'probe',name:'Probe',reasoning:true,input:['text'],contextWindow:32000,maxTokens:1024}]}}}));
const sessions=join(root,'sessions');mkdirSync(sessions,{mode:0o700});
const sm=SessionManager.create(root,sessions);
const source=sm.getSessionFile();closeSync(openSync(source,'wx',0o600));sm.setSessionFile(source);
sm.appendModelChange('probe','probe');sm.appendThinkingLevelChange('high');sm.appendCustomMessageEntry('fixture-old-context','old context',false);
const oldID='id:'+sm.getSessionId();
const statePath=join(root,'account.auto.json'),requestPath=statePath+'.request';
const hash=s=>createHash('sha256').update(s).digest('hex');
const seed='Historical source only. read_messages(target=...). Prior session: '+source;
let token=0;
function authorize(generation='1') {
 const nonce='native-nonce-'+(++token);
 writeFileSync(statePath,JSON.stringify({Generation:1,Attempt:{Token:nonce,Phase:'resetting'}}),{mode:0o600});
 writeFileSync(requestPath,JSON.stringify({token:nonce,sourceID:oldID,sourceFile:source,hash:hash(readFileSync(source)),generation,provider:'probe',model:'probe',thinking:'high',seed}),{mode:0o600});
 return nonce;
}
const extension=fileURLToPath(new URL('./xmpp-tools.ts',import.meta.url));
const cancelExtension=join(root,'cancel.ts');
writeFileSync(cancelExtension,'export default pi => { pi.on("session_before_switch", () => ({cancel:true})); }');
let children=[];
function launch(sessionPath,{cancel=false,enabled=true}={}) {
 let seq=0, buffer='',stderr='';const pending=new Map(),events=[];
 const env={PATH:process.env.PATH,HOME:root,PI_CODING_AGENT_DIR:agentDir,PI_OFFLINE:'1',PI_MSG_TOOLS:'',PI_MSG_PROCESS_EXTENSION:'',PI_MSG_CAPABILITIES:'{}'};
 if(enabled)env.PI_MSG_IDLE_REQUEST=requestPath;
 const args=[join(pkg,'dist/cli.js'),'--mode','rpc','--no-extensions','--no-context-files','--no-skills','--no-prompt-templates','--no-themes','--session',sessionPath,'-e',extension];
 if(cancel)args.push('-e',cancelExtension);
 const child=spawn(process.execPath,args,{cwd:root,env,stdio:['pipe','pipe','pipe']});children.push(child);
 child.stderr.on('data',d=>stderr+=d);
 child.stdout.on('data',data=>{buffer+=data;for(;;){const n=buffer.indexOf('\n');if(n<0)break;const line=buffer.slice(0,n);buffer=buffer.slice(n+1);if(!line)continue;const e=JSON.parse(line);
  if(e.type==='extension_ui_request' && e.method==='select')child.stdin.write(JSON.stringify({type:'extension_ui_response',id:e.id,value:'ok'})+'\n');
  else if(e.type==='response' && pending.has(e.id)){pending.get(e.id)(e);pending.delete(e.id);}else events.push(e);
 }});
 return {events,request(command){return new Promise((resolve,reject)=>{const id='probe-'+(++seq);const timer=setTimeout(()=>reject(Error('RPC timeout '+JSON.stringify(command)+' stderr '+stderr)),15000);pending.set(id,e=>{clearTimeout(timer);resolve(e)});child.stdin.write(JSON.stringify({id,...command})+'\n');});},async stop(){child.stdin.end();await new Promise(r=>child.on('exit',r));children=children.filter(c=>c!==child);}};
}
try {
 const p=launch(source);
 const commands=await p.request({type:'get_commands'});
 assert(commands.data.commands.some(c=>c.name==='pi-msg-idle-reset' && c.source==='extension' && c.sourceInfo?.path===extension && c.sourceInfo.source==='cli'),JSON.stringify({commands,events:p.events}));
 const bad=authorize('0');await p.request({type:'prompt',message:'/pi-msg-idle-reset '+bad});
 let outcome=JSON.parse(readFileSync(requestPath+'.receipt','utf8'));assert.equal(outcome.status,'failed');
 assert.equal((await p.request({type:'get_state'})).data.sessionId,oldID.slice(3));
 const nonce=authorize();const accepted=await p.request({type:'prompt',message:'/pi-msg-idle-reset '+nonce});assert.equal(accepted.success,true);
 outcome=JSON.parse(readFileSync(requestPath+'.receipt','utf8'));
 assert.equal(outcome.status,'success');assert.equal(outcome.cancelled,false);assert.equal(outcome.token,nonce);assert.equal(outcome.seedHash,hash(seed));
 assert.notEqual(outcome.id,oldID);assert.notEqual(outcome.file,source);assert.equal(statSync(outcome.file).mode&0o777,0o600);
 const fresh=(await p.request({type:'get_state'})).data;assert.equal('id:'+fresh.sessionId,outcome.id);assert.equal(fresh.sessionFile,outcome.file);assert.equal(fresh.thinkingLevel,'high');assert.equal(fresh.model.id,'probe');assert.equal(fresh.isStreaming,false);
 const entries=(await p.request({type:'get_entries'})).data.entries;
 assert.equal(entries.filter(e=>e.type==='custom_message' && e.customType==='pi-msg-idle-seed' && e.content===seed).length,1);
 assert(!entries.some(e=>e.type==='message'));assert(!p.events.some(e=>e.type==='agent_start'));
 await p.stop();
 const restarted=launch(outcome.file,{enabled:false});
 const restored=(await restarted.request({type:'get_state'})).data;assert.equal('id:'+restored.sessionId,outcome.id);assert.equal(restored.sessionFile,outcome.file);assert.equal(restored.thinkingLevel,'high');assert.equal(restored.model.id,'probe');
 assert.equal((await restarted.request({type:'get_messages'})).data.messages.filter(m=>m.customType==='pi-msg-idle-seed' && m.content===seed).length,1);
 assert(!(await restarted.request({type:'get_commands'})).data.commands.some(c=>c.name==='pi-msg-idle-reset'));
 assert(!restarted.events.some(e=>e.type==='agent_start'));await restarted.stop();
 // Inspect provider cache identity through Pi's public SDK runtime seam too:
 // get_state alone cannot reveal the Agent/SessionManager split fixed here.
 const {createAgentSessionRuntime,createAgentSessionServices,createAgentSessionFromServices}=await import(pathToFileURL(join(pkg,'dist/core/sdk.js')));
 const factory=async ({cwd,sessionManager,sessionStartEvent})=>{
  const services=await createAgentSessionServices({cwd,agentDir,resourceLoaderOptions:{noExtensions:true,noContextFiles:true,noSkills:true,noThemes:true,noPromptTemplates:true}});
  return {...await createAgentSessionFromServices({services,sessionManager,sessionStartEvent}),services,diagnostics:services.diagnostics};
 };
 const runtime=await createAgentSessionRuntime(factory,{cwd:root,agentDir,sessionManager:SessionManager.open(source)});
 await runtime.switchSession(outcome.file);
 assert.equal('id:'+runtime.session.agent.sessionId,outcome.id);
 assert.equal('id:'+runtime.session.sessionManager.getSessionId(),outcome.id);
 assert.equal(runtime.session.agent.state.messages.filter(m=>m.customType==='pi-msg-idle-seed').length,1);
 await runtime.dispose();
 const cancelled=launch(source,{cancel:true});const cancelNonce=authorize();await cancelled.request({type:'prompt',message:'/pi-msg-idle-reset '+cancelNonce});
 const cancelResult=JSON.parse(readFileSync(requestPath+'.receipt','utf8'));assert.equal(cancelResult.status,'cancelled');assert.equal(cancelResult.cancelled,true);assert.equal((await cancelled.request({type:'get_state'})).data.sessionId,oldID.slice(3));await cancelled.stop();
 console.log(JSON.stringify({pi:JSON.parse(readFileSync(join(pkg,'package.json'))).version,materialization:'native standalone SessionManager + exact native switchSession',finalUUID:outcome.id,finalPath:outcome.file,seedOnce:true,restart:true,agentCacheIdentity:true,preservedModelThinking:true,cancelled:true,staleGenerationRejected:true,noModelTurn:true,disabledCommandAbsent:true}));
} finally {for(const c of children)c.kill('SIGKILL');rmSync(root,{recursive:true,force:true});}
