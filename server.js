// WhatsApp webhook + OCR queue service. Final financial submit remains human-only.
const express=require('express');
const {createWorker}=require('tesseract.js');
const app=express();
app.use(express.json({limit:'2mb'}));
const PORT=process.env.PORT||10000;
const DASHBOARD_URL=process.env.DASHBOARD_URL||'https://payment-approval-dashboard-legend-fd31.vercel.app';
const SUPABASE_URL=process.env.SUPABASE_URL||'';
const SUPABASE_ANON_KEY=process.env.SUPABASE_ANON_KEY||'';
const INGEST_KEY=process.env.WHATSAPP_INGEST_KEY||'';
const VERIFY_TOKEN=process.env.WHATSAPP_VERIFY_TOKEN||'';
const ACCESS_TOKEN=process.env.WHATSAPP_ACCESS_TOKEN||'';
const POLL_MS=Number(process.env.WHATSAPP_POLL_MS||8000);
const startedAt=new Date().toISOString();
let processor={running:false,lastPoll:null,lastInboxId:null,lastError:null};
let ocrWorker=null;
async function rpc(name,body){
 const r=await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`,{method:'POST',headers:{'Content-Type':'application/json',apikey:SUPABASE_ANON_KEY,Authorization:`Bearer ${SUPABASE_ANON_KEY}`},body:JSON.stringify(body)});
 const t=await r.text(); if(!r.ok) throw new Error(t||`RPC ${name} failed`); return t?JSON.parse(t):null;
}
function extractPayment(text){
 const flat=String(text||'').replace(/\r/g,' ');
 let utr='';
 let m=flat.match(/(?:UPI\s*(?:transaction)?\s*(?:ID|Id)|UTR|Transaction\s*ID|Reference\s*(?:No|Number)?)\s*[:#-]?\s*([A-Z0-9]{10,30})/i);
 if(m)utr=m[1].replace(/\s/g,'');
 if(!utr){m=flat.match(/\b\d{12}\b/);if(m)utr=m[0];}
 let amount='';
 m=flat.match(/(?:₹|Rs\.?|INR)\s*([0-9,]+(?:\.\d{1,2})?)/i);
 if(m)amount=m[1].replace(/,/g,'');
 return {amount:amount?Number(amount):null,utr:utr||null};
}
async function getOcrWorker(){if(!ocrWorker)ocrWorker=await createWorker('eng');return ocrWorker;}
async function downloadWhatsAppMedia(mediaId){
 const meta=await fetch(`https://graph.facebook.com/${encodeURIComponent(mediaId)}`,{headers:{Authorization:`Bearer ${ACCESS_TOKEN}`}});
 if(!meta.ok)throw new Error(`Meta media lookup failed ${meta.status}`);
 const info=await meta.json(); if(!info.url)throw new Error('Meta media URL missing');
 const media=await fetch(info.url,{headers:{Authorization:`Bearer ${ACCESS_TOKEN}`}});
 if(!media.ok)throw new Error(`Meta media download failed ${media.status}`);
 return Buffer.from(await media.arrayBuffer());
}
async function processOne(){
 if(!ACCESS_TOKEN)return false;
 const rows=await rpc('whatsapp_claim_next',{p_ingest_key:INGEST_KEY});
 const job=Array.isArray(rows)?rows[0]:rows;
 if(!job||!job.inbox_id)return false;
 processor.lastInboxId=job.inbox_id;
 try{
  if(!job.client_id){await rpc('whatsapp_finish',{p_ingest_key:INGEST_KEY,p_inbox_id:job.inbox_id,p_status:'needs_review',p_amount:null,p_utr:null,p_error:'Client phone mapping not found'});return true;}
  const image=await downloadWhatsAppMedia(job.media_id);
  const worker=await getOcrWorker();
  const result=await worker.recognize(image);
  const x=extractPayment(result.data?.text||'');
  const status=(x.amount&&x.utr)?'ready':'needs_review';
  const error=status==='ready'?null:'Could not confidently detect amount and UTR';
  await rpc('whatsapp_finish',{p_ingest_key:INGEST_KEY,p_inbox_id:job.inbox_id,p_status:status,p_amount:x.amount,p_utr:x.utr,p_error:error});
 }catch(e){
  const msg=String(e.message||e).slice(0,500);
  await rpc('whatsapp_finish',{p_ingest_key:INGEST_KEY,p_inbox_id:job.inbox_id,p_status:'failed',p_amount:null,p_utr:null,p_error:msg}).catch(()=>{});
  throw e;
 }
 return true;
}
async function processorLoop(){
 if(processor.running)return;processor.running=true;
 while(true){
  try{processor.lastPoll=new Date().toISOString();const worked=await processOne();processor.lastError=null;if(!worked)await new Promise(r=>setTimeout(r,POLL_MS));}
  catch(e){processor.lastError=String(e.message||e);console.error('whatsapp processor error',processor.lastError);await new Promise(r=>setTimeout(r,Math.max(POLL_MS,15000)));}
 }
}
app.get('/',(req,res)=>res.json({service:'payment-credit-worker',mode:'manual-submit+whatsapp-ocr',ok:true}));
app.get('/health',(req,res)=>res.json({ok:true,mode:'manual-submit+whatsapp-ocr',startedAt,dashboard:DASHBOARD_URL,webhookConfigured:!!(SUPABASE_URL&&SUPABASE_ANON_KEY&&INGEST_KEY&&VERIFY_TOKEN),mediaAccessConfigured:!!ACCESS_TOKEN,processor}));
app.get('/workflow',(req,res)=>res.json({steps:['WhatsApp screenshot received','Message queued and deduplicated','Mapped client identified','OCR reads amount/UTR','Draft goes to dashboard review','Human verifies GPay and approves','Human submits admin deposit','Successful confirmed then Mark Credited'],finalSubmit:'human-required'}));
app.get('/whatsapp/webhook',(req,res)=>{
 const mode=req.query['hub.mode'],token=req.query['hub.verify_token'],challenge=req.query['hub.challenge'];
 if(mode==='subscribe'&&VERIFY_TOKEN&&token===VERIFY_TOKEN)return res.status(200).send(challenge||'');
 return res.sendStatus(403);
});
app.post('/whatsapp/webhook',async(req,res)=>{
 res.sendStatus(200);
 try{
  const entries=Array.isArray(req.body?.entry)?req.body.entry:[];
  for(const entry of entries)for(const change of(entry.changes||[])){
   const value=change.value||{};
   for(const msg of(value.messages||[])){
    if(msg.type!=='image'||!msg.image?.id)continue;
    await rpc('whatsapp_enqueue',{p_api_key:INGEST_KEY,p_message_id:String(msg.id),p_sender_phone:String(msg.from||''),p_media_id:String(msg.image.id),p_mime_type:String(msg.image.mime_type||''),p_caption:msg.image.caption||null});
   }
  }
 }catch(e){console.error('whatsapp webhook enqueue failed',String(e.message||e));}
});
app.listen(PORT,()=>{console.log(`payment worker listening on ${PORT}`);processorLoop();});
process.on('SIGTERM',async()=>{try{if(ocrWorker)await ocrWorker.terminate();}finally{process.exit(0)}});
