const express=require('express');
const app=express();
app.use(express.json({limit:'2mb'}));
const PORT=process.env.PORT||10000;
const DASHBOARD_URL=process.env.DASHBOARD_URL||'https://payment-approval-dashboard-legend-fd31.vercel.app';
const SUPABASE_URL=process.env.SUPABASE_URL||'';
const SUPABASE_ANON_KEY=process.env.SUPABASE_ANON_KEY||'';
const INGEST_KEY=process.env.WHATSAPP_INGEST_KEY||'';
const VERIFY_TOKEN=process.env.WHATSAPP_VERIFY_TOKEN||'';
const startedAt=new Date().toISOString();
async function rpc(name,body){
 const r=await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`,{method:'POST',headers:{'Content-Type':'application/json',apikey:SUPABASE_ANON_KEY,Authorization:`Bearer ${SUPABASE_ANON_KEY}`},body:JSON.stringify(body)});
 const t=await r.text(); if(!r.ok) throw new Error(t||`RPC ${name} failed`); return t?JSON.parse(t):null;
}
app.get('/',(req,res)=>res.json({service:'payment-credit-worker',mode:'manual-submit+whatsapp-ingest',ok:true}));
app.get('/health',(req,res)=>res.json({ok:true,mode:'manual-submit+whatsapp-ingest',startedAt,dashboard:DASHBOARD_URL,whatsappWebhookConfigured:!!(SUPABASE_URL&&SUPABASE_ANON_KEY&&INGEST_KEY&&VERIFY_TOKEN)}));
app.get('/workflow',(req,res)=>res.json({steps:['WhatsApp screenshot received','Message queued with sender/media ID','OCR worker reads amount/UTR','Dashboard pending draft created after review','GPay verified by human','Admin deposit submitted by human','Successful confirmed','Payment marked credited'],finalSubmit:'human-required'}));
app.get('/whatsapp/webhook',(req,res)=>{
 const mode=req.query['hub.mode'],token=req.query['hub.verify_token'],challenge=req.query['hub.challenge'];
 if(mode==='subscribe'&&VERIFY_TOKEN&&token===VERIFY_TOKEN)return res.status(200).send(challenge||'');
 return res.sendStatus(403);
});
app.post('/whatsapp/webhook',async(req,res)=>{
 res.sendStatus(200);
 try{
  const entries=Array.isArray(req.body?.entry)?req.body.entry:[];
  for(const entry of entries) for(const change of (entry.changes||[])){
   const value=change.value||{};
   for(const msg of (value.messages||[])){
    if(msg.type!=='image'||!msg.image?.id) continue;
    await rpc('whatsapp_enqueue',{p_api_key:INGEST_KEY,p_message_id:String(msg.id),p_sender_phone:String(msg.from||''),p_media_id:String(msg.image.id),p_mime_type:String(msg.image.mime_type||''),p_caption:msg.image.caption||null});
   }
  }
 }catch(e){console.error('whatsapp webhook enqueue failed',String(e.message||e));}
});
app.listen(PORT,()=>console.log(`payment worker listening on ${PORT}`));