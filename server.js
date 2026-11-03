// WhatsApp webhook + OCR queue service. Final financial submit remains human-only.
const express = require('express');
const { createWorker } = require('tesseract.js');
const QRCode = require('qrcode');
const app = express();
app.use(express.json({ limit: '2mb' }));

const PORT = process.env.PORT || 10000;
const DASHBOARD_URL = process.env.DASHBOARD_URL || 'https://payment-approval-dashboard-legend-fd31.vercel.app';
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || '';
const INGEST_KEY = process.env.WHATSAPP_INGEST_KEY || '';
const VERIFY_TOKEN = process.env.WHATSAPP_VERIFY_TOKEN || '';
const ACCESS_TOKEN = process.env.WHATSAPP_ACCESS_TOKEN || '';
const QR_SEND_KEY = process.env.QR_SEND_KEY || '';
const POLL_MS = Number(process.env.WHATSAPP_POLL_MS || 8000);
const startedAt = new Date().toISOString();

let processor = { running: false, lastPoll: null, lastInboxId: null, lastError: null };
let ocrWorker = null;
let activePhoneNumberId = process.env.WHATSAPP_PHONE_NUMBER_ID || '';

async function rpc(name, body) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', apikey: SUPABASE_ANON_KEY, Authorization: `Bearer ${SUPABASE_ANON_KEY}` },
    body: JSON.stringify(body)
  });
  const t = await r.text();
  if (!r.ok) throw new Error(t || `RPC ${name} failed`);
  return t ? JSON.parse(t) : null;
}

async function loadSavedPhoneNumberId() {
  try {
    const v = await rpc('whatsapp_get_phone_number_id', { p_api_key: INGEST_KEY });
    if (v) activePhoneNumberId = String(v);
  } catch (e) {
    console.error('load phone number id failed', String(e.message || e));
  }
}

async function savePhoneNumberId(id) {
  const v = String(id || '').trim();
  if (!v) return;
  activePhoneNumberId = v;
  try {
    await rpc('whatsapp_set_phone_number_id', { p_api_key: INGEST_KEY, p_phone_number_id: v });
  } catch (e) {
    console.error('save phone number id failed', String(e.message || e));
  }
}

function parseMoney(raw) {
  const n = Number(String(raw || '').replace(/[,\s]/g, ''));
  return Number.isFinite(n) && n > 0 && n < 100000000 ? n : null;
}

function normalizeOcrText(text) {
  return String(text || '')
    .replace(/\r/g, '')
    .replace(/[|]/g, 'I')
    .replace(/[₹]/g, ' ₹ ')
    .replace(/[ \t]+/g, ' ')
    .trim();
}

function extractPayment(text) {
  const raw = normalizeOcrText(text);
  const flat = raw.replace(/\n+/g, ' ').replace(/\s+/g, ' ').trim();

  let utr = null;
  let m = flat.match(/(?:UPI\s*(?:transaction)?\s*(?:ID|Id)|UTR|Transaction\s*(?:ID|Id)|Reference\s*(?:No|Number)?|Ref\s*(?:No|Number)?)\s*[:#-]?\s*([A-Z0-9]{10,30})/i);
  if (m) utr = m[1].replace(/\s/g, '');
  if (!utr) {
    m = flat.match(/\b\d{12}\b/);
    if (m) utr = m[0];
  }

  // Be conservative with amount detection. Only accept a number when the OCR text
  // clearly indicates currency or payment context. Never guess from the largest number.
  const lines = raw.split(/\n+/).map(s => s.trim()).filter(Boolean);
  const amountMatchers = [
    /(?:₹|Rs\.?|INR|Rupees?)\s*[:=\-]?\s*([0-9][0-9,]*(?:\.\d{1,2})?)/i,
    /([0-9][0-9,]*(?:\.\d{1,2})?)\s*(?:₹|Rs\.?|INR|Rupees?)/i,
    /(?:amount|paid|payment|sent|received|debited|credited|paying)\s*(?:of|is|:|₹|Rs\.?|INR|-)*\s*([0-9][0-9,]*(?:\.\d{1,2})?)/i
  ];

  let amount = null;
  const candidates = [];
  for (const line of lines) {
    for (const re of amountMatchers) {
      const hit = line.match(re);
      if (!hit) continue;
      const v = parseMoney(hit[1]);
      if (!v) continue;
      const digits = String(Math.trunc(v)).length;
      if (digits >= 10) continue;
      const hasCurrency = /₹|\bRs\.?\b|\bINR\b|Rupees?/i.test(line);
      const hasPaymentWord = /amount|paid|payment|sent|received|debited|credited|paying/i.test(line);
      const score = (hasCurrency ? 3 : 0) + (hasPaymentWord ? 2 : 0) - (digits >= 8 ? 2 : 0);
      candidates.push({ v, score });
    }
  }

  if (candidates.length) {
    candidates.sort((a, b) => b.score - a.score);
    if (candidates[0].score >= 2) amount = candidates[0].v;
  }

  return { amount, utr };
}

async function getOcrWorker() {
  if (!ocrWorker) ocrWorker = await createWorker('eng');
  return ocrWorker;
}

async function downloadWhatsAppMedia(mediaId) {
  const meta = await fetch(`https://graph.facebook.com/v26.0/${encodeURIComponent(mediaId)}`, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });
  if (!meta.ok) throw new Error(`Meta media lookup failed ${meta.status}`);
  const info = await meta.json();
  if (!info.url) throw new Error('Meta media URL missing');
  const media = await fetch(info.url, { headers: { Authorization: `Bearer ${ACCESS_TOKEN}` } });
  if (!media.ok) throw new Error(`Meta media download failed ${media.status}`);
  return Buffer.from(await media.arrayBuffer());
}

async function processOne() {
  if (!ACCESS_TOKEN) return false;
  const rows = await rpc('whatsapp_claim_next', { p_ingest_key: INGEST_KEY });
  const job = Array.isArray(rows) ? rows[0] : rows;
  if (!job || !job.inbox_id) return false;
  processor.lastInboxId = job.inbox_id;
  try {
    const image = await downloadWhatsAppMedia(job.media_id);
    const worker = await getOcrWorker();
    const result = await worker.recognize(image);
    const x = extractPayment(result.data?.text || '');
    const status = (x.amount && x.utr) ? 'ready' : 'needs_review';
    const missing = [];
    if (!x.amount) missing.push('amount');
    if (!x.utr) missing.push('UTR');
    const error = status === 'ready' ? null : `Could not confidently detect ${missing.join(' and ')}`;
    await rpc('whatsapp_finish', { p_ingest_key: INGEST_KEY, p_inbox_id: job.inbox_id, p_status: status, p_amount: x.amount, p_utr: x.utr, p_error: error });
  } catch (e) {
    const msg = String(e.message || e).slice(0, 500);
    await rpc('whatsapp_finish', { p_ingest_key: INGEST_KEY, p_inbox_id: job.inbox_id, p_status: 'failed', p_amount: null, p_utr: null, p_error: msg }).catch(() => {});
    throw e;
  }
  return true;
}

async function processorLoop() {
  if (processor.running) return;
  processor.running = true;
  while (true) {
    try {
      processor.lastPoll = new Date().toISOString();
      const worked = await processOne();
      processor.lastError = null;
      if (!worked) await new Promise(r => setTimeout(r, POLL_MS));
    } catch (e) {
      processor.lastError = String(e.message || e);
      console.error('whatsapp processor error', processor.lastError);
      await new Promise(r => setTimeout(r, Math.max(POLL_MS, 15000)));
    }
  }
}

function normalizePhone(v) { return String(v || '').replace(/[^0-9]/g, ''); }
function upiUri(upiId, payeeName, amount) {
  const p = new URLSearchParams({ pa: upiId, pn: payeeName || 'Payment', am: Number(amount).toFixed(2), cu: 'INR' });
  return `upi://pay?${p.toString()}`;
}

async function sendQrToWhatsApp({ to, amount, upi_id, payee_name }) {
  if (!ACCESS_TOKEN) throw new Error('WhatsApp access token missing');
  if (!activePhoneNumberId) throw new Error('WhatsApp phone number ID not configured');
  const phone = normalizePhone(to);
  const amt = parseMoney(amount);
  const upi = String(upi_id || '').trim();
  if (!phone) throw new Error('Customer phone missing');
  if (!amt) throw new Error('Valid amount required');
  if (!upi || !upi.includes('@')) throw new Error('Valid UPI ID required');

  const uri = upiUri(upi, String(payee_name || 'Payment').trim() || 'Payment', amt);
  const png = await QRCode.toBuffer(uri, { type: 'png', width: 700, margin: 2, errorCorrectionLevel: 'M' });
  const form = new FormData();
  form.append('messaging_product', 'whatsapp');
  form.append('type', 'image/png');
  form.append('file', new Blob([png], { type: 'image/png' }), 'payment-qr.png');

  const upload = await fetch(`https://graph.facebook.com/v26.0/${activePhoneNumberId}/media`, { method: 'POST', headers: { Authorization: `Bearer ${ACCESS_TOKEN}` }, body: form });
  const ut = await upload.text();
  if (!upload.ok) throw new Error(`QR media upload failed ${upload.status}: ${ut.slice(0, 180)}`);
  const uj = JSON.parse(ut);
  if (!uj.id) throw new Error('QR media id missing');

  const send = await fetch(`https://graph.facebook.com/v26.0/${activePhoneNumberId}/messages`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${ACCESS_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ messaging_product: 'whatsapp', to: phone, type: 'image', image: { id: uj.id, caption: `Payment QR • ₹${amt.toLocaleString('en-IN', { maximumFractionDigits: 2 })}` } })
  });
  const st = await send.text();
  if (!send.ok) throw new Error(`WhatsApp QR send failed ${send.status}: ${st.slice(0, 180)}`);
  return { ok: true, phone, amount: amt };
}

function legalPage(title, body) {
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${title}</title><style>body{font-family:Arial,sans-serif;max-width:820px;margin:40px auto;padding:0 20px;line-height:1.6;color:#172033}h1,h2{color:#111827}a{color:#2563eb}.muted{color:#6b7280}</style></head><body><h1>${title}</h1>${body}<p class="muted">Last updated: 3 November 2026</p></body></html>`;
}

app.get('/privacy', (req, res) => res.type('html').send(legalPage('Privacy Policy', `<p>This policy applies to paymentapp operated for Shiva contraction.</p><h2>Information we process</h2><p>When a user sends a WhatsApp message or payment screenshot, the service may process the sender phone number, WhatsApp profile name, message identifier, image metadata, payment amount and transaction reference/UTR detected from the image.</p><h2>How we use information</h2><p>Information is used only to receive payment evidence, prevent duplicate processing, assist manual payment review, and maintain operational records.</p><h2>Sharing</h2><p>We do not sell personal information. Data may be processed by service providers needed to run the system, including Meta/WhatsApp, hosting and database providers.</p><h2>Retention and security</h2><p>Operational data is retained only as needed for payment verification, audit and legal obligations, and is protected using access controls and server-side secrets.</p><h2>Your choices</h2><p>You may request deletion of eligible personal data using the data deletion instructions at <a href="/data-deletion">/data-deletion</a>.</p>`)));
app.get('/terms', (req, res) => res.type('html').send(legalPage('Terms of Service', `<p>paymentapp is a payment-evidence intake and review tool for Shiva contraction.</p><p>Users must provide accurate information and must not submit fraudulent, unlawful or misleading payment evidence. Automated extraction is only an aid; final payment confirmation is performed by an authorized human reviewer.</p><p>The service may be changed, suspended or restricted to protect security, prevent abuse or comply with law and platform policies.</p><p>Use of WhatsApp is also subject to Meta and WhatsApp terms and policies.</p>`)));
app.get('/data-deletion', (req, res) => res.type('html').send(legalPage('User Data Deletion Instructions', `<p>To request deletion of personal data associated with your WhatsApp interaction, contact the business through the same WhatsApp conversation or other official business support channel and state that you want your paymentapp data deleted.</p><p>Please include enough information to identify the relevant interaction, such as your WhatsApp number and approximate date of the message. Do not send passwords, OTPs, access tokens or other secrets.</p><p>After verification, eligible personal data will be deleted or anonymized unless retention is required for security, fraud prevention, accounting, legal or regulatory obligations.</p>`)));

app.get('/', (req, res) => res.json({ service: 'payment-credit-worker', mode: 'manual-submit+whatsapp-ocr', ok: true }));
app.get('/health', (req, res) => res.json({ ok: true, mode: 'manual-submit+whatsapp-ocr', startedAt, dashboard: DASHBOARD_URL, webhookConfigured: !!(SUPABASE_URL && SUPABASE_ANON_KEY && INGEST_KEY && VERIFY_TOKEN), mediaAccessConfigured: !!ACCESS_TOKEN, qrSendConfigured: !!(ACCESS_TOKEN && QR_SEND_KEY), phoneNumberIdDetected: !!activePhoneNumberId, processor }));
app.post('/whatsapp/send-qr', async (req, res) => {
  try {
    if (!QR_SEND_KEY || req.get('x-qr-send-key') !== QR_SEND_KEY) return res.status(403).json({ error: 'forbidden' });
    res.json(await sendQrToWhatsApp(req.body || {}));
  } catch (e) {
    res.status(400).json({ error: String(e.message || e) });
  }
});
app.get('/workflow', (req, res) => res.json({ steps: ['WhatsApp screenshot received', 'Message queued and deduplicated', 'WhatsApp profile name captured', 'OCR reads amount/UTR', 'Draft goes to dashboard review', 'Human verifies GPay and approves', 'Human submits admin deposit', 'Successful confirmed then Mark Credited'], finalSubmit: 'human-required' }));
app.get('/whatsapp/webhook', (req, res) => {
  const mode = req.query['hub.mode'], token = req.query['hub.verify_token'], challenge = req.query['hub.challenge'];
  if (mode === 'subscribe' && VERIFY_TOKEN && token === VERIFY_TOKEN) return res.status(200).send(challenge || '');
  return res.sendStatus(403);
});
app.post('/whatsapp/webhook', async (req, res) => {
  res.sendStatus(200);
  try {
    const entries = Array.isArray(req.body?.entry) ? req.body.entry : [];
    for (const entry of entries) for (const change of (entry.changes || [])) {
      const value = change.value || {};
      if (value.metadata?.phone_number_id) await savePhoneNumberId(value.metadata.phone_number_id);
      const contacts = Array.isArray(value.contacts) ? value.contacts : [];
      const nameByPhone = new Map(contacts.map(c => [String(c.wa_id || ''), String(c.profile?.name || '').trim()]));
      for (const msg of (value.messages || [])) {
        if (msg.type !== 'image' || !msg.image?.id) continue;
        const senderPhone = String(msg.from || '');
        const senderName = nameByPhone.get(senderPhone) || null;
        await rpc('whatsapp_enqueue', { p_api_key: INGEST_KEY, p_message_id: String(msg.id), p_sender_phone: senderPhone, p_media_id: String(msg.image.id), p_mime_type: String(msg.image.mime_type || ''), p_caption: msg.image.caption || null, p_sender_name: senderName });
      }
    }
  } catch (e) {
    console.error('whatsapp webhook enqueue failed', String(e.message || e));
  }
});

app.listen(PORT, '0.0.0.0', async () => {
  console.log(`payment worker listening on ${PORT}`);
  await loadSavedPhoneNumberId();
  processorLoop();
});
process.on('SIGTERM', async () => { try { if (ocrWorker) await ocrWorker.terminate(); } finally { process.exit(0); } });
