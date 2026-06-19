const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const { Queue, Worker } = require("bullmq");
const IORedis = require("ioredis");

const app = express();
app.use(express.json({ limit: "50mb" }));

// 🟢 Expor a pasta temporária publicamente para a Lovable acessar
app.use("/videos", express.static("/tmp/video-worker"));

// ---------- CONFIG (Render Env Vars) ----------
const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Google Drive — defina no painel de variáveis do Render
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const GOOGLE_REFRESH_TOKEN = process.env.GOOGLE_REFRESH_TOKEN;
const GOOGLE_DRIVE_FOLDER_ID = process.env.GOOGLE_DRIVE_FOLDER_ID;

const DEFAULT_WIDTH = parseInt(process.env.DEFAULT_WIDTH || "720", 10);
const DEFAULT_HEIGHT = parseInt(process.env.DEFAULT_HEIGHT || "1280", 10);
const DEFAULT_FPS = parseInt(process.env.DEFAULT_FPS || "30", 10);

// ---------- REDIS & QUEUE CONFIG ----------
if (!REDIS_URL) {
  console.error("❌ ERRO: Variável REDIS_URL não configurada no Render!");
}

const connection = new IORedis(REDIS_URL, {
  maxRetriesPerRequest: null,
});

const videoQueue = new Queue("video-processing", { connection });

// ---------- FUNÇÕES AUXILIARES ----------
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function cleanDir(p) {
  if (fs.existsSync(p)) fs.rmSync(p, { recursive: true, force: true });
}

async function downloadToFile(url, filePath) {
  const r = await axios({
    url,
    responseType: "stream",
    timeout: 120000,
    headers: { "User-Agent": "video-worker/1.0" },
  });

  await new Promise((resolve, reject) => {
    const w = fs.createWriteStream(filePath);
    r.data.pipe(w);
    w.on("finish", resolve);
    w.on("error", reject);
  });
}

async function callWebhook(webhook_url, webhook_secret, payload) {
  await axios.post(webhook_url, payload, {
    headers: {
      "Content-Type": "application/json",
      "x-webhook-secret": webhook_secret,
    },
    timeout: 30000,
  });
}

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    const p = spawn("ffmpeg", args, { stdio: ["ignore", "pipe", "pipe"] });
    p.on("error", reject);
    p.on("close", (code) => {
      if (code === 0) return resolve();
      reject(new Error(`ffmpeg saiu com code ${code}`));
    });
  });
}

// ---------- RESILIÊNCIA: FETCH COM RETRY + EXPONENTIAL BACKOFF ----------

async function fetchComRetry(url, options = {}, maxTentativas = 3) {
  let ultimoErro;
  for (let tentativa = 0; tentativa < maxTentativas; tentativa++) {
    try {
      const signal = AbortSignal.timeout(30_000); // 30s por tentativa
      const res = await fetch(url, { ...options, signal });
      return res;
    } catch (err) {
      ultimoErro = err;
      if (tentativa < maxTentativas - 1) {
        const delay = 1000 * Math.pow(2, tentativa); // 1s → 2s → 4s
        console.warn(`[fetchComRetry] Tentativa ${tentativa + 1} falhou (${err.message}). Aguardando ${delay}ms...`);
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }
  throw ultimoErro;
}

async function getGoogleAccessToken() {
  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET || !GOOGLE_REFRESH_TOKEN) {
    throw new Error("Credenciais Google Drive ausentes — configure GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET e GOOGLE_REFRESH_TOKEN no Render.");
  }
  const res = await fetchComRetry("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      refresh_token: GOOGLE_REFRESH_TOKEN,
      client_id: GOOGLE_CLIENT_ID,
      client_secret: GOOGLE_CLIENT_SECRET,
      grant_type: "refresh_token",
    }).toString(),
  });
  const data = await res.json();
  if (!data.access_token) {
    throw new Error(`Falha ao obter access_token: ${JSON.stringify(data)}`);
  }
  return data.access_token;
}

async function uploadParaDrive(filePath, fileName) {
  const accessToken = await getGoogleAccessToken();

  const fileBuffer = fs.readFileSync(filePath);
  const boundary = `----DriveUpload_${Date.now()}`;
  const metadataStr = JSON.stringify({
    name: fileName,
    ...(GOOGLE_DRIVE_FOLDER_ID ? { parents: [GOOGLE_DRIVE_FOLDER_ID] } : {}),
  });
  const body = Buffer.concat([
    Buffer.from(`--${boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n${metadataStr}\r\n--${boundary}\r\nContent-Type: video/mp4\r\n\r\n`),
    fileBuffer,
    Buffer.from(`\r\n--${boundary}--`),
  ]);

  const uploadRes = await fetchComRetry(
    "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,webViewLink",
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": `multipart/related; boundary=${boundary}`,
      },
      body,
    }
  );

  const result = await uploadRes.json();
  if (!uploadRes.ok) throw new Error(`Erro ao subir no Drive: ${JSON.stringify(result)}`);

  // Tornar o arquivo acessível publicamente por link
  await fetchComRetry(`https://www.googleapis.com/drive/v3/files/${result.id}/permissions`, {
    method: "POST",
    headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" },
    body: JSON.stringify({ role: "reader", type: "anyone" }),
  });

  return result.webViewLink || `https://drive.google.com/file/d/${result.id}/view`;
}

// ---------- ENDPOINT: ADICIONAR NA FILA ----------
app.post("/render", async (req, res) => {
  const body = req.body || {};
  const { job_id, broll_urls, audio_url } = body;

  if (!job_id || !audio_url || (!broll_urls && !body.timeline)) {
    return res.status(400).json({ error: "Campos obrigatórios ausentes" });
  }

  // Adiciona o trabalho na fila do BullMQ
  const job = await videoQueue.add("render-job", body, {
    removeOnComplete: true,
    removeOnFail: false,
    attempts: 2,                           // 1 retry automático em caso de falha
    backoff: { type: "fixed", delay: 5000 }, // aguarda 5s antes de reprocessar
  });

  console.log(`[Queue] Vídeo enfileirado: Job ID ${job_id} (Queue ID: ${job.id})`);
  
  // Responde imediatamente para o Supabase/Frontend
  res.json({ status: "queued", job_id, queue_id: job.id });
});

// ---------- WORKER: PROCESSAR FILA (1 POR VEZ) ----------
const worker = new Worker("video-processing", async (job) => {
  const body = job.data;
  const { job_id, webhook_url, webhook_secret, audio_url } = body;
  const output_config = body.output_config || {};
  
  const timeline = body.timeline || body.broll_timeline || output_config.timeline || [];
  const broll_urls = body.broll_urls || [];
  const subtitle_url = body.subtitle_url || output_config.subtitle_url;
  const subtitle_text = body.subtitle_text || output_config.subtitle_text;

  const width = Number(output_config.width || DEFAULT_WIDTH);
  const height = Number(output_config.height || DEFAULT_HEIGHT);
  const fps = Number(output_config.fps || DEFAULT_FPS);

  const workDir = path.join("/tmp", "video-worker", job_id);
  // Limpa restos de uma tentativa anterior antes de começar (segurança no retry)
  cleanDir(workDir);
  ensureDir(workDir);

  const audioPath = path.join(workDir, "audio.mp3");
  const outputPath = path.join(workDir, "output.mp4");
  const srtPath = path.join(workDir, "subs.srt");
  let activeSubtitlePath = null;
  const downloadedClipsMap = {};

  try {
    console.log(`[Worker] Iniciando Job ${job_id}...`);
    
    // Download do áudio
    await downloadToFile(audio_url, audioPath);

    const urlsToDownload = new Set();
    if (timeline && timeline.length > 0) {
      timeline.forEach(clip => urlsToDownload.add(clip.url || clip.src));
    } else {
      broll_urls.forEach(url => urlsToDownload.add(url));
    }

    // Download dos clipes
    console.log(`[job ${job_id}] Baixando clipes originais...`);
    let index = 0;
    for (const url of urlsToDownload) {
      const cPath = path.join(workDir, `raw_${index}.mp4`);
      await downloadToFile(url, cPath);
      downloadedClipsMap[url] = cPath; // 🟢 Corrigido aqui
      index++;
    }

    const normalizedClips = [];
    const vf = `fps=${fps},scale=${width}:${height}:force_original_aspect_ratio=increase,crop=${width}:${height},format=yuv420p`;

    // Processar clipes com FFmpeg
    console.log(`[job ${job_id}] Normalizando clipes...`);
    if (timeline && timeline.length > 0) {
      for (let i = 0; i < timeline.length; i++) {
        const clip = timeline[i];
        const rawPath = downloadedClipsMap[clip.url || clip.src];
        if (!rawPath) continue;

        const normPath = path.join(workDir, `slice_${i}.mp4`);
        const startTime = clip.start || clip.startTime || clip.ss || 0;
        
        await runFfmpeg([
          "-y", "-ss", String(startTime), "-t", "5", "-i", rawPath,
          "-vf", vf, "-c:v", "libx264", "-preset", "ultrafast", "-an", normPath
        ]);
        normalizedClips.push(normPath);
      }
    } else {
      let i = 0;
      for (const url in downloadedClipsMap) {
        const normPath = path.join(workDir, `slice_${i}.mp4`);
        await runFfmpeg([
          "-y", "-t", "5", "-i", downloadedClipsMap[url],
          "-vf", vf, "-c:v", "libx264", "-preset", "ultrafast", "-an", normPath
        ]);
        normalizedClips.push(normPath);
        i++;
      }
    }

    // Gerar Playlist para concatenação
    const playlistPath = path.join(workDir, "playlist.txt");
    fs.writeFileSync(playlistPath, normalizedClips.map(clip => `file '${clip}'`).join("\n"));

    // Sublegendas
    if (subtitle_url) {
      await downloadToFile(subtitle_url, srtPath);
      activeSubtitlePath = srtPath;
    } else if (subtitle_text) {
      fs.writeFileSync(srtPath, subtitle_text);
      activeSubtitlePath = srtPath;
    }

    // Render Final
    console.log(`[job ${job_id}] Renderizando vídeo final...`);
    const finalArgs = [
      "-y", "-f", "concat", "-safe", "0", "-i", playlistPath, "-i", audioPath
    ];

    if (activeSubtitlePath) {
      const forceStyle = `Alignment=2,MarginV=90,Fontname=Montserrat,Bold=1,Fontsize=8,BorderStyle=1,Outline=0.4,OutlineColour=&H00000000`;
      finalArgs.push("-vf", `subtitles=${activeSubtitlePath}:force_style='${forceStyle}'`);
    }

    finalArgs.push(
      "-map", "0:v:0", "-map", "1:a:0",
      "-c:v", "libx264", "-preset", "ultrafast", "-shortest", outputPath
    );

    await runFfmpeg(finalArgs);
    console.log(`[job ${job_id}] ✅ Render concluído. Fazendo upload para o Google Drive...`);

    const driveFileName = `video_${job_id}_${Date.now()}.mp4`;
    const video_url = await uploadParaDrive(outputPath, driveFileName);
    console.log(`[job ${job_id}] ✅ Upload no Drive concluído: ${video_url}`);

    await callWebhook(webhook_url, webhook_secret, { job_id, status: "completed", video_url });
    console.log(`[job ${job_id}] ✅ Webhook de sucesso enviado!`);

  } catch (e) {
    console.error(`[Worker Job ${job_id}] FALHOU:`, e.message);
    await callWebhook(webhook_url, webhook_secret, { job_id, status: "failed", error: e.message });
    throw e; // Permite que o BullMQ registre a falha
  } finally {
    // Limpeza automática após 15 minutos (para dar tempo de baixar)
    setTimeout(() => {
      if (fs.existsSync(workDir)) fs.rmSync(workDir, { recursive: true, force: true });
    }, 15 * 60 * 1000);
  }
}, {
  connection,
  concurrency: 1,        // 1 vídeo por vez — preserva CPU/RAM no Render
  lockDuration: 600_000, // 10 min — cobre qualquer renderização realista
  lockRenewTime: 120_000, // renova o lock a cada 2 min (lockDuration / 5)
});

app.get("/", (req, res) => res.send("Worker de Vídeo com Fila - Ativo"));
app.listen(PORT, () => console.log("Servidor rodando na porta", PORT));