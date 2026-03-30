const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

const app = express();
app.use(express.json({ limit: "50mb" }));

// Expor pasta temporária
app.use("/videos", express.static("/tmp/video-worker"));

// ---------- CONFIG ----------
const PORT = process.env.PORT || 3000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const FINAL_BUCKET = process.env.FINAL_BUCKET || "final-videos";

const DEFAULT_WIDTH = parseInt(process.env.DEFAULT_WIDTH || "720", 10);
const DEFAULT_HEIGHT = parseInt(process.env.DEFAULT_HEIGHT || "1280", 10);
const DEFAULT_FPS = parseInt(process.env.DEFAULT_FPS || "30", 10);

// ---------- SISTEMA DE FILA ----------
const videoQueue = [];
let isProcessing = false;

// Funções Auxiliares
function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }

async function downloadToFile(url, filePath) {
  const r = await axios({ url, responseType: "stream", timeout: 120000 });
  await new Promise((resolve, reject) => {
    const w = fs.createWriteStream(filePath);
    r.data.pipe(w);
    w.on("finish", resolve);
    w.on("error", reject);
  });
}

async function uploadMp4ToSupabase(localFilePath, objectPath) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) throw new Error("Credenciais Supabase ausentes");
  const uploadUrl = `${SUPABASE_URL}/storage/v1/object/${FINAL_BUCKET}/${objectPath}`;
  const stat = fs.statSync(localFilePath);
  await axios.put(uploadUrl, fs.createReadStream(localFilePath), {
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "video/mp4",
      "Content-Length": stat.size,
      "x-upsert": "true",
    },
    maxBodyLength: Infinity, timeout: 300000,
  });
  return `${SUPABASE_URL}/storage/v1/object/public/${FINAL_BUCKET}/${objectPath}`;
}

async function callWebhook(url, secret, payload) {
  try {
    await axios.post(url, payload, { headers: { "x-webhook-secret": secret }, timeout: 30000 });
  } catch (e) { console.log("Erro no Webhook:", e.message); }
}

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    const p = spawn("ffmpeg", args);
    p.on("close", (code) => code === 0 ? resolve() : reject(new Error(`FFmpeg erro ${code}`)));
  });
}

// ---------- PROCESSADOR DA FILA ----------
async function processQueue() {
  if (isProcessing || videoQueue.length === 0) return;
  
  isProcessing = true;
  const job = videoQueue.shift(); // Pega o primeiro da fila
  const { job_id, webhook_url, webhook_secret, audio_url, timeline, broll_urls, output_config, subtitle_url, subtitle_text } = job;
  
  const workDir = path.join("/tmp", "video-worker", job_id);
  ensureDir(workDir);

  try {
    console.log(`[Job ${job_id}] Iniciando processamento...`);
    
    // 1. Downloads
    const audioPath = path.join(workDir, "audio.mp3");
    await downloadToFile(audio_url, audioPath);

    const urlsToDownload = timeline.length > 0 ? timeline.map(c => c.url || c.src) : broll_urls;
    const downloadedClipsMap = {};
    for (let i = 0; i < urlsToDownload.length; i++) {
      const p = path.join(workDir, `raw_${i}.mp4`);
      await downloadToFile(urlsToDownload[i], p);
      downloadedClipsMap[urlsToDownload[i]] = p;
    }

    // 2. Normalização de Clipes
    const width = output_config.width || DEFAULT_WIDTH;
    const height = output_config.height || DEFAULT_HEIGHT;
    const fps = output_config.fps || DEFAULT_FPS;
    const vf = `fps=${fps},scale=${width}:${height}:force_original_aspect_ratio=increase,crop=${width}:${height},format=yuv420p`;
    
    const normalizedClips = [];
    for (let i = 0; i < (timeline.length || broll_urls.length); i++) {
      const normPath = path.join(workDir, `slice_${i}.mp4`);
      const clip = timeline[i] || { url: broll_urls[i], start: 0 };
      const rawPath = downloadedClipsMap[clip.url || clip.src];
      
      await runFfmpeg([
        "-y", "-ss", String(clip.start || 0), "-t", "5", "-i", rawPath,
        "-vf", vf, "-c:v", "libx264", "-preset", "ultrafast", "-an", normPath
      ]);
      normalizedClips.push(normPath);
    }

    // 3. Legendas
    let activeSubtitlePath = null;
    const srtPath = path.join(workDir, "subs.srt");
    if (subtitle_url) {
      await downloadToFile(subtitle_url, srtPath);
      activeSubtitlePath = srtPath;
    } else if (subtitle_text) {
      fs.writeFileSync(srtPath, subtitle_text);
      activeSubtitlePath = srtPath;
    }

    // 4. Montagem Final
    const playlistPath = path.join(workDir, "playlist.txt");
    fs.writeFileSync(playlistPath, normalizedClips.map(p => `file '${p}'`).join("\n"));
    
    const outputPath = path.join(workDir, "output.mp4");
    const finalArgs = ["-y", "-f", "concat", "-safe", "0", "-i", playlistPath, "-i", audioPath];
    
    if (activeSubtitlePath) {
      const style = `Alignment=2,MarginV=90,Fontname=Montserrat,Bold=1,Fontsize=8,PrimaryColour=&H00FFFFFF,BorderStyle=1,Outline=0.4`;
      finalArgs.push("-vf", `subtitles=${activeSubtitlePath}:force_style='${style}'`);
    }

    finalArgs.push("-c:v", "libx264", "-preset", "ultrafast", "-crf", "30", "-c:a", "aac", "-shortest", outputPath);
    await runFfmpeg(finalArgs);

    // 5. Upload para Supabase (O seu desejo)
    console.log(`[Job ${job_id}] Fazendo upload para Supabase...`);
    const finalUrl = await uploadMp4ToSupabase(outputPath, `${job_id}/final_video.mp4`);

    // 6. Webhook de Sucesso
    await callWebhook(webhook_url, webhook_secret, { job_id, status: "completed", video_url: finalUrl });

  } catch (error) {
    console.error(`[Job ${job_id}] Erro:`, error.message);
    await callWebhook(webhook_url, webhook_secret, { job_id, status: "failed", error: error.message });
  } finally {
    // Limpeza e Próximo da fila
    setTimeout(() => fs.rmSync(workDir, { recursive: true, force: true }), 5000);
    isProcessing = false;
    processQueue(); 
  }
}

// ---------- ENDPOINTS ----------
app.post("/render", (req, res) => {
  const jobData = {
    job_id: req.body.job_id,
    webhook_url: req.body.webhook_url,
    webhook_secret: req.body.webhook_secret,
    audio_url: req.body.audio_url,
    timeline: req.body.timeline || [],
    broll_urls: req.body.broll_urls || [],
    output_config: req.body.output_config || {},
    subtitle_url: req.body.subtitle_url,
    subtitle_text: req.body.subtitle_text
  };

  videoQueue.push(jobData);
  processQueue(); // Tenta processar

  res.json({ status: "queued", job_id: jobData.job_id, position: videoQueue.length });
});

app.listen(PORT, () => console.log(`Worker ativo na porta ${PORT}`));
