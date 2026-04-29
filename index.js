const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const { Queue, Worker } = require("bullmq");
const IORedis = require("ioredis");

const app = express();
app.use(express.json({ limit: "50mb" }));
app.use("/videos", express.static("/tmp/video-worker"));

const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL;

const connection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null });
const videoQueue = new Queue("video-processing", { connection });

// Função para rodar o FFmpeg de forma silenciosa e limpa
function runFfmpeg(args, stepName = "Processando") {
  return new Promise((resolve, reject) => {
    // Adicionamos -loglevel error para ele não cuspir lixo técnico
    // Adicionamos -nostdin para ele não travar esperando teclado
    const finalArgs = ["-hide_banner", "-loglevel", "error", "-nostdin", "-y", ...args];
    
    console.log(`⏳ [FFMPEG] Iniciando: ${stepName}...`);
    
    const p = spawn("ffmpeg", finalArgs);

    // Capturamos apenas erros reais
    p.stderr.on("data", (data) => {
      console.error(`❌ [ERRO FFMPEG]: ${data}`);
    });

    p.on("close", (code) => {
      if (code === 0) {
        console.log(`✅ [SUCESSO]: ${stepName} concluído.`);
        return resolve();
      }
      reject(new Error(`${stepName} falhou com código ${code}`));
    });
  });
}

async function downloadToFile(url, filePath) {
  const r = await axios({ url, responseType: "stream", timeout: 120000 });
  await new Promise((resolve, reject) => {
    const w = fs.createWriteStream(filePath);
    r.data.pipe(w);
    w.on("finish", resolve);
    w.on("error", reject);
  });
}

async function getMediaDuration(filePath) {
  return new Promise((resolve) => {
    const p = spawn("ffprobe", ["-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", filePath]);
    let output = "";
    p.stdout.on("data", (data) => output += data.toString());
    p.on("close", () => {
      const dur = parseFloat(output.trim());
      resolve(isNaN(dur) ? 0 : dur);
    });
    p.on("error", () => resolve(0));
  });
}

app.post("/render", async (req, res) => {
  const { job_id, broll_urls, audio_url } = req.body;
  if (!job_id || !audio_url) return res.status(400).json({ error: "Dados ausentes" });

  // Adicionamos attempts: 1 para ele não ficar tentando pra sempre se der erro
  const job = await videoQueue.add("render-job", req.body, { 
    removeOnComplete: true, 
    removeOnFail: { age: 3600 }, // Remove falhas após 1 hora
    attempts: 1 
  });

  console.log(`🚀 [FILA] Novo vídeo recebido! ID: ${job_id}`);
  res.json({ status: "queued", job_id });
});

const worker = new Worker("video-processing", async (job) => {
  const { job_id, audio_url, webhook_url, webhook_secret, logo_url } = job.data;
  const workDir = path.join("/tmp", "video-worker", job_id);
  const output_config = job.data.output_config || {};
  const width = output_config.width || 720;
  const height = output_config.height || 1280;

  try {
    fs.mkdirSync(workDir, { recursive: true });
    const audioPath = path.join(workDir, "audio.mp3");
    const outputPath = path.join(workDir, "output.mp4");

    console.log(`📦 [JOB ${job_id}] PREPARANDO: Baixando áudio...`);
    await downloadToFile(audio_url, audioPath);
    const duration = await getMediaDuration(audioPath);

    const broll_urls = job.data.broll_urls || [];
    const downloadedClips = [];

    console.log(`🎥 [JOB ${job_id}] ETAPA 1: Baixando ${broll_urls.length} vídeos de fundo...`);
    for (let i = 0; i < broll_urls.length; i++) {
      const p = path.join(workDir, `raw_${i}.mp4`);
      await downloadToFile(broll_urls[i], p);
      downloadedClips.push(p);
    }

    const vf = `fps=30,scale=${width}:${height}:force_original_aspect_ratio=increase,crop=${width}:${height},eq=contrast=1.05:saturation=1.1,unsharp=5:5:0.8:5:5:0.0,format=yuv420p`;
    const normalizedClips = [];

    console.log(`⚙️ [JOB ${job_id}] ETAPA 2: Aplicando Nitidez e Cores nos clipes...`);
    for (let i = 0; i < downloadedClips.length; i++) {
      const normPath = path.join(workDir, `slice_${i}.mp4`);
      await runFfmpeg(["-ss", "0", "-t", "5", "-i", downloadedClips[i], "-vf", vf, "-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-an", normPath], `Corte do Clipe ${i+1}/${downloadedClips.length}`);
      normalizedClips.push(normPath);
    }

    const playlistPath = path.join(workDir, "playlist.txt");
    fs.writeFileSync(playlistPath, normalizedClips.map(p => `file '${p}'`).join("\n"));

    console.log(`🎬 [JOB ${job_id}] ETAPA 3: Montagem Final (Vídeo + Áudio + Estilo)...`);
    const finalArgs = ["-f", "concat", "-safe", "0", "-i", playlistPath, "-i", audioPath];
    
    // Filtro de legenda/logo simplificado para o log
    let filter = "[0:v]copy[v]"; 
    finalArgs.push("-map", "[v]", "-map", "1:a:0", "-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-pix_fmt", "yuv420p", "-shortest", outputPath);

    await runFfmpeg(finalArgs, "Renderização Final");

    const serverUrl = process.env.RENDER_EXTERNAL_URL || `https://${process.env.RENDER_HOSTNAME}`;
    const video_url = `${serverUrl}/videos/${job_id}/output.mp4`;

    await axios.post(webhook_url, { job_id, status: "completed", video_url }, { headers: { "x-webhook-secret": webhook_secret } });
    console.log(`✨ [JOB ${job_id}] FINALIZADO COM SUCESSO!`);

  } catch (e) {
    console.error(`💥 [JOB ${job_id}] ERRO CRÍTICO:`, e.message);
    await axios.post(webhook_url, { job_id, status: "failed", error: e.message }, { headers: { "x-webhook-secret": webhook_secret } });
  }
}, { connection, concurrency: 1 });

app.listen(PORT, () => console.log(`🚀 Worker de Vídeo Ativo na porta ${PORT}`));
