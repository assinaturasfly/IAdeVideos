const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(express.json({ limit: "50mb" }));

// 🟢 PASSO 1: Expor a pasta temporária publicamente
app.use("/videos", express.static("/tmp/video-worker"));

// ---------- CONFIG (Render Env Vars) ----------
const PORT = process.env.PORT || 3000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const FINAL_BUCKET = process.env.FINAL_BUCKET || "final-videos";

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;

const DEFAULT_WIDTH = parseInt(process.env.DEFAULT_WIDTH || "720", 10);
const DEFAULT_HEIGHT = parseInt(process.env.DEFAULT_HEIGHT || "1280", 10);
const DEFAULT_FPS = parseInt(process.env.DEFAULT_FPS || "30", 10);
// --------------------------------------------

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

app.get("/", (req, res) => res.send("Worker Fila Ativo - Monitorando..."));
app.get("/health", (req, res) => res.json({ ok: true }));

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

async function downloadToFile(url, filePath) {
  if (!url || !url.startsWith('http')) throw new Error(`URL Inválida: ${url}`);
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
  if (!webhook_url) return;
  await axios.post(webhook_url, payload, {
    headers: {
      "Content-Type": "application/json",
      "x-webhook-secret": webhook_secret || "",
    },
    timeout: 30000,
  }).catch(e => console.log("[Webhook] Falha ao notificar:", e.message));
}

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    console.log("[ffmpeg] cmd:", `ffmpeg ${args.join(" ")}`);
    const p = spawn("ffmpeg", args);

    p.stderr.on("data", (d) => console.log("[ffmpeg][log]", d.toString().trim()));

    p.on("error", reject);
    p.on("close", (code) => {
      if (code === 0) return resolve();
      reject(new Error(`ffmpeg saiu com code ${code}`));
    });
  });
}

// ----------------------------------------------------------------------
// MOTOR DA FILA (LOOP INFINITO)
// ----------------------------------------------------------------------
async function processarFila() {
  let job_id = null;
  let workDir = null;

  try {
    console.log("[FILA] Verificando novos vídeos no Supabase...");

    // 1. Busca o próximo trabalho com status 'na_fila'
    const { data: job, error: buscaError } = await supabase
      .from('videos')
      .select('*')
      .eq('status', 'na_fila')
      .order('created_at', { ascending: true })
      .limit(1)
      .single();

    if (buscaError) {
      if (buscaError.code !== 'PGRST116') { // Ignora erro de "não encontrado"
          console.error("[SUPABASE ERRO]:", buscaError.message);
      }
      setTimeout(processarFila, 5000);
      return;
    }

    if (!job) {
      // Se não houver nada, aguarda 5 segundos e tenta de novo
      setTimeout(processarFila, 5000);
      return;
    }

    job_id = job.id;
    console.log(`[FILA] >>> VÍDEO ENCONTRADO! ID: ${job_id}. Iniciando...`);
    
    // Trava o vídeo no banco para evitar que outro worker pegue
    await supabase.from('videos').update({ status: 'processando' }).eq('id', job_id);

    const audio_url = job.narration_audio_url || job.audio_url;
    const subtitle_url = job.subtitle_url;
    const raw_broll_urls = job.b_roll_video_urls || [];
    let timeline = [];

    // Reconstrução da timeline (Sua lógica de 15 cortes)
    if (job.timeline_data && Array.isArray(job.timeline_data)) {
        timeline = job.timeline_data;
    } else if (raw_broll_urls.length > 0) {
        for (let i = 0; i < 15; i++) {
            const urlToUse = raw_broll_urls[i % raw_broll_urls.length];
            timeline.push({ url: urlToUse, start: 0 });
        }
    }

    if (!audio_url || timeline.length === 0) {
      throw new Error("Dados insuficientes: narration_audio_url ou b_roll_video_urls vazios.");
    }

    workDir = path.join("/tmp", "video-worker", job_id);
    ensureDir(workDir);

    const audioPath = path.join(workDir, "audio.mp3");
    const outputPath = path.join(workDir, "output.mp4");
    const srtPath = path.join(workDir, "subs.srt");
    
    let hasSubtitles = false;

    // Download do áudio
    console.log(`[job ${job_id}] Baixando áudio...`);
    await downloadToFile(audio_url, audioPath);

    // Download da legenda (Subtitle_url preenchido pela Edge Function)
    if (subtitle_url) {
      console.log(`[job ${job_id}] Baixando arquivo .srt...`);
      await downloadToFile(subtitle_url, srtPath);
      hasSubtitles = true;
    }

    // Processamento de clipes
    const normalizedClips = [];
    const vf = `fps=${DEFAULT_FPS},scale=${DEFAULT_WIDTH}:${DEFAULT_HEIGHT}:force_original_aspect_ratio=increase,crop=${DEFAULT_WIDTH}:${DEFAULT_HEIGHT},format=yuv420p`;

    for (let i = 0; i < timeline.length; i++) {
      const clip = timeline[i];
      const clipUrl = clip.url || clip;
      const rawPath = path.join(workDir, `raw_${i}.mp4`);
      const normPath = path.join(workDir, `clip_${i}.mp4`);
      
      console.log(`[job ${job_id}] Processando clipe ${i+1}/${timeline.length}...`);
      await downloadToFile(clipUrl, rawPath);
      
      await runFfmpeg([
        "-y", "-ss", String(clip.start || 0), "-t", "5", "-i", rawPath,
        "-vf", vf, "-c:v", "libx264", "-preset", "ultrafast", "-an", normPath
      ]);
      normalizedClips.push(normPath);
    }

    const playlistPath = path.join(workDir, "playlist.txt");
    fs.writeFileSync(playlistPath, normalizedClips.map(p => `file '${p}'`).join("\n"));

    // Montagem final (Sua lógica de estilo de legenda original)
    console.log(`[job ${job_id}] Renderizando vídeo final...`);
    const finalArgs = [
      "-y", "-f", "concat", "-safe", "0", "-i", playlistPath,
      "-i", audioPath
    ];

    if (hasSubtitles) {
      // SUA LÓGICA DE ESTILO ORIGINAL MANTIDA
      const forceStyle = `Alignment=2,MarginV=90,Fontname=Montserrat,Bold=1,Fontsize=8,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=1,Outline=0.4,Shadow=0`;
      finalArgs.push("-vf", `subtitles=${srtPath}:force_style='${forceStyle}'`);
    }

    finalArgs.push(
      "-map", "0:v:0", "-map", "1:a:0",
      "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30",
      "-c:a", "aac", "-b:a", "128k",
      "-shortest", "-movflags", "+faststart", "-pix_fmt", "yuv420p",
      outputPath
    );

    await runFfmpeg(finalArgs);

    // Finalização e URL
    const serverUrl = process.env.RENDER_EXTERNAL_URL || `https://${process.env.RENDER_SERVICE_NAME}.onrender.com`;
    const video_url = `${serverUrl}/videos/${job_id}/output.mp4`;

    await supabase.from('videos').update({ 
      status: 'in_review', 
      final_video_url: video_url 
    }).eq('id', job_id);

    await callWebhook(WEBHOOK_URL, WEBHOOK_SECRET, { job_id, status: "completed", video_url });

    console.log(`[job ${job_id}] ✅ CONCLUÍDO COM SUCESSO!`);
    
    // Limpeza (Opcional: 15 minutos depois)
    setTimeout(() => {
        fs.rmSync(workDir, { recursive: true, force: true });
    }, 15 * 60 * 1000);

    // Volta para o início da fila imediatamente
    processarFila();

  } catch (e) {
    console.error(`[ERRO CRÍTICO] Job ${job_id}:`, e.message);
    if (job_id) {
      await supabase.from('videos').update({ status: 'failed' }).eq('id', job_id);
    }
    setTimeout(processarFila, 10000); // Aguarda 10s para tentar de novo após erro
  }
}

// Inicia o motor
processarFila();

app.listen(PORT, () => console.log(`Worker Fila ON na porta ${PORT}`));
