const express = require("express");
const cors = require("cors");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const { Queue, Worker } = require("bullmq");
const IORedis = require("ioredis");
const { google } = require("googleapis");

const app = express();
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'Accept']
}));
app.use(express.json({ limit: "50mb" }));
app.use("/videos", express.static("/tmp/video-worker"));

const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL;

const connection = new IORedis(REDIS_URL, { maxRetriesPerRequest: null });
const videoQueue = new Queue("video-processing", { connection });
videoQueue.obliterate({ force: true }).catch(() => {});

function runFfmpeg(args, stepName = "Processando") {
  return new Promise((resolve, reject) => {
    const finalArgs = ["-hide_banner", "-loglevel", "error", "-nostdin", "-y", ...args];
    console.log(`⏳ [FFMPEG] Iniciando: ${stepName}...`);
    const p = spawn("ffmpeg", finalArgs);
    p.stderr.on("data", (data) => console.error(`❌ [ERRO FFMPEG]: ${data}`));
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

  const job = await videoQueue.add("render-job", req.body, { 
    removeOnComplete: true, 
    removeOnFail: { age: 3600 }, 
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
    const srtPath = path.join(workDir, "subs.srt");

    console.log(`📦 [JOB ${job_id}] Baixando áudio...`);
    await downloadToFile(audio_url, audioPath);
    const duration = await getMediaDuration(audioPath);

    const broll_urls = job.data.broll_urls || [];
    const downloadedClips = [];

    for (let i = 0; i < broll_urls.length; i++) {
      const p = path.join(workDir, `raw_${i}.mp4`);
      await downloadToFile(broll_urls[i], p);
      downloadedClips.push(p);
    }

    // Configura a altura do vídeo proporcionalmente (65% do total)
    const liftVideo = job.data.logo_always_on === true && job.data.logo_width_type === 'full';
    const videoHeight = liftVideo ? Math.round(height * 0.65) : height;

    const vf = `fps=30,scale=${width}:${videoHeight}:force_original_aspect_ratio=increase,crop=${width}:${videoHeight},eq=contrast=1.05:saturation=1.3,unsharp=5:5:0.8:5:5:0.0,format=yuv420p`;
    const normalizedClips = [];

    for (let i = 0; i < downloadedClips.length; i++) {
      const normPath = path.join(workDir, `slice_${i}.mp4`);
      await runFfmpeg(["-ss", "0", "-t", "5", "-i", downloadedClips[i], "-vf", vf, "-c:v", "libx264", "-preset", "veryfast", "-crf", "16", "-an", normPath], `Corte Clipe ${i+1}`);
      normalizedClips.push(normPath);
    }

    const playlistPath = path.join(workDir, "playlist.txt");
    fs.writeFileSync(playlistPath, normalizedClips.map(p => `file '${p}'`).join("\n"));

    let activeSubtitlePath = null;
    const subtitle_url = job.data.subtitle_url || output_config.subtitle_url;
    const subtitle_text = job.data.subtitle_text || output_config.subtitle_text;

    if (subtitle_url) {
      await downloadToFile(subtitle_url, srtPath);
      activeSubtitlePath = srtPath;
    } else if (subtitle_text) {
      fs.writeFileSync(srtPath, subtitle_text);
      activeSubtitlePath = srtPath;
    }

    const finalArgs = ["-stream_loop", "-1", "-f", "concat", "-safe", "0", "-i", playlistPath, "-i", audioPath];
    
    if (logo_url) {
      await downloadToFile(logo_url, path.join(workDir, "logo.png"));
      finalArgs.push("-i", path.join(workDir, "logo.png"));
    }

    let videoMap = "0:v:0";
    
    // 👇 ADAPTAÇÃO AUTOMÁTICA RESPONSIVA 👇
    const marginV = job.data.subtitle_margin_v !== undefined ? job.data.subtitle_margin_v : 90;
    const forceStyle = `Alignment=2,MarginV=${marginV},Fontname=Montserrat,Bold=1,Fontsize=8,BorderStyle=1,Outline=0.4,OutlineColour=&H00000000`;
    
    const totalVideoLength = duration > 0 ? duration : normalizedClips.length * 5;
    const isAlwaysOn = job.data.logo_always_on === true;
    const showLogoFrom = isAlwaysOn ? 0 : Math.max(0, totalVideoLength - 3);
    
    const isFullWidth = job.data.logo_width_type === 'full' || job.data.logo_width === 1080;
    const logoWidth = isFullWidth ? width : (job.data.logo_width || 350);
    const logoX = isFullWidth ? 0 : (job.data.logo_x !== undefined ? job.data.logo_x : '(W-w)/2');
    const logoY = isFullWidth ? 'H-h' : (job.data.logo_y !== undefined ? job.data.logo_y : (job.data.logo_position === 'bottom' ? 'H-h-40' : '40'));

    // 👇 RENDERIZAÇÃO COMPLEXA DA TELA DIVIDIDA 👇
    if (activeSubtitlePath && logo_url) {
      const filterComplex = liftVideo 
        ? `[0:v]pad=${width}:${height}:0:0:black[padded];[padded]subtitles=${activeSubtitlePath}:force_style='${forceStyle}'[subbed];[2:v]scale=${logoWidth}:-1[logo];[subbed][logo]overlay=${logoX}:${logoY}:enable='gte(t,${showLogoFrom})'[v]`
        : `[0:v]subtitles=${activeSubtitlePath}:force_style='${forceStyle}'[subbed];[2:v]scale=${logoWidth}:-1[logo];[subbed][logo]overlay=${logoX}:${logoY}:enable='gte(t,${showLogoFrom})'[v]`;
      finalArgs.push("-filter_complex", filterComplex);
      videoMap = "[v]";
    } else if (activeSubtitlePath && !logo_url) {
      if (liftVideo) {
        const filterComplex = `[0:v]pad=${width}:${height}:0:0:black[padded];[padded]subtitles=${activeSubtitlePath}:force_style='${forceStyle}'[v]`;
        finalArgs.push("-filter_complex", filterComplex);
        videoMap = "[v]";
      } else {
        finalArgs.push("-vf", `subtitles=${activeSubtitlePath}:force_style='${forceStyle}'`);
      }
    } else if (!activeSubtitlePath && logo_url) {
      const filterComplex = liftVideo
        ? `[0:v]pad=${width}:${height}:0:0:black[padded];[2:v]scale=${logoWidth}:-1[logo];[padded][logo]overlay=${logoX}:${logoY}:enable='gte(t,${showLogoFrom})'[v]`
        : `[2:v]scale=${logoWidth}:-1[logo];[0:v][logo]overlay=${logoX}:${logoY}:enable='gte(t,${showLogoFrom})'[v]`;
      finalArgs.push("-filter_complex", filterComplex);
      videoMap = "[v]";
    }

    finalArgs.push("-map", videoMap, "-map", "1:a:0", "-c:v", "libx264", "-preset", "veryfast", "-crf", "16", "-pix_fmt", "yuv420p", "-shortest", outputPath);

    await runFfmpeg(finalArgs, "Renderização Final");

    let finalVideoUrl = "";
    try {
      console.log(`🔍 [DRIVE] Buscando token no Supabase...`);
      const SUPABASE_URL = process.env.SUPABASE_URL;
      const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
      
      const { data: configData } = await axios.get(`${SUPABASE_URL}/rest/v1/app_config?key=eq.google_drive_refresh_token&select=value`, {
        headers: { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}` }
      });
      
      const refreshToken = configData[0]?.value;
      if (!refreshToken) throw new Error("Token não encontrado no banco.");

      const clientId = process.env.GOOGLE_CLIENT_ID;
      const clientSecret = process.env.GOOGLE_CLIENT_SECRET;
      const folderId = process.env.GOOGLE_DRIVE_FOLDER_ID;

      console.log(`☁️ [DRIVE] Autenticando e fazendo upload pesado...`);
      const oauth2Client = new google.auth.OAuth2(clientId, clientSecret);
      oauth2Client.setCredentials({ refresh_token: refreshToken });
      const drive = google.drive({ version: 'v3', auth: oauth2Client });

      const response = await drive.files.create({
        requestBody: { name: `video_${job_id}.mp4`, parents: [folderId] },
        media: { mimeType: 'video/mp4', body: fs.createReadStream(outputPath) },
        fields: 'id, webViewLink'
      });

      await drive.permissions.create({
        fileId: response.data.id,
        requestBody: { role: 'reader', type: 'anyone' }
      });

      finalVideoUrl = response.data.webViewLink;
      console.log(`✅ [DRIVE] Link permanente gerado: ${finalVideoUrl}`);

    } catch (err) {
      console.error("❌ Erro ao subir no Drive. Usando link temporário.", err.message);
      const serverUrl = process.env.RENDER_EXTERNAL_URL || `https://${process.env.RENDER_HOSTNAME}`;
      finalVideoUrl = `${serverUrl}/videos/${job_id}/output.mp4`;
    }

    await axios.post(webhook_url, { job_id, status: "completed", video_url: finalVideoUrl }, { headers: { "x-webhook-secret": webhook_secret } });
    console.log(`✨ [JOB ${job_id}] FINALIZADO COM SUCESSO!`);

  } catch (e) {
    console.error(`💥 [JOB ${job_id}] ERRO CRÍTICO:`, e.message);
    await axios.post(webhook_url, { job_id, status: "failed", error: e.message }, { headers: { "x-webhook-secret": webhook_secret } });
  } finally {
    setTimeout(() => {
      if (fs.existsSync(workDir)) fs.rmSync(workDir, { recursive: true, force: true });
    }, 15 * 60 * 1000);
  }
}, { connection, concurrency: 1 });

app.listen(PORT, () => console.log(`🚀 Worker de Vídeo Ativo na porta ${PORT}`));
