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
//videoQueue.obliterate({ force: true }).catch(() => {});

async function executeWithRetry(action, maxTentativas = 3, logContext = "Sistema") {
  for (let tentativa = 1; tentativa <= maxTentativas; tentativa++) {
    try {
      return await action();
    } catch (error) {
      if (tentativa === maxTentativas) throw error;
      const delay = 1000 * Math.pow(2, tentativa);
      console.warn(`[RETRY][${logContext}] ⚠️ Falha na tentativa ${tentativa} (${error.message}). Retentando em ${delay}ms...`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

// O jobId foi adicionado para podermos rastrear exatamente de qual vídeo é o processo
function runFfmpeg(args, stepName = "Processando", jobId = "SYS") {
  return new Promise((resolve, reject) => {
    const finalArgs = ["-hide_banner", "-loglevel", "error", "-nostdin", "-y", ...args];
    console.log(`[FFMPEG][${jobId}] ⏳ Iniciando: ${stepName}...`);
    
    const p = spawn("ffmpeg", finalArgs);
    let errorLog = ""; // Acumula erros para não sujar a log com chunks
    
    p.stderr.on("data", (data) => errorLog += data.toString());
    
    p.on("close", (code) => {
      if (code === 0) {
        console.log(`[FFMPEG][${jobId}] ✅ Concluído: ${stepName}`);
        return resolve();
      }
      console.error(`[FFMPEG][${jobId}] ❌ Falha no passo '${stepName}' (Código ${code}). Detalhe: ${errorLog.trim()}`);
      reject(new Error(`${stepName} falhou com código ${code}`));
    });
  });
}

async function downloadToFile(url, filePath) {
  try {
    const r = await axios({ url, responseType: "stream", timeout: 60000 });
    await new Promise((resolve, reject) => {
      const w = fs.createWriteStream(filePath);
      r.data.pipe(w);
      
      w.on("finish", resolve);
      w.on("error", reject);
      r.data.on("error", reject);
      
      r.data.setTimeout(30000, () => {
        r.data.destroy();
        reject(new Error("Timeout a receber os dados da rede"));
      });
    });
  } catch (error) {
    throw new Error(`Falha no download da rede: ${error.message}`);
  }
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
  
  if (!job_id) {
    console.warn("[API] ⚠️ Tentativa de render bloqueada: job_id ausente no payload.");
    return res.status(400).json({ error: "job_id ausente" });
  }

  // Log compacta numa única linha
  console.log(`[API][${job_id}] 📥 Requisição recebida com ${broll_urls?.length || 0} clips.`);

  const job = await videoQueue.add("render-job", req.body, { 
    removeOnComplete: true, 
    removeOnFail: { age: 3600 }, 
    attempts: 2,
    backoff: { type: "fixed", delay: 5000 }
  });
  
  console.log(`[FILA][${job_id}] 🚀 Job inserido na fila do BullMQ com sucesso.`);
  res.json({ status: "queued", job_id });
});

const worker = new Worker("video-processing", async (job) => {
  const { job_id, audio_url, webhook_url, webhook_secret, logo_url, overlay_image_url, tipo_video, watermark_url } = job.data;
  
  console.log(`[WORKER][${job_id}] ⚙️ Processamento iniciado (Tipo: ${tipo_video || 'padrão'}).`);

  const workDir = path.join("/tmp", "video-worker", job_id);
  const output_config = job.data.output_config || {};
  const width = output_config.width || 720;
  const height = output_config.height || 1280;

  try {
    if (fs.existsSync(workDir)) fs.rmSync(workDir, { recursive: true, force: true });
    fs.mkdirSync(workDir, { recursive: true });
    
    const audioPath = path.join(workDir, "audio.mp3");
    const outputPath = path.join(workDir, "output.mp4");
    const srtPath = path.join(workDir, "subs.srt");

    const isSilenceMp3 = !audio_url || audio_url === "silence" || (typeof audio_url === 'string' && audio_url.includes("silence.mp3"));
    let duration = 0;
    
    if (!isSilenceMp3) {
      console.log(`[WORKER][${job_id}] 📦 Descarregando ficheiro de áudio...`);
      await downloadToFile(audio_url, audioPath);
      duration = await getMediaDuration(audioPath);
      console.log(`[WORKER][${job_id}] 🎵 Duração do áudio: ${duration}s`);
    } else {
      console.log(`[WORKER][${job_id}] 🔇 Modo sem áudio. Pulando download.`);
    }

    const broll_urls = job.data.broll_urls || [];
    const downloadedClips = [];

    console.log(`[WORKER][${job_id}] 📦 Descarregando ${broll_urls.length} clip(s) de vídeo...`);
    for (let i = 0; i < broll_urls.length; i++) {
      const p = path.join(workDir, `raw_${i}.mp4`);
      await downloadToFile(broll_urls[i], p);
      downloadedClips.push(p);
    }

    const isEstatico = tipo_video === 'reels_estatico';
    const isDestino = tipo_video === 'destino' || job.data.card_mode === 'destino';
    const videoHeight = isDestino ? Math.round(height * 0.65) : height;
    
    const vf = `fps=30,scale=${width}:${videoHeight}:force_original_aspect_ratio=increase,crop=${width}:${videoHeight},eq=contrast=1.05:saturation=1.3,unsharp=5:5:0.8:5:5:0.0,format=yuv420p`;
    const normalizedClips = [];

    for (let i = 0; i < downloadedClips.length; i++) {
      const normPath = path.join(workDir, `slice_${i}.mp4`);
      let sliceArgs = ["-ss", "0"];

      if (downloadedClips.length > 1) {
        sliceArgs.push("-t", "5");
      } else {
        if (duration > 0) sliceArgs.push("-t", duration.toString());
      }

      sliceArgs.push("-i", downloadedClips[i], "-vf", vf, "-c:v", "libx264", "-preset", "veryfast", "-crf", "16", "-threads", "2", "-an", normPath);
      
      await runFfmpeg(sliceArgs, `Normalização Clipe ${i+1}`, job_id);
      normalizedClips.push(normPath);
    }

    const playlistPath = path.join(workDir, "playlist.txt");
    fs.writeFileSync(playlistPath, normalizedClips.map(p => `file '${p}'`).join("\n"));

    let activeSubtitlePath = null;
    const subtitle_url = job.data.subtitle_url || output_config.subtitle_url;
    const subtitle_text = job.data.subtitle_text || output_config.subtitle_text;

    if (!isEstatico) {
      if (subtitle_url) {
        console.log(`[WORKER][${job_id}] 📝 Descarregando legendas...`);
        await downloadToFile(subtitle_url, srtPath);
        activeSubtitlePath = srtPath;
      } else if (subtitle_text) {
        fs.writeFileSync(srtPath, subtitle_text);
        activeSubtitlePath = srtPath;
      }
    }

    const finalArgs = ["-stream_loop", "-1", "-f", "concat", "-safe", "0", "-i", playlistPath];
    
    if (isEstatico) {
      if (isSilenceMp3) {
        finalArgs.push("-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100");
      } else {
        finalArgs.push("-stream_loop", "-1", "-i", audioPath);
      }
    } else {
      if (isSilenceMp3) {
        finalArgs.push("-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100");
      } else {
        finalArgs.push("-i", audioPath);
      }
    }

    let inputIndex = 2;
    
    const overlayImg = overlay_image_url || logo_url;
    let overlayIndex = -1;
    if (overlayImg) {
      await downloadToFile(overlayImg, path.join(workDir, "overlay.png"));
      finalArgs.push("-i", path.join(workDir, "overlay.png"));
      overlayIndex = inputIndex++;
    }

    let watermarkIndex = -1;
    if (watermark_url) {
      console.log(`[WORKER][${job_id}] 📦 Descarregando Marca D'água...`);
      await downloadToFile(watermark_url, path.join(workDir, "watermark.png"));
      finalArgs.push("-i", path.join(workDir, "watermark.png"));
      watermarkIndex = inputIndex++;
    }

    let totalVideoLength = duration;
    if (totalVideoLength === 0) {
      if (downloadedClips.length === 1) {
        totalVideoLength = await getMediaDuration(downloadedClips[0]);
      } else {
        totalVideoLength = normalizedClips.length * 5;
      }
    }
    
    const isAlwaysOn = job.data.logo_always_on === true;
    const showLogoFrom = isAlwaysOn ? 0 : Math.max(0, totalVideoLength - 3);

    let filterParts = [];
    let currentV = "0:v:0";

    if (activeSubtitlePath && !isEstatico) {
      const dynamicMargin = isDestino ? 70 : 90;
      const dynamicFontSize = isDestino ? 12 : 8; 
      const forceStyle = `Alignment=2,MarginV=${dynamicMargin},Fontname=Montserrat,Bold=1,Fontsize=${dynamicFontSize},BorderStyle=1,Outline=0.4,OutlineColour=&H00000000`;
      const escapedSrtPath = activeSubtitlePath.replace(/\\/g, "/").replace(/:/g, "\\:").replace(/'/g, "'\\\\\\''");
      filterParts.push(`[${currentV}]subtitles='${escapedSrtPath}':force_style='${forceStyle}'[v_subbed]`);
      currentV = "v_subbed";
    }

    if (isDestino && !isEstatico) {
      filterParts.push(`[${currentV}]pad=${width}:${height}:0:0:black[v_padded]`);
      currentV = "v_padded";
    }

    if (overlayIndex !== -1) {
      if (isEstatico) {
        filterParts.push(`[${overlayIndex}:v]scale=${width}:${height}[logo]`);
        filterParts.push(`[${currentV}][logo]overlay=0:0[v_overlay]`);
      } else if (isDestino) {
        filterParts.push(`[${overlayIndex}:v]scale=${width}:-1[logo]`);
        filterParts.push(`[${currentV}][logo]overlay=0:H-h:enable='gte(t,${showLogoFrom})'[v_overlay]`);
      } else {
        filterParts.push(`[${overlayIndex}:v]scale=350:-1[logo]`);
        filterParts.push(`[${currentV}][logo]overlay=(W-w)/2:40:enable='gte(t,${showLogoFrom})'[v_overlay]`);
      }
      currentV = "v_overlay";
    }

    if (watermarkIndex !== -1) {
      filterParts.push(`[${watermarkIndex}:v]scale=${width}:${height}[wm]`);
      filterParts.push(`[${currentV}][wm]overlay=0:0[v_watermark]`);
      currentV = "v_watermark";
    }

    let videoMap = "0:v:0";
    if (currentV !== "0:v:0") {
      finalArgs.push("-filter_complex", filterParts.join(';'));
      videoMap = `[${currentV}]`;
    }

    finalArgs.push("-map", videoMap, "-map", "1:a:0");

    if (isEstatico) {
      finalArgs.push("-t", "30"); 
    } else {
      if (isSilenceMp3) {
        finalArgs.push("-t", totalVideoLength.toString());
      } else {
        finalArgs.push("-shortest");
      }
    }

    finalArgs.push("-c:v", "libx264", "-preset", "veryfast", "-crf", "16", "-pix_fmt", "yuv420p", "-threads", "2", outputPath);

    await runFfmpeg(finalArgs, "Renderização Final", job_id);

    console.log(`[WORKER][${job_id}] 🧘‍♂️ Pausa de 10s para estabilização de rede...`);
    await new Promise(r => setTimeout(r, 10000));

    let finalVideoUrl = "";
    try {
      console.log(`[WORKER][${job_id}] 🔍 Buscando credenciais do Google Drive...`);
      const SUPABASE_URL = process.env.SUPABASE_URL;
      const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
      
      const { data: configData } = await executeWithRetry(() => 
        axios.get(`${SUPABASE_URL}/rest/v1/app_config?key=eq.google_drive_refresh_token&select=value`, {
          headers: { 
            'apikey': SUPABASE_KEY, 
            'Authorization': `Bearer ${SUPABASE_KEY}`,
            'Accept-Profile': 'viral'
          }
        }), 3, job_id
      );
      
      const refreshToken = configData[0]?.value || process.env.GOOGLE_REFRESH_TOKEN;
      if (!refreshToken) throw new Error("Token não encontrado.");

      const clientId = process.env.GOOGLE_CLIENT_ID;
      const clientSecret = process.env.GOOGLE_CLIENT_SECRET;
      const folderId = process.env.GOOGLE_DRIVE_FOLDER_ID;

      const tokenRes = await executeWithRetry(() => 
        axios.post("https://oauth2.googleapis.com/token", new URLSearchParams({
          client_id: clientId,
          client_secret: clientSecret,
          refresh_token: refreshToken,
          grant_type: "refresh_token"
        }).toString(), {
          headers: { "Content-Type": "application/x-www-form-urlencoded" }
        }), 3, job_id
      );

      const oauth2Client = new google.auth.OAuth2();
      oauth2Client.setCredentials({ access_token: tokenRes.data.access_token });
      const drive = google.drive({ version: 'v3', auth: oauth2Client });

      console.log(`[WORKER][${job_id}] ☁️ A enviar vídeo para o Google Drive...`);
      const response = await executeWithRetry(() => drive.files.create({
        requestBody: { name: `video_${job_id}.mp4`, parents: [folderId] },
        media: { mimeType: 'video/mp4', body: fs.createReadStream(outputPath) },
        fields: 'id, webViewLink'
      }), 3, job_id);

      await executeWithRetry(() => 
        axios.post(`https://www.googleapis.com/drive/v3/files/${response.data.id}/permissions`, 
        { role: 'reader', type: 'anyone' },
        { headers: { Authorization: `Bearer ${tokenRes.data.access_token}` } }
        ), 3, job_id
      );

      finalVideoUrl = response.data.webViewLink;
      console.log(`[WORKER][${job_id}] ✅ Upload concluído: ${finalVideoUrl}`);

    } catch (err) {
      console.error(`[WORKER][${job_id}] ⚠️ Falha no Drive, a usar URL local. Motivo: ${err.message}`);
      const serverUrl = process.env.RENDER_EXTERNAL_URL || `https://${process.env.RENDER_HOSTNAME}`;
      finalVideoUrl = `${serverUrl}/videos/${job_id}/output.mp4`;
    }

    await axios.post(webhook_url, { job_id, status: "completed", video_url: finalVideoUrl }, { headers: { "x-webhook-secret": webhook_secret } });
    console.log(`[WORKER][${job_id}] ✨ JOB FINALIZADO COM SUCESSO!`);

  } catch (e) {
    console.error(`[WORKER][${job_id}] 💥 ERRO CRÍTICO NO JOB: ${e.message}`);
    await axios.post(webhook_url, { job_id, status: "failed", error: e.message }, { headers: { "x-webhook-secret": webhook_secret } });
  } finally {
    setTimeout(() => {
      if (fs.existsSync(workDir)) fs.rmSync(workDir, { recursive: true, force: true });
    }, 15 * 60 * 1000);
  }
}, { 
  connection, 
  concurrency: 1,
  lockDuration: 600000, 
  lockRenewTime: 120000 
});

// Proxy TripAdvisor Terra API
app.get("/api/tripadvisor", async (req, res) => {
  const query = String(req.query.query ?? "").trim();
  if (!query) return res.status(400).json({ error: "Parâmetro 'query' obrigatório" });

  const apiKey = process.env.TRIPADVISOR_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "TRIPADVISOR_API_KEY ausente" });

  const BASE = "https://terra.tripadvisor.com/api";
  const taHeaders = { accept: "application/json", "X-API-Key": apiKey };

  try {
    console.log(`[PROXY][TripAdvisor] Nova busca por: ${query}`);
    const searchRes = await axios.get(`${BASE}/locations/search`, {
      params: { query, category: "HOTEL", locale: "pt-BR", size: 5 },
      headers: taHeaders,
    });
    // O resto mantém-se igual (omitido processamento longo para manter limpo, mas mantendo a estrutura que já estava correta)
    const locations = searchRes.data?.data ?? [];
    if (!locations.length) return res.json({ data: [], restaurants: [], attractions: [] });

    const locationId = locations[0].location?.id;
    if (!locationId) return res.json({ data: [], restaurants: [], attractions: [] });

    const nearbyParams = { location_id: locationId, radius: 8, unit: "KM", size: 20, locale: "pt-BR" };

    const [photosRes, restaurantsRes, attractionsRes] = await Promise.all([
      axios.get(`${BASE}/locations/${locationId}/photos`, { params: { size: 50 }, headers: taHeaders }),
      axios.get(`${BASE}/locations/nearby`, { params: { ...nearbyParams, category: "RESTAURANT" }, headers: taHeaders }),
      axios.get(`${BASE}/locations/nearby`, { params: { ...nearbyParams, category: "ATTRACTION" }, headers: taHeaders })
    ]);

    const photos = (photosRes.data?.data ?? []).map((item) => ({
      id: String(item.id ?? ""),
      url: item.photo?.original_size_url ?? "",
      thumb: item.photo?.original_size_url ?? "",
    })).filter((p) => p.url);

    const parseNearby = (items) => (items ?? []).map((item) => {
      const names = item.location?.names ?? [];
      const name = names.find((n) => n.language === "pt-BR")?.value ?? names[0]?.value ?? "";
      const cats = item.location?.categories ?? [];
      const category = cats[0]?.name ?? "";
      const distance_km = item.distance_kilometers ?? null;
      return { name, category, distance_km };
    }).filter((r) => r.name);

    res.json({ data: photos, restaurants: parseNearby(restaurantsRes.data?.data), attractions: parseNearby(attractionsRes.data?.data) });
  } catch (err) {
    console.error(`[PROXY][TripAdvisor] ❌ Erro: ${err.message}`);
    res.status(502).json({ error: `Erro na API: ${err.message}` });
  }
});

// Proxy de imagem
app.get("/api/proxy-image", async (req, res) => {
  const url = String(req.query.url ?? "").trim();
  if (!url) return res.status(400).json({ error: "Parâmetro 'url' obrigatório" });
  try {
    const response = await axios.get(url, {
      responseType: "arraybuffer",
      timeout: 15000,
      headers: { "User-Agent": "Mozilla/5.0 (compatible; ViralFlux/1.0)" },
    });
    res.set({
      "Access-Control-Allow-Origin": "*",
      "Content-Type": response.headers["content-type"] || "image/jpeg",
      "Cache-Control": "public, max-age=86400"
    });
    res.send(Buffer.from(response.data));
  } catch (err) {
    console.error(`[PROXY][Imagem] ❌ Erro ao buscar ${url}: ${err.message}`);
    res.status(502).json({ error: `Falha ao buscar imagem: ${err.message}` });
  }
});

worker.on("active", (job) => {
  console.log(`[BULLMQ][${job.id}] 🟢 Estado: ATIVO na Fila`);
});

worker.on("failed", (job, err) => {
  console.error(`[BULLMQ][${job.id}] ❌ Estado: FALHOU (${err.message})`);
});

worker.on("error", (err) => {
  console.error(`[BULLMQ][SYS] 🚨 Erro de Conexão no Worker: ${err.message}`);
});

app.get("/", (req, res) => res.send("🚀 Worker de Vídeo Ativo"));
app.listen(PORT, () => console.log(`[SYSTEM] 🚀 Worker de Vídeo Ativo na porta ${PORT}`));
