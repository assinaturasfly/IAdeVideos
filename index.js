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

async function executeWithRetry(action, maxTentativas = 3) {
  for (let tentativa = 1; tentativa <= maxTentativas; tentativa++) {
    try {
      return await action();
    } catch (error) {
      if (tentativa === maxTentativas) throw error;
      const delay = 1000 * Math.pow(2, tentativa);
      console.warn(`⚠️ Falha na tentativa ${tentativa} (${error.message}). Retentando em ${delay}ms...`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

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
  console.log("📥 DADOS RECEBIDOS NA PORTA DE ENTRADA:", JSON.stringify(req.body, null, 2));

  const { job_id, broll_urls, audio_url } = req.body;
  if (!job_id || !audio_url) return res.status(400).json({ error: "Dados ausentes" });

  const job = await videoQueue.add("render-job", req.body, { 
    removeOnComplete: true, 
    removeOnFail: { age: 3600 }, 
    attempts: 2,
    backoff: { type: "fixed", delay: 5000 }
  });
  console.log(`🚀 [FILA] Novo vídeo recebido! ID: ${job_id}`);
  res.json({ status: "queued", job_id });
});

const worker = new Worker("video-processing", async (job) => {
  const { job_id, audio_url, webhook_url, webhook_secret, logo_url, overlay_image_url, tipo_video } = job.data;
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

    const isEstatico = tipo_video === 'reels_estatico';
    const isDestino = tipo_video === 'destino' || job.data.card_mode === 'destino';
    const videoHeight = isDestino ? Math.round(height * 0.65) : height;
    
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

    if (!isEstatico) {
      if (subtitle_url) {
        await downloadToFile(subtitle_url, srtPath);
        activeSubtitlePath = srtPath;
      } else if (subtitle_text) {
        fs.writeFileSync(srtPath, subtitle_text);
        activeSubtitlePath = srtPath;
      }
    }

    const finalArgs = ["-stream_loop", "-1", "-f", "concat", "-safe", "0", "-i", playlistPath];
    
    // A MÁGICA ESTÁ AQUI: Se for estático e o frontend mandar o 'silence.mp3',
    // nós usamos o gerador de silêncio perfeito do FFmpeg em vez de tentar ler o MP3 quebrado!
    const isSilenceMp3 = audio_url && audio_url.includes("silence.mp3");

    if (isEstatico) {
      if (isSilenceMp3) {
        finalArgs.push("-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100");
      } else {
        finalArgs.push("-stream_loop", "-1", "-i", audioPath); // Se for música normal, ele faz o loop normal.
      }
    } else {
      finalArgs.push("-i", audioPath);
    }

    const overlayImg = overlay_image_url || logo_url;
    if (overlayImg) {
      await downloadToFile(overlayImg, path.join(workDir, "overlay.png"));
      finalArgs.push("-i", path.join(workDir, "overlay.png"));
    }

    const totalVideoLength = duration > 0 ? duration : normalizedClips.length * 5;
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

    if (overlayImg) {
      if (isEstatico) {
        filterParts.push(`[2:v]scale=${width}:${height}[logo]`);
        filterParts.push(`[${currentV}][logo]overlay=0:0[v_final]`);
      } else if (isDestino) {
        filterParts.push(`[2:v]scale=${width}:-1[logo]`);
        filterParts.push(`[${currentV}][logo]overlay=0:H-h:enable='gte(t,${showLogoFrom})'[v_final]`);
      } else {
        filterParts.push(`[2:v]scale=350:-1[logo]`);
        filterParts.push(`[${currentV}][logo]overlay=(W-w)/2:40:enable='gte(t,${showLogoFrom})'[v_final]`);
      }
      currentV = "v_final";
    }

    let videoMap = "0:v:0";
    if (currentV !== "0:v:0") {
      finalArgs.push("-filter_complex", filterParts.join(';'));
      videoMap = `[${currentV}]`;
    }

    finalArgs.push("-map", videoMap, "-map", "1:a:0");

    // Limite de tempo (30 segundos para estático, corte no áudio para os outros)
    if (isEstatico) {
      finalArgs.push("-t", "30"); 
    } else {
      finalArgs.push("-shortest");
    }

    finalArgs.push("-c:v", "libx264", "-preset", "veryfast", "-crf", "16", "-pix_fmt", "yuv420p", outputPath);

    await runFfmpeg(finalArgs, "Renderização Final");

    console.log("🧘‍♂️ Dando um respiro de 10 segundos para o servidor recuperar a rede...");
    await new Promise(r => setTimeout(r, 10000));

    let finalVideoUrl = "";
    try {
      console.log(`🔍 [DRIVE] Buscando token no Supabase...`);
      const SUPABASE_URL = process.env.SUPABASE_URL;
      const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
      
      const { data: configData } = await executeWithRetry(() => 
        axios.get(`${SUPABASE_URL}/rest/v1/app_config?key=eq.google_drive_refresh_token&select=value`, {
          headers: { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}` }
        })
      );
      
      const refreshToken = configData[0]?.value || process.env.GOOGLE_REFRESH_TOKEN;
      if (!refreshToken) throw new Error("Token não encontrado no banco nem nas variáveis.");

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
        })
      );

      const oauth2Client = new google.auth.OAuth2();
      oauth2Client.setCredentials({ access_token: tokenRes.data.access_token });
      const drive = google.drive({ version: 'v3', auth: oauth2Client });

      const response = await executeWithRetry(() => drive.files.create({
        requestBody: { name: `video_${job_id}.mp4`, parents: [folderId] },
        media: { mimeType: 'video/mp4', body: fs.createReadStream(outputPath) },
        fields: 'id, webViewLink'
      }));

      await executeWithRetry(() => 
        axios.post(`https://www.googleapis.com/drive/v3/files/${response.data.id}/permissions`, 
        { role: 'reader', type: 'anyone' },
        { headers: { Authorization: `Bearer ${tokenRes.data.access_token}` } }
        )
      );

      finalVideoUrl = response.data.webViewLink;
      console.log(`✅ [DRIVE] Link permanente gerado: ${finalVideoUrl}`);

    } catch (err) {
      console.error("❌ Erro ao subir no Drive.", err.message);
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
}, { 
  connection, 
  concurrency: 1,
  lockDuration: 600000, 
  lockRenewTime: 120000 
});

// ── Proxy TripAdvisor Terra API (evita bloqueio de CORS no browser) ─────────
app.get("/api/tripadvisor", async (req, res) => {
  const query = String(req.query.query ?? "").trim();
  if (!query) return res.status(400).json({ error: "Parâmetro 'query' obrigatório" });

  const apiKey = process.env.TRIPADVISOR_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "TRIPADVISOR_API_KEY não configurada no servidor" });

  // Terra API — nova plataforma (substitui api.content.tripadvisor.com)
  const BASE = "https://terra.tripadvisor.com/api";
  const taHeaders = { accept: "application/json", "X-API-Key": apiKey };

  try {
    // Passo 1: localizar o hotel pelo nome
    const searchRes = await axios.get(`${BASE}/locations/search`, {
      params: { query, category: "HOTEL", locale: ["pt-BR"], size: 5 },
      headers: taHeaders,
    });
    const locations = searchRes.data?.data ?? [];
    if (!locations.length) return res.json({ data: [] });

    const locationId = locations[0].location?.id;
    if (!locationId) return res.json({ data: [] });

    const nearbyParams = { location_id: locationId, size: 5, unit: "KM", locale: ["pt-BR"] };

    // Passos 2-4 em paralelo: fotos + restaurantes próximos + atrações próximas
    const [photosRes, restaurantsRes, attractionsRes] = await Promise.all([
      axios.get(`${BASE}/locations/${locationId}/photos`, {
        params: { size: 50 },
        headers: taHeaders,
      }),
      axios.get(`${BASE}/locations/nearby`, {
        params: { ...nearbyParams, category: "RESTAURANT" },
        headers: taHeaders,
      }),
      axios.get(`${BASE}/locations/nearby`, {
        params: { ...nearbyParams, category: "ATTRACTION" },
        headers: taHeaders,
      }),
    ]);

    console.log("[TripAdvisor] Qtd fotos recebidas:", photosRes.data?.data?.length);
    console.log("[TripAdvisor] RAW Photo[0]:", JSON.stringify(photosRes.data?.data?.[0]));
    console.log("[TripAdvisor] Restaurantes próximos:", restaurantsRes.data?.data?.length);
    console.log("[TripAdvisor] Atrações próximas:", attractionsRes.data?.data?.length);

    const photos = (photosRes.data?.data ?? [])
      .map((item) => ({
        id:    String(item.id ?? ""),
        url:   item.photo?.original_size_url ?? "",
        thumb: item.photo?.original_size_url ?? "",
      }))
      .filter((p) => p.url);

    const parseNearby = (items) => (items ?? []).map((item) => {
      const names = item.location?.names ?? [];
      const name = names.find((n) => n.language === "pt-BR")?.value ?? names[0]?.value ?? "";
      const cats = item.location?.categories ?? [];
      const category = cats[0]?.name ?? "";
      const distance_km = item.distance_kilometers ?? null;
      return { name, category, distance_km };
    }).filter((r) => r.name);

    const restaurants = parseNearby(restaurantsRes.data?.data);
    const attractions = parseNearby(attractionsRes.data?.data);

    console.log("[TripAdvisor] Restaurantes parseados:", JSON.stringify(restaurants));
    console.log("[TripAdvisor] Atrações parseadas:", JSON.stringify(attractions));

    res.json({ data: photos, restaurants, attractions });
  } catch (err) {
    if (err.response) {
      console.error("[TripAdvisor proxy erro real]:", JSON.stringify(err.response.data));
    } else {
      console.error("[TripAdvisor proxy erro mensagem]:", err.message);
    }
    res.status(502).json({ error: `Erro ao consultar TripAdvisor: ${err.message}` });
  }
});

app.get("/", (req, res) => res.send("🚀 Worker de Vídeo Ativo"));
app.listen(PORT, () => console.log(`🚀 Worker de Vídeo Ativo na porta ${PORT}`));
