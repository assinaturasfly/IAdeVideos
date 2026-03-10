const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

const app = express();
app.use(express.json({ limit: "50mb" }));

// ---------- CONFIG (Render Env Vars) ----------
const PORT = process.env.PORT || 3000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const FINAL_BUCKET = process.env.FINAL_BUCKET || "final-videos";

const DEFAULT_WIDTH = parseInt(process.env.DEFAULT_WIDTH || "720", 10); // Ajustado para Shorts
const DEFAULT_HEIGHT = parseInt(process.env.DEFAULT_HEIGHT || "1280", 10);
const DEFAULT_FPS = parseInt(process.env.DEFAULT_FPS || "30", 10); // Melhor fluidez
// --------------------------------------------

app.get("/", (req, res) => res.send("OK"));
app.get("/health", (req, res) => res.json({ ok: true }));

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
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

async function uploadMp4ToSupabase(localFilePath, objectPath) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error("SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY não configurados");
  }

  const uploadUrl = `${SUPABASE_URL}/storage/v1/object/${FINAL_BUCKET}/${objectPath}`;
  const stat = fs.statSync(localFilePath);
  const stream = fs.createReadStream(localFilePath);

  await axios.put(uploadUrl, stream, {
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "video/mp4",
      "Content-Length": stat.size,
      "x-upsert": "true",
    },
    maxBodyLength: Infinity,
    maxContentLength: Infinity,
    timeout: 300000,
  });

  return `${SUPABASE_URL}/storage/v1/object/public/${FINAL_BUCKET}/${objectPath}`;
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
    console.log("[ffmpeg] cmd:", `ffmpeg ${args.join(" ")}`);
    const p = spawn("ffmpeg", args, { stdio: ["ignore", "pipe", "pipe"] });

    p.stdout.on("data", (d) => console.log("[ffmpeg][out]", d.toString().trim()));
    p.stderr.on("data", (d) => console.log("[ffmpeg][err]", d.toString().trim()));

    p.on("error", reject);
    p.on("close", (code) => {
      if (code === 0) return resolve();
      reject(new Error(`ffmpeg saiu com code ${code}`));
    });
  });
}

app.post("/render", async (req, res) => {
  const body = req.body || {};

  const job_id = body.job_id;
  const webhook_url = body.webhook_url;
  const webhook_secret = body.webhook_secret;
  const audio_url = body.audio_url;
  const output_config = body.output_config || {};
  
  // 🔥 MUDANÇA CRUCIAL: Agora o worker escuta a timeline enviada pelo app
  const timeline = body.timeline || body.broll_timeline || output_config.timeline || [];
  const broll_urls = body.broll_urls || []; // Ainda usado como fallback para download
  
  const subtitle_url = body.subtitle_url || output_config.subtitle_url;
  const subtitle_text = body.subtitle_text || output_config.subtitle_text;

  const width = Number(output_config.width || DEFAULT_WIDTH);
  const height = Number(output_config.height || DEFAULT_HEIGHT);
  const fps = Number(output_config.fps || DEFAULT_FPS);

  if (!job_id || !webhook_url || !webhook_secret || !audio_url || broll_urls.length === 0) {
    return res.status(400).json({ error: "Campos obrigatórios ausentes" });
  }

  res.json({ status: "accepted", job_id });

  (async () => {
    const workDir = path.join("/tmp", "video-worker", job_id);
    ensureDir(workDir);

    const audioPath = path.join(workDir, "audio.mp3");
    const outputPath = path.join(workDir, "output.mp4");
    const srtPath = path.join(workDir, "subs.srt");
    
    let activeSubtitlePath = null;
    const downloadedClipsMap = {}; // Guarda o caminho local para cada URL baixada

    try {
      console.log(`[job ${job_id}] --------------------------------------------------`);
      console.log(`[job ${job_id}] INICIANDO: Processando timeline com ${timeline.length} cortes.`);
      console.log(`[job ${job_id}] --------------------------------------------------`);

      console.log(`[job ${job_id}] baixando áudio...`);
      await downloadToFile(audio_url, audioPath);

      // 1. Baixar apenas os vídeos que estão na timeline (ou todos se não houver timeline)
      const urlsToDownload = new Set();
      if (timeline && timeline.length > 0) {
        timeline.forEach(clip => urlsToDownload.add(clip.url || clip.src));
      } else {
        broll_urls.forEach(url => urlsToDownload.add(url));
      }

      console.log(`[job ${job_id}] Baixando ${urlsToDownload.size} vídeos originais...`);
      
      let index = 0;
      for (const url of urlsToDownload) {
        const cPath = path.join(workDir, `raw_${index}.mp4`);
        await downloadToFile(url, cPath);
        downloadedClipsMap[url] = cPath; // Associa a URL ao arquivo local
        index++;
      }

      const normalizedClips = [];
      const vf = `fps=${fps},scale=${width}:${height}:force_original_aspect_ratio=increase,crop=${width}:${height},format=yuv420p`;

      // 2. Aplicar os cortes EXATOS da timeline (Inteligência Artificial)
      if (timeline && timeline.length > 0) {
        console.log(`[job ${job_id}] Recortando clipes com base na timeline (Sem loop fixo)...`);
        
        for (let i = 0; i < timeline.length; i++) {
          const clip = timeline[i];
          const rawPath = downloadedClipsMap[clip.url || clip.src];
          
          if (!rawPath) continue;

          const normPath = path.join(workDir, `slice_${i}.mp4`);
          const startTime = clip.start || clip.startTime || clip.ss || 0;
          // Se o payload informar a duração exata do corte, use; se não, tente deduzir
          const duration = clip.duration || (clip.end ? clip.end - startTime : 3); 
          
          console.log(`[job ${job_id}] Cortando slice ${i}: início ${startTime}s, duração ${duration}s`);
          
          // O Comando FFmpeg que respeita o nosso "offset dinâmico"
          await runFfmpeg([
            "-y", "-hide_banner", "-loglevel", "error",
            "-ss", String(startTime), // Pula até o tempo correto (não é mais sempre zero)
            "-t", String(duration),   // Corta a quantidade de segundos que pedimos (ex: 3s)
            "-i", rawPath,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
            "-an", // Remove o áudio original do TikTok
            normPath
          ]);
          normalizedClips.push(normPath);
        }
      } else {
        // Fallback: Se o Lovable por algum motivo não mandar timeline, ele corta os 15 primeiros segs
        console.log(`[job ${job_id}] AVISO: Nenhuma timeline enviada. Processando clipes crus com corte base.`);
        let i = 0;
        for (const url in downloadedClipsMap) {
           const normPath = path.join(workDir, `slice_${i}.mp4`);
           await runFfmpeg([
            "-y", "-hide_banner", "-loglevel", "error",
            "-t", "15", // Pega mais tempo para evitar buracos
            "-i", downloadedClipsMap[url],
            "-vf", vf,
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
            "-an", 
            normPath
          ]);
          normalizedClips.push(normPath);
          i++;
        }
      }

      // 3. Montar a Playlist
      // Sem loops malucos! Ele vai simplesmente juntar as fatias ordenadas na timeline
      const playlistPath = path.join(workDir, "playlist.txt");
      let playlistContent = "";
      
      for (const clip of normalizedClips) {
          playlistContent += `file '${clip}'\n`;
      }
      fs.writeFileSync(playlistPath, playlistContent);

      if (subtitle_url) {
        console.log(`[job ${job_id}] baixando arquivo de legenda...`);
        await downloadToFile(subtitle_url, srtPath);
        activeSubtitlePath = srtPath;
      } else if (subtitle_text) {
        console.log(`[job ${job_id}] salvando texto de legenda...`);
        fs.writeFileSync(srtPath, subtitle_text);
        activeSubtitlePath = srtPath;
      }

      console.log(`[job ${job_id}] iniciando montagem final do vídeo...`);
      
      const finalArgs = [
        "-y", "-hide_banner", "-loglevel", "info",
        "-f", "concat", "-safe", "0", "-i", playlistPath, 
        "-i", audioPath 
      ];

      // Legenda intocada conforme o seu estilo!
     if (activeSubtitlePath) {
        // Ignoramos a variável do app e travamos a margem em 50 (Centro)
        let marginV = 50; 

        const forceStyle = `Alignment=2,MarginV=${marginV},Fontname=Montserrat,Bold=1,Fontsize=14,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=1,Outline=0.7,Shadow=0`;
        
        finalArgs.push("-vf", `subtitles=${activeSubtitlePath}:force_style='${forceStyle}'`);
        
        console.log(`[job ${job_id}] Legenda FIXADA na posição Centro (Margem=${marginV})`);
      }

      finalArgs.push(
        "-map", "0:v:0",
        "-map", "1:a:0",
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        "-shortest", // Garante que o vídeo acabe assim que a locução acabar! (trava do 60s)
        outputPath
      );

      await runFfmpeg(finalArgs);
      console.log(`[job ${job_id}] ffmpeg finalizou ✅`);

      console.log(`[job ${job_id}] upload Supabase...`);
      const objectPath = `final/${job_id}.mp4`;
      const video_url = await uploadMp4ToSupabase(outputPath, objectPath);
      
      await callWebhook(webhook_url, webhook_secret, { job_id, status: "completed", video_url });
      
    } catch (e) {
      console.log(`[job ${job_id}] FALHOU:`, e?.message || e);
      try {
        await callWebhook(webhook_url, webhook_secret, { job_id, status: "failed", error: e?.message || String(e) });
      } catch (err2) {
        console.log("[webhook] ERRO ao enviar failed");
      }
    }
  })();
});

app.listen(PORT, () => console.log("Worker rodando na porta", PORT));