const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

const app = express();
app.use(express.json({ limit: "50mb" }));

// 🟢 PASSO 1: Expor a pasta temporária publicamente
// Isso permite que a URL do vídeo gerado possa ser acessada pelo seu Webhook para download
app.use("/videos", express.static("/tmp/video-worker"));

// ---------- CONFIG (Render Env Vars) ----------
const PORT = process.env.PORT || 3000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const FINAL_BUCKET = process.env.FINAL_BUCKET || "final-videos";

const DEFAULT_WIDTH = parseInt(process.env.DEFAULT_WIDTH || "720", 10);
const DEFAULT_HEIGHT = parseInt(process.env.DEFAULT_HEIGHT || "1280", 10);
const DEFAULT_FPS = parseInt(process.env.DEFAULT_FPS || "30", 10);
// --------------------------------------------

app.get("/", (req, res) => res.send("OK"));
app.get("/health", (req, res) => res.json({ ok: true }));

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

// Função para descarregar ficheiros (áudio, vídeos crus, etc.)
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

// Função mantida para o caso de precisares no futuro
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

// Função que avisa a aplicação externa (Lovable) sobre o estado do job
async function callWebhook(webhook_url, webhook_secret, payload) {
  await axios.post(webhook_url, payload, {
    headers: {
      "Content-Type": "application/json",
      "x-webhook-secret": webhook_secret,
    },
    timeout: 30000,
  });
}

// Wrapper para executar os comandos do FFmpeg
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
  
  const timeline = body.timeline || body.broll_timeline || output_config.timeline || [];
  const broll_urls = body.broll_urls || [];
  
  const subtitle_url = body.subtitle_url || output_config.subtitle_url;
  const subtitle_text = body.subtitle_text || output_config.subtitle_text;

  const width = Number(output_config.width || DEFAULT_WIDTH);
  const height = Number(output_config.height || DEFAULT_HEIGHT);
  const fps = Number(output_config.fps || DEFAULT_FPS);

  if (!job_id || !webhook_url || !webhook_secret || !audio_url || broll_urls.length === 0) {
    return res.status(400).json({ error: "Campos obrigatórios ausentes" });
  }

  // Responde imediatamente com HTTP 200 para não prender a requisição original
  res.json({ status: "accepted", job_id });

  (async () => {
    const workDir = path.join("/tmp", "video-worker", job_id);
    ensureDir(workDir);

    const audioPath = path.join(workDir, "audio.mp3");
    const outputPath = path.join(workDir, "output.mp4");
    const srtPath = path.join(workDir, "subs.srt");
    
    let activeSubtitlePath = null;
    const downloadedClipsMap = {};

    try {
      console.log(`[job ${job_id}] --------------------------------------------------`);
      console.log(`[job ${job_id}] INICIANDO: Processando timeline com ${timeline.length} cortes.`);
      console.log(`[job ${job_id}] --------------------------------------------------`);

      console.log(`[job ${job_id}] baixando áudio...`);
      await downloadToFile(audio_url, audioPath);

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
        downloadedClipsMap[url] = cPath;
        index++;
      }

      const normalizedClips = [];
      const vf = `fps=${fps},scale=${width}:${height}:force_original_aspect_ratio=increase,crop=${width}:${height},format=yuv420p`;

      if (timeline && timeline.length > 0) {
        console.log(`[job ${job_id}] Recortando clipes com base na timeline (Sem loop fixo)...`);
        
        for (let i = 0; i < timeline.length; i++) {
          const clip = timeline[i];
          const rawPath = downloadedClipsMap[clip.url || clip.src];
          
          if (!rawPath) continue;

          const normPath = path.join(workDir, `slice_${i}.mp4`);
          const startTime = clip.start || clip.startTime || clip.ss || 0;
          const duration = 5; 
          
          console.log(`[job ${job_id}] Cortando slice ${i}: início ${startTime}s, duração ${duration}s`);
          
          await runFfmpeg([
            "-y", "-hide_banner", "-loglevel", "error",
            "-ss", String(startTime),
            "-t", String(duration),  
            "-i", rawPath,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
            "-an", 
            normPath
          ]);
          normalizedClips.push(normPath);
        }
      } else {
        console.log(`[job ${job_id}] AVISO: Nenhuma timeline enviada. Processando clipes crus com corte base.`);
        let i = 0;
        for (const url in downloadedClipsMap) {
           const normPath = path.join(workDir, `slice_${i}.mp4`);
           await runFfmpeg([
            "-y", "-hide_banner", "-loglevel", "error",
            "-t", "5", 
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

      if (activeSubtitlePath) {
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
        "-shortest", 
        outputPath
      );

      await runFfmpeg(finalArgs);
      console.log(`[job ${job_id}] ffmpeg finalizou ✅`);

      // 🟢 PASSO 2 E 3: Criar a URL temporária e remover o upload do Supabase
      console.log(`[job ${job_id}] Gerando URL temporária para o webhook...`);
      
      // Pegamos a URL base do Render (ex: https://meu-worker.onrender.com)
      const serverUrl = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
      
      // Montamos a URL pública para o ficheiro criado
      const video_url = `${serverUrl}/videos/${job_id}/output.mp4`;
      
      console.log(`[job ${job_id}] Enviando para o webhook: ${video_url}`);
      
      // 🟢 NOVA LÓGICA DE PROTEÇÃO: Envolvemos a chamada de sucesso num bloco try/catch próprio
      try {
        await callWebhook(webhook_url, webhook_secret, { job_id, status: "completed", video_url });
        console.log(`[job ${job_id}] ✅ Webhook de sucesso enviado e recebido pela Lovable!`);
      } catch (webhookError) {
        // Se a Lovable der erro 500, o código cai aqui, mas NÃO apaga o vídeo!
        console.log(`[job ${job_id}] ⚠️ AVISO: O vídeo foi criado com sucesso, mas a Lovable recusou o Webhook.`);
        // Esta linha vai imprimir o motivo exato que a Lovable devolveu (muito útil para diagnóstico)
        console.log(`[job ${job_id}] ⚠️ Detalhes do erro da Lovable:`, webhookError?.response?.data || webhookError.message);
      }

      // 🟢 LIMPEZA APÓS SUCESSO (Aguarda 15 minutos e apaga os ficheiros pesados)
      setTimeout(() => {
        console.log(`[job ${job_id}] 🧹 Limpando arquivos temporários do disco após 15 minutos...`);
        try {
          fs.rmSync(workDir, { recursive: true, force: true });
          console.log(`[job ${job_id}] 🗑️ Pasta apagada com sucesso! Espaço libertado.`);
        } catch (cleanupErr) {
          console.log(`[job ${job_id}] ⚠️ Erro ao limpar pasta:`, cleanupErr.message);
        }
      }, 15 * 60 * 1000); 
      
    } catch (e) {
      // ESTE BLOCO AGORA SÓ É ATIVADO SE HOUVER ERRO REAL NA GERAÇÃO DO VÍDEO (Ex: FFmpeg falhar)
      console.log(`[job ${job_id}] FALHOU A GERAÇÃO:`, e?.message || e);
      try {
        await callWebhook(webhook_url, webhook_secret, { job_id, status: "failed", error: e?.message || String(e) });
      } catch (err2) {
        console.log("[webhook] ERRO ao enviar aviso de falha");
      }

      // LIMPEZA IMEDIATA APENAS SE A GERAÇÃO DO VÍDEO DEU ERRO
      try {
        if (fs.existsSync(workDir)) {
          fs.rmSync(workDir, { recursive: true, force: true });
          console.log(`[job ${job_id}] 🗑️ Lixo de erro apagado.`);
        }
      } catch (cleanupErr) {
         // Ignorar erro silenciosamente para não travar o servidor
      }
    }
  })();
});

app.listen(PORT, () => console.log("Worker rodando na porta", PORT));


me envie o codigo inteiro
