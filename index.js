const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const { createClient } = require('@supabase/supabase-js'); // 🟢 Adicionado Supabase

const app = express();
app.use(express.json({ limit: "50mb" }));

// 🟢 PASSO 1: Expor a pasta temporária publicamente
app.use("/videos", express.static("/tmp/video-worker"));

// ---------- CONFIG (Render Env Vars) ----------
const PORT = process.env.PORT || 3000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const FINAL_BUCKET = process.env.FINAL_BUCKET || "final-videos";

const WEBHOOK_URL = process.env.WEBHOOK_URL; // 🟢 Necessário para fila
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;

const DEFAULT_WIDTH = parseInt(process.env.DEFAULT_WIDTH || "720", 10);
const DEFAULT_HEIGHT = parseInt(process.env.DEFAULT_HEIGHT || "1280", 10);
const DEFAULT_FPS = parseInt(process.env.DEFAULT_FPS || "30", 10);
// --------------------------------------------

// 🟢 Inicia o cliente do Supabase
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

app.get("/", (req, res) => res.send("Worker Fila Ativo"));
app.get("/health", (req, res) => res.json({ ok: true }));

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

// Função para descarregar ficheiros
async function downloadToFile(url, filePath) {
  // 🟢 Proteção contra URL vazia adicionada
  if (!url || !url.startsWith('http')) {
    throw new Error(`URL Inválida para download: ${url}`);
  }

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
  if (!webhook_url) return;
  try {
    await axios.post(webhook_url, payload, {
      headers: {
        "Content-Type": "application/json",
        "x-webhook-secret": webhook_secret,
      },
      timeout: 30000,
    });
  } catch (e) {
    console.log("[Webhook] Falha ao notificar a Lovable:", e.message);
  }
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

// ----------------------------------------------------------------------
// 🟢 NOVA FUNÇÃO PRINCIPAL: SISTEMA DE FILA
// (Substitui o app.post("/render") mas mantém toda a sua lógica de edição)
// ----------------------------------------------------------------------
async function processarFila() {
  let job_id = null;

  try {
    // 1. Busca o próximo vídeo na fila
    const { data: job, error: buscaError } = await supabase
      .from('videos')
      .select('*')
      .eq('status', 'na_fila')
      .order('created_at', { ascending: true })
      .limit(1)
      .single(); 

    // Se não achar nada, espera 5s e tenta de novo
    if (buscaError || !job) {
      setTimeout(processarFila, 5000); 
      return;
    }

    job_id = job.id; 
    console.log(`\n==================================================`);
    console.log(`[FILA] Iniciando Processamento do Vídeo: ${job_id}`);
    console.log(`==================================================`);

    // 🟢 Atualiza a UI: Gerando Narração
    await supabase.from('videos').update({ status: 'generating_audio' }).eq('id', job_id);

    // Mapeamento dos dados do seu banco de dados
    const audio_url = job.narration_audio_url; 
    const subtitle_url = job.subtitle_url; 
    const subtitle_text = job.subtitle_text;
    
    // O seu código esperava um array com url e startTime, o Supabase já devolve isso como jsonb
    const timeline = job.b_roll_video_urls || []; 
    const broll_urls = []; // Fallback, caso a timeline estivesse vazia na sua lógica original

    const width = DEFAULT_WIDTH;
    const height = DEFAULT_HEIGHT;
    const fps = DEFAULT_FPS;

    if (!audio_url || (timeline.length === 0 && broll_urls.length === 0)) {
       throw new Error("Áudio ou B-rolls ausentes no banco de dados.");
    }

    // ------------------------------------------------------------------
    // A PARTIR DAQUI: A SUA LÓGICA DE EDIÇÃO EXATA (Copiada do seu código original)
    // ------------------------------------------------------------------

    const workDir = path.join("/tmp", "video-worker", job_id);
    ensureDir(workDir);

    const audioPath = path.join(workDir, "audio.mp3");
    const outputPath = path.join(workDir, "output.mp4");
    const srtPath = path.join(workDir, "subs.srt");
    
    let activeSubtitlePath = null;
    const downloadedClipsMap = {};

    console.log(`[job ${job_id}] --------------------------------------------------`);
    console.log(`[job ${job_id}] INICIANDO: Processando timeline com ${timeline.length} cortes.`);
    console.log(`[job ${job_id}] --------------------------------------------------`);

    console.log(`[job ${job_id}] baixando áudio...`);
    await downloadToFile(audio_url, audioPath);

    // 🟢 Atualiza a UI: Buscando B-rolls
    await supabase.from('videos').update({ status: 'searching_broll' }).eq('id', job_id);

    const urlsToDownload = new Set();
    if (timeline && timeline.length > 0) {
      timeline.forEach(clip => {
         // Se o clip for um objeto (como salvo no novo frontend), extrai a URL
         const clipUrl = typeof clip === 'string' ? clip : (clip.url || clip.src);
         if(clipUrl) urlsToDownload.add(clipUrl);
      });
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
        const clipUrl = typeof clip === 'string' ? clip : (clip.url || clip.src);
        const rawPath = downloadedClipsMap[clipUrl];
        
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

    // 🟢 Atualiza a UI: Gerando Legendas
    await supabase.from('videos').update({ status: 'generating_subtitles' }).eq('id', job_id);

    if (subtitle_url) {
      console.log(`[job ${job_id}] baixando arquivo de legenda...`);
      await downloadToFile(subtitle_url, srtPath);
      activeSubtitlePath = srtPath;
    } else if (subtitle_text) {
      console.log(`[job ${job_id}] salvando texto de legenda...`);
      fs.writeFileSync(srtPath, subtitle_text);
      activeSubtitlePath = srtPath;
    }

    // 🟢 Atualiza a UI: Montando o Vídeo
    await supabase.from('videos').update({ status: 'rendering_video' }).eq('id', job_id);

    console.log(`[job ${job_id}] iniciando montagem final do vídeo...`);
    
    const finalArgs = [
      "-y", "-hide_banner", "-loglevel", "info",
      "-f", "concat", "-safe", "0", "-i", playlistPath, 
      "-i", audioPath 
    ];

    if (activeSubtitlePath) {
      let marginV = 90; 
      const forceStyle = `Alignment=2,MarginV=${marginV},Fontname=Montserrat,Bold=1,Fontsize=8,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=1,Outline=0.4.,Shadow=0`;
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

    // 🟢 PASSO 2 E 3: Criar a URL temporária
    console.log(`[job ${job_id}] Gerando URL temporária para o webhook...`);
    
    const serverUrl = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
    const video_url = `${serverUrl}/videos/${job_id}/output.mp4`;
    
    console.log(`[job ${job_id}] Enviando para o webhook: ${video_url}`);
    
    // 🟢 Atualiza o status Final no banco de dados para concluir a UI
    await supabase.from('videos').update({ 
        status: 'in_review', 
        final_video_url: video_url 
    }).eq('id', job_id);

    try {
      await callWebhook(WEBHOOK_URL, WEBHOOK_SECRET, { job_id, status: "completed", video_url });
      console.log(`[job ${job_id}] ✅ Webhook de sucesso enviado e recebido pela Lovable!`);
    } catch (webhookError) {
      console.log(`[job ${job_id}] ⚠️ AVISO: O vídeo foi criado com sucesso, mas a Lovable recusou o Webhook.`);
    }

    // Limpeza Após Sucesso
    setTimeout(() => {
      console.log(`[job ${job_id}] 🧹 Limpando arquivos temporários do disco...`);
      try {
        fs.rmSync(workDir, { recursive: true, force: true });
        console.log(`[job ${job_id}] 🗑️ Pasta apagada com sucesso! Espaço libertado.`);
      } catch (cleanupErr) {
        // ignora silenciosamente
      }
    }, 15 * 60 * 1000); 
    
    // 🟢 LOOP: Chama o próximo vídeo da fila imediatamente
    processarFila();

  } catch (e) {
    console.log(`[FILA] FALHOU A GERAÇÃO:`, e?.message || e);
    
    if (job_id) {
       // Atualiza a UI para refletir a falha
       await supabase.from('videos').update({ status: 'failed' }).eq('id', job_id);
       await callWebhook(WEBHOOK_URL, WEBHOOK_SECRET, { job_id, status: "failed", error: e?.message || String(e) });
       
       const workDir = path.join("/tmp", "video-worker", job_id);
       try {
         if (fs.existsSync(workDir)) fs.rmSync(workDir, { recursive: true, force: true });
       } catch (cleanupErr) {}
    }

    // 🟢 Prevenção de Loop Infinito (Espera 10s se der erro antes de tentar o próximo)
    setTimeout(processarFila, 10000);
  }
}

// Inicia a Fila
processarFila();

app.listen(PORT, () => console.log("Worker rodando na porta", PORT));
