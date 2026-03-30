const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(express.json({ limit: "50mb" }));
// Caminho para servir os vídeos gerados
app.use("/videos", express.static("/tmp/video-worker"));

// ---------- CONFIG (Render Env Vars) --------
const PORT = process.env.PORT || 3000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY; 

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;

const DEFAULT_WIDTH = parseInt(process.env.DEFAULT_WIDTH || "720", 10);
const DEFAULT_HEIGHT = parseInt(process.env.DEFAULT_HEIGHT || "1280", 10);
const DEFAULT_FPS = parseInt(process.env.DEFAULT_FPS || "30", 10);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

app.get("/", (req, res) => res.send("Worker com Fila rodando OK"));
app.get("/health", (req, res) => res.json({ ok: true }));

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

async function downloadToFile(url, filePath) {
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

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    console.log("[ffmpeg] cmd:", `ffmpeg ${args.join(" ")}`);
    const p = spawn("ffmpeg", args, { stdio: ["ignore", "pipe", "pipe"] });

    p.on("error", reject);
    p.on("close", (code) => {
      if (code === 0) return resolve();
      reject(new Error(`ffmpeg saiu com code ${code}`));
    });
  });
}

async function callWebhook(webhook_url, webhook_secret, payload) {
    if (!webhook_url) return; 
    try {
        await axios.post(webhook_url, payload, {
          headers: {
            "Content-Type": "application/json",
            "x-webhook-secret": webhook_secret || "",
          },
          timeout: 30000,
        });
    } catch (e) {
        console.log("[Webhook] Erro ao disparar:", e.message);
    }
}

// ----------------------------------------------------------------------
// FUNÇÃO PRINCIPAL DA FILA
// ----------------------------------------------------------------------
async function processarFila() {
    let job_id;

    try {
        const { data: job, error: buscaError } = await supabase
            .from('videos')
            .select('*')
            .eq('status', 'na_fila')
            .order('created_at', { ascending: true })
            .limit(1)
            .single(); 

        if (buscaError || !job) {
            setTimeout(processarFila, 5000); 
            return;
        }

        job_id = job.id; 
        console.log(`\n[FILA] Processando: ${job_id}`);

        await supabase
            .from('videos')
            .update({ status: 'processando' })
            .eq('id', job_id);

        // NOMES DAS COLUNAS AJUSTADOS PARA O SEU BANCO
        const audio_url = job.narration_audio_url; 
        const subtitle_url = job.subtitle_url; 
        const subtitle_text = job.subtitle_text;
        const timeline = job.b_roll_video_urls || [];

        const workDir = path.join("/tmp", "video-worker", job_id);
        ensureDir(workDir);

        const audioPath = path.join(workDir, "audio.mp3");
        const outputPath = path.join(workDir, "output.mp4");
        const srtPath = path.join(workDir, "subs.srt");
        
        let activeSubtitlePath = null;
        const downloadedClipsMap = {};

        // 1. Download do Áudio
        if(audio_url) {
            console.log(`[job ${job_id}] baixando áudio...`);
            await downloadToFile(audio_url, audioPath);
        }

        // 2. Download dos Vídeos (Timeline)
        const urlsToDownload = new Set();
        timeline.forEach(url => {
            if (url) urlsToDownload.add(url);
        });

        if (urlsToDownload.size === 0) {
            throw new Error("Nenhum vídeo (B-roll) foi selecionado para montagem.");
        }

        console.log(`[job ${job_id}] Baixando ${urlsToDownload.size} clipes...`);
        let idx = 0;
        for (const url of urlsToDownload) {
            const cPath = path.join(workDir, `raw_${idx}.mp4`);
            await downloadToFile(url, cPath);
            downloadedClipsMap[url] = cPath;
            idx++;
        }

        // 3. Normalização dos clipes
        const normalizedClips = [];
        const vf = `fps=${DEFAULT_FPS},scale=${DEFAULT_WIDTH}:${DEFAULT_HEIGHT}:force_original_aspect_ratio=increase,crop=${DEFAULT_WIDTH}:${DEFAULT_HEIGHT},format=yuv420p`;

        for (let i = 0; i < timeline.length; i++) {
            const url = timeline[i];
            const rawPath = downloadedClipsMap[url];
            if (!rawPath) continue;

            const normPath = path.join(workDir, `slice_${i}.mp4`);
            console.log(`[job ${job_id}] Processando clipe ${i}...`);

            await runFfmpeg([
                "-y", "-ss", "0", "-t", "5", 
                "-i", rawPath,
                "-vf", vf,
                "-c:v", "libx264", "-preset", "ultrafast", "-an", 
                normPath
            ]);
            normalizedClips.push(normPath);
        }

        // 4. Concatenação
        const playlistPath = path.join(workDir, "playlist.txt");
        const playlistContent = normalizedClips.map(p => `file '${p}'`).join('\n');
        fs.writeFileSync(playlistPath, playlistContent);

        // 5. Legendas
        if (subtitle_url) {
            await downloadToFile(subtitle_url, srtPath);
            activeSubtitlePath = srtPath;
        } else if (subtitle_text) {
            fs.writeFileSync(srtPath, subtitle_text);
            activeSubtitlePath = srtPath;
        }

        // 6. Montagem Final
        console.log(`[job ${job_id}] Gerando vídeo final...`);
        const finalArgs = ["-y", "-f", "concat", "-safe", "0", "-i", playlistPath];
        
        if(fs.existsSync(audioPath)) finalArgs.push("-i", audioPath);

        if (activeSubtitlePath) {
            const style = `Alignment=2,MarginV=90,Fontname=Montserrat,Bold=1,Fontsize=18`;
            finalArgs.push("-vf", `subtitles=${activeSubtitlePath}:force_style='${style}'`);
        }

        finalArgs.push("-c:v", "libx264", "-preset", "ultrafast", "-crf", "28");
        if(fs.existsSync(audioPath)) finalArgs.push("-c:a", "aac", "-shortest");
        
        finalArgs.push(outputPath);
        await runFfmpeg(finalArgs);

        // 7. Finalização
        const serverUrl = process.env.RENDER_EXTERNAL_URL || `https://${process.env.RENDER_SERVICE_NAME}.onrender.com`;
        const video_url = `${serverUrl}/videos/${job_id}/output.mp4`;

        await supabase.from('videos').update({ 
            status: 'concluido', 
            drive_link: video_url 
        }).eq('id', job_id);

        await callWebhook(WEBHOOK_URL, WEBHOOK_SECRET, { job_id, status: "completed", video_url });

        processarFila(); // Próximo!

    } catch (e) {
        console.error(`[ERRO]`, e.message);
        if (job_id) {
            await supabase.from('videos').update({ status: 'failed' }).eq('id', job_id);
            await callWebhook(WEBHOOK_URL, WEBHOOK_SECRET, { job_id, status: "failed", error: e.message });
        }
        setTimeout(processarFila, 10000);
    }
}

processarFila();
app.listen(PORT, () => console.log("Servidor rodando na porta", PORT));
