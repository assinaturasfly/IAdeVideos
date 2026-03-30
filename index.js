const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(express.json({ limit: "50mb" }));
app.use("/videos", express.static("/tmp/video-worker"));

// ---------- CONFIG --------
const PORT = process.env.PORT || 3000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY; 

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;

const DEFAULT_WIDTH = parseInt(process.env.DEFAULT_WIDTH || "1080", 10);
const DEFAULT_HEIGHT = parseInt(process.env.DEFAULT_HEIGHT || "1920", 10);
const DEFAULT_FPS = parseInt(process.env.DEFAULT_FPS || "30", 10);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

app.get("/", (req, res) => res.send("Worker Fila Ativo"));

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

async function downloadToFile(url, filePath) {
  if (!url || !url.startsWith('http')) throw new Error(`URL Inválida: ${url}`);
  const r = await axios({ url, responseType: "stream", timeout: 120000 });
  await new Promise((resolve, reject) => {
    const w = fs.createWriteStream(filePath);
    r.data.pipe(w);
    w.on("finish", resolve);
    w.on("error", reject);
  });
}

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    const p = spawn("ffmpeg", args);
    p.on("close", (code) => code === 0 ? resolve() : reject(new Error(`FFmpeg erro: ${code}`)));
  });
}

async function callWebhook(webhook_url, webhook_secret, payload) {
    if (!webhook_url) return; 
    try {
        await axios.post(webhook_url, payload, {
          headers: { "Content-Type": "application/json", "x-webhook-secret": webhook_secret || "" },
          timeout: 20000,
        });
    } catch (e) { console.log("[Webhook] Falha silenciada:", e.message); }
}

// ----------------------------------------------------------------------
// MOTOR DA FILA COM ATUALIZAÇÃO DE ETAPAS (CICLOS)
// ----------------------------------------------------------------------
async function processarFila() {
    let job_id = null;

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
        console.log(`\n--- [FILA] Iniciando: ${job_id} ---`);

        // 🟢 ETAPA 1: Gerando Narração (Início do download do áudio)
        await supabase.from('videos').update({ status: 'generating_audio' }).eq('id', job_id);

        const audio_url = job.narration_audio_url; 
        const timeline = job.b_roll_video_urls || [];
        const subtitle_text = job.subtitle_text;
        
        const workDir = path.join("/tmp", "video-worker", job_id);
        ensureDir(workDir);

        const audioPath = path.join(workDir, "audio.mp3");
        const outputPath = path.join(workDir, "output.mp4");
        const srtPath = path.join(workDir, "subs.srt");

        if (audio_url) {
            console.log("Baixando áudio...");
            await downloadToFile(audio_url, audioPath);
        }

        // 🟢 ETAPA 2: Buscando/Processando B-roll
        await supabase.from('videos').update({ status: 'searching_broll' }).eq('id', job_id);

        console.log(`Processando ${timeline.length} clipes...`);
        const clips = [];
        const vfBase = `fps=${DEFAULT_FPS},scale=${DEFAULT_WIDTH}:${DEFAULT_HEIGHT}:force_original_aspect_ratio=increase,crop=${DEFAULT_WIDTH}:${DEFAULT_HEIGHT},format=yuv420p`;

        for (let i = 0; i < timeline.length; i++) {
            const cPath = path.join(workDir, `raw_${i}.mp4`);
            const nPath = path.join(workDir, `norm_${i}.mp4`);
            await downloadToFile(timeline[i], cPath);
            
            await runFfmpeg([
                "-y", "-t", "5", "-i", cPath,
                "-vf", vfBase,
                "-c:v", "libx264", "-preset", "ultrafast", "-an", nPath
            ]);
            clips.push(nPath);
        }

        // 🟢 ETAPA 3: Gerando Legendas
        await supabase.from('videos').update({ status: 'generating_subtitles' }).eq('id', job_id);
        let hasSubtitles = false;
        if (subtitle_text) {
            fs.writeFileSync(srtPath, subtitle_text);
            hasSubtitles = true;
            console.log("Legenda salva no disco.");
        }

        // 🟢 ETAPA 4: Montando Vídeo (Render Final)
        await supabase.from('videos').update({ status: 'rendering_video' }).eq('id', job_id);

        const playlistPath = path.join(workDir, "playlist.txt");
        fs.writeFileSync(playlistPath, clips.map(p => `file '${p}'`).join('\n'));

        console.log("Renderizando final...");
        const finalArgs = ["-y", "-f", "concat", "-safe", "0", "-i", playlistPath];
        
        if (fs.existsSync(audioPath)) {
            finalArgs.push("-i", audioPath);
        }

        if (hasSubtitles) {
            const style = `Alignment=2,MarginV=120,Fontname=Montserrat,Bold=1,Fontsize=18,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=1,Outline=1`;
            finalArgs.push("-vf", `subtitles=${srtPath}:force_style='${style}'`);
        }

        finalArgs.push(
            "-c:v", "libx264", 
            "-preset", "ultrafast", 
            "-crf", "28", 
            "-c:a", "aac", 
            "-b:a", "128k",
            "-shortest", 
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            outputPath
        );
        
        await runFfmpeg(finalArgs);

        // 🟢 ETAPA 5: Finalização / Em Revisão
        const serverUrl = process.env.RENDER_EXTERNAL_URL || `https://${process.env.RENDER_SERVICE_NAME}.onrender.com`;
        const video_url = `${serverUrl}/videos/${job_id}/output.mp4`;

        await supabase.from('videos').update({ 
            status: 'in_review', 
            final_video_url: video_url 
        }).eq('id', job_id);

        await callWebhook(WEBHOOK_URL, WEBHOOK_SECRET, { job_id, status: "completed", video_url });

        console.log("✅ Vídeo pronto e Webhook enviado!");
        
        // Pequeno delay para garantir que o banco atualizou antes de buscar o próximo
        setTimeout(processarFila, 1000);

    } catch (e) {
        console.error(`❌ ERRO NO JOB ${job_id}:`, e.message);
        if (job_id) {
            await supabase.from('videos').update({ status: 'failed' }).eq('id', job_id);
        }
        setTimeout(processarFila, 10000);
    }
}

processarFila();
app.listen(PORT, () => console.log("Worker pronto na porta", PORT));
