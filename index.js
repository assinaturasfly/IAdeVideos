const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(express.json({ limit: "50mb" }));
app.use("/videos", express.static("/tmp/video-worker"));

// ---------- CONFIG (Render Env Vars) ----------
const PORT = process.env.PORT || 3000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY; 

const WEBHOOK_URL = process.env.WEBHOOK_URL;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;

const DEFAULT_WIDTH = parseInt(process.env.DEFAULT_WIDTH || "720", 10);
const DEFAULT_HEIGHT = parseInt(process.env.DEFAULT_HEIGHT || "1280", 10);
const DEFAULT_FPS = parseInt(process.env.DEFAULT_FPS || "30", 10);

// Inicia o cliente do Supabase
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
// --------------------------------------------

app.get("/", (req, res) => res.send("Worker com Fila rodando OK"));
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

// Função para avisar a Vercel sobre o status
async function callWebhook(webhook_url, webhook_secret, payload) {
    if (!webhook_url) return; 
    await axios.post(webhook_url, payload, {
      headers: {
        "Content-Type": "application/json",
        "x-webhook-secret": webhook_secret || "",
      },
      timeout: 30000,
    });
  }

// ----------------------------------------------------------------------
// FUNÇÃO PRINCIPAL DA FILA
// ----------------------------------------------------------------------
async function processarFila() {
    let job_id; // Declarado no escopo principal para ser acessível no catch

    try {
        // A. Procura o vídeo mais antigo que está 'na_fila'
        const { data: job, error: buscaError } = await supabase
            .from('videos')
            .select('*')
            .eq('status', 'na_fila')
            .order('created_at', { ascending: true })
            .limit(1)
            .single(); 

        // Se a fila estiver vazia, aguarda 5 segundos e tenta de novo
        if (buscaError || !job) {
            setTimeout(processarFila, 5000); 
            return;
        }

        job_id = job.id; 
        console.log(`\n==================================================`);
        console.log(`[FILA] Encontrou vídeo para processar: ${job_id}`);
        console.log(`==================================================`);

        // B. Trava o vídeo no banco 
        await supabase
            .from('videos')
            .update({ status: 'processando' })
            .eq('id', job_id);

        // C. Extrai os dados do banco (VERIFIQUE OS NOMES DAS COLUNAS AQUI!)
        const audio_url = job.audio_url; 
        const subtitle_url = job.subtitle_url; 
        const subtitle_text = job.subtitle_text;
        const timeline = job.timeline_data || []; 
        const broll_urls = job.broll_urls || [];

        const width = DEFAULT_WIDTH;
        const height = DEFAULT_HEIGHT;
        const fps = DEFAULT_FPS;

        const workDir = path.join("/tmp", "video-worker", job_id);
        ensureDir(workDir);

        const audioPath = path.join(workDir, "audio.mp3");
        const outputPath = path.join(workDir, "output.mp4");
        const srtPath = path.join(workDir, "subs.srt");
        
        let activeSubtitlePath = null;
        const downloadedClipsMap = {};

        // ----------------------------------------------------------------
        // GERAÇÃO DO VÍDEO COM FFMPEG
        // ----------------------------------------------------------------
        console.log(`[job ${job_id}] baixando áudio...`);
        if(audio_url) await downloadToFile(audio_url, audioPath); 

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
            console.log(`[job ${job_id}] Recortando clipes com base na timeline...`);
            for (let i = 0; i < timeline.length; i++) {
                const clip = timeline[i];
                const rawPath = downloadedClipsMap[clip.url || clip.src];
                if (!rawPath) continue;

                const normPath = path.join(workDir, `slice_${i}.mp4`);
                const startTime = clip.start || clip.startTime || clip.ss || 0;
                const duration = 5; 
                
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
            console.log(`[job ${job_id}] Processando clipes crus...`);
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
            console.log(`[job ${job_id}] baixando legenda...`);
            await downloadToFile(subtitle_url, srtPath);
            activeSubtitlePath = srtPath;
        } else if (subtitle_text) {
            fs.writeFileSync(srtPath, subtitle_text);
            activeSubtitlePath = srtPath;
        }

        console.log(`[job ${job_id}] montagem final...`);
        const finalArgs = [
            "-y", "-hide_banner", "-loglevel", "info",
            "-f", "concat", "-safe", "0", "-i", playlistPath, 
        ];
        
        if(fs.existsSync(audioPath)){
           finalArgs.push("-i", audioPath);
        }

        if (activeSubtitlePath) {
            let marginV = 90; 
            const forceStyle = `Alignment=2,MarginV=${marginV},Fontname=Montserrat,Bold=1,Fontsize=8,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=1,Outline=0.4.,Shadow=0`;
            finalArgs.push("-vf", `subtitles=${activeSubtitlePath}:force_style='${forceStyle}'`);
        }

        finalArgs.push(
            "-map", "0:v:0"
        );
        
        if(fs.existsSync(audioPath)){
             finalArgs.push("-map", "1:a:0");
        }

        finalArgs.push(
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            "-shortest", 
            outputPath
        );

        await runFfmpeg(finalArgs);
        console.log(`[job ${job_id}] ffmpeg finalizou ✅`);

        // ----------------------------------------------------------------
        // FINALIZAÇÃO: Atualiza banco, avisa Webhook e limpa pasta
        // ----------------------------------------------------------------
        
        const serverUrl = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
        const video_url = `${serverUrl}/videos/${job_id}/output.mp4`;

        // 1. Atualiza o banco
        await supabase
            .from('videos')
            .update({ 
                status: 'concluido',
                drive_link: video_url 
            })
            .eq('id', job_id);

        console.log(`[job ${job_id}] ✅ Banco de dados atualizado com sucesso!`);

        // 2. Avisa a Vercel
        try {
            console.log(`[job ${job_id}] Enviando aviso para o Webhook da Vercel...`);
            await callWebhook(WEBHOOK_URL, WEBHOOK_SECRET, { 
                job_id: job_id, 
                status: "completed", 
                video_url: video_url 
            });
            console.log(`[job ${job_id}] ✅ Webhook recebido pela Vercel!`);
        } catch (webhookError) {
            console.log(`[job ${job_id}] ⚠️ O vídeo foi gerado, mas a Vercel falhou ao receber o Webhook:`, webhookError?.message);
        }

        // 3. Limpeza
        setTimeout(() => {
            try {
                fs.rmSync(workDir, { recursive: true, force: true });
                console.log(`[job ${job_id}] 🗑️ Pasta apagada com sucesso!`);
            } catch (cleanupErr) {
                console.log(`[job ${job_id}] ⚠️ Erro ao limpar pasta:`, cleanupErr.message);
            }
        }, 15 * 60 * 1000); 

        // 4. Chama o próximo da fila
        processarFila();

    } catch (e) {
        console.error(`[FILA ERRO CRÍTICO]`, e?.message || e);
        
        // Se houver job_id, avisa o erro no banco e para o webhook
        if (job_id) {
            try {
                await supabase.from('videos').update({ status: 'erro' }).eq('id', job_id);
                
                await callWebhook(WEBHOOK_URL, WEBHOOK_SECRET, { 
                    job_id: job_id, 
                    status: "failed", 
                    error: e?.message || String(e) 
                });
            } catch (err2) {
                console.log("[webhook] ERRO ao atualizar falha:", err2.message);
            }
        }
        
        // Pausa 10s e tenta de novo
        setTimeout(processarFila, 10000); 
    }
}

// Inicia o motor da fila
console.log("Iniciando Worker de Vídeos com Fila Supabase...");
processarFila();

app.listen(PORT, () => console.log("Worker (Web Server) rodando na porta", PORT));
