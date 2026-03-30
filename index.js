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

const DEFAULT_WIDTH = parseInt(process.env.DEFAULT_WIDTH || "720", 10);
const DEFAULT_HEIGHT = parseInt(process.env.DEFAULT_HEIGHT || "1280", 10);
const DEFAULT_FPS = parseInt(process.env.DEFAULT_FPS || "30", 10);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

app.get("/", (req, res) => res.send("Worker ativo"));
app.get("/health", (req, res) => res.json({ ok: true }));

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

async function downloadToFile(url, filePath) {
  // Validação rigorosa de URL para evitar o erro de "Invalid URL"
  if (!url || typeof url !== 'string' || !url.startsWith('http')) {
    throw new Error(`URL inválida ou vazia recebida: ${url}`);
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
    console.log("[ffmpeg] executando...");
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
    } catch (e) {
        console.log("[Webhook] Falha silenciada:", e.message);
    }
}

// ----------------------------------------------------------------------
// MOTOR DA FILA
// ----------------------------------------------------------------------
async function processarFila() {
    let job_id = null;

    try {
        // 1. Busca o próximo
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

        // 2. MUDA O STATUS IMEDIATAMENTE (Trava o registro)
        // Usamos 'editing_video' ou 'failed' para tirar do status 'na_fila'
        await supabase.from('videos').update({ status: 'editing_video' }).eq('id', job_id);

        const audio_url = job.narration_audio_url; 
        const timeline = job.b_roll_video_urls || [];
        const workDir = path.join("/tmp", "video-worker", job_id);
        ensureDir(workDir);

        const audioPath = path.join(workDir, "audio.mp3");
        const outputPath = path.join(workDir, "output.mp4");

        // 3. Downloads
        if (audio_url) {
            console.log("Baixando áudio...");
            await downloadToFile(audio_url, audioPath);
        }

        if (!timeline || timeline.length === 0) throw new Error("Timeline vazia");

        console.log(`Baixando ${timeline.length} clipes...`);
        const clips = [];
        for (let i = 0; i < timeline.length; i++) {
            const cPath = path.join(workDir, `raw_${i}.mp4`);
            const nPath = path.join(workDir, `norm_${i}.mp4`);
            await downloadToFile(timeline[i], cPath);
            
            // Normaliza cada clipe
            await runFfmpeg([
                "-y", "-t", "5", "-i", cPath,
                "-vf", `fps=${DEFAULT_FPS},scale=${DEFAULT_WIDTH}:${DEFAULT_HEIGHT}:force_original_aspect_ratio=increase,crop=${DEFAULT_WIDTH}:${DEFAULT_HEIGHT},format=yuv420p`,
                "-c:v", "libx264", "-preset", "ultrafast", "-an", nPath
            ]);
            clips.push(nPath);
        }

        // 4. Concatena e Finaliza
        const playlistPath = path.join(workDir, "playlist.txt");
        fs.writeFileSync(playlistPath, clips.map(p => `file '${p}'`).join('\n'));

        console.log("Renderizando final...");
        const finalArgs = ["-y", "-f", "concat", "-safe", "0", "-i", playlistPath];
        if (fs.existsSync(audioPath)) finalArgs.push("-i", audioPath);
        finalArgs.push("-c:v", "libx264", "-preset", "ultrafast", "-crf", "28", "-movflags", "+faststart", outputPath);
        
        await runFfmpeg(finalArgs);

        // 5. Update Sucesso
        const serverUrl = process.env.RENDER_EXTERNAL_URL || `https://${process.env.RENDER_SERVICE_NAME}.onrender.com`;
        const video_url = `${serverUrl}/videos/${job_id}/output.mp4`;

        // Se o seu banco usa 'completed' ou 'concluido', verifique aqui:
        await supabase.from('videos').update({ status: 'in_review', final_video_url: video_url }).eq('id', job_id);
        await callWebhook(WEBHOOK_URL, WEBHOOK_SECRET, { job_id, status: "completed", video_url });

        console.log("✅ Sucesso!");
        processarFila();

    } catch (e) {
        console.error(`❌ ERRO NO JOB ${job_id}:`, e.message);
        
        if (job_id) {
            // Tenta mudar o status para 'failed' para o vídeo sair da fila de busca
            await supabase.from('videos').update({ status: 'failed', error_message: e.message }).eq('id', job_id);
        }
        
        // Espera 10 segundos antes de tentar o PRÓXIMO vídeo (evita loop infinito de erro)
        setTimeout(processarFila, 10000);
    }
}

processarFila();
app.listen(PORT, () => console.log("Worker pronto na porta", PORT));
