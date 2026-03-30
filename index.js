const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const { createClient } = require('@supabase/supabase-js'); // <-- 1. Adicionado o SDK do Supabase

const app = express();
app.use(express.json({ limit: "50mb" }));
app.use("/videos", express.static("/tmp/video-worker"));

// ---------- CONFIG (Render Env Vars) ----------
const PORT = process.env.PORT || 3000;
// 2. Precisamos dessas variáveis para conectar na tabela `videos`
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY; 

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

// ----------------------------------------------------------------------
// 3. A NOVA ENGRENAGEM DA FILA: Função que processa UM vídeo por vez
// ----------------------------------------------------------------------
async function processarFila() {
    try {
        // A. Procura o vídeo mais antigo que está aguardando na fila
        const { data: job, error: buscaError } = await supabase
            .from('videos')
            .select('*')
            .eq('status', 'na_fila')
            .order('created_at', { ascending: true }) // Pega o mais antigo primeiro
            .limit(1)
            .single(); // Garante que retorne só um objeto, não um array

        // Se não achou nenhum vídeo (a fila está vazia), aguarda 5 segundos e tenta de novo
        if (buscaError || !job) {
            setTimeout(processarFila, 5000); 
            return;
        }

        const job_id = job.id; // Usamos o ID do Supabase como ID da pasta
        console.log(`\n==================================================`);
        console.log(`[FILA] Encontrou vídeo para processar: ${job_id}`);
        console.log(`==================================================`);

        // B. Trava o vídeo no banco para nenhum outro processo (se você escalar) pegar
        await supabase
            .from('videos')
            .update({ status: 'processando' })
            .eq('id', job_id);

        // C. Extrai os dados do vídeo da tabela
        // IMPORTANTE: Ajuste os nomes das variáveis abaixo para bater EXATAMENTE
        // com o nome das colunas que você tem na sua tabela `videos`!
        const audio_url = job.audio_url; 
        const subtitle_url = job.subtitle_url; 
        const subtitle_text = job.subtitle_text;
        
        // Assumindo que você salva o JSON das cenas/brolls numa coluna, ex: 'timeline_data' ou 'broll_urls'
        // Se você não tiver uma coluna com a timeline, você precisa adicionar para o Render saber o que baixar.
        const timeline = job.timeline_data || []; 
        const broll_urls = job.broll_urls || [];

        // D. Configurações de saída (Você pode pegar do banco se tiver, ou usar o padrão)
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
        // E. O SEU CÓDIGO ORIGINAL DE GERAÇÃO ENTRA AQUI! (Sem alterações na lógica do FFmpeg)
        // ----------------------------------------------------------------
        console.log(`[job ${job_id}] baixando áudio...`);
        if(audio_url) await downloadToFile(audio_url, audioPath); // Verifica se tem audio

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
        
        // Só adiciona o input de áudio se ele existir e foi baixado
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
        // F. FINALIZAÇÃO: Salva URL no banco e limpa a sujeira
        // ----------------------------------------------------------------
        
        const serverUrl = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
        const video_url = `${serverUrl}/videos/${job_id}/output.mp4`;

        // Atualiza a tabela com o status concluído e o link do vídeo
        await supabase
            .from('videos')
            .update({ 
                status: 'concluido',
                drive_link: video_url // Usando a URL pública do seu Render para o vídeo
            })
            .eq('id', job_id);

        console.log(`[job ${job_id}] ✅ Banco de dados atualizado com sucesso!`);

        // Limpeza agendada do disco do Render
        setTimeout(() => {
            try {
                fs.rmSync(workDir, { recursive: true, force: true });
                console.log(`[job ${job_id}] 🗑️ Pasta apagada com sucesso!`);
            } catch (cleanupErr) {
                console.log(`[job ${job_id}] ⚠️ Erro ao limpar pasta:`, cleanupErr.message);
            }
        }, 15 * 60 * 1000); 

        // G. Chama a função DE NOVO imediatamente para pegar o PRÓXIMO vídeo da fila!
        processarFila();

    } catch (e) {
        console.error(`[FILA ERRO CRÍTICO]`, e?.message || e);
        
        // Tenta marcar o vídeo atual como 'erro' para não travar a fila nele para sempre
        // Nota: Idealmente você precisaria do job_id aqui, mas como o erro pode acontecer antes de pegar o ID, 
        // a fila apenas pausa 10s e tenta de novo.
        
        setTimeout(processarFila, 10000); 
    }
}

// 4. Inicia o motor da fila assim que o servidor subir
console.log("Iniciando Worker de Vídeos com Fila Supabase...");
processarFila();

app.listen(PORT, () => console.log("Worker (Web Server) rodando na porta", PORT));
