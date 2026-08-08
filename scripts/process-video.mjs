import { existsSync } from 'node:fs';
import { mkdir, rm } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { createRequire } from 'node:module';

// Use createRequire to safely load CommonJS GramJS modules inside an ESM file
const require = createRequire(import.meta.url);
const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions/index.js');

const payload = JSON.parse(process.env.JOB_PAYLOAD ?? '{}');
const token = process.env.TELEGRAM_BOT_TOKEN;
const apiId = parseInt(process.env.TELEGRAM_API_ID ?? '0', 10);
const apiHash = process.env.TELEGRAM_API_HASH;

const safeJobId = String(payload.jobId ?? '').replace(/[^A-Za-z0-9_-]/g, '_').slice(0, 100);
const workDir = path.resolve(process.env.RUNNER_TEMP ?? '/tmp', `video-job-${safeJobId}`);
const sourcePath = path.join(workDir, 'source');
const safeFileName = path.basename(payload.newFileName).replace(/[<>:"/\\|?*\u0000-\u001F]/g, '_').replace(/[. ]+$/, '').slice(0, 240);
const outputPath = path.join(workDir, safeFileName);

if (!token || !safeJobId || !payload.chatId || !payload.messageId || !payload.newFileName || !payload.callbackUrl || !payload.callbackToken) {
  throw new Error('اطلاعات job ناقص است');
}

if (!apiId || !apiHash) {
  throw new Error('TELEGRAM_API_ID and TELEGRAM_API_HASH are missing. Please add them to GitHub Repository Secrets.');
}

async function callback(status, errorMessage) {
  const response = await fetch(payload.callbackUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'X-Callback-Token': payload.callbackToken },
    body: JSON.stringify({ jobId: payload.jobId, status, errorMessage }),
  });
  if (!response.ok) throw new Error(`callback failed: ${response.status}`);
}

function run(command, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: 'inherit' });
    child.on('error', reject);
    child.on('close', (code) => code === 0 ? resolve() : reject(new Error(`${command} exited with ${code}`)));
  });
}

try {
  await mkdir(workDir, { recursive: true });

  const client = new TelegramClient(new StringSession(''), apiId, apiHash, {
    connectionRetries: 5,
    useWSS: false,
  });

  console.log('Logging in to Telegram via MTProto...');
  await client.start({ botAuthToken: token });

  // Parse chat ID (GramJS accepts numbers for normal/supergroups)
  const peer = /^-?\d+$/.test(payload.chatId) ? Number(payload.chatId) : payload.chatId;
  
  console.log(`Fetching message ${payload.messageId} from chat ${peer}...`);
  const messages = await client.getMessages(peer, { ids: payload.messageId });
  const message = Array.isArray(messages) ? messages[0] : messages;

  if (!message || !message.media) {
    throw new Error('Could not find the media message via MTProto. Ensure the bot has access to the chat history.');
  }

  console.log('Downloading media via MTProto (bypassing 20MB limit)...');
  let lastDownloadPct = -1;
  await client.downloadMedia(message, {
    outputFile: sourcePath,
    progressCallback: (progress) => {
      const pct = Math.round(progress * 100);
      if (pct !== lastDownloadPct && pct % 10 === 0) {
        console.log(`Download progress: ${pct}%`);
        lastDownloadPct = pct;
      }
    },
  });

  if (process.env.REMUX_WITH_FFMPEG === 'true') {
    console.log('Remuxing with FFmpeg...');
    await run('ffmpeg', ['-y', '-i', sourcePath, '-map', '0', '-c', 'copy', outputPath]);
  } else {
    console.log('Copying file...');
    await run('cp', [sourcePath, outputPath]);
  }

  console.log('Uploading media via MTProto (bypassing 50MB limit)...');
  let lastUploadPct = -1;
  await client.sendFile(peer, {
    file: outputPath, // GramJS automatically streams local files
    forceDocument: true,
    progressCallback: (progress) => {
      const pct = Math.round(progress * 100);
      if (pct !== lastUploadPct && pct % 10 === 0) {
        console.log(`Upload progress: ${pct}%`);
        lastUploadPct = pct;
      }
    },
  });

  console.log('Disconnecting...');
  await client.disconnect();
  
  await callback('succeeded');
} catch (error) {
  console.error(error);
  try { await callback('failed', error instanceof Error ? (error.stack || error.message) : String(error)); } catch (callbackError) { console.error(callbackError); }
  process.exitCode = 1;
} finally {
  if (existsSync(workDir)) await rm(workDir, { recursive: true, force: true });
}
