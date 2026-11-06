import { createWriteStream, existsSync } from 'node:fs';
import { copyFile, mkdir, stat } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import path from 'node:path';

const payload = JSON.parse(process.env.JOB_PAYLOAD ?? '{}');
const token = process.env.TELEGRAM_BOT_TOKEN;
const apiBase = (process.env.TELEGRAM_API_BASE_URL || 'http://127.0.0.1:8081').replace(/\/$/, '');

if (!token || !payload.fileId || !payload.fileName || !payload.chatId) {
  throw new Error('Incomplete job payload.');
}

const downloadDir = path.resolve(process.cwd(), 'downloads');
await mkdir(downloadDir, { recursive: true });

async function callTelegram(method, body) {
  const res = await fetch(`${apiBase}/bot${token}/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  if (!res.ok || !data.ok) {
    throw new Error(`Telegram ${method} failed: ${JSON.stringify(data)}`);
  }
  return data.result;
}

async function notify(text) {
  await callTelegram('sendMessage', { chat_id: payload.chatId, text }).catch(console.error);
}

try {
  console.log(`Resolving file_id: ${payload.fileId}...`);
  const file = await callTelegram('getFile', { file_id: payload.fileId });

  if (!file.file_path) {
    throw new Error('Telegram did not return a valid file_path.');
  }

  const safeName = path.basename(payload.fileName).replace(/[<>:"/\\|?*\u0000-\u001F]/g, '_');
  const targetPath = path.join(downloadDir, safeName);

  // If using local Bot API server in --local mode, file is directly on disk
  if (path.isAbsolute(file.file_path) && existsSync(file.file_path)) {
    console.log(`Local mode detected. Copying directly from ${file.file_path} to ${targetPath}...`);
    await copyFile(file.file_path, targetPath);
  } else {
    // Stream download (supports up to 2GB)
    const downloadUrl = `${apiBase}/file/bot${token}/${file.file_path}`;
    console.log(`Downloading from ${downloadUrl}...`);
    const res = await fetch(downloadUrl);
    if (!res.ok || !res.body) {
      throw new Error(`HTTP ${res.status}: ${res.statusText}`);
    }
    await pipeline(Readable.fromWeb(res.body), createWriteStream(targetPath));
  }

  const stats = await stat(targetPath);
  const sizeMB = (stats.size / (1024 * 1024)).toFixed(2);
  console.log(`Successfully saved ${safeName} (${sizeMB} MB) to ${targetPath}`);

  await notify(`File \`${safeName}\` (${sizeMB} MB) successfully saved to \`downloads/\` folder in the repository!`);
} catch (error) {
  console.error('Download failed:', error);
  await notify(`Failed to save RAR file: ${error.message}`);
  process.exitCode = 1;
}
