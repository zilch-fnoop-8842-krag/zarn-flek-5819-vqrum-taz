import { createWriteStream, existsSync } from 'node:fs';
import { copyFile, mkdir, stat } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import path from 'node:path';

const payload = JSON.parse(process.env.JOB_PAYLOAD ?? '{}');
const token = process.env.TELEGRAM_BOT_TOKEN;
// Local bot API server URL (running via deploy/telegram-bot-api.service)
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

try {
  console.log(`Requesting getFile for ${payload.fileId} from Local Bot API...`);
  // Calling getFile against the LOCAL Bot API server (supports up to 2000 MB)
  const file = await callTelegram('getFile', { file_id: payload.fileId });

  if (!file.file_path) {
    throw new Error('Telegram did not return a valid file_path.');
  }

  const safeName = path.basename(payload.fileName).replace(/[<>:"/\\|?*\u0000-\u001F]/g, '_');
  const targetPath = path.join(downloadDir, safeName);

  // When telegram-bot-api runs in --local mode, file_path is an absolute path on disk
  if (path.isAbsolute(file.file_path) && existsSync(file.file_path)) {
    console.log(`Local file detected at ${file.file_path}. Copying to ${targetPath}...`);
    await copyFile(file.file_path, targetPath);
  } else {
    // Or stream from local HTTP endpoint
    const downloadUrl = `${apiBase}/file/bot${token}/${file.file_path}`;
    console.log(`Downloading stream from ${downloadUrl}...`);
    const res = await fetch(downloadUrl);
    if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
    await pipeline(Readable.fromWeb(res.body), createWriteStream(targetPath));
  }

  const fileStats = await stat(targetPath);
  const sizeMB = (fileStats.size / (1024 * 1024)).toFixed(2);
  console.log(`Done! File saved: ${targetPath} (${sizeMB} MB)`);

  await callTelegram('sendMessage', {
    chat_id: payload.chatId,
    text: `✅ File \`${safeName}\` (${sizeMB} MB) successfully downloaded and saved in repository root \`downloads/\` folder!`,
    parse_mode: 'Markdown',
  });
} catch (error) {
  console.error('Download error:', error);
  await callTelegram('sendMessage', {
    chat_id: payload.chatId,
    text: `❌ Download failed: ${error.message}`,
  }).catch(() => {});
  process.exitCode = 1;
}
