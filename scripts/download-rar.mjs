import { createWriteStream, existsSync } from 'node:fs';
import { mkdir, stat } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';

const execAsync = promisify(exec);

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

try {
  console.log(`Requesting getFile for ${payload.fileId} from Local Bot API...`);
  const file = await callTelegram('getFile', { file_id: payload.fileId });

  if (!file.file_path) {
    throw new Error('Telegram did not return a valid file_path.');
  }

  const safeName = path.basename(payload.fileName).replace(/[<>:"/\\|?*\u0000-\u001F]/g, '_');
  const targetPath = path.join(downloadDir, safeName);

  console.log(`Telegram local file path: ${file.file_path}`);

  // 1. Try direct copy with sudo (bypasses Docker permissions and copies 2GB instantly)
  let copied = false;
  try {
    console.log(`Copying via sudo: ${file.file_path} -> ${targetPath}...`);
    await execAsync(`sudo cp "${file.file_path}" "${targetPath}"`);
    // Ensure the runner user owns the file so git can commit it
    await execAsync(`sudo chown $(id -u):$(id -g) "${targetPath}"`);
    copied = true;
    console.log(`Successfully copied directly from disk.`);
  } catch (copyErr) {
    console.warn(`Direct copy failed (${copyErr.message}). Falling back to HTTP stream...`);
  }

  // 2. Fallback: Stream via HTTP with corrected relative path
  if (!copied) {
    // Strip the local directory prefix so it forms a valid relative endpoint: 'documents/file_0'
    const relativePath = file.file_path
      .replace(/^\/?var\/lib\/telegram-bot-api\/[^\/]+\//, '')
      .replace(/^\/+/, '');

    const downloadUrl = `${apiBase}/file/bot${token}/${relativePath}`;
    console.log(`Downloading stream from ${downloadUrl}...`);

    const res = await fetch(downloadUrl);
    if (!res.ok || !res.body) {
      throw new Error(`HTTP ${res.status}: ${res.statusText}`);
    }
    await pipeline(Readable.fromWeb(res.body), createWriteStream(targetPath));
  }

  const fileStats = await stat(targetPath);
  const sizeMB = (fileStats.size / (1024 * 1024)).toFixed(2);
  console.log(`Done! File saved: ${targetPath} (${sizeMB} MB)`);

  await callTelegram('sendMessage', {
    chat_id: payload.chatId,
    text: `✅ File \`${safeName}\` (${sizeMB} MB) successfully saved in repository root \`downloads/\` folder!`,
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
