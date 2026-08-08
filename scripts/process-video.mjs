import { createWriteStream, existsSync } from 'node:fs';
import { mkdir, readFile, rm, stat } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { spawn } from 'node:child_process';
import path from 'node:path';

const payload = JSON.parse(process.env.JOB_PAYLOAD ?? '{}');
const apiBase = (process.env.TELEGRAM_API_BASE_URL ?? 'http://127.0.0.1:8081').replace(/\/$/, '');
const token = process.env.TELEGRAM_BOT_TOKEN;
const safeJobId = String(payload.jobId ?? '').replace(/[^A-Za-z0-9_-]/g, '_').slice(0, 100);
const workDir = path.resolve(process.env.RUNNER_TEMP ?? '/tmp', `video-job-${safeJobId}`);
const sourcePath = path.join(workDir, 'source');
const safeFileName = path.basename(payload.newFileName).replace(/[<>:"/\\|?*\u0000-\u001F]/g, '_').replace(/[. ]+$/, '').slice(0, 240);
const outputPath = path.join(workDir, safeFileName);
const MAX_FILE_BYTES = 1536 * 1024 * 1024;

if (!token || !safeJobId || !payload.fileId || !payload.chatId || !payload.newFileName || !payload.callbackUrl || !payload.callbackToken) {
  throw new Error('اطلاعات job ناقص است');
}

async function api(method, body) {
  const response = await fetch(`${apiBase}/bot${token}/${method}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
  });
  const result = await response.json();
  if (!response.ok || !result.ok) throw new Error(`${method} failed: ${response.status}`);
  return result.result;
}

async function downloadFile() {
  const file = await api('getFile', { file_id: payload.fileId });
  if (!file.file_path) throw new Error('Telegram file path is missing');
  if (path.isAbsolute(file.file_path) && existsSync(file.file_path)) {
    await run('cp', [file.file_path, sourcePath]);
  } else {
    const fileResponse = await fetch(`${apiBase}/file/bot${token}/${file.file_path}`);
    if (!fileResponse.ok || !fileResponse.body) throw new Error(`download failed: ${fileResponse.status}`);
    await pipeline(fileResponse.body, createWriteStream(sourcePath));
  }
  const sourceStats = await stat(sourcePath);
  if (sourceStats.size > MAX_FILE_BYTES) throw new Error('فایل بزرگ‌تر از حد مجاز ۱.۵ گیگابایت است');
}

function run(command, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: 'inherit' });
    child.on('error', reject);
    child.on('close', (code) => code === 0 ? resolve() : reject(new Error(`${command} exited with ${code}`)));
  });
}

async function sendDocument() {
  // Do not load a 1.5GB file into memory. curl streams the file from disk.
  const responseFile = path.join(workDir, 'telegram-response.json');
  await run('curl', ['--fail', '--silent', '--show-error', '-X', 'POST', `${apiBase}/bot${token}/sendDocument`,
    '-F', `chat_id=${payload.chatId}`, '-F', `document=@${outputPath};filename=${safeFileName}`, '-o', responseFile]);
  const result = JSON.parse(await readFile(responseFile, 'utf8'));
  if (!result.ok) throw new Error('sendDocument returned an unsuccessful Telegram response');
}

async function callback(status, errorMessage) {
  const response = await fetch(payload.callbackUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'X-Callback-Token': payload.callbackToken },
    body: JSON.stringify({ jobId: payload.jobId, status, errorMessage }),
  });
  if (!response.ok) throw new Error(`callback failed: ${response.status}`);
}

try {
  await mkdir(workDir, { recursive: true });
  await downloadFile();
  // Remux only when requested. This changes the container without re-encoding.
  if (process.env.REMUX_WITH_FFMPEG === 'true') {
    await run('ffmpeg', ['-y', '-i', sourcePath, '-map', '0', '-c', 'copy', outputPath]);
  } else {
    await run('cp', [sourcePath, outputPath]);
  }
  await sendDocument();
  await callback('succeeded');
} catch (error) {
  console.error(error);
  try { await callback('failed', error instanceof Error ? error.message : 'unknown error'); } catch (callbackError) { console.error(callbackError); }
  process.exitCode = 1;
} finally {
  if (existsSync(workDir)) await rm(workDir, { recursive: true, force: true });
}