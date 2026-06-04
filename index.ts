// EstateEdge — Monorepo Entry Point
// Starts all services in a single process (local dev only)
// For production, each service runs independently via its own Dockerfile

import { spawn, ChildProcess } from 'child_process';
import path from 'path';

const services: { name: string; cwd: string; entry: string }[] = [
  { name: 'gateway',           cwd: 'gateway',                       entry: 'src/index.ts' },
  { name: 'user-service',      cwd: 'services/user-service',         entry: 'index.ts' },
  { name: 'site-service',      cwd: 'services/site-service',         entry: 'index.ts' },
  { name: 'ai-service',        cwd: 'services/ai-service',           entry: 'src/index.ts' },
  { name: 'analytics-service', cwd: 'services/analytics-service',    entry: 'index.ts' },
];

function startService(svc: { name: string; cwd: string; entry: string }): ChildProcess {
  const cwd = path.resolve(__dirname, svc.cwd);
  const proc = spawn(
    'npx',
    ['ts-node-dev', '--respawn', '--transpile-only', svc.entry],
    {
      cwd,
      stdio: 'pipe',
      shell: true,
      env: { ...process.env },
    }
  );

  proc.stdout?.on('data', (d) => process.stdout.write(`[${svc.name}] ${d}`));
  proc.stderr?.on('data', (d) => process.stderr.write(`[${svc.name}] ${d}`));

  proc.on('exit', (code) => {
    if (code !== 0) {
      console.error(`[${svc.name}] exited with code ${code} — restarting in 3s...`);
      setTimeout(() => startService(svc), 3000);
    }
  });

  console.log(`[launcher] Started ${svc.name} (cwd: ${cwd})`);
  return proc;
}

services.forEach(startService);

process.on('SIGTERM', () => process.exit(0));
process.on('SIGINT',  () => process.exit(0));
