// scripts/runpod.mjs — RunPod pods as test machines for miner clusters (REST API, CPU pods).
//   import { createPod, waitRunning, podInfo, endpoint, terminatePod, listPods } from './runpod.mjs'
// A pod = { id, name, publicIp, portMappings } once running; `endpoint(pod, port)` gives the public
// address of a '<port>/tcp' mapping (ip:port) or the proxy URL of a '<port>/http' one.
// Env: RUNPOD_API_KEY. Every pod this module creates is named `miner-*` or `broker-*` so a sweep can
// find and kill strays: `node scripts/runpod.mjs sweep` terminates every pod with those prefixes.
const API = 'https://rest.runpod.io/v1';
const key = () => { const k = process.env.RUNPOD_API_KEY; if (!k) throw new Error('RUNPOD_API_KEY missing'); return k; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function call(method, path, body) {
  const r = await fetch(API + path, { method, headers: { authorization: `Bearer ${key()}`, 'content-type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  const text = await r.text(); let d = null; try { d = JSON.parse(text); } catch {}
  if (!r.ok) throw new Error(`runpod ${method} ${path} → ${r.status} ${text.slice(0, 200)}`);
  return d;
}

/// a CPU pod from a public image. ports: ['9092/tcp'] gets a public ip:port, ['8080/http'] a proxy URL.
export async function createPod({ name, image, env = {}, ports = ['8080/http'], vcpu = 2, flavor = 'cpu3c', cloud = 'SECURE', diskGb = 10, cmd = null, entrypoint = null, publicIp = false }) {
  const body = { name, imageName: image, computeType: 'CPU', cpuFlavorIds: [flavor], vcpuCount: vcpu, cloudType: cloud, containerDiskInGb: diskGb, volumeInGb: 0, ports, env, supportPublicIp: publicIp || ports.some((p) => p.endsWith('/tcp')), ...(cmd ? { dockerStartCmd: cmd } : {}), ...(entrypoint ? { dockerEntrypoint: entrypoint } : {}) };
  const d = await call('POST', '/pods', body);
  return { id: d.id, name, costPerHr: d.costPerHr ?? null };
}
export const podInfo = (id) => call('GET', `/pods/${id}`);
export const listPods = async () => (await call('GET', '/pods')) || [];
export async function terminatePod(id) { try { await call('DELETE', `/pods/${id}`); return true; } catch (e) { if (/404|not found/i.test(e.message)) return false; throw e; } }

/// wait until the pod runs and its port mappings are known (RunPod assigns the public ip:port after boot)
export async function waitRunning(id, { timeoutMs = 300000, needPorts = [] } = {}) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    const p = await podInfo(id);
    const status = p.desiredStatus || p.status; const mappings = p.portMappings || p.runtime?.ports || null;
    const ready = status === 'RUNNING' && (!needPorts.length || needPorts.every((port) => endpoint({ ...p, portMappings: mappings }, port)));
    if (ready) return { ...p, portMappings: mappings };
    await sleep(5000);
  }
  throw new Error(`pod ${id} not running with its ports after ${timeoutMs / 1000} s`);
}
/// the public address of a pod port: ip:port for a tcp mapping, https://<id>-<port>.proxy.runpod.net for http
export function endpoint(p, port) {
  const m = p.portMappings;
  if (m && typeof m === 'object') { const pub = m[String(port)] ?? m[port]; if (pub && p.publicIp) return `${p.publicIp}:${pub}`; }
  if (Array.isArray(m)) { const e = m.find((x) => Number(x.privatePort) === Number(port)); if (e && (e.ip || p.publicIp)) return `${e.ip || p.publicIp}:${e.publicPort}`; }
  return p.id ? `https://${p.id}-${port}.proxy.runpod.net` : null;
}
/// GET a miner's /metrics or /healthz through its proxy URL
export async function getJson(url, { timeoutMs = 15000 } = {}) {
  const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), timeoutMs);
  try { const r = await fetch(url, { signal: ctl.signal }); const txt = await r.text(); try { return JSON.parse(txt); } catch { return { raw: txt.slice(0, 2000), status: r.status }; } } finally { clearTimeout(t); }
}
/// kill every miner-*/broker-* pod (a failed run must never keep billing)
export async function sweep(prefixes = ['miner-', 'broker-']) {
  const pods = await listPods(); let n = 0;
  for (const p of pods) if (prefixes.some((x) => String(p.name || '').startsWith(x))) { await terminatePod(p.id); n++; console.log('terminated', p.id, p.name); }
  return n;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const cmd = process.argv[2];
  if (cmd === 'list') console.log(JSON.stringify(await listPods(), null, 1).slice(0, 4000));
  else if (cmd === 'sweep') console.log('swept', await sweep());
  else if (cmd === 'info') console.log(JSON.stringify(await podInfo(process.argv[3]), null, 1).slice(0, 3000));
  else console.log('usage: node scripts/runpod.mjs list | info <id> | sweep');
}
