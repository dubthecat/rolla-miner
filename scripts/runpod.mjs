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
export async function createPod({ name, image, env = {}, ports = ['8080/http'], vcpu = 2, flavor = 'cpu3c', cloud = 'SECURE', diskGb = 10, cmd = null, entrypoint = null, publicIp = false, gpu = null }) {
  // gpu: a GPU type id (e.g. 'NVIDIA GeForce RTX 3090') makes this a GPU pod — the miner image is CPU-only but runs anywhere;
  // GPU hosts are what RunPod has most of when the CPU fleet in the default data centers is empty
  const compute = gpu ? { computeType: 'GPU', gpuTypeIds: [gpu], gpuCount: 1, minVCPUPerGPU: Math.max(2, vcpu) } : { computeType: 'CPU', cpuFlavorIds: [flavor], vcpuCount: vcpu };
  const body = { name, imageName: image, ...compute, cloudType: cloud, containerDiskInGb: diskGb, volumeInGb: 0, ports, env, supportPublicIp: publicIp || ports.some((p) => p.endsWith('/tcp')), ...(cmd ? { dockerStartCmd: cmd } : {}), ...(entrypoint ? { dockerEntrypoint: entrypoint } : {}) };
  const d = await call('POST', '/pods', body);
  return { id: d.id, name, costPerHr: d.costPerHr ?? null };
}
const GQL = 'https://api.runpod.io/graphql';
/// the REST object plus the GraphQL runtime: REST says what was asked for (desiredStatus), the runtime says what
/// is actually up (uptime, the public ip:port of every exposed tcp port — assigned only once the container runs)
export async function podInfo(id) {
  const rest = await call('GET', `/pods/${id}`);
  const q = `{ pod(input:{podId:"${id}"}) { id desiredStatus runtime { uptimeInSeconds ports { ip isIpPublic privatePort publicPort type } } } }`;
  const r = await fetch(GQL, { method: 'POST', headers: { authorization: `Bearer ${key()}`, 'content-type': 'application/json' }, body: JSON.stringify({ query: q }) });
  const d = await r.json().catch(() => ({})); const rt = d?.data?.pod?.runtime || null;
  return { ...rest, runtime: rt, uptime: rt?.uptimeInSeconds ?? -1, runtimePorts: rt?.ports || null };
}
export const listPods = async () => (await call('GET', '/pods')) || [];
export async function terminatePod(id) { try { await call('DELETE', `/pods/${id}`); return true; } catch (e) { if (/404|not found/i.test(e.message)) return false; throw e; } }

/// wait until the pod runs and its port mappings are known (RunPod assigns the public ip:port after boot)
export async function waitRunning(id, { timeoutMs = 300000, needPorts = [] } = {}) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    const p = await podInfo(id);
    const up = p.uptime > 0;   // desiredStatus is RUNNING from the moment the pod is rented; the runtime says when it actually is
    const ready = up && (!needPorts.length || needPorts.every((port) => { const e = endpoint(p, port); return e && !e.startsWith('https://'); }));
    if (ready) return p;
    await sleep(5000);
  }
  throw new Error(`pod ${id} not running with its ports after ${timeoutMs / 1000} s`);
}
/// the public address of a pod port: ip:port for a tcp mapping, https://<id>-<port>.proxy.runpod.net for http
export function endpoint(p, port) {
  const rp = p.runtimePorts || p.runtime?.ports || null;
  if (Array.isArray(rp)) { const e = rp.find((x) => Number(x.privatePort) === Number(port) && String(x.type || 'tcp').toLowerCase() === 'tcp' && x.isIpPublic !== false && x.ip); if (e) return `${e.ip}:${e.publicPort}`; }
  const m = p.portMappings;
  if (m && typeof m === 'object' && !Array.isArray(m)) { const pub = m[String(port)] ?? m[port]; if (pub && p.publicIp) return `${p.publicIp}:${pub}`; }
  return p.id ? `https://${p.id}-${port}.proxy.runpod.net` : null;
}
/// GET a miner's /metrics or /healthz through its proxy URL
export async function getJson(url, { timeoutMs = 15000 } = {}) {
  const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), timeoutMs);
  try { const r = await fetch(url, { signal: ctl.signal }); const txt = await r.text(); try { return JSON.parse(txt); } catch { return { raw: txt.slice(0, 20000), status: r.status }; } } finally { clearTimeout(t); }
}
/// kill every miner-*/broker-* pod (a failed run must never keep billing)
export async function sweep(prefixes = ['miner-', 'broker-', 'bench-', 'sim-']) {
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
