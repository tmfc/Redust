import assert from 'node:assert/strict';
import { createClient } from 'redis';

function uniq(prefix) {
  return `${prefix}:${Date.now()}:${Math.random().toString(16).slice(2)}`;
}

function getRedisUrl() {
  const addr = process.env.REDUST_ADDR ?? '127.0.0.1:6379';
  const [host, port] = addr.split(':');
  return `redis://${host}:${port}`;
}

async function withClient(fn) {
  const url = getRedisUrl();
  const client = createClient({ url });
  client.on('error', (err) => {
    // node-redis 在连接阶段可能会 emit error；这里直接抛出让测试失败。
    throw err;
  });
  await client.connect();
  try {
    await fn(client);
  } finally {
    await client.quit();
  }
}

async function testBasicCommands() {
  await withClient(async (client) => {
    const pong = await client.ping();
    assert.equal(pong, 'PONG');

    const key = uniq('node:basic:key');

    const ok = await client.set(key, 'bar');
    assert.equal(ok, 'OK');

    const v = await client.get(key);
    assert.equal(v, 'bar');

    const cntKey = uniq('node:basic:cnt');
    await client.set(cntKey, '0');

    const v1 = await client.incr(cntKey);
    assert.equal(v1, 1);

    const v2 = await client.incrBy(cntKey, 5);
    assert.equal(v2, 6);

    await client.del(key, cntKey);
  });
}

async function testPipeline() {
  await withClient(async (client) => {
    const key = uniq('node:pipe:key');
    const cntKey = uniq('node:pipe:cnt');

    // MULTI 在 node-redis 里通常用来做 pipeline（execAsPipeline=true）
    const tx = client.multi();
    tx.ping();
    tx.set(key, 'v');
    tx.incr(cntKey);
    tx.get(key);

    const res = await tx.exec(true);
    assert.equal(Array.isArray(res), true);
    assert.equal(res.length, 4);

    // node-redis 的 execAsPipeline=true 在不同配置/版本下可能返回：
    // - 值数组：['PONG','OK',1,'v']
    // - 二元组数组：[[err,value], ...]
    const normalized = Array.isArray(res[0]) ? res.map(([err, val]) => {
      assert.equal(err, null);
      return val;
    }) : res;

    assert.equal(normalized[0], 'PONG');
    assert.equal(normalized[1], 'OK');
    assert.equal(normalized[2], 1);
    assert.equal(normalized[3], 'v');

    await client.del(key, cntKey);
  });
}

async function main() {
  await testBasicCommands();
  await testPipeline();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
