/**
 * Worker thread for ONNX embedding generation.
 * Uses @huggingface/transformers to run paraphrase-multilingual-mpnet-base-v2 locally.
 * Receives { id, text } messages, returns { id, embedding } or { id, error }.
 */
import { parentPort } from 'worker_threads';

let embedder: any = null;

async function init() {
  const { pipeline, env } = await import('@huggingface/transformers');
  if (process.env.TRANSFORMERS_CACHE) {
    env.cacheDir = process.env.TRANSFORMERS_CACHE;
  }
  env.allowLocalModels = true;

  embedder = await pipeline(
    'feature-extraction',
    'Xenova/paraphrase-multilingual-mpnet-base-v2',
    { dtype: 'fp32' },
  );
}

const initPromise = init().catch((err) => {
  console.error('Worker init error:', err);
});

parentPort?.on('message', async (msg: { id: string; text: string }) => {
  try {
    await initPromise;
    if (!embedder) {
      parentPort?.postMessage({ id: msg.id, error: 'Model not loaded' });
      return;
    }

    const result = await embedder(msg.text, {
      pooling: 'mean',
      normalize: true,
    });
    const embedding = Array.from(result.data as Float32Array);
    parentPort?.postMessage({ id: msg.id, embedding });
  } catch (err) {
    parentPort?.postMessage({
      id: msg.id,
      error: err instanceof Error ? err.message : String(err),
    });
  }
});
