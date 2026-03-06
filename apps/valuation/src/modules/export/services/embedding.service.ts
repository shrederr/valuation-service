import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Worker } from 'worker_threads';
import * as path from 'path';

interface PendingRequest {
  resolve: (embedding: number[]) => void;
  reject: (error: Error) => void;
}

@Injectable()
export class EmbeddingService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(EmbeddingService.name);
  private worker: Worker | null = null;
  private pending = new Map<string, PendingRequest>();
  private counter = 0;
  private ready = false;

  async onModuleInit() {
    try {
      this.startWorker();
      await this.embed('test');
      this.ready = true;
      this.logger.log('Embedding service ready (model loaded in worker thread)');
    } catch (error) {
      this.logger.warn(
        `Embedding service failed to initialize: ${error instanceof Error ? error.message : error}. ` +
        `Text-based dedup (Level 4) will be skipped.`,
      );
    }
  }

  onModuleDestroy() {
    this.worker?.terminate();
    this.worker = null;
  }

  isReady(): boolean {
    return this.ready;
  }

  async embed(text: string): Promise<number[]> {
    if (!this.worker) {
      throw new Error('Embedding worker not started');
    }

    const id = String(++this.counter);

    return new Promise<number[]>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error('Embedding timeout (30s)'));
      }, 30000);

      this.pending.set(id, {
        resolve: (embedding) => {
          clearTimeout(timeout);
          resolve(embedding);
        },
        reject: (error) => {
          clearTimeout(timeout);
          reject(error);
        },
      });

      this.worker!.postMessage({ id, text });
    });
  }

  toVectorString(embedding: number[]): string {
    return `[${embedding.join(',')}]`;
  }

  private startWorker() {
    const workerPath = path.join(__dirname, 'embedding.worker.js');
    this.worker = new Worker(workerPath, {
      env: {
        ...process.env,
        TRANSFORMERS_CACHE: process.env.TRANSFORMERS_CACHE || '',
      },
    });

    this.worker.on('message', (msg: { id: string; embedding?: number[]; error?: string }) => {
      const pending = this.pending.get(msg.id);
      if (!pending) return;
      this.pending.delete(msg.id);

      if (msg.error) {
        pending.reject(new Error(msg.error));
      } else {
        pending.resolve(msg.embedding!);
      }
    });

    this.worker.on('error', (error) => {
      this.logger.error(`Worker error: ${error.message}`);
      for (const [, req] of this.pending) {
        req.reject(error);
      }
      this.pending.clear();
    });

    this.worker.on('exit', (code) => {
      if (code !== 0) {
        this.logger.error(`Worker exited with code ${code}`);
      }
      this.worker = null;
      this.ready = false;
    });
  }
}
