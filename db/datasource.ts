import { DataSource } from 'typeorm';
import { config } from 'dotenv';

config();

const requiredEnvVars = ['DB_HOST', 'DB_PORT', 'DB_USERNAME', 'DB_PASSWORD', 'DB_DATABASE'] as const;

for (const envVar of requiredEnvVars) {
  if (!process.env[envVar]) {
    throw new Error(`Missing required environment variable: ${envVar}`);
  }
}

export default new DataSource({
  type: 'postgres',
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT!, 10),
  username: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  entities: ['libs/database/src/entities/*.entity.ts'],
  migrations: ['db/migrations/*.ts'],
  ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false,
});
