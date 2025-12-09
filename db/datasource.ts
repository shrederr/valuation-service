import { DataSource } from 'typeorm';
import { config } from 'dotenv';

config();

export default new DataSource({
  type: 'postgres',
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5432', 10),
  username: process.env.DB_USERNAME || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  database: process.env.DB_DATABASE || 'valuation',
  entities: ['libs/database/src/entities/*.entity.ts'],
  migrations: ['db/migrations/*.ts'],
  ssl: process.env.DB_SSL === 'true'
    ? { rejectUnauthorized: false }
    : false,
});
