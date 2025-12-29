import { DataSource } from 'typeorm';

async function clearListings() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    console.error('DATABASE_URL is required');
    process.exit(1);
  }

  const ds = new DataSource({
    type: 'postgres',
    url,
    ssl: { rejectUnauthorized: false },
  });

  await ds.initialize();
  console.log('Connected to database');

  const countBefore = await ds.query('SELECT COUNT(*) as cnt FROM unified_listings');
  console.log(`Records before: ${countBefore[0].cnt}`);

  await ds.query('TRUNCATE unified_listings CASCADE');
  console.log('Table truncated');

  const countAfter = await ds.query('SELECT COUNT(*) as cnt FROM unified_listings');
  console.log(`Records after: ${countAfter[0].cnt}`);

  await ds.destroy();
  console.log('Done');
}

clearListings().catch(console.error);
