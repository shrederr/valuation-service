const { DataSource } = require('typeorm');
const ds = new DataSource({
  type: 'postgres',
  host: 'localhost',
  port: 5433,
  database: 'valuation',
  username: 'postgres',
  password: 'postgres',
});

ds.initialize().then(async () => {
  const result = await ds.query("SELECT * FROM aggregator_import WHERE external_url LIKE '%IDUkpAQ%' LIMIT 1");
  if (result[0]) {
    const obj = result[0];

    console.log('========== ОСНОВНІ ПОЛЯ ==========');
    console.log('id:', obj.id);
    console.log('deal_type:', obj.deal_type);
    console.log('realty_type:', obj.realty_type);
    console.log('realty_platform:', obj.realty_platform);
    console.log('geo_id:', obj.geo_id);
    console.log('street_id:', obj.street_id);
    console.log('topzone_id:', obj.topzone_id);
    console.log('complex_id:', obj.complex_id);
    console.log('house_number:', obj.house_number);
    console.log('lat:', obj.lat);
    console.log('lng:', obj.lng);
    console.log('price:', obj.price);
    console.log('is_active:', obj.is_active);
    console.log('external_url:', obj.external_url);
    console.log('');

    console.log('========== ATTRIBUTES ==========');
    const attrs = JSON.parse(obj.attributes || '{}');
    console.log(JSON.stringify(attrs, null, 2));
    console.log('');

    console.log('========== DESCRIPTION ==========');
    const desc = JSON.parse(obj.description || '{}');
    console.log(JSON.stringify(desc, null, 2));
    console.log('');

    console.log('========== PRIMARY_DATA ==========');
    const pd = JSON.parse(obj.primary_data || '{}');
    console.log(JSON.stringify(pd, null, 2));
  }
  await ds.destroy();
}).catch(e => console.error('Error:', e.message));
