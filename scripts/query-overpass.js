const https = require('https');

// Try finding ЖК in OSM using Overpass API
const query = `
[out:json][timeout:30];
(
  // Search for residential complexes near Odessa by name
  way["building"]["name"~"OZONE|Ménars|ITOWN|Cuvee|Аркадия|Гагарин|Море",i](46.3,30.4,46.8,31.1);
  relation["building"]["name"~"OZONE|Ménars|ITOWN|Cuvee|Аркадия|Гагарин|Море",i](46.3,30.4,46.8,31.1);
  way["landuse"="residential"]["name"](46.3,30.4,46.8,31.1);
  relation["landuse"="residential"]["name"](46.3,30.4,46.8,31.1);
);
out body;
>;
out skel qt;
`;

const url = 'https://overpass-api.de/api/interpreter';
const postData = 'data=' + encodeURIComponent(query);

const options = {
  hostname: 'overpass-api.de',
  path: '/api/interpreter',
  method: 'POST',
  headers: {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Content-Length': Buffer.byteLength(postData)
  }
};

console.log('Querying Overpass API...');

const req = https.request(options, (res) => {
  let data = '';
  res.on('data', chunk => data += chunk);
  res.on('end', () => {
    try {
      const json = JSON.parse(data);
      console.log('Found elements:', json.elements?.length || 0);

      const named = json.elements?.filter(e => e.tags?.name) || [];
      console.log('With names:', named.length);

      named.forEach(e => {
        console.log(`  - ${e.type} ${e.id}: ${e.tags.name}`);
        if (e.tags.building) console.log(`    building: ${e.tags.building}`);
      });
    } catch(e) {
      console.log('Parse error:', e.message);
      console.log('Response:', data.substring(0, 1000));
    }
  });
});

req.on('error', e => console.error('Request error:', e.message));
req.write(postData);
req.end();
