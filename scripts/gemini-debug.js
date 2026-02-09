const apiKey = "AIzaSyAbf3k_38nP6s1Q-m3V70WXe404BeHBsHg";

const testText = "Продам 2-комн квартиру на ул. Маршала Говорова 15, Одесса";

async function test() {
  console.log('Testing Gemini API...\n');

  const response = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${apiKey}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        contents: [{
          parts: [{
            text: `Извлеки улицу из текста объявления. Верни JSON: {"street": "название"} или {"street": null}

Текст: ${testText}`
          }]
        }],
        generationConfig: {
          temperature: 0,
          maxOutputTokens: 100,
        }
      })
    }
  );

  const data = await response.json();
  console.log('Full response:');
  console.log(JSON.stringify(data, null, 2));
}

test().catch(console.error);
