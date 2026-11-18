const { createClient } = require('@supabase/supabase-js');

const ARK_API_URL = 'https://ark.cn-beijing.volces.com/api/v3/chat/completions';
const MODEL_NAME = process.env.DOUBAO_MODEL || 'doubao-seed-evolving';

const SYSTEM_PROMPT = `你是一位资深的中文起名顾问，精通诗词典故、音韵美学与名字寓意。
用户会告诉你起名需求（性别、风格、姓氏、期望含义等）。
你必须给出 3 组名字方案，每组严格按下面格式输出（用 Markdown）：

### 方案一：某某某
- **读音**：
- **寓意**：
- **适合**：

三个方案之间用空行分隔，不要输出其它多余内容，不要解释你在做什么。`;

function getSupabase() {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_ANON_KEY;
  if (!url || !key) return null;
  return createClient(url, key);
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return;
  }
  if (req.method !== 'POST') {
    res.status(405).json({ error: '只支持 POST 请求' });
    return;
  }

  let body = req.body;
  if (typeof body === 'string') {
    try {
      body = JSON.parse(body);
    } catch (e) {
      body = {};
    }
  }
  const requirement = body && body.requirement ? String(body.requirement).trim() : '';

  if (!requirement) {
    res.status(400).json({ error: '请先填写起名需求' });
    return;
  }
  if (requirement.length > 500) {
    res.status(400).json({ error: '需求请控制在 500 字以内' });
    return;
  }

  const apiKey = process.env.DOUBAO_API_KEY;
  if (!apiKey) {
    res.status(500).json({ error: '服务端未配置 DOUBAO_API_KEY 环境变量' });
    return;
  }

  // SSE 响应头
  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');

  const send = (obj) => {
    res.write(`data: ${JSON.stringify(obj)}\n\n`);
  };

  let fullText = '';

  try {
    const upstream = await fetch(ARK_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: MODEL_NAME,
        stream: true,
        temperature: 0.8,
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user', content: `起名需求：${requirement}` },
        ],
      }),
    });

    if (!upstream.ok) {
      const errText = await upstream.text();
      send({ error: `模型调用失败（${upstream.status}）：${errText.slice(0, 300)}` });
      res.end();
      return;
    }

    const reader = upstream.body.getReader();
    const decoder = new TextDecoder('utf-8');
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();

      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || !trimmed.startsWith('data:')) continue;
        const payload = trimmed.slice(5).trim();
        if (payload === '[DONE]') continue;

        try {
          const json = JSON.parse(payload);
          const delta = json.choices && json.choices[0] && json.choices[0].delta
            ? json.choices[0].delta.content
            : '';
          if (delta) {
            fullText += delta;
            send({ text: delta });
          }
        } catch (e) {
          // 忽略不完整的分片
        }
      }
    }

    send({ done: true, model: MODEL_NAME });

    // 流式结束后写入 Supabase 历史记录
    const supabase = getSupabase();
    if (supabase && fullText) {
      try {
        const { error } = await supabase.from('name_history').insert([
          {
            user_requirement: requirement,
            name_result: fullText,
            model_name: MODEL_NAME,
          },
        ]);
        if (error) console.error('写入历史失败：', error.message);
      } catch (e) {
        console.error('写入历史失败：', e.message, e.cause && e.cause.message);
      }
    }

    res.end();
  } catch (err) {
    send({ error: err.message || '服务异常' });
    res.end();
  }
};
