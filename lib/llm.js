// One place to call a language model, so agents don't care which provider runs.
// If TOGETHER_API_KEY is set, requests go to Together AI (US-hosted, OpenAI-style
// API, cheaper per token). Otherwise they go to Anthropic with ANTHROPIC_API_KEY.
// Model names are env-configurable because provider catalogs change.
const fetch = require('node-fetch');

const TOGETHER_URL = 'https://api.together.xyz/v1/chat/completions';
const ANTHROPIC_URL = 'https://api.anthropic.com/v1/messages';

function provider() {
  if (process.env.TOGETHER_API_KEY) return 'together';
  if (process.env.ANTHROPIC_API_KEY) return 'anthropic';
  return null;
}

// complete({ system, prompt, maxTokens }) → text. Throws on any failure.
async function complete({ system, prompt, maxTokens = 700, timeoutMs = 25000 }) {
  const p = provider();
  if (!p) throw new Error('No AI provider configured');
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    if (p === 'together') {
      const r = await fetch(TOGETHER_URL, {
        method: 'POST', signal: ctl.signal,
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${process.env.TOGETHER_API_KEY}` },
        body: JSON.stringify({
          model: process.env.TOGETHER_MODEL || 'deepseek-ai/DeepSeek-V3',
          max_tokens: maxTokens, temperature: 0.4,
          messages: [system ? { role: 'system', content: system } : null, { role: 'user', content: prompt }].filter(Boolean)
        })
      });
      const j = await r.json();
      if (!r.ok || j.error) throw new Error((j.error && (j.error.message || j.error)) || `Together ${r.status}`);
      return (j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.content) || '';
    }
    const r = await fetch(ANTHROPIC_URL, {
      method: 'POST', signal: ctl.signal,
      headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({
        model: process.env.AGENT_MODEL || 'claude-haiku-4-5-20251001',
        max_tokens: maxTokens, system: system || undefined,
        messages: [{ role: 'user', content: prompt }]
      })
    });
    const j = await r.json();
    if (!r.ok || j.error) throw new Error((j.error && j.error.message) || `Anthropic ${r.status}`);
    return (j.content || []).filter(b => b.type === 'text').map(b => b.text).join('');
  } finally {
    clearTimeout(timer);
  }
}

// Pull the first JSON object out of a model reply.
function parseJson(text) {
  const s = String(text || '');
  const a = s.indexOf('{'), b = s.lastIndexOf('}');
  if (a < 0 || b <= a) throw new Error('No JSON in reply');
  return JSON.parse(s.slice(a, b + 1));
}

module.exports = { complete, parseJson, provider };
