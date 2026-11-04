// Text the reps type (and business names that come back from Google) gets
// painted into the page in a lot of places. A name like "<img onerror=...>"
// would run as code in a teammate's browser. We neutralize anything that
// looks like the start of an HTML tag by swapping "<" for a look-alike "‹".
// "price < 5" is left alone; only "<" directly followed by a letter, "/",
// "!" or "?" changes. Big base64 fields are skipped.
const SKIP = new Set(['pdf_data', 'business_card_image', 'image', 'image_base64', 'audio', 'file', 'password']);
const TAG_START = /<(?=[a-zA-Z\/!?])/g;

function defang(v, key) {
  if (typeof v === 'string') {
    if (SKIP.has(key) || v.length > 200000) return v;
    // A website is shown as a link: never let it be a script link.
    if ((key === 'website' || key === 'url') && /^\s*(javascript|data|vbscript):/i.test(v)) return '';
    return v.replace(TAG_START, '‹');
  }
  if (Array.isArray(v)) return v.map(x => defang(x, key));
  if (v && typeof v === 'object') {
    for (const k of Object.keys(v)) v[k] = defang(v[k], k);
    return v;
  }
  return v;
}

function defangBody(req, res, next) {
  if (req.body && typeof req.body === 'object' && req.method !== 'GET') defang(req.body, '');
  next();
}

module.exports = { defang, defangBody };
