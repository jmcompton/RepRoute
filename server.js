require('dotenv').config();
const express = require('express');
const path = require('path');
const session = require('express-session');
const pgSession = require('connect-pg-simple')(session);
const { pool, initDB } = require('./db');

const authRoutes = require('./routes/auth');
const dashRoutes = require('./routes/dashboard');
const prospectsRoutes = require('./routes/prospects');
const callsRoutes = require('./routes/calls');
const aiRoutes = require('./routes/ai');
const onboardingRoutes = require('./routes/onboarding');
const weeklyRoutes = require('./routes/weekly');
const managerRoutes = require('./routes/manager');
const calendarRoutes = require('./routes/calendar');
const { router: emailRoutes } = require('./routes/email');
const samplesRoutes = require('./routes/samples');
const adminRoutes = require('./routes/admin');
const placesRoutes = require('./routes/places');
const morningRoutes = require('./routes/morning');
const { router: notificationsRoutes, evaluateForUser } = require('./routes/notifications');
const { router: brandMappingsRoutes } = require('./routes/brand_mappings');
const quotesRoutes = require('./routes/quotes');
const zohoRoutes   = require('./routes/zoho');
const voiceRoutes  = require('./routes/voice');

const app = express();
app.use(express.json({ limit: '25mb' }));
app.use(express.urlencoded({ extended: true, limit: '25mb' }));
app.use(express.static(path.join(__dirname, 'public')));

app.use(session({
  store: new pgSession({ pool, tableName: 'sessions' }),
  secret: process.env.SESSION_SECRET || 'reproute-secret-2025',
  resave: false, saveUninitialized: false,
  rolling: true,  // reset expiry on every response — session stays alive as long as rep is active
  cookie: { maxAge: 7 * 24 * 60 * 60 * 1000 }  // 7 days; resets on every request with rolling:true
}));

app.use((req, res, next) => { res.locals.user = req.session.user || null; next(); });

function requireAuth(req, res, next) { if (!req.session.user) return res.redirect('/'); next(); }
function requireManager(req, res, next) { if (!req.session.user || req.session.user.role !== 'manager') return res.redirect('/app'); next(); }

// ── Domain routing ──────────────────────────────────────────────
// comptongroupllc.com / www.comptongroupllc.com  → company homepage
// app.comptongroupllc.com                         → RepRoute (normal)
// localhost / any other host                      → RepRoute (normal)
function isCompanyDomain(req) {
  const host = (req.hostname || '').toLowerCase().replace(/^www\./, '');
  return host === 'comptongroupllc.com';
}

app.get('/', (req, res) => {
  if (isCompanyDomain(req)) {
    return res.sendFile(path.join(__dirname, 'public', 'compton-home.html'));
  }
  if (req.session.user) return res.redirect('/app');
  res.sendFile(path.join(__dirname, 'views', 'login.html'));
});

// POST /api/contact — Compton Group LLC contact form
app.post('/api/contact', async (req, res) => {
  try {
    const { name, email, subject, message } = req.body;
    if (!name || !email || !message) {
      return res.status(400).json({ error: 'Name, email, and message are required.' });
    }
    const emailRx = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRx.test(email)) {
      return res.status(400).json({ error: 'Please enter a valid email address.' });
    }
    // Log the submission
    console.log(`[contact] ${new Date().toISOString()} | from=${email} | name=${name} | subject=${subject || 'none'}`);
    console.log(`[contact] message: ${message}`);
    // TODO: Send email notification via SendGrid or Nodemailer
    // e.g. sgMail.send({ to: 'hello@comptongroupllc.com', from: 'noreply@comptongroupllc.com', subject, text: message })
    res.json({ success: true, message: 'Message received' });
  } catch (e) {
    console.error('[contact] error:', e.message);
    res.status(500).json({ error: 'Failed to send message. Please try again.' });
  }
});

app.get('/app', requireAuth, (req, res) => res.sendFile(path.join(__dirname, 'views', 'app.html')));
app.get('/morning', requireAuth, (req, res) => res.sendFile(path.join(__dirname, 'views', 'morning.html')));
app.get('/landing', (req, res) => res.sendFile(path.join(__dirname, 'views', 'landing.html')));

app.post('/request-access', async (req, res) => {
  const { name, email, company, reason } = req.body;
  if (!name || !email) return res.json({ error: 'Name and email required' });
  try {
    await pool.query('INSERT INTO access_requests (name, email, company, reason) VALUES ($1,$2,$3,$4)',
      [name, email, company || '', reason || '']);
    res.json({ ok: true });
  } catch (e) {
    if (e.code === '23505') return res.json({ error: 'This email already submitted a request' });
    res.json({ error: 'Failed to submit request' });
  }
});

app.get('/admin', (req, res) => {
  if (!req.session.user || req.session.user.role !== 'manager') return res.redirect('/');
  res.sendFile(path.join(__dirname, 'views', 'admin.html'));
});
app.use('/admin', (req, res, next) => {
  if (!req.session.user || req.session.user.role !== 'manager') return res.status(403).json({ error: 'Forbidden' });
  next();
}, adminRoutes);

app.use('/', authRoutes);
app.use('/app', requireAuth, dashRoutes);
app.use('/api/prospects', requireAuth, prospectsRoutes);
app.use('/api/calls', requireAuth, callsRoutes);
app.use('/api/ai', requireAuth, aiRoutes);
app.use('/api/onboarding', requireAuth, onboardingRoutes);
app.use('/api/weekly', requireAuth, weeklyRoutes);
app.use('/api/manager', requireAuth, requireManager, managerRoutes);
app.use('/api/calendar', requireAuth, calendarRoutes);
app.use('/api/email', requireAuth, emailRoutes);
app.use('/api/samples', requireAuth, samplesRoutes);
app.use('/api/places', requireAuth, placesRoutes);
app.use('/api/morning', requireAuth, morningRoutes);
app.use('/api/notifications', requireAuth, notificationsRoutes);
app.use('/api/brand-mappings', requireAuth, brandMappingsRoutes);
app.use('/api/quotes', requireAuth, quotesRoutes);
app.use('/api/zoho',  requireAuth, zohoRoutes);
app.use('/api/voice', requireAuth, voiceRoutes);
app.get('/zoho-import', requireAuth, (req, res) =>
  res.sendFile(path.join(__dirname, 'views', 'zoho-import.html'))
);
app.use('/auth', emailRoutes);

app.get('/api/me', requireAuth, (req, res) => {
  res.json({ name: req.session.user.name, email: req.session.user.email, role: req.session.user.role });
});
app.get('/api/me/settings', requireAuth, async (req, res) => {
  try {
    const r = await pool.query('SELECT daily_call_goal FROM users WHERE id=$1', [req.session.user.id]);
    res.json({ daily_call_goal: r.rows[0]?.daily_call_goal || 10 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/api/me/settings', requireAuth, async (req, res) => {
  try {
    const goal = parseInt(req.body.daily_call_goal) || 10;
    await pool.query('UPDATE users SET daily_call_goal=$1 WHERE id=$2', [goal, req.session.user.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post('/app/update-territory', requireAuth, async (req, res) => {
  const { territory } = req.body;
  if (!territory) return res.status(400).json({ error: 'Territory required' });
  try {
    await pool.query('UPDATE users SET territory=$1 WHERE id=$2', [territory, req.session.user.id]);
    req.session.user.territory = territory;
    res.json({ ok: true, territory });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Daily notification evaluation - runs every hour, only fires once per user per day between 6-9am
const { pool: dbPool } = require('./db');
let lastEvaluation = {};
async function dailyEvaluation() {
  try {
    const today = new Date().toISOString().split('T')[0];
    const users = await dbPool.query('SELECT id FROM users');
    for (const u of users.rows) {
      if (lastEvaluation[u.id] === today) continue;
      const hour = new Date().getHours();
      if (hour >= 6 && hour <= 9) {
        await evaluateForUser(u.id);
        lastEvaluation[u.id] = today;
        console.log('Evaluated notifications for user', u.id);
      }
    }
  } catch(e) { console.error('Eval cron error:', e.message); }
}
setInterval(dailyEvaluation, 60 * 60 * 1000); // hourly
setTimeout(dailyEvaluation, 30 * 1000); // run 30s after boot

const PORT = process.env.PORT || 3000;
initDB().then(() => {
  app.listen(PORT, () => console.log(`RepRoute running on port ${PORT}`));
}).catch(err => { console.error('DB init failed:', err); process.exit(1); });
