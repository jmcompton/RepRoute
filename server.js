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

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

app.use(session({
  store: new pgSession({ pool, tableName: 'sessions' }),
  secret: process.env.SESSION_SECRET || 'reproute-secret-2025',
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 7 * 24 * 60 * 60 * 1000 }
}));

app.use((req, res, next) => {
  res.locals.user = req.session.user || null;
  next();
});

function requireAuth(req, res, next) {
  if (!req.session.user) return res.redirect('/');
  next();
}
function requireManager(req, res, next) {
  if (!req.session.user || req.session.user.role !== 'manager') return res.redirect('/app');
  next();
}

app.get('/', (req, res) => {
  if (req.session.user) return res.redirect('/app');
  res.sendFile(path.join(__dirname, 'views', 'login.html'));
});

app.get('/app', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'views', 'app.html'));
});

app.use('/', authRoutes);
app.use('/app', requireAuth, dashRoutes);
app.use('/api/prospects', requireAuth, prospectsRoutes);
app.use('/api/calls', requireAuth, callsRoutes);
app.use('/api/ai', requireAuth, aiRoutes);
app.use('/api/onboarding', requireAuth, onboardingRoutes);
app.use('/api/weekly', requireAuth, weeklyRoutes);
app.use('/api/manager', requireAuth, requireManager, managerRoutes);
app.use('/api/calendar', requireAuth, calendarRoutes);

const PORT = process.env.PORT || 3000;
initDB().then(() => {
  app.listen(PORT, () => console.log(`RepRoute running on port ${PORT}`));
});

app.post('/app/update-territory', requireAuth, async (req, res) => {
  const { territory } = req.body;
  if (!territory) return res.status(400).json({ error: 'Territory required' });
  try {
    await pool.query('UPDATE users SET territory=$1 WHERE id=$2', [territory, req.session.user.id]);
    req.session.user.territory = territory;
    res.json({ ok: true, territory });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});
