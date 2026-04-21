const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT UNIQUE NOT NULL,
      password TEXT NOT NULL,
      role TEXT NOT NULL DEFAULT 'rep',
      territory TEXT DEFAULT 'Atlanta Metro',
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS prospects (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES users(id),
      company TEXT NOT NULL,
      category TEXT NOT NULL,
      city TEXT,
      state TEXT DEFAULT 'GA',
      phone TEXT,
      email TEXT,
      contact TEXT,
      website TEXT,
      products TEXT,
      status TEXT DEFAULT 'New',
      priority TEXT DEFAULT 'Medium',
      pipeline_stage TEXT DEFAULT 'New Lead',
      notes TEXT,
      source TEXT DEFAULT 'AI',
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS pipeline_stage TEXT DEFAULT 'New Lead';
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS email TEXT;

    CREATE TABLE IF NOT EXISTS calls (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES users(id),
      prospect_id INTEGER REFERENCES prospects(id),
      call_date DATE NOT NULL,
      call_type TEXT,
      outcome TEXT,
      products_discussed TEXT,
      next_step TEXT,
      next_step_date DATE,
      notes TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS onboarding_plans (
      id SERIAL PRIMARY KEY,
      rep_id INTEGER REFERENCES users(id),
      plan_json TEXT,
      created_by INTEGER REFERENCES users(id),
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS weekly_plans (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES users(id),
      week_start DATE NOT NULL,
      plan_json TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS sessions (
      sid VARCHAR NOT NULL COLLATE "default",
      sess JSON NOT NULL,
      expire TIMESTAMP(6) NOT NULL,
      CONSTRAINT sessions_pkey PRIMARY KEY (sid)
    );

    CREATE INDEX IF NOT EXISTS IDX_session_expire ON sessions(expire);

    CREATE TABLE IF NOT EXISTS calendar_events (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
      title TEXT NOT NULL,
      description TEXT DEFAULT '',
      start_time TIMESTAMPTZ NOT NULL,
      end_time TIMESTAMPTZ NOT NULL,
      event_type TEXT DEFAULT 'general',
      location TEXT DEFAULT '',
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    ALTER TABLE users ADD COLUMN IF NOT EXISTS ical_token TEXT;
  `);
  console.log('Database initialized');
}

module.exports = { pool, initDB };
