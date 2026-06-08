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

    CREATE TABLE IF NOT EXISTS samples (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
      prospect_id INTEGER REFERENCES prospects(id) ON DELETE CASCADE,
      product_line TEXT NOT NULL,
      quantity INTEGER DEFAULT 1,
      notes TEXT DEFAULT '',
      sent_date DATE NOT NULL,
      follow_up_date DATE,
      status TEXT DEFAULT 'pending',
      outcome_notes TEXT DEFAULT '',
      closed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS access_requests (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL,
      company TEXT DEFAULT '',
      reason TEXT DEFAULT '',
      status TEXT DEFAULT 'pending',
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    ALTER TABLE users ADD COLUMN IF NOT EXISTS outlook_access_token TEXT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS outlook_refresh_token TEXT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS outlook_token_expiry TIMESTAMPTZ;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS daily_call_goal INTEGER DEFAULT 10;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS address TEXT;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS google_place_id TEXT;
    CREATE INDEX IF NOT EXISTS idx_prospects_place_id ON prospects(user_id, google_place_id);
    CREATE INDEX IF NOT EXISTS idx_prospects_company_lower ON prospects(user_id, LOWER(company));

    CREATE TABLE IF NOT EXISTS brand_mappings (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      brand TEXT NOT NULL,
      channel TEXT NOT NULL,
      customer_types JSONB NOT NULL DEFAULT '[]'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(user_id, brand, channel)
    );

    CREATE TABLE IF NOT EXISTS notifications (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      prospect_id INTEGER REFERENCES prospects(id) ON DELETE CASCADE,
      type TEXT NOT NULL,
      urgency TEXT NOT NULL DEFAULT 'today',
      title TEXT NOT NULL,
      body TEXT,
      action_url TEXT,
      unique_key TEXT UNIQUE NOT NULL,
      status TEXT NOT NULL DEFAULT 'unread',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      read_at TIMESTAMPTZ,
      dismissed_at TIMESTAMPTZ,
      acted_at TIMESTAMPTZ
    );

    CREATE INDEX IF NOT EXISTS idx_notif_user_status ON notifications(user_id, status);
    CREATE INDEX IF NOT EXISTS idx_notif_created ON notifications(created_at DESC);

    CREATE TABLE IF NOT EXISTS email_logs (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
      prospect_id INTEGER REFERENCES prospects(id) ON DELETE CASCADE,
      direction TEXT DEFAULT 'out',
      subject TEXT,
      body TEXT,
      to_email TEXT,
      from_email TEXT,
      sent_at TIMESTAMPTZ DEFAULT NOW(),
      source TEXT DEFAULT 'manual'
    );

    -- Lead gen improvements: opportunity scoring + rep home base
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS opportunity_score INTEGER DEFAULT 5;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS channel TEXT DEFAULT 'Contractor';
    ALTER TABLE users ADD COLUMN IF NOT EXISTS home_base_lat NUMERIC;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS home_base_lng NUMERIC;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS home_base_city TEXT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS radius_miles INTEGER DEFAULT 50;

    -- Quotes table
    CREATE TABLE IF NOT EXISTS quotes (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      rep_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      rep_name TEXT,
      quote_number TEXT,
      status TEXT NOT NULL DEFAULT 'Draft',
      account_name TEXT NOT NULL,
      contact_name TEXT,
      amount NUMERIC(12,2),
      products TEXT,
      comments TEXT,
      quote_date DATE,
      follow_up_date DATE,
      pdf_filename TEXT,
      pdf_data TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_quotes_user ON quotes(user_id);
    CREATE INDEX IF NOT EXISTS idx_quotes_status ON quotes(status);
    CREATE INDEX IF NOT EXISTS idx_quotes_followup ON quotes(follow_up_date);
    ALTER TABLE quotes ADD COLUMN IF NOT EXISTS rep_name TEXT;

    -- ════════════════════════════════════════════════════════════
    -- CRM Intelligence Upgrade: data_status + company_type
    -- data_status: Unvetted | Contacted | Verified CRM Data
    -- company_type: Distributor | Contractor
    -- These columns exist permanently — no re-query dependency
    -- ════════════════════════════════════════════════════════════
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS data_status TEXT DEFAULT 'Unvetted';
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS company_type TEXT DEFAULT 'Contractor';
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS manufacturer_assoc TEXT;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS manager_notes TEXT;

    CREATE INDEX IF NOT EXISTS idx_prospects_data_status ON prospects(user_id, data_status);
    CREATE INDEX IF NOT EXISTS idx_prospects_company_type ON prospects(user_id, company_type);

    -- Auto-populate company_type from existing category data (one-time migration)
    UPDATE prospects
    SET company_type = 'Distributor'
    WHERE company_type IS NULL OR company_type = 'Contractor'
      AND category ILIKE ANY(ARRAY[
        '%distributor%','%dealer%','%supply%','%wholesale%',
        '%building material%','%lumber%'
      ]);

    -- Auto-populate data_status = 'Contacted' for prospects that have call records
    UPDATE prospects p
    SET data_status = 'Contacted'
    WHERE (data_status IS NULL OR data_status = 'Unvetted')
      AND EXISTS (SELECT 1 FROM calls c WHERE c.prospect_id = p.id);

    -- ════════════════════════════════════════════════════════════
    -- Zoho CRM Import: zoho_id dedup key + import_history table
    -- ════════════════════════════════════════════════════════════
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS zoho_id TEXT;
    CREATE INDEX IF NOT EXISTS idx_prospects_zoho_id ON prospects(zoho_id) WHERE zoho_id IS NOT NULL;

    -- Business Card Scanner: store scanned card image on the prospect record
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS business_card_image TEXT;

    -- Voice Logger confirm card: extended contact fields
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS title  TEXT;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS mobile TEXT;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS zip    TEXT;

    CREATE TABLE IF NOT EXISTS import_history (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      source TEXT NOT NULL DEFAULT 'zoho',
      contacts_imported INTEGER DEFAULT 0,
      contacts_skipped INTEGER DEFAULT 0,
      accounts_imported INTEGER DEFAULT 0,
      accounts_skipped INTEGER DEFAULT 0,
      total_imported INTEGER GENERATED ALWAYS AS (contacts_imported + accounts_imported) STORED,
      imported_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- ════════════════════════════════════════════════════════════
    -- FIX: correct salesperson name — Sean Conroy (not Sean Compton)
    -- The prior migration had the rename direction backwards.
    -- These are safe idempotent UPDATEs — run on every deploy.
    -- ════════════════════════════════════════════════════════════
    UPDATE users
    SET name = 'Sean Conroy'
    WHERE LOWER(TRIM(name)) = 'sean compton';

    UPDATE quotes
    SET rep_name = 'Sean Conroy', updated_at = NOW()
    WHERE LOWER(TRIM(COALESCE(rep_name, ''))) IN ('sean compton', 'sean connery', 'sean conro');

    -- ════════════════════════════════════════════════════════════
    -- Personal Time Tracker — JohnMark Compton only
    -- ════════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS time_sessions (
      id               SERIAL PRIMARY KEY,
      user_id          INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      start_time       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      end_time         TIMESTAMPTZ,
      duration_minutes INTEGER,
      description      TEXT DEFAULT '',
      created_at       TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_time_sessions_user ON time_sessions(user_id, start_time DESC);

    -- Unique constraint required for ON CONFLICT DO NOTHING on (user_id, start_time)
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_time_sessions_user_start'
      ) THEN
        ALTER TABLE time_sessions
          ADD CONSTRAINT uq_time_sessions_user_start UNIQUE (user_id, start_time);
      END IF;
    END $$;

    -- Seed: historical session for June 4, 2026 (9 AM–1 PM, 240 min)
    -- ON CONFLICT DO NOTHING is safe once the unique constraint exists
    INSERT INTO time_sessions (user_id, start_time, end_time, duration_minutes, description)
    SELECT u.id,
           '2026-06-04 09:00:00+00'::timestamptz,
           '2026-06-04 13:00:00+00'::timestamptz,
           240,
           'Voice call logger, business card scanner, contact detail page, mobile optimization, invoice generation, board meeting PDF'
    FROM users u
    WHERE u.email = 'johnmarkcompton@gmail.com'
    ON CONFLICT (user_id, start_time) DO NOTHING;

    -- ════════════════════════════════════════════════════════════
    -- Weekly Report: AI-generated per-rep activity reports built off
    -- logged calls. period_type = 'week' (Mon–Fri) or 'month'.
    -- ai_sections JSON: { summary_takeaways, follow_ups, opportunities, risks }
    -- activity_stats JSON: { total_calls, calls_per_day, calls_by_line,
    --                        calls_by_outcome, accounts_touched }
    -- UNIQUE (user_id, period_type, period_start) makes regeneration
    -- idempotent (overwrite, never duplicate). Idempotent migration.
    -- ════════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS weekly_reports (
      id            SERIAL PRIMARY KEY,
      user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      period_type   TEXT NOT NULL DEFAULT 'week',
      period_start  DATE NOT NULL,
      period_end    DATE NOT NULL,
      generated_at  TIMESTAMPTZ DEFAULT NOW(),
      ai_sections   JSONB NOT NULL DEFAULT '{}'::jsonb,
      activity_stats JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at    TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (user_id, period_type, period_start)
    );
    CREATE INDEX IF NOT EXISTS idx_weekly_reports_user ON weekly_reports(user_id, period_type, period_start DESC);

    -- ── Weekly Planner ─────────────────────────────────────────────
    -- Forward-looking plan: each rep plans stops + appointments per day.
    -- "visited" is NOT stored here — computed live from the calls table.
    CREATE TABLE IF NOT EXISTS planner_items (
      id           SERIAL PRIMARY KEY,
      rep_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      planned_date DATE NOT NULL,
      item_type    TEXT NOT NULL DEFAULT 'stop',   -- 'stop' | 'appointment'
      account_id   INTEGER REFERENCES prospects(id) ON DELETE SET NULL,
      title        TEXT,
      appt_time    TEXT,
      note         TEXT,
      sort_order   INTEGER NOT NULL DEFAULT 0,
      created_at   TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_planner_items_rep_date ON planner_items(rep_id, planned_date);

  `);
  console.log('Database initialized');
}

module.exports = { pool, initDB };
