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
    -- FIX 1 ROOT CAUSE: Sean Connery/Conroy → correct names
    -- Root cause: users.name was "Sean Connery"/"Sean Conroy" so the
    --   quotes GET query (SELECT q.*, u.name as rep_name) displayed it
    --   on every quote created by that user, overwriting rep_name.
    -- Fix: rename the user + scrub any quotes with that rep_name.
    -- These are safe idempotent UPDATEs — run on every deploy.
    -- ════════════════════════════════════════════════════════════
    UPDATE users
    SET name = 'Sean Compton'
    WHERE LOWER(TRIM(name)) IN ('sean connery', 'sean conroy', 'sean conro');

    UPDATE quotes
    SET rep_name = 'Ray Breedlove', updated_at = NOW()
    WHERE LOWER(TRIM(COALESCE(rep_name, ''))) IN ('sean connery', 'sean conroy', 'sean conro');

  `);
  console.log('Database initialized');
}

module.exports = { pool, initDB };
