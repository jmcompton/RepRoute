const { Pool, types } = require('pg');

// DATE columns (call_date, quote_date, next_step_date...) are calendar days.
// The default parser turns '2026-11-04' into midnight in the server's zone
// (UTC on Railway), which a browser in Atlanta or Birmingham shows as Nov 3.
// Noon UTC lands on the same calendar day in every US time zone, and
// toISOString().slice(0,10) still gives back '2026-11-04'.
types.setTypeParser(1082, v => (v == null ? null : new Date(v + 'T12:00:00Z')));
const fs = require('fs');
const path = require('path');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// The team works Eastern (Atlanta) and Central (Birmingham). Run every DB
// session on Eastern time so CURRENT_DATE, NOW()::date and to_char() times
// match the reps' day instead of flipping to tomorrow at 7 PM (UTC midnight).
pool.on('connect', client => {
  client.query("SET TIME ZONE 'America/New_York'").catch(e => console.error('[db] set time zone:', e.message));
});

// Minimal RFC-4180 CSV parser (handles quoted fields with embedded commas /
// newlines / doubled quotes). Returns an array of objects keyed by header name.
function parseCsv(text) {
  const out = [];
  let field = '', row = [], inQ = false;
  text = String(text || '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) {
      if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else inQ = false; }
      else field += c;
    } else if (c === '"') { inQ = true; }
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); out.push(row); row = []; field = ''; }
    else field += c;
  }
  if (field.length || row.length) { row.push(field); out.push(row); }
  if (!out.length) return [];
  const headers = out[0].map(h => h.trim());
  return out.slice(1)
    .filter(r => r.some(c => (c || '').trim() !== ''))
    .map(r => { const o = {}; headers.forEach((h, idx) => { o[h] = (r[idx] != null ? r[idx] : '').trim(); }); return o; });
}

async function runOnce(name, fn) {
  const done = await pool.query('SELECT 1 FROM app_migrations WHERE name=$1', [name]);
  if (done.rows.length) return;
  const result = await fn();
  await pool.query('INSERT INTO app_migrations (name, result) VALUES ($1, $2) ON CONFLICT DO NOTHING', [name, JSON.stringify(result || {})]);
  console.log('[migrate] ran once:', name, JSON.stringify(result || {}));
}

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
    -- Managers can turn a login off (rep leaves) without deleting their history.
    ALTER TABLE users ADD COLUMN IF NOT EXISTS disabled_at TIMESTAMPTZ;
    -- "Last visit per account" and "my calls this week" run on every screen.
    CREATE INDEX IF NOT EXISTS idx_calls_prospect_date ON calls(prospect_id, call_date DESC, id DESC);
    CREATE INDEX IF NOT EXISTS idx_calls_user_date ON calls(user_id, call_date);
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
    -- Quote follow-up toggle: TRUE = follow up, FALSE = don't (no date), NULL = legacy.
    ALTER TABLE quotes ADD COLUMN IF NOT EXISTS follow_up_enabled BOOLEAN;
    -- Rep's threshold: quotes under this amount get no automatic follow-up.
    ALTER TABLE users ADD COLUMN IF NOT EXISTS quote_followup_min NUMERIC(12,2) DEFAULT 5000;
    ALTER TABLE quotes ADD COLUMN IF NOT EXISTS rep_name TEXT;

    -- ── Customer number is a SEPARATE field from quote_number. Reps reuse the
    --    same customer number across every quote for a given customer, so it must
    --    NEVER feed the duplicate-quote-number check. quote_number stays nullable
    --    (many quotes legitimately have none); the dup check keys on quote_number
    --    ONLY. This column is additive/idempotent — safe to re-run on prod.
    ALTER TABLE quotes ADD COLUMN IF NOT EXISTS customer_number TEXT;

    -- ── Duplicate quote numbers are LEGITIMATE (revisions, different customers,
    --    manufacturer-specific numbering). Relax any historical UNIQUE constraint
    --    or unique index on quote_number so the "Save anyway" override can persist
    --    a duplicate. Idempotent: a no-op when none exists. (Only UNIQUE
    --    constraints/indexes are touched — the id primary key is never affected.)
    DO $$
    DECLARE r RECORD;
    BEGIN
      FOR r IN
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        WHERE rel.relname = 'quotes'
          AND con.contype = 'u'
          AND pg_get_constraintdef(con.oid) ILIKE '%quote_number%'
      LOOP
        EXECUTE 'ALTER TABLE quotes DROP CONSTRAINT ' || quote_ident(r.conname);
      END LOOP;

      FOR r IN
        SELECT i.relname AS idxname
        FROM pg_index idx
        JOIN pg_class i ON i.oid = idx.indexrelid
        JOIN pg_class t ON t.oid = idx.indrelid
        WHERE t.relname = 'quotes'
          AND idx.indisunique
          AND NOT idx.indisprimary
          AND pg_get_indexdef(idx.indexrelid) ILIKE '%quote_number%'
      LOOP
        EXECUTE 'DROP INDEX ' || quote_ident(r.idxname);
      END LOOP;
    END $$;

    UPDATE quotes SET contact_name = regexp_replace(contact_name, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE contact_name ~ '<[a-zA-Z/!?]';
    UPDATE quotes SET products = regexp_replace(products, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE products ~ '<[a-zA-Z/!?]';
    UPDATE quotes SET comments = regexp_replace(comments, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE comments ~ '<[a-zA-Z/!?]';
    UPDATE planner_items SET note = regexp_replace(note, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE note ~ '<[a-zA-Z/!?]';
    UPDATE route_stops SET stops = regexp_replace(stops::text, '<([a-zA-Z/!?])', '‹\\1', 'g')::jsonb WHERE stops::text ~ '<[a-zA-Z/!?]';

    UPDATE prospects SET website = '' WHERE website ~* '^\\s*(javascript|data|vbscript):';
    -- Drive-route logging used to write statuses the edit screen doesn't have.
    UPDATE prospects SET status = CASE status WHEN 'Hot Lead' THEN 'Hot' WHEN 'Quoted' THEN 'Hot'
      WHEN 'Not Interested' THEN 'Cold' WHEN 'Sampled' THEN 'Warm' WHEN 'Contacted' THEN 'New' END
     WHERE status IN ('Hot Lead','Quoted','Not Interested','Sampled','Contacted');

    -- Keep a NON-unique index for quote_number lookups.
    CREATE INDEX IF NOT EXISTS idx_quotes_quote_number ON quotes(quote_number);

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
    WHERE (company_type IS NULL OR company_type = 'Contractor')
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
    -- Street-address geocode (set when an account's address is saved) so the
    -- planner can place it precisely instead of at its city centroid.
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS lng DOUBLE PRECISION;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS geocoded_at TIMESTAMPTZ;
    -- Phone-enrichment attempt marker: set whenever Places enrichment runs for an
    -- account (success OR miss) so unmatchable businesses leave the "missing phone"
    -- queue instead of being retried forever. Re-eligible after ~30 days.
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS enrich_attempted_at TIMESTAMPTZ;

    -- Commission-import contact lifecycle. Drives the account badge + the
    -- "Find missing info" enrichment queue:
    --   needs_info    – new account from an import, no phone/address yet (grey badge)
    --   ai_found      – Google Places returned a single clear result (amber "verify")
    --   needs_review  – Places found plausible results in DIFFERENT cities; user picks
    --   verified      – a human confirmed the contact info (green badge)
    -- NULL = not part of this flow (manual/legacy accounts are unaffected).
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS contact_status TEXT;
    -- Top Places candidates stashed when contact_status='needs_review' so the UI
    -- can let the user choose the right city without re-querying Places.
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS enrich_candidates JSONB;
    -- Medium-confidence import match ("review match"): the candidate EXISTING
    -- account this new account MIGHT be a duplicate of. Set means the blue
    -- "Review match" badge shows so the user confirms (merge) or rejects (keep new).
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS match_review_candidate TEXT;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS match_review_candidate_id INTEGER;

    -- Reconnect tab: per-account re-engagement controls. snoozed_until hides an
    -- account from the lapsed list until that moment (then it returns); dismissed
    -- removes it from tracking entirely ("not a real account"). Both additive.
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS reconnect_snoozed_until TIMESTAMPTZ;
    ALTER TABLE prospects ADD COLUMN IF NOT EXISTS reconnect_dismissed BOOLEAN DEFAULT FALSE;

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

    -- ── Route stops (Today's Route / "+ Route") ─────────────────────
    -- One row per user so the route syncs across desktop and phone.
    CREATE TABLE IF NOT EXISTS route_stops (
      user_id    INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      stops      JSONB NOT NULL DEFAULT '[]'::jsonb,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- ── Duplicate-account review: groups the rep chose to skip ────────
    -- key = lib/account-key.js accountKey() of the account name.
    CREATE TABLE IF NOT EXISTS account_dupe_skips (
      user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      key        TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (user_id, key)
    );

    -- ── Tasks page ──────────────────────────────────────────────────
    -- client_id = the task's old browser id, so the one-time upload of
    -- localStorage tasks can be retried without duplicating.
    CREATE TABLE IF NOT EXISTS tasks (
      id         SERIAL PRIMARY KEY,
      user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      client_id  TEXT,
      title      TEXT NOT NULL,
      type       TEXT NOT NULL DEFAULT 'other',
      contact    TEXT,
      due        TEXT,
      done       BOOLEAN NOT NULL DEFAULT FALSE,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (user_id, client_id)
    );
    CREATE INDEX IF NOT EXISTS idx_tasks_user ON tasks(user_id, created_at DESC);

    -- ── Keep-on-my-leads ────────────────────────────────────────────
    -- Leads a rep kept on the Lead Finder after logging a call ("Called — follow up").
    CREATE TABLE IF NOT EXISTS kept_leads (
      user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      lead_key   TEXT NOT NULL,
      lead       JSONB NOT NULL,
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (user_id, lead_key)
    );

    -- Offline call queue idempotency key (route-stop call logging retries).
    ALTER TABLE calls ADD COLUMN IF NOT EXISTS client_ref TEXT;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_calls_client_ref ON calls(user_id, client_ref) WHERE client_ref IS NOT NULL;

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

    -- Weekly Planner AI assist: provenance + AI annotations (nullable, additive).
    ALTER TABLE planner_items ADD COLUMN IF NOT EXISTS source    TEXT DEFAULT 'manual';  -- 'manual' | 'ai' | 'route'
    -- Today's Route → planner sync: identifies which route stop a 'route' item came from.
    ALTER TABLE planner_items ADD COLUMN IF NOT EXISTS route_key TEXT;
    ALTER TABLE planner_items ADD COLUMN IF NOT EXISTS ai_reason TEXT;
    ALTER TABLE planner_items ADD COLUMN IF NOT EXISTS ai_prep   TEXT;

    -- Today view: authoritative "rep marked this stop done" flag (nullable, additive).
    ALTER TABLE planner_items ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP;

    -- Per-day MANUAL anchor cities for the Weekly Planner. One row per rep+day;
    -- only present when the rep has explicitly set/overridden the anchor. Auto
    -- anchors (first stop's city) are derived live and never stored here, so a
    -- manual anchor is never clobbered by auto behavior. Clearing = delete row.
    CREATE TABLE IF NOT EXISTS planner_anchors (
      rep_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      anchor_date DATE NOT NULL,
      city        TEXT NOT NULL,
      updated_at  TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (rep_id, anchor_date)
    );

    -- ════════════════════════════════════════════════════════════
    -- Commission Import + Account-Matching Engine
    -- Turns Trilogy "XtraReport" commission statements into clean,
    -- account-linked revenue data. account_id references prospects(id)
    -- (prospects IS the account store in RepRoute). Idempotent block.
    -- ════════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS commission_imports (
      id               SERIAL PRIMARY KEY,
      rep_name         TEXT,
      rep_id           INTEGER REFERENCES users(id) ON DELETE SET NULL,
      period_start     DATE,
      period_end       DATE,
      source_filename  TEXT,
      row_count        INTEGER,
      total_sales      NUMERIC(12,2),
      total_commission NUMERIC(12,2),
      status           TEXT NOT NULL DEFAULT 'pending_review'
                         CHECK (status IN ('pending_review','confirmed','discarded')),
      created_by       INTEGER REFERENCES users(id) ON DELETE SET NULL,
      created_at       TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS commission_lines (
      id                  SERIAL PRIMARY KEY,
      import_id           INTEGER NOT NULL REFERENCES commission_imports(id) ON DELETE CASCADE,
      rep_name            TEXT,
      manufacturer        TEXT,
      customer_raw        TEXT,
      customer_normalized TEXT,
      account_id          INTEGER REFERENCES prospects(id) ON DELETE SET NULL,
      sales_amount        NUMERIC(12,2),
      commission_amount   NUMERIC(12,2),
      period_start        DATE,
      period_end          DATE,
      is_adjustment       BOOLEAN DEFAULT FALSE,
      created_at          TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_commission_lines_import ON commission_lines(import_id);
    CREATE INDEX IF NOT EXISTS idx_commission_lines_account ON commission_lines(account_id);
    CREATE INDEX IF NOT EXISTS idx_commission_lines_norm ON commission_lines(customer_normalized);

    -- Per-rep learned map: once a raw customer (normalized) is linked to an
    -- account, future imports for that rep auto-match it. UNIQUE per rep so the
    -- same normalized string can resolve to different accounts across reps.
    CREATE TABLE IF NOT EXISTS commission_customer_map (
      id                  SERIAL PRIMARY KEY,
      user_id             INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      customer_normalized TEXT NOT NULL,
      account_id          INTEGER NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
      confidence          NUMERIC,
      created_at          TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (user_id, customer_normalized)
    );
    CREATE INDEX IF NOT EXISTS idx_commission_map_user ON commission_customer_map(user_id);

    -- ════════════════════════════════════════════════════════════
    -- Manufacturer "lines" — first-class product lines built from the
    -- commission data. A line is a deduped manufacturer/principal. Each
    -- confirmed commission_line is resolved to a line; account_lines is the
    -- per-account × per-line revenue rollup. Foundation for cross-sell and
    -- per-line reporting (later prompts). Idempotent block.
    -- ════════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS lines (
      id              SERIAL PRIMARY KEY,
      name            TEXT,
      normalized_name TEXT UNIQUE,
      status          TEXT DEFAULT 'active',
      category_hint   TEXT,
      created_at      TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS account_lines (
      id               SERIAL PRIMARY KEY,
      account_id       INTEGER REFERENCES prospects(id) ON DELETE CASCADE,
      line_id          INTEGER REFERENCES lines(id) ON DELETE CASCADE,
      total_sales      NUMERIC(12,2),
      total_commission NUMERIC(12,2),
      line_count       INTEGER,
      first_period     DATE,
      last_period      DATE,
      updated_at       TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (account_id, line_id)
    );
    CREATE INDEX IF NOT EXISTS idx_account_lines_account ON account_lines(account_id);
    CREATE INDEX IF NOT EXISTS idx_account_lines_line ON account_lines(line_id);

    -- ════════════════════════════════════════════════════════════
    -- Account contacts — the one-to-many people list under an account
    -- (prospect). Distinct from the single embedded "Contact Information"
    -- stored on the prospect row itself; this powers the Contacts
    -- related-list + "+ Add Contact" on the account detail page.
    -- ════════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS contacts (
      id          SERIAL PRIMARY KEY,
      prospect_id INTEGER NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
      name        TEXT NOT NULL,
      title       TEXT,
      phone       TEXT,
      email       TEXT,
      status      TEXT DEFAULT 'New',
      created_by  INTEGER REFERENCES users(id) ON DELETE SET NULL,
      created_at  TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_contacts_prospect ON contacts(prospect_id);
    -- is_primary marks the contact backfilled from a prospect's embedded contact
    -- (the historical single "Contact Information"). Used for idempotency + ordering.
    ALTER TABLE contacts ADD COLUMN IF NOT EXISTS is_primary BOOLEAN DEFAULT FALSE;

    ALTER TABLE commission_lines ADD COLUMN IF NOT EXISTS line_id INTEGER REFERENCES lines(id);
    CREATE INDEX IF NOT EXISTS idx_commission_lines_line ON commission_lines(line_id);

    -- Import-time account match outcome per commission line:
    --   matched – attached to a high-confidence existing account
    --   review  – medium confidence; account created NEW but flagged for the user
    --             to confirm/reject a merge into match_candidate_name
    --   new     – no confident match; brand-new account
    ALTER TABLE commission_lines ADD COLUMN IF NOT EXISTS match_status TEXT;
    ALTER TABLE commission_lines ADD COLUMN IF NOT EXISTS match_candidate_name TEXT;

    -- line_aliases makes manual merges durable: a normalized manufacturer string
    -- is pinned to a line_id so re-resolution / backfill never re-splits a merge.
    CREATE TABLE IF NOT EXISTS line_aliases (
      id SERIAL PRIMARY KEY,
      normalized_manufacturer TEXT UNIQUE,
      line_id INTEGER REFERENCES lines(id) ON DELETE CASCADE,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_line_aliases_line ON line_aliases(line_id);

    -- ════════════════════════════════════════════════════════════
    -- Fortress Railing Promo — a self-contained, pre-routed dealer-visit
    -- campaign (separate from the main Accounts list). Stops are seeded once
    -- from fortress_promo_routes.csv; reps log visits against them.
    -- UNIQUE(rep, company, address) makes the seed idempotent.
    -- ════════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS fortress_promo_stops (
      id          SERIAL PRIMARY KEY,
      rep         TEXT,
      day         TEXT,
      stop_order  INTEGER,
      company     TEXT,
      address     TEXT,
      city        TEXT,
      zip         TEXT,
      phone       TEXT,
      status      TEXT,
      source      TEXT,
      lat         DOUBLE PRECISION,
      lng         DOUBLE PRECISION,
      visited_at  TIMESTAMPTZ,
      outcome     TEXT,
      notes       TEXT,
      created_at  TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (rep, company, address)
    );
    CREATE INDEX IF NOT EXISTS idx_fortress_rep_day ON fortress_promo_stops(rep, day, stop_order);

  `);

  // ── One-time idempotent backfill: embedded prospect contact → contacts list ──
  // Every prospect that carries an embedded contact (name/email/title/mobile) gets
  // ONE primary row in the contacts table, so people live in a single source. The
  // NOT EXISTS guard (already-primary, or a name/email match) makes this safe to
  // re-run on every boot — it inserts only what's missing. Embedded columns are
  // left intact for safety; we just stop relying on them for display/creation.
  try {
    const mig = await pool.query(`
      INSERT INTO contacts (prospect_id, name, title, phone, email, status, created_by, is_primary, created_at)
      SELECT p.id,
             COALESCE(NULLIF(TRIM(p.contact), ''), 'Primary Contact'),
             NULLIF(TRIM(p.title), ''),
             COALESCE(NULLIF(TRIM(p.mobile), ''), NULLIF(TRIM(p.phone), '')),
             NULLIF(TRIM(p.email), ''),
             COALESCE(NULLIF(TRIM(p.status), ''), 'New'),
             p.user_id,
             TRUE,
             COALESCE(p.created_at, NOW())
        FROM prospects p
       WHERE (COALESCE(NULLIF(TRIM(p.contact), ''), '') <> ''
              OR COALESCE(NULLIF(TRIM(p.email),  ''), '') <> ''
              OR COALESCE(NULLIF(TRIM(p.title),  ''), '') <> ''
              OR COALESCE(NULLIF(TRIM(p.mobile), ''), '') <> '')
         AND NOT EXISTS (
              SELECT 1 FROM contacts c
               WHERE c.prospect_id = p.id
                 AND (c.is_primary = TRUE
                      OR (NULLIF(TRIM(p.contact), '') IS NOT NULL AND LOWER(TRIM(c.name)) = LOWER(TRIM(p.contact)))
                      OR (NULLIF(TRIM(p.email), '') IS NOT NULL AND c.email IS NOT NULL AND LOWER(TRIM(c.email)) = LOWER(TRIM(p.email))))
         )
      RETURNING id`);
    console.log('[migrate] embedded contacts → contacts list:', mig.rowCount, 'migrated');
  } catch (e) {
    console.error('[migrate] embedded-contacts backfill skipped:', e.message);
  }

  // Agent output cache: one morning brief per rep per day.
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS agent_briefs (
      user_id    INTEGER REFERENCES users(id) ON DELETE CASCADE,
      brief_date DATE NOT NULL,
      content    JSONB NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (user_id, brief_date))`);
  } catch (e) {
    console.error('[migrate] agent_briefs skipped:', e.message);
  }

  // Sample status is lowercase everywhere the desktop reads it ('pending',
  // 'converted', 'no_interest'). Normalize any capitalized rows. Idempotent.
  try {
    await pool.query(`UPDATE samples SET status = LOWER(REPLACE(status, ' ', '_'))
                       WHERE status IN ('Pending','Converted','No Interest','No_Interest')`);
  } catch (e) {
    console.error('[migrate] sample status normalize skipped:', e.message);
  }

  // (Removed Nov 2026) A startup "backfill" here forced every call CREATED today
  // onto CURRENT_DATE (UTC) on every boot. Because Railway restarts on every
  // deploy, it silently re-dated legitimate backdated visits (yesterday's visit
  // logged this morning) and pushed evening visits to tomorrow. Calls now carry
  // the phone's local date from the client, so nothing needs realigning.

  // ── One-time idempotent seed: Fortress Railing Promo stops from CSV ──────────
  // Loads fortress_promo_routes.csv (repo root) into fortress_promo_stops. The
  // route is already optimized/ordered in the CSV — we only load it verbatim.
  // ON CONFLICT (rep, company, address) DO NOTHING makes re-runs duplicate-free.
  // If the CSV isn't committed yet, this logs and skips (never blocks startup).
  try {
    const csvPath = path.join(__dirname, 'fortress_promo_routes.csv');
    if (fs.existsSync(csvPath)) {
      const rows = parseCsv(fs.readFileSync(csvPath, 'utf8'));
      let inserted = 0;
      for (const r of rows) {
        const company = r.Company || '';
        const address = r.Address || '';
        if (!company && !address) continue;
        const res = await pool.query(
          `INSERT INTO fortress_promo_stops
             (rep, day, stop_order, company, address, city, zip, phone, status, source, lat, lng)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
           ON CONFLICT (rep, company, address) DO NOTHING
           RETURNING id`,
          [r.Rep || null, r.Day || null,
           Number.isFinite(parseInt(r.Stop, 10)) ? parseInt(r.Stop, 10) : null,
           company, address, r.City || null, r.ZIP || null, r.Phone || null,
           r.Status || null, r.Source || null,
           r.lat !== '' && r.lat != null && !isNaN(parseFloat(r.lat)) ? parseFloat(r.lat) : null,
           r.lng !== '' && r.lng != null && !isNaN(parseFloat(r.lng)) ? parseFloat(r.lng) : null]
        );
        if (res.rowCount) inserted++;
      }
      console.log('[migrate] fortress promo stops seeded:', inserted, 'inserted of', rows.length, 'CSV rows');
    } else {
      console.log('[migrate] fortress_promo_routes.csv not in repo root — promo seed skipped');
    }
  } catch (e) {
    console.error('[migrate] fortress promo seed skipped:', e.message);
  }

  // One-time data jobs. Each runs once per database, recorded in app_migrations.
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS app_migrations (name TEXT PRIMARY KEY, ran_at TIMESTAMPTZ DEFAULT NOW(), result JSONB)`);
    await pool.query(`ALTER TABLE quotes ADD COLUMN IF NOT EXISTS prospect_id INTEGER REFERENCES prospects(id) ON DELETE SET NULL`);
    // Kody's field notes: when the buyer is actually there, and what this branch does.
    await pool.query(`ALTER TABLE prospects ADD COLUMN IF NOT EXISTS best_time TEXT`);
    await pool.query(`ALTER TABLE prospects ADD COLUMN IF NOT EXISTS specialty TEXT`);
    // Parent company for multi-location accounts (CRS + CRS Internet Sales).
    await pool.query(`ALTER TABLE prospects ADD COLUMN IF NOT EXISTS parent_id INTEGER REFERENCES prospects(id) ON DELETE SET NULL`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_prospects_parent ON prospects(parent_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_quotes_prospect ON quotes(prospect_id)`);
    // Shared document library and shelf photos. The routes also create these
    // on first use; having them here means every query can count on them.
    await pool.query(`CREATE TABLE IF NOT EXISTS documents (
      id SERIAL PRIMARY KEY, name TEXT NOT NULL, line TEXT, kind TEXT, filename TEXT NOT NULL,
      mime TEXT NOT NULL, size INTEGER NOT NULL, data BYTEA NOT NULL,
      uploaded_by INTEGER REFERENCES users(id) ON DELETE SET NULL, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await pool.query(`CREATE TABLE IF NOT EXISTS account_documents (
      prospect_id INTEGER NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      added_by INTEGER REFERENCES users(id) ON DELETE SET NULL, created_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (prospect_id, document_id))`);
    await pool.query(`CREATE TABLE IF NOT EXISTS shelf_photos (
      id SERIAL PRIMARY KEY,
      prospect_id INTEGER NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
      user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      mime TEXT NOT NULL, size INTEGER NOT NULL, data BYTEA NOT NULL,
      note TEXT, reading JSONB, read_error TEXT, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_shelf_photos_prospect ON shelf_photos(prospect_id, created_at DESC)`);
    // Junk that got saved as accounts (meeting notes from an import, a test
    // account) and a test visit. Each row is copied into archived_records
    // first, so nothing is lost. Only removed when nothing hangs off it.
    await pool.query(`CREATE TABLE IF NOT EXISTS archived_records (
      id SERIAL PRIMARY KEY, table_name TEXT NOT NULL, original_id INTEGER, data JSONB NOT NULL,
      reason TEXT, archived_at TIMESTAMPTZ DEFAULT NOW())`);
    await runOnce('2026-11-20 archive junk accounts', async () => {
      const junk = await pool.query(`
        SELECT p.* FROM prospects p
         WHERE (p.company LIKE '03-27 Consultation:%'
             OR p.company LIKE '03-30 Failed Initial Test of Zapier%'
             OR p.company = '04-05 Demonstration and Strategy for an AI-Powered NIL Agent Platform'
             OR p.company LIKE '04-06 Meeting: Dealer Conversion Incentives%'
             OR p.company = '2026-03-30 09:26:05'
             OR p.company = 'ZZ TEST Manufacturer (delete me)')
           AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.prospect_id = p.id)
           AND NOT EXISTS (SELECT 1 FROM samples s WHERE s.prospect_id = p.id)
           AND NOT EXISTS (SELECT 1 FROM quotes q WHERE q.prospect_id = p.id)
           AND NOT EXISTS (SELECT 1 FROM commission_lines l WHERE l.account_id = p.id)`);
      for (const row of junk.rows) {
        await pool.query(`INSERT INTO archived_records (table_name, original_id, data, reason) VALUES ('prospects',$1,$2,'junk account (Nov 20 audit)')`, [row.id, JSON.stringify(row)]);
        await pool.query('DELETE FROM prospects WHERE id=$1', [row.id]);
      }
      const tc = await pool.query(`SELECT * FROM calls WHERE notes = 'TEST call from Claude, delete'`);
      for (const row of tc.rows) {
        await pool.query(`INSERT INTO archived_records (table_name, original_id, data, reason) VALUES ('calls',$1,$2,'test visit (Nov 20 audit)')`, [row.id, JSON.stringify(row)]);
        await pool.query('DELETE FROM calls WHERE id=$1', [row.id]);
      }
      return { accounts: junk.rows.map(r => r.company), test_visits: tc.rows.length };
    });
    // Free fill: accounts with a full street address but no city get the city
    // and state from that address ("12 Main St, Toccoa, GA 30577").
    await runOnce('2026-11-20 city from address', async () => {
      const r = await pool.query(`
        UPDATE prospects SET
          city  = TRIM(m[1]),
          state = COALESCE(NULLIF(TRIM(state),''), m[2])
          FROM (SELECT id AS pid, regexp_match(address, ',\\s*([A-Za-z][A-Za-z .''-]{1,38}[A-Za-z]),\\s*([A-Z]{2})\\s*(\\d{5})?(-\\d{4})?\\s*(,\\s*(USA|US|United States))?\\s*$') AS m
                  FROM prospects WHERE (city IS NULL OR TRIM(city)='') AND address IS NOT NULL) x
         WHERE prospects.id = x.pid AND x.m IS NOT NULL`);
      return { filled: r.rowCount };
    });
    await runOnce('2026-11-20 link quotes to accounts', async () => {
      const { accountKeySql } = require('./lib/account-key');
      const r = await pool.query(`
        UPDATE quotes q SET prospect_id = m.id
          FROM (SELECT DISTINCT ON (q2.id) q2.id AS qid, p.id
                  FROM quotes q2 JOIN prospects p ON ${accountKeySql('p.company')} = ${accountKeySql('q2.account_name')}
                 ORDER BY q2.id, (p.user_id = COALESCE(q2.rep_id, q2.user_id)) DESC, p.id ASC) m
         WHERE q.id = m.qid AND q.prospect_id IS NULL`);
      return { linked: r.rowCount };
    });
    await runOnce('2026-11-20 pipeline catch-up', async () => require('./lib/pipeline').backfill(pool));

    // "What you sell": the lines the team actively sells, with who buys them.
    // Every agent reads this (lead hunt, next steps, shelf photos, documents).
    // Built on the lines table the commission import already fills.
    await pool.query(`ALTER TABLE lines ADD COLUMN IF NOT EXISTS featured BOOLEAN DEFAULT FALSE`);
    await pool.query(`ALTER TABLE lines ADD COLUMN IF NOT EXISTS what TEXT`);
    await pool.query(`ALTER TABLE lines ADD COLUMN IF NOT EXISTS buyer_segments TEXT[] DEFAULT '{}'`);
    await pool.query(`ALTER TABLE lines ADD COLUMN IF NOT EXISTS competitors TEXT`);
    await pool.query(`ALTER TABLE lines ADD COLUMN IF NOT EXISTS match_words TEXT`);
    await pool.query(`ALTER TABLE lines ADD COLUMN IF NOT EXISTS sort_order INTEGER DEFAULT 0`);
    await runOnce('2026-11-20 seed what you sell', async () => {
      const { resolveLine } = require('./lib/lines-store');
      // Buyer segments come from the Lead Finder pairings Kody tuned.
      const seed = [
        ['Soudal', 'Sealants, adhesives and foams. Lead product: Fix All.', ['Lumber / Building Supply', 'Window/Door Installer'], 'soudal, fix all, fixall, sealant'],
        ['ShurTape', 'Flashing and building tapes.', ['Roofing Contractor', 'Siding Distributor', 'Window/Door Installer'], 'shurtape, shur tape, flashing tape, tape'],
        ['Alum-A-Pole', 'Pump jack scaffolding and work platforms.', ['Siding Contractor', 'Construction Fasteners'], 'alum-a-pole, alumapole, alum a pole, pump jack, ladder'],
        ['Fortress', 'Steel deck framing, railing and fencing.', ['Deck Contractor', 'Deck Distributor/Dealer'], 'fortress, railing'],
        ['BOSS', 'Sealants and roofing products.', ['Roofing Contractor', 'Roofing Distributor'], 'boss']
      ];
      const had = (await pool.query('SELECT COUNT(*)::int n FROM lines WHERE featured')).rows[0].n;
      if (had) return { skipped: 'already set up' };
      let i = 0;
      for (const [name, what, segs, words] of seed) {
        const id = await resolveLine(pool, name);
        await pool.query(`UPDATE lines SET featured=TRUE, what=COALESCE(what,$2), buyer_segments=$3, match_words=COALESCE(match_words,$4), sort_order=$5 WHERE id=$1`,
          [id, what, segs, words, i++]);
      }
      return { lines: seed.length };
    });

    // The rest of what Compton sells, from the commission statements: every
    // line with sales on file goes on What you sell. Buyers filled in where
    // the commission data makes them clear (Daniel's book, Nov 20); the rest
    // are left for a manager to set.
    await runOnce('2026-11-20 what you sell from commissions', async () => {
      const KNOWN = [
        [/closet ?maid/, 'Closet and storage shelving systems.', ['Closet & Shower Door Shop', 'Lumber / Building Supply'], 'closet maid, closetmaid, shelving'],
        [/virginia ?mirror/, 'Mirrors and shower door glass.', ['Glass Shop', 'Closet & Shower Door Shop'], 'virginia mirror, mirror, shower door'],
        [/no ?burn/, 'Fire retardant coatings.', ['Spray Foam Contractor'], 'no burn, noburn, fire retardant'],
        [/fortress/, null, ['Fence Contractor', 'Deck Contractor', 'Deck Distributor/Dealer'], null],
        [/boss/, null, ['Roofing Contractor', 'Roofing Distributor', 'Spray Foam Contractor'], null]
      ];
      const lines = (await pool.query(
        `SELECT l.id, l.name, l.featured, COALESCE(l.buyer_segments, '{}') AS segs, COALESCE(SUM(al.total_sales),0)::float AS sales
           FROM lines l LEFT JOIN account_lines al ON al.line_id = l.id GROUP BY l.id`)).rows;
      let added = 0, filled = 0;
      for (const l of lines) {
        const k = KNOWN.find(x => x[0].test(String(l.name).toLowerCase()));
        if (!l.featured && l.sales > 0) {
          await pool.query(`UPDATE lines SET featured=TRUE, sort_order=100 WHERE id=$1`, [l.id]); added++;
        }
        if (k && (l.featured || l.sales > 0)) {
          const segs = [...new Set(l.segs.concat(k[2]))];
          await pool.query(`UPDATE lines SET buyer_segments=$2, what=COALESCE(what,$3), match_words=COALESCE(match_words,$4) WHERE id=$1`, [l.id, segs, k[1], k[3]]);
          filled++;
        }
      }
      return { added, filled };
    });

    // Overnight lead hunt results, one row per rep per Google listing.
    await pool.query(`CREATE TABLE IF NOT EXISTS leads (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      place_id TEXT NOT NULL,
      company TEXT NOT NULL, segment TEXT, category TEXT, address TEXT, city TEXT, state TEXT,
      phone TEXT, website TEXT, lat DOUBLE PRECISION, lng DOUBLE PRECISION,
      score INTEGER DEFAULT 0, why TEXT, pitch TEXT,
      status TEXT NOT NULL DEFAULT 'new',
      prospect_id INTEGER REFERENCES prospects(id) ON DELETE SET NULL,
      found_at TIMESTAMPTZ DEFAULT NOW(), decided_at TIMESTAMPTZ,
      UNIQUE (user_id, place_id))`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_leads_user_status ON leads(user_id, status)`);
    await pool.query(`CREATE TABLE IF NOT EXISTS lead_hunt_runs (
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, run_day DATE NOT NULL,
      found INTEGER DEFAULT 0, note TEXT, ran_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (user_id, run_day))`);
    await pool.query(`CREATE TABLE IF NOT EXISTS day_plan_runs (
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, run_day DATE NOT NULL,
      added INTEGER DEFAULT 0, miles NUMERIC, ran_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (user_id, run_day))`);
    // Which layout a person uses: 'classic' until they switch to the new one.
    await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS ui_layout TEXT DEFAULT 'new'`);
    await pool.query(`ALTER TABLE users ALTER COLUMN ui_layout SET DEFAULT 'new'`);
    // Nov 20: the new layout goes on for everyone (they can still switch back).
    await runOnce('2026-11-20 new layout for everyone', async () => {
      const r = await pool.query(`UPDATE users SET ui_layout='new' WHERE COALESCE(ui_layout,'classic') <> 'new'`);
      return { switched: r.rowCount };
    });
  } catch (e) {
    console.error('[migrate] one-time jobs:', e.message);
  }

  // Neutralize any saved HTML tag text (see lib/defang.js). Only touches rows that have one.
  // Runs last so every column it touches exists, including on a brand-new database.
  try {
    await pool.query(`
    UPDATE prospects SET company = regexp_replace(company, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE company ~ '<[a-zA-Z/!?]';
    UPDATE prospects SET contact = regexp_replace(contact, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE contact ~ '<[a-zA-Z/!?]';
    UPDATE prospects SET city = regexp_replace(city, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE city ~ '<[a-zA-Z/!?]';
    UPDATE prospects SET address = regexp_replace(address, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE address ~ '<[a-zA-Z/!?]';
    UPDATE prospects SET notes = regexp_replace(notes, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE notes ~ '<[a-zA-Z/!?]';
    UPDATE prospects SET products = regexp_replace(products, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE products ~ '<[a-zA-Z/!?]';
    UPDATE prospects SET title = regexp_replace(title, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE title ~ '<[a-zA-Z/!?]';
    UPDATE contacts SET name = regexp_replace(name, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE name ~ '<[a-zA-Z/!?]';
    UPDATE contacts SET title = regexp_replace(title, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE title ~ '<[a-zA-Z/!?]';
    UPDATE contacts SET email = regexp_replace(email, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE email ~ '<[a-zA-Z/!?]';
    UPDATE contacts SET phone = regexp_replace(phone, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE phone ~ '<[a-zA-Z/!?]';
    UPDATE calls SET notes = regexp_replace(notes, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE notes ~ '<[a-zA-Z/!?]';
    UPDATE calls SET next_step = regexp_replace(next_step, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE next_step ~ '<[a-zA-Z/!?]';
    UPDATE calls SET products_discussed = regexp_replace(products_discussed, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE products_discussed ~ '<[a-zA-Z/!?]';
    UPDATE calls SET outcome = regexp_replace(outcome, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE outcome ~ '<[a-zA-Z/!?]';
    UPDATE quotes SET account_name = regexp_replace(account_name, '<([a-zA-Z/!?])', '‹\\1', 'g') WHERE account_name ~ '<[a-zA-Z/!?]';
    `);
  } catch (e) {
    console.error('[migrate] defang cleanup skipped:', e.message);
  }

  console.log('Database initialized');
}

module.exports = { pool, initDB, parseCsv };
