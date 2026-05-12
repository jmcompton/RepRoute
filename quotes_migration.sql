-- Create the quotes table
CREATE TABLE IF NOT EXISTS quotes (
  id SERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  rep_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
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
