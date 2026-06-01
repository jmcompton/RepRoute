const express = require('express');
const XLSX    = require('xlsx');
const { pool } = require('../db');
const router  = express.Router();

// ── Column helper: case-insensitive, whitespace-tolerant cell lookup ──
function col(row, ...names) {
  for (const name of names) {
    if (row[name] !== undefined) return String(row[name] == null ? '' : row[name]).trim();
    const key = Object.keys(row).find(
      k => k.trim().toLowerCase() === name.toLowerCase()
    );
    if (key !== undefined) return String(row[key] == null ? '' : row[key]).trim();
  }
  return '';
}

// ── Parse a base64 string or data-URL into an XLSX workbook ──────────
function parseWorkbook(b64) {
  // Strip optional "data:<mime>;base64," prefix
  const raw = b64.includes(',') ? b64.split(',')[1] : b64;
  const buf = Buffer.from(raw, 'base64');
  return XLSX.read(buf, { type: 'buffer', cellDates: true, cellText: false });
}

function sheetToRows(wb) {
  const sheet = wb.Sheets[wb.SheetNames[0]];
  return XLSX.utils.sheet_to_json(sheet, { defval: '' });
}

// ── GET /api/zoho/users ─── return user list for rep-mapping UI ──────
router.get('/users', async (req, res) => {
  try {
    const r = await pool.query('SELECT id, name, email FROM users ORDER BY name');
    res.json(r.rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/zoho/history ─── past import records ───────────────────
router.get('/history', async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT ih.*, u.name as imported_by
       FROM import_history ih
       LEFT JOIN users u ON ih.user_id = u.id
       ORDER BY ih.imported_at DESC LIMIT 10`
    );
    res.json(r.rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/zoho/parse ─── parse files, store in session, return preview ──
router.post('/parse', async (req, res) => {
  try {
    const { contacts_b64, accounts_b64 } = req.body;
    if (!contacts_b64 || !accounts_b64) {
      return res.status(400).json({ error: 'Both contacts and accounts files are required.' });
    }

    // ── Parse accounts ───────────────────────────────────────────────
    const accountRows = sheetToRows(parseWorkbook(accounts_b64));
    const accountsMap  = new Map(); // key: lowercase company name → enriched object
    const accountsParsed = [];

    for (const row of accountRows) {
      const name = col(row, 'Account Name');
      if (!name) continue;
      const parsed = {
        company_name : name,
        phone        : col(row, 'Phone'),
        website      : col(row, 'Website'),
        owner        : col(row, 'Account Owner'),
        zoho_id      : col(row, 'Record Id', 'Account Id', 'id'),
      };
      accountsParsed.push(parsed);
      // Store first occurrence for each lowercase name
      const key = name.toLowerCase().trim();
      if (!accountsMap.has(key)) accountsMap.set(key, parsed);
    }

    // ── Parse contacts ───────────────────────────────────────────────
    const contactRows = sheetToRows(parseWorkbook(contacts_b64));
    const contactsParsed = [];
    const zohoOwnerNames = new Set();

    for (const row of contactRows) {
      const fullName   = col(row, 'Contact Name', 'Full Name', 'Name');
      const acctName   = col(row, 'Account Name', 'Company', 'Organization');
      if (!fullName && !acctName) continue;

      const spaceIdx   = fullName.indexOf(' ');
      const firstName  = spaceIdx === -1 ? fullName : fullName.substring(0, spaceIdx);
      const lastName   = spaceIdx === -1 ? '' : fullName.substring(spaceIdx + 1).trim();

      const owner      = col(row, 'Contact Owner', 'Owner', 'Assigned To');
      const email      = col(row, 'Email', 'Email Address');
      const mobile     = col(row, 'Mobile', 'Mobile Phone', 'Cell Phone', 'Phone');
      const zohoId     = col(row, 'Record Id', 'Contact Id', 'id');

      // Look up account enrichment
      const acctKey    = acctName.toLowerCase().trim();
      const acct       = accountsMap.get(acctKey);
      const phone      = mobile || (acct ? acct.phone : '');
      const website    = acct ? acct.website : '';

      if (owner) zohoOwnerNames.add(owner);

      contactsParsed.push({
        full_name   : fullName,
        first_name  : firstName,
        last_name   : lastName,
        company_name: acctName || fullName,
        email,
        phone,
        website,
        owner,
        zoho_id     : zohoId,
        _acct_found : !!acct,
      });
    }

    // ── Find orphaned accounts (no contact references them) ─────────
    const referencedAccts = new Set(
      contactsParsed.map(c => c.company_name.toLowerCase().trim()).filter(Boolean)
    );
    const orphanedAccounts = accountsParsed.filter(
      a => !referencedAccts.has(a.company_name.toLowerCase().trim())
    );

    // Collect all owner names (contacts + accounts)
    accountsParsed.forEach(a => { if (a.owner) zohoOwnerNames.add(a.owner); });

    // ── Check for duplicates against existing prospects ──────────────
    const existingEmailsRes  = await pool.query(
      'SELECT LOWER(TRIM(email)) as e FROM prospects WHERE email IS NOT NULL AND email != \'\''
    );
    const existingCosRes = await pool.query(
      'SELECT LOWER(TRIM(company)) as c FROM prospects WHERE company IS NOT NULL AND company != \'\''
    );
    const existingEmails = new Set(existingEmailsRes.rows.map(r => r.e));
    const existingCos    = new Set(existingCosRes.rows.map(r => r.c));

    let contactDupes = 0, acctDupes = 0;

    contactsParsed.forEach(c => {
      c.is_duplicate =
        (c.email && existingEmails.has(c.email.toLowerCase())) ||
        (!c.email && c.company_name && existingCos.has(c.company_name.toLowerCase().trim()));
      if (c.is_duplicate) contactDupes++;
    });
    orphanedAccounts.forEach(a => {
      a.is_duplicate = existingCos.has(a.company_name.toLowerCase().trim());
      if (a.is_duplicate) acctDupes++;
    });

    // ── Stats ────────────────────────────────────────────────────────
    const withPhone = contactsParsed.filter(c => c.phone).length;
    const byOwner   = {};
    contactsParsed.forEach(c => {
      const k = c.owner || '(unassigned)';
      byOwner[k] = (byOwner[k] || 0) + 1;
    });

    // ── Get RepRoute users for rep mapping ───────────────────────────
    const usersRes = await pool.query('SELECT id, name, email FROM users ORDER BY name');
    const users    = usersRes.rows;

    // ── Store parsed data in session (used by /import) ───────────────
    req.session.zoho_parsed = {
      contacts        : contactsParsed,
      orphaned_accounts: orphanedAccounts,
      accounts_total  : accountsParsed.length,
    };
    await new Promise((resolve, reject) =>
      req.session.save(err => err ? reject(err) : resolve())
    );

    // ── Build sample duplicate lists ─────────────────────────────────
    const dupContactSamples = contactsParsed.filter(c => c.is_duplicate).slice(0, 20).map(c => ({
      name        : c.full_name,
      company_name: c.company_name,
      email       : c.email,
    }));
    const dupAcctSamples = orphanedAccounts.filter(a => a.is_duplicate).slice(0, 20).map(a => ({
      company_name: a.company_name,
      phone       : a.phone,
    }));

    res.json({
      contacts: {
        total       : contactsParsed.length,
        with_phone  : withPhone,
        duplicates  : contactDupes,
        by_owner    : byOwner,
        preview     : contactsParsed.slice(0, 25).map(c => ({
          full_name   : c.full_name,
          company_name: c.company_name,
          email       : c.email,
          phone       : c.phone,
          owner       : c.owner,
          is_duplicate: c.is_duplicate,
        })),
      },
      orphaned_accounts: {
        total     : orphanedAccounts.length,
        duplicates: acctDupes,
        preview   : orphanedAccounts.slice(0, 25).map(a => ({
          company_name: a.company_name,
          phone       : a.phone,
          website     : a.website,
          owner       : a.owner,
          is_duplicate: a.is_duplicate,
        })),
      },
      accounts_total : accountsParsed.length,
      zoho_owners    : Array.from(zohoOwnerNames).sort(),
      users,
      dup_contacts   : dupContactSamples,
      dup_accounts   : dupAcctSamples,
      total_dupes    : contactDupes + acctDupes,
    });

  } catch (e) {
    console.error('[ZohoImport] parse error:', e);
    res.status(500).json({ error: 'Parse failed: ' + e.message });
  }
});

// ── POST /api/zoho/import ─── run the actual import ──────────────────
// Body: { rep_mapping: { 'Sean': userId, 'Daniel Compton': userId }, skip_dup_contacts, skip_dup_accounts, default_user_id, default_category }
router.post('/import', async (req, res) => {
  const parsed = req.session.zoho_parsed;
  if (!parsed) {
    return res.status(400).json({ error: 'No parsed data found. Please upload and parse files first.' });
  }

  const {
    rep_mapping       = {},   // { zohoOwnerName: repRouteUserId }
    skip_dup_contacts = true,
    skip_dup_accounts = true,
    default_user_id,
    default_category  = 'Roofing Contractor',
  } = req.body;

  // Build a fast owner→user_id lookup
  // rep_mapping keys are Zoho owner names (exact), values are RepRoute user_id strings
  const repLookup = {};
  for (const [zohoName, uid] of Object.entries(rep_mapping)) {
    if (uid) repLookup[zohoName.trim().toLowerCase()] = parseInt(uid, 10);
  }

  // Auto-populate from users table by first/full name if not in explicit mapping
  const usersRes = await pool.query('SELECT id, name FROM users');
  usersRes.rows.forEach(u => {
    const lower = u.name.trim().toLowerCase();
    if (!repLookup[lower]) repLookup[lower] = u.id;
    const firstName = lower.split(' ')[0];
    if (!repLookup[firstName]) repLookup[firstName] = u.id;
  });

  const defaultUid = parseInt(default_user_id, 10) || req.session.user.id;

  function resolveUid(ownerName) {
    if (!ownerName || !ownerName.trim()) return defaultUid;
    return repLookup[ownerName.trim().toLowerCase()] || defaultUid;
  }

  // Resolve company_type from category
  function resolveType(cat) {
    const lower = (cat || '').toLowerCase();
    if (lower.includes('distributor') || lower.includes('supply') ||
        lower.includes('wholesale') || lower.includes('dealer') ||
        lower.includes('lumber') || lower.includes('building material')) {
      return 'Distributor';
    }
    return 'Contractor';
  }

  const importDate = new Date().toLocaleDateString('en-US', {
    month: 'long', day: 'numeric', year: 'numeric'
  });

  // Re-check duplicates at import time (in case DB changed since parse)
  const existEmailsRes = await pool.query(
    'SELECT LOWER(TRIM(email)) as e FROM prospects WHERE email IS NOT NULL AND email != \'\''
  );
  const existCosRes    = await pool.query(
    'SELECT LOWER(TRIM(company)) as c FROM prospects WHERE company IS NOT NULL AND company != \'\''
  );
  const existEmails    = new Set(existEmailsRes.rows.map(r => r.e));
  const existCos       = new Set(existCosRes.rows.map(r => r.c));

  const { contacts, orphaned_accounts, accounts_total } = parsed;
  let contactsImported = 0, contactsSkipped = 0;
  let accountsImported = 0, accountsSkipped = 0;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // ── 1. Import contacts ────────────────────────────────────────────
    for (const c of contacts) {
      const isDupe =
        (c.email && existEmails.has(c.email.toLowerCase())) ||
        (!c.email && c.company_name && existCos.has(c.company_name.toLowerCase().trim()));

      if (skip_dup_contacts && isDupe) {
        contactsSkipped++;
        continue;
      }

      const uid      = resolveUid(c.owner);
      const company  = (c.company_name || c.full_name || 'Unknown').trim();
      const category = default_category;
      const coType   = resolveType(category);
      const notes    = `Imported from Zoho CRM on ${importDate}.`;

      await client.query(
        `INSERT INTO prospects
           (user_id, company, contact, email, phone, website, category, company_type,
            source, data_status, notes, zoho_id, last_activity_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())`,
        [
          uid,
          company,
          c.full_name  || null,
          c.email      || null,
          c.phone      || null,
          c.website    || null,
          category,
          coType,
          'zoho_import',
          'Unvetted',
          notes,
          c.zoho_id    || null,
        ]
      );
      contactsImported++;

      // Track company so orphaned accounts skip duplication
      existCos.add(company.toLowerCase().trim());
    }

    // ── 2. Import orphaned accounts (accounts with no contacts) ───────
    for (const a of orphaned_accounts) {
      const isDupe = existCos.has(a.company_name.toLowerCase().trim());

      if (skip_dup_accounts && isDupe) {
        accountsSkipped++;
        continue;
      }

      const uid      = resolveUid(a.owner);
      const category = default_category;
      const coType   = resolveType(category);
      const notes    = `Imported from Zoho CRM on ${importDate}. (Company-only record — no contact linked in Zoho export)`;

      await client.query(
        `INSERT INTO prospects
           (user_id, company, phone, website, category, company_type,
            source, data_status, notes, zoho_id, last_activity_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())`,
        [
          uid,
          a.company_name.trim(),
          a.phone   || null,
          a.website || null,
          category,
          coType,
          'zoho_import',
          'Unvetted',
          notes,
          a.zoho_id || null,
        ]
      );
      accountsImported++;
      existCos.add(a.company_name.toLowerCase().trim());
    }

    await client.query('COMMIT');

    // ── 3. Record import history ─────────────────────────────────────
    try {
      await pool.query(
        `INSERT INTO import_history
           (user_id, source, contacts_imported, contacts_skipped, accounts_imported, accounts_skipped)
         VALUES ($1,'zoho',$2,$3,$4,$5)`,
        [req.session.user.id, contactsImported, contactsSkipped, accountsImported, accountsSkipped]
      );
    } catch (_) { /* non-fatal */ }

    // Clear session data
    delete req.session.zoho_parsed;
    req.session.save(() => {});

    console.log(
      `[ZohoImport] contacts: ${contactsImported} imported, ${contactsSkipped} skipped ` +
      `| orphaned accounts: ${accountsImported} imported, ${accountsSkipped} skipped`
    );

    res.json({
      ok                : true,
      contacts_imported : contactsImported,
      contacts_skipped  : contactsSkipped,
      accounts_imported : accountsImported,
      accounts_skipped  : accountsSkipped,
    });

  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[ZohoImport] import error:', e);
    res.status(500).json({ error: 'Import failed: ' + e.message });
  } finally {
    client.release();
  }
});

module.exports = router;
