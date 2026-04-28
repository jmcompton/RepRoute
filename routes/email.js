const express = require('express');
const { pool } = require('../db');
const router = express.Router();

const MICROSOFT_CLIENT_ID = process.env.MICROSOFT_CLIENT_ID;
const MICROSOFT_CLIENT_SECRET = process.env.MICROSOFT_CLIENT_SECRET;
const APP_URL = process.env.APP_URL || 'https://comptongroupllc.com';
const REDIRECT_URI = APP_URL + '/auth/outlook/callback';
const SCOPES = 'openid profile email Mail.Read Mail.Send offline_access';

// Step 1: Redirect user to Microsoft login
router.get('/connect/outlook', (req, res) => {
  const authUrl = 'https://login.microsoftonline.com/common/oauth2/v2.0/authorize?' +
    new URLSearchParams({
      client_id: MICROSOFT_CLIENT_ID,
      response_type: 'code',
      redirect_uri: REDIRECT_URI,
      scope: SCOPES,
      response_mode: 'query',
      state: req.session.user.id.toString()
    }).toString();
  res.redirect(authUrl);
});

// Step 2: Handle callback, exchange code for tokens
router.get('/outlook/callback', async (req, res) => {
  const { code, state } = req.query;
  if (!code) return res.redirect('/app?error=no_code');
  try {
    const tokenRes = await fetch('https://login.microsoftonline.com/common/oauth2/v2.0/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id: MICROSOFT_CLIENT_ID,
        client_secret: MICROSOFT_CLIENT_SECRET,
        code,
        redirect_uri: REDIRECT_URI,
        grant_type: 'authorization_code'
      })
    });
    const tokens = await tokenRes.json();
    if (tokens.error) { console.error('Token error:', JSON.stringify(tokens)); throw new Error(tokens.error_description || tokens.error); }

    const userId = parseInt(state);
    await pool.query(
      `UPDATE users SET 
        outlook_access_token=$1, 
        outlook_refresh_token=$2, 
        outlook_token_expiry=NOW() + INTERVAL '1 hour'
       WHERE id=$3`,
      [tokens.access_token, tokens.refresh_token, userId]
    );
    res.redirect('/app?outlook=connected');
  } catch (e) {
    console.error('Outlook auth error:', e.message);
    res.redirect('/app?error=' + encodeURIComponent(e.message));
  }
});

// Refresh access token if expired
async function getValidToken(userId) {
  const result = await pool.query(
    'SELECT outlook_access_token, outlook_refresh_token, outlook_token_expiry FROM users WHERE id=$1',
    [userId]
  );
  const user = result.rows[0];
  if (!user || !user.outlook_access_token) throw new Error('Not connected to Outlook');

  const expiry = new Date(user.outlook_token_expiry);
  if (expiry > new Date(Date.now() + 60000)) return user.outlook_access_token;

  // Refresh the token
  const tokenRes = await fetch('https://login.microsoftonline.com/common/oauth2/v2.0/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: MICROSOFT_CLIENT_ID,
      client_secret: MICROSOFT_CLIENT_SECRET,
      refresh_token: user.outlook_refresh_token,
      grant_type: 'refresh_token'
    })
  });
  const tokens = await tokenRes.json();
  if (tokens.error) throw new Error('Token refresh failed');

  await pool.query(
    `UPDATE users SET outlook_access_token=$1, outlook_token_expiry=NOW() + INTERVAL '1 hour' WHERE id=$2`,
    [tokens.access_token, userId]
  );
  return tokens.access_token;
}

// Get emails for a contact
router.get('/emails/:prospectId', async (req, res) => {
  const userId = req.session.user.id;
  try {
    const prospect = await pool.query('SELECT * FROM prospects WHERE id=$1', [req.params.prospectId]);
    if (!prospect.rows[0]) return res.status(404).json({ error: 'Contact not found' });
    const email = prospect.rows[0].email;
    if (!email) return res.json({ emails: [], message: 'No email address on this contact' });

    // Get manually logged emails from DB
    const logged = await pool.query(
      `SELECT * FROM email_logs WHERE prospect_id=$1 ORDER BY sent_at DESC`,
      [req.params.prospectId]
    );

    // Try to fetch from Outlook if connected
    let outlookEmails = [];
    try {
      const token = await getValidToken(userId);
      const search = await fetch(
        `https://graph.microsoft.com/v1.0/me/messages?$search="from:${email} OR to:${email}"&$top=20&$orderby=receivedDateTime desc&$select=subject,from,toRecipients,receivedDateTime,bodyPreview,webLink`,
        { headers: { Authorization: 'Bearer ' + token } }
      );
      const data = await search.json();
      if (data.value) {
        outlookEmails = data.value.map(m => ({
          source: 'outlook',
          subject: m.subject,
          from: m.from?.emailAddress?.address,
          to: m.toRecipients?.map(r => r.emailAddress.address).join(', '),
          preview: m.bodyPreview,
          date: m.receivedDateTime,
          link: m.webLink
        }));
      }
    } catch (e) {
      // Not connected to Outlook, just return logged emails
    }

    res.json({ emails: logged.rows, outlookEmails });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Send email via Outlook
router.post('/send', async (req, res) => {
  const userId = req.session.user.id;
  const { prospect_id, to, subject, body } = req.body;
  try {
    const token = await getValidToken(userId);
    const sendRes = await fetch('https://graph.microsoft.com/v1.0/me/sendMail', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: {
          subject,
          body: { contentType: 'Text', content: body },
          toRecipients: [{ emailAddress: { address: to } }]
        },
        saveToSentItems: true
      })
    });

    if (!sendRes.ok) {
      const err = await sendRes.json();
      throw new Error(err.error?.message || 'Send failed');
    }

    // Log the email
    await pool.query(
      `INSERT INTO email_logs (user_id, prospect_id, direction, subject, body, to_email, sent_at, source)
       VALUES ($1,$2,'out',$3,$4,$5,NOW(),'outlook')`,
      [userId, prospect_id, subject, body, to]
    );

    res.json({ success: true });
  } catch (e) {
    // If not connected, just log it manually
    if (e.message.includes('Not connected')) {
      await pool.query(
        `INSERT INTO email_logs (user_id, prospect_id, direction, subject, body, to_email, sent_at, source)
         VALUES ($1,$2,'out',$3,$4,$5,NOW(),'manual')`,
        [userId, prospect_id, subject, body, to]
      );
      res.json({ success: true, manual: true });
    } else {
      res.status(500).json({ error: e.message });
    }
  }
});

// Log email manually
router.post('/log', async (req, res) => {
  const userId = req.session.user.id;
  const { prospect_id, direction, subject, body, to_email, from_email } = req.body;
  try {
    await pool.query(
      `INSERT INTO email_logs (user_id, prospect_id, direction, subject, body, to_email, from_email, sent_at, source)
       VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),'manual')`,
      [userId, prospect_id, direction || 'out', subject, body, to_email, from_email]
    );
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Check connection status
router.get('/status', async (req, res) => {
  const userId = req.session.user.id;
  try {
    const result = await pool.query(
      'SELECT outlook_access_token IS NOT NULL as connected, outlook_token_expiry FROM users WHERE id=$1',
      [userId]
    );
    res.json({ connected: result.rows[0]?.connected || false });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = { router, getValidToken };

// Get full inbox
router.get('/inbox', async (req, res) => {
  const userId = req.session.user.id;
  try {
    const token = await getValidToken(userId);
    const response = await fetch(
      'https://graph.microsoft.com/v1.0/me/messages?$top=50&$orderby=receivedDateTime desc&$select=subject,from,toRecipients,receivedDateTime,bodyPreview,webLink,isRead',
      { headers: { Authorization: 'Bearer ' + token } }
    );
    const data = await response.json();
    if (data.error) throw new Error(data.error.message);
    res.json({ emails: data.value || [] });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Get Outlook contacts
router.get('/outlook-contacts', async (req, res) => {
  const userId = req.session.user.id;
  try {
    const token = await getValidToken(userId);
    const response = await fetch(
      'https://graph.microsoft.com/v1.0/me/contacts?$top=100&$select=displayName,emailAddresses,companyName',
      { headers: { Authorization: 'Bearer ' + token } }
    );
    const data = await response.json();
    if (data.error) throw new Error(data.error.message);
    res.json({ contacts: data.value || [] });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});
