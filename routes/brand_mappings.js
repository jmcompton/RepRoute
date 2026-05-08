const express = require('express');
const { pool } = require('../db');
const router = express.Router();

const DEFAULT_MAPPINGS = [
  // BOSS Products
  { brand: 'BOSS Products', channel: 'Dealer', customer_types: ['Roofing Distributor', 'Roofing Supply', 'Roofing Wholesale Dealer'] },
  { brand: 'BOSS Products', channel: 'Contractor', customer_types: ['Roofing Contractor', 'Commercial Roofing Contractor'] },

  // ShurTape
  { brand: 'ShurTape', channel: 'Dealer', customer_types: ['One-Step Specialty Dealer', 'Building Material Specialty Dealer', 'Siding Supply Distributor'] },
  { brand: 'ShurTape', channel: 'Contractor', customer_types: ['Deck Contractor', 'Window Installer', 'Door Installer', 'Siding Contractor', 'Cornice Contractor'] },

  // Alum-A-Pole
  { brand: 'Alum-A-Pole', channel: 'Dealer', customer_types: ['One-Step Specialty Dealer', 'Building Material Specialty Dealer', 'Fastener Dealer', 'Tool Equipment Dealer', 'Construction Equipment Supplier'] },
  { brand: 'Alum-A-Pole', channel: 'Contractor', customer_types: ['Siding Contractor', 'Cornice Contractor', 'Painting Contractor'] }
];

async function seedDefaultsForUser(userId) {
  for (const m of DEFAULT_MAPPINGS) {
    await pool.query(
      `INSERT INTO brand_mappings (user_id, brand, channel, customer_types)
       VALUES ($1, $2, $3, $4::jsonb)
       ON CONFLICT (user_id, brand, channel) DO NOTHING`,
      [userId, m.brand, m.channel, JSON.stringify(m.customer_types)]
    );
  }
}

router.get('/', async (req, res) => {
  const uid = req.session.user.id;
  try {
    let r = await pool.query('SELECT brand, channel, customer_types FROM brand_mappings WHERE user_id=$1 ORDER BY brand, channel', [uid]);
    if (r.rows.length === 0) {
      await seedDefaultsForUser(uid);
      r = await pool.query('SELECT brand, channel, customer_types FROM brand_mappings WHERE user_id=$1 ORDER BY brand, channel', [uid]);
    }
    res.json({ ok: true, mappings: r.rows });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

router.post('/', async (req, res) => {
  const uid = req.session.user.id;
  const { brand, channel, customer_types } = req.body;
  if (!brand || !channel || !Array.isArray(customer_types)) {
    return res.status(400).json({ error: 'brand, channel, customer_types required' });
  }
  try {
    await pool.query(
      `INSERT INTO brand_mappings (user_id, brand, channel, customer_types, updated_at)
       VALUES ($1, $2, $3, $4::jsonb, NOW())
       ON CONFLICT (user_id, brand, channel)
       DO UPDATE SET customer_types=$4::jsonb, updated_at=NOW()`,
      [uid, brand, channel, JSON.stringify(customer_types)]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

module.exports = { router, seedDefaultsForUser };
