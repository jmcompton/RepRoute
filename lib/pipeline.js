// The Deal Pipeline moves on its own from what reps already log, so nobody has
// to drag cards (dragging doesn't work on an iPhone anyway):
//   a visit or call      → Contacted
//   a sample left        → Sampled
//   a quote              → Quoted
//   a won quote / order  → Closed Won
// Stages only ever move forward here. A rep can still move a card by hand.
const STAGES = ['New Lead', 'Contacted', 'Sampled', 'Quoted', 'Closed Won'];

function rank(stage) {
  const i = STAGES.indexOf(stage || 'New Lead');
  return i < 0 ? 0 : i;
}

// SQL CASE giving a stage's rank, for "only move forward" updates.
const RANK_SQL = `CASE COALESCE(pipeline_stage,'New Lead')
  WHEN 'New Lead' THEN 0 WHEN 'Contacted' THEN 1 WHEN 'Sampled' THEN 2
  WHEN 'Quoted' THEN 3 WHEN 'Closed Won' THEN 4 WHEN 'Closed Lost' THEN 99 ELSE 0 END`;

async function advance(pool, prospectId, stage) {
  const id = parseInt(prospectId, 10);
  if (!Number.isFinite(id) || !STAGES.includes(stage)) return false;
  try {
    const r = await pool.query(
      `UPDATE prospects SET pipeline_stage = $2 WHERE id = $1 AND ${RANK_SQL} < $3`,
      [id, stage, rank(stage)]);
    return r.rowCount > 0;
  } catch (e) {
    console.error('[pipeline] advance', id, stage, e.message);
    return false;
  }
}

// Stage a logged visit implies.
function stageForOutcome(outcome) {
  const o = String(outcome || '');
  if (/order placed/i.test(o)) return 'Closed Won';
  if (/left sample/i.test(o)) return 'Sampled';
  return 'Contacted';
}

// One-time catch-up from history already in the database. Forward-only, so it
// can't undo anything a rep set by hand to a later stage.
async function backfill(pool) {
  const steps = [
    ['Contacted', `SELECT DISTINCT prospect_id AS id FROM calls WHERE prospect_id IS NOT NULL`],
    ['Sampled', `SELECT DISTINCT prospect_id AS id FROM samples WHERE prospect_id IS NOT NULL`],
    ['Quoted', `SELECT DISTINCT prospect_id AS id FROM quotes WHERE prospect_id IS NOT NULL`],
    ['Closed Won', `SELECT DISTINCT prospect_id AS id FROM quotes WHERE prospect_id IS NOT NULL AND status = 'Won'`]
  ];
  const moved = {};
  for (const [stage, sql] of steps) {
    const r = await pool.query(
      `UPDATE prospects SET pipeline_stage = $1 WHERE id IN (${sql}) AND ${RANK_SQL} < $2`, [stage, rank(stage)]);
    moved[stage] = r.rowCount;
  }
  return moved;
}

module.exports = { STAGES, advance, stageForOutcome, backfill, rank };
