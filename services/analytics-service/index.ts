import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import { v4 as uuidv4 } from 'uuid';
import { query, queryOne, queryMany, checkDbHealth } from '../../shared/db';
import { publishEvent, startConsumer } from '../../shared/kafka';
import { KAFKA_TOPICS } from '../../shared/types';

const app = express();
const PORT = process.env.ANALYTICS_SERVICE_PORT ?? 4004;

app.use(helmet());
app.use(cors());
app.use(express.json());

// ─── Health ───────────────────────────────────────────────────────────────────

app.get('/health', async (_req, res) => {
  const dbOk = await checkDbHealth();
  res.json({ service: 'analytics-service', status: dbOk ? 'ok' : 'degraded', timestamp: new Date().toISOString() });
});

// ─── Leads ────────────────────────────────────────────────────────────────────

// Public endpoint — called from published site contact forms
app.post('/leads', async (req, res) => {
  try {
    const { siteId, email, firstName, lastName, phone, message, source, metadata } = req.body;

    if (!siteId || !email) {
      return res.status(400).json({ error: 'siteId and email are required' });
    }

    const site = await queryOne(
      `SELECT user_id FROM sites WHERE id = $1 AND status = 'published'`,
      [siteId]
    );
    if (!site) return res.status(404).json({ error: 'Site not found or not published' });

    const lead = await queryOne(
      `INSERT INTO leads (id, site_id, user_id, email, first_name, last_name, phone, message, source, metadata)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
      [
        uuidv4(), siteId, site.user_id,
        email.toLowerCase().trim(),
        firstName ?? null, lastName ?? null, phone ?? null,
        message ?? null,
        source ?? 'contact-form',
        JSON.stringify(metadata ?? {}),
      ]
    );

    // Kafka event triggers AI lead scoring (if Kafka running)
    await publishEvent(KAFKA_TOPICS.LEAD_CREATED, {
      leadId: lead!.id, siteId, userId: site.user_id, email,
    });

    res.status(201).json({ data: { id: lead!.id, message: 'Thank you for your inquiry!' } });
  } catch (err) {
    console.error('[Analytics Service] POST /leads:', err);
    res.status(500).json({ error: (err as Error).message });
  }
});

// Protected — fetch leads for a user
app.get('/leads', async (req, res) => {
  try {
    const userId = req.headers['x-user-id'] as string;
    if (!userId) return res.status(401).json({ error: 'Unauthorized' });

    const siteId = req.query.siteId as string | undefined;
    const status = req.query.status as string | undefined;
    const page = parseInt((req.query.page as string) ?? '1');
    const limit = parseInt((req.query.limit as string) ?? '50');
    const offset = (page - 1) * limit;

    let sql = `
      SELECT l.*, s.name as site_name
      FROM leads l
      JOIN sites s ON l.site_id = s.id
      WHERE l.user_id = $1
    `;
    const params: unknown[] = [userId];

    if (siteId) { params.push(siteId); sql += ` AND l.site_id = $${params.length}`; }
    if (status) { params.push(status); sql += ` AND l.status = $${params.length}`; }

    sql += ` ORDER BY l.created_at DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
    params.push(limit, offset);

    const leads = await queryMany(sql, params);

    const countSql = `SELECT COUNT(*) FROM leads WHERE user_id = $1${siteId ? ' AND site_id = $2' : ''}`;
    const countRes = await queryOne<{ count: string }>(
      countSql, siteId ? [userId, siteId] : [userId]
    );
    const total = parseInt(countRes?.count ?? '0');

    res.json({
      data: leads,
      meta: { total, page, pageSize: limit, hasMore: offset + leads.length < total },
    });
  } catch (err) {
    console.error('[Analytics Service] GET /leads:', err);
    res.status(500).json({ error: (err as Error).message });
  }
});

app.get('/leads/:id', async (req, res) => {
  try {
    const userId = req.headers['x-user-id'] as string;
    const lead = await queryOne(
      `SELECT * FROM leads WHERE id = $1 AND user_id = $2`,
      [req.params.id, userId]
    );
    if (!lead) return res.status(404).json({ error: 'Lead not found' });
    res.json({ data: lead });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

app.patch('/leads/:id', async (req, res) => {
  try {
    const userId = req.headers['x-user-id'] as string;
    const { status } = req.body;
    const lead = await queryOne(
      `UPDATE leads SET status=$1, updated_at=NOW() WHERE id=$2 AND user_id=$3 RETURNING *`,
      [status, req.params.id, userId]
    );
    if (!lead) return res.status(404).json({ error: 'Lead not found' });
    await publishEvent(KAFKA_TOPICS.LEAD_UPDATED, { leadId: req.params.id, userId, status });
    res.json({ data: lead });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

// ─── Analytics ────────────────────────────────────────────────────────────────

app.get('/analytics/:siteId', async (req, res) => {
  try {
    const { siteId } = req.params;
    const days = parseInt((req.query.days as string) ?? '30');
    const since = new Date();
    since.setDate(since.getDate() - days);

    const rows = await queryMany(
      `SELECT * FROM site_analytics_daily
       WHERE site_id = $1 AND date >= $2 ORDER BY date ASC`,
      [siteId, since.toISOString().split('T')[0]]
    );

    const totalViews    = rows.reduce((s, r) => s + (Number(r.page_views) || 0), 0);
    const totalVisitors = rows.reduce((s, r) => s + (Number(r.unique_visitors) || 0), 0);
    const totalLeads    = rows.reduce((s, r) => s + (Number(r.leads_captured) || 0), 0);
    const avgSession    = rows.length
      ? Math.round(rows.reduce((s, r) => s + (Number(r.avg_session_duration_sec) || 0), 0) / rows.length)
      : 0;
    const avgBounce = rows.length
      ? parseFloat((rows.reduce((s, r) => s + (Number(r.bounce_rate) || 0), 0) / rows.length).toFixed(1))
      : null;

    res.json({
      data: {
        siteId,
        pageViews: totalViews,
        uniqueVisitors: totalVisitors,
        leadsCaptured: totalLeads,
        avgSessionDuration: avgSession,
        bounceRate: avgBounce,
        topPages: rows[rows.length - 1]?.top_pages ?? [],
        trafficSources: rows[rows.length - 1]?.traffic_sources ?? {},
        daily: rows,
      },
    });
  } catch (err) {
    console.error('[Analytics Service] GET /analytics:', err);
    res.status(500).json({ error: (err as Error).message });
  }
});

// Page view ingest (called from published site tracker script)
app.post('/analytics/events/pageview', async (req, res) => {
  try {
    const { siteId, path, sessionId } = req.body;
    await publishEvent(KAFKA_TOPICS.PAGE_VIEWED, {
      siteId, path, sessionId,
      timestamp: new Date().toISOString(),
    });
    res.status(202).json({ ok: true });
  } catch {
    res.status(202).json({ ok: true }); // Always 202 — never break tracking pixels
  }
});

// ─── Kafka Consumer (optional) ────────────────────────────────────────────────

async function startKafkaConsumer(): Promise<void> {
  await startConsumer({
    groupId: 'analytics-service-group',
    topics: [KAFKA_TOPICS.PAGE_VIEWED, KAFKA_TOPICS.LEAD_CREATED],
    handler: async (message) => {
      const today = new Date().toISOString().split('T')[0];

      if (message.topic === KAFKA_TOPICS.PAGE_VIEWED) {
        const { siteId } = message.payload as { siteId: string };
        await query(
          `INSERT INTO site_analytics_daily (id, site_id, date, page_views)
           VALUES ($1,$2,$3,1)
           ON CONFLICT (site_id, date)
           DO UPDATE SET page_views = site_analytics_daily.page_views + 1`,
          [uuidv4(), siteId, today]
        ).catch(console.error);
      }

      if (message.topic === KAFKA_TOPICS.LEAD_CREATED) {
        const { siteId } = message.payload as { siteId: string };
        await query(
          `INSERT INTO site_analytics_daily (id, site_id, date, leads_captured)
           VALUES ($1,$2,$3,1)
           ON CONFLICT (site_id, date)
           DO UPDATE SET leads_captured = site_analytics_daily.leads_captured + 1`,
          [uuidv4(), siteId, today]
        ).catch(console.error);
      }
    },
  });
}

// ─── Bootstrap ────────────────────────────────────────────────────────────────

async function bootstrap() {
  const dbOk = await checkDbHealth();
  if (!dbOk) {
    console.error('[Analytics Service] Cannot connect to PostgreSQL. Check your .env POSTGRES_* settings.');
    process.exit(1);
  }
  console.log('[Analytics Service] PostgreSQL connected');

  await startKafkaConsumer().catch((err) => {
    console.warn('[Analytics Service] Kafka consumer failed (non-fatal):', (err as Error).message);
  });

  app.listen(PORT, () => console.log(`[Analytics Service] Running on port ${PORT}`));
}

process.on('unhandledRejection', (reason) => {
  console.warn('[Analytics Service] Unhandled rejection (non-fatal):', reason);
});

bootstrap().catch((err) => {
  console.error('[Analytics Service] Fatal startup error:', err);
  process.exit(1);
});

