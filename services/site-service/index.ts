
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import { v4 as uuidv4 } from 'uuid';
import axios from 'axios';
import { query, queryOne, queryMany, withTransaction, checkDbHealth } from '../../shared/db';
import { publishEvent, startConsumer } from '../../shared/kafka';
import { KAFKA_TOPICS, SiteGenerationInput } from '../../shared/types';

const app = express();
const PORT = process.env.SITE_SERVICE_PORT ?? 4001;
const AI_SERVICE_URL = process.env.AI_SERVICE_URL ?? 'http://localhost:4002';
// const KAFKA_ENABLED = process.env.KAFKA_ENABLED !== 'false';
const KAFKA_ENABLED = process.env.KAFKA_ENABLED === 'true';

app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '5mb' }));

// ─── Health ───────────────────────────────────────────────────────────────────
console.log("🔥 SITE SERVICE BOOT FILE STARTED");
app.get('/health', async (_req, res) => {
  const dbOk = await checkDbHealth();
  res.json({ service: 'site-service', status: dbOk ? 'ok' : 'degraded', timestamp: new Date().toISOString() });
});

// ─── Sites ────────────────────────────────────────────────────────────────────

app.get('/sites', async (req, res) => {
  try {
    const userId = req.headers['x-user-id'] as string;
    if (!userId) return res.status(401).json({ error: 'Unauthorized' });
    const sites = await queryMany(
      `SELECT * FROM sites WHERE user_id = $1 ORDER BY updated_at DESC`,
      [userId]
    );
    res.json({ data: sites });
  } catch (err) {
    console.error('[Site Service] GET /sites:', err);
    res.status(500).json({ error: (err as Error).message });
  }
});

app.get('/sites/by-subdomain/:subdomain', async (req, res) => {
  try {
    const site = await queryOne(
      `SELECT s.*, COALESCE(
  json_agg(
    json_build_object(
      'id', p.id,
      'title', p.title,
      'slug', p.slug,
      'page_type', p.page_type,
      'content', p.content,
      'seo', p.seo,
      'status', p.status,
      'sort_order', p.sort_order
    ) ORDER BY p.sort_order
  ) FILTER (WHERE p.id IS NOT NULL),
  '[]'
) as pages
       FROM sites s LEFT JOIN pages p ON p.site_id = s.id
       WHERE s.subdomain = $1 AND s.status = 'published' GROUP BY s.id`,
      [req.params.subdomain]
    );
    if (!site) return res.status(404).json({ error: 'Site not found' });
    res.json({ data: site });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

app.get('/sites/by-domain/:domain', async (req, res) => {
  try {
    const site = await queryOne(
      `SELECT s.*, json_agg(
         json_build_object('id', p.id, 'title', p.title, 'slug', p.slug,
           'page_type', p.page_type, 'content', p.content, 'seo', p.seo,
           'status', p.status, 'sort_order', p.sort_order) ORDER BY p.sort_order
       ) FILTER (WHERE p.id IS NOT NULL) as pages
       FROM sites s LEFT JOIN pages p ON p.site_id = s.id
      WHERE s.subdomain = $1 AND s.status IN ('published', 'draft')`,
      [req.params.domain]
    );
    if (!site) return res.status(404).json({ error: 'Site not found' });
    res.json({ data: site });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

app.get('/sites/:id', async (req, res) => {
  try {
    const site = await queryOne(`SELECT * FROM sites WHERE id = $1`, [req.params.id]);
    if (!site) return res.status(404).json({ error: 'Site not found' });
    res.json({ data: site });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

app.post('/sites', async (req, res) => {
  try {
    const userId = req.headers['x-user-id'] as string;
    if (!userId) return res.status(401).json({ error: 'Unauthorized' });
    const { name, slug, theme, settings, seo } = req.body;
    const id = uuidv4();
    const subdomain = slug + '-' + id.split('-')[0];
    const site = await queryOne(
      `INSERT INTO sites (id, user_id, name, slug, subdomain, theme, settings, seo)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [id, userId, name, slug, subdomain,
        JSON.stringify(theme ?? {}), JSON.stringify(settings ?? {}), JSON.stringify(seo ?? {})]
    );
    await publishEvent(KAFKA_TOPICS.SITE_CREATED, { siteId: id, userId });
    res.status(201).json({ data: site });
  } catch (err) {
    console.error('[Site Service] POST /sites:', err);
    res.status(500).json({ error: (err as Error).message });
  }
});

app.patch('/sites/:id', async (req, res) => {
  try {
    const { name, theme, settings, seo } = req.body;
    const site = await queryOne(
      `UPDATE sites SET
         name = COALESCE($1, name),
         theme = COALESCE($2::jsonb, theme),
         settings = COALESCE($3::jsonb, settings),
         seo = COALESCE($4::jsonb, seo),
         updated_at = NOW()
       WHERE id = $5 RETURNING *`,
      [name ?? null, theme ? JSON.stringify(theme) : null,
       settings ? JSON.stringify(settings) : null,
       seo ? JSON.stringify(seo) : null, req.params.id]
    );
    res.json({ data: site });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

app.post('/sites/:id/publish', async (req, res) => {
  try {
    const userId = req.headers['x-user-id'] as string;
    const site = await queryOne(
      `UPDATE sites SET status = 'published', published_at = NOW(), updated_at = NOW()
       WHERE id = $1 RETURNING *`,
      [req.params.id]
    );
    await publishEvent(KAFKA_TOPICS.SITE_PUBLISHED, { siteId: req.params.id, userId });
    res.json({ data: site });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

app.delete('/sites/:id', async (req, res) => {
  try {
    const userId = req.headers['x-user-id'] as string;
    await query(`DELETE FROM sites WHERE id = $1 AND user_id = $2`, [req.params.id, userId]);
    res.json({ data: { success: true } });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

// ─── Pages ────────────────────────────────────────────────────────────────────

app.get('/sites/:siteId/pages', async (req, res) => {
  try {
    const pages = await queryMany(
      `SELECT * FROM pages WHERE site_id = $1 ORDER BY sort_order ASC`,
      [req.params.siteId]
    );
    res.json({ data: pages });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

app.post('/sites/:siteId/pages', async (req, res) => {
  try {
    const { title, slug, pageType, content, seo, sortOrder } = req.body;
    const page = await queryOne(
      `INSERT INTO pages (id, site_id, title, slug, page_type, content, seo, sort_order)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [uuidv4(), req.params.siteId, title, slug, pageType ?? 'custom',
       JSON.stringify(content ?? { blocks: [] }), JSON.stringify(seo ?? {}), sortOrder ?? 0]
    );
    res.status(201).json({ data: page });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

app.get('/pages/:id', async (req, res) => {
  try {
    const page = await queryOne(`SELECT * FROM pages WHERE id = $1`, [req.params.id]);
    if (!page) return res.status(404).json({ error: 'Page not found' });
    res.json({ data: page });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

app.patch('/pages/:id', async (req, res) => {
  try {
    const { title, content, seo, status } = req.body;
    const page = await queryOne(
      `UPDATE pages SET
         title = COALESCE($1, title),
         content = COALESCE($2::jsonb, content),
         seo = COALESCE($3::jsonb, seo),
         status = COALESCE($4, status),
         updated_at = NOW()
       WHERE id = $5 RETURNING *`,
      [title ?? null, content ? JSON.stringify(content) : null,
       seo ? JSON.stringify(seo) : null, status ?? null, req.params.id]
    );
    res.json({ data: page });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

// ─── AI Site Generation ───────────────────────────────────────────────────────
// KEY FIX: When Kafka is disabled, we call ai-service directly (synchronous HTTP)
// instead of publishing a Kafka event and waiting. This means the response
// comes back with the siteId already set — no polling needed.


app.post('/generate', async (req, res) => {
  try {
    console.log('STEP 1: entered /generate');

    const userId = req.headers['x-user-id'] as string;

    if (!userId) {
      console.log('STEP 1A: no userId');
      return res.status(401).json({ error: 'Unauthorized' });
    }

    console.log('STEP 1B: userId =', userId);

    const input: SiteGenerationInput = req.body;
    const jobId = uuidv4();

    // Create the job record
    await query(
      `INSERT INTO generation_jobs (id, user_id, status, input)
       VALUES ($1,$2,'pending',$3)`,
      [jobId, userId, JSON.stringify(input)]
    );

    console.log('STEP 2: job created', jobId);
    console.log('STEP 2A: KAFKA_ENABLED =', KAFKA_ENABLED);

    if (!KAFKA_ENABLED) {
      console.log('STEP 3: entering sync path');

      await query(
        `UPDATE generation_jobs
         SET status='processing', updated_at=NOW()
         WHERE id=$1`,
        [jobId]
      );

      try {
        console.log('STEP 4: calling AI service');
        console.log('AI URL =', `${AI_SERVICE_URL}/generate/site`);

        const aiRes = await axios.post(
          `${AI_SERVICE_URL}/generate/site`,
          input,
          {
            headers: {
              'x-user-id': userId,
            },
            timeout: 120000,
          }
        );

        console.log('STEP 5: AI service responded');
        console.log('AI RESPONSE:', JSON.stringify(aiRes.data, null, 2));

        const { site: spec, tokensUsed, durationMs } = aiRes.data.data;

        console.log('STEP 6: building site');
        

        const siteId = await buildSiteFromSpec(
          aiRes.data.data.site.websiteSpec,
          userId,
          jobId,
          tokensUsed,
          durationMs
        );

        console.log('STEP 7: site built', siteId);

        const job = await queryOne(
          `SELECT * FROM generation_jobs WHERE id=$1`,
          [jobId]
        );

        console.log(
          'STEP 8: returning job',
          JSON.stringify(job, null, 2)
        );
  
        console.log(
  "FULL WEBSITE SPEC:",
  JSON.stringify(spec, null, 2)
);

        return res.status(201).json({ data: job });

      } catch (aiErr) {
        console.error('FULL AI ERROR:', aiErr);

        const errMsg =
          aiErr instanceof Error
            ? `${aiErr.name}: ${aiErr.message}`
            : JSON.stringify(aiErr);

        console.error(
          '[Site Service] Direct AI call failed:',
          errMsg
        );

        await query(
          `UPDATE generation_jobs
           SET status='failed',
               error=$1,
               updated_at=NOW()
           WHERE id=$2`,
          [errMsg, jobId]
        );

        return res.status(500).json({ error: errMsg });
      }

    } else {
      console.log('STEP KAFKA: publishing event');

      await publishEvent(
        KAFKA_TOPICS.AI_GENERATION_REQUESTED,
        { jobId, userId, input }
      );

      return res.status(202).json({
        data: {
          id: jobId,
          status: 'pending',
          message: 'Site generation started',
        },
      });
    }

  } catch (err) {
    console.error('OUTER ERROR:', err);

    return res.status(500).json({
      error: err instanceof Error ? err.message : 'Unknown error',
    });
  }
});

// ─── Helper: Build site from AI spec ─────────────────────────────────────────

async function buildSiteFromSpec(
  spec: Record<string, unknown>,
  userId: string,
  jobId: string,
  tokensUsed?: number,
  durationMs?: number
): Promise<string> {
  
  return await withTransaction(async (client) => {
    const siteId = uuidv4();
    const rawName = String(spec.name ?? 'My Real Estate Site');
    const slug = rawName.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');
    const subdomain = slug + '-' + siteId.split('-')[0];
  
    await client.query(
      `INSERT INTO sites (id, user_id, name, slug, subdomain, theme, seo, ai_generated, status)
       VALUES ($1,$2,$3,$4,$5,$6,$7,true,'draft')`,
      [siteId, userId, rawName, slug, subdomain,
       JSON.stringify(spec.theme ?? {}),
       JSON.stringify(spec.seo ?? {})]
    );

  const rawPages =
  Array.isArray(spec.pages)
    ? spec.pages
    : typeof spec.pages === "string"
      ? JSON.parse(spec.pages)
      : [];

const normalizedPages = rawPages.map((p: any, index: number) => ({
  title: p.title ?? p.name,

  slug:
    p.slug ??
    p.url?.replace(/^\/+/, '') ??
    `page-${index}`,

  pageType: p.pageType ?? 'custom',

  blocks: p.blocks ?? p.contentSections ?? [],

  seo: {
    title: p.name,
    description: Array.isArray(p.contentSections)
      ? String(p.contentSections[0]).slice(0, 160)
      : '',
  },
}));
    for (let i = 0; i < normalizedPages.length; i++) {
  const p = normalizedPages[i];

  await client.query(
    `INSERT INTO pages (
      id,
      site_id,
      title,
      slug,
      page_type,
      content,
      seo,
      sort_order,
      status
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'published')`,
    [
      uuidv4(),
      siteId,
      p.title,
      p.slug,
      p.pageType,
      JSON.stringify({ blocks: p.blocks }),
      JSON.stringify(p.seo),
      i,
    ]
  );
}
      console.log("📄 RAW SPEC PAGES:", spec.pages);
console.log("📄 PARSED PAGES:", normalizedPages);

    await client.query(
      `UPDATE generation_jobs
       SET status='completed', site_id=$1, tokens_used=$2, duration_ms=$3, updated_at=NOW()
       WHERE id=$4`,
      [siteId, tokensUsed ?? null, durationMs ?? null, jobId]
    );

    console.log(`[Site Service] Built site ${siteId} (${normalizedPages.length} pages) from job ${jobId}`);
    return siteId;
  });
}

// ─── Jobs ─────────────────────────────────────────────────────────────────────

app.get('/jobs', async (req, res) => {
  try {
    const userId = req.headers['x-user-id'] as string;
    const jobs = await queryMany(
      `SELECT * FROM generation_jobs WHERE user_id = $1 ORDER BY created_at DESC LIMIT 20`,
      [userId]
    );
    res.json({ data: jobs });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

app.get('/jobs/:jobId', async (req, res) => {
  try {
    const job = await queryOne(`SELECT * FROM generation_jobs WHERE id=$1`, [req.params.jobId]);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    res.json({ data: job });
  } catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

// ─── Kafka Consumer — handles async path when Kafka IS running ────────────────

async function startKafkaConsumer(): Promise<void> {
  await startConsumer({
    groupId: 'site-service-group',
    topics: [KAFKA_TOPICS.AI_GENERATION_COMPLETED],
    handler: async (message) => {
      if (message.topic !== KAFKA_TOPICS.AI_GENERATION_COMPLETED) return;
      const { jobId, userId, result, success, error } = message.payload as {
        jobId: string; userId: string;
        result?: { site: Record<string, unknown>; tokensUsed: number; durationMs: number };
        success: boolean; error?: string;
      };

      if (!success || !result?.site) {
        await query(
          `UPDATE generation_jobs SET status='failed', error=$1, updated_at=NOW() WHERE id=$2`,
          [error ?? 'Unknown AI error', jobId]
        );
        return;
      }

      await buildSiteFromSpec(result.site, userId, jobId, result.tokensUsed, result.durationMs)
        .then((siteId) => publishEvent(KAFKA_TOPICS.SITE_CREATED, { siteId, userId, aiGenerated: true }))
        .catch((err) => console.error('[Site Service] Kafka build site error:', err));
    },
  });
}

// ─── Bootstrap ────────────────────────────────────────────────────────────────

async function bootstrap() {
  const dbOk = await checkDbHealth();
  if (!dbOk) {
    console.error('[Site Service] Cannot connect to PostgreSQL. Check your .env POSTGRES_* settings.');
    process.exit(1);
  }
  console.log('[Site Service] PostgreSQL connected');
  console.log(`[Site Service] Generation mode: ${KAFKA_ENABLED ? 'async (Kafka)' : 'sync (direct HTTP)'}`);

  await startKafkaConsumer().catch((err) => {
    console.warn('[Site Service] Kafka consumer failed (non-fatal):', (err as Error).message);
  });

  app.listen(PORT, () => console.log(`[Site Service] Running on port ${PORT}`));
}

process.on('unhandledRejection', (reason) => {
  console.warn('[Site Service] Unhandled rejection (non-fatal):', reason);
});

bootstrap().catch((err) => {
  console.error('[Site Service] Fatal startup error:', err);
  process.exit(1);
});

