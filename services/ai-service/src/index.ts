// EstateEdge — AI Service
// Handles all LLM interactions via Anthropic Claude
// Kafka consumer is OPTIONAL — service starts fine without Kafka

import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import { startConsumer, publishEvent } from '../../../shared/kafka';
import { KAFKA_TOPICS, KafkaMessage, SiteGenerationInput } from '../../../shared/types';
import { generateSite } from '../generateSite';
import { generateContent, ContentType } from '../generateContent';
import { scoreLeadWithAI } from '../scoreLead';
import { generateMarketReport } from '../generateMarketReport';
import { checkDbHealth } from '../../../shared/db';
const app = express();
const PORT = process.env.AI_SERVICE_PORT ?? 4002;

app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '2mb' }));

// ─── Health ───────────────────────────────────────────────────────────────────
console.log("🔥 AI SERVICE BOOT FILE STARTED");
console.log("PORT =", PORT);

app.get('/health', async (_req, res) => {
  const dbOk = await checkDbHealth();
  console.log("HEALTH HIT");
  res.json({
    service: 'ai-service',
    status: dbOk ? 'ok' : 'degraded',
    timestamp: new Date().toISOString(),
  });
});

// ─── REST Endpoints ───────────────────────────────────────────────────────────

app.post('/generate/site', async (req, res) => {
  try {
    const input: SiteGenerationInput = req.body;
    const userId: string = req.headers['x-user-id'] as string;
    const result = await generateSite(input, userId);
    res.json({ data: result });
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : 'Unknown error';
    console.error('[AI Service] /generate/site error:', msg);
    res.status(500).json({ error: msg });
  }
});

app.post('/generate/content', async (req, res) => {
  try {
    const result = await generateContent(req.body);
    res.json({ data: result });
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : 'Unknown error';
    console.error('[AI Service] /generate/content error:', msg);
    res.status(500).json({ error: msg });
  }
});

app.post('/generate/market-report', async (req, res) => {
  try {
    const result = await generateMarketReport(req.body);
    res.json({ data: result });
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : 'Unknown error';
    res.status(500).json({ error: msg });
  }
});

// ─── Kafka Consumer (optional) ────────────────────────────────────────────────

async function startKafkaConsumer(): Promise<void> {
  // startConsumer returns null if Kafka is disabled or unavailable — that's fine
  await startConsumer({
    groupId: 'ai-service-group',
    topics: [
      KAFKA_TOPICS.AI_GENERATION_REQUESTED,
      KAFKA_TOPICS.AI_CONTENT_REQUESTED,
      KAFKA_TOPICS.LEAD_CREATED,
    ],
    handler: async (message: KafkaMessage) => {
      switch (message.topic) {
        case KAFKA_TOPICS.AI_GENERATION_REQUESTED: {
          const { input, userId, jobId } = message.payload as {
            input: SiteGenerationInput;
            userId: string;
            jobId: string;
          };
          try {
            const result = await generateSite(input, userId, jobId);
            await publishEvent(KAFKA_TOPICS.AI_GENERATION_COMPLETED, {
              jobId, userId, result, success: true,
            });
          } catch (err) {
            await publishEvent(KAFKA_TOPICS.AI_GENERATION_COMPLETED, {
              jobId, userId,
              error: err instanceof Error ? err.message : 'AI generation failed',
              success: false,
            });
          }
          break;
        }

        case KAFKA_TOPICS.AI_CONTENT_REQUESTED: {
          const { contentType, prompt, userId, requestId, siteId, pageId } = message.payload as {
            contentType: string; prompt: string; userId: string;
            requestId: string; siteId?: string; pageId?: string;
          };
          try {
            const result = await generateContent({ contentType: contentType as ContentType, prompt, siteId, pageId });
            await publishEvent(KAFKA_TOPICS.AI_CONTENT_COMPLETED, {
              requestId, userId, result, success: true,
            });
          } catch (err) {
            await publishEvent(KAFKA_TOPICS.AI_CONTENT_COMPLETED, {
              requestId, userId,
              error: err instanceof Error ? err.message : 'Content generation failed',
              success: false,
            });
          }
          break;
        }

        case KAFKA_TOPICS.LEAD_CREATED: {
          const leadData = message.payload as { leadId: string; siteId: string };
          await scoreLeadWithAI(leadData.leadId, leadData.siteId).catch((err) => {
            console.error('[AI Service] Lead scoring failed', err);
          });
          break;
        }
      }
    },
  });
}

// ─── Bootstrap ────────────────────────────────────────────────────────────────

async function bootstrap() {
  // Start Kafka consumer — won't crash if Kafka is unavailable
  await startKafkaConsumer().catch((err) => {
    console.warn('[AI Service] Kafka consumer setup failed (non-fatal):', err.message);
  });

  app.listen(PORT, () => {
    console.log(`[AI Service] Running on port ${PORT}`);
  });
}

bootstrap().catch((err) => {
  console.error('[AI Service] Fatal startup error', err);
  process.exit(1);
});
