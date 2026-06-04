// EstateEdge — AI Service: Lead Scoring Handler
// Uses Claude to analyze lead data and assign an intelligence score

import Anthropic from '@anthropic-ai/sdk';
import { query, queryOne } from '../../shared/db';
import { publishEvent } from '../../shared/kafka';
import { KAFKA_TOPICS } from '../../shared/types';

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

interface LeadRow {
  id: string;
  email: string;
  first_name: string;
  last_name: string;
  phone: string;
  message: string;
  source: string;
  metadata: Record<string, unknown>;
  created_at: string;
}

export async function scoreLeadWithAI(leadId: string, siteId: string): Promise<void> {
  const lead = await queryOne<LeadRow>(
    `SELECT * FROM leads WHERE id = $1 AND site_id = $2`,
    [leadId, siteId]
  );

  if (!lead) {
    console.warn(`[AI Score] Lead ${leadId} not found`);
    return;
  }

  const prompt = `Score this real estate lead on a scale of 0-100 for purchase intent and quality.

Lead data:
- Name: ${lead.first_name ?? 'Unknown'} ${lead.last_name ?? ''}
- Email: ${lead.email}
- Phone: ${lead.phone ? 'Provided' : 'Not provided'}
- Source: ${lead.source}
- Message: ${lead.message ?? 'No message'}
- Submitted: ${lead.created_at}
- Metadata: ${JSON.stringify(lead.metadata)}

Return ONLY a JSON object:
{
  "score": <0-100>,
  "priority": "hot|warm|cold",
  "reasoning": "One sentence explanation",
  "suggestedAction": "Recommended next step for the agent"
}`;

  const response = await anthropic.messages.create({
    model: 'claude-sonnet-4-20250514',
    max_tokens: 256,
    messages: [{ role: 'user', content: prompt }],
  });

  const raw = response.content
    .filter((b) => b.type === 'text')
    .map((b) => (b as { type: 'text'; text: string }).text)
    .join('')
    .replace(/```json|```/g, '')
    .trim();

  let scored: { score: number; priority: string; reasoning: string; suggestedAction: string };
  try {
    scored = JSON.parse(raw);
  } catch {
    console.error('[AI Score] Failed to parse score JSON', raw);
    return;
  }

  // Update lead score in database
  await query(
    `UPDATE leads SET score = $1, metadata = metadata || $2::jsonb, updated_at = NOW() WHERE id = $3`,
    [
      scored.score,
      JSON.stringify({
        aiScore: scored.score,
        aiPriority: scored.priority,
        aiReasoning: scored.reasoning,
        aiSuggestedAction: scored.suggestedAction,
        aiScoredAt: new Date().toISOString(),
      }),
      leadId,
    ]
  );

  // Publish scored event
  await publishEvent(KAFKA_TOPICS.LEAD_SCORED, {
    leadId,
    siteId,
    score: scored.score,
    priority: scored.priority,
    suggestedAction: scored.suggestedAction,
  });

  console.log(`[AI Score] Lead ${leadId} scored: ${scored.score} (${scored.priority})`);
}

export async function generateMarketReport(_input: unknown): Promise<unknown> {
  // Placeholder — implemented in generateMarketReport.ts
  return {};
}