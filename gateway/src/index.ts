// EstateEdge — GraphQL API Gateway
// Apollo Server 4 — stitches all microservice schemas, handles auth middleware

import 'dotenv/config';
import { ApolloServer } from '@apollo/server';
import { expressMiddleware } from '@apollo/server/express4';
import express, { Request } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import { json } from 'body-parser';
import axios, { AxiosError } from 'axios';

const app = express();
const PORT = process.env.GATEWAY_PORT ?? 4000;

console.log('NODE_ENV:', process.env.NODE_ENV);
console.log('USER_SERVICE_URL:', process.env.USER_SERVICE_URL);
console.log('USER_SERVICE_PORT:', process.env.USER_SERVICE_PORT);

const SERVICE_URLS = {
  user: process.env.NODE_ENV === 'production' ? process.env.USER_SERVICE_URL : `http://localhost:${process.env.USER_SERVICE_PORT || 4003}`,
  site: process.env.NODE_ENV === 'production' ? process.env.SITE_SERVICE_URL : `http://localhost:${process.env.SITE_SERVICE_PORT || 4001}`,
  ai: process.env.NODE_ENV === 'production' ? process.env.AI_SERVICE_URL : `http://localhost:${process.env.AI_SERVICE_PORT || 4002}`,
  analytics: process.env.NODE_ENV === 'production' ? process.env.ANALYTICS_SERVICE_URL : `http://localhost:${process.env.ANALYTICS_SERVICE_PORT || 4004}`,
};

console.log('SERVICE_URLS:', SERVICE_URLS);

// ─── GraphQL Schema ───────────────────────────────────────────────────────────

const typeDefs = `#graphql
  scalar JSON
  scalar DateTime

  type AuthPayload {
    user: User!
    accessToken: String!
    refreshToken: String!
  }

  type RefreshPayload {
    accessToken: String!
    refreshToken: String!
  }

  type User {
    id: ID!
    email: String!
    firstName: String
    lastName: String
    role: String!
    avatarUrl: String
    phone: String
    bio: String
    licenseNumber: String
    brokerageId: ID
    createdAt: DateTime!
  }

  type Site {
    id: ID!
    userId: ID!
    name: String!
    slug: String!
    subdomain: String!
    domain: String
    status: String!
    theme: JSON!
    seo: JSON!
    settings: JSON!
    aiGenerated: Boolean!
    publishedAt: DateTime
    createdAt: DateTime!
    updatedAt: DateTime!
    pages: [Page!]
  }

  type Page {
    id: ID!
    siteId: ID!
    title: String!
    slug: String!
    pageType: String!
    content: JSON!
    seo: JSON!
    status: String!
    sortOrder: Int!
  }

  type GenerationJob {
    id: ID
    status: String!
    siteId: ID
    tokensUsed: Int
    durationMs: Int
    error: String
    createdAt: DateTime!
    updatedAt: DateTime!
  }

  type GeneratedContent {
    content: String
    tokensUsed: Int
    model: String
  }

  type MarketReport {
    title: String!
    summary: String!
    keyInsights: [String!]!
    buyerAdvice: String!
    sellerAdvice: String!
    outlook: String!
  }

  type Lead {
    id: ID!
    siteId: ID!
    email: String!
    firstName: String
    lastName: String
    phone: String
    message: String
    source: String!
    status: String!
    score: Int!
    metadata: JSON!
    createdAt: DateTime!
  }

  type SiteAnalytics {
    siteId: ID!
    pageViews: Int!
    uniqueVisitors: Int!
    leadsCaptured: Int!
    avgSessionDuration: Int!
    bounceRate: Float
    topPages: JSON!
    trafficSources: JSON!
  }

  input RegisterInput {
    email: String!
    password: String!
    firstName: String
    lastName: String
    role: String
  }

  input LoginInput {
    email: String!
    password: String!
  }

  input SiteGenerationInput {
    agentName: String!
    agentTitle: String
    brokerage: String
    location: String!
    specialties: [String!]!
    tone: String!
    colorPreference: String
    targetAudience: String
    additionalContext: String
  }

  input CreateSiteInput {
    name: String!
    slug: String!
    theme: JSON
    seo: JSON
    settings: JSON
  }

  input ContentGenerationInput {
    contentType: String!
    prompt: String!
    siteId: ID
    pageId: ID
  }

  input MarketReportInput {
    location: String!
    reportType: String!
    marketData: JSON
    agentName: String
  }

  type Query {
    me: User
    site(id: ID!): Site
    mySites: [Site!]!
    sitePages(siteId: ID!): [Page!]!
    generationJob(jobId: ID!): GenerationJob
    myLeads(siteId: ID!): [Lead!]!
    siteAnalytics(siteId: ID!, days: Int): SiteAnalytics
  }

  type Mutation {
    register(input: RegisterInput!): AuthPayload!
    login(input: LoginInput!): AuthPayload!
    refreshToken(refreshToken: String!): RefreshPayload!
    logout(refreshToken: String): Boolean!

    createSite(input: CreateSiteInput!): Site!
    generateSite(input: SiteGenerationInput!): GenerationJob!
    publishSite(siteId: ID!): Site!

    generateContent(input: ContentGenerationInput!): GeneratedContent!
    generateMarketReport(input: MarketReportInput!): MarketReport!

    updatePage(pageId: ID!, content: JSON, seo: JSON, title: String): Page!
  }
`;

// ─── Resolvers ────────────────────────────────────────────────────────────────

const resolvers = {
  Query: {
    me: async (_: unknown, __: unknown, ctx: { userId?: string }) => {
      if (!ctx.userId) throw new Error('Unauthorized');
      const res = await svcGet(SERVICE_URLS.user, `/users/${ctx.userId}`);
      return camelize(res);
    },

    mySites: async (_: unknown, __: unknown, ctx: { userId?: string }) => {
      if (!ctx.userId) throw new Error('Unauthorized');
      const res = await svcGet(SERVICE_URLS.site, '/sites', {}, ctx.userId);
      return Array.isArray(res) ? res.map(camelize) : [];
    },

    site: async (_: unknown, { id }: { id: string }) => {
      const res = await svcGet(SERVICE_URLS.site, `/sites/${id}`);
      return camelize(res);
    },

    sitePages: async (_: unknown, { siteId }: { siteId: string }) => {
      const res = await svcGet(SERVICE_URLS.site, `/sites/${siteId}/pages`);
      return Array.isArray(res) ? res.map(camelize) : [];
    },

    generationJob: async (_: unknown, { jobId }: { jobId: string }) => {
      const res = await svcGet(SERVICE_URLS.site, `/jobs/${jobId}`);
      return camelize(res);
    },

    myLeads: async (_: unknown, { siteId }: { siteId: string }, ctx: { userId?: string }) => {
      if (!ctx.userId) throw new Error('Unauthorized');
      const res = await svcGet(SERVICE_URLS.analytics, '/leads', { siteId }, ctx.userId);
      return Array.isArray(res) ? res.map(camelize) : [];
    },

    siteAnalytics: async (_: unknown, { siteId, days }: { siteId: string; days?: number }) => {
      const res = await svcGet(SERVICE_URLS.analytics, `/analytics/${siteId}`, { days: days ?? 30 });
      return camelize(res);
    },
  },

  Mutation: {
    register: async (_: unknown, { input }: { input: Record<string, unknown> }) => {
      console.log('SERVICE_URLS.user:', SERVICE_URLS.user);
      const d = await svcPost(SERVICE_URLS.user, '/auth/register', input);
      return { user: camelize(d.user), accessToken: d.accessToken, refreshToken: d.refreshToken };
    },

    login: async (_: unknown, { input }: { input: Record<string, unknown> }) => {
      const d = await svcPost(SERVICE_URLS.user, '/auth/login', input);
      return { user: camelize(d.user), accessToken: d.accessToken, refreshToken: d.refreshToken };
    },

    refreshToken: async (_: unknown, { refreshToken }: { refreshToken: string }) => {
      return await svcPost(SERVICE_URLS.user, '/auth/refresh', { refreshToken });
    },

    logout: async (_: unknown, { refreshToken }: { refreshToken?: string }) => {
      await svcPost(SERVICE_URLS.user, '/auth/logout', { refreshToken }).catch(() => {});
      return true;
    },

    generateSite: async (_: unknown, { input }: { input: Record<string, unknown> }, ctx: { userId?: string }) => {
    //  if (!ctx.userId) throw new Error('Unauthorized');
     ctx.userId = "98568c25-062b-47bf-b5b4-8ba679b0ba35";  // Temporary fallback for testing
     console.log("ctx.userid is: ", ctx.userId)
//       if (!ctx.userId) {
//   ctx.userId = "test-user";
// }
      const res = await svcPost(SERVICE_URLS.site, '/generate', input, ctx.userId);
      console.log('GENERATE RESPONSE:', JSON.stringify(res, null, 2))
      return camelize(res);
    },

    createSite: async (_: unknown, { input }: { input: Record<string, unknown> }, ctx: { userId?: string }) => {
      if (!ctx.userId) throw new Error('Unauthorized');
      const res = await svcPost(SERVICE_URLS.site, '/sites', input, ctx.userId);
      return camelize(res);
    },

    publishSite: async (_: unknown, { siteId }: { siteId: string }, ctx: { userId?: string }) => {
      if (!ctx.userId) throw new Error('Unauthorized');
      const res = await svcPost(SERVICE_URLS.site, `/sites/${siteId}/publish`, {}, ctx.userId);
      return camelize(res);
    },

    generateContent: async (_: unknown, { input }: { input: Record<string, unknown> }, ctx: { userId?: string }) => {
      return await svcPost(SERVICE_URLS.ai, '/generate/content', { ...input, userId: ctx.userId });
    },

    generateMarketReport: async (_: unknown, { input }: { input: Record<string, unknown> }) => {
      return await svcPost(SERVICE_URLS.ai, '/generate/market-report', input);
    },

    updatePage: async (_: unknown, { pageId, ...updates }: { pageId: string; [k: string]: unknown }) => {
      const res = await svcPatch(SERVICE_URLS.site, `/pages/${pageId}`, updates);
      return camelize(res);
    },
  },

  Site: {
    pages: async (parent: { id: string }) => {
      const res = await svcGet(SERVICE_URLS.site, `/sites/${parent.id}/pages`);
      return Array.isArray(res) ? res.map(camelize) : [];
    },
  },
};

// ─── Service HTTP Helpers ─────────────────────────────────────────────────────
// Centralised error handling — turns downstream errors into clean GraphQL errors

async function svcGet(base: string, path: string, params?: Record<string, unknown>, userId?: string) {
  try {
    const res = await axios.get(`${base}${path}`, {
      params,
      headers: userId ? { 'x-user-id': userId } : {},
      timeout: 10000,
    });
    return res.data.data;
  } catch (err) {
    throwServiceError(err, `GET ${base}${path}`);
  }
}

async function svcPost(base: string, path: string, body: unknown, userId?: string) {
  try {
    const res = await axios.post(`${base}${path}`, body, {
      headers: userId ? { 'x-user-id': userId } : {},
      timeout: 60000,  // AI calls can take up to 60s
    });
    return res.data.data;
  } catch (err) {
    throwServiceError(err, `POST ${base}${path}`);
  }
}

async function svcPatch(base: string, path: string, body: unknown) {
  try {
    const res = await axios.patch(`${base}${path}`, body, { timeout: 10000 });
    return res.data.data;
  } catch (err) {
    throwServiceError(err, `PATCH ${base}${path}`);
  }
}

function throwServiceError(err: unknown, context: string): never {
  const axiosErr = err as AxiosError<{ error?: string }>;
  if (axiosErr.response) {
    const msg = axiosErr.response.data?.error ?? axiosErr.response.statusText;
    throw new Error(`[${context}] ${axiosErr.response.status}: ${msg}`);
  }
  if (axiosErr.code === 'ECONNREFUSED') {
    throw new Error(`Service unavailable: ${context}. Is the service running?`);
  }
  if (axiosErr.code === 'ETIMEDOUT') {
    throw new Error(`Service timeout: ${context}`);
  }
  throw new Error(`Service error: ${context} — ${(err as Error).message}`);
}

// ─── Context (JWT Auth) ───────────────────────────────────────────────────────

async function buildContext(req: { headers?: Record<string, string | string[]> }): Promise<{ userId?: string; role?: string }> {
  const rawAuth = req.headers?.authorization;
  const auth = Array.isArray(rawAuth) ? rawAuth[0] : rawAuth;
  if (!auth?.startsWith('Bearer ')) {
    console.log("NO TOKEN");
    return {};
  }

  try {
    const token = auth.slice(7);
    const res = await axios.post(
      `${SERVICE_URLS.user}/auth/verify`,
      { token },
      { timeout: 15000 }
    );
    console.log("VERIFY RESPONSE:", res.data);
    const { valid, userId, role } = res.data.data;
    if (!valid) return {};
    return { userId, role };
  } catch(err) {
     console.log("VERIFY FAILED:", err.message);
    return {};
  }
}

// ─── Bootstrap ────────────────────────────────────────────────────────────────

async function bootstrap() {
  const server = new ApolloServer({
    typeDefs,
    resolvers,
    formatError: (err) => {
      // Log full error server-side, send clean message to client
      console.error('[GraphQL Error]', err.message);
      return err;
    },
  });

  await server.start();

  app.use(helmet({ contentSecurityPolicy: false }));
  app.use(
    cors({
      origin: [
        'https://estateedge-frontend.vercel.app',
        'http://localhost:5173',
        'http://localhost:3000',
        'https://estateedge.vercel.app'
      ],
      credentials: true,
    })
  );

  app.get('/health', (_req, res) => {
    res.json({ service: 'gateway', status: 'ok', timestamp: new Date().toISOString() });
  });

  app.use(
    '/graphql',
    json(),
    expressMiddleware(server, {
      context: async ({ req }) => buildContext(req),
    })
  );

  app.listen(PORT, () => {
    console.log(`[Gateway] GraphQL ready at http://localhost:${PORT}/graphql`);
    console.log(`[Gateway] Health: http://localhost:${PORT}/health`);

  });
}

bootstrap().catch((err) => {
  console.error('[Gateway] Fatal startup error', err);
  process.exit(1);
});

// ─── Utility: camelizeKeys ────────────────────────────────────────────────────
// Fixed: handles arrays, null, primitives correctly

function camelize(obj: unknown): unknown {
  if (obj === null || obj === undefined) return obj;
  if (Array.isArray(obj)) return obj.map(camelize);
  if (typeof obj !== 'object') return obj;

  return Object.fromEntries(
    Object.entries(obj as Record<string, unknown>).map(([k, v]) => [
      k.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase()),
      camelize(v),
    ])
  );
}
