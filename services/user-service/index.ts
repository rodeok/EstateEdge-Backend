// EstateEdge — User Service
// Authentication, JWT management, user and brokerage CRUD

import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import { v4 as uuidv4 } from 'uuid';
import { query, queryOne, queryMany } from '../../shared/db';

const app = express();
const PORT = process.env.USER_SERVICE_PORT ?? 4003;
const JWT_SECRET = process.env.JWT_SECRET ?? 'dev-secret-change-in-production';
const JWT_EXPIRES = process.env.JWT_EXPIRES ?? '3d';
const REFRESH_EXPIRES_DAYS = 30;

app.use(helmet());
app.use(cors());
app.use(express.json());

// ─── Health ───────────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => res.json({ service: 'user-service', status: 'ok' }));

// ─── Auth ─────────────────────────────────────────────────────────────────────

app.post('/auth/register', async (req, res) => {
  try {
    const { email, password, firstName, lastName, role } = req.body;

    const existing = await queryOne(`SELECT id FROM users WHERE email = $1`, [email]);
    if (existing) return res.status(409).json({ error: 'Email already registered' });

    const passwordHash = await bcrypt.hash(password, 12);
    const user = await queryOne(
      `INSERT INTO users (id, email, password_hash, first_name, last_name, role)
       VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, email, first_name, last_name, role, created_at`,
      [uuidv4(), email, passwordHash, firstName, lastName, role ?? 'agent']
    );

    const { accessToken, refreshToken } = await issueTokens(user!.id as string);
    res.status(201).json({ data: { user, accessToken, refreshToken } });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

app.post('/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;

    const user = await queryOne(
      `SELECT * FROM users WHERE email = $1`,
      [email]
    );

    if (!user || !(await bcrypt.compare(password, user.password_hash as string))) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    await query(`UPDATE users SET last_login_at = NOW() WHERE id = $1`, [user.id]);
    const { accessToken, refreshToken } = await issueTokens(user.id as string);

    const { password_hash: _ph, ...safeUser } = user as Record<string, unknown>;
    res.json({ data: { user: safeUser, accessToken, refreshToken } });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

app.post('/auth/refresh', async (req, res) => {
  try {
    const { refreshToken } = req.body;
    if (!refreshToken) return res.status(400).json({ error: 'Refresh token required' });

    const tokenHash = hashToken(refreshToken);
    const stored = await queryOne(
      `SELECT * FROM refresh_tokens WHERE token_hash = $1 AND expires_at > NOW()`,
      [tokenHash]
    );

    if (!stored) return res.status(401).json({ error: 'Invalid or expired refresh token' });

    // Rotate refresh token
    await query(`DELETE FROM refresh_tokens WHERE id = $1`, [stored.id]);
    const { accessToken, refreshToken: newRefresh } = await issueTokens(stored.user_id as string);

    res.json({ data: { accessToken, refreshToken: newRefresh } });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

app.post('/auth/logout', async (req, res) => {
  try {
    const { refreshToken } = req.body;
    if (refreshToken) {
      await query(`DELETE FROM refresh_tokens WHERE token_hash = $1`, [hashToken(refreshToken)]);
    }
    res.json({ data: { success: true } });
  } catch {
    res.json({ data: { success: true } });
  }
});

// ─── User Profile ─────────────────────────────────────────────────────────────

app.get('/users/:id', async (req, res) => {
  try {
    const user = await queryOne(
      `SELECT id, email, first_name, last_name, role, avatar_url, phone, license_number, bio, brokerage_id, created_at
       FROM users WHERE id = $1`,
      [req.params.id]
    );
    if (!user) return res.status(404).json({ error: 'User not found' });
    res.json({ data: user });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

app.patch('/users/:id', async (req, res) => {
  try {
    const { firstName, lastName, phone, bio, avatarUrl, licenseNumber } = req.body;
    const user = await queryOne(
      `UPDATE users SET
         first_name = COALESCE($1, first_name),
         last_name = COALESCE($2, last_name),
         phone = COALESCE($3, phone),
         bio = COALESCE($4, bio),
         avatar_url = COALESCE($5, avatar_url),
         license_number = COALESCE($6, license_number),
         updated_at = NOW()
       WHERE id = $7
       RETURNING id, email, first_name, last_name, role, avatar_url, phone, license_number, bio`,
      [firstName, lastName, phone, bio, avatarUrl, licenseNumber, req.params.id]
    );
    res.json({ data: user });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// ─── Token Verification (used by gateway) ────────────────────────────────────

app.post('/auth/verify', async (req, res) => {
  try {
    const { token } = req.body;
    const payload = jwt.verify(token, JWT_SECRET) as { userId: string; role: string };
    res.json({ data: { valid: true, userId: payload.userId, role: payload.role } });
  } catch (err) {
  console.error("VERIFY ERROR:", err);
  res.status(401).json({
    data: {
      valid: false,
      error: (err as Error).message,
    },
  });
  }});

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function issueTokens(userId: string): Promise<{ accessToken: string; refreshToken: string }> {
  const user = await queryOne(`SELECT role FROM users WHERE id = $1`, [userId]);

  const accessToken = jwt.sign(
    { userId, role: user?.role },
    JWT_SECRET,
    { expiresIn: JWT_EXPIRES }
  );

  const refreshToken = uuidv4() + '-' + uuidv4();
  const tokenHash = hashToken(refreshToken);
  const expiresAt = new Date();
  expiresAt.setDate(expiresAt.getDate() + REFRESH_EXPIRES_DAYS);

  await query(
    `INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at) VALUES ($1, $2, $3, $4)`,
    [uuidv4(), userId, tokenHash, expiresAt.toISOString()]
  );

  return { accessToken, refreshToken };
}

function hashToken(token: string): string {
  const crypto = require('crypto');
  return crypto.createHash('sha256').update(token).digest('hex');
}

app.listen(PORT, () => console.log(`[User Service] Running on port ${PORT}`));