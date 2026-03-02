import * as admin from 'firebase-admin';
import type { EngineStatus } from './types';

declare global {
  // eslint-disable-next-line no-var
  var __FIREBASE_ADMIN_APP__: admin.app.App | undefined;
  // eslint-disable-next-line no-var
  var __FIREBASE_ADMIN_DB__: admin.firestore.Firestore | undefined;
}

function initializeFirebase(): admin.app.App {
  if (globalThis.__FIREBASE_ADMIN_APP__) return globalThis.__FIREBASE_ADMIN_APP__;
  if (admin.apps.length > 0 && admin.apps[0]) {
    globalThis.__FIREBASE_ADMIN_APP__ = admin.apps[0];
    return admin.apps[0];
  }

  const projectId   = process.env.FIREBASE_PROJECT_ID;
  const privateKey  = process.env.FIREBASE_PRIVATE_KEY;
  const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;

  if (!projectId || !privateKey || !clientEmail) {
    throw new Error(
      'Firebase Admin: Missing env vars. Set FIREBASE_PROJECT_ID, FIREBASE_PRIVATE_KEY, FIREBASE_CLIENT_EMAIL.'
    );
  }

  const app = admin.initializeApp({
    credential: admin.credential.cert({
      projectId,
      clientEmail,
      privateKey: privateKey.replace(/\\n/g, '\n'),
    }),
  });

  globalThis.__FIREBASE_ADMIN_APP__ = app;
  console.log('✅ Firebase Admin initialized (env-based, HMR-safe)');
  return app;
}

function getDb(): admin.firestore.Firestore {
  if (globalThis.__FIREBASE_ADMIN_DB__) return globalThis.__FIREBASE_ADMIN_DB__;
  const app = initializeFirebase();
  const database = admin.firestore(app);
  try { database.settings({ ignoreUndefinedProperties: true }); } catch {}
  globalThis.__FIREBASE_ADMIN_DB__ = database;
  return database;
}

export const db: admin.firestore.Firestore = new Proxy(
  {} as admin.firestore.Firestore,
  {
    get(_target, prop, receiver) {
      const realDb = getDb();
      const value  = Reflect.get(realDb, prop, receiver);
      if (typeof value === 'function') return value.bind(realDb);
      return value;
    },
  }
);

export async function updateEngineHeartbeat(): Promise<void> {
  try {
    const database = getDb();
    const data: Omit<EngineStatus, 'lastRunAt'> & { lastRunAt: admin.firestore.FieldValue } = {
      lastRunAt: admin.firestore.FieldValue.serverTimestamp(),
      status: 'running',
      message: 'GitHub Auto-Pilot is running background tasks',
    };
    await database.collection('system').doc('engine_status').set(data, { merge: true });
    console.log('💓 Heartbeat updated — Engine is ONLINE');
  } catch (error) {
    console.error('❌ Heartbeat update failed:', error);
  }
}

export { admin };
