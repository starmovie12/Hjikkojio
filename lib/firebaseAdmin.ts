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
  
  // Check if already initialized
  if (admin.apps.length > 0 && admin.apps[0]) {
    globalThis.__FIREBASE_ADMIN_APP__ = admin.apps[0];
    return admin.apps[0];
  }

  // 👇 JSON details directly added here
  const projectId = "bhaag-df531";
  const clientEmail = "firebase-adminsdk-5pplx@bhaag-df531.iam.gserviceaccount.com";
  const privateKey = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDNVzaGeal14g/T\naiZZ4MLMJWKVO2jxLqN1rxQL+YSA94NikMppMuGM9w5lOZL1xgIU33HnOK/m3im+\nvFxINO2xYFdrW/1KlIzV5QkVe4tA6NFLlsjalDOYRQ6P+BlGucijhkrHHOriCotU\n2ZWtBaY4z0mDwxL7Ozhm+kzG7cdv8beWBY1QHflGKa6Du8TVK/4EFfrCL/TFYm+6\n2aqzMG2tTOvDX1kmmAyoaIuwLQzEySupdxxDD3Z9zNDSQuSqo/lhQd5q4Pupw7Bk\nhjRNj/ojvdziQmxx+37bWnjrWo2eUhie8XTqPGXNgI6ODvYr2/S+25dG/rBS3kOb\ntfwD+pe1AgMBAAECggEACj3uNCE6W20LTAi7NqLCz0A9crhpYL+U8J2VxY53APXC\nLGGQxFQriDFEcGEW8ZekP+/wiaS8BVW8cE012ct7YEGwFTnxNXiZA7bY+qTUX8hH\ntC+mVLICrCF5pfhXzmgdgqaD8VAsblox8+lRvAplmDzNQRCy+tahLBBnpvPHjaTt\nG3ZmvM08b2t5F/4CC4FvUOZJCPURe0H1W1tCsbD66mZ0XdEscFs62OBgghxchPo/\nPpTDYbjyfCesVbZMpN/YMNPukvlQPRFvZQlsDF939aoX1d39vLWYl9Rb+HSFHB4v\nT45wuWyWU1ff9Ei5vRc/X6DBrBsQzqLyeBGvc8aWCwKBgQDmvYoArWHJdfGqtVp/\nFSHR5hUyRiwUxvJYP4VTF/InuEjFiGEFeW+doBqW3MhqeJL7D3a4U1zh0zGsXdFg\nwj+H/a81acOBxAeNJrIPp9tz4CyfbRD/WYTpE9QZthaFAhbb1XEAxkOjB/2lYZ7F\nfu30iG5x6OpdAQWwwfwnNpi5vwKBgQDj0dnqs+NkJFY0U0XzVc51L3WfLh9UTk+s\nWQSjp4nGJIZQk+rfHVg3XjLu+qpZwEfEb3MWENn4XfLvTPzw5WrSDkp2B0WOmFHQ\nejYOjWMM5194fA8rBVQC4Op2K0SrCebvvroPC8LLha6CW+99c7iru6t5fTDuMt2J\n1HQg/j+DiwKBgG6hJc+ZUa9EC3Crrw4LVcHLrRIDrxLvKDbDjer/Ki19H/cFom77\ngFZ08wquJLFXyjDxgxxxa7Eij0hzWvYnbEqJiT30zbYkBPLaQLlc4801CHAP3Pxx\nMVaHGUSSl02CaO9R8PJMHRXHuQdYPMW4S+LYnwuifuvEl7Pd7kXm2WcFAoGAcnCi\n7/RHQLHEH+rI07CB0nnxsvF/SWFBQolA/FiXq9IDKozzSfq6qq2GFmgSlJ0zL+jw\nPeBfLhU7iJv636PO4g/NtbZ3aWb4iiop52t8mynK2oIvOGQnzH5hKNUZXHXP7RS4\n7//vbT8M52z5Q+KSnKncPF3361/fy3HOmBg/nXsCgYEAstIXAFnz9VBhRDVND4Nl\n8y5x6CML+F1iHjLLq6lzKgjjgcEy/D6GUU91ZatPH38sfuQFYjc4rQfZu1O46Q1w\nWuA1edciuVH3KrdOXGxHIFPJ8LxNQIdwjfqO2nVJySaCTvu8sLlxdpCkNm/WGZ7X\niW0uC51EiFYqkMlQdjjNxW4=\n-----END PRIVATE KEY-----\n";

  const app = admin.initializeApp({
    credential: admin.credential.cert({
      projectId,
      clientEmail,
      privateKey,
    }),
  });

  globalThis.__FIREBASE_ADMIN_APP__ = app;
  console.log('✅ Firebase Admin initialized (Direct Keys)');
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
