import type { firestore } from 'firebase-admin';

export type TaskStatus = 'pending' | 'processing' | 'completed' | 'failed';
export type LinkStatus = 'pending' | 'processing' | 'completed' | 'failed';

export interface ExtractedLink {
  id: string;
  name: string;
  link: string;
  status: LinkStatus;
  finalLink?: string;
  error?: string;
  solvedBy?: string;
  retryCount?: number;
}

export interface MovieMetadata {
  quality: string;
  languages: string;
  audioLabel: string;
}

export interface MoviePreview {
  title: string;
  posterUrl: string | null;
}

export interface ScrapingTask {
  id: string;
  url: string;
  status: TaskStatus;
  links?: ExtractedLink[];
  metadata?: MovieMetadata;
  preview?: MoviePreview;
  createdAt: firestore.Timestamp | Date;
  updatedAt: firestore.Timestamp | Date;
  completedAt?: firestore.Timestamp | Date;
  error?: string;
  retryCount?: number;
  totalLinks?: number;
  completedLinks?: number;
  failedLinks?: number;
}

export interface QueueItem {
  id: string;
  url: string;
  addedAt: firestore.Timestamp | Date;
  retryCount: number;
  status: TaskStatus;
  taskId?: string;
}

export interface HubCloudButton {
  button_name: string;
  download_link: string;
}

export interface HubCloudNativeResult {
  status: 'success' | 'error';
  best_button_name?: string;
  best_download_link?: string;
  all_available_buttons?: HubCloudButton[];
  message?: string;
}

export interface ExtractMovieLinksResult {
  status: 'success' | 'error';
  total?: number;
  links?: Array<{ name: string; link: string }>;
  metadata?: MovieMetadata;
  preview?: MoviePreview;
  message?: string;
}

export interface HBLinksResult {
  status: 'success' | 'fail' | 'error';
  link?: string;
  source?: string;
  message?: string;
}

export interface HubCDNResult {
  status: 'success' | 'failed' | 'error';
  final_link?: string;
  message?: string;
}

export interface HubDriveResult {
  status: 'success' | 'fail' | 'error';
  link?: string;
  message?: string;
}

export interface GadgetsWebResult {
  status: 'success' | 'error';
  link?: string;
  message?: string;
}

export interface EngineStatus {
  lastRunAt: firestore.Timestamp | Date;
  status: 'running' | 'offline';
  message: string;
}

export interface AdminAnalytics {
  totalTasks: number;
  completedTasks: number;
  failedTasks: number;
  pendingTasks: number;
  processingTasks: number;
  totalLinksProcessed: number;
  successRate: number;
}
