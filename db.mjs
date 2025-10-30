import { open } from 'sqlite';
import sqlite3 from 'sqlite3';

// Open the database once and share it
const db = await open({
  filename: 'data/planner.db',
  driver: sqlite3.Database
});

// Enable WAL mode for better concurrency
await db.exec('PRAGMA journal_mode = WAL;');

export default db;
