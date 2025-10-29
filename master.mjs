
import { importPlans } from './planner.mjs';
import { processDueJobs } from './runner.mjs';
import { open } from 'sqlite';
import sqlite3 from 'sqlite3';

// Open the database once and share it
const db = await open({
  filename: 'data/planner.db',
  driver: sqlite3.Database
});

// Enable WAL mode for better concurrency
await db.exec('PRAGMA journal_mode = WAL;');

// Initial import on startup
await importPlans(db);

// Schedule periodic imports
let plannerInterval = setInterval(() => importPlans(db), 1000 * 60 * 60); // every hour

// Initial job processing on startup
await processDueJobs(db);

// Schedule job processing every minute
let runnerInterval = setInterval(() => processDueJobs(db), 1000 * 60);

// Handle graceful shutdown

function shutdownHandler(signal) {
  clearInterval(plannerInterval);
  clearInterval(runnerInterval);
  db.close().then(() => {
    console.log(`Received ${signal}. Cleared intervals, closed DB, and exiting.`);
    process.exit(0);
  });
}

process.on('SIGINT', () => shutdownHandler('SIGINT'));
process.on('SIGTERM', () => shutdownHandler('SIGTERM'));
