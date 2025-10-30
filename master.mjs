
import { importPlans } from './planner.mjs';
import { processDueJobs } from './runner.mjs';
import db from './db.mjs';

// Initial import on startup
await importPlans();

// Schedule periodic imports
let plannerInterval = setInterval(importPlans, 1000 * 60 * 60); // every hour

// Initial job processing on startup
await processDueJobs();

// Schedule job processing every minute
let runnerInterval = setInterval(processDueJobs, 1000 * 60);

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
