
import { importPlans } from './planner.mjs';
import { processPlans } from './monitor.mjs';
import db from './db.mjs';

// Initial import on startup
await importPlans();

// Schedule periodic imports
let plannerInterval = setInterval(importPlans, 1000 * 60 * 60); // every hour

// Initial plan processing on startup
await processPlans();

// Schedule plan processing every minute
let runnerInterval = setInterval(processPlans, 1000 * 60);

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
