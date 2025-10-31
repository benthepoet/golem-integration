
import { importPlans } from './planner.mjs';
import { processPlans } from './monitor.mjs';
import db from './db.mjs';
import { glm, shutdown } from './glm.mjs';

// Handle graceful shutdown
process.on('SIGINT', () => shutdownHandler('SIGINT'));
process.on('SIGTERM', () => shutdownHandler('SIGTERM'));

// Initial import on startup
await importPlans();

// Schedule periodic imports
let plannerInterval = setInterval(importPlans, 1000 * 60 * 60); // every hour

// Connect to Golem Network
await glm.connect();

// Initial plan processing on startup
await processPlans();

// Schedule plan processing every minute
let runnerInterval = setInterval(processPlans, 1000 * 60);

function shutdownHandler(signal) {
  // Clear intervals
  clearInterval(plannerInterval);
  clearInterval(runnerInterval);

  // Abort any ongoing Golem operations
  shutdown.abort();

  // Disconnect from Golem Network
  glm.disconnect();

  // Close DB connection
  db.close()
    .then(() => {
      console.log(`Received ${signal}. Cleared intervals, closed DB, and exiting.`);
      process.exit(0);
    });
}
