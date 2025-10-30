
import config from 'config';
import timespan from 'timespan-parser';
import db from './db.mjs';

let runningJobs = new Set();

/**
 * Process jobs that are due to start (start_at <= now and not yet started).
 */
export async function processDueJobs() {
  const now = Date.now();

  // Subtract time lag from config
  const timespanParser = timespan({ unit: 'ms' });
  const timeLag = timespanParser.parse(config.get('timeLag'));
  const adjustedNow = now - timeLag;
  const minimumDuration = timespanParser.parse(config.get('minimumDuration'));

  // You may need to add a 'status' column to node_plan_job for production use
  // For now, we'll assume jobs are always eligible if their start time is due
  const jobs = await db.all(`
    SELECT
      np.node_id,
      np.gpu_class_id,
      npj.node_plan_id,
      npj.order_index,
      npj.start_at + npj.duration - $adjustedNow AS adjusted_duration,
      (npj.start_at + npj.duration - $adjustedNow) / CAST(npj.duration AS REAL) * npj.invoice_amount AS adjusted_invoice_amount
    FROM node_plan_job npj
    JOIN node_plan np ON np.id = npj.node_plan_id
    WHERE $adjustedNow > npj.start_at
      AND $adjustedNow < npj.start_at + npj.duration
      AND npj.start_at + npj.duration - $adjustedNow > $minimumDuration
  `, {
    $adjustedNow: adjustedNow,
    $minimumDuration: minimumDuration,
  });

  console.log(`Processing ${jobs.length} due jobs at ${new Date(now).toISOString()}`);

  for (const job of jobs) {
    // Skip if already processing
    if (runningJobs.has(job.node_plan_id + '-' + job.order_index)) {
      console.log(`Job for node_id=${job.node_id} (plan_id=${job.node_plan_id}) is already in progress. Skipping.`);
      continue;
    }
    // Kick off the job
    runningJobs.add(job.node_plan_id + '-' + job.order_index);
    console.log(`Kicking off job for node_id=${job.node_id} (plan_id=${job.node_plan_id}) at ${new Date(now).toISOString()}`);
  }
}
