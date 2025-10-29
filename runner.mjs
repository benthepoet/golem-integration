
/**
 * Process jobs that are due to start (start_at <= now and not yet started).
 * @param {import('sqlite').Database} db - The opened sqlite database instance.
 */
export async function processDueJobs(db) {
  const now = Date.now();

  // Subtract time lag from config
  const timespanParser = timespan({ unit: 'ms' });
  const timeLag = timespanParser.parse(config.get('timeLag'));
  const adjustedNow = now - timeLag;

  // You may need to add a 'status' column to node_plan_job for production use
  // For now, we'll assume jobs are always eligible if their start time is due
  const jobs = await db.all(`
    SELECT node_plan_job.*, node_plan.start_at
    FROM node_plan_job
    JOIN node_plan ON node_plan_job.node_plan_id = node_plan.id
    WHERE node_plan.start_at <= ?
  `, [adjustedNow]);

  for (const job of jobs) {
    // Kick off your task here (e.g., spawn a process, call an API, etc.)
    // For demonstration, just log the job
    console.log(`Kicking off job for node_id=${job.node_id} (plan_id=${job.node_plan_id}) at ${new Date(now).toISOString()}`);
    // Optionally, update job status here
    // await db.run('UPDATE node_plan_job SET status = ? WHERE node_plan_id = ?', ['running', job.node_plan_id]);
  }
}
