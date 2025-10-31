import db from './db.mjs';

/**
 * Execute a plan with an initial job.
 * @param {Object} initialJob - The initial job to execute.
 */
export async function executePlan(initialJob) {
  // TODO: Allocate resources as needed

  // Simulate job execution
  let currentJob = initialJob;

  do {
    // Do the work for the current job
    console.log(`Executing job for node_id=${currentJob.node_id} (plan_id=${currentJob.node_plan_id})`);
    await new Promise(resolve => setTimeout(resolve, currentJob.adjusted_duration)); // Simulate async work
    console.log(`Finished job for node_id=${currentJob.node_id} (plan_id=${currentJob.node_plan_id})`);

    // Grab the next job from the plan, if any
    currentJob = await db.get(`
      SELECT
        np.node_id,
        np.gpu_class_id,
        npj.node_plan_id,
        npj.order_index,
        npj.start_at + npj.duration - $adjustedNow AS adjusted_duration,
        (npj.start_at + npj.duration - $adjustedNow) / CAST(npj.duration AS REAL) * npj.invoice_amount AS adjusted_invoice_amount
      FROM node_plan_job npj
      JOIN node_plan np ON np.id = npj.node_plan_id AND np.status != 'completed'
      WHERE npj.node_plan_id = $nodePlanId
        AND npj.order_index = $nextOrderIndex
    `, {
      $nodePlanId: initialJob.node_plan_id,
      $nextOrderIndex: initialJob.order_index + 1
    });
    // Loop until there are no more jobs in the plan
  } while (currentJob != null);

  // Mark plan as completed in database
  await db.run(`
    UPDATE node_plan
    SET status = 'completed'
    WHERE id = $nodePlanId
  `, {
    $nodePlanId: initialJob.node_plan_id
  });

  console.log(`All jobs for plan_id=${initialJob.node_plan_id} completed.`);

  // TODO: Shutdown resources as needed
}
