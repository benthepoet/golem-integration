import { createReadStream, readdirSync, renameSync, existsSync, mkdirSync } from 'fs';
import csv from 'csv-parser';
import config from 'config';

// CSV column keys
const CSV_KEYS = {
  NODE_ID: 'key.1',
  START_AT: 'value.0',
  STOP_AT: 'value.1',
  INVOICE_AMOUNT: 'value.2',
  GPU_CLASS_ID: 'value.5'
}

/**
 * Import plans into the database.
 * @param {import('sqlite').Database} db - The opened sqlite database instance.
 */
async function importPlans(db) {

  // Ensure tables exist
  await db.exec(`
    CREATE TABLE IF NOT EXISTS node_plan (
      id INTEGER PRIMARY KEY,
      node_id TEXT,
      csv_file TEXT,
      start_at INTEGER,
      stop_at INTEGER,
      gpu_class_id TEXT
    )
  `);

  await db.exec(`
    CREATE TABLE IF NOT EXISTS node_plan_job (
      node_plan_id INTEGER,
      node_id TEXT,
      order_index INTEGER,
      duration INTEGER,
      invoice_amount REAL,
      FOREIGN KEY (node_plan_id) REFERENCES node_plan(id)
    )
  `);

  // Get minimum and maximum duration from config
  const minimumDuration = config.get('minimumDuration') * 60 * 1000; // convert minutes to milliseconds
  const maximumDuration = config.get('maximumDuration') * 60 * 1000; // convert minutes to milliseconds

  // Process all CSV files in the pending directory
  const pendingDir = 'data/pending';
  const importedDir = 'data/imported';
  const failedDir = 'data/failed';

  // Ensure pending directory exists
  if (!existsSync(pendingDir)) {
    mkdirSync(pendingDir, { recursive: true });
  }

  // Read CSV files
  const files = readdirSync(pendingDir).filter(f => f.endsWith('.csv'));
  console.log(`Found ${files.length} CSV files to process.`);
  
  // Process each CSV file
  for (const csvFile of files) {
    console.log(`Processing file: ${csvFile}`);

    const csvFilePath = `${pendingDir}/${csvFile}`;
    const rows = [];
    let importSuccess = true;

    // Read CSV and collect rows
    await new Promise((resolve, reject) => {
      createReadStream(csvFilePath)
        .pipe(csv())
        .on('data', (row) => {
          const totalDuration = row[CSV_KEYS.STOP_AT] - row[CSV_KEYS.START_AT];
          if (totalDuration >= minimumDuration) {
            rows.push(row);
          }
        })
        .on('end', resolve)
        .on('error', (err) => {
          importSuccess = false;
          reject(err);
        });
    }).catch((err) => {
      console.error(`Failed to read ${csvFile}:`, err);
      importSuccess = false;
    });

    if (importSuccess) {
      await db.run('BEGIN TRANSACTION');

      try {
        // Prepare plan insert statement
        const insertPlan = await db.prepare(`
          INSERT INTO node_plan (
            node_id,
            csv_file,
            start_at,
            stop_at,
            gpu_class_id
          ) VALUES (
            ?,
            ?,
            ?,
            ?,
            ?
          )
        `);

        // Prepare job insert statement
        const insertJob = await db.prepare(`
          INSERT INTO node_plan_job (
            node_plan_id,
            node_id,
            order_index,
            duration,
            invoice_amount
          ) VALUES (
            ?,
            ?,
            ?,
            ?,
            ?
          )
        `);

        // Insert plans and jobs
        for (const row of rows) {
          // Insert plan
          const result = await insertPlan.run(
            row[CSV_KEYS.NODE_ID],
            csvFilePath,
            row[CSV_KEYS.START_AT],
            row[CSV_KEYS.STOP_AT],
            row[CSV_KEYS.GPU_CLASS_ID]
          );

          // Calculate job parameters
          const totalInvoiceAmount = row[CSV_KEYS.INVOICE_AMOUNT];
          let totalDuration = row[CSV_KEYS.STOP_AT] - row[CSV_KEYS.START_AT];
          let remainingDuration = totalDuration;
          let orderIndex = 0;

          // Split into jobs based on maximumDuration
          do {
            // Calculate job duration
            const jobDuration = Math.min(remainingDuration, maximumDuration);
            
            // Insert job if it meets minimum duration
            if (jobDuration >= minimumDuration) {
              await insertJob.run(
                result.lastID,
                row[CSV_KEYS.NODE_ID],
                orderIndex++,
                jobDuration,
                (jobDuration / totalDuration) * totalInvoiceAmount
              );
            }

            // Decrease remaining duration
            remainingDuration -= jobDuration;
          } while (remainingDuration > 0);
        }

        // Finalize statements and commit transaction
        await insertPlan.finalize();
        await insertJob.finalize();
        await db.run('COMMIT');
        console.log(`${csvFile} successfully processed and rows inserted efficiently`);
        
        // Move file to imported/
        renameSync(csvFilePath, `${importedDir}/${csvFile}`);
      } catch (err) {
        await db.run('ROLLBACK');
        console.error(`Error importing ${csvFile}:`, err);

        // Move file to failed/
        renameSync(csvFilePath, `${failedDir}/${csvFile}`);
      }
    } else {
      // Move file to failed/
      renameSync(csvFilePath, `${failedDir}/${csvFile}`);
    }
  }
}

export { importPlans };