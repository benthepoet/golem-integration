import { createReadStream } from 'fs';
import { promises as fsp } from 'fs';
import csv from 'csv-parser';
import config from 'config';
import timespan from 'timespan-parser';

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
    CREATE TABLE IF NOT EXISTS csv_import_file (
      id INTEGER PRIMARY KEY,
      file_name TEXT UNIQUE
    )
  `);

  await db.exec(`
    CREATE TABLE IF NOT EXISTS node_plan (
      id INTEGER PRIMARY KEY,
      node_id TEXT,
      csv_import_file_id INTEGER,
      start_at INTEGER,
      stop_at INTEGER,
      gpu_class_id TEXT,
      FOREIGN KEY (csv_import_file_id) REFERENCES csv_import_file(id)
    )
  `);

  await db.exec(`
    CREATE TABLE IF NOT EXISTS node_plan_job (
      node_plan_id INTEGER,
      order_index INTEGER,
      start_at INTEGER,
      duration INTEGER,
      invoice_amount REAL,
      FOREIGN KEY (node_plan_id) REFERENCES node_plan(id)
    )
  `);

  // Get minimum and maximum duration from config
  const timespanParser = timespan({ unit: 'ms' });
  const minimumDuration = timespanParser.parse(config.get('minimumDuration'));
  const maximumDuration = timespanParser.parse(config.get('maximumDuration'));

  // Process all CSV files in the pending directory
  const pendingDir = 'data/pending';
  const importedDir = 'data/imported';
  const failedDir = 'data/failed';

  // Ensure pending directory exists
  await fsp.mkdir(pendingDir, { recursive: true });

  // Ensure imported directory exists
  await fsp.mkdir(importedDir, { recursive: true });

  // Ensure failed directory exists
  await fsp.mkdir(failedDir, { recursive: true });

  // Read CSV files
  const files = (await fsp.readdir(pendingDir)).filter(f => f.endsWith('.csv'));
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
        // Insert CSV file record
        const insertCsvFile = await db.prepare(`
          INSERT INTO csv_import_file (file_name) VALUES (?)
        `);
        const csvFileResult = await insertCsvFile.run(csvFile);
        const csvFileId = csvFileResult.lastID;

        // Prepare plan insert statement
        const insertPlan = await db.prepare(`
          INSERT INTO node_plan (
            node_id,
            csv_import_file_id,
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
            order_index,
            start_at,
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
            csvFileId,
            row[CSV_KEYS.START_AT],
            row[CSV_KEYS.STOP_AT],
            row[CSV_KEYS.GPU_CLASS_ID]
          );

          // Calculate job parameters
          const totalInvoiceAmount = row[CSV_KEYS.INVOICE_AMOUNT];
          let totalDuration = row[CSV_KEYS.STOP_AT] - row[CSV_KEYS.START_AT];
          let remainingDuration = totalDuration;
          let orderIndex = 0;
          let jobStartAt = parseInt(row[CSV_KEYS.START_AT]);

          // Split into jobs based on maximumDuration
          do {
            // Calculate job duration
            const jobDuration = Math.min(remainingDuration, maximumDuration);

            // Insert job if it meets minimum duration
            if (jobDuration >= minimumDuration) {
              await insertJob.run(
                result.lastID,
                orderIndex++,
                jobStartAt,
                jobDuration,
                (jobDuration / totalDuration) * totalInvoiceAmount
              );
            }

            // Decrease remaining duration
            remainingDuration -= jobDuration;

            // Update job start time
            jobStartAt += jobDuration;
          } while (remainingDuration > 0);
        }

        // Finalize statements and commit transaction
        await insertCsvFile.finalize();
        await insertPlan.finalize();
        await insertJob.finalize();
        await db.run('COMMIT');
        console.log(`${csvFile} successfully processed and rows inserted efficiently`);

        // Move file to imported/
        await fsp.rename(csvFilePath, `${importedDir}/${csvFile}`);
      } catch (err) {
        await db.run('ROLLBACK');
        console.error(`Error importing ${csvFile}:`, err);

        // Move file to failed/
        await fsp.rename(csvFilePath, `${failedDir}/${csvFile}`);
      }
    } else {
      // Move file to failed/
      await fsp.rename(csvFilePath, `${failedDir}/${csvFile}`);
    }
  }
}

export { importPlans };
