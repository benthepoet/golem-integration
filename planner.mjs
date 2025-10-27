import { createReadStream } from 'fs';
import csv from 'csv-parser';
import { open } from 'sqlite';
import sqlite3 from 'sqlite3';
import config from 'config';

const CSV_KEYS = {
  NODE_ID: 'key.1',
  START_AT: 'value.0',
  STOP_AT: 'value.1',
  INVOICE_AMOUNT: 'value.2',
  GPU_CLASS_ID: 'value.5'
}

async function main() {
  // Open the database
  const db = await open({
    filename: 'data/planner.db',
    driver: sqlite3.Database
  });

  // Create tables
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

  // Read CSV and collect rows
  const csvFilePath = 'data/20251025-20251026.csv';
  const rows = [];
  await new Promise((resolve, reject) => {
    createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', (row) => {
        const startAt = row[CSV_KEYS.START_AT];
        const stopAt = row[CSV_KEYS.STOP_AT];

        // Filter rows based on minimumDuration
        if ((stopAt - startAt) >= minimumDuration) {
          rows.push(row);
        }
      })
      .on('end', resolve)
      .on('error', reject);
  });

  // Bulk insert using a transaction
  await db.run('BEGIN TRANSACTION');
  try {
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
    
    for (const row of rows) {
      // Insert into node_plan and get the primary key (id)
      const result = await insertPlan.run(
        row[CSV_KEYS.NODE_ID],
        csvFilePath,
        row[CSV_KEYS.START_AT],
        row[CSV_KEYS.STOP_AT],
        row[CSV_KEYS.GPU_CLASS_ID]
      );

      // Calculate total duration
      const totalInvoiceAmount = row[CSV_KEYS.INVOICE_AMOUNT];
      let totalDuration = row[CSV_KEYS.STOP_AT] - row[CSV_KEYS.START_AT];
      let remainingDuration = totalDuration;
      let orderIndex = 0;
      
      // Insert jobs based on duration constraints
      do {
        // Check if the remaining duration is less than the maximum allowed
        if (remainingDuration < maximumDuration ) {
          // Only insert the remainder if it meets the minimum duration
          if (remainingDuration >= minimumDuration) {
            await insertJob.run(
              result.lastID, 
              row[CSV_KEYS.NODE_ID],
              orderIndex++,
              remainingDuration,
              (remainingDuration / totalDuration) * totalInvoiceAmount
            );
          }

          remainingDuration -= remainingDuration;
        }
        // Else, insert a job with maximum duration
        else {
          await insertJob.run(
            result.lastID, 
            row[CSV_KEYS.NODE_ID],
            orderIndex++,
            maximumDuration,
            (maximumDuration / totalDuration) * totalInvoiceAmount
          );

          remainingDuration -= maximumDuration;
        }
      } while (remainingDuration > 0);
    }
    await insertPlan.finalize();
    await insertJob.finalize();
    await db.run('COMMIT');
    console.log('CSV file successfully processed and rows inserted efficiently');
  } catch (err) {
    await db.run('ROLLBACK');
    console.error('Error inserting rows:', err);
  } finally {
    await db.close();
  }
}

main();