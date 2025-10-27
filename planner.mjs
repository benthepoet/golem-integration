import { createReadStream } from 'fs';
import csv from 'csv-parser';
import { open } from 'sqlite';
import sqlite3 from 'sqlite3';
import config from 'config';

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
        const startAt = row['value.0'];
        const stopAt = row['value.1'];

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
    const insertPlan = await db.prepare('INSERT INTO node_plan (node_id, csv_file, start_at, stop_at, gpu_class_id) VALUES (?, ?, ?, ?, ?)');
    const insertJob = await db.prepare('INSERT INTO node_plan_job (node_plan_id, node_id, order_index, duration) VALUES (?, ?, ?, ?)');
    
    for (const row of rows) {
      // Insert into node_plan and get the primary key (id)
      const result = await insertPlan.run(
        row['key.1'],
        csvFilePath,
        row['value.0'],
        row['value.1'],
        row['value.5']
      );

      // Calculate total duration
      let runningDuration = row['value.1'] - row['value.0'];
      let orderIndex = 0;

      // Insert jobs based on duration constraints
      do {
        // Check if the remaining duration is less than the maximum allowed
        if (runningDuration < maximumDuration ) {
          // Only insert the remainder if it meets the minimum duration
          if (runningDuration >= minimumDuration) {
            await insertJob.run(
              result.lastID, 
              row['key.1'],
              orderIndex++,
              runningDuration
            );
          }

          runningDuration -= runningDuration;
        }
        // Else, insert a job with maximum duration
        else {
          await insertJob.run(
            result.lastID, 
            row['key.1'],
            orderIndex++,
            maximumDuration
          );

          runningDuration -= maximumDuration;
        }
      } while (runningDuration > 0);
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