export const recordDeliveredEventBatchSQL = `UPDATE job
  SET event_delivered_batches = event_delivered_batches + 1,
      event_last_error = CASE
        WHEN NOT EXISTS (SELECT 1 FROM event_outbox) THEN NULL
        ELSE event_last_error
      END,
      event_updated_at = ?
  WHERE singleton = 1`;

export const recordReceivedEventSequenceSQL = `UPDATE job
  SET event_sequence = MAX(event_sequence, ?)
  WHERE singleton = 1`;
