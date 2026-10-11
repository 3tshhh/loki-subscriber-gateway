import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Every `notifications` row now has every column filled — no NULLs — under
 * a "status-change" model:
 *
 * - `sent_at` becomes `status_changed_at`: when the row last entered its
 *   current status (insert time while `pending`, then the moment it became
 *   `sent`/`failed`/`cancelled`), instead of only ever being set for `sent`.
 * - `reason` is set for every status: `pending` rows get
 *   'awaiting_delivery' and `sent` rows 'delivered', alongside the existing
 *   outcome reasons. `failed`/`cancelled` rows from before the `reason`
 *   column existed (AddNotificationReasonAndCreatedAtColumns) have no
 *   recorded cause, so they're backfilled with 'unknown_legacy' rather than
 *   a guessed one.
 * - `response_time_ms` is arrival in jobs:notify → the row's latest status
 *   change, for every status rather than only `sent`.
 *
 * Backfill for existing rows (new rows are written fully by the app):
 * status_changed_at falls back to created_at (now() if that were ever
 * missing — it's NOT NULL since InitSchema, so this is belt-and-braces);
 * response_time_ms, where NULL, becomes status_changed_at − created_at
 * clamped to [0, INT max] — the job's true arrival time was never stored,
 * so insert→status-change is the closest knowable lower bound (and 0 for
 * pending/legacy rows, whose status_changed_at was just set to created_at).
 * `sent` rows that already had a response_time_ms keep it.
 *
 * The reason CHECK was added inline by ADD COLUMN, so its name was chosen
 * by Postgres; it's looked up from pg_constraint rather than assumed. The
 * replacement is created under an explicit name so down() can drop it
 * directly.
 */
export class FillAllNotificationColumns1789800000000 implements MigrationInterface {
  name = 'FillAllNotificationColumns1789800000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    // status_changed_at
    await queryRunner.query(
      `ALTER TABLE "notifications" RENAME COLUMN "sent_at" TO "status_changed_at"`,
    );
    await queryRunner.query(`
      UPDATE "notifications"
      SET "status_changed_at" = COALESCE("created_at", LOCALTIMESTAMP)
      WHERE "status_changed_at" IS NULL
    `);

    // reason: widen the CHECK, then backfill
    await queryRunner.query(`
      DO $$
      DECLARE c record;
      BEGIN
        FOR c IN
          SELECT conname FROM pg_constraint
          WHERE conrelid = '"notifications"'::regclass
            AND contype = 'c'
            AND pg_get_constraintdef(oid) LIKE '%reason%'
        LOOP
          EXECUTE format('ALTER TABLE "notifications" DROP CONSTRAINT %I', c.conname);
        END LOOP;
      END $$
    `);
    await queryRunner.query(`
      ALTER TABLE "notifications" ADD CONSTRAINT "notifications_reason_check"
      CHECK ("reason" IN ('awaiting_delivery', 'delivered', 'bot_blocked', 'user_unsubscribed', 'delivery_retries_exhausted', 'unknown_legacy'))
    `);
    await queryRunner.query(`
      UPDATE "notifications"
      SET "reason" = CASE "status"
        WHEN 'pending' THEN 'awaiting_delivery'
        WHEN 'sent' THEN 'delivered'
        ELSE 'unknown_legacy'
      END
      WHERE "reason" IS NULL
    `);

    // response_time_ms
    await queryRunner.query(`
      UPDATE "notifications"
      SET "response_time_ms" = LEAST(2147483647, GREATEST(0, ROUND(EXTRACT(EPOCH FROM ("status_changed_at" - "created_at")) * 1000)))::int
      WHERE "response_time_ms" IS NULL
    `);

    // Enforce. status and created_at have been NOT NULL since InitSchema.
    await queryRunner.query(`
      ALTER TABLE "notifications"
        ALTER COLUMN "status_changed_at" SET NOT NULL,
        ALTER COLUMN "reason" SET NOT NULL,
        ALTER COLUMN "response_time_ms" SET NOT NULL
    `);
  }

  /**
   * Restores the pre-migration shape and semantics: the synthetic reasons
   * go back to NULL, response_time_ms/sent_at go back to NULL for anything
   * not `sent`. Backfilled response_time_ms values on legacy `sent` rows
   * can't be told apart from real ones, so those are kept.
   */
  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      ALTER TABLE "notifications"
        ALTER COLUMN "status_changed_at" DROP NOT NULL,
        ALTER COLUMN "reason" DROP NOT NULL,
        ALTER COLUMN "response_time_ms" DROP NOT NULL
    `);

    await queryRunner.query(`
      UPDATE "notifications" SET "reason" = NULL
      WHERE "reason" IN ('awaiting_delivery', 'delivered', 'unknown_legacy')
    `);
    await queryRunner.query(
      `ALTER TABLE "notifications" DROP CONSTRAINT "notifications_reason_check"`,
    );
    await queryRunner.query(`
      ALTER TABLE "notifications" ADD CONSTRAINT "notifications_reason_check"
      CHECK ("reason" IN ('bot_blocked', 'user_unsubscribed', 'delivery_retries_exhausted'))
    `);

    await queryRunner.query(`
      UPDATE "notifications"
      SET "response_time_ms" = NULL, "status_changed_at" = NULL
      WHERE "status" <> 'sent'
    `);
    await queryRunner.query(
      `ALTER TABLE "notifications" RENAME COLUMN "status_changed_at" TO "sent_at"`,
    );
  }
}
