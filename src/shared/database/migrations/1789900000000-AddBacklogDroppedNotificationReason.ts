import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Allows the new 'backlog_dropped' reason: a `cancelled` row whose
 * notify:telegram batch the startup backlog check dropped instead of
 * sending (see NotificationReason.BACKLOG_DROPPED). Replaces the
 * notifications_reason_check constraint created by
 * FillAllNotificationColumns with the widened list.
 */
export class AddBacklogDroppedNotificationReason1789900000000 implements MigrationInterface {
  name = 'AddBacklogDroppedNotificationReason1789900000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "notifications" DROP CONSTRAINT "notifications_reason_check"`,
    );
    await queryRunner.query(`
      ALTER TABLE "notifications" ADD CONSTRAINT "notifications_reason_check"
      CHECK ("reason" IN ('awaiting_delivery', 'delivered', 'bot_blocked', 'user_unsubscribed', 'recipient_inactive', 'delivery_retries_exhausted', 'delivery_status_unknown', 'backlog_dropped', 'unknown_legacy'))
    `);
  }

  /** Rows already cancelled as backlog_dropped have no older equivalent;
   * they're relabelled unknown_legacy (the "real cause not representable"
   * reason) so the narrower CHECK can be restored. */
  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "notifications" DROP CONSTRAINT "notifications_reason_check"`,
    );
    await queryRunner.query(
      `UPDATE "notifications" SET "reason" = 'unknown_legacy' WHERE "reason" = 'backlog_dropped'`,
    );
    await queryRunner.query(`
      ALTER TABLE "notifications" ADD CONSTRAINT "notifications_reason_check"
      CHECK ("reason" IN ('awaiting_delivery', 'delivered', 'bot_blocked', 'user_unsubscribed', 'recipient_inactive', 'delivery_retries_exhausted', 'delivery_status_unknown', 'unknown_legacy'))
    `);
  }
}
