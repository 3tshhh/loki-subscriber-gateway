import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * /stop now cancels a user's not-yet-delivered notifications (matching the
 * pre-split pipeline's cancel_pending_user_notifications) instead of leaving
 * them pending forever, so this needs a 'cancelled' terminal status.
 */
export class AddCancelledNotificationStatus1789526816336 implements MigrationInterface {
  name = 'AddCancelledNotificationStatus1789526816336';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "notifications" DROP CONSTRAINT "notifications_status_check"`,
    );
    await queryRunner.query(
      `ALTER TABLE "notifications" ADD CONSTRAINT "notifications_status_check" CHECK ("status" IN ('pending', 'sent', 'failed', 'cancelled'))`,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "notifications" DROP CONSTRAINT "notifications_status_check"`,
    );
    await queryRunner.query(
      `ALTER TABLE "notifications" ADD CONSTRAINT "notifications_status_check" CHECK ("status" IN ('pending', 'sent', 'failed'))`,
    );
  }
}
