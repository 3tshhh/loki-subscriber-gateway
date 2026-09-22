import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * `reason` records why a notification ended up `failed`/`cancelled` — one
 * of NotificationReason's values (see notification.entity.ts), enforced
 * with the same CHECK-constraint style as `status`; nullable since
 * `pending`/`sent` rows never have one. `created_at` is backfilled as
 * nullable (no default) on `categories`/`sources`/`user_categories`/
 * `user_sources` since existing rows predate the column and we don't want
 * to fabricate a creation time for them; new rows get it set at insert
 * time via the entity.
 */
export class AddNotificationReasonAndCreatedAtColumns1789700000000 implements MigrationInterface {
  name = 'AddNotificationReasonAndCreatedAtColumns1789700000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      ALTER TABLE "notifications" ADD "reason" TEXT
      CHECK ("reason" IN ('bot_blocked', 'user_unsubscribed', 'delivery_retries_exhausted'))
    `);

    await queryRunner.query(
      `ALTER TABLE "categories" ADD "created_at" TIMESTAMP`,
    );
    await queryRunner.query(`ALTER TABLE "sources" ADD "created_at" TIMESTAMP`);
    await queryRunner.query(
      `ALTER TABLE "user_categories" ADD "created_at" TIMESTAMP`,
    );
    await queryRunner.query(
      `ALTER TABLE "user_sources" ADD "created_at" TIMESTAMP`,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "user_sources" DROP COLUMN "created_at"`,
    );
    await queryRunner.query(
      `ALTER TABLE "user_categories" DROP COLUMN "created_at"`,
    );
    await queryRunner.query(`ALTER TABLE "sources" DROP COLUMN "created_at"`);
    await queryRunner.query(
      `ALTER TABLE "categories" DROP COLUMN "created_at"`,
    );

    await queryRunner.query(`ALTER TABLE "notifications" DROP COLUMN "reason"`);
  }
}
