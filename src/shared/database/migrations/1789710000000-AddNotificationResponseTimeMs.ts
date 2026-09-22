import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * `response_time_ms` mirrors the end-to-end latency already logged to the
 * terminal per delivery (see logDelivery in telegram-dispatch.service.ts)
 * — nullable since it's only ever set alongside a `sent` row; `pending`/
 * `failed`/`cancelled` rows never get one.
 */
export class AddNotificationResponseTimeMs1789710000000 implements MigrationInterface {
  name = 'AddNotificationResponseTimeMs1789710000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "notifications" ADD "response_time_ms" INTEGER`,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "notifications" DROP COLUMN "response_time_ms"`,
    );
  }
}
