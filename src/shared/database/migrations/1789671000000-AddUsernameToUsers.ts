import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Telegram @username, captured alongside chat_id on every /start or resume
 * — chat_id already doubles as the user's id (see UserEntity's own comment
 * on why there's no separate user_id column), so this is the only new
 * column needed to also have the username on hand.
 */
export class AddUsernameToUsers1789671000000 implements MigrationInterface {
  name = 'AddUsernameToUsers1789671000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "users" ADD "username" TEXT`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "users" DROP COLUMN "username"`);
  }
}
