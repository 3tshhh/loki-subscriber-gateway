import { MigrationInterface, QueryRunner } from 'typeorm';

export class InitSchema1757900000000 implements MigrationInterface {
  name = 'InitSchema1757900000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      CREATE TABLE "users" (
        "chat_id" BIGINT PRIMARY KEY,
        "active" BOOLEAN NOT NULL DEFAULT true,
        "deactivated_at" TIMESTAMP,
        "created_at" TIMESTAMP NOT NULL DEFAULT now()
      )
    `);

    await queryRunner.query(`
      CREATE TABLE "categories" (
        "id" SERIAL PRIMARY KEY,
        "name" TEXT UNIQUE NOT NULL,
        "enabled" BOOLEAN NOT NULL DEFAULT true
      )
    `);

    await queryRunner.query(`
      CREATE TABLE "sources" (
        "id" SERIAL PRIMARY KEY,
        "name" TEXT UNIQUE NOT NULL,
        "enabled" BOOLEAN NOT NULL DEFAULT true
      )
    `);

    await queryRunner.query(`
      CREATE TABLE "user_categories" (
        "chat_id" BIGINT NOT NULL REFERENCES "users"("chat_id") ON DELETE CASCADE,
        "category_id" INT NOT NULL REFERENCES "categories"("id") ON DELETE CASCADE,
        PRIMARY KEY ("chat_id", "category_id")
      )
    `);

    await queryRunner.query(`
      CREATE TABLE "user_sources" (
        "chat_id" BIGINT NOT NULL REFERENCES "users"("chat_id") ON DELETE CASCADE,
        "source_id" INT NOT NULL REFERENCES "sources"("id") ON DELETE CASCADE,
        PRIMARY KEY ("chat_id", "source_id")
      )
    `);

    await queryRunner.query(`
      CREATE TABLE "notifications" (
        "job_id" TEXT NOT NULL,
        "chat_id" BIGINT NOT NULL REFERENCES "users"("chat_id") ON DELETE CASCADE,
        "status" TEXT NOT NULL DEFAULT 'pending' CHECK ("status" IN ('pending', 'sent', 'failed')),
        "created_at" TIMESTAMP NOT NULL DEFAULT now(),
        "sent_at" TIMESTAMP,
        PRIMARY KEY ("job_id", "chat_id")
      )
    `);

    await queryRunner.query(
      `CREATE INDEX "IDX_notifications_job_id" ON "notifications" ("job_id")`,
    );

    await queryRunner.query(
      `CREATE INDEX "IDX_users_active_deactivated_at" ON "users" ("active", "deactivated_at")`,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP TABLE "notifications"`);
    await queryRunner.query(`DROP TABLE "user_sources"`);
    await queryRunner.query(`DROP TABLE "user_categories"`);
    await queryRunner.query(`DROP TABLE "sources"`);
    await queryRunner.query(`DROP TABLE "categories"`);
    await queryRunner.query(`DROP TABLE "users"`);
  }
}
