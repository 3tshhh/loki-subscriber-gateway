import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Job-arrival subscriber matching moved from an O(1) Redis SUNION to a
 * direct Postgres query (see job-stream.service.ts) — it now runs on the
 * hot path for every incoming job. user_categories' only index is the
 * composite PK (chat_id, category_id), which doesn't help a
 * "WHERE category_id = $1" filter since category_id isn't the leftmost
 * column. user_sources needs no new index — its PK's leftmost column is
 * already chat_id, which is exactly what the EXISTS/NOT EXISTS subqueries
 * filter on.
 */
export class IndexUserCategoriesForMatching1789542923215 implements MigrationInterface {
  name = 'IndexUserCategoriesForMatching1789542923215';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE INDEX "IDX_user_categories_category_id" ON "user_categories" ("category_id")`,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX "IDX_user_categories_category_id"`);
  }
}
