import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Switches categories/sources from auto-increment integer PKs to stable
 * string slug ids (e.g. 'backend', 'linkedin') — matching is now done on
 * this id, not the display name, so Python's jobs:notify payload sends the
 * slug directly instead of relying on case-insensitive name matching.
 *
 * Dev-only data existed under the old integer ids, so this truncates
 * categories/sources (and their junction tables) rather than trying to
 * remap old numeric ids to the new slugs — there's nothing meaningful to
 * preserve. Seeds the initial category/source list at the end.
 */
export class CategorySourceSlugIds1789538982035 implements MigrationInterface {
  name = 'CategorySourceSlugIds1789538982035';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "user_categories" DROP CONSTRAINT "user_categories_category_id_fkey"`,
    );
    await queryRunner.query(
      `ALTER TABLE "user_sources" DROP CONSTRAINT "user_sources_source_id_fkey"`,
    );

    await queryRunner.query(
      `TRUNCATE TABLE "user_categories", "user_sources", "categories", "sources" CASCADE`,
    );

    await queryRunner.query(
      `ALTER TABLE "categories" ALTER COLUMN "id" DROP DEFAULT`,
    );
    await queryRunner.query(
      `ALTER TABLE "categories" ALTER COLUMN "id" TYPE TEXT USING "id"::TEXT`,
    );
    await queryRunner.query(`DROP SEQUENCE IF EXISTS "categories_id_seq"`);

    await queryRunner.query(
      `ALTER TABLE "sources" ALTER COLUMN "id" DROP DEFAULT`,
    );
    await queryRunner.query(
      `ALTER TABLE "sources" ALTER COLUMN "id" TYPE TEXT USING "id"::TEXT`,
    );
    await queryRunner.query(`DROP SEQUENCE IF EXISTS "sources_id_seq"`);

    await queryRunner.query(
      `ALTER TABLE "user_categories" ALTER COLUMN "category_id" TYPE TEXT USING "category_id"::TEXT`,
    );
    await queryRunner.query(
      `ALTER TABLE "user_sources" ALTER COLUMN "source_id" TYPE TEXT USING "source_id"::TEXT`,
    );

    await queryRunner.query(
      `ALTER TABLE "user_categories" ADD CONSTRAINT "user_categories_category_id_fkey" FOREIGN KEY ("category_id") REFERENCES "categories"("id") ON DELETE CASCADE`,
    );
    await queryRunner.query(
      `ALTER TABLE "user_sources" ADD CONSTRAINT "user_sources_source_id_fkey" FOREIGN KEY ("source_id") REFERENCES "sources"("id") ON DELETE CASCADE`,
    );

    await queryRunner.query(`
      INSERT INTO "categories" ("id", "name") VALUES
        ('ai_ml', 'AI/ML Data Science'),
        ('backend', 'Backend Development'),
        ('data_analysis', 'Data Analysis'),
        ('frontend', 'Frontend Development'),
        ('full_stack', 'Full Stack Development'),
        ('game_dev', 'Game Development'),
        ('mobile_app', 'Mobile App Development'),
        ('graphic_design', 'Graphic Design')
      ON CONFLICT ("id") DO NOTHING
    `);

    await queryRunner.query(`
      INSERT INTO "sources" ("id", "name") VALUES
        ('kafiil', 'Kafiil'),
        ('freelancer', 'Freelancer'),
        ('mostaql', 'Mostaql'),
        ('nafezly', 'Nafezly'),
        ('linkedin', 'LinkedIn'),
        ('wuzzuf', 'Wuzzuf')
      ON CONFLICT ("id") DO NOTHING
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "user_categories" DROP CONSTRAINT "user_categories_category_id_fkey"`,
    );
    await queryRunner.query(
      `ALTER TABLE "user_sources" DROP CONSTRAINT "user_sources_source_id_fkey"`,
    );

    await queryRunner.query(
      `TRUNCATE TABLE "user_categories", "user_sources", "categories", "sources" CASCADE`,
    );

    await queryRunner.query(`CREATE SEQUENCE "categories_id_seq"`);
    await queryRunner.query(
      `ALTER TABLE "categories" ALTER COLUMN "id" TYPE INT USING nextval('categories_id_seq')`,
    );
    await queryRunner.query(
      `ALTER TABLE "categories" ALTER COLUMN "id" SET DEFAULT nextval('categories_id_seq')`,
    );

    await queryRunner.query(`CREATE SEQUENCE "sources_id_seq"`);
    await queryRunner.query(
      `ALTER TABLE "sources" ALTER COLUMN "id" TYPE INT USING nextval('sources_id_seq')`,
    );
    await queryRunner.query(
      `ALTER TABLE "sources" ALTER COLUMN "id" SET DEFAULT nextval('sources_id_seq')`,
    );

    await queryRunner.query(
      `ALTER TABLE "user_categories" ALTER COLUMN "category_id" TYPE INT USING "category_id"::INT`,
    );
    await queryRunner.query(
      `ALTER TABLE "user_sources" ALTER COLUMN "source_id" TYPE INT USING "source_id"::INT`,
    );

    await queryRunner.query(
      `ALTER TABLE "user_categories" ADD CONSTRAINT "user_categories_category_id_fkey" FOREIGN KEY ("category_id") REFERENCES "categories"("id") ON DELETE CASCADE`,
    );
    await queryRunner.query(
      `ALTER TABLE "user_sources" ADD CONSTRAINT "user_sources_source_id_fkey" FOREIGN KEY ("source_id") REFERENCES "sources"("id") ON DELETE CASCADE`,
    );
  }
}
