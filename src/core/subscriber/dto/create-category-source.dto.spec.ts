import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { CreateCategoryDto } from './create-category.dto';
import { CreateSourceDto } from './create-source.dto';
import { RESERVED_CATEGORY_IDS, RESERVED_SOURCE_IDS } from './reserved-ids';

describe.each([
  ['CreateSourceDto', CreateSourceDto, RESERVED_SOURCE_IDS],
  ['CreateCategoryDto', CreateCategoryDto, RESERVED_CATEGORY_IDS],
] as const)('%s', (_name, Dto, reserved) => {
  const errorsFor = async (id: string) => {
    const errors = await validate(plainToInstance(Dto, { id, name: 'Name' }));
    return errors.filter((e) => e.property === 'id');
  };

  it('reserves the id used by the picker Done button', () => {
    expect(reserved).toContain('done');
  });

  it.each(reserved.map((id) => [id]))(
    'rejects the reserved id %p with a clear message',
    async (id) => {
      const [error] = await errorsFor(id);

      expect(error?.constraints?.isNotIn).toMatch(/reserved/);
      expect(error?.constraints?.isNotIn).toContain(id);
    },
  );

  it.each([['linkedin'], ['done_deals'], ['not_done']])(
    'accepts an ordinary id like %p',
    async (id) => {
      expect(await errorsFor(id)).toEqual([]);
    },
  );
});
