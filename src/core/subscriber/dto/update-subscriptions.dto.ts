import { IsArray, IsString } from 'class-validator';

export class UpdateSubscriptionsDto {
  @IsArray()
  @IsString({ each: true })
  categoryIds: string[];

  @IsArray()
  @IsString({ each: true })
  sourceIds: string[];
}
