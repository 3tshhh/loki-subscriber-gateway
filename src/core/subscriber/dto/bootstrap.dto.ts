import { IsBoolean, IsOptional } from 'class-validator';

export class BootstrapDto {
  @IsBoolean()
  @IsOptional()
  activate?: boolean;
}
