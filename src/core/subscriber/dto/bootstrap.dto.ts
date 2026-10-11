import { IsBoolean, IsOptional, IsString } from 'class-validator';

export class BootstrapDto {
  @IsBoolean()
  @IsOptional()
  activate?: boolean;

  @IsOptional()
  @IsString()
  username?: string;
}
