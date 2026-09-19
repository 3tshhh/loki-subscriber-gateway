import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { UserEntity } from './entities/user.entity';
import { CategoryEntity } from './entities/category.entity';
import { SourceEntity } from './entities/source.entity';
import { UserCategoryEntity } from './entities/user-category.entity';
import { UserSourceEntity } from './entities/user-source.entity';
import { SubscriberService } from './subscriber.service';
import { SubscriberController } from './subscriber.controller';
import { UserRetentionService } from './user-retention.service';

@Module({
  imports: [
    TypeOrmModule.forFeature([
      UserEntity,
      CategoryEntity,
      SourceEntity,
      UserCategoryEntity,
      UserSourceEntity,
    ]),
  ],
  controllers: [SubscriberController],
  providers: [SubscriberService, UserRetentionService],
  exports: [SubscriberService],
})
export class SubscriberModule {}
