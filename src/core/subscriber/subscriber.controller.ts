import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { SubscriberService } from './subscriber.service';
import { UpdateSubscriptionsDto } from './dto/update-subscriptions.dto';
import { BootstrapDto } from './dto/bootstrap.dto';
import { CreateCategoryDto } from './dto/create-category.dto';
import { CreateSourceDto } from './dto/create-source.dto';

/**
 * Synchronous request/response surface for the telegram-bot module. Low
 * volume, user-is-waiting-for-a-reply paths only — bulk fan-out happens over
 * the notify:telegram Redis stream instead. chat_id doubles as the user's
 * identity throughout, since this bot only ever does 1:1 DMs.
 */
@Controller()
export class SubscriberController {
  constructor(private readonly subscriber: SubscriberService) {}

  /** Idempotent row creation only — never reactivates. Call at the start of
   * every bot command, matching ensure_user in the pre-split bot. */
  @Post('users/:chatId')
  async ensureUser(@Param('chatId') chatId: string) {
    const user = await this.subscriber.ensureUser(chatId);
    return { chatId: user.chatId, active: user.active };
  }

  /** Explicit activation — /start (always) and the "Resume" picker button. */
  @Post('users/:chatId/activate')
  async activate(@Param('chatId') chatId: string) {
    await this.subscriber.activateUser(chatId);
    return { chatId, active: true };
  }

  /**
   * Everything /start, /categories, /sources need to render their first
   * screen, in one call — ensures the row exists, optionally activates
   * (/start passes activate:true; /categories, /sources pass false), and
   * returns current selection + both enabled lists together.
   */
  @Post('users/:chatId/bootstrap')
  async bootstrap(@Param('chatId') chatId: string, @Body() dto: BootstrapDto) {
    return this.subscriber.bootstrap(chatId, dto.activate ?? false);
  }

  @Get('categories')
  async listCategories() {
    return this.subscriber.getEnabledCategories();
  }

  @Post('categories')
  async createCategory(@Body() dto: CreateCategoryDto) {
    return this.subscriber.createCategory(dto.id, dto.name);
  }

  @Get('sources')
  async listSources() {
    return this.subscriber.getEnabledSources();
  }

  @Post('sources')
  async createSource(@Body() dto: CreateSourceDto) {
    return this.subscriber.createSource(dto.id, dto.name);
  }

  @Get('users/:chatId/subscriptions')
  async getSubscriptions(@Param('chatId') chatId: string) {
    return this.subscriber.getUserSubscriptions(chatId);
  }

  @Post('users/:chatId/subscriptions')
  async updateSubscriptions(
    @Param('chatId') chatId: string,
    @Body() dto: UpdateSubscriptionsDto,
  ) {
    await this.subscriber.updateSubscriptions(
      chatId,
      dto.categoryIds,
      dto.sourceIds,
    );
    return this.subscriber.getUserSubscriptions(chatId);
  }

  /** /stop — also cancels this user's still-pending notifications. */
  @Post('users/:chatId/deactivate')
  async deactivate(@Param('chatId') chatId: string) {
    await this.subscriber.deactivateUser(chatId);
    return { chatId, active: false };
  }
}
