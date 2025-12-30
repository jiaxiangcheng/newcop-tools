# Best Seller Badge Assignment Job

自动根据 Airtable 销售数据为顶级产品分配 best seller badge 的任务。

## 功能概述

该脚本实现以下功能：

1. **获取顶级产品**: 从指定的 Airtable view 获取前 N 个产品（默认 50 个）
2. **清除现有 badges**: 移除所有当前标记为 best seller 的产品 badge
3. **分配新 badges**: 为从 Airtable 获取的顶级产品添加 best seller badge
4. **定时执行**: 支持每周日 00:00 自动运行

## 核心特性

- ✅ **GraphQL API**: 使用 Shopify GraphQL API 高效更新产品 metafield
- ✅ **并发处理**: 使用线程池并发更新多个产品（最多 5 个并发）
- ✅ **智能差异检测**: 只更新需要改变的产品
- ✅ **干运行模式**: 支持预览模式，不实际更新数据
- ✅ **全面日志**: 详细的执行日志和错误处理
- ✅ **灵活调度**: 支持手动执行和定时执行（每周日 00:00）

## 配置要求

### 环境变量

在 `.env` 文件中配置以下变量：

```bash
# Shopify 配置
SHOPIFY_ADMIN_TOKEN=your_shopify_admin_token
SHOPIFY_SHOP_DOMAIN=your_shop.myshopify.com

# Airtable 配置
AIRTABLE_TOKEN=your_airtable_token

# Best Seller Badge 配置
BEST_SELLER_AIRTABLE_BASE_ID=appDE0y01TchMqX8N
BEST_SELLER_AIRTABLE_TABLE_ID=tbljkyhWy5D6b65Im
BEST_SELLER_AIRTABLE_VIEW_ID=viwRCdtRuUTkqLOp3
BEST_SELLER_TOP_N=50
BEST_SELLER_DRY_RUN=false
```

### Airtable 要求

- **Base**: 销售数据所在的 Airtable base
- **Table**: 包含产品销售信息的表
- **View**: 按销售额排序的视图（确保第一行是销量最高的产品）
- **必需字段**:
  - `Product Title`: 产品标题
  - `∞ Shopify Id`: 产品在 Shopify 中的 ID

### Shopify 权限

需要以下 Shopify API 权限：

- `read_products`
- `write_products`
- `read_product_metafields`
- `write_product_metafields`

## 使用方法

### 通过主菜单运行

```bash
python main.py
# 选择选项 7: 🏅 Best Seller Badge
```

### 直接运行

```bash
# 手动运行一次
python scripts/job_assign_best_seller_badge/main.py --mode manual

# 定时模式（每周日 00:00 运行）
python scripts/job_assign_best_seller_badge/main.py --mode scheduled

# 干运行（预览更改）
python scripts/job_assign_best_seller_badge/main.py --dry-run
```

## 工作流程

1. **获取数据**
   - 从 Airtable view 获取前 50 个产品
   - 提取有效的 Shopify 产品 ID

2. **检查现状**
   - 使用 GraphQL 查询所有产品的 `custom.best_seller` metafield
   - 识别哪些产品当前有 badge

3. **计算差异**
   - 确定需要添加 badge 的产品
   - 确定需要移除 badge 的产品

4. **执行更新**
   - 先移除所有不应该有 badge 的产品的 badge
   - 然后为应该有 badge 的产品添加 badge
   - 使用并发处理提高效率

5. **报告结果**
   - 记录成功/失败的更新数量
   - 输出详细的执行统计

## 输出示例

```
============================================================
🏅 Starting Best Seller Badge Sync
============================================================
Timestamp: 2025-11-12 12:16:56
Mode: ✅ LIVE
Top N products: 50
Airtable Base: appDE0y01TchMqX8N
Airtable View: viwRCdtRuUTkqLOp3
============================================================
📥 Fetching top products from Airtable...
📦 Fetched 50 products from Airtable
✅ Found 50 products with valid Shopify IDs
🔍 Checking current best seller badge status...
📊 Found 25 products with best_seller badge
🔄 Updates needed:
  ➕ Add badge: 30 products
  ➖ Remove badge: 5 products
🚀 Executing badge updates...
➖ Removing badges from 5 products...
  Progress: 5/5 processed
➕ Adding badges to 30 products...
  Progress: 10/30 processed
  Progress: 20/30 processed
  Progress: 30/30 processed
✅ Updates completed: 35 successful, 0 failed
============================================================
📊 Best Seller Badge Sync Summary
============================================================
Mode: ✅ LIVE
Execution time: 15.32s

📥 Airtable products fetched: 50
  ✅ Valid Shopify IDs: 50
  ❌ Invalid Shopify IDs: 0

🔄 Badge updates:
  ➕ Badges to add: 30
  ➖ Badges to remove: 5

📊 Update results:
  ✅ Successful: 35
  ❌ Failed: 0
============================================================
✅ Best Seller Badge Sync completed successfully!
============================================================
```

## 定时执行

脚本支持以下定时模式：

- **每周日 00:00**: 自动运行 badge 更新

### 设置 Cron 任务

要在服务器上设置定时任务，可以使用 cron：

```bash
# 编辑 crontab
crontab -e

# 添加以下行（每周日 00:00 运行）
0 0 * * 0 cd /path/to/newcop-backend-jobs && source venv/bin/activate && python scripts/job_assign_best_seller_badge/main.py --mode manual >> logs/best_seller_badge_cron.log 2>&1
```

或者使用脚本的 scheduled 模式（作为长期运行的进程）：

```bash
# 在后台运行 scheduled 模式
nohup python scripts/job_assign_best_seller_badge/main.py --mode scheduled > logs/best_seller_badge_scheduled.log 2>&1 &
```

## 故障排查

### 常见问题

1. **产品没有 Shopify ID**
   - 检查 Airtable 中的 `∞ Shopify Id` 字段是否正确填写
   - 脚本会跳过没有有效 ID 的产品并记录警告

2. **GraphQL 错误**
   - 检查 Shopify API token 是否有足够的权限
   - 确保 metafield 定义存在（namespace: custom, key: best_seller, type: boolean）

3. **速率限制**
   - 脚本内置了重试逻辑和延迟
   - 如果遇到速率限制，脚本会自动重试

4. **Airtable view 顺序错误**
   - 确保 view 按正确的销售字段排序
   - 第一条记录应该是销量最高的产品

## 架构说明

### 文件结构

```
scripts/job_assign_best_seller_badge/
├── __init__.py              # 模块初始化
├── main.py                  # 主入口和调度器
├── badge_manager.py         # Badge 管理逻辑
├── models.py                # 数据模型定义
└── README.md               # 本文档
```

### 核心组件

- **BestSellerBadgeOrchestrator**: 主协调器，处理环境验证、客户端初始化和调度
- **BadgeManager**: 核心业务逻辑，处理 badge 同步
- **Models**: Pydantic 数据模型，确保类型安全

### GraphQL 使用

脚本使用 Shopify GraphQL API 进行以下操作：

1. **查询产品 badge 状态**:
   ```graphql
   query {
     products(first: 250) {
       edges {
         node {
           id
           metafield(namespace: "custom", key: "best_seller") {
             value
           }
         }
       }
     }
   }
   ```

2. **更新产品 metafield**:
   ```graphql
   mutation setProductMetafield($metafields: [MetafieldsSetInput!]!) {
     metafieldsSet(metafields: $metafields) {
       metafields {
         id
         namespace
         key
         value
       }
     }
   }
   ```

## 性能指标

- **产品获取**: ~2 秒（50 个产品）
- **状态查询**: ~5 秒（所有产品，分页）
- **更新执行**: ~10-20 秒（50 个产品更新，并发执行）
- **总执行时间**: 通常 15-30 秒

## 扩展性

脚本设计为易于扩展：

- 修改 `BEST_SELLER_TOP_N` 可以改变 top 产品数量
- 可以调整并发数量（`MAX_CONCURRENT_UPDATES`）
- 支持添加其他定时模式（修改 `CronTrigger` 参数）
- 可以扩展为支持不同的 Airtable view 或数据源

## 日志

日志文件位置：`logs/best_seller_badge.log`

日志包含：
- 执行开始/结束时间
- 获取的产品数量
- 更新统计
- 错误和警告信息
- 性能指标
