# Product Discount Calculator Job

自动计算所有产品 variant 的折扣百分比，并更新产品的 `custom.discounts` metafield。

## 功能概述

该脚本会：

1. **获取所有产品及其 variants**：使用 GraphQL 从 Shopify 获取所有产品
2. **计算折扣百分比**：对于每个有 `compare_at_price` 的 variant，计算折扣
3. **四舍五入到 5 的倍数**：将折扣百分比四舍五入到最近的 5% (5%, 10%, 15%, 20%, 25%, ...)
4. **去重并存储**：将每个产品的唯一折扣百分比存储到 metafield

## 折扣计算逻辑

### 计算公式

```
discount_percentage = ((compare_at_price - price) / compare_at_price) × 100
rounded_discount = round(discount_percentage / 5) × 5
```

### 示例

假设一个产品有 10 个 variants：

| Variant | Price | Compare At Price | 原始折扣 | 四舍五入 |
|---------|-------|------------------|---------|---------|
| 1 | €80 | €100 | 20% | 20% |
| 2 | €85 | €100 | 15% | 15% |
| 3 | €88 | €100 | 12% | 10% |
| 4 | €75 | €100 | 25% | 25% |
| 5 | €96 | €100 | 4% | 5% |
| 6 | €50 | - | - | (无折扣) |
| 7 | €82 | €100 | 18% | 20% |
| 8 | €77 | €100 | 23% | 25% |
| 9 | €90 | - | - | (无折扣) |
| 10 | €83 | €100 | 17% | 15% |

**结果**：
- 有折扣的 variants: 8 个
- 原始折扣: 20%, 15%, 12%, 25%, 4%, 18%, 23%, 17%
- 四舍五入: 20%, 15%, 10%, 25%, 5%, 20%, 25%, 15%
- 唯一值（去重）: **5%, 10%, 15%, 20%, 25%**
- Metafield 值: `["5%", "10%", "15%", "20%", "25%"]`

## 核心特性

- ✅ **GraphQL API**: 高效获取所有产品和 variants
- ✅ **智能计算**: 自动计算折扣并四舍五入到 5 的倍数
- ✅ **去重处理**: 每个产品只存储唯一的折扣百分比
- ✅ **自动转换**: 自动将旧格式（无 %）转换为新格式（带 %）
- ✅ **并发更新**: 使用线程池并发更新（最多 5 个并发）
- ✅ **干运行模式**: 支持预览模式
- ✅ **定时执行**: 每天 00:00 自动运行
- ✅ **全面日志**: 详细的执行统计

## 配置要求

### 环境变量

只需要基本的 Shopify 配置（无需额外配置）：

```bash
# Shopify 配置
SHOPIFY_ADMIN_TOKEN=your_shopify_admin_token
SHOPIFY_SHOP_DOMAIN=your_shop.myshopify.com
```

### Shopify Metafield 定义

需要在 Shopify Admin 中定义以下 metafield：

- **Owner Type**: Product
- **Namespace**: `custom`
- **Key**: `discounts`
- **Type**: `list.single_line_text_field`
- **Description**: Product variant discount percentages

### Shopify 权限

需要以下 Shopify API 权限：

- `read_products`
- `read_product_metafields`
- `write_product_metafields`

## 使用方法

### 通过主菜单运行

```bash
python main.py
# 选择选项 8: 💰 Product Discounts
```

### 直接运行

```bash
# 手动运行一次
python scripts/job_set_discounts_to_products/main.py --mode manual

# 定时模式（每天 00:00 运行）
python scripts/job_set_discounts_to_products/main.py --mode scheduled

# 干运行（预览更改）
python scripts/job_set_discounts_to_products/main.py --dry-run
```

## 工作流程

1. **获取数据**
   - 使用 GraphQL 分页获取所有产品及其 variants
   - 同时获取当前 `custom.discounts` metafield 的值

2. **分析折扣**
   - 遍历每个产品的所有 variants
   - 检查 `compare_at_price` 是否存在且大于 `price`
   - 计算折扣百分比并四舍五入到 5 的倍数

3. **计算唯一值**
   - 收集每个产品所有 variants 的折扣百分比
   - 去重得到唯一的折扣值
   - 排序后转换为字符串列表

4. **检测变化**
   - 比较计算的折扣值与 metafield 中的当前值
   - 只更新有变化的产品

5. **执行更新**
   - 使用 GraphQL mutation 并发更新 metafield
   - 记录成功和失败的更新

6. **报告结果**
   - 输出详细的执行统计
   - 记录所有发现的唯一折扣百分比

## 输出示例

```
============================================================
💰 Starting Product Discount Sync
============================================================
Timestamp: 2025-11-14 15:01:49
Mode: ✅ LIVE
============================================================
💰 Starting Product Discount Sync (dry_run=False)
📥 Fetching all products with variants from Shopify...
  Progress: Fetched 1250 products...
✅ Fetched 1712 products in 7 pages
📦 Fetched 1712 products
🔍 Analyzing discounts for all products...
📊 Found 167 products with discounts
📊 Total unique discount percentages: [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65]
🔄 167 products need metafield updates
🚀 Executing metafield updates...
  Progress: 10/167 processed
  Progress: 20/167 processed
  ...
  Progress: 160/167 processed
✅ Updates completed: 167 successful, 0 failed
============================================================
💰 Product Discount Sync Summary
============================================================
Mode: ✅ LIVE
Execution time: 45.32s

📦 Products processed: 1712
💰 Products with discounts: 167
📊 Unique discount percentages found: [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65]

🔄 Products needing update: 167

📊 Update results:
  ✅ Successful: 167
  ❌ Failed: 0
============================================================
✅ Product Discount Sync completed successfully!
============================================================
```

## 定时执行

脚本支持每天 00:00 自动运行。

### 设置 Cron 任务

在服务器上设置定时任务：

```bash
# 编辑 crontab
crontab -e

# 添加以下行（每天 00:00 运行）
0 0 * * * cd /path/to/newcop-backend-jobs && source venv/bin/activate && python scripts/job_set_discounts_to_products/main.py --mode manual >> logs/product_discounts_cron.log 2>&1
```

或者使用脚本的 scheduled 模式（作为长期运行的进程）：

```bash
# 在后台运行 scheduled 模式
nohup python scripts/job_set_discounts_to_products/main.py --mode scheduled > logs/product_discounts_scheduled.log 2>&1 &
```

## 自动格式转换

### 向后兼容性

脚本会自动处理旧格式的数据（不带 `%` 符号）并转换为新格式：

| 场景 | 旧 Metafield 值 | 新 Metafield 值 | 是否更新 |
|------|----------------|----------------|---------|
| 旧格式，值相同 | `["15", "20"]` | `["15%", "20%"]` | ✅ 是（格式转换） |
| 新格式，值相同 | `["15%", "20%"]` | `["15%", "20%"]` | ❌ 否 |
| 旧格式，值不同 | `["10", "25"]` | `["15%", "20%"]` | ✅ 是（格式+值） |
| 新格式，值不同 | `["10%", "25%"]` | `["15%", "20%"]` | ✅ 是（值） |

**工作原理**：

1. 读取现有 metafield 值
2. 检测是否包含 `%` 符号
3. 如果没有 `%` 符号，自动标记为需要更新
4. 更新时统一使用带 `%` 的格式

**首次运行**：

如果你的商店之前使用旧格式（`["15", "20", "30"]`），首次运行脚本时会：
- 自动检测到需要格式转换
- 将所有旧格式更新为新格式（`["15%", "20%", "30%"]`）
- 保持折扣值不变，只添加 `%` 符号

## 故障排查

### 常见问题

1. **没有计算折扣**
   - 检查 variant 是否有 `compare_at_price` 值
   - 确保 `compare_at_price > price`
   - 系统会自动跳过没有折扣的 variants

2. **Metafield 更新失败**
   - 检查 Shopify API token 权限
   - 确保 metafield 定义存在（namespace: custom, key: discounts, type: list.single_line_text_field）
   - 查看日志了解具体错误

3. **GraphQL 错误**
   - 确保 Shopify Admin API token 有效
   - 检查 API 版本（使用 2025-01）

4. **速率限制**
   - 脚本内置重试逻辑
   - 并发限制为 5 个请求
   - 如遇速率限制会自动等待

## 架构说明

### 文件结构

```
scripts/job_set_discounts_to_products/
├── __init__.py                 # 模块初始化
├── main.py                     # 主入口和调度器
├── discount_calculator.py      # 折扣计算和同步逻辑
├── models.py                   # 数据模型定义
└── README.md                   # 本文档
```

### 核心组件

- **ProductDiscountOrchestrator**: 主协调器，处理环境验证、客户端初始化和调度
- **DiscountCalculator**: 核心业务逻辑，处理折扣计算和同步
- **Models**: Pydantic 数据模型，确保类型安全

### GraphQL 使用

#### 1. 查询所有产品及 variants

```graphql
query getAllProducts($cursor: String) {
  products(first: 250, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        legacyResourceId
        title
        variants(first: 100) {
          edges {
            node {
              id
              legacyResourceId
              title
              price
              compareAtPrice
            }
          }
        }
        metafield(namespace: "custom", key: "discounts") {
          id
          value
          type
        }
      }
    }
  }
}
```

#### 2. 更新产品 metafield

```graphql
mutation setProductMetafield($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields {
      id
      namespace
      key
      value
    }
    userErrors {
      field
      message
    }
  }
}
```

使用示例：

```json
{
  "metafields": [
    {
      "ownerId": "gid://shopify/Product/123456",
      "namespace": "custom",
      "key": "discounts",
      "value": "[\"5%\", \"10%\", \"15%\", \"20%\"]",
      "type": "list.single_line_text_field"
    }
  ]
}
```

## 性能指标

基于测试结果（1712 个产品，167 个有折扣）：

- **产品获取**: ~7 秒（使用 GraphQL 分页）
- **折扣分析**: <1 秒（本地计算）
- **Metafield 更新**: ~30-40 秒（167 个产品，并发更新）
- **总执行时间**: 约 45 秒

### 性能优化

- 使用 GraphQL 一次性获取所有数据
- 只更新有变化的产品
- 并发更新 metafield（最多 5 个并发）
- 批量处理和进度报告

## 扩展性

脚本设计为易于扩展：

- 可以调整四舍五入的倍数（目前是 5，可改为 10 或其他值）
- 可以修改并发数量（`MAX_CONCURRENT_UPDATES`）
- 可以添加其他折扣计算逻辑
- 支持自定义折扣百分比范围过滤

## 日志

日志文件位置：`logs/product_discounts.log`

日志包含：
- 执行开始/结束时间
- 产品和折扣统计
- 更新进度
- 错误和警告信息
- 性能指标

## 与前端集成

前端可以使用 metafield 数据展示折扣标签：

```liquid
{% if product.metafields.custom.discounts %}
  <div class="discount-badges">
    {% for discount in product.metafields.custom.discounts %}
      <span class="discount-badge">-{{ discount }}</span>
    {% endfor %}
  </div>
{% endif %}
```

或者只显示最大折扣：

```liquid
{% if product.metafields.custom.discounts %}
  {% assign max_discount = product.metafields.custom.discounts | last %}
  <span class="max-discount">最高 -{{ max_discount }}</span>
{% endif %}
```
