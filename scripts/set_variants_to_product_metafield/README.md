# Set Variants to Product Metafield

自动将产品的所有 variant names 同步到 `custom.variants` metafield。

## 功能说明

此脚本会：
1. 获取所有 Shopify 产品及其 variants
2. **解析和归一化** variant titles：
   - 分割 " - " 并只保留第一部分（尺码部分）
   - 归一化小数尺码：
     - `.0, .1, .2, .3` → 去除小数（如 `37.3 EU` → `37 EU`）
     - `.4, .5` → 保持为 `.5`（如 `37.5 EU` → `37.5 EU`）
     - `.6, .7, .8, .9` → 四舍五入到 `.5`（如 `37.6 EU` → `37.5 EU`）
3. **去重**：移除重复的 variant names（解析后可能产生重复）
4. 将处理后的 variant names 推送到 `custom.variants` metafield
5. 默认只更新空的 `custom.variants`，跳过已有值的产品
6. 支持 `--all` 参数强制更新所有产品

## Variant Title 解析示例

| 原始 Variant Title | 解析后 | 说明 |
|-------------------|--------|------|
| `35.5 EU - Color` | `35.5 EU` | 保留 `.5` |
| `36 EU - Red` | `36 EU` | 整数尺码 |
| `37.3 EU - Blue` | `37 EU` | `.3` 去除小数 |
| `37.6 EU - Green` | `37.5 EU` | `.6` 四舍五入到 `.5` |
| `38.0 EU` | `38 EU` | `.0` 转为整数 |
| `38.4 EU` | `38.5 EU` | `.4` 保持为 `.5` |
| `40 EU W - Wide` | `40 EU W` | 保留 "W" 等后缀 |
| `Default Title` | `Default Title` | 非尺码不变 |

## Metafield 配置

- **Namespace**: `custom`
- **Key**: `variants`
- **Type**: `list.single_line_text_field`
- **Value**: 解析、归一化并去重后的 variant names 数组

## 使用方法

### 1. 激活虚拟环境

```bash
source venv/bin/activate
```

### 2. 运行脚本

#### Dry-run 模式（查看将要更新的内容，不实际更新）

```bash
python scripts/set_variants_to_product_metafield/main.py --dry-run
```

#### 更新空的 custom.variants（默认模式）

```bash
python scripts/set_variants_to_product_metafield/main.py
```

#### 强制更新所有产品

```bash
python scripts/set_variants_to_product_metafield/main.py --all
```

#### 组合使用

```bash
# Dry-run 模式 + 更新所有
python scripts/set_variants_to_product_metafield/main.py --all --dry-run
```

## 参数说明

| 参数 | 说明 |
|------|------|
| `--all` | 更新所有产品，即使 custom.variants 已有值 |
| `--dry-run` | 仅分析，不实际更新（推荐先运行此模式） |

## 示例输出

### Dry-run 模式

```
============================================================
🚀 STARTING VARIANTS METAFIELD SYNC
============================================================
Mode: UPDATE EMPTY ONLY
Dry run: True
============================================================
📦 Fetching all products with variants from Shopify...
✅ Fetched 1728 products total
📊 Processing 1728 products...
Mode: UPDATE EMPTY ONLY
Dry run: True
📝 Products to update: 1728
⏭️  Products to skip: 0
🔍 DRY RUN - No changes will be made
  Would update: Nike Air Force 1 Low White Supreme (15 variants)
  Would update: Nike Air Force 1 Low Black Supreme (16 variants)
  ...
```

### 实际更新模式

```
🔄 Updating 1728 products...
✅ [1/1728] Updated: Nike Air Force 1 Low White Supreme (15 variants)
✅ [2/1728] Updated: Nike Air Force 1 Low Black Supreme (16 variants)
...

============================================================
📊 SYNC SUMMARY
============================================================
Total products: 1728
✅ Updated: 1728
⏭️  Skipped: 0
❌ Failed: 0
============================================================
```

## 性能优化

- **并发处理**: 最多 5 个产品同时更新
- **分页获取**: 每次获取 50 个产品
- **GraphQL API**: 使用 Shopify GraphQL API 提高效率

## 日志

日志文件保存在：
```
logs/set_variants_metafield.log
```

## 注意事项

1. **首次运行建议使用 dry-run 模式**，确认要更新的内容
2. **默认模式**只更新空的 `custom.variants`，不会覆盖已有数据
3. 使用 `--all` 参数会**覆盖所有现有数据**，请谨慎使用
4. 确保 Shopify Admin API Token 有 `write_products` 权限

## 环境变量

需要在 `.env` 文件中配置：

```env
SHOPIFY_ADMIN_TOKEN=your_shopify_admin_token
SHOPIFY_SHOP_DOMAIN=yourshop.myshopify.com
```

## 错误处理

- 单个产品更新失败不会影响其他产品
- 所有错误都会被记录到日志文件
- 失败的产品会在最终摘要中显示
