# 删除Shopify商品标题翻译工具

这个工具集用于检测和删除Shopify商品的标题翻译。默认语言是西班牙语(es),但商品标题应该使用英语,不需要翻译成其他语言。

## 背景

在Shopify中,商品标题可能有多语言翻译。但对于我们的商店:
- 默认语言设置为西班牙语(es)
- 但所有商品标题实际上应该使用英语
- 不需要英语、法语、意大利语等其他语言的翻译

这个工具可以帮助批量删除这些不需要的翻译。

## 功能

### 1. 检查商品翻译 (`check_translations.py`)

检查单个商品的标题翻译情况。

**用法:**
```bash
# 使用默认测试商品ID
python scripts/remove_products_title_translations/check_translations.py

# 在脚本中修改test_product_id变量来检查其他商品
```

**输出示例:**
```
============================================================
商品翻译检查报告
============================================================
商品ID: 9941119435093
商品GID: gid://shopify/Product/9941119435093
当前标题: Air Jordan 4 Retro White Cement

默认语言: es
  值: Air Jordan 4 Retro White Cement

找到 1 个翻译:

  语言: fr ✅ 最新
    值: Air Jordan 4 Retro Blanc Ciment

============================================================

⚠️  该商品有标题翻译，语言包括: fr
建议: 删除这些翻译，因为默认字段已经使用英语名称
```

### 2. 删除单个商品的翻译 (`remove_translations.py`)

删除指定商品的标题翻译。

**用法:**
```bash
# 检查并删除默认测试商品的翻译(带确认)
python scripts/remove_products_title_translations/remove_translations.py --check-first

# 使用--yes自动确认
python scripts/remove_products_title_translations/remove_translations.py --check-first --yes

# 指定商品ID
python scripts/remove_products_title_translations/remove_translations.py -p 9941119435093 --check-first --yes

# 只删除特定语言
python scripts/remove_products_title_translations/remove_translations.py -p 9941119435093 -l fr --check-first --yes

# 模拟删除(不实际执行)
python scripts/remove_products_title_translations/remove_translations.py --check-first --dry-run
```

**参数说明:**
- `-p, --product-id`: 商品ID (默认: 9941119435093)
- `-l, --locales`: 要删除的语言列表 (默认: en fr it)
- `--check-first`: 先检查翻译再删除(推荐)
- `--dry-run`: 模拟删除,不实际执行
- `-y, --yes`: 自动确认,不询问

### 3. 批量删除所有商品的翻译 (`batch_remove.py`)

批量处理所有商品,删除标题翻译。

**用法:**
```bash
# 测试: 模拟删除前5个商品的翻译
python scripts/remove_products_title_translations/batch_remove.py --dry-run --limit 5

# 测试: 模拟删除前20个商品的翻译
python scripts/remove_products_title_translations/batch_remove.py --dry-run --limit 20

# 实际删除所有商品的翻译(需要确认)
python scripts/remove_products_title_translations/batch_remove.py

# 实际删除所有商品的翻译(自动确认)
python scripts/remove_products_title_translations/batch_remove.py --yes

# 只删除法语翻译
python scripts/remove_products_title_translations/batch_remove.py -l fr --yes

# 删除英语和法语翻译
python scripts/remove_products_title_translations/batch_remove.py -l en fr --yes
```

**参数说明:**
- `-l, --locales`: 要删除的语言列表 (默认: en fr it)
- `--dry-run`: 模拟删除,不实际执行
- `--limit`: 限制处理的商品数量(用于测试)
- `-y, --yes`: 自动确认,不询问

**输出示例:**
```
============================================================
批量处理报告
============================================================
总商品数: 5
有翻译的商品数: 5
成功处理的商品数: 5
删除的翻译数: 10
失败次数: 0
============================================================

详细信息 (显示前10个):

1. Nike Air Force 1 Low White Supreme (7543936286890)
   发现的翻译: en, fr
   已删除: en, fr

2. Nike Air Force 1 Low Black Supreme (7543937499306)
   发现的翻译: en, fr
   已删除: en, fr
...
```

## 使用建议

1. **首先进行小规模测试:**
   ```bash
   python scripts/remove_products_title_translations/batch_remove.py --dry-run --limit 10
   ```

2. **确认结果正常后,扩大范围测试:**
   ```bash
   python scripts/remove_products_title_translations/batch_remove.py --dry-run --limit 50
   ```

3. **最后执行实际删除:**
   ```bash
   python scripts/remove_products_title_translations/batch_remove.py --yes
   ```

## 技术说明

### 支持的语言代码
- `en`: 英语
- `fr`: 法语
- `it`: 意大利语
- `es`: 西班牙语(默认语言)

### API说明
脚本使用Shopify GraphQL Admin API:
- 查询翻译: `translatableResource` query
- 删除翻译: `translationsRemove` mutation

### 安全机制
- 支持dry-run模式进行模拟测试
- 需要用户确认(除非使用--yes参数)
- API限流保护(请求之间有延迟)
- 详细的日志记录

## 环境变量

需要在`.env`文件中配置:
```
SHOPIFY_ADMIN_TOKEN=your_admin_token
SHOPIFY_SHOP_DOMAIN=your_shop.myshopify.com
```

## 注意事项

1. ⚠️ **删除操作不可逆**: 删除翻译后需要手动重新添加
2. ⚠️ **API限制**: 批量操作可能需要较长时间
3. ⚠️ **权限要求**: 需要Shopify Admin API的翻译写入权限
4. ✅ **建议先用dry-run模式测试**
5. ✅ **建议先限制处理数量测试(--limit)**

## 示例工作流

### 测试单个商品
```bash
# 1. 检查商品翻译
python scripts/remove_products_title_translations/check_translations.py

# 2. 模拟删除
python scripts/remove_products_title_translations/remove_translations.py --check-first --dry-run

# 3. 实际删除
python scripts/remove_products_title_translations/remove_translations.py --check-first --yes

# 4. 再次检查确认
python scripts/remove_products_title_translations/check_translations.py
```

### 批量处理所有商品
```bash
# 1. 小规模测试(5个商品)
python scripts/remove_products_title_translations/batch_remove.py --dry-run --limit 5

# 2. 中等规模测试(50个商品)
python scripts/remove_products_title_translations/batch_remove.py --dry-run --limit 50

# 3. 全量执行
python scripts/remove_products_title_translations/batch_remove.py --yes
```

## 故障排查

### 错误: Missing environment variables
**原因:** 缺少环境变量配置
**解决:** 在`.env`文件中配置`SHOPIFY_ADMIN_TOKEN`和`SHOPIFY_SHOP_DOMAIN`

### 错误: GraphQL errors
**原因:** API权限不足或查询错误
**解决:** 检查Shopify Admin API token是否有足够的权限

### 错误: Rate limited
**原因:** API请求过于频繁
**解决:** 脚本会自动重试,耐心等待即可

## 更多信息

查看源代码中的注释和文档字符串获取更多技术细节。
