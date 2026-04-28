#!/bin/bash
# =============================================
# Beyond Compare 彻底卸载脚本
# 保留安装包/许可证文件
# =============================================

set -e

APP_NAME="Beyond Compare"
BUNDLE_ID="com.ScooterSoftware.BeyondCompare"

echo "=========================================="
echo "  $APP_NAME 彻底卸载脚本"
echo "=========================================="

# 1. 关闭运行中的进程
if pgrep -f "$APP_NAME.app" > /dev/null 2>&1; then
    echo "[1/5] 关闭运行中的 $APP_NAME..."
    pkill -f "$APP_NAME.app" || true
    sleep 1
else
    echo "[1/5] $APP_NAME 未在运行，跳过"
fi

# 2. 删除应用本体
if [ -d "/Applications/$APP_NAME.app" ]; then
    echo "[2/5] 删除应用本体: /Applications/$APP_NAME.app"
    sudo rm -rf "/Applications/$APP_NAME.app"
else
    echo "[2/5] 应用本体不存在，跳过"
fi

# 3. 删除用户缓存数据
CACHE_PATH="$HOME/Library/Caches/$BUNDLE_ID"
if [ -d "$CACHE_PATH" ]; then
    echo "[3/5] 删除缓存: $CACHE_PATH"
    rm -rf "$CACHE_PATH"
else
    echo "[3/5] 缓存不存在，跳过"
fi

# 4. 删除用户偏好设置（包含注册许可证）
PLIST_PATH="$HOME/Library/Preferences/$BUNDLE_ID.plist"
if [ -f "$PLIST_PATH" ]; then
    echo "[4/5] 删除偏好设置(含许可证): $PLIST_PATH"
    rm -f "$PLIST_PATH"
else
    echo "[4/5] 偏好设置不存在，跳过"
fi

# 5. 删除 Application Support（设置/配置）
AS_PATH="$HOME/Library/Application Support/$APP_NAME"
AS_PATH2="$HOME/Library/Application Support/$APP_NAME 5"
if [ -d "$AS_PATH" ] || [ -d "$AS_PATH2" ]; then
    echo "[5/5] 删除 Application Support..."
    [ -d "$AS_PATH" ] && rm -rf "$AS_PATH" && echo "  已删除: $AS_PATH"
    [ -d "$AS_PATH2" ] && rm -rf "$AS_PATH2" && echo "  已删除: $AS_PATH2"
else
    echo "[5/5] Application Support 不存在，跳过"
fi

echo ""
echo "=========================================="
echo "  卸载完成！"
echo "=========================================="
echo ""
echo "已删除内容："
echo "  - 应用本体"
echo "  - 缓存数据"
echo "  - 偏好设置（含许可证信息）"
echo "  - Application Support 配置"
echo ""
echo "保留内容："
echo "  - .dmg 安装包（可在桌面/下载目录找到）"
echo "  - 任何手动备份的许可证文件"
echo ""
echo "重新安装：双击 .dmg 即可，许可证可重新输入。"
