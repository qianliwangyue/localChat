#!/usr/bin/env bash
# 构建 LoacalCP 的 MSIX 包 (供 Microsoft Store MSIX 提交使用)
# 用法: bash build-msix.sh
# 前置: 1) 项目根目录已用 fyne package 生成 localchat.exe
#       2) msix/identity.conf 中的身份字段已按 Partner Center 填写
set -euo pipefail

PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MSIX_DIR="$PROJ/msix"
STAGE="$MSIX_DIR/stage"
DIST="$PROJ/dist"
MAKEAPPX="${MAKEAPPX:-D:/Tools/MSIXTools/x64/makeappx.exe}"

# 读取配置
set -a; . "$MSIX_DIR/identity.conf"; set +a

SRC_EXE="${1:-$PROJ/localchat.exe}"
[ -f "$SRC_EXE" ] || { echo "缺少 $SRC_EXE，请先在项目根目录执行 fyne package -os windows -icon 1.ico -release -app-id com.ggbond.boom"; exit 1; }
[ -x "$MAKEAPPX" ] || [ -f "$MAKEAPPX" ] || { echo "找不到 makeappx: $MAKEAPPX"; exit 1; }

echo "== 1/4 准备暂存目录"
rm -rf "$STAGE"; mkdir -p "$STAGE/Assets" "$DIST"

echo "== 2/4 复制文件"
cp "$SRC_EXE" "$STAGE/LoacalCP.exe"
cp "$MSIX_DIR/Assets/"*.png "$STAGE/Assets/"

echo "== 3/4 生成 AppxManifest.xml"
sed -e "s|__IDENTITY_NAME__|${IDENTITY_NAME}|g" \
    -e "s|__PUBLISHER__|${PUBLISHER}|g" \
    -e "s|__VERSION__|${VERSION}|g" \
    -e "s|__DISPLAY_NAME__|${DISPLAY_NAME}|g" \
    -e "s|__PUBLISHER_DISPLAY_NAME__|${PUBLISHER_DISPLAY_NAME}|g" \
    -e "s|__DESCRIPTION__|${DESCRIPTION}|g" \
    "$MSIX_DIR/AppxManifest.template.xml" > "$STAGE/AppxManifest.xml"

echo "== 4/4 打包"
OUT_MSIX="$DIST/LocalCP-${VERSION}-x64.msix"
rm -f "$OUT_MSIX"
STAGE_W="$(cygpath -w "$STAGE")"
OUT_W="$(cygpath -w "$OUT_MSIX")"
"$MAKEAPPX" pack -d "$STAGE_W" -p "$OUT_W" -o

# 同时生成 .msixupload (商店提交格式 = 内含 msix 的 zip)
OUT_UP="$DIST/LocalCP-${VERSION}-x64.msixupload"
rm -f "$OUT_UP"
python - "$OUT_MSIX" "$OUT_UP" <<'PY'
import sys, zipfile
msix, up = sys.argv[1], sys.argv[2]
with zipfile.ZipFile(up, 'w', zipfile.ZIP_DEFLATED) as z:
    z.write(msix, 'LocalCP.msix')
print("  msixupload:", up)
PY

echo
echo "构建完成:"
ls -la "$DIST" | grep -i loacalcp
