#!/usr/bin/env bash
# WorkBuddy 反代管家 .deb 构建器（Linux / macOS 通用，不依赖 dpkg-deb）
#   用 ar + tar + xz 手工组装 debian-binary / control.tar.xz / data.tar.xz
set -euo pipefail

cd "$(dirname "$0")"

PKG=$(awk -F': *' '/^Package:/{print $2}' DEBIAN/control)
VER=$(awk -F': *' '/^Version:/{print $2}'  DEBIAN/control)
ARCH=$(awk -F': *' '/^Architecture:/{print $2}' DEBIAN/control)
OUT="dist/${PKG}_${VER}_${ARCH}.deb"

echo "==> 构建 ${PKG} ${VER} (${ARCH})"
rm -rf build dist
mkdir -p build/DEBIAN dist

# ── 1. 校验源码 ──
python3 -c "import ast,sys; ast.parse(open('src/usr/share/workbuddy/wb_server.py',encoding='utf-8').read()); print('   wb_server.py AST OK')"
python3 -c "
import sys
a=open('src/usr/share/workbuddy/index.html','rb').read()
b=open('src/usr/share/workbuddy/panel/index.html','rb').read()
assert a==b, 'index.html 与 panel/index.html 不同步'
print('   两份 HTML 同步 OK (%d bytes)' % len(a))
"

# ── 2. 拷贝源码到 staging ──
cp -a src/. build/

# 修权限：plist / 脚本
chmod 755 build/usr/share/workbuddy/wbstart || true
chmod 644 build/usr/share/workbuddy/wb_server.py
chmod 644 build/usr/share/workbuddy/index.html
chmod 644 build/usr/share/workbuddy/panel/index.html
chmod 644 build/Library/LaunchDaemons/com.jkgl.workbuddy.plist || true

# Installed-Size (KB)
SIZE=$(du -sk build | awk '{print $1}')
if grep -q '^Installed-Size:' DEBIAN/control; then
  sed -i.bak "s/^Installed-Size:.*/Installed-Size: ${SIZE}/" DEBIAN/control && rm -f DEBIAN/control.bak
else
  printf 'Installed-Size: %s\n' "${SIZE}" >> DEBIAN/control
fi

# ── 3. control.tar.xz ──
tar -C DEBIAN --owner=0 --group=0 --numeric-owner \
    -cJf build/control.tar.xz ./control ./postinst ./prerm

# ── 4. data.tar.xz ──
tar -C build --owner=0 --group=0 --numeric-owner \
    -cJf build/data.tar.xz ./usr ./Library ./rootfs

# ── 5. ar 组装 deb ──
printf '2.0\n' > build/debian-binary
cd build
ar rc "../${OUT}" debian-binary control.tar.xz data.tar.xz
cd ..

# ── 6. 产物信息 ──
ls -l "${OUT}"
echo
echo "==> 包内文件清单"
tar tJf build/data.tar.xz
echo
echo "==> control"
cat DEBIAN/control
echo
echo "==> 校验和"
sha256sum "${OUT}"
