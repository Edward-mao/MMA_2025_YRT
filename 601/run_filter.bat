@echo off
chcp 65001 > nul
echo.
echo ====================================
echo    过滤 arrival_rate.json 文件
echo    只保留 601001 和 601002 路线
echo ====================================
echo.

REM 检查Python是否安装
python --version > nul 2>&1
if errorlevel 1 (
    echo 错误: 未检测到 Python!
    echo 请先安装 Python 3.x
    pause
    exit /b 1
)

REM 运行过滤脚本
echo 开始运行过滤脚本...
echo.

REM 首先尝试流式处理版本（包含简化处理）
python filter_arrival_rate_stream.py

if errorlevel 1 (
    echo.
    echo 流式处理版本失败，尝试标准版本...
    python filter_arrival_rate.py
)

echo.
echo ====================================
echo.
pause 