"""
简化的模拟运行脚本（不包含ETL）
"""
import sys
import os

# 确保能找到simulation包
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from simulation.simulation_runner import SimulationRunner
from sim_hook.enhanced_integration import integrate_data_collection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """运行单次模拟，不启动ETL"""
    logger.info("=" * 80)
    logger.info("开始简化模拟（不含ETL）")
    logger.info("=" * 80)
    
    # 1. 初始化SimulationRunner
    simulation = SimulationRunner(scenario_name="601")
    logger.info(f"模拟初始化成功 - 日期: {simulation.selected_month}月{simulation.selected_day}日")
    
    # 2. 集成数据采集钩子
    data_hook = integrate_data_collection(simulation, output_dir="./simulation_data")
    logger.info("数据采集钩子已集成")
    
    # 3. 运行模拟
    try:
        logger.info("开始运行模拟...")
        simulation.run()
        logger.info("模拟运行完成！")
    except Exception as e:
        logger.error(f"模拟运行失败: {e}", exc_info=True)
    finally:
        # 停止数据采集钩子
        try:
            data_hook.stop()
            logger.info("数据采集钩子已停止")
        except Exception as e:
            logger.warning(f"停止数据采集钩子失败: {e}")
    
    # 4. 检查生成的数据文件
    data_dir = "./simulation_data"
    if os.path.exists(data_dir):
        files = os.listdir(data_dir)
        logger.info(f"\n生成了 {len(files)} 个数据文件")
        if files:
            for i, f in enumerate(files[:10]):  # 只显示前10个
                size = os.path.getsize(os.path.join(data_dir, f))
                logger.info(f"  {i+1}. {f} ({size:,} bytes)")
            if len(files) > 10:
                logger.info(f"  ... 还有 {len(files) - 10} 个文件")

if __name__ == "__main__":
    main() 