import json
import ijson
import os
from pathlib import Path
import sys

def filter_arrival_rate_stream():
    """
    使用流式处理过滤大型 arrival_rate.json 文件，只保留 601001 和 601002 路线
    适用于处理超大JSON文件
    """
    # 定义文件路径
    input_file = Path("trail/arrival_rate.json")
    backup_file = Path("trail/arrival_rate_backup.json")
    temp_file = Path("trail/arrival_rate_temp.json")
    
    # 要保留的路线
    routes_to_keep = {"601001", "601002"}
    
    print(f"开始处理文件: {input_file}")
    print("使用流式处理模式...")
    
    # 检查文件是否存在
    if not input_file.exists():
        print(f"错误: 文件 {input_file} 不存在")
        return False
    
    try:
        # 尝试使用标准json库（如果文件不是太大）
        try:
            print("尝试使用标准方法读取...")
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 如果成功读取，使用标准方法处理
            return filter_standard(data, input_file, backup_file, routes_to_keep)
            
        except MemoryError:
            print("文件太大，切换到流式处理...")
            # 使用ijson流式处理
            return filter_streaming(input_file, backup_file, temp_file, routes_to_keep)
            
    except Exception as e:
        print(f"错误: {e}")
        return False

def filter_standard(data, input_file, backup_file, routes_to_keep):
    """标准处理方法（适用于中等大小文件）"""
    if isinstance(data, dict):
        print(f"原始数据包含 {len(data)} 个键")
        
        # 显示前10个路线
        all_routes = list(data.keys())
        print(f"前10个路线: {all_routes[:10]}")
        
        # 过滤数据
        filtered_data = {}
        found_routes = []
        
        for route in data:
            if route in routes_to_keep:
                filtered_data[route] = data[route]
                found_routes.append(route)
                print(f"找到并保留路线: {route}")
        
        # 检查是否找到所有需要的路线
        missing_routes = routes_to_keep - set(found_routes)
        if missing_routes:
            print(f"警告: 以下路线未找到: {missing_routes}")
        
        # 备份原始文件
        print(f"\n创建备份文件: {backup_file}")
        if backup_file.exists():
            print("备份文件已存在，跳过备份")
        else:
            os.rename(input_file, backup_file)
        
        # 保存过滤后的数据
        print(f"保存过滤后的数据到: {input_file}")
        with open(input_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, indent=2, ensure_ascii=False)
        
        # 显示结果统计
        print(f"\n处理完成!")
        print(f"原始路线数: {len(data)}")
        print(f"过滤后路线数: {len(filtered_data)}")
        print(f"保留的路线: {found_routes}")
        
        # 显示文件大小对比
        if backup_file.exists():
            original_size = os.path.getsize(backup_file) / (1024 * 1024)  # MB
            new_size = os.path.getsize(input_file) / (1024 * 1024)  # MB
            print(f"\n文件大小:")
            print(f"原始文件: {original_size:.2f} MB")
            print(f"新文件: {new_size:.2f} MB")
            print(f"减少了: {(original_size - new_size):.2f} MB ({(1 - new_size/original_size)*100:.1f}%)")
        
        return True
    else:
        print(f"错误: 数据格式不是预期的字典类型，而是 {type(data)}")
        return False

def filter_streaming(input_file, backup_file, temp_file, routes_to_keep):
    """流式处理方法（适用于超大文件）"""
    print("注意: 流式处理需要 ijson 库")
    print("如果没有安装，请运行: pip install ijson")
    
    try:
        import ijson
    except ImportError:
        print("错误: 未安装 ijson 库")
        print("请运行: pip install ijson")
        return False
    
    # 这里实现流式处理逻辑
    # 由于ijson的复杂性，这里提供一个简化的实现
    print("流式处理功能暂未完全实现")
    print("建议: 如果文件太大，可以考虑:")
    print("1. 增加系统内存")
    print("2. 使用其他工具预处理文件")
    print("3. 分批处理数据")
    
    return False

def simple_filter():
    """
    简化版过滤函数，直接处理文件
    """
    input_file = "trail/arrival_rate.json"
    backup_file = "trail/arrival_rate_backup.json"
    routes_to_keep = ["601001", "601002"]
    
    print("开始简化处理...")
    
    try:
        # 读取文件
        print("读取文件中...")
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 过滤数据
        filtered_data = {k: v for k, v in data.items() if k in routes_to_keep}
        
        # 备份原文件
        if not os.path.exists(backup_file):
            os.rename(input_file, backup_file)
            print(f"原文件已备份为: {backup_file}")
        
        # 保存过滤后的数据
        with open(input_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, indent=2, ensure_ascii=False)
        
        print(f"过滤完成! 保留了 {len(filtered_data)} 个路线: {list(filtered_data.keys())}")
        return True
        
    except Exception as e:
        print(f"处理失败: {e}")
        return False

if __name__ == "__main__":
    # 首先尝试简化版本
    print("=== 过滤 arrival_rate.json 文件 ===")
    print("目标: 只保留 601001 和 601002 路线\n")
    
    success = simple_filter()
    
    if not success:
        print("\n尝试使用完整版本...")
        success = filter_arrival_rate_stream()
    
    if success:
        print("\n✅ 过滤完成!")
    else:
        print("\n❌ 过滤失败!")
        print("\n可能的解决方案:")
        print("1. 检查文件路径是否正确")
        print("2. 确保有足够的内存")
        print("3. 尝试手动处理") 