import json
import os
from pathlib import Path

def filter_arrival_rate():
    """
    过滤 arrival_rate.json 文件，只保留 601001 和 601002 路线
    """
    # 定义文件路径
    input_file = Path("trail/arrival_rate.json")
    backup_file = Path("trail/arrival_rate_backup.json")
    
    # 要保留的路线
    routes_to_keep = ["601001", "601002"]
    
    print(f"开始处理文件: {input_file}")
    
    # 检查文件是否存在
    if not input_file.exists():
        print(f"错误: 文件 {input_file} 不存在")
        return False
    
    try:
        # 读取原始数据
        print("正在读取原始数据...")
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 显示原始数据信息
        if isinstance(data, dict):
            print(f"原始数据包含 {len(data)} 个键")
            all_routes = list(data.keys())
            print(f"所有路线: {all_routes}")
            
            # 过滤数据
            filtered_data = {}
            for route in routes_to_keep:
                if route in data:
                    filtered_data[route] = data[route]
                    print(f"保留路线: {route}")
                else:
                    print(f"警告: 路线 {route} 不存在于原始数据中")
            
            # 备份原始文件
            print(f"\n创建备份文件: {backup_file}")
            os.rename(input_file, backup_file)
            
            # 保存过滤后的数据
            print(f"保存过滤后的数据到: {input_file}")
            with open(input_file, 'w', encoding='utf-8') as f:
                json.dump(filtered_data, f, indent=2, ensure_ascii=False)
            
            # 显示结果统计
            print(f"\n处理完成!")
            print(f"原始路线数: {len(data)}")
            print(f"过滤后路线数: {len(filtered_data)}")
            print(f"删除的路线: {set(all_routes) - set(routes_to_keep)}")
            
            # 显示文件大小对比
            original_size = os.path.getsize(backup_file) / (1024 * 1024)  # MB
            new_size = os.path.getsize(input_file) / (1024 * 1024)  # MB
            print(f"\n文件大小:")
            print(f"原始文件: {original_size:.2f} MB")
            print(f"新文件: {new_size:.2f} MB")
            print(f"减少了: {(original_size - new_size):.2f} MB ({(1 - new_size/original_size)*100:.1f}%)")
            
        else:
            print(f"错误: 数据格式不是预期的字典类型，而是 {type(data)}")
            return False
            
    except json.JSONDecodeError as e:
        print(f"错误: JSON 解析失败 - {e}")
        return False
    except Exception as e:
        print(f"错误: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = filter_arrival_rate()
    if success:
        print("\n✅ 过滤完成! 原始文件已备份为 arrival_rate_backup.json")
    else:
        print("\n❌ 过滤失败!") 