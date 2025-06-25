import pandas as pd
import zipfile
import os
import shutil

def filter_gtfs_for_route_601(input_gtfs, output_gtfs):
    # 清理临时目录
    if os.path.exists('temp_gtfs'):
        shutil.rmtree('temp_gtfs')
    if os.path.exists('filtered_gtfs'):
        shutil.rmtree('filtered_gtfs')
    
    # 解压GTFS文件
    print("解压GTFS文件...")
    with zipfile.ZipFile(input_gtfs, 'r') as zip_ref:
        zip_ref.extractall('temp_gtfs')
    
    # 读取routes.txt，查找601路线
    print("查找601路线...")
    routes = pd.read_csv('temp_gtfs/routes.txt')
    
    # 尝试多种方式找到601路线
    route_601 = routes[
        (routes['route_short_name'].astype(str) == '601') |
        (routes['route_short_name'].astype(str) == '0601') |
        (routes['route_id'].astype(str) == '601') |
        (routes['route_long_name'].astype(str).str.contains('601', na=False))
    ]
    
    if route_601.empty:
        print("错误：未找到601路线！")
        print("可用的路线short_name:")
        print(routes['route_short_name'].unique())
        return False
    
    print(f"找到601路线: {route_601[['route_id', 'route_short_name', 'route_long_name']].to_string()}")
    
    # 获取601路线的route_id
    route_ids = route_601['route_id'].tolist()
    
    # 过滤trips.txt
    print("过滤trips...")
    trips = pd.read_csv('temp_gtfs/trips.txt')
    filtered_trips = trips[trips['route_id'].isin(route_ids)]
    trip_ids = filtered_trips['trip_id'].tolist()
    
    print(f"找到 {len(trip_ids)} 个601路线的班次")
    
    if len(trip_ids) == 0:
        print("错误：没有找到601路线的班次数据")
        return False
    
    # 过滤stop_times.txt
    print("过滤stop_times...")
    stop_times = pd.read_csv('temp_gtfs/stop_times.txt')
    filtered_stop_times = stop_times[stop_times['trip_id'].isin(trip_ids)]
    
    # 获取相关站点
    stop_ids = filtered_stop_times['stop_id'].unique()
    print(f"找到 {len(stop_ids)} 个601路线相关的站点")
    
    # 过滤stops.txt
    print("过滤stops...")
    stops = pd.read_csv('temp_gtfs/stops.txt')
    filtered_stops = stops[stops['stop_id'].isin(stop_ids)]
    
    # 创建输出目录
    os.makedirs('filtered_gtfs', exist_ok=True)
    
    # 保存过滤后的文件
    print("保存过滤后的文件...")
    route_601.to_csv('filtered_gtfs/routes.txt', index=False)
    filtered_trips.to_csv('filtered_gtfs/trips.txt', index=False)
    filtered_stop_times.to_csv('filtered_gtfs/stop_times.txt', index=False)
    filtered_stops.to_csv('filtered_gtfs/stops.txt', index=False)
    
    # 复制其他必要文件
    required_files = ['agency.txt', 'calendar.txt', 'calendar_dates.txt', 'shapes.txt']
    for file in required_files:
        if os.path.exists(f'temp_gtfs/{file}'):
            try:
                df = pd.read_csv(f'temp_gtfs/{file}')
                df.to_csv(f'filtered_gtfs/{file}', index=False)
                print(f"复制了 {file}")
            except:
                print(f"跳过 {file} (可能为空或格式问题)")
    
    # 创建新的zip文件
    print(f"创建过滤后的GTFS文件: {output_gtfs}")
    with zipfile.ZipFile(output_gtfs, 'w') as zip_ref:
        for root, dirs, files in os.walk('filtered_gtfs'):
            for file in files:
                if file.endswith('.txt'):
                    zip_ref.write(os.path.join(root, file), file)
    
    # 清理临时文件
    shutil.rmtree('temp_gtfs')
    shutil.rmtree('filtered_gtfs')
    
    print(f"✅ 成功！过滤后的GTFS文件已保存为: {output_gtfs}")
    return True

# 运行过滤
if __name__ == "__main__":
    success = filter_gtfs_for_route_601('google_transit.zip', 'google_transit_601.zip')
    if success:
        print("\n下一步运行:")
        print("python gtfs/gtfs2pt.py --gtfs google_transit_601.zip --date 20250511 -n map.net.xml --route-output ./routes_601.rou.xml --additional-output ./stops_601.add.xml")