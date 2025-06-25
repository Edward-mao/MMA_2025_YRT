#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动生成站点映射文件脚本
用于生成真实站点ID和SUMO站点ID之间的映射关系

使用方法：
python generate_stop_mapping.py [--route_id 601] [--output stop_mapping.json]
"""

import pandas as pd
import xml.etree.ElementTree as ET
import json
import argparse
import os
import re
from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher

def similarity(a: str, b: str) -> float:
    """计算两个字符串的相似度"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def normalize_stop_name(name: str) -> str:
    """标准化站点名称，去除特殊字符和多余空格"""
    # 去除平台信息
    name = re.sub(r'\s+PLATFORM\s+\d+', '', name, flags=re.IGNORECASE)
    # 去除方向信息
    name = re.sub(r'\s+(NORTHBOUND|SOUTHBOUND|NB|SB)', '', name, flags=re.IGNORECASE)
    # 去除多余的空格和特殊字符
    name = re.sub(r'\s+', ' ', name.strip())
    # 统一分隔符
    name = name.replace(' / ', ' / ').replace('/', ' / ')
    return name

def load_route_stops(trail_stops_file: str, route_id: str = "601") -> Dict[str, List[str]]:
    """
    从trail/stops.json文件加载路线站点信息
    返回: {"northbound": [stop_ids], "southbound": [stop_ids]}
    """
    try:
        with open(trail_stops_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        routes = data.get('routes', {})
        
        # 查找主要的北向和南向路线（通常是最长的完整路线）
        northbound_stops = []
        southbound_stops = []
        
        # 明确指定主要路线ID
        main_northbound_route = f"{route_id}001"  # 601001
        main_southbound_route = f"{route_id}002"  # 601002
        
        print(f"寻找主要路线: {main_northbound_route} (北向), {main_southbound_route} (南向)")
        
        for route_key, route_info in routes.items():
            direction = route_info.get('direction', '').upper()
            stops = route_info.get('stops', [])
            
            # 按sequence排序
            stops.sort(key=lambda x: x.get('sequence', 0))
            stop_ids = [stop['stop_id'] for stop in stops]
            
            if route_key == main_northbound_route and direction == 'NORTHBOUND':
                northbound_stops = stop_ids
                print(f"✓ 找到北向主路线 {route_key}: {len(stop_ids)} 个站点")
            elif route_key == main_southbound_route and direction == 'SOUTHBOUND':
                southbound_stops = stop_ids
                print(f"✓ 找到南向主路线 {route_key}: {len(stop_ids)} 个站点")
        
        # 如果没有找到主要路线，则使用最长的路线作为备选
        if not northbound_stops or not southbound_stops:
            print("没有找到指定的主要路线，搜索所有相关路线...")
            northbound_candidates = []
            southbound_candidates = []
            
            for route_key, route_info in routes.items():
                if route_key.startswith(route_id):
                    direction = route_info.get('direction', '').upper()
                    stops = route_info.get('stops', [])
                    
                    # 按sequence排序
                    stops.sort(key=lambda x: x.get('sequence', 0))
                    stop_ids = [stop['stop_id'] for stop in stops]
                    
                    if direction == 'NORTHBOUND':
                        northbound_candidates.append((route_key, stop_ids))
                    elif direction == 'SOUTHBOUND':
                        southbound_candidates.append((route_key, stop_ids))
            
            # 选择最长的路线
            if not northbound_stops and northbound_candidates:
                northbound_candidates.sort(key=lambda x: len(x[1]), reverse=True)
                selected_route, northbound_stops = northbound_candidates[0]
                print(f"⚠️  使用备选北向路线 {selected_route}: {len(northbound_stops)} 个站点")
            
            if not southbound_stops and southbound_candidates:
                southbound_candidates.sort(key=lambda x: len(x[1]), reverse=True)
                selected_route, southbound_stops = southbound_candidates[0]
                print(f"⚠️  使用备选南向路线 {selected_route}: {len(southbound_stops)} 个站点")
        
        print(f"已加载路线 {route_id}:")
        print(f"  北向站点: {len(northbound_stops)} 个")
        print(f"  南向站点: {len(southbound_stops)} 个")
        
        return {
            "northbound": northbound_stops,
            "southbound": southbound_stops
        }
    except Exception as e:
        print(f"读取路线停站文件时出错: {e}")
        return {"northbound": [], "southbound": []}

def load_gtfs_stops(gtfs_stops_file: str) -> Dict[str, str]:
    """
    从GTFS stops.txt文件加载站点信息
    返回: {stop_id: stop_name}
    """
    try:
        df = pd.read_csv(gtfs_stops_file)
        stops = {}
        for _, row in df.iterrows():
            stop_id = str(row['stop_id'])
            stop_name = normalize_stop_name(str(row['stop_name']))
            stops[stop_id] = stop_name
        print(f"已加载 {len(stops)} 个GTFS站点")
        return stops
    except Exception as e:
        print(f"读取GTFS停站文件时出错: {e}")
        return {}

def load_sumo_stops(sumo_stops_file: str) -> Dict[str, Tuple[str, str]]:
    """
    从SUMO stops.add.xml文件加载站点信息
    返回: {sumo_stop_id: (stop_name, route_direction)}
    """
    try:
        tree = ET.parse(sumo_stops_file)
        root = tree.getroot()
        
        stops = {}
        for bus_stop in root.findall('busStop'):
            stop_id = bus_stop.get('id')
            stop_name = normalize_stop_name(bus_stop.get('name', ''))
            
            # 从stop_id中提取路线方向信息
            route_base = stop_id.split('.')[0] if '.' in stop_id else stop_id
            stops[stop_id] = (stop_name, route_base)
        
        print(f"已加载 {len(stops)} 个SUMO站点")
        return stops
    except Exception as e:
        print(f"读取SUMO停站文件时出错: {e}")
        return {}

def create_stop_mapping_from_route_order(route_stops: Dict[str, List[str]], 
                                        gtfs_stops: Dict[str, str], 
                                        sumo_stops: Dict[str, Tuple[str, str]], 
                                        route_id: str = "601") -> Dict:
    """
    根据路线站点顺序创建站点映射
    """
    mapping = {
        "simpy_to_sumo": {
            "northbound": {},
            "southbound": {}
        },
        "sumo_routes": {}
    }
    
    # 设置路线信息
    northbound_route = "1875876"
    southbound_route = "1875927"
    
    mapping["sumo_routes"][route_id] = {
        "northbound": northbound_route,
        "southbound": southbound_route
    }
    
    print(f"\n=== 开始映射站点 ===")
    print(f"北向SUMO路线: {northbound_route}")
    print(f"南向SUMO路线: {southbound_route}")
    
    # 映射北向站点
    northbound_gtfs = route_stops["northbound"]
    southbound_gtfs = route_stops["southbound"]
    
    print(f"\n=== 北向站点映射 ===")
    print(f"北向站点总数: {len(northbound_gtfs)}")
    northbound_matches = []
    for i, gtfs_stop_id in enumerate(northbound_gtfs):
        if gtfs_stop_id in gtfs_stops:
            gtfs_name = gtfs_stops[gtfs_stop_id]
            
            # 北向：直接按顺序映射到 1875876.0 到 1875876.26
            sumo_stop_id = f"{northbound_route}.{i}"
            mapping["simpy_to_sumo"]["northbound"][gtfs_stop_id] = sumo_stop_id
            northbound_matches.append((gtfs_stop_id, gtfs_name, sumo_stop_id))
            print(f"✓ [{i+1:2d}] {gtfs_stop_id} '{gtfs_name}' -> {sumo_stop_id}")
        else:
            print(f"✗ [{i+1:2d}] {gtfs_stop_id} -> 在GTFS数据中未找到")
    
    print(f"\n=== 南向站点映射 ===")
    print(f"南向站点总数: {len(southbound_gtfs)}")
    southbound_matches = []
    for i, gtfs_stop_id in enumerate(southbound_gtfs):
        if gtfs_stop_id in gtfs_stops:
            gtfs_name = gtfs_stops[gtfs_stop_id]
            
            # 南向站点映射逻辑：
            # 根据SUMO路线文件，南向路线的站点顺序为：
            # 1875876.26 (起点，对应北向终点)
            # 1875927.1 到 1875927.25 (中间站点)
            # 1875876.0 (终点，对应北向起点)
            
            if i == 0:
                # 第一个站点映射到北向终点 (NEWMARKET TERMINAL)
                sumo_stop_id = f"{northbound_route}.26"
            elif i <= 25:
                # 第2-26个站点映射到南向路线 1875927.1 到 1875927.25
                sumo_stop_id = f"{southbound_route}.{i}"
            else:
                # 第27个及以后的站点映射到北向起点 (FINCH GO BUS TERMINAL)
                sumo_stop_id = f"{northbound_route}.0"
            
            mapping["simpy_to_sumo"]["southbound"][gtfs_stop_id] = sumo_stop_id
            southbound_matches.append((gtfs_stop_id, gtfs_name, sumo_stop_id))
            print(f"✓ [{i+1:2d}] {gtfs_stop_id} '{gtfs_name}' -> {sumo_stop_id}")
        else:
            print(f"✗ [{i+1:2d}] {gtfs_stop_id} -> 在GTFS数据中未找到")
    
    # 打印匹配结果
    print(f"\n=== 匹配结果汇总 ===")
    print(f"北向成功映射: {len(northbound_matches)} 对")
    print(f"南向成功映射: {len(southbound_matches)} 对")
    print(f"北向预期站点数: {len(northbound_gtfs)}")
    print(f"南向预期站点数: {len(southbound_gtfs)}")
    
    # 验证映射完整性
    if len(northbound_matches) != len(northbound_gtfs):
        print(f"⚠️  北向映射不完整: {len(northbound_matches)}/{len(northbound_gtfs)}")
    else:
        print(f"✅ 北向映射完整")
        
    if len(southbound_matches) != len(southbound_gtfs):
        print(f"⚠️  南向映射不完整: {len(southbound_matches)}/{len(southbound_gtfs)}")
    else:
        print(f"✅ 南向映射完整")
    
    return mapping

def save_mapping(mapping: Dict, output_file: str):
    """保存映射到JSON文件"""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, indent=2, ensure_ascii=False)
        print(f"\n映射文件已保存到: {output_file}")
    except Exception as e:
        print(f"保存映射文件时出错: {e}")

def main():
    parser = argparse.ArgumentParser(description='自动生成站点映射文件')
    parser.add_argument('--route_id', default='601', help='路线ID (默认: 601)')
    parser.add_argument('--output', default='stop_mapping.json', help='输出文件名 (默认: stop_mapping.json)')
    parser.add_argument('--gtfs_dir', default='SUMO/google_transit_601', help='GTFS数据目录')
    parser.add_argument('--sumo_file', default='SUMO/stops_601.add.xml', help='SUMO站点文件')
    parser.add_argument('--trail_stops', default='trail/stops.json', help='路线站点文件')
    
    args = parser.parse_args()
    
    # 构建文件路径
    gtfs_stops_file = os.path.join(args.gtfs_dir, 'stops.txt')
    sumo_stops_file = args.sumo_file
    trail_stops_file = args.trail_stops
    
    # 检查文件是否存在
    if not os.path.exists(gtfs_stops_file):
        print(f"错误: GTFS停站文件不存在: {gtfs_stops_file}")
        return
    
    if not os.path.exists(sumo_stops_file):
        print(f"错误: SUMO停站文件不存在: {sumo_stops_file}")
        return
        
    if not os.path.exists(trail_stops_file):
        print(f"错误: 路线停站文件不存在: {trail_stops_file}")
        return
    
    print(f"正在处理路线 {args.route_id}...")
    print(f"GTFS文件: {gtfs_stops_file}")
    print(f"SUMO文件: {sumo_stops_file}")
    print(f"路线文件: {trail_stops_file}")
    
    # 加载数据
    route_stops = load_route_stops(trail_stops_file, args.route_id)
    gtfs_stops = load_gtfs_stops(gtfs_stops_file)
    sumo_stops = load_sumo_stops(sumo_stops_file)
    
    if not route_stops["northbound"] or not route_stops["southbound"]:
        print("错误: 无法加载路线站点数据")
        return
        
    if not gtfs_stops or not sumo_stops:
        print("错误: 无法加载站点数据")
        return
    
    # 创建映射
    mapping = create_stop_mapping_from_route_order(route_stops, gtfs_stops, sumo_stops, args.route_id)
    
    # 保存结果
    save_mapping(mapping, args.output)
    
    northbound_count = len(mapping['simpy_to_sumo']['northbound'])
    southbound_count = len(mapping['simpy_to_sumo']['southbound'])
    print(f"\n完成! 共生成 {northbound_count} 个北向站点映射和 {southbound_count} 个南向站点映射")

if __name__ == "__main__":
    main() 