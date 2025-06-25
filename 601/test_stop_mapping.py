#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试新的双向站点映射功能
"""
import sys
import os
import json

# 添加simulation模块到路径
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from simulation.data_loader import load_stop_mapping
from simulation.bus import Bus
from simulation.bus_stop import BusStop
from simulation.event_handler import EventHandler
import simpy


def test_new_mapping_format():
    """测试新的双向映射格式"""
    print("=== 测试新的双向站点映射格式 ===\n")
    
    # 1. 测试加载映射文件
    print("1. 测试加载映射文件...")
    mapping = load_stop_mapping('601')
    
    if isinstance(mapping, dict) and 'simpy_to_sumo' in mapping:
        print("✓ 成功加载新格式的映射文件")
        print(f"  - 北向站点数: {len(mapping['simpy_to_sumo'].get('northbound', {}))}")
        print(f"  - 南向站点数: {len(mapping['simpy_to_sumo'].get('southbound', {}))}")
        
        # 显示一些示例映射
        if mapping['simpy_to_sumo']['northbound']:
            print("\n  北向映射示例:")
            for i, (simpy_id, sumo_id) in enumerate(list(mapping['simpy_to_sumo']['northbound'].items())[:3]):
                print(f"    {simpy_id} -> {sumo_id}")
                
        if mapping['simpy_to_sumo']['southbound']:
            print("\n  南向映射示例:")
            for i, (simpy_id, sumo_id) in enumerate(list(mapping['simpy_to_sumo']['southbound'].items())[:3]):
                print(f"    {simpy_id} -> {sumo_id}")
    else:
        print("✗ 映射文件格式不正确")
        return False
        
    # 2. 测试Bus类是否能正确处理新格式
    print("\n2. 测试Bus类处理新格式...")
    
    # 创建模拟环境
    env = simpy.Environment()
    event_handler = EventHandler(env)
    
    # 创建一些模拟的bus stops
    bus_stops = {
        '9769': BusStop(env, '9769', [], {}, {}, {}, 1, 1, simulation_start_time=0),
        '9770': BusStop(env, '9770', [], {}, {}, {}, 1, 1, simulation_start_time=0),
    }
    
    # 测试北向Bus
    print("\n  测试北向Bus:")
    bus_nb = Bus(
        env=env,
        bus_id='test_bus_601001',
        route_id='601001',  # 北向路线
        route_stops=['9769', '9770'],
        bus_stops=bus_stops,
        event_handler=event_handler,
        start_time=0,
        stop_mapping=mapping
    )
    
    print(f"    方向: {bus_nb.direction}")
    print(f"    映射数量: {len(bus_nb.stop_mapping)}")
    if '9769' in bus_nb.stop_mapping:
        print(f"    9769 -> {bus_nb.stop_mapping['9769']}")
    
    # 测试南向Bus
    print("\n  测试南向Bus:")
    bus_sb = Bus(
        env=env,
        bus_id='test_bus_601002',
        route_id='601002',  # 南向路线
        route_stops=['9809', '9808'],
        bus_stops=bus_stops,
        event_handler=event_handler,
        start_time=0,
        stop_mapping=mapping
    )
    
    print(f"    方向: {bus_sb.direction}")
    print(f"    映射数量: {len(bus_sb.stop_mapping)}")
    if '9809' in bus_sb.stop_mapping:
        print(f"    9809 -> {bus_sb.stop_mapping['9809']}")
        
    return True


def test_backward_compatibility():
    """测试向后兼容性"""
    print("\n\n=== 测试向后兼容性 ===\n")
    
    # 创建一个旧格式的映射
    old_format_mapping = {
        '9769': '1875876.0',
        '9770': '1875876.1',
        '9771': '1875876.2'
    }
    
    # 创建模拟环境
    env = simpy.Environment()
    event_handler = EventHandler(env)
    bus_stops = {}
    
    # 测试Bus类是否能处理旧格式
    print("测试Bus类处理旧格式...")
    bus = Bus(
        env=env,
        bus_id='test_bus_old',
        route_id='601',
        route_stops=['9769', '9770'],
        bus_stops=bus_stops,
        event_handler=event_handler,
        start_time=0,
        stop_mapping=old_format_mapping
    )
    
    print(f"  方向: {bus.direction}")
    print(f"  映射数量: {len(bus.stop_mapping)}")
    if '9769' in bus.stop_mapping:
        print(f"  9769 -> {bus.stop_mapping['9769']}")
        
    return True


if __name__ == "__main__":
    print("开始测试新的双向站点映射功能...\n")
    
    # 确保新的映射文件存在
    mapping_file = 'stop_mapping.json'  # 使用相对路径
    if not os.path.exists(mapping_file):
        print(f"错误: 映射文件 {mapping_file} 不存在")
        print("请先运行 python generate_stop_mapping.py 生成映射文件")
        sys.exit(1)
        
    # 运行测试
    test1_result = test_new_mapping_format()
    test2_result = test_backward_compatibility()
    
    print("\n" + "="*50)
    print("测试结果:")
    print(f"  新格式测试: {'通过' if test1_result else '失败'}")
    print(f"  向后兼容性测试: {'通过' if test2_result else '失败'}")
    print("="*50) 