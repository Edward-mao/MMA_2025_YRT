#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Automatic stop mapping file generation script
Generates mapping relationships between real stop IDs and SUMO stop IDs

Usage:
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
    """Calculate similarity between two strings"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def normalize_stop_name(name: str) -> str:
    """Normalize stop names, remove special characters and extra spaces"""
    # Remove platform information
    name = re.sub(r'\s+PLATFORM\s+\d+', '', name, flags=re.IGNORECASE)
    # Remove direction information
    name = re.sub(r'\s+(NORTHBOUND|SOUTHBOUND|NB|SB)', '', name, flags=re.IGNORECASE)
    # Remove extra spaces and special characters
    name = re.sub(r'\s+', ' ', name.strip())
    # Standardize separators
    name = name.replace(' / ', ' / ').replace('/', ' / ')
    return name

def load_route_stops(trail_stops_file: str, route_id: str = "601") -> Dict[str, List[str]]:
    """
    Load route stop information from trail/stops.json file
    Returns: {"northbound": [stop_ids], "southbound": [stop_ids]}
    """
    try:
        with open(trail_stops_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        routes = data.get('routes', {})
        
        # Find main northbound and southbound routes (usually the longest complete routes)
        northbound_stops = []
        southbound_stops = []
        
        # Explicitly specify main route IDs
        main_northbound_route = f"{route_id}001"  # 601001
        main_southbound_route = f"{route_id}002"  # 601002
        
        print(f"Looking for main routes: {main_northbound_route} (northbound), {main_southbound_route} (southbound)")
        
        for route_key, route_info in routes.items():
            direction = route_info.get('direction', '').upper()
            stops = route_info.get('stops', [])
            
            # Sort by sequence
            stops.sort(key=lambda x: x.get('sequence', 0))
            stop_ids = [stop['stop_id'] for stop in stops]
            
            if route_key == main_northbound_route and direction == 'NORTHBOUND':
                northbound_stops = stop_ids
                print(f"Found northbound main route {route_key}: {len(stop_ids)} stops")
            elif route_key == main_southbound_route and direction == 'SOUTHBOUND':
                southbound_stops = stop_ids
                print(f"Found southbound main route {route_key}: {len(stop_ids)} stops")
        
        # If main routes not found, use the longest route as fallback
        if not northbound_stops or not southbound_stops:
            print("Main routes not found, searching all related routes...")
            northbound_candidates = []
            southbound_candidates = []
            
            for route_key, route_info in routes.items():
                if route_key.startswith(route_id):
                    direction = route_info.get('direction', '').upper()
                    stops = route_info.get('stops', [])
                    
                    # Sort by sequence
                    stops.sort(key=lambda x: x.get('sequence', 0))
                    stop_ids = [stop['stop_id'] for stop in stops]
                    
                    if direction == 'NORTHBOUND':
                        northbound_candidates.append((route_key, stop_ids))
                    elif direction == 'SOUTHBOUND':
                        southbound_candidates.append((route_key, stop_ids))
            
            # Select the longest route
            if not northbound_stops and northbound_candidates:
                northbound_candidates.sort(key=lambda x: len(x[1]), reverse=True)
                selected_route, northbound_stops = northbound_candidates[0]
                print(f"Using fallback northbound route {selected_route}: {len(northbound_stops)} stops")
            
            if not southbound_stops and southbound_candidates:
                southbound_candidates.sort(key=lambda x: len(x[1]), reverse=True)
                selected_route, southbound_stops = southbound_candidates[0]
                print(f"Using fallback southbound route {selected_route}: {len(southbound_stops)} stops")
        
        print(f"Loaded route {route_id}:")
        print(f"  Northbound stops: {len(northbound_stops)}")
        print(f"  Southbound stops: {len(southbound_stops)}")
        
        return {
            "northbound": northbound_stops,
            "southbound": southbound_stops
        }
    except Exception as e:
        print(f"Error reading route stops file: {e}")
        return {"northbound": [], "southbound": []}

def load_gtfs_stops(gtfs_stops_file: str) -> Dict[str, str]:
    """
    Load stop information from GTFS stops.txt file
    Returns: {stop_id: stop_name}
    """
    try:
        df = pd.read_csv(gtfs_stops_file)
        stops = {}
        for _, row in df.iterrows():
            stop_id = str(row['stop_id'])
            stop_name = normalize_stop_name(str(row['stop_name']))
            stops[stop_id] = stop_name
        print(f"Loaded {len(stops)} GTFS stops")
        return stops
    except Exception as e:
        print(f"Error reading GTFS stops file: {e}")
        return {}

def load_sumo_stops(sumo_stops_file: str) -> Dict[str, Tuple[str, str]]:
    """
    Load stop information from SUMO stops.add.xml file
    Returns: {sumo_stop_id: (stop_name, route_direction)}
    """
    try:
        tree = ET.parse(sumo_stops_file)
        root = tree.getroot()
        
        stops = {}
        for bus_stop in root.findall('busStop'):
            stop_id = bus_stop.get('id')
            stop_name = normalize_stop_name(bus_stop.get('name', ''))
            
            # Extract route direction information from stop_id
            route_base = stop_id.split('.')[0] if '.' in stop_id else stop_id
            stops[stop_id] = (stop_name, route_base)
        
        print(f"Loaded {len(stops)} SUMO stops")
        return stops
    except Exception as e:
        print(f"Error reading SUMO stops file: {e}")
        return {}

def create_stop_mapping_from_route_order(route_stops: Dict[str, List[str]], 
                                        gtfs_stops: Dict[str, str], 
                                        sumo_stops: Dict[str, Tuple[str, str]], 
                                        route_id: str = "601") -> Dict:
    """
    Create stop mapping based on route stop order
    """
    mapping = {
        "simpy_to_sumo": {
            "northbound": {},
            "southbound": {}
        },
        "sumo_routes": {}
    }
    
    # Set route information
    northbound_route = "1875876"
    southbound_route = "1875927"
    
    mapping["sumo_routes"][route_id] = {
        "northbound": northbound_route,
        "southbound": southbound_route
    }
    
    print(f"\n=== Starting stop mapping ===")
    print(f"Northbound SUMO route: {northbound_route}")
    print(f"Southbound SUMO route: {southbound_route}")
    
    # Map northbound stops
    northbound_gtfs = route_stops["northbound"]
    southbound_gtfs = route_stops["southbound"]
    
    print(f"\n=== Northbound stop mapping ===")
    print(f"Total northbound stops: {len(northbound_gtfs)}")
    northbound_matches = []
    for i, gtfs_stop_id in enumerate(northbound_gtfs):
        if gtfs_stop_id in gtfs_stops:
            gtfs_name = gtfs_stops[gtfs_stop_id]
            
            # Northbound: map directly in sequence to 1875876.0 to 1875876.26
            sumo_stop_id = f"{northbound_route}.{i}"
            mapping["simpy_to_sumo"]["northbound"][gtfs_stop_id] = sumo_stop_id
            northbound_matches.append((gtfs_stop_id, gtfs_name, sumo_stop_id))
            print(f"[{i+1:2d}] {gtfs_stop_id} '{gtfs_name}' -> {sumo_stop_id}")
        else:
            print(f"[{i+1:2d}] {gtfs_stop_id} -> Not found in GTFS data")
    
    print(f"\n=== Southbound stop mapping ===")
    print(f"Total southbound stops: {len(southbound_gtfs)}")
    southbound_matches = []
    for i, gtfs_stop_id in enumerate(southbound_gtfs):
        if gtfs_stop_id in gtfs_stops:
            gtfs_name = gtfs_stops[gtfs_stop_id]
            
            # Southbound stop mapping logic:
            # Based on SUMO route file, southbound route stop order is:
            # 1875876.26 (start point, corresponds to northbound end)
            # 1875927.1 to 1875927.25 (intermediate stops)
            # 1875876.0 (end point, corresponds to northbound start)
            
            if i == 0:
                # First stop maps to northbound end (NEWMARKET TERMINAL)
                sumo_stop_id = f"{northbound_route}.26"
            elif i <= 25:
                # 2nd-26th stops map to southbound route 1875927.1 to 1875927.25
                sumo_stop_id = f"{southbound_route}.{i}"
            else:
                # 27th and later stops map to northbound start (FINCH GO BUS TERMINAL)
                sumo_stop_id = f"{northbound_route}.0"
            
            mapping["simpy_to_sumo"]["southbound"][gtfs_stop_id] = sumo_stop_id
            southbound_matches.append((gtfs_stop_id, gtfs_name, sumo_stop_id))
            print(f"[{i+1:2d}] {gtfs_stop_id} '{gtfs_name}' -> {sumo_stop_id}")
        else:
            print(f"[{i+1:2d}] {gtfs_stop_id} -> Not found in GTFS data")
    
    # Print matching results
    print(f"\n=== Mapping results summary ===")
    print(f"Northbound successful mappings: {len(northbound_matches)} pairs")
    print(f"Southbound successful mappings: {len(southbound_matches)} pairs")
    print(f"Expected northbound stops: {len(northbound_gtfs)}")
    print(f"Expected southbound stops: {len(southbound_gtfs)}")
    
    # Verify mapping completeness
    if len(northbound_matches) != len(northbound_gtfs):
        print(f"Northbound mapping incomplete: {len(northbound_matches)}/{len(northbound_gtfs)}")
    else:
        print(f"Northbound mapping complete")
        
    if len(southbound_matches) != len(southbound_gtfs):
        print(f"Southbound mapping incomplete: {len(southbound_matches)}/{len(southbound_gtfs)}")
    else:
        print(f"Southbound mapping complete")
    
    return mapping

def save_mapping(mapping: Dict, output_file: str):
    """Save mapping to JSON file"""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, indent=2, ensure_ascii=False)
        print(f"\nMapping file saved to: {output_file}")
    except Exception as e:
        print(f"Error saving mapping file: {e}")

def main():
    parser = argparse.ArgumentParser(description='Automatically generate stop mapping file')
    parser.add_argument('--route_id', default='601', help='Route ID (default: 601)')
    parser.add_argument('--output', default='stop_mapping.json', help='Output filename (default: stop_mapping.json)')
    parser.add_argument('--gtfs_dir', default='SUMO/google_transit_601', help='GTFS data directory')
    parser.add_argument('--sumo_file', default='SUMO/stops_601.add.xml', help='SUMO stops file')
    parser.add_argument('--trail_stops', default='trail/stops.json', help='Route stops file')
    
    args = parser.parse_args()
    
    # Build file paths
    gtfs_stops_file = os.path.join(args.gtfs_dir, 'stops.txt')
    sumo_stops_file = args.sumo_file
    trail_stops_file = args.trail_stops
    
    # Check if files exist
    if not os.path.exists(gtfs_stops_file):
        print(f"Error: GTFS stops file does not exist: {gtfs_stops_file}")
        return
    
    if not os.path.exists(sumo_stops_file):
        print(f"Error: SUMO stops file does not exist: {sumo_stops_file}")
        return
        
    if not os.path.exists(trail_stops_file):
        print(f"Error: Route stops file does not exist: {trail_stops_file}")
        return
    
    print(f"Processing route {args.route_id}...")
    print(f"GTFS file: {gtfs_stops_file}")
    print(f"SUMO file: {sumo_stops_file}")
    print(f"Route file: {trail_stops_file}")
    
    # Load data
    route_stops = load_route_stops(trail_stops_file, args.route_id)
    gtfs_stops = load_gtfs_stops(gtfs_stops_file)
    sumo_stops = load_sumo_stops(sumo_stops_file)
    
    if not route_stops["northbound"] or not route_stops["southbound"]:
        print("Error: Unable to load route stops data")
        return
        
    if not gtfs_stops or not sumo_stops:
        print("Error: Unable to load stops data")
        return
    
    # Create mapping
    mapping = create_stop_mapping_from_route_order(route_stops, gtfs_stops, sumo_stops, args.route_id)
    
    # Save results
    save_mapping(mapping, args.output)
    
    northbound_count = len(mapping['simpy_to_sumo']['northbound'])
    southbound_count = len(mapping['simpy_to_sumo']['southbound'])
    print(f"\nComplete! Generated {northbound_count} northbound stop mappings and {southbound_count} southbound stop mappings")

if __name__ == "__main__":
    main() 