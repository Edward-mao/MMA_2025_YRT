import os
import subprocess
import sys
import argparse

def generate_traffic(factor, begin_time, end_time, seed, mode):
    """
    Generates random traffic for a SUMO simulation.

    :param factor: A float to scale the traffic volume.
    :param begin_time: The beginning time of the simulation in seconds.
    :param end_time: The end time of the simulation in seconds.
    :param seed: The random seed for reproducibility.
    :param mode: The traffic generation mode ('mixed', 'peak', 'offpeak').
    """
    try:
        sumo_home = os.environ.get("SUMO_HOME")
        if not sumo_home:
            sys.exit("Error: Please declare the environment variable 'SUMO_HOME'")
        
        tools = os.path.join(sumo_home, 'tools')
        sys.path.append(tools)
        
        from sumolib import checkBinary  # noqa
    except ImportError:
        sys.exit("Error: Please declare the environment variable 'SUMO_HOME'")

    net_file = "map.net.xml"
    if not os.path.exists(net_file):
        sys.exit(f"Error: Network file '{net_file}' not found.")

    # Base period for vehicle generation (in seconds). Smaller is more frequent.
    # These values will be divided by the factor.
    off_peak_base_period = 2.0
    peak_base_period = 0.5

    trip_files = []
    
    if mode == "mixed":
        print("Generating mixed traffic with peak and off-peak periods...")
        # Define peak and off-peak hours for mixed mode, covering 24 hours (86400s)
        time_periods = [
            (0, 21600, False),      # 0h-6h: Off-peak
            (21600, 32400, True),   # 6h-9h: Morning Peak
            (32400, 57600, False),  # 9h-16h: Midday Off-peak
            (57600, 68400, True),   # 16h-19h: Evening Peak
            (68400, 86400, False)  # 19h-24h: Night Off-peak
        ]

        for i, (start, end, is_peak) in enumerate(time_periods):
            if start >= end_time:
                continue
            
            effective_end = min(end, end_time)
            prefix = f"m{i}" # Add a unique prefix for mixed mode, e.g., m0, m1...

            if is_peak:
                period = peak_base_period / factor
                trip_file = f"trips_peak_{i}.trips.xml"
            else:
                period = off_peak_base_period / factor
                trip_file = f"trips_offpeak_{i}.trips.xml"
            
            command = [
                sys.executable, os.path.join(tools, 'randomTrips.py'),
                "-n", net_file,
                "-o", trip_file,
                "-b", str(start),
                "-e", str(effective_end),
                "-p", str(period),
                "--vtype", "car_passenger",
                "--prefix", prefix,
                "--validate",
                "--seed", str(seed)
            ]
            
            subprocess.run(command, check=True)
            trip_files.append(trip_file)
            print(f"  - Generated {trip_file} for period {start}s to {effective_end}s")
            
    else: # mode is 'peak' or 'offpeak'
        print(f"Generating uniform '{mode}' traffic from {begin_time}s to {end_time}s...")
        if mode == 'peak':
            period = peak_base_period / factor
            trip_file = "trips_uniform_peak.trips.xml"
            prefix = "p" # Add prefix for peak mode
        else: # offpeak
            period = off_peak_base_period / factor
            trip_file = "trips_uniform_offpeak.trips.xml"
            prefix = "op" # Add prefix for offpeak mode
        
        command = [
            sys.executable, os.path.join(tools, 'randomTrips.py'),
            "-n", net_file,
            "-o", trip_file,
            "-b", str(begin_time),
            "-e", str(end_time),
            "-p", str(period),
            "--vtype", "car_passenger",
            "--prefix", prefix,
            "--validate",
            "--seed", str(seed)
        ]
        
        subprocess.run(command, check=True)
        trip_files.append(trip_file)
        print(f"  - Generated {trip_file}")

    # Run duarouter to convert trips to routes
    output_route_file = "random_routes.rou.xml"
    duarouter_cmd = [
        checkBinary('duarouter'),
        "-n", net_file,
        "--trip-files", ",".join(trip_files),
        "-o", output_route_file,
        "--ignore-errors",
        "--seed", str(seed)
    ]
    
    print("\nRunning duarouter to generate route file...")
    subprocess.run(duarouter_cmd, check=True)

    print(f"\nSuccessfully generated random traffic file: {output_route_file}")
    
    # Clean up intermediate trip files
    print("Cleaning up temporary trip files...")
    for f in trip_files:
        os.remove(f)
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate random traffic for SUMO simulation.")
    parser.add_argument("--factor", type=float, default=1.0,
                        help="A factor to control traffic density. >1 means more traffic.")
    parser.add_argument("--begin", type=int, default=0,
                        help="Begin time for traffic generation in seconds.")
    parser.add_argument("--end", type=int, default=86400,
                        help="End time for traffic generation in seconds (default: 86400 for 24 hours).")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility.")
    parser.add_argument("--mode", type=str, default="mixed", choices=["mixed", "peak", "offpeak"],
                        help="Traffic generation mode: 'mixed' for combined periods, 'peak' for uniform peak traffic, 'offpeak' for uniform off-peak traffic.")
    
    args = parser.parse_args()

    generate_traffic(args.factor, args.begin, args.end, args.seed, args.mode) 