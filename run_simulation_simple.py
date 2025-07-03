"""
Simplified simulation run script (without ETL)
"""
import sys
import os

# Ensure the simulation package can be found
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from simulation.simulation_runner import SimulationRunner
from sim_hook.enhanced_integration import integrate_data_collection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Run a single simulation without starting ETL"""
    logger.info("=" * 80)
    logger.info("Starting simplified simulation (without ETL)")
    logger.info("=" * 80)
    
    # 1. Initialize SimulationRunner
    simulation = SimulationRunner(scenario_name="601")
    logger.info(f"Simulation initialized successfully - Date: Month {simulation.selected_month}, Day {simulation.selected_day}")
    
    # 2. Integrate data collection hook
    data_hook = integrate_data_collection(simulation, output_dir="./simulation_data")
    logger.info("Data collection hook integrated")
    
    # 3. Run simulation
    try:
        logger.info("Starting simulation...")
        simulation.run()
        logger.info("Simulation completed successfully!")
    except Exception as e:
        logger.error(f"Simulation run failed: {e}", exc_info=True)
    finally:
        # Stop data collection hook
        try:
            data_hook.stop()
            logger.info("Data collection hook stopped")
        except Exception as e:
            logger.warning(f"Failed to stop data collection hook: {e}")
    
    # 4. Check generated data files
    data_dir = "./simulation_data"
    if os.path.exists(data_dir):
        files = os.listdir(data_dir)
        logger.info(f"\nGenerated {len(files)} data files")
        if files:
            for i, f in enumerate(files[:10]):  # Show only first 10
                size = os.path.getsize(os.path.join(data_dir, f))
                logger.info(f"  {i+1}. {f} ({size:,} bytes)")
            if len(files) > 10:
                logger.info(f"  ... and {len(files) - 10} more files")

if __name__ == "__main__":
    main() 