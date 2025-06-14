# carbon-aware-app

Carbon-Aware Job Processor demo application that intelligently schedules and processes jobs based on real-time carbon intensity data from the WattTime API. 

## Key Features:
1. Carbon Intensity Monitoring
- Connects to WattTime API to fetch real-time carbon intensity data
- Classifies carbon levels into 5 categories (Very Low to Very High)
- Updates data every 5 minutes
- Fetches forecasts to predict better execution times

2. Intelligent Job Scheduling
- Jobs have carbon threshold requirements (e.g., ML training only runs during very low carbon periods)
- High-priority jobs get more flexibility in carbon thresholds
- Defers jobs when carbon intensity is too high
- Automatically re-queues deferred jobs for later processing

3. Dynamic Worker Scaling

- Scales worker threads based on carbon intensity:
  - Very Low carbon: 2x capacity
  - Low carbon: 1.5x capacity
  - Medium carbon: Normal capacity
  - High carbon: 0.5x capacity
  - Very High carbon: 0.25x capacity

4. Carbon Savings Tracking
- Calculates carbon emissions for each job
- Compares against US grid average (450 gCO2/kWh)
- Tracks total carbon saved by running during cleaner periods

## How to Use:
1. Setup WattTime Credentials
- First, register for a free WattTime account at https://docs.watttime.org/
then set environment variables:
```
export WATTTIME_USERNAME="your_username"
export WATTTIME_PASSWORD="your_password"
export BA_CODE="CAISO_NORTH"  # Your region code
```

2. Install Dependencies
```
pip install requests
```

3. Run the Demo
```py
python carbon_aware_demo.py
```

```
Sample Output:
The application will show real-time stats like:
============================================================
CARBON-AWARE JOB PROCESSOR STATS
============================================================
Current Carbon Intensity: 245.3 gCO2/kWh (low)
Jobs Processed: 15
Jobs Deferred: 5
Jobs in Queue: 3
Pending Jobs: 2
Total Carbon Saved: 125.45g CO2
Active Workers: 4
============================================================
```

## Architecture Highlights:

- WattTimeClient: Handles API authentication and data fetching
- CarbonAwareScheduler: Makes decisions about job execution timing
- JobProcessor: Simulates job processing with carbon calculations
- CarbonAwareJobQueue: Main orchestrator managing the entire system

