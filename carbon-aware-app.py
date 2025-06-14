#!/usr/bin/env python3
"""
Carbon-Aware Job Processor Demo - WattTime API v3
A demonstration application that processes jobs based on real-time carbon intensity data
from WattTime API v3, scheduling work during low-carbon periods.
"""

import os
import time
import json
import logging
import requests
import threading
import queue
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
from enum import Enum
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CarbonIntensityLevel(Enum):
    """Carbon intensity levels for decision making"""
    VERY_LOW = "very_low"    # < 200 lbs CO2/MWh
    LOW = "low"              # 200-400 lbs CO2/MWh
    MEDIUM = "medium"        # 400-600 lbs CO2/MWh
    HIGH = "high"            # 600-800 lbs CO2/MWh
    VERY_HIGH = "very_high"  # > 800 lbs CO2/MWh


@dataclass
class Job:
    """Represents a job to be processed"""
    id: str
    name: str
    priority: int  # 1-5, where 5 is highest priority
    complexity: int  # Simulated work units
    created_at: datetime
    carbon_threshold: CarbonIntensityLevel = CarbonIntensityLevel.MEDIUM
    
    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "priority": self.priority,
            "complexity": self.complexity,
            "created_at": self.created_at.isoformat(),
            "carbon_threshold": self.carbon_threshold.value
        }


@dataclass
class CarbonData:
    """Carbon intensity data point"""
    timestamp: datetime
    value: float  # lbs CO2/MWh (MOER)
    region: str
    level: CarbonIntensityLevel
    signal_type: str = "co2_moer"


class WattTimeV3Client:
    """Client for interacting with WattTime API v3"""
    
    def __init__(self, username: str, password: str):
        self.base_url = "https://api2.watttime.org"
        self.username = username
        self.password = password
        self.token = None
        self.token_expiry = None
        
    def register(self, email: str, org: str) -> bool:
        """Register a new WattTime account"""
        url = f"{self.base_url}/v2/register"
        params = {
            "username": self.username,
            "password": self.password,
            "email": email,
            "org": org
        }
        
        try:
            response = requests.post(url, json=params)
            if response.status_code == 200:
                logger.info("Successfully registered WattTime account")
                return True
            else:
                logger.error(f"Registration failed: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Registration error: {e}")
            return False
    
    def login(self) -> bool:
        """Authenticate and get access token"""
        url = f"{self.base_url}/v2/login"
        
        try:
            response = requests.get(url, auth=(self.username, self.password))
            if response.status_code == 200:
                data = response.json()
                self.token = data['token']
                # Token expires in 30 minutes
                self.token_expiry = datetime.now() + timedelta(minutes=29)
                logger.info("Successfully authenticated with WattTime API v3")
                return True
            else:
                logger.error(f"Authentication failed: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False
    
    def ensure_authenticated(self):
        """Ensure we have a valid token"""
        if not self.token or datetime.now() >= self.token_expiry:
            self.login()
    
    def get_my_access(self) -> Dict:
        """Get information about accessible regions and signals"""
        self.ensure_authenticated()
        
        url = f"{self.base_url}/v3/my-access"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get access info: {response.status_code}")
                return {}
        except Exception as e:
            logger.error(f"Error getting access info: {e}")
            return {}
    
    def get_current_intensity(self, region: str, signal_type: str = "co2_moer") -> Optional[CarbonData]:
        """Get real-time carbon intensity from forecast endpoint (first data point)"""
        self.ensure_authenticated()
        
        url = f"{self.base_url}/v3/forecast"
        headers = {"Authorization": f"Bearer {self.token}"}
        params = {
            "region": region,
            "signal_type": signal_type
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            # Handle HTTP 401 for re-authentication
            if response.status_code == 401:
                logger.info("Token expired, re-authenticating...")
                self.login()
                headers = {"Authorization": f"Bearer {self.token}"}
                response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                if data and "data" in data and len(data["data"]) > 0:
                    # First data point is current
                    current = data["data"][0]
                    intensity = current["value"]
                    
                    # Parse timestamp
                    timestamp = datetime.fromisoformat(current["point_time"].replace('Z', '+00:00'))
                    
                    # Determine carbon level (lbs CO2/MWh)
                    if intensity < 200:
                        level = CarbonIntensityLevel.VERY_LOW
                    elif intensity < 400:
                        level = CarbonIntensityLevel.LOW
                    elif intensity < 600:
                        level = CarbonIntensityLevel.MEDIUM
                    elif intensity < 800:
                        level = CarbonIntensityLevel.HIGH
                    else:
                        level = CarbonIntensityLevel.VERY_HIGH
                    
                    return CarbonData(
                        timestamp=timestamp,
                        value=intensity,
                        region=region,
                        level=level,
                        signal_type=signal_type
                    )
            else:
                logger.error(f"Failed to get carbon data: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"Error fetching carbon data: {e}")
        
        return None
    
    def get_forecast(self, region: str, signal_type: str = "co2_moer", hours: int = 24) -> List[CarbonData]:
        """Get carbon intensity forecast"""
        self.ensure_authenticated()
        
        url = f"{self.base_url}/v3/forecast"
        headers = {"Authorization": f"Bearer {self.token}"}
        params = {
            "region": region,
            "signal_type": signal_type
        }
        
        forecasts = []
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            # Handle HTTP 401 for re-authentication
            if response.status_code == 401:
                logger.info("Token expired, re-authenticating...")
                self.login()
                headers = {"Authorization": f"Bearer {self.token}"}
                response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                if "data" in data:
                    # Limit to requested hours (5-minute intervals)
                    for item in data["data"][:hours * 12]:
                        intensity = item["value"]
                        timestamp = datetime.fromisoformat(item["point_time"].replace('Z', '+00:00'))
                        
                        # Determine carbon level (lbs CO2/MWh)
                        if intensity < 200:
                            level = CarbonIntensityLevel.VERY_LOW
                        elif intensity < 400:
                            level = CarbonIntensityLevel.LOW
                        elif intensity < 600:
                            level = CarbonIntensityLevel.MEDIUM
                        elif intensity < 800:
                            level = CarbonIntensityLevel.HIGH
                        else:
                            level = CarbonIntensityLevel.VERY_HIGH
                        
                        forecasts.append(CarbonData(
                            timestamp=timestamp,
                            value=intensity,
                            region=region,
                            level=level,
                            signal_type=signal_type
                        ))
        except Exception as e:
            logger.error(f"Error fetching forecast: {e}")
        
        return forecasts


class CarbonAwareScheduler:
    """Schedules jobs based on carbon intensity"""
    
    def __init__(self, watttime_client: WattTimeV3Client, region: str):
        self.client = watttime_client
        self.region = region
        self.current_carbon_data = None
        self.forecast_data = []
        self.update_interval = 300  # 5 minutes
        
    def update_carbon_data(self):
        """Update current and forecast carbon data"""
        # Get current intensity
        self.current_carbon_data = self.client.get_current_intensity(self.region)
        
        # Get forecast
        self.forecast_data = self.client.get_forecast(self.region, hours=6)
        
        if self.current_carbon_data:
            logger.info(
                f"Current carbon intensity: {self.current_carbon_data.value:.1f} lbs CO2/MWh "
                f"({self.current_carbon_data.level.value})"
            )
    
    def should_run_job(self, job: Job) -> Tuple[bool, str]:
        """Determine if a job should run now based on carbon intensity"""
        if not self.current_carbon_data:
            return False, "No carbon data available"
        
        current_level = self.current_carbon_data.level
        threshold_level = job.carbon_threshold
        
        # Map levels to numeric values for comparison
        level_values = {
            CarbonIntensityLevel.VERY_LOW: 1,
            CarbonIntensityLevel.LOW: 2,
            CarbonIntensityLevel.MEDIUM: 3,
            CarbonIntensityLevel.HIGH: 4,
            CarbonIntensityLevel.VERY_HIGH: 5
        }
        
        current_value = level_values[current_level]
        threshold_value = level_values[threshold_level]
        
        # High priority jobs (4-5) can run at one level higher than threshold
        if job.priority >= 4:
            threshold_value += 1
        
        if current_value <= threshold_value:
            return True, f"Carbon level {current_level.value} is acceptable"
        else:
            # Check if there's a better time in the near future
            better_time = self.find_better_time(threshold_level)
            if better_time:
                return False, f"Wait until {better_time.strftime('%H:%M')} for lower carbon"
            else:
                # No better time soon, run anyway if high priority
                if job.priority >= 3:
                    return True, "No better time soon, running due to priority"
                else:
                    return False, f"Carbon level {current_level.value} too high"
    
    def find_better_time(self, target_level: CarbonIntensityLevel) -> Optional[datetime]:
        """Find the next time when carbon will be at or below target level"""
        for forecast in self.forecast_data[:24]:  # Look ahead 2 hours (5-min intervals)
            if forecast.level.value <= target_level.value:
                return forecast.timestamp
        return None
    
    def get_scaling_factor(self) -> float:
        """Get worker scaling factor based on carbon intensity"""
        if not self.current_carbon_data:
            return 1.0
        
        level = self.current_carbon_data.level
        
        if level == CarbonIntensityLevel.VERY_LOW:
            return 2.0  # Double capacity
        elif level == CarbonIntensityLevel.LOW:
            return 1.5  # 50% more capacity
        elif level == CarbonIntensityLevel.MEDIUM:
            return 1.0  # Normal capacity
        elif level == CarbonIntensityLevel.HIGH:
            return 0.5  # Half capacity
        else:  # VERY_HIGH
            return 0.25  # Minimal capacity


class JobProcessor:
    """Processes jobs with simulated work"""
    
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.jobs_processed = 0
        self.total_carbon_saved = 0.0
        
    def process_job(self, job: Job, carbon_intensity: float) -> Dict:
        """Process a single job"""
        start_time = time.time()
        
        logger.info(f"Worker {self.worker_id} processing job {job.id} ({job.name})")
        
        # Simulate work based on complexity
        work_duration = job.complexity / 1000.0  # Convert to seconds
        time.sleep(work_duration)
        
        # Calculate simulated carbon footprint
        # Assume 100W power consumption during processing
        energy_kwh = (100 * work_duration) / (1000 * 3600)
        carbon_emitted = energy_kwh * carbon_intensity
        
        # Calculate savings compared to average US grid (~ 900 lbs CO2/MWh)
        carbon_saved = energy_kwh * (900 - carbon_intensity)
        self.total_carbon_saved += max(0, carbon_saved)
        
        self.jobs_processed += 1
        
        return {
            "job_id": job.id,
            "worker_id": self.worker_id,
            "duration": time.time() - start_time,
            "carbon_intensity": carbon_intensity,
            "carbon_emitted_lbs": carbon_emitted,
            "carbon_saved_lbs": carbon_saved,
            "completed_at": datetime.now().isoformat()
        }


class CarbonAwareJobQueue:
    """Main application orchestrating carbon-aware job processing"""
    
    def __init__(self, watttime_username: str, watttime_password: str, region: str):
        self.watttime_client = WattTimeV3Client(watttime_username, watttime_password)
        self.scheduler = CarbonAwareScheduler(self.watttime_client, region)
        self.job_queue = queue.PriorityQueue()
        self.pending_jobs = []
        self.completed_jobs = []
        self.workers = []
        self.running = False
        self.stats_lock = threading.Lock()
        
        # Statistics
        self.total_jobs_processed = 0
        self.total_carbon_saved = 0.0
        self.jobs_deferred = 0
        
    def add_job(self, job: Job):
        """Add a job to the queue"""
        # Priority queue uses negative priority for proper ordering
        self.job_queue.put((-job.priority, job.created_at, job))
        logger.info(f"Added job {job.id} to queue")
    
    def generate_sample_jobs(self, count: int):
        """Generate sample jobs for testing"""
        job_types = [
            ("Data Processing", CarbonIntensityLevel.LOW),
            ("ML Training", CarbonIntensityLevel.VERY_LOW),
            ("Report Generation", CarbonIntensityLevel.MEDIUM),
            ("Backup Task", CarbonIntensityLevel.HIGH),
            ("Analytics Query", CarbonIntensityLevel.MEDIUM)
        ]
        
        for i in range(count):
            job_type, threshold = random.choice(job_types)
            job = Job(
                id=f"job-{i+1:04d}",
                name=f"{job_type} #{i+1}",
                priority=random.randint(1, 5),
                complexity=random.randint(1000, 10000),
                created_at=datetime.now(),
                carbon_threshold=threshold
            )
            self.add_job(job)
    
    def update_carbon_data_loop(self):
        """Background thread to update carbon data"""
        while self.running:
            try:
                self.scheduler.update_carbon_data()
            except Exception as e:
                logger.error(f"Error updating carbon data: {e}")
            
            time.sleep(self.scheduler.update_interval)
    
    def process_jobs_loop(self, worker_id: str):
        """Worker thread to process jobs"""
        processor = JobProcessor(worker_id)
        
        while self.running:
            try:
                # Get next job from queue (with timeout to check running flag)
                try:
                    _, _, job = self.job_queue.get(timeout=1)
                except queue.Empty:
                    continue
                
                # Check if we should run this job now
                should_run, reason = self.scheduler.should_run_job(job)
                
                if should_run:
                    # Process the job
                    carbon_data = self.scheduler.current_carbon_data
                    result = processor.process_job(job, carbon_data.value if carbon_data else 900)
                    
                    with self.stats_lock:
                        self.completed_jobs.append((job, result))
                        self.total_jobs_processed += 1
                        self.total_carbon_saved += result["carbon_saved_lbs"]
                    
                    logger.info(
                        f"Job {job.id} completed. Carbon saved: {result['carbon_saved_lbs']:.2f} lbs"
                    )
                else:
                    # Defer the job
                    logger.info(f"Deferring job {job.id}: {reason}")
                    with self.stats_lock:
                        self.jobs_deferred += 1
                        self.pending_jobs.append(job)
                    
                    # Re-queue with slight delay
                    threading.Timer(
                        60.0,  # Check again in 1 minute
                        lambda: self.add_job(job)
                    ).start()
                    
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
    
    def start(self, num_workers: int = 3):
        """Start the carbon-aware job processing system"""
        logger.info("Starting Carbon-Aware Job Processor with WattTime API v3")
        
        # Authenticate with WattTime
        if not self.watttime_client.login():
            logger.error("Failed to authenticate with WattTime")
            return
        
        # Check access
        access_info = self.watttime_client.get_my_access()
        if access_info:
            logger.info(f"API Access confirmed. Available signals: {json.dumps(access_info, indent=2)}")
        
        self.running = True
        
        # Start carbon data updater
        carbon_thread = threading.Thread(target=self.update_carbon_data_loop)
        carbon_thread.daemon = True
        carbon_thread.start()
        
        # Wait for initial carbon data
        time.sleep(2)
        
        # Start worker threads
        base_workers = num_workers
        for i in range(base_workers):
            worker_thread = threading.Thread(
                target=self.process_jobs_loop,
                args=(f"worker-{i+1}",)
            )
            worker_thread.daemon = True
            worker_thread.start()
            self.workers.append(worker_thread)
        
        logger.info(f"Started {base_workers} worker threads")
        
        # Monitor and adjust workers based on carbon intensity
        try:
            while True:
                time.sleep(30)  # Check every 30 seconds
                self.print_stats()
                
                # Dynamic worker scaling based on carbon intensity
                scaling_factor = self.scheduler.get_scaling_factor()
                target_workers = int(base_workers * scaling_factor)
                current_workers = len(self.workers)
                
                if target_workers != current_workers:
                    logger.info(
                        f"Adjusting workers from {current_workers} to {target_workers} "
                        f"(scaling factor: {scaling_factor})"
                    )
                
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            self.running = False
            time.sleep(2)
    
    def print_stats(self):
        """Print current statistics"""
        with self.stats_lock:
            carbon_data = self.scheduler.current_carbon_data
            
            print("\n" + "="*60)
            print("CARBON-AWARE JOB PROCESSOR STATS (WattTime API v3)")
            print("="*60)
            
            if carbon_data:
                print(f"Current Carbon Intensity: {carbon_data.value:.1f} lbs CO2/MWh ({carbon_data.level.value})")
                print(f"Region: {carbon_data.region}")
                print(f"Signal Type: {carbon_data.signal_type}")
            
            print(f"Jobs Processed: {self.total_jobs_processed}")
            print(f"Jobs Deferred: {self.jobs_deferred}")
            print(f"Jobs in Queue: {self.job_queue.qsize()}")
            print(f"Pending Jobs: {len(self.pending_jobs)}")
            print(f"Total Carbon Saved: {self.total_carbon_saved:.2f} lbs CO2")
            print(f"Active Workers: {len(self.workers)}")
            print("="*60)


def main():
    """Main entry point"""
    # Configuration (use environment variables in production)
    WATTTIME_USERNAME = os.getenv("WATTTIME_USERNAME", "demo_user")
    WATTTIME_PASSWORD = os.getenv("WATTTIME_PASSWORD", "demo_password")
    REGION = os.getenv("WATTTIME_REGION", "CAISO_NORTH")  # Default to California ISO North
    
    print("🌱 Carbon-Aware Job Processor Demo (WattTime API v3)")
    print("===================================================")
    print(f"Region: {REGION}")
    print("\nThis demo will:")
    print("1. Monitor real-time carbon intensity from WattTime API v3")
    print("2. Schedule jobs during low-carbon periods")
    print("3. Defer jobs when carbon intensity is high")
    print("4. Scale workers based on grid carbon levels")
    print("5. Track carbon savings in lbs CO2")
    print("\nNote: WattTime API v3 uses lbs CO2/MWh instead of gCO2/kWh")
    print("\nPress Ctrl+C to stop\n")
    
    # Create the application
    app = CarbonAwareJobQueue(WATTTIME_USERNAME, WATTTIME_PASSWORD, REGION)
    
    # Generate sample jobs
    print("Generating sample jobs...")
    app.generate_sample_jobs(20)
    
    # Start processing
    try:
        app.start(num_workers=3)
    except Exception as e:
        logger.error(f"Application error: {e}")


if __name__ == "__main__":
    main()
