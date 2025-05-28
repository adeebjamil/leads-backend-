import asyncio
import uuid
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from . import scrapers

@dataclass
class ScraperTask:
    task_id: str
    scraper_name: str
    status: str  # 'pending', 'running', 'completed', 'failed'
    progress: int
    message: str
    created_at: datetime
    completed_at: Optional[datetime] = None
    filename: Optional[str] = None
    total_records: int = 0

@dataclass 
class ScrapeStats:
    scraper_name: str
    display_name: str
    total_records: int
    runs: int
    last_run: Optional[datetime]

class ScraperManager:
    def __init__(self):
        self.tasks: Dict[str, ScraperTask] = {}
        self.daily_stats: List[ScrapeStats] = []
        self.scrapers = {
            'googlemaps': scrapers.googlemaps.scrape_googlemaps,
        }
        self.scraper_display_names = {
            'googlemaps': 'Google Maps UAE',
        }

    async def start_scraping(self, scraper_name: str, params: dict) -> str:
        """Start a scraping task"""
        if scraper_name not in self.scrapers:
            raise ValueError(f"Unknown scraper: {scraper_name}")
        
        task_id = str(uuid.uuid4())
        
        # Create task
        task = ScraperTask(
            task_id=task_id,
            scraper_name=scraper_name,
            status='pending',
            progress=0,
            message='Initializing...',
            created_at=datetime.now()
        )
        
        self.tasks[task_id] = task
        
        # Start scraping in background
        asyncio.create_task(self._run_scraper(task_id, scraper_name, params))
        
        return task_id

    async def _run_scraper(self, task_id: str, scraper_name: str, params: dict):
        """Run the scraper"""
        task = self.tasks[task_id]
        
        try:
            task.status = 'running'
            task.message = 'Starting scraper...'
            
            # Progress callback
            def progress_callback(tid: str, progress: int, message: str):
                if tid in self.tasks:
                    self.tasks[tid].progress = progress
                    self.tasks[tid].message = message
            
            # Create request object
            from .models import ScrapeRequest
            request = ScrapeRequest(
                search_term=params.get('search_term', ''),
                location=params.get('location', 'UAE'),
                category=params.get('category', ''),
                max_pages=params.get('max_pages', 5)
            )
            
            # Run scraper
            scraper_func = self.scrapers[scraper_name]
            result = await scraper_func(request, progress_callback, task_id)
            
            # Update task with results
            task.status = 'completed'
            task.progress = 100
            task.message = f'Completed! Found {result.get("total_records", 0)} businesses'
            task.completed_at = datetime.now()
            task.filename = result.get('filename')
            task.total_records = result.get('total_records', 0)
            
            # Update daily stats
            self._update_daily_stats(scraper_name, result.get('total_records', 0))
            
        except Exception as e:
            task.status = 'failed'
            task.message = f'Error: {str(e)}'
            print(f"Scraper failed: {e}")

    def _update_daily_stats(self, scraper_name: str, records: int):
        """Update daily statistics"""
        display_name = self.scraper_display_names.get(scraper_name, scraper_name)
        
        # Find existing stat or create new
        existing_stat = None
        for stat in self.daily_stats:
            if stat.scraper_name == scraper_name:
                existing_stat = stat
                break
        
        if existing_stat:
            existing_stat.total_records += records
            existing_stat.runs += 1
            existing_stat.last_run = datetime.now()
        else:
            new_stat = ScrapeStats(
                scraper_name=scraper_name,
                display_name=display_name,
                total_records=records,
                runs=1,
                last_run=datetime.now()
            )
            self.daily_stats.append(new_stat)

    def get_task_status(self, task_id: str) -> Optional[dict]:
        """Get task status"""
        if task_id in self.tasks:
            return asdict(self.tasks[task_id])
        return None

    def get_all_tasks(self) -> List[dict]:
        """Get all tasks"""
        return [asdict(task) for task in self.tasks.values()]

    def get_daily_stats(self) -> List[dict]:
        """Get today's statistics"""
        return [asdict(stat) for stat in self.daily_stats]

    def get_today_summary(self) -> dict:
        """Get today's scraping summary - MISSING METHOD ADDED"""
        total_tasks = len(self.tasks)
        completed_tasks = len([t for t in self.tasks.values() if t.status == 'completed'])
        running_tasks = len([t for t in self.tasks.values() if t.status == 'running'])
        failed_tasks = len([t for t in self.tasks.values() if t.status == 'failed'])
        total_records = sum(stat.total_records for stat in self.daily_stats)
        
        return {
            'total_tasks': total_tasks,
            'completed_tasks': completed_tasks,
            'running_tasks': running_tasks,
            'failed_tasks': failed_tasks,
            'total_records': total_records,
            'stats': [asdict(stat) for stat in self.daily_stats]
        }

    def get_available_scrapers(self) -> List[dict]:
        """Get list of available scrapers"""
        return [
            {
                'name': name,
                'display_name': self.scraper_display_names.get(name, name),
                'description': 'High-quality verified business leads from Google Maps'
            }
            for name in self.scrapers.keys()
        ]

# Create a global instance of the scraper manager
scraper_manager = ScraperManager()