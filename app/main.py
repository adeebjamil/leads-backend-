from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import os
from .models import ScrapeRequest, ScrapeResponse, ScraperStatus
from .scraper_manager import scraper_manager

app = FastAPI(
    title="Google Maps Business Scraper",
    description="High-quality verified business leads from Google Maps",
    version="2.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "message": "Google Maps Business Scraper API", 
        "status": "running",
        "version": "2.0.0",
        "description": "High-quality verified business leads from Google Maps"
    }

# ONLY GOOGLE MAPS SCRAPER ENDPOINT
@app.post("/scrape/googlemaps", response_model=ScrapeResponse)
async def scrape_googlemaps(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """Start Google Maps scraping"""
    try:
        # Convert request to dict for scraper_manager
        params = {
            'search_term': request.search_term,
            'location': request.location,
            'category': request.category,
            'max_pages': request.max_pages
        }
        
        task_id = await scraper_manager.start_scraping("googlemaps", params)
        
        return ScrapeResponse(
            task_id=task_id,
            message="Google Maps scraping started",
            status=ScraperStatus.RUNNING
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# TASK STATUS ENDPOINTS
@app.get("/status/{task_id}")
async def get_task_status(task_id: str):
    """Get scraping task status"""
    task = scraper_manager.get_task_status(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"task": task}  # Wrap in object for consistency

@app.get("/status")
async def get_all_tasks():
    """Get all scraping tasks"""
    return {"tasks": scraper_manager.get_all_tasks()}

# STATISTICS ENDPOINTS
@app.get("/stats/daily")
async def get_daily_stats():
    """Get daily scraping statistics"""
    return {"stats": scraper_manager.get_daily_stats()}

@app.get("/stats/today")
async def get_today_summary():
    """Get today's scraping summary"""
    return scraper_manager.get_today_summary()

# SCRAPERS INFO ENDPOINT
@app.get("/scrapers")
async def get_available_scrapers():
    """Get available scrapers"""
    return {"scrapers": scraper_manager.get_available_scrapers()}

# FIXED DOWNLOAD ENDPOINTS
@app.get("/download/{filename}/csv")
async def download_csv(filename: str):
    """Download CSV file"""
    exports_dir = "exports"
    filepath = os.path.join(exports_dir, f"{filename}.csv")
    
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="CSV file not found")
    
    return FileResponse(
        filepath,
        media_type="text/csv",
        filename=f"{filename}.csv",
        headers={"Content-Disposition": f"attachment; filename={filename}.csv"}
    )

@app.get("/download/{filename}/excel")
async def download_excel(filename: str):
    """Download Excel file"""
    exports_dir = "exports"
    filepath = os.path.join(exports_dir, f"{filename}.xlsx")
    
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Excel file not found")
    
    return FileResponse(
        filepath,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"{filename}.xlsx",
        headers={"Content-Disposition": f"attachment; filename={filename}.xlsx"}
    )

# LEGACY DOWNLOAD ENDPOINT (for backward compatibility)
@app.get("/download/{filename}")
async def download_file(filename: str, file_type: str = "csv"):
    """Download scraped data file"""
    exports_dir = "exports"
    file_extension = "csv" if file_type == "csv" else "xlsx"
    filepath = os.path.join(exports_dir, f"{filename}.{file_extension}")
    
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")
    
    media_type = "text/csv" if file_type == "csv" else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    
    return FileResponse(
        filepath,
        media_type=media_type,
        filename=f"{filename}.{file_extension}",
        headers={"Content-Disposition": f"attachment; filename={filename}.{file_extension}"}
    )

@app.get("/debug/stats")
async def debug_stats():
    """Debug endpoint to check statistics"""
    tasks = scraper_manager.get_all_tasks()
    daily_stats = scraper_manager.get_daily_stats()
    today_summary = scraper_manager.get_today_summary()
    
    return {
        "tasks": tasks,
        "daily_stats": daily_stats,
        "today_summary": today_summary,
        "tasks_count": len(tasks),
        "completed_tasks": [t for t in tasks if t['status'] == 'completed'],
        "total_records_from_tasks": sum(t.get('total_records', 0) for t in tasks if t['status'] == 'completed')
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)