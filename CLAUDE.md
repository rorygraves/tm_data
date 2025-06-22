# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an experimental project for exploring Toastmasters International (TMI) historical data. It downloads performance data for clubs, areas, divisions, and districts from the Toastmasters dashboards and stores it in a PostgreSQL database for analysis.

## Environment Setup

The project requires:
- Scala 2.13.16
- SBT 1.10.2
- PostgreSQL database

## Database Configuration

The application requires a PostgreSQL database connection with the following environment variables:
- `TM_DATA_DB_USER` - Database username
- `TM_DATA_DB_PASSWORD` - Database password  
- `TM_DATA_DB_URL` - JDBC connection URL

## Common Commands

### Build and Run

```bash
# Build the project
sbt compile

# Run the main application (including environment variables from .env file)
env $(cat .env | grep -v "^#" | xargs) sbt "runMain com.github.rorygraves.tm_data.Main"

# Run the debug application (including environment variables from .env file)
env $(cat .env | grep -v "^#" | xargs) sbt "runMain com.github.rorygraves.tm_data.DebugMain"

# Run the data fix application (including environment variables from .env file)
env $(cat .env | grep -v "^#" | xargs) sbt "runMain com.github.rorygraves.tm_data.DataFixMain"
```

### Testing and Quality

```bash
# Format code using Scalafmt
sbt scalafmt

# Check code formatting
sbt scalafmtCheck

# No formal test suite is currently implemented
```

### Project Maintenance

```bash
# Check for dependency updates
sbt dependencyUpdates

# Show dependency tree
sbt dependencyTree
```

## Project Architecture

The project is structured as follows:

1. **Data Downloaders** - Components that fetch CSV data from Toastmasters dashboards:
   - `TMDocumentDownloader` - Base downloader functionality
   - `TMAreaDataDownloader` - Area-specific data downloader
   - `TMDivisionDataDownloader` - Division-specific data downloader
   - `TMClubDataDownloader` (via `HistoricClubPerfGenerator`) - Club-specific data

2. **Data Models** - Classes representing different data points:
   - `TMClubDataPoint` - Club performance metrics
   - `TMAreaDataPoint` - Area performance metrics
   - `TMDivisionDataPoint` - Division performance metrics  
   - `TMDistrictSummaryDataPoint` - District summary metrics

3. **Database Layer** - Tables and database access:
   - `AbstractTable` - Base table functionality
   - `HistoricClubPerfTable`, `HistoricAreaPerfTable`, etc. - Specific table implementations
   - `DBRunner` - Database execution utilities

4. **Utilities**:
   - `HttpUtil` - HTTP requests with caching
   - `TMUtil` - Toastmasters-specific utility functions
   - `FormatUtil` - Formatting utilities
   - `CSVCodec` - CSV parsing utilities

## Data Flow

1. The application fetches district summary data for all districts and years
2. For each district, it downloads:
   - Club information
   - Club performance metrics
   - Area performance metrics
   - Division performance metrics
3. All data is stored in database tables
4. SQL Views (created manually) provide simplified access to the latest data

## Implementation Notes

- HTTP requests are cached in a local folder to avoid repeated downloads
- The code includes rate limiting (sleep between district imports) to avoid overloading the Toastmasters servers
- Historical data is fetched back to 2012
- Database tables use Slick for type-safe database interactions
- Code is formatted with Scalafmt (config in `.scalafmt.conf`) with 140 character line limit
- Logging is configured via Logback (`src/main/resources/logback.xml`)

## Additional Applications

- **Main** - Primary data collection application for downloading and storing TMI data
- **DebugMain** - Debug version for testing and development purposes
- **DataFixMain** - Utility for fixing or updating existing data in the database