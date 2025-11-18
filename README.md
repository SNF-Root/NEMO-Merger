# NEMO-Merger

A comprehensive toolset for migrating SNSF (Stanford Nanofabrication Facility) data to the NEMO (Nano Environment Management and Operations) system. This project provides scripts to create accounts, projects, users, tools, rates, and interlocks in NEMO based on data from SNSF Excel files.

## Overview

The NEMO-Merger project consists of several Python scripts that work together to migrate data from SNSF spreadsheets into the NEMO system. The migration process follows three main phases:

1. **Download Phase**: Download existing data from NEMO API to check for duplicates and create lookup mappings
2. **Data Preparation Phase**: Process and clean Excel spreadsheets from SNSF
3. **Creation Phase**: Create new entities in NEMO (accounts, projects, users, tools, etc.)

## Prerequisites

- Python 3.7+
- Access to NEMO API with valid authentication token
- SNSF data files in Excel format

## Installation

1. Clone the repository:
```bash
git clone https://github.com/SNF-Root/NEMO-Merger.git
cd NEMO-Merger
```

2. Create a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file with your NEMO API token:
```bash
echo "NEMO_TOKEN=your_actual_nemo_token_here" > .env
```

## Configuration

### Environment Variables

Create a `.env` file in the project root with:

```env
NEMO_TOKEN=your_nemo_api_token_here
```

### Data Files

Place your SNSF data files in the `SNSF-Data/` directory. The main files used are:

- `Copy of SNSF PTAs for Alex Denton.xlsx` - Contains PI information, project details (PTAs), and account types
- `Internal User Tracking and Emails.xlsx` - Internal user information
- `SNSF - External Users.xlsx` - External user information
- `SNC Tools.xlsx` - SNC facility tools
- `SNL Tools.xlsx` - SNL facility tools  
- `SMF Tools.xlsx` - SMF facility tools
- `SNC_rates_report.xlsx`, `SNL_rates_report.xlsx`, `SMF_rates_report.xlsx` - Rate information
- `Inventory Rates.txt` - Additional rate data

## Process Flow

### Phase 1: Download Existing Data from NEMO

Before creating new entities, download existing data from NEMO to:
- Check for duplicates
- Create lookup mappings (account names → IDs, PTAs → project IDs, etc.)
- Understand current system state

```
┌─────────────────────────────────────────────────────────┐
│ Phase 1: Download Existing Data from NEMO              │
└─────────────────────────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
        ▼                 ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Download     │  │ Download     │  │ Download     │
│ Account      │  │ Projects     │  │ Users        │
│ Types        │  │              │  │              │
└──────────────┘  └──────────────┘  └──────────────┘
        │                 │                 │
        ▼                 ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Download     │  │ Download     │  │ Download     │
│ Rate         │  │ Tools        │  │ Interlock    │
│ Categories   │  │              │  │ Categories   │
└──────────────┘  └──────────────┘  └──────────────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │ Generate Lookup Files │
              │ (JSON mappings)       │
              └───────────────────────┘
```

**Download Scripts:**

1. **Download Account Types**
   ```bash
   python3 download_account_types.py
   ```
   - Creates: `nemo_account_types.json`
   - Maps account type names to IDs (Local, Industrial, No Charge, etc.)

2. **Download Rate Categories**
```bash
python3 download_rate_categories.py
```
   - Creates: `nemo_rate_categories.json`, `rate_category_mapping.json`
   - Maps Excel account types to NEMO rate category IDs

3. **Download Existing Accounts**
```bash
python3 download_accounts.py
```
   - Creates: `nemo_accounts.json`, `nemo_accounts.csv`, `account_lookup.json`
   - Maps account names to account IDs for duplicate checking

4. **Download Existing Projects**
   ```bash
   python3 download_projects.py
   ```
   - Creates: `nemo_projects.json`, `nemo_projects.csv`, `pta_lookup.json`
   - Maps PTAs (application_identifier) to project IDs

5. **Download Existing Users**
   ```bash
   python3 download_users.py
   ```
   - Creates: `snf_user_download.json`, `existing_emails.json`, `existing_usernames.json`
   - Used to check for duplicate users by email or username

6. **Download Existing Tools**
   ```bash
   python3 download_tools.py
   ```
   - Creates: `tools_download.json`, `tool_lookup.json`
   - Maps tool names to tool IDs

7. **Download Interlock Card Categories**
   ```bash
   python3 download_interlock_card_categories.py
   ```
   - Creates: `interlock_card_categories_download.json`, `interlock_card_category_lookup.json`

8. **Download Rate Types**
   ```bash
   python3 download_rate_types.py
   ```
   - Creates: `billing_rate_types_download.json`, `billing_rate_type_lookup.json`

### Phase 2: Data Preparation & Spreadsheet Massaging

Process and clean the Excel spreadsheets from SNSF to prepare them for import:

```
┌─────────────────────────────────────────────────────────┐
│ Phase 2: Data Preparation & Spreadsheet Massaging       │
└─────────────────────────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
        ▼                 ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Read Excel   │  │ Clean &      │  │ Validate &   │
│ Files from   │  │ Normalize    │  │ Filter Data  │
│ SNSF-Data/   │  │ Data         │  │              │
└──────────────┘  └──────────────┘  └──────────────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │ Data Processing Steps │
              │                      │
              │ • Remove duplicates   │
              │ • Handle missing      │
              │   values (NaN)        │
              │ • Normalize column    │
              │   names               │
              │ • Strip whitespace    │
              │ • Map Excel types to  │
              │   NEMO types          │
              │ • Filter invalid      │
              │   records             │
              └───────────────────────┘
```

**Data Processing Details:**

The scripts automatically handle:

- **Column Name Normalization**: Supports both old format (`pi email`, `type`) and new format (`Account`, `project_type`)
- **Data Cleaning**:
  - Removes rows with missing required fields (NaN values)
  - Strips whitespace from text fields
  - Filters out invalid account names (empty strings, "nan", ",")
  - Handles duplicate PTAs and account names
- **Type Mapping**: Maps Excel account types to NEMO account types:
  - `local` → Local
  - `industrial` → Industrial
  - `no charge` → No Charge
  - `other academic` → Other Academic
  - `industrial-sbir` → Industrial-SBIR
  - `foreign` → Other Academic (default)

**Example Spreadsheet Processing:**

For accounts, the script reads from `Copy of SNSF PTAs for Alex Denton.xlsx`:
- Extracts unique account names and types
- Filters out duplicates
- Maps account types to NEMO account type IDs
- Validates data before creation

For projects, the script:
- Extracts unique PTAs (application_identifier)
- Links projects to accounts by account name
- Sets project type based on account type

### Phase 3: Create Entities in NEMO

Create new entities in NEMO using the prepared data:

```
┌─────────────────────────────────────────────────────────┐
│ Phase 3: Create Entities in NEMO                       │
└─────────────────────────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
        ▼                 ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Create       │  │ Create       │  │ Create       │
│ Accounts     │  │ Projects     │  │ Users        │
│              │  │              │  │              │
└──────────────┘  └──────────────┘  └──────────────┘
        │                 │                 │
        ▼                 ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Create       │  │ Create       │  │ Create       │
│ Tools        │  │ Rates        │  │ Interlocks   │
└──────────────┘  └──────────────┘  └──────────────┘
```

**Creation Scripts:**

1. **Create Accounts**
```bash
python3 create_accounts.py
```
   - Reads from: `SNSF-Data/Copy of SNSF PTAs for Alex Denton.xlsx`
   - Filters out existing accounts using `account_lookup.json`
   - Creates accounts with proper account type mapping
   - Output: Creates accounts via NEMO API

2. **Create Projects**
```bash
python3 create_projects.py
```
   - Reads from: `SNSF-Data/Copy of SNSF PTAs for Alex Denton.xlsx`
   - Filters out existing projects by PTA using `pta_lookup.json`
   - Links projects to accounts via account name
   - Sets rate categories based on project type
   - Output: Creates projects via NEMO API, logs to `project_creation_log_*.log` and `created_projects_*.json`

3. **Create Internal Users**
   ```bash
   python3 create_internal_users.py
   ```
   - Reads from: `SNSF-Data/Internal User Tracking and Emails.xlsx`
   - Checks for duplicate emails/usernames using `existing_emails.json` and `existing_usernames.json`
   - Links users to projects via PTA lookup
   - Output: Creates internal users via NEMO API

4. **Create External Users**
   ```bash
   python3 create_external_users.py
   ```
   - Reads from: `SNSF-Data/SNSF - External Users.xlsx` and facility-specific qualified users files
   - Checks for duplicate emails/usernames
   - Links users to projects via PTA lookup
   - Output: Creates external users via NEMO API with detailed logging

5. **Create Tools**
   ```bash
   python3 create_tools.py
   ```
   - Reads from: `SNSF-Data/SNC Tools.xlsx`, `SNL Tools.xlsx`, `SMF Tools.xlsx`
   - Filters out existing tools using `tool_lookup.json`
   - Output: Creates tools via NEMO API, logs to `tool_creation_log_*.log` and `created_tools_*.json`

6. **Create Rate Types**
   ```bash
   python3 create_rate_type.py
   ```
   - Creates rate types in NEMO

7. **Create Rates**
   ```bash
   python3 create_rates.py
   ```
   - Reads from rate report Excel files
   - Creates billing rates in NEMO

8. **Create Interlocks**
   ```bash
   python3 create_interlocks.py
   ```
   - Creates interlock entries in NEMO
   - Uses interlock card category lookup

## Complete Workflow Example

Here's a typical migration workflow:

```bash
# 1. Download existing data (Phase 1)
python3 download_account_types.py
python3 download_rate_categories.py
python3 download_accounts.py
python3 download_projects.py
python3 download_users.py
python3 download_tools.py
python3 download_interlock_card_categories.py
python3 download_rate_types.py

# 2. Create entities (Phase 3 - data preparation happens automatically)
python3 create_accounts.py          # Creates accounts from Excel
python3 create_projects.py          # Creates projects from Excel
python3 create_internal_users.py    # Creates internal users from Excel
python3 create_external_users.py    # Creates external users from Excel
python3 create_tools.py             # Creates tools from Excel
python3 create_rate_type.py         # Creates rate types
python3 create_rates.py              # Creates rates from Excel
python3 create_interlocks.py        # Creates interlocks
```

## Script Details

### Download Scripts

All download scripts follow a similar pattern:
- Test API connection and authentication
- Download data from NEMO API (with pagination support where needed)
- Save raw data to JSON files
- Generate lookup files (name → ID mappings)
- Optionally save to CSV for easier inspection

### Creation Scripts

All creation scripts follow a similar pattern:
- Load lookup files from Phase 1
- Read and process Excel files from `SNSF-Data/`
- Filter out duplicates using lookup files
- Validate and clean data
- Create entities via NEMO API
- Log results (both console output and JSON log files)

### Utility Scripts

- `compare_accounts.py` - Compare accounts between different sources
- `check_accounts_with_projects.py` - Check which accounts have associated projects
- `find_duplicate_ptas.py` - Find duplicate PTAs in the data
- `map_dirty_to_nemo_ids.py` - Map "dirty" account names to NEMO account IDs
- `remove_accounts.py` - Remove accounts from NEMO
- `add_allen_prefix.py` / `remove_allen_prefix.py` - Utility scripts for account name manipulation
- `add_area_to_user.py` - Add area information to users

## File Structure

```
NEMO-Merger/
├── SNSF-Data/                          # SNSF Excel data files
│   ├── Copy of SNSF PTAs for Alex Denton.xlsx
│   ├── Internal User Tracking and Emails.xlsx
│   ├── SNSF - External Users.xlsx
│   ├── SNC Tools.xlsx
│   ├── SNL Tools.xlsx
│   ├── SMF Tools.xlsx
│   ├── SNC_rates_report.xlsx
│   ├── SNL_rates_report.xlsx
│   ├── SMF_rates_report.xlsx
│   └── Inventory Rates.txt
├── download_*.py                       # Download scripts (Phase 1)
├── create_*.py                         # Creation scripts (Phase 3)
├── *.py                                # Utility scripts
├── .env                                # Environment variables (not committed)
├── .gitignore                          # Git ignore rules
├── requirements.txt                    # Python dependencies
└── README.md                           # This file
```

## Generated Files

The scripts generate several JSON and CSV files (not committed to git):

**Lookup Files:**
- `nemo_account_types.json` - Account types from NEMO
- `nemo_rate_categories.json` - Rate categories from NEMO
- `rate_category_mapping.json` - Excel type to rate category mapping
- `nemo_accounts.json` / `nemo_accounts.csv` - Accounts from NEMO
- `account_lookup.json` - Account name to ID mapping
- `nemo_projects.json` / `nemo_projects.csv` - Projects from NEMO
- `pta_lookup.json` - PTA to project ID mapping
- `snf_user_download.json` - Users from NEMO
- `existing_emails.json` - Existing email addresses
- `existing_usernames.json` - Existing usernames
- `tools_download.json` - Tools from NEMO
- `tool_lookup.json` - Tool name to ID mapping
- `interlock_card_categories_download.json` - Interlock categories
- `interlock_card_category_lookup.json` - Interlock category lookup
- `billing_rate_types_download.json` - Rate types
- `billing_rate_type_lookup.json` - Rate type lookup

**Log Files:**
- `project_creation_log_*.log` - Project creation logs
- `created_projects_*.json` - Created projects log
- `tool_creation_log_*.log` - Tool creation logs
- `created_tools_*.json` - Created tools log

## Error Handling

All scripts include comprehensive error handling:
- API connection testing before operations
- Authentication validation
- Data validation and cleaning
- Detailed logging and progress reporting
- Graceful fallbacks for missing data
- Duplicate detection and filtering
- Network error handling with retries

## API Endpoints

The scripts interact with these NEMO API endpoints:
- `https://nemo.stanford.edu/api/accounts/` - Account management
- `https://nemo.stanford.edu/api/projects/` - Project management
- `https://nemo.stanford.edu/api/users/` - User management
- `https://nemo.stanford.edu/api/tools/` - Tool management
- `https://nemo.stanford.edu/api/billing/rate_categories/` - Rate categories
- `https://nemo.stanford.edu/api/billing/rate_types/` - Rate types
- `https://nemo.stanford.edu/api/interlock_card_categories/` - Interlock categories

## Best Practices

1. **Always run download scripts first** to get the latest state from NEMO
2. **Review generated lookup files** before running creation scripts
3. **Check logs** after running creation scripts to verify success
4. **Run scripts in order**: Accounts → Projects → Users → Tools → Rates
5. **Keep backups** of Excel files and generated JSON files
6. **Test with small datasets** before running full migrations

## Troubleshooting

### Common Issues

**Authentication Errors:**
- Verify `NEMO_TOKEN` is set correctly in `.env` file
- Check token hasn't expired
- Ensure token has necessary permissions

**Missing Data Files:**
- Verify Excel files are in `SNSF-Data/` directory
- Check file names match expected names in scripts
- Ensure files are not open in Excel when running scripts

**Duplicate Errors:**
- Run download scripts to refresh lookup files
- Check for case sensitivity issues in account/project names
- Review duplicate detection logic in scripts

**API Rate Limiting:**
- Scripts include delays between API calls
- If rate limited, wait and retry
- Consider running scripts during off-peak hours

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions, please create an issue in the GitHub repository or contact the SNF team.

## Changelog

### v1.0.0
- Initial release
- Support for accounts, projects, users, tools, rates, and interlocks
- Rate category mapping
- Comprehensive error handling
- Excel data processing
- Duplicate detection and filtering